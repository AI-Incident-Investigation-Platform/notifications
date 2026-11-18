import base64
import json
import socket
from datetime import UTC, datetime, timedelta
from uuid import uuid7

import httpx
import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.messaging.message import MessageV1
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from testcontainers.community.postgres import PostgresContainer

from src.api.service import NotificationService
from src.core.db.models import deliveries, idempotency_keys, inbox_items, metadata, outbox
from src.core.rules import sign_webhook
from src.main import create_app
from src.worker.delivery import DeliveryWorker
from src.worker.events import EventHandler


@pytest.mark.asyncio
async def test_inbox_coalescing_and_signed_webhook(monkeypatch):
    with PostgresContainer("postgres:17-alpine", dbname="notifications_test") as pg:
        url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql+asyncpg://")
        engine = create_async_engine(url)
        async with engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        user_id, session_id, project_id = (uuid7() for _ in range(3))
        principal = Principal(user_id=user_id, session_id=session_id)
        access = FakeAccessChecker()
        access.allow(project_id, user_id, "project.manage")
        service = NotificationService(sessions, access, {"hooks.example.com"}, "https://t.me/test")

        async def authenticate(_authorization):
            return principal

        app = create_app(service, authenticate, "telegram-secret")
        transport = httpx.ASGITransport(app=app)
        monkeypatch.setattr(
            socket,
            "getaddrinfo",
            lambda host, port, type: [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port))
            ],
        )
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = await client.post(
                f"/api/v1/projects/{project_id}/webhooks",
                json={"url": "https://hooks.example.com/receive", "events": ["incident.created"]},
            )
            assert created.status_code == 201, created.text
            webhook_id = created.json()["webhook"]["id"]
            secret = created.json()["secret"]
            listed = await client.get(f"/api/v1/projects/{project_id}/webhooks")
            assert listed.status_code == 200
            assert "secret" not in str(listed.json())
            handler = EventHandler(sessions)
            await handler(
                MessageV1.event(
                    "member.added", "projects", {"user_id": str(user_id)}, project_id=project_id
                )
            )
            first = MessageV1.event(
                "incident.created",
                "incidents",
                {"incident_id": str(uuid7()), "status": "open"},
                project_id=project_id,
            )
            await handler(first)
            await handler(first)
            await handler(
                MessageV1.event(
                    "incident.created",
                    "incidents",
                    {"incident_id": str(uuid7()), "status": "open"},
                    project_id=project_id,
                )
            )
            inbox = await client.get("/api/v1/inbox")
            assert inbox.status_code == 200, inbox.text
            incident_items = [
                row for row in inbox.json()["items"] if row["kind"] == "incident.created"
            ]
            assert len(incident_items) == 1
            assert incident_items[0]["title"] == "2 incidents created"
            assert (await client.get("/api/v1/inbox/unread-count")).json()["unread_count"] == 2
            read = await client.post("/api/v1/inbox/read", json={"ids": [incident_items[0]["id"]]})
            assert read.status_code == 200, read.text
            assert read.json()["updated"] == 1
            assert (await client.get("/api/v1/inbox/unread-count")).json()["unread_count"] == 1
            async with sessions() as session:
                queued = (
                    (
                        await session.execute(
                            select(deliveries).where(deliveries.c.webhook_id == webhook_id)
                        )
                    )
                    .mappings()
                    .all()
                )
                assert len(queued) == 2
            seen = []

            def deliver(request):
                seen.append(request)
                assert request.url.host == "93.184.215.14"
                assert request.headers["host"] == "hooks.example.com"
                assert request.extensions["sni_hostname"] == "hooks.example.com"
                assert request.headers["x-aiip-signature"] == sign_webhook(
                    base64.urlsafe_b64decode(secret), request.content
                )
                return httpx.Response(204)

            async with httpx.AsyncClient(transport=httpx.MockTransport(deliver)) as outgoing:
                worker = DeliveryWorker(sessions, outgoing, {"hooks.example.com"}, "")
                assert await worker.run_once() == 2
            assert len(seen) == 2
            async with sessions() as session:
                states = (await session.execute(select(deliveries.c.status))).scalars().all()
                assert states == ["delivered", "delivered"]
            test_request = await client.post(f"/api/v1/webhooks/{webhook_id}/test")
            assert test_request.status_code == 202, test_request.text

            def fail(_request):
                return httpx.Response(503)

            async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as outgoing:
                worker = DeliveryWorker(sessions, outgoing, {"hooks.example.com"}, "")
                assert await worker.run_once() == 1
            async with sessions.begin() as session:
                retry = (
                    (
                        await session.execute(
                            select(deliveries).where(deliveries.c.status == "failed")
                        )
                    )
                    .mappings()
                    .one()
                )
                assert retry["attempts"] == 1
                assert retry["last_status_code"] == 503
                await session.execute(
                    update(deliveries)
                    .where(deliveries.c.id == retry["id"])
                    .values(next_attempt_at=retry["created_at"])
                )
            async with httpx.AsyncClient(transport=httpx.MockTransport(deliver)) as outgoing:
                worker = DeliveryWorker(sessions, outgoing, {"hooks.example.com"}, "")
                assert await worker.run_once() == 1
            async with sessions() as session:
                recovered = await session.scalar(
                    select(deliveries.c.status).where(deliveries.c.id == retry["id"])
                )
                assert recovered == "delivered"
            disabled = await client.put(
                "/api/v1/notification-preferences",
                json={
                    "items": [
                        {"project_id": str(project_id), "kind": "incident.created", "channels": []}
                    ]
                },
            )
            assert disabled.status_code == 200, disabled.text
            assert (await client.get("/api/v1/notification-preferences")).json()["items"] == [
                {"project_id": str(project_id), "kind": "incident.created", "channels": []}
            ]
            before_count = (await client.get("/api/v1/inbox/unread-count")).json()["unread_count"]
            await handler(
                MessageV1.event(
                    "incident.created",
                    "incidents",
                    {"incident_id": str(uuid7())},
                    project_id=project_id,
                )
            )
            assert (await client.get("/api/v1/inbox/unread-count")).json()[
                "unread_count"
            ] == before_count
            link = await client.post("/api/v1/channels/telegram/link")
            assert link.status_code == 200
            linked = await client.post(
                "/internal/v1/telegram/updates",
                headers={"X-Telegram-Bot-Api-Secret-Token": "telegram-secret"},
                json={
                    "message": {
                        "chat": {"id": 12345, "first_name": "Test"},
                        "text": f"/start {link.json()['code']}",
                    }
                },
            )
            assert linked.json() == {"ok": True}
            assert (await client.get("/api/v1/channels")).json()["items"][0]["type"] == "telegram"
            preference = await client.put(
                "/api/v1/notification-preferences",
                json={
                    "items": [
                        {
                            "project_id": str(project_id),
                            "kind": "investigation.finished",
                            "channels": ["telegram"],
                        }
                    ]
                },
            )
            assert preference.status_code == 200
            await handler(
                MessageV1.event(
                    "investigation.finished",
                    "incidents",
                    {"investigation_id": str(uuid7()), "title": "RCA ready"},
                    project_id=project_id,
                )
            )
            telegram_requests = []

            def telegram_or_webhook(request):
                if request.url.host == "api.telegram.org":
                    telegram_requests.append(request)
                    assert request.url.path.endswith("/sendMessage")
                    assert json.loads(request.content)["chat_id"] == "12345"
                return httpx.Response(204)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(telegram_or_webhook)
            ) as outgoing:
                worker = DeliveryWorker(sessions, outgoing, {"hooks.example.com"}, "bot-token")
                assert await worker.run_once() == 2
            assert len(telegram_requests) == 1
            assert (await client.post(f"/api/v1/webhooks/{webhook_id}/test")).status_code == 202
            async with sessions.begin() as session:
                pending_id = await session.scalar(
                    select(deliveries.c.id).where(
                        deliveries.c.webhook_id == webhook_id,
                        deliveries.c.status == "pending",
                    )
                )
                assert pending_id is not None
                await session.execute(
                    update(deliveries).where(deliveries.c.id == pending_id).values(attempts=9)
                )
            async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as outgoing:
                worker = DeliveryWorker(sessions, outgoing, {"hooks.example.com"}, "")
                assert await worker.run_once() == 1
            async with sessions() as session:
                terminal = (
                    await session.execute(
                        select(deliveries.c.status, deliveries.c.attempts).where(
                            deliveries.c.id == pending_id
                        )
                    )
                ).one()
                assert terminal == ("dead", 10)
            email_only = await client.put(
                "/api/v1/notification-preferences",
                json={
                    "items": [
                        {
                            "project_id": str(project_id),
                            "kind": "incident.created",
                            "channels": ["email"],
                        }
                    ]
                },
            )
            assert email_only.status_code == 200
            async with sessions.begin() as session:
                await session.execute(
                    update(idempotency_keys)
                    .where(
                        idempotency_keys.c.user_id == user_id,
                        idempotency_keys.c.key == f"incident-storm:{project_id}",
                    )
                    .values(
                        response={
                            "window_start": (datetime.now(UTC) - timedelta(minutes=6)).isoformat(),
                            "count": 1,
                        }
                    )
                )
            async with sessions() as session:
                before_outbox = await session.scalar(select(func.count()).select_from(outbox))
            for _ in range(2):
                await handler(
                    MessageV1.event(
                        "incident.created",
                        "incidents",
                        {"incident_id": str(uuid7())},
                        project_id=project_id,
                    )
                )
            async with sessions() as session:
                after_outbox = await session.scalar(select(func.count()).select_from(outbox))
            assert after_outbox == (before_outbox or 0) + 1

            another_member, invitee = uuid7(), uuid7()
            await handler(
                MessageV1.event(
                    "member.added",
                    "projects",
                    {"user_id": str(another_member)},
                    project_id=project_id,
                )
            )
            await handler(
                MessageV1.event(
                    "export.completed",
                    "content",
                    {"export_id": str(uuid7()), "requested_by": str(user_id)},
                    project_id=project_id,
                )
            )
            await handler(
                MessageV1.event(
                    "invitation.created",
                    "projects",
                    {"invitation_id": str(uuid7()), "user_id": str(invitee)},
                    project_id=project_id,
                )
            )
            async with sessions() as session:
                export_recipients = set(
                    (
                        await session.execute(
                            select(inbox_items.c.user_id).where(
                                inbox_items.c.kind == "export.ready"
                            )
                        )
                    ).scalars()
                )
                invited_recipients = set(
                    (
                        await session.execute(
                            select(inbox_items.c.user_id).where(
                                inbox_items.c.kind == "invitation.received"
                            )
                        )
                    ).scalars()
                )
            assert export_recipients == {user_id}
            assert invited_recipients == {invitee}
            access.revoked_sessions.add(session_id)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                base_url="http://test",
            ) as revoked_client:
                revoked = await revoked_client.get(f"/api/v1/projects/{project_id}/webhooks")
            assert revoked.status_code == 401
        await engine.dispose()
