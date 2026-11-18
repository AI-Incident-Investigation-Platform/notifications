import asyncio
import base64
import hashlib
import secrets
from datetime import UTC, datetime
from uuid import UUID, uuid7

from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import delete, func, insert, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.schemas import PreferencesRequest, ReadRequest, WebhookCreate, WebhookPatch
from src.core.db.models import (
    audit_events,
    channels,
    deliveries,
    inbox_items,
    outbox,
    preferences,
    telegram_links,
    webhooks,
)
from src.core.rules import CHANNELS, new_link_code, validate_webhook_url


class NotificationService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        access: AccessChecker,
        allowed_hosts: set[str],
        bot_url: str,
    ):
        self.sessions = sessions
        self.access = access
        self.allowed_hosts = allowed_hosts
        self.bot_url = bot_url
        self.writer = OutboxWriter(outbox)

    async def list_inbox(
        self,
        user_id: UUID,
        unread: bool | None,
        project_id: UUID | None,
        cursor: UUID | None,
        limit: int = 50,
    ):
        async with self.sessions() as session:
            query = select(inbox_items).where(inbox_items.c.user_id == user_id)
            if unread is not None:
                query = query.where(
                    inbox_items.c.read_at.is_(None)
                    if unread
                    else inbox_items.c.read_at.is_not(None)
                )
            if project_id:
                query = query.where(inbox_items.c.project_id == project_id)
            if cursor:
                anchor = (
                    await session.execute(
                        select(inbox_items.c.created_at).where(
                            inbox_items.c.id == cursor, inbox_items.c.user_id == user_id
                        )
                    )
                ).scalar_one_or_none()
                if anchor:
                    query = query.where(
                        or_(
                            inbox_items.c.created_at < anchor,
                            (inbox_items.c.created_at == anchor) & (inbox_items.c.id < cursor),
                        )
                    )
            rows = (
                (
                    await session.execute(
                        query.order_by(
                            inbox_items.c.created_at.desc(), inbox_items.c.id.desc()
                        ).limit(limit + 1)
                    )
                )
                .mappings()
                .all()
            )
            return {
                "items": [dict(row) for row in rows[:limit]],
                "next_cursor": str(rows[limit - 1]["id"]) if len(rows) > limit else None,
            }

    async def unread_count(self, user_id: UUID):
        async with self.sessions() as session:
            return (
                await session.scalar(
                    select(func.count())
                    .select_from(inbox_items)
                    .where(inbox_items.c.user_id == user_id, inbox_items.c.read_at.is_(None))
                )
                or 0
            )

    async def read(self, user_id: UUID, body: ReadRequest):
        async with self.sessions.begin() as session:
            where = (
                inbox_items.c.id.in_(body.ids)
                if body.ids is not None
                else inbox_items.c.created_at <= body.before
            )
            result = await session.execute(
                update(inbox_items)
                .where(inbox_items.c.user_id == user_id, inbox_items.c.read_at.is_(None), where)
                .values(read_at=datetime.now(UTC))
                .returning(inbox_items.c.id)
            )
            await self.changed(session, user_id)
            return {"updated": len(result.scalars().all())}

    async def delete_item(self, user_id: UUID, item_id: UUID):
        async with self.sessions.begin() as session:
            result = await session.execute(
                delete(inbox_items)
                .where(inbox_items.c.user_id == user_id, inbox_items.c.id == item_id)
                .returning(inbox_items.c.id)
            )
            removed = result.scalar_one_or_none() is not None
            if removed:
                await self.changed(session, user_id)
            return removed

    async def changed(self, session: AsyncSession, user_id: UUID):
        count = await session.scalar(
            select(func.count())
            .select_from(inbox_items)
            .where(inbox_items.c.user_id == user_id, inbox_items.c.read_at.is_(None))
        )
        message = MessageV1.event(
            "inbox.changed", "notifications", {"type": "inbox.changed", "unread_count": count}
        )
        await self.writer.add(
            session, message, Destination.live("notifications", f"user.{user_id}")
        )

    async def get_preferences(self, user_id: UUID):
        async with self.sessions() as session:
            rows = (
                (await session.execute(select(preferences).where(preferences.c.user_id == user_id)))
                .mappings()
                .all()
            )
            groups: dict[tuple[UUID | None, str], list[str]] = {}
            for row in rows:
                key = (row["project_id"], row["event_kind"])
                channels = groups.setdefault(key, [])
                if row["enabled"]:
                    channels.append(row["channel"])
            return {
                "items": [
                    {"project_id": p, "kind": k, "channels": v} for (p, k), v in groups.items()
                ]
            }

    async def put_preferences(self, principal: Principal, body: PreferencesRequest):
        user_id = principal.user_id
        for item in body.items:
            if item.project_id:
                await self.access.grant(principal, item.project_id)
        async with self.sessions.begin() as session:
            await session.execute(delete(preferences).where(preferences.c.user_id == user_id))
            for item in body.items:
                for channel in CHANNELS:
                    await session.execute(
                        insert(preferences).values(
                            user_id=user_id,
                            project_id=item.project_id,
                            project_key=item.project_id or UUID(int=0),
                            event_kind=item.kind,
                            channel=channel,
                            enabled=channel in item.channels,
                        )
                    )
        return body.model_dump(mode="json")

    async def link_telegram(self, user_id: UUID):
        code, digest, expiry = new_link_code()
        async with self.sessions.begin() as session:
            await session.execute(
                insert(telegram_links).values(code_hash=digest, user_id=user_id, expires_at=expiry)
            )
        return {"code": code, "bot_url": self.bot_url, "expires_at": expiry}

    async def complete_telegram_link(self, code: str, chat_id: str, title: str) -> bool:
        if not code or not chat_id:
            return False
        digest = hashlib.sha256(code.encode()).hexdigest()
        async with self.sessions.begin() as session:
            link = (
                await session.execute(
                    update(telegram_links)
                    .where(
                        telegram_links.c.code_hash == digest,
                        telegram_links.c.used_at.is_(None),
                        telegram_links.c.expires_at > datetime.now(UTC),
                    )
                    .values(used_at=datetime.now(UTC))
                    .returning(telegram_links.c.user_id)
                )
            ).scalar_one_or_none()
            if link is None:
                return False
            await session.execute(
                insert(channels).values(
                    id=uuid7(), user_id=link, type="telegram", title=title[:256], chat_id=chat_id
                )
            )
            return True

    async def list_channels(self, user_id: UUID):
        async with self.sessions() as session:
            return [
                dict(row)
                for row in (
                    await session.execute(
                        select(
                            channels.c.id, channels.c.type, channels.c.title, channels.c.created_at
                        ).where(channels.c.user_id == user_id)
                    )
                ).mappings()
            ]

    async def delete_channel(self, user_id: UUID, channel_id: UUID):
        async with self.sessions.begin() as session:
            result = await session.execute(
                delete(channels)
                .where(channels.c.id == channel_id, channels.c.user_id == user_id)
                .returning(channels.c.id)
            )
            return result.scalar_one_or_none() is not None

    async def list_webhooks(self, principal: Principal, project_id: UUID):
        await self.access.require(principal, project_id, "project.manage")
        async with self.sessions() as session:
            return [
                self.public_webhook(row)
                for row in (
                    await session.execute(
                        select(webhooks).where(webhooks.c.project_id == project_id)
                    )
                ).mappings()
            ]

    @staticmethod
    def public_webhook(row):
        return {
            key: row[key] for key in ("id", "project_id", "url", "events", "active", "created_at")
        }

    async def create_webhook(self, principal: Principal, project_id: UUID, body: WebhookCreate):
        await self.access.require(principal, project_id, "project.manage")
        await asyncio.to_thread(validate_webhook_url, body.url, self.allowed_hosts)
        secret = secrets.token_bytes(32)
        hook_id = uuid7()
        async with self.sessions.begin() as session:
            await session.execute(
                insert(webhooks).values(
                    id=hook_id,
                    project_id=project_id,
                    url=body.url,
                    events=body.events,
                    secret=secret,
                )
            )
            await session.execute(
                insert(audit_events).values(
                    id=uuid7(),
                    actor_id=principal.user_id,
                    action="webhook.created",
                    target_id=hook_id,
                )
            )
        return {
            "webhook": {
                "id": hook_id,
                "project_id": project_id,
                "url": body.url,
                "events": body.events,
                "active": True,
                "created_at": datetime.now(UTC),
            },
            "secret": base64.urlsafe_b64encode(secret).decode(),
        }

    async def update_webhook(
        self, principal: Principal, project_id: UUID, hook_id: UUID, body: WebhookPatch
    ):
        await self.access.require(principal, project_id, "project.manage")
        changes = body.model_dump(exclude_unset=True, exclude_none=True)
        if "url" in changes:
            await asyncio.to_thread(validate_webhook_url, changes["url"], self.allowed_hosts)
        async with self.sessions.begin() as session:
            row = (
                (
                    await session.execute(
                        update(webhooks)
                        .where(webhooks.c.id == hook_id, webhooks.c.project_id == project_id)
                        .values(**changes)
                        .returning(webhooks)
                    )
                )
                .mappings()
                .one_or_none()
                if changes
                else (
                    await session.execute(
                        select(webhooks).where(
                            webhooks.c.id == hook_id, webhooks.c.project_id == project_id
                        )
                    )
                )
                .mappings()
                .one_or_none()
            )
            return self.public_webhook(row) if row else None

    async def delete_webhook(self, principal: Principal, project_id: UUID, hook_id: UUID):
        await self.access.require(principal, project_id, "project.manage")
        async with self.sessions.begin() as session:
            result = await session.execute(
                delete(webhooks)
                .where(webhooks.c.id == hook_id, webhooks.c.project_id == project_id)
                .returning(webhooks.c.id)
            )
            return result.scalar_one_or_none() is not None

    async def test_webhook(self, principal: Principal, hook_id: UUID):
        async with self.sessions() as session:
            project_id = await session.scalar(
                select(webhooks.c.project_id).where(webhooks.c.id == hook_id)
            )
        if project_id is None:
            return False
        await self.access.require(principal, project_id, "project.manage")
        async with self.sessions.begin() as session:
            exists = await session.scalar(
                select(webhooks.c.id).where(
                    webhooks.c.id == hook_id, webhooks.c.project_id == project_id
                )
            )
            if exists is None:
                return False
            await session.execute(
                insert(deliveries).values(
                    id=uuid7(),
                    webhook_id=hook_id,
                    source_event_id=uuid7(),
                    payload=MessageV1.event(
                        "webhook.test", "notifications", {"project_id": str(project_id)}
                    ).model_dump(mode="json"),
                )
            )
            return True

    async def list_deliveries(self, principal: Principal, hook_id: UUID):
        async with self.sessions() as session:
            hook = (
                await session.execute(select(webhooks.c.project_id).where(webhooks.c.id == hook_id))
            ).scalar_one_or_none()
        if hook is None:
            return None
        await self.access.require(principal, hook, "project.manage")
        async with self.sessions() as session:
            rows = (
                (
                    await session.execute(
                        select(
                            deliveries.c.id,
                            deliveries.c.webhook_id,
                            deliveries.c.source_event_id.label("event_id"),
                            deliveries.c.status,
                            deliveries.c.attempts,
                            deliveries.c.last_status_code,
                            deliveries.c.next_attempt_at,
                            deliveries.c.created_at,
                        )
                        .where(deliveries.c.webhook_id == hook_id)
                        .order_by(deliveries.c.created_at.desc())
                        .limit(100)
                    )
                )
                .mappings()
                .all()
            )
            return [dict(row) for row in rows]
