import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

from aiip_kit.messaging.message import MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.inbox import InboxGuard
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.models import (
    channels,
    deliveries,
    idempotency_keys,
    inbox,
    inbox_items,
    outbox,
    preferences,
    project_members,
    webhooks,
)
from src.core.rules import ALLOWED_KINDS, event_view


class EventHandler:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self.sessions = sessions
        self.guard = InboxGuard(inbox)
        self.writer = OutboxWriter(outbox)

    async def __call__(self, message: MessageV1, attempt: int = 1) -> None:
        async with self.sessions.begin() as session:
            if not await self.guard.accept(session, "notifications", message):
                return
            if message.type == "member.removed":
                await self.remove_member(session, message)
                return
            if message.type == "member.added" and message.project_id:
                await session.execute(
                    pg_insert(project_members)
                    .values(project_id=message.project_id, user_id=UUID(message.data["user_id"]))
                    .on_conflict_do_nothing()
                )
            if message.type == "project.deleting":
                await self.delete_project(session, message)
                return
            kind = {
                "integration.health_changed": "integration.degraded",
                "export.completed": "export.ready",
                "invitation.created": "invitation.received",
            }.get(message.type, message.type)
            if kind not in ALLOWED_KINDS and message.type != "incident.resolved":
                return
            user_ids = self.recipients(message)
            if message.project_id and not user_ids and message.type != "export.completed":
                user_ids = list(
                    (
                        await session.execute(
                            select(project_members.c.user_id).where(
                                project_members.c.project_id == message.project_id
                            )
                        )
                    ).scalars()
                )
            if kind in ALLOWED_KINDS:
                for user_id in user_ids:
                    await self.add_for_user(session, user_id, kind, message)
            if message.project_id and message.type in {
                "incident.created",
                "incident.resolved",
                "investigation.finished",
                "review.created",
            }:
                await self.enqueue_webhooks(session, message)

    @staticmethod
    def recipients(message: MessageV1) -> list[UUID]:
        if message.type == "export.completed":
            requester = message.data.get("requested_by")
            return [UUID(str(requester))] if requester else []
        raw = message.data.get("participants") or message.data.get("user_ids")
        if not isinstance(raw, list):
            raw = [message.data.get("user_id")] if message.data.get("user_id") else []
        return list({UUID(str(value)) for value in raw if value})

    async def add_for_user(
        self, session: AsyncSession, user_id: UUID, kind: str, message: MessageV1
    ) -> None:
        if message.type == "integration.health_changed" and message.data.get("status") not in {
            "degraded",
            "down",
        }:
            return
        if message.project_id:
            membership = await session.scalar(
                select(project_members.c.user_id).where(
                    project_members.c.project_id == message.project_id,
                    project_members.c.user_id == user_id,
                )
            )
            if membership is None and message.type not in {"member.added", "invitation.created"}:
                return
        rows = (
            await session.execute(
                select(
                    preferences.c.channel, preferences.c.enabled, preferences.c.project_key
                ).where(
                    preferences.c.user_id == user_id,
                    preferences.c.event_kind == kind,
                    preferences.c.project_key.in_([message.project_id or UUID(int=0), UUID(int=0)]),
                )
            )
        ).all()
        flags = {row.channel: row.enabled for row in rows if row.project_key == UUID(int=0)}
        flags.update(
            {row.channel: row.enabled for row in rows if row.project_key == message.project_id}
        )
        if not any(
            (flags.get("inbox", True), flags.get("email", False), flags.get("telegram", False))
        ):
            return
        title, body, target = event_view(kind, message.data, project_id=message.project_id)
        coalesced = False
        coalesce_key = None
        if kind == "incident.created" and message.project_id:
            # Alerts for one project collapse into one inbox row for five minutes.
            coalesce_key = f"{user_id}:{message.project_id}:{kind}"
            lock_key = int.from_bytes(hashlib.sha256(coalesce_key.encode()).digest()[:8], "big") & (
                (1 << 63) - 1
            )
            await session.execute(select(func.pg_advisory_xact_lock(lock_key)))
            now = datetime.now(UTC)
            marker_key = f"incident-storm:{message.project_id}"
            marker = await session.scalar(
                select(idempotency_keys.c.response).where(
                    idempotency_keys.c.user_id == user_id,
                    idempotency_keys.c.key == marker_key,
                )
            )
            if marker is not None and datetime.fromisoformat(
                marker["window_start"]
            ) >= now - timedelta(minutes=5):
                coalesced = True
                marker_count = marker["count"] + 1
                marker_start = marker["window_start"]
            else:
                marker_count = 1
                marker_start = now.isoformat()
            await session.execute(
                pg_insert(idempotency_keys)
                .values(
                    user_id=user_id,
                    key=marker_key,
                    response={"window_start": marker_start, "count": marker_count},
                )
                .on_conflict_do_update(
                    index_elements=[idempotency_keys.c.user_id, idempotency_keys.c.key],
                    set_={"response": {"window_start": marker_start, "count": marker_count}},
                )
            )
            recent = (
                await session.execute(
                    select(inbox_items.c.id, inbox_items.c.coalesce_count)
                    .where(
                        inbox_items.c.user_id == user_id,
                        inbox_items.c.coalesce_key == coalesce_key,
                        inbox_items.c.created_at >= now - timedelta(minutes=5),
                    )
                    .order_by(inbox_items.c.created_at.desc())
                    .limit(1)
                )
            ).one_or_none()
            if recent is not None and coalesced and flags.get("inbox", True):
                await session.execute(
                    update(inbox_items)
                    .where(inbox_items.c.id == recent.id)
                    .values(
                        coalesce_count=recent.coalesce_count + 1,
                        title=f"{recent.coalesce_count + 1} incidents created",
                        body=(
                            f"{recent.coalesce_count + 1} incidents were created in this project"
                        ),
                    )
                )
        if flags.get("inbox", True):
            created = False
            if not coalesced:
                inserted = await session.execute(
                    pg_insert(inbox_items)
                    .values(
                        id=uuid7(),
                        user_id=user_id,
                        project_id=message.project_id,
                        source_event_id=message.message_id,
                        kind=kind,
                        title=title,
                        body=body,
                        target=target,
                        coalesce_key=coalesce_key,
                    )
                    .on_conflict_do_nothing()
                    .returning(inbox_items.c.id)
                )
                created = inserted.scalar_one_or_none() is not None
            if created or coalesced:
                count = await session.scalar(
                    select(func.count())
                    .select_from(inbox_items)
                    .where(inbox_items.c.user_id == user_id, inbox_items.c.read_at.is_(None))
                )
                live = message.caused(
                    "inbox.changed",
                    "notifications",
                    {"type": "inbox.changed", "unread_count": count},
                )
                await self.writer.add(
                    session, live, Destination.live("notifications", f"user.{user_id}")
                )
        if flags.get("email", False) and not coalesced:
            email = message.caused(
                "send.bulk",
                "notifications",
                {
                    "user_id": str(user_id),
                    "kind": kind,
                    "title": title,
                    "body": body,
                    "target": target,
                },
            )
            await self.writer.add(session, email, Destination.email("send.bulk"))
        if flags.get("telegram", False) and not coalesced:
            chat_rows = (
                (await session.execute(select(channels.c.id).where(channels.c.user_id == user_id)))
                .scalars()
                .all()
            )
            for channel_id in chat_rows:
                await session.execute(
                    pg_insert(deliveries)
                    .values(
                        id=uuid7(),
                        channel_id=channel_id,
                        source_event_id=message.message_id,
                        payload={
                            "title": title,
                            "body": body,
                            "project_id": str(message.project_id) if message.project_id else None,
                        },
                    )
                    .on_conflict_do_nothing()
                )

    async def enqueue_webhooks(self, session: AsyncSession, message: MessageV1) -> None:
        rows = (
            await session.execute(
                select(webhooks.c.id, webhooks.c.events).where(
                    webhooks.c.project_id == message.project_id, webhooks.c.active.is_(True)
                )
            )
        ).all()
        for row in rows:
            if message.type in row.events:
                await session.execute(
                    pg_insert(deliveries)
                    .values(
                        id=uuid7(),
                        webhook_id=row.id,
                        source_event_id=message.message_id,
                        payload=message.model_dump(mode="json"),
                    )
                    .on_conflict_do_nothing()
                )

    async def remove_member(self, session: AsyncSession, message: MessageV1) -> None:
        user_id = UUID(message.data["user_id"])
        if message.project_id:
            await session.execute(
                delete(project_members).where(
                    project_members.c.project_id == message.project_id,
                    project_members.c.user_id == user_id,
                )
            )
            await session.execute(
                delete(inbox_items).where(
                    inbox_items.c.user_id == user_id, inbox_items.c.project_id == message.project_id
                )
            )
            await session.execute(
                delete(preferences).where(
                    preferences.c.user_id == user_id, preferences.c.project_id == message.project_id
                )
            )
            await session.execute(
                delete(deliveries).where(
                    deliveries.c.channel_id.in_(
                        select(channels.c.id).where(channels.c.user_id == user_id)
                    ),
                    deliveries.c.payload["project_id"].astext == str(message.project_id),
                    deliveries.c.status != "delivered",
                )
            )

    async def delete_project(self, session: AsyncSession, message: MessageV1) -> None:
        project_id = message.project_id or UUID(message.data["project_id"])
        await session.execute(delete(inbox_items).where(inbox_items.c.project_id == project_id))
        await session.execute(
            delete(deliveries).where(deliveries.c.payload["project_id"].astext == str(project_id))
        )
        await session.execute(
            delete(project_members).where(project_members.c.project_id == project_id)
        )
        await session.execute(delete(preferences).where(preferences.c.project_id == project_id))
        await session.execute(delete(webhooks).where(webhooks.c.project_id == project_id))
        done = message.caused(
            "cleanup.completed",
            "notifications",
            {
                "project_id": str(project_id),
                "lifecycle_version": message.lifecycle_version,
                "participant": "notifications",
            },
        )
        await self.writer.add(session, done, Destination.command("projects", "cleanup.completed"))
