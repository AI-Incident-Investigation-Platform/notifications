import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

import httpx
from sqlalchemy import delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.models import channels, deliveries, webhooks
from src.core.rules import delivery_backoff, resolve_webhook_target, sign_webhook, webhook_body


class DeliveryWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        http: httpx.AsyncClient,
        allowed_hosts: set[str],
        telegram_token: str,
    ):
        self.sessions = sessions
        self.http = http
        self.allowed_hosts = allowed_hosts
        self.telegram_token = telegram_token

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.run_once()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), 1)

    async def claim(self) -> list[tuple[dict, UUID]]:
        now = datetime.now(UTC)
        async with self.sessions.begin() as session:
            rows = (
                (
                    await session.execute(
                        select(deliveries)
                        .where(
                            deliveries.c.status.in_(["pending", "failed"]),
                            deliveries.c.next_attempt_at <= now,
                            or_(deliveries.c.lease_until.is_(None), deliveries.c.lease_until < now),
                        )
                        .order_by(deliveries.c.next_attempt_at)
                        .limit(20)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .all()
            )
            claimed: list[tuple[dict, UUID]] = []
            for row in rows:
                token = uuid7()
                await session.execute(
                    update(deliveries)
                    .where(deliveries.c.id == row["id"])
                    .values(lease_token=token, lease_until=now + timedelta(seconds=30))
                )
                claimed.append((dict(row), token))
            return claimed

    async def run_once(self) -> int:
        claimed = await self.claim()
        for row, token in claimed:
            status_code = await self.send(row)
            await self.finish(row, token, status_code)
        async with self.sessions.begin() as session:
            await session.execute(
                delete(deliveries).where(
                    deliveries.c.created_at < datetime.now(UTC) - timedelta(days=14)
                )
            )
        return len(claimed)

    async def send(self, row: dict) -> int | None:
        try:
            async with self.sessions() as session:
                if row["webhook_id"]:
                    webhook = (
                        (
                            await session.execute(
                                select(webhooks).where(webhooks.c.id == row["webhook_id"])
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if webhook is None or not webhook["active"]:
                        return 410
                else:
                    channel = (
                        (
                            await session.execute(
                                select(channels).where(channels.c.id == row["channel_id"])
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if channel is None or not self.telegram_token:
                        return 410
                    url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
                    chat_id = channel["chat_id"]
            if row["webhook_id"]:
                assert webhook is not None
                target = await asyncio.to_thread(
                    resolve_webhook_target, webhook["url"], self.allowed_hosts
                )
                body = webhook_body(row["payload"])
                headers = {
                    "Content-Type": "application/json",
                    "Host": target.host,
                    "X-AIIP-Signature": sign_webhook(webhook["secret"], body),
                    "X-AIIP-Timestamp": str(int(datetime.now(UTC).timestamp())),
                }
                request = self.http.build_request(
                    "POST",
                    target.url,
                    content=body,
                    headers=headers,
                    timeout=5.0,
                    extensions={"sni_hostname": target.host},
                )
                response = await self.http.send(request, follow_redirects=False)
            else:
                assert channel is not None
                response = await self.http.post(
                    url,
                    json={
                        "chat_id": chat_id,
                        "text": f"{row['payload']['title']}\n{row['payload']['body']}",
                    },
                    timeout=5.0,
                    follow_redirects=False,
                )
            return response.status_code
        except httpx.HTTPError, ValueError, KeyError:
            return None

    async def finish(self, row: dict, token: UUID, status_code: int | None) -> None:
        attempts = row["attempts"] + 1
        ok = status_code is not None and 200 <= status_code < 300
        status = "delivered" if ok else "dead" if status_code == 410 or attempts >= 10 else "failed"
        async with self.sessions.begin() as session:
            await session.execute(
                update(deliveries)
                .where(deliveries.c.id == row["id"], deliveries.c.lease_token == token)
                .values(
                    status=status,
                    attempts=attempts,
                    last_status_code=status_code,
                    next_attempt_at=(
                        None
                        if status in {"delivered", "dead"}
                        else datetime.now(UTC) + delivery_backoff(attempts)
                    ),
                    lease_token=None,
                    lease_until=None,
                )
            )
