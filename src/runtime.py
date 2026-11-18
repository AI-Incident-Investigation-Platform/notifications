import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
import httpx
import uvicorn
from aiip_kit.access.checker import DragonflyAccessChecker
from aiip_kit.access.source import HttpAclSource
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec
from aiip_kit.messaging.publisher import AmqpPublisher
from aiip_kit.outbox.relay import OutboxRelay
from aiip_kit.service_auth.jwks import JwksKeys
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from fastapi import FastAPI
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.service import NotificationService
from src.core.db.models import outbox
from src.core.user_auth import JwtUserAuthenticator
from src.main import create_app
from src.worker.delivery import DeliveryWorker
from src.worker.events import EventHandler


class Config(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)
    cache_url: str = Field(min_length=1)
    auth_client_secret: str = Field(min_length=1)
    telegram_webhook_secret: str = Field(min_length=1)
    telegram_bot_token: str = ""
    telegram_bot_url: str = "https://t.me/aiip_bot"
    webhook_allowed_hosts: str = ""
    auth_url: str = "http://auth.apps:8001"
    projects_url: str = "http://projects.apps:8003"
    port: int = 8010


cfg = Config()  # type: ignore[call-arg]
engine = create_async_engine(cfg.database_url, pool_size=3, max_overflow=0, pool_timeout=0.5)
sessions = async_sessionmaker(engine, expire_on_commit=False)
http = httpx.AsyncClient(timeout=5.0, trust_env=False)
redis = Redis.from_url(cfg.cache_url, decode_responses=True)
keys = JwksKeys(http, f"{cfg.auth_url.rstrip('/')}/.well-known/jwks.json")
authenticator = JwtUserAuthenticator(keys)
tokens = AuthServiceTokenProvider(http, cfg.auth_url, "notifications", cfg.auth_client_secret)
source = HttpAclSource(
    http, cfg.projects_url, lambda: tokens.token("projects", ["projects.acl.read"])
)
access = DragonflyAccessChecker(redis, source)
publisher = AmqpPublisher(cfg.rabbitmq_url)
relay = OutboxRelay(sessions, outbox, publisher)
allowed_hosts = {
    value.strip().lower() for value in cfg.webhook_allowed_hosts.split(",") if value.strip()
}
service = NotificationService(sessions, access, allowed_hosts, cfg.telegram_bot_url)
delivery = DeliveryWorker(sessions, http, allowed_hosts, cfg.telegram_bot_token)
stop = asyncio.Event()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    connection = await aio_pika.connect_robust(cfg.rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True)
    handler = EventHandler(sessions)
    specs = [
        QueueSpec(
            "notifications.incidents-events",
            [
                Binding("incidents.events", "topic", key)
                for key in (
                    "incident.created",
                    "incident.resolved",
                    "investigation.finished",
                    "review.created",
                )
            ],
        ),
        QueueSpec(
            "notifications.projects-events",
            [
                Binding("projects.events", "topic", key)
                for key in ("member.*", "invitation.created", "project.deleting")
            ],
        ),
        QueueSpec(
            "notifications.integrations-events",
            [Binding("integrations.events", "topic", "integration.health_changed")],
        ),
        QueueSpec("notifications.content-events", [Binding("content.events", "topic", "export.*")]),
    ]
    for spec in specs:
        await Consumer(channel, spec, handler).start()
    relay_task = asyncio.create_task(relay.run(stop))
    delivery_task = asyncio.create_task(delivery.run(stop))
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(relay_task, delivery_task)
        await channel.close()
        await connection.close()
        await publisher.close()
        await http.aclose()
        await redis.aclose()
        await engine.dispose()


app = create_app(service, authenticator.authenticate, cfg.telegram_webhook_secret)
app.router.lifespan_context = lifespan


@app.get("/ready")
async def ready() -> dict[str, str]:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    await redis.ping()
    return {"status": "ok"}


def main() -> None:
    uvicorn.run("src.runtime:app", host="0.0.0.0", port=cfg.port, workers=1)
