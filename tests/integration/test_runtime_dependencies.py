import asyncio
import importlib
import time
from uuid import uuid7

import aio_pika
import pytest
from aiip_kit.access.checker import AclCacheWriter
from aiip_kit.access.model import AclSnapshot, Principal
from aiip_kit.messaging.amqp import to_amqp
from aiip_kit.messaging.message import MessageV1
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import create_async_engine
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer

from src.core.db.models import inbox_items, metadata, outbox, project_members


async def wait_until(predicate, timeout=10):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if await predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("Expected runtime event was not processed")


@pytest.mark.asyncio
async def test_runtime_consumes_events_relays_live_and_checks_dragonfly(monkeypatch):
    with (
        PostgresContainer("postgres:17-alpine", dbname="notifications_test") as pg,
        DockerContainer("rabbitmq:4-management-alpine")
        .with_env("RABBITMQ_DEFAULT_USER", "notifications_test")
        .with_env("RABBITMQ_DEFAULT_PASS", "notifications_test")
        .with_exposed_ports(5672) as rabbit,
        DockerContainer("docker.dragonflydb.io/dragonflydb/dragonfly:v2.0.2")
        .with_command(["dragonfly", "--logtostderr", "--proactor_threads=2", "--maxmemory=768mb"])
        .with_exposed_ports(6379) as dragonfly,
    ):
        database_url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+asyncpg://"
        )
        rabbit_url = (
            f"amqp://notifications_test:notifications_test@{rabbit.get_container_host_ip()}:"
            f"{rabbit.get_exposed_port(5672)}/"
        )
        cache_url = (
            f"redis://{dragonfly.get_container_host_ip()}:{dragonfly.get_exposed_port(6379)}/0"
        )
        monkeypatch.setenv("DATABASE_URL", database_url)
        monkeypatch.setenv("RABBITMQ_URL", rabbit_url)
        monkeypatch.setenv("CACHE_URL", cache_url)
        monkeypatch.setenv("AUTH_CLIENT_SECRET", "test-secret")
        monkeypatch.setenv("TELEGRAM_WEBHOOK_SECRET", "test-secret")
        engine = create_async_engine(database_url)
        async with engine.begin() as pg_connection:
            await pg_connection.run_sync(metadata.create_all)
        await engine.dispose()

        runtime = importlib.import_module("src.runtime")
        user_id, session_id, project_id = (uuid7() for _ in range(3))
        redis = Redis.from_url(cache_url)
        try:

            async def dragonfly_ready():
                try:
                    return bool(await redis.ping())
                except RedisError:
                    return False

            await wait_until(dragonfly_ready, timeout=20)
            await redis.set(f"auth:session:{session_id}", "active", ex=60)
            now = time.time_ns() // 1_000_000
            snapshot = AclSnapshot(
                project_id=project_id,
                user_id=user_id,
                project_state="active",
                lifecycle_version=1,
                acl_version=1,
                member_role_key="owner",
                member_version=1,
                role_key="owner",
                role_version=1,
                permissions=frozenset({"project.manage"}),
                observed_at_ms=now,
                fresh_until_ms=now + 5000,
            )
            assert await AclCacheWriter(redis).store(snapshot) == 3
            principal = Principal(user_id=user_id, session_id=session_id)
            assert (await runtime.access.require(principal, project_id, "project.manage")).has(
                "project.manage"
            )

            async with asyncio.timeout(20):
                rabbit_connection = await aio_pika.connect_robust(
                    rabbit_url, fail_fast=False, reconnect_interval=1
                )
            channel = await rabbit_connection.channel()
            live = await channel.declare_exchange(
                "notifications.live", aio_pika.ExchangeType.TOPIC, durable=True
            )
            live_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
            await live_queue.bind(live, routing_key=f"user.{user_id}")
            try:
                async with runtime.lifespan(runtime.app):
                    projects = await channel.get_exchange("projects.events")
                    incidents = await channel.get_exchange("incidents.events")
                    member = MessageV1.event(
                        "member.added", "projects", {"user_id": str(user_id)}, project_id=project_id
                    )
                    await projects.publish(to_amqp(member), routing_key=member.type)

                    async def member_recorded():
                        async with runtime.sessions() as session:
                            return (
                                await session.scalar(
                                    select(project_members.c.user_id).where(
                                        project_members.c.project_id == project_id,
                                        project_members.c.user_id == user_id,
                                    )
                                )
                                is not None
                            )

                    await wait_until(member_recorded)
                    incident = MessageV1.event(
                        "incident.created",
                        "incidents",
                        {"incident": {"id": str(uuid7()), "title": "Live incident"}},
                        project_id=project_id,
                    )
                    await incidents.publish(to_amqp(incident), routing_key=incident.type)

                    async def incident_recorded():
                        async with runtime.sessions() as session:
                            return (
                                await session.scalar(
                                    select(func.count())
                                    .select_from(inbox_items)
                                    .where(
                                        inbox_items.c.user_id == user_id,
                                        inbox_items.c.kind == "incident.created",
                                    )
                                )
                                == 1
                            )

                    await wait_until(incident_recorded)
                    received = None

                    async def live_received():
                        nonlocal received
                        received = await live_queue.get(fail=False, timeout=1)
                        return received is not None

                    await wait_until(live_received)
                    assert received is not None
                    assert MessageV1.model_validate_json(received.body).type == "inbox.changed"
                    await received.ack()
                    async with runtime.sessions() as session:
                        assert (
                            await session.scalar(
                                select(func.count())
                                .select_from(outbox)
                                .where(outbox.c.delivered_at.is_not(None))
                            )
                            >= 1
                        )
            finally:
                await channel.close()
                await rabbit_connection.close()
        finally:
            await redis.aclose()
