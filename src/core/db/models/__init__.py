from aiip_kit.outbox.tables import inbox_table, outbox_table
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB

metadata = MetaData()
inbox = inbox_table(metadata)
outbox = outbox_table(metadata)
project_members = Table(
    "project_members",
    metadata,
    Column("project_id", Uuid, nullable=False),
    Column("user_id", Uuid, nullable=False),
    PrimaryKeyConstraint("project_id", "user_id"),
)
preferences = Table(
    "preferences",
    metadata,
    Column("user_id", Uuid, nullable=False),
    Column("project_id", Uuid),
    Column("event_kind", String(64), nullable=False),
    Column("channel", String(16), nullable=False),
    Column("enabled", Boolean, nullable=False, server_default=text("true")),
    Column(
        "project_key",
        Uuid,
        nullable=False,
        server_default=text("'00000000-0000-0000-0000-000000000000'::uuid"),
    ),
    PrimaryKeyConstraint("user_id", "project_key", "event_kind", "channel"),
    CheckConstraint("channel IN ('inbox','email','telegram')", name="ck_preference_channel"),
)
inbox_items = Table(
    "inbox_items",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("user_id", Uuid, nullable=False),
    Column("project_id", Uuid),
    Column("source_event_id", Uuid, nullable=False),
    Column("kind", String(64), nullable=False),
    Column("title", String(300), nullable=False),
    Column("body", String(4000), nullable=False),
    Column("target", JSONB, nullable=False),
    Column("read_at", DateTime(timezone=True)),
    Column("coalesce_key", String(160)),
    Column("coalesce_count", Integer, nullable=False, server_default=text("1")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("user_id", "source_event_id", name="uq_inbox_event"),
    Index("ix_inbox_user_time", "user_id", "created_at", "id"),
    Index("ix_inbox_unread", "user_id", "created_at", postgresql_where=text("read_at IS NULL")),
)
channels = Table(
    "channels",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("user_id", Uuid, nullable=False),
    Column("type", String(16), nullable=False),
    Column("title", String(256), nullable=False),
    Column("chat_id", String(128), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    UniqueConstraint("user_id", "type", "chat_id", name="uq_channel_chat"),
)
telegram_links = Table(
    "telegram_links",
    metadata,
    Column("code_hash", String(64), primary_key=True),
    Column("user_id", Uuid, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("used_at", DateTime(timezone=True)),
)
webhooks = Table(
    "webhooks",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("project_id", Uuid, nullable=False),
    Column("url", String(2048), nullable=False),
    Column("secret", LargeBinary, nullable=False),
    Column("events", JSONB, nullable=False),
    Column("active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Index("ix_webhook_project", "project_id"),
)
deliveries = Table(
    "deliveries",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("channel_id", Uuid, ForeignKey("channels.id", ondelete="CASCADE")),
    Column("webhook_id", Uuid, ForeignKey("webhooks.id", ondelete="CASCADE")),
    Column("source_event_id", Uuid, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("status", String(16), nullable=False, server_default=text("'pending'")),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_status_code", Integer),
    Column("next_attempt_at", DateTime(timezone=True), server_default=func.now()),
    Column("lease_token", Uuid),
    Column("lease_until", DateTime(timezone=True)),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("(channel_id IS NULL) <> (webhook_id IS NULL)", name="ck_delivery_one_target"),
    Index(
        "uq_delivery_channel_event",
        "channel_id",
        "source_event_id",
        unique=True,
        postgresql_where=text("channel_id IS NOT NULL"),
    ),
    Index(
        "uq_delivery_webhook_event",
        "webhook_id",
        "source_event_id",
        unique=True,
        postgresql_where=text("webhook_id IS NOT NULL"),
    ),
    Index(
        "ix_delivery_due",
        "next_attempt_at",
        postgresql_where=text("status IN ('pending','failed')"),
    ),
)
audit_events = Table(
    "audit_events",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("actor_id", Uuid),
    Column("action", String(80), nullable=False),
    Column("target_id", Uuid),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
idempotency_keys = Table(
    "idempotency_keys",
    metadata,
    Column("user_id", Uuid, nullable=False),
    Column("key", String(128), nullable=False),
    Column("response", JSONB, nullable=False),
    PrimaryKeyConstraint("user_id", "key"),
)
