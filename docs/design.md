# Design

`src/worker/events.py` uses `InboxGuard` and `OutboxWriter` in one PostgreSQL transaction. Inbox rows have a unique `(user_id, source_event_id)` constraint. `project_members` is a local projection of `member.added` and `member.removed`; missing membership fails closed. The consumer declares durable quorum queues with bounded size, retry queues, and dead queues through `aiip-kit`.

For incident storms, the transaction takes a per-user/project advisory lock and maintains a five-minute marker in `idempotency_keys`. This suppresses later email and Telegram deliveries even when inbox is disabled, and updates the existing inbox row's count when enabled.

`src/worker/delivery.py` polls due deliveries under `SKIP LOCKED`. Responses outside 2xx and transport errors receive exponential retry, capped at 300 seconds and 10 attempts. Delivery rows are removed after 14 days. Webhook requests carry `X-AIIP-Signature: sha256=<HMAC>` and `X-AIIP-Timestamp`.

The claim transaction ends before DNS resolution and outbound HTTP. Webhook DNS resolution rejects any non-public address. The request connects to a selected validated IP with the configured hostname in Host and TLS SNI; redirects and environment proxies are disabled. The completion transaction uses the claim token so a timed-out worker cannot overwrite a newer attempt.

The migration creates all service tables and indexes. A destructive downgrade is deliberately unavailable.
