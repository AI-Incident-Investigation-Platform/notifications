# AIIP notifications

Notifications owns the user's inbox, delivery preferences, Telegram channel links, project webhooks and delivery history. It consumes domain events through RabbitMQ and publishes `inbox.changed` to `notifications.live` using the `aiip-kit` transactional outbox. Email is queued to the `email` exchange with `send.bulk`.

## Local development

Python 3.14 and `uv` are required. Copy `.env.example` into your local environment, supply real secrets there, then run `make upgrade`, `make run`, `make check`, and `make test`. The database migration is additive and has no destructive downgrade.

The service runs as one process per pod and uses three PostgreSQL connections per pod. Public routes are at `/api/v1`; Telegram updates are accepted at `/internal/v1/telegram/updates` with the Telegram secret header. `/ready` checks PostgreSQL and Dragonfly.

## Current integration limits

- Existing project members are known only after `member.added` events arrive. A source snapshot or internal projects members API is needed to seed members for projects that predate the service. Without it, events for those projects are not fanned out.
- Email notifications are queued as `send.bulk` through the outbox. The upstream `email` service does not yet consume `email.bulk`; its EML-1 templates and EML-2 queue are external prerequisites for end-to-end email delivery.
- Webhook delivery resolves every address and rejects the URL if any is non-public. It connects to the validated IP with the original host as HTTP Host and TLS SNI, with redirects and environment proxies disabled. DNS resolution and outbound HTTP happen outside database transactions.
- PostgreSQL integration tests cover inbox and email coalescing, private export routing, invitations, Telegram linking and delivery, webhook signing, address pinning, retry recovery, terminal failure, and revoked-session responses. An isolated live test covers RabbitMQ consumption, outbox relay to `notifications.live`, and Dragonfly ACL checks. The image builds locally; k3d deployment has not been run.
