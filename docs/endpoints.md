# Endpoints

All user endpoints require a bearer access token for audience `notifications`.

| Method | Path | Result |
|---|---|---|
| GET | `/api/v1/inbox` | `items`, `next_cursor`; filters `unread`, `project_id`, `cursor`, `limit` |
| GET | `/api/v1/inbox/unread-count` | `unread_count` |
| POST | `/api/v1/inbox/read` | Mark `ids` or items before `before` |
| DELETE | `/api/v1/inbox/{id}` | Delete one owned item |
| GET/PUT | `/api/v1/notification-preferences` | Preferences |
| POST | `/api/v1/channels/telegram/link` | One-time code, bot URL, expiry |
| GET | `/api/v1/channels` | Linked channels |
| DELETE | `/api/v1/channels/{id}` | Unlink channel |
| GET/POST | `/api/v1/projects/{id}/webhooks` | Project webhooks; POST reveals secret once |
| PATCH/DELETE | `/api/v1/projects/{id}/webhooks/{id}` | Edit/remove webhook |
| POST | `/api/v1/webhooks/{id}/test` | Queue a test delivery |
| GET | `/api/v1/webhooks/{id}/deliveries` | Up to 100 newest attempts |
| POST | `/internal/v1/telegram/updates` | Bot update, secret header |
