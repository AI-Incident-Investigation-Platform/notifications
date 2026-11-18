# Requirements

- NTF-1: Consume incident, investigation, review, project, integration, and export events. Store a unique inbox item per user and source event. Expose paginated inbox, count, read, and delete. Publish `inbox.changed` transactionally.
- NTF-2: Store user notification preferences by project and kind; queue bulk email through the shared outbox. Incident creation events for one user and project coalesce across inbox, email and Telegram within a fixed five-minute window, including when inbox is disabled. End-to-end email delivery depends on upstream EML-1/EML-2.
- NTF-3: Issue a one-time 10 minute Telegram code; accept bot updates with a secret header; retry Telegram delivery through `deliveries`.
- NTF-4: Require `project.manage` for project webhook CRUD, tests and delivery history. Sign the exact JSON body with HMAC SHA-256. Enforce HTTPS, exact host allowlist and public DNS, with retry and dead status.
- Project cleanup: consume `project.deleting`, delete project data and publish `cleanup.completed`.
