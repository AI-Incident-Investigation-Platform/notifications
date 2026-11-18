# ruff: noqa: B008 -- FastAPI dependency declarations use callable defaults.
from collections.abc import Awaitable, Callable
from uuid import UUID

from aiip_kit.access.model import Principal
from aiip_kit.errors import (
    DependencyUnavailableError,
    NotAuthenticatedError,
    PermissionDeniedError,
    ProjectNotFoundError,
)
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request, Response

from src.api.schemas import PreferencesRequest, ReadRequest, WebhookCreate, WebhookPatch
from src.api.service import NotificationService

Auth = Callable[[str | None], Awaitable[Principal]]


def create_app(service: NotificationService, authenticate: Auth, telegram_secret: str) -> FastAPI:
    app = FastAPI(title="AIIP Notifications", version="0.1.0")
    api = APIRouter(prefix="/api/v1")

    async def principal(authorization: str | None = Header(default=None)) -> Principal:
        try:
            return await authenticate(authorization)
        except NotAuthenticatedError as exc:
            raise HTTPException(401, "Authentication required") from exc

    @app.exception_handler(ProjectNotFoundError)
    async def missing(_: Request, exc: ProjectNotFoundError):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Project not found"}, status_code=404)

    @app.exception_handler(NotAuthenticatedError)
    async def unauthenticated(_: Request, exc: NotAuthenticatedError):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Authentication required"}, status_code=401)

    @app.exception_handler(PermissionDeniedError)
    async def forbidden(_: Request, exc: PermissionDeniedError):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Permission denied"}, status_code=403)

    @app.exception_handler(DependencyUnavailableError)
    async def unavailable(_: Request, exc: DependencyUnavailableError):
        from fastapi.responses import JSONResponse

        return JSONResponse({"detail": "Dependency unavailable"}, status_code=503)

    @api.get("/inbox")
    async def list_inbox(
        unread: bool | None = None,
        project_id: UUID | None = None,
        cursor: UUID | None = None,
        limit: int = Query(50, ge=1, le=100),
        user: Principal = Depends(principal),
    ):
        return await service.list_inbox(user.user_id, unread, project_id, cursor, limit)

    @api.get("/inbox/unread-count")
    async def count(user: Principal = Depends(principal)):
        return {"unread_count": await service.unread_count(user.user_id)}

    @api.post("/inbox/read")
    async def read(body: ReadRequest, user: Principal = Depends(principal)):
        return await service.read(user.user_id, body)

    @api.delete("/inbox/{item_id}", status_code=204)
    async def delete_item(item_id: UUID, user: Principal = Depends(principal)):
        if not await service.delete_item(user.user_id, item_id):
            raise HTTPException(404, "Notification not found")
        return Response(status_code=204)

    @api.get("/notification-preferences")
    async def get_preferences(user: Principal = Depends(principal)):
        return await service.get_preferences(user.user_id)

    @api.put("/notification-preferences")
    async def put_preferences(body: PreferencesRequest, user: Principal = Depends(principal)):
        return await service.put_preferences(user, body)

    @api.post("/channels/telegram/link")
    async def link(user: Principal = Depends(principal)):
        return await service.link_telegram(user.user_id)

    @api.get("/channels")
    async def list_channels(user: Principal = Depends(principal)):
        return {"items": await service.list_channels(user.user_id)}

    @api.delete("/channels/{channel_id}", status_code=204)
    async def delete_channel(channel_id: UUID, user: Principal = Depends(principal)):
        if not await service.delete_channel(user.user_id, channel_id):
            raise HTTPException(404, "Channel not found")
        return Response(status_code=204)

    @api.get("/projects/{project_id}/webhooks")
    async def list_webhooks(project_id: UUID, user: Principal = Depends(principal)):
        return {"items": await service.list_webhooks(user, project_id)}

    @api.post("/projects/{project_id}/webhooks", status_code=201)
    async def create_webhook(
        project_id: UUID, body: WebhookCreate, user: Principal = Depends(principal)
    ):
        try:
            return await service.create_webhook(user, project_id, body)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @api.patch("/projects/{project_id}/webhooks/{hook_id}")
    async def update_webhook(
        project_id: UUID, hook_id: UUID, body: WebhookPatch, user: Principal = Depends(principal)
    ):
        try:
            result = await service.update_webhook(user, project_id, hook_id, body)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if result is None:
            raise HTTPException(404, "Webhook not found")
        return result

    @api.delete("/projects/{project_id}/webhooks/{hook_id}", status_code=204)
    async def delete_webhook(project_id: UUID, hook_id: UUID, user: Principal = Depends(principal)):
        if not await service.delete_webhook(user, project_id, hook_id):
            raise HTTPException(404, "Webhook not found")
        return Response(status_code=204)

    @api.post("/webhooks/{hook_id}/test", status_code=202)
    async def test_webhook(hook_id: UUID, user: Principal = Depends(principal)):
        if not await service.test_webhook(user, hook_id):
            raise HTTPException(404, "Webhook not found")
        return {"queued": True}

    @api.get("/webhooks/{hook_id}/deliveries")
    async def deliveries(hook_id: UUID, user: Principal = Depends(principal)):
        rows = await service.list_deliveries(user, hook_id)
        if rows is None:
            raise HTTPException(404, "Webhook not found")
        return {"items": rows}

    @app.post("/internal/v1/telegram/updates")
    async def telegram_updates(
        request: Request, x_telegram_bot_api_secret_token: str | None = Header(default=None)
    ):
        if not telegram_secret or x_telegram_bot_api_secret_token != telegram_secret:
            raise HTTPException(401, "Invalid Telegram token")
        body = await request.json()
        message = body.get("message") or {}
        chat = message.get("chat") or {}
        text = message.get("text") or ""
        if not isinstance(text, str) or not text.startswith("/start "):
            return {"ok": True}
        return {
            "ok": await service.complete_telegram_link(
                text[7:].strip(),
                str(chat.get("id", "")),
                str(chat.get("title") or chat.get("first_name") or "Telegram"),
            )
        }

    app.include_router(api)
    return app
