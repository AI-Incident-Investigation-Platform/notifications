import hashlib
import hmac
import ipaddress
import json
import secrets
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

ALLOWED_KINDS = frozenset(
    {
        "incident.created",
        "investigation.finished",
        "review.created",
        "member.added",
        "integration.degraded",
        "export.ready",
        "invitation.received",
    }
)
WEBHOOK_EVENTS = frozenset(
    {"incident.created", "incident.resolved", "investigation.finished", "review.created"}
)
CHANNELS = frozenset({"inbox", "email", "telegram"})


@dataclass(frozen=True)
class WebhookTarget:
    url: str
    host: str


def resolve_webhook_target(url: str, allowed_hosts: set[str]) -> WebhookTarget:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or len(url) > 2048
    ):
        raise ValueError("Webhook URL must be an HTTPS URL without credentials or fragment")
    host = parsed.hostname.rstrip(".").lower()
    if host not in allowed_hosts:
        raise ValueError("Webhook host is not allowlisted")
    try:
        addresses = socket.getaddrinfo(host, parsed.port or 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ValueError("Webhook host cannot be resolved") from exc
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError("Webhook host resolves to a non-public address")
    # The HTTP client connects to this address, so a second DNS lookup cannot
    # replace the validated public address with an internal one.
    address = str(addresses[0][4][0])
    port = parsed.port or 443
    literal = f"[{address}]" if ":" in address else address
    path = parsed.path or "/"
    if parsed.query:
        path += f"?{parsed.query}"
    return WebhookTarget(f"https://{literal}:{port}{path}", host)


def validate_webhook_url(url: str, allowed_hosts: set[str]) -> str:
    resolve_webhook_target(url, allowed_hosts)
    return url


def sign_webhook(secret: bytes, body: bytes) -> str:
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def webhook_body(message: dict) -> bytes:
    return json.dumps(message, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def new_link_code() -> tuple[str, str, datetime]:
    code = secrets.token_urlsafe(24)
    return (
        code,
        hashlib.sha256(code.encode()).hexdigest(),
        datetime.now(UTC) + timedelta(minutes=10),
    )


def delivery_backoff(attempts: int) -> timedelta:
    return timedelta(seconds=min(300, 5 * 2 ** max(0, attempts - 1)))


def event_view(kind: str, data: dict, *, project_id=None) -> tuple[str, str, dict]:
    nested_key = (
        "incident" if kind == "incident.created" else "review" if kind == "review.created" else None
    )
    nested = data.get(nested_key) if nested_key else None
    source = nested if isinstance(nested, dict) else data
    if kind == "incident.created":
        target_type, target_id = "incident", source.get("id") or source.get("incident_id")
    elif kind == "investigation.finished":
        target_type, target_id = "investigation", data.get("investigation_id")
    elif kind == "review.created":
        target_type, target_id = "incident", source.get("incident_id")
    elif kind == "invitation.received":
        target_type, target_id = "invitation", data.get("invitation_id")
    elif kind == "export.ready":
        target_type, target_id = "export", data.get("export_id")
    else:
        target_type, target_id = "project", project_id or data.get("project_id")
    title = str(source.get("title") or kind.replace(".", " ").title())[:300]
    body = str(source.get("summary") or source.get("description") or title)[:4000]
    return title, body, {"type": target_type, "id": str(target_id)}
