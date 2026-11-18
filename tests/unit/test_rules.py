import hashlib
import hmac
import socket
from datetime import timedelta
from uuid import uuid7

import pytest
from aiip_kit.messaging.message import MessageV1

from src.core.rules import (
    delivery_backoff,
    event_view,
    new_link_code,
    resolve_webhook_target,
    sign_webhook,
    validate_webhook_url,
)
from src.worker.events import EventHandler


class DNS:
    @staticmethod
    def public(host, port, type):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", port))]

    @staticmethod
    def private(host, port, type):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]


def test_webhook_rejects_private_resolution(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", DNS.private)
    with pytest.raises(ValueError, match="non-public"):
        validate_webhook_url("https://hooks.example.com/event", {"hooks.example.com"})


def test_webhook_rejects_mixed_public_and_private_resolution(monkeypatch):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, type: DNS.public(host, port, type) + DNS.private(host, port, type),
    )
    with pytest.raises(ValueError, match="non-public"):
        resolve_webhook_target("https://hooks.example.com/event", {"hooks.example.com"})


def test_webhook_pins_public_address_and_preserves_request_path(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", DNS.public)
    target = resolve_webhook_target(
        "https://hooks.example.com:8443/event?key=value", {"hooks.example.com"}
    )
    assert target.url == "https://93.184.215.14:8443/event?key=value"
    assert target.host == "hooks.example.com"


def test_webhook_rejects_unsafe_url_and_unlisted_host(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", DNS.public)
    for url in (
        "http://hooks.example.com/x",
        "https://user@hooks.example.com/x",
        "https://other.example.com/x",
        "https://hooks.example.com/x#fragment",
    ):
        with pytest.raises(ValueError):
            validate_webhook_url(url, {"hooks.example.com"})
    assert (
        validate_webhook_url("https://hooks.example.com/x", {"hooks.example.com"})
        == "https://hooks.example.com/x"
    )


def test_signature_matches_body_hmac():
    secret = b"key"
    body = b'{"type":"incident.created"}'
    assert (
        sign_webhook(secret, body) == "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    )


def test_link_codes_are_distinct_and_hashed():
    a, digest, _ = new_link_code()
    b, _, _ = new_link_code()
    assert a != b
    assert digest == hashlib.sha256(a.encode()).hexdigest()


def test_delivery_backoff_is_bounded():
    assert delivery_backoff(1) == timedelta(seconds=5)
    assert delivery_backoff(99) == timedelta(seconds=300)


def test_recipient_list_deduplicates():
    user = uuid7()
    message = MessageV1.event(
        "incident.created", "incidents", {"participants": [str(user), str(user)]}
    )
    assert EventHandler.recipients(message) == [user]


def test_event_targets_use_nested_review_and_envelope_project_id():
    incident_id, investigation_id, project_id = uuid7(), uuid7(), uuid7()
    _, _, review_target = event_view(
        "review.created", {"review": {"incident_id": str(incident_id)}}
    )
    _, _, integration_target = event_view(
        "integration.degraded", {"integration_id": str(uuid7())}, project_id=project_id
    )
    _, _, investigation_target = event_view(
        "investigation.finished",
        {"incident_id": str(incident_id), "investigation_id": str(investigation_id)},
    )
    assert review_target == {"type": "incident", "id": str(incident_id)}
    assert integration_target == {"type": "project", "id": str(project_id)}
    assert investigation_target == {"type": "investigation", "id": str(investigation_id)}
