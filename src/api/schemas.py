from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from src.core.rules import ALLOWED_KINDS, WEBHOOK_EVENTS


class ReadRequest(BaseModel):
    ids: list[UUID] | None = Field(default=None, max_length=200)
    before: datetime | None = None

    @model_validator(mode="after")
    def exactly_one(self):
        if (self.ids is None) == (self.before is None):
            raise ValueError("Supply exactly one of ids or before")
        return self


class PreferenceItem(BaseModel):
    project_id: UUID | None = None
    kind: str
    channels: list[Literal["inbox", "email", "telegram"]]

    @model_validator(mode="after")
    def check(self):
        if self.kind not in ALLOWED_KINDS or len(self.channels) != len(set(self.channels)):
            raise ValueError("Invalid preference")
        return self


class PreferencesRequest(BaseModel):
    items: list[PreferenceItem] = Field(max_length=500)


class WebhookCreate(BaseModel):
    url: str = Field(max_length=2048)
    events: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def check(self):
        if not set(self.events) <= WEBHOOK_EVENTS or len(self.events) != len(set(self.events)):
            raise ValueError("Invalid webhook events")
        return self


class WebhookPatch(BaseModel):
    url: str | None = Field(default=None, max_length=2048)
    events: list[str] | None = None
    active: bool | None = None

    @model_validator(mode="after")
    def check(self):
        if self.events is not None and (not self.events or not set(self.events) <= WEBHOOK_EVENTS):
            raise ValueError("Invalid webhook events")
        return self
