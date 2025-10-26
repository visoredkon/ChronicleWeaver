from datetime import datetime

from pydantic import BaseModel, Field

from .events import EventPayloadModel


class EventRequestModel(BaseModel):
    event_id: str
    topic: str
    source: str
    payload: EventPayloadModel
    timestamp: datetime = Field(default=..., description="ISO 8601 timestamp")


class PublishRequestModel(BaseModel):
    events: list[EventRequestModel]
