from datetime import datetime

from pydantic import BaseModel, Field


class EventPayloadModel(BaseModel):
    message: str
    timestamp: datetime = Field(default=..., description="ISO 8601 timestamp")


class EventModel(BaseModel):
    event_id: str
    topic: str
    source: str
    payload: EventPayloadModel
    timestamp: datetime = Field(default=..., description="ISO 8601 timestamp")
