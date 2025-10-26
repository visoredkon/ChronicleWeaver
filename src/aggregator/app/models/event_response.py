from pydantic import BaseModel

from .events import EventModel


class EventResponseModel(BaseModel):
    count: int
    events: list[EventModel]
