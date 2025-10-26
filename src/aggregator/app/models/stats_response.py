from pydantic import BaseModel
from pydantic.types import NonNegativeInt


class StatsResponseModel(BaseModel):
    received: NonNegativeInt
    unique_processed: NonNegativeInt
    duplicated_dropped: NonNegativeInt
    topics: list[str]
    uptime: NonNegativeInt
