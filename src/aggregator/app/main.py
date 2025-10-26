from contextlib import asynccontextmanager
from os import getenv
from typing import cast

from fastapi import FastAPI, HTTPException

from .models.event_response import EventResponseModel
from .models.events import EventModel
from .models.publish_request import PublishRequestModel
from .models.stats_response import StatsResponseModel
from .services.consumer import ConsumerService
from .services.event_queue import EventQueueService

consumer: ConsumerService = ConsumerService()


@asynccontextmanager
async def lifespan(_app: FastAPI):  # noqa: ANN201
    await consumer.initialize()
    await consumer.start()

    yield

    await consumer.close()


app: FastAPI = FastAPI(
    title="ChronicleWeaver",
    description="Publish-Subscribe Log Aggregator",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get(path="/")
def root() -> dict[str, str]:
    return {"message": "ChronicleWeaver is running..."}


@app.post(path="/publish")
async def publish_events(request: PublishRequestModel) -> dict[str, str | int]:
    try:
        queue: EventQueueService = EventQueueService()

        for event_request in request.events:
            event: EventModel = EventModel(
                event_id=event_request.event_id,
                topic=event_request.topic,
                source=event_request.source,
                payload=event_request.payload,
                timestamp=event_request.timestamp,
            )

            await queue.put(event)

        return {
            "status": "success",
            "message": f"Published {len(request.events)} events",
            "events_count": len(request.events),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to publish events: {str(e)}"
        )


@app.get(path="/events", response_model=EventResponseModel)
async def get_events(topic: str | None = None) -> EventResponseModel:
    try:
        if topic is None:
            events: list[EventModel] = consumer.get_all_events()
        else:
            events = consumer.get_events_by_topic(topic)

        return EventResponseModel(count=len(events), events=events)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve events: {str(e)}"
        )


@app.get(path="/stats", response_model=StatsResponseModel)
async def get_stats() -> StatsResponseModel:
    try:
        stats: dict[str, object] = consumer.get_stats()

        return StatsResponseModel(
            received=cast(int, stats["received"]),
            unique_processed=cast(int, stats["unique_processed"]),
            duplicated_dropped=cast(int, stats["duplicated_dropped"]),
            topics=cast(list[str], stats["topics"]),
            uptime=cast(int, stats["uptime"]),
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve stats: {str(e)}"
        )


@app.get(path="/health")
async def get_health() -> dict[str, str]:
    return {"message": "healthy"}


if __name__ == "__main__":
    import uvicorn

    APP_PORT: int = int(getenv(key="APP_PORT", default="8000"))

    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
