from asyncio import CancelledError, Task, create_task
from datetime import datetime, timedelta
from typing import cast

from loguru import logger

from ..models.events import EventModel
from .deduplication_store import DeduplicationStoreService
from .event_queue import EventQueueService


class ConsumerService:
    def __init__(self) -> None:
        self.__event_queue: EventQueueService = EventQueueService()
        self.__deduplication_store: DeduplicationStoreService = (
            DeduplicationStoreService()
        )
        self.__start_time: datetime = datetime.now()
        self.__running: bool = False
        self.__task: "Task[None] | None" = None

    async def initialize(self) -> None:
        await self.__deduplication_store.initialize()

    async def start(self) -> None:
        if self.__running:
            return

        self.__running = True
        self.__task = create_task(coro=self.__consume_loop())

    async def stop(self) -> None:
        self.__running = False
        if self.__task:
            _ = self.__task.cancel()

            try:
                await self.__task
            except CancelledError:
                pass

    async def __consume_loop(self) -> None:
        while self.__running:
            try:
                event: EventModel = await self.__event_queue.get()

                await self.__deduplication_store.update_received()
                await self.__deduplication_store.add_topic(event.topic)

                if await self.__deduplication_store.is_processed(
                    event.event_id, event.topic
                ):
                    await self.__deduplication_store.update_duplicated_dropped()
                    logger.warning(
                        f"Duplicate event detected and dropped: event_id={event.event_id}, topic={event.topic}"
                    )
                else:
                    await self.__deduplication_store.mark_processed(event)

                    logger.info(
                        f"Processed unique event: event_id={event.event_id}, topic={event.topic}"
                    )
            except Exception as e:
                logger.error(f"Error processing event: {e}")

    def get_events_by_topic(self, topic: str) -> list["EventModel"]:
        return self.__deduplication_store.get_events_by_topic(topic)

    def get_all_events(self) -> list["EventModel"]:
        return self.__deduplication_store.get_all_events()

    def get_stats(self) -> dict[str, object]:
        stats: dict[str, object] = self.__deduplication_store.get_stats()
        uptime: timedelta = datetime.now() - self.__start_time
        duplicated_dropped: int = cast(int, stats["duplicated_dropped"])

        if duplicated_dropped > 0:
            logger.warning(
                f"Total duplicate events detected and dropped: {duplicated_dropped}"
            )

        return {
            "received": stats["received"],
            "unique_processed": self.__deduplication_store.get_unique_processed(),
            "duplicated_dropped": duplicated_dropped,
            "topics": list[str](cast(set[str], stats["topics"])),
            "uptime": int(uptime.total_seconds()),
        }

    async def close(self) -> None:
        await self.stop()
        await self.__deduplication_store.close()
