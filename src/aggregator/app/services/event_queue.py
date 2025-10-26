from asyncio import Queue

from ..models.events import EventModel


class EventQueueService:
    __instance: "EventQueueService | None" = None
    __queue: "Queue[EventModel] | None" = None

    def __new__(cls) -> "EventQueueService":
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
            cls.__queue = Queue[EventModel]()

        return cls.__instance

    async def get(self) -> "EventModel":
        if self.__queue is None:
            raise RuntimeError("Queue not initialized")

        return await self.__queue.get()

    async def put(self, event: "EventModel") -> None:
        if self.__queue is None:
            raise RuntimeError("Queue not initialized")

        await self.__queue.put(event)
