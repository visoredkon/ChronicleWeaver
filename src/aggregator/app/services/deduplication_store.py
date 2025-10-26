from collections.abc import Iterable
from datetime import datetime
from os import getenv
from typing import cast

from aiosqlite import Connection, Cursor, Row, connect
from orjson import dumps, loads

from ..models.events import EventModel, EventPayloadModel


class DeduplicationStoreService:
    def __new__(cls) -> "DeduplicationStoreService":
        return super().__new__(cls)

    def __init__(self) -> None:
        self.__connection: "Connection | None" = None
        self.__processed_set: set[tuple[str, str]] = set()
        self.__processed_events: dict[str, list[EventModel]] = {}
        self.__stats: dict[str, object] = {}

    async def initialize(self) -> None:
        self.__connection = await connect(
            database=getenv(key="DEDUPLICATION_DB_PATH", default=".chronicle.db")
        )

        _: Cursor = await self.__connection.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                event_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                source TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (event_id, topic)
            )
        """)

        _: Cursor = await self.__connection.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY,
                received INTEGER NOT NULL DEFAULT 0,
                duplicated_dropped INTEGER NOT NULL DEFAULT 0,
                topics TEXT NOT NULL DEFAULT '[]'
            )
        """)

        await self.__connection.commit()

        cursor: Cursor = await self.__connection.execute(
            "SELECT event_id, topic, source, payload, timestamp FROM processed_events"
        )
        rows: Iterable[Row] = await cursor.fetchall()

        await cursor.close()

        self.__processed_set = {(cast(str, row[0]), cast(str, row[1])) for row in rows}
        self.__processed_events = {}
        for row in rows:
            event_payload: EventPayloadModel = EventPayloadModel.model_validate_json(
                json_data=cast(str, row[3])
            )
            event = EventModel(
                event_id=cast(str, row[0]),
                topic=cast(str, row[1]),
                source=cast(str, row[2]),
                payload=event_payload,
                timestamp=datetime.fromisoformat(cast(str, row[4])),
            )

            topic = cast(str, row[1])
            if topic not in self.__processed_events:
                self.__processed_events[topic] = []

            self.__processed_events[topic].append(event)

        cursor = await self.__connection.execute(
            "SELECT received, duplicated_dropped, topics FROM stats WHERE id=1"
        )
        stats = await cursor.fetchone()
        await cursor.close()

        if stats:
            self.__stats = {
                "received": cast(int, stats[0]),
                "duplicated_dropped": cast(int, stats[1]),
                "topics": set[str](cast(list[str], loads(cast(str, stats[2])))),
            }
        else:
            self.__stats = {
                "received": 0,
                "duplicated_dropped": 0,
                "topics": set[str](),
            }

    async def mark_processed(self, event: EventModel) -> None:
        if self.__connection is None:
            raise RuntimeError("Connection not initialized")

        key: tuple[str, str] = (event.event_id, event.topic)
        if key not in self.__processed_set:
            self.__processed_set.add(key)

            _ = await self.__connection.execute(
                "INSERT OR IGNORE INTO processed_events (event_id, topic, source, payload, timestamp) VALUES (?, ?, ?, ?, ?)",
                (
                    event.event_id,
                    event.topic,
                    event.source,
                    event.payload.model_dump_json(),
                    event.timestamp.isoformat(),
                ),
            )

            await self.__connection.commit()

            if event.topic not in self.__processed_events:
                self.__processed_events[event.topic] = []

            self.__processed_events[event.topic].append(event)

    async def is_processed(self, event_id: str, topic: str) -> bool:
        if self.__connection is None:
            raise RuntimeError("Connection not initialized")

        return (event_id, topic) in self.__processed_set

    async def update_received(self) -> None:
        self.__stats["received"] = cast(int, self.__stats["received"]) + 1

        await self.__save_stats()

    async def update_duplicated_dropped(self) -> None:
        self.__stats["duplicated_dropped"] = (
            cast(int, self.__stats["duplicated_dropped"]) + 1
        )

        await self.__save_stats()

    async def add_topic(self, topic: str) -> None:
        cast(set[str], self.__stats["topics"]).add(topic)

        await self.__save_stats()

    async def __save_stats(self) -> None:
        if self.__connection is None:
            raise RuntimeError("Connection not initialized")

        _ = await self.__connection.execute(
            "INSERT OR REPLACE INTO stats (id, received, duplicated_dropped, topics) VALUES (1, ?, ?, ?)",
            (
                self.__stats["received"],
                self.__stats["duplicated_dropped"],
                dumps(list[str](cast(set[str], self.__stats["topics"]))),
            ),
        )

        await self.__connection.commit()

    def get_stats(self) -> dict[str, object]:
        return self.__stats.copy()

    def get_unique_processed(self) -> int:
        return len(self.__processed_set)

    def get_all_events(self) -> list[EventModel]:
        events: list[EventModel] = []
        for events_list in self.__processed_events.values():
            events.extend(events_list)

        return events

    def get_events_by_topic(self, topic: str) -> list[EventModel]:
        return self.__processed_events.get(topic, [])

    async def close(self) -> None:
        if self.__connection:
            await self.__connection.close()
