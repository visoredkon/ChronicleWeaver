from subprocess import Popen
from time import sleep
from typing import Any, LiteralString

from orjson import loads
from utils.testing import (
    EventData,
    cleanup_db,
    generate_test_events,
    get_request,
    post_request,
    start_server,
    stop_server,
)


def test_deduplication() -> None:
    db_path: str = "test_deduplication.db"
    port: str = "8001"

    cleanup_db(db_path)

    server_url: LiteralString = f"http://127.0.0.1:{port}"

    server: Popen[bytes] = start_server(db_path, port)

    url: str = f"{server_url}/publish"

    events: list[EventData] = generate_test_events(count=100, duplicate_ratio=0.5)
    data: dict[str, list[EventData]] = {"events": events}
    status, _ = post_request(url, data)
    assert status == 200

    sleep(2)

    stats_url: str = f"{server_url}/stats"
    stats_status, stats_response = get_request(url=stats_url)
    assert stats_status == 200

    stats: dict[str, int | list[str]] = loads(stats_response or "{}")  # pyright: ignore[reportAny]
    assert stats["received"] == 100
    assert stats["unique_processed"] == 50
    assert stats["duplicated_dropped"] == 50

    events_url: str = f"{server_url}/events?topic=publisher-topic"
    events_status, events_response = get_request(url=events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")  # pyright: ignore[reportExplicitAny, reportAny]
    assert events_data["count"] == 50
    assert len(events_data["events"]) == 50  # pyright: ignore[reportAny]

    event_ids: set[str] = {event["event_id"] for event in events_data["events"]}  # pyright: ignore[reportAny]
    assert len(event_ids) == 50

    stop_server(server)
    cleanup_db(db_path)
