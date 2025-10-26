from subprocess import Popen
from time import sleep, time
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


def test_batch_stress() -> None:
    db_path: str = "test_batch_stress.db"
    port: str = "8005"

    cleanup_db(db_path)

    server_url: LiteralString = f"http://127.0.0.1:{port}"

    server: Popen[bytes] = start_server(db_path, port)

    url: str = f"{server_url}/publish"

    event_count: int = 250
    duplicate_ratio: float = 0.2

    events: list[EventData] = generate_test_events(
        count=event_count, duplicate_ratio=duplicate_ratio
    )
    data: dict[str, list[EventData]] = {"events": events}

    start_time: float = time()
    status, _ = post_request(url, data)
    end_time: float = time()

    publish_time: float = end_time - start_time

    assert status == 200
    assert publish_time < 7

    sleep(7)

    stats_url: str = f"{server_url}/stats"
    stats_status, stats_response = get_request(url=stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    received: int = stats["received"]
    unique_processed: int = stats["unique_processed"]
    duplicated_dropped: int = stats["duplicated_dropped"]

    assert received == event_count
    assert unique_processed == int(event_count * (1 - duplicate_ratio))
    assert duplicated_dropped == int(event_count * duplicate_ratio)
    assert received == unique_processed + duplicated_dropped

    events_url: str = f"{server_url}/events"
    events_status, events_response = get_request(url=events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    all_events_count: int = events_data["count"]

    assert all_events_count == unique_processed

    stop_server(server)
    cleanup_db(db_path)
