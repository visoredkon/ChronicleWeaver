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


def test_deduplication_persistence() -> None:
    db_path: str = "test_persistence.db"
    port: str = "8002"

    cleanup_db(db_path)

    server_url: LiteralString = f"http://127.0.0.1:{port}"

    server01: Popen[bytes] = start_server(db_path, port)

    url: LiteralString = f"{server_url}/publish"

    events: list[EventData] = generate_test_events(count=100, duplicate_ratio=0.5)
    data: dict[str, list[EventData]] = {"events": events}
    status, _ = post_request(url, data)
    assert status == 200

    sleep(2)

    stats_url: str = f"{server_url}/stats"
    stats_status, stats_response = get_request(url=stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")  # pyright: ignore[reportAny, reportExplicitAny, reportRedeclaration]
    assert stats["unique_processed"] == 50

    stop_server(server01)

    server02: Popen[bytes] = start_server(db_path, port)

    status, _ = post_request(url, data)
    assert status == 200

    sleep(2)

    stats_status, stats_response = get_request(stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")  # pyright: ignore[reportAny, reportExplicitAny]
    assert stats["received"] == 200
    assert stats["unique_processed"] == 50
    assert stats["duplicated_dropped"] == 150

    stop_server(server02)
    cleanup_db(db_path)
