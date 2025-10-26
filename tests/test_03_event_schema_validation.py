from subprocess import Popen
from typing import Any, LiteralString

from utils.testing import (
    EventData,
    cleanup_db,
    post_request,
    start_server,
    stop_server,
)


def test_event_schema_validation() -> None:
    db_path: str = "test_schema_validation.db"
    port: str = "8003"

    cleanup_db(db_path)

    server_url: LiteralString = f"http://127.0.0.1:{port}"

    server: Popen[bytes] = start_server(db_path, port)

    url: str = f"{server_url}/publish"

    valid_events: list[EventData] = [
        {
            "event_id": "test-event-1",
            "topic": "test-topic",
            "source": "test-source",
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data: dict[str, list[EventData]] = {"events": valid_events}
    status, _ = post_request(url, data)
    assert status == 200

    invalid_events_missing_topic: list[dict[str, Any]] = [
        {
            "source": "test-source",
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_missing_topic}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_missing_source: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_missing_source}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_missing_payload: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "source": "test-source",
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_missing_payload}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_invalid_payload: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "source": "test-source",
            "payload": {
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_invalid_payload}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_wrong_type_topic: list[dict[str, Any]] = [
        {
            "topic": 123,
            "source": "test-source",
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_wrong_type_topic}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_wrong_type_source: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "source": 456,
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_wrong_type_source}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_invalid_timestamp: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "source": "test-source",
            "payload": {
                "message": "Test message",
                "timestamp": "2025-01-01T00:00:00",
            },
            "timestamp": "invalid-timestamp",
        }
    ]
    data = {"events": invalid_events_invalid_timestamp}
    status, _ = post_request(url, data)
    assert status == 422

    invalid_events_invalid_payload_timestamp: list[dict[str, Any]] = [
        {
            "topic": "test-topic",
            "source": "test-source",
            "payload": {
                "message": "Test message",
                "timestamp": "invalid-timestamp",
            },
            "timestamp": "2025-01-01T00:00:00",
        }
    ]
    data = {"events": invalid_events_invalid_payload_timestamp}
    status, _ = post_request(url, data)
    assert status == 422

    data = {"events": []}
    status, _ = post_request(url, data)
    assert status == 200

    stop_server(server)
    cleanup_db(db_path)
