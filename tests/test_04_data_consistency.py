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


def test_data_consistency() -> None:
    db_path: str = "test_data_consistency.db"
    port: str = "8004"

    cleanup_db(db_path)

    server_url: LiteralString = f"http://127.0.0.1:{port}"

    server: Popen[bytes] = start_server(db_path, port)

    url: str = f"{server_url}/publish"

    events: list[EventData] = generate_test_events(count=100, duplicate_ratio=0.3)
    data: dict[str, list[EventData]] = {"events": events}
    status, _ = post_request(url, data)
    assert status == 200

    sleep(2)

    stats_url: str = f"{server_url}/stats"
    stats_status, stats_response = get_request(url=stats_url)
    assert stats_status == 200

    stats: dict[str, Any] = loads(stats_response or "{}")
    received: int = stats["received"]
    unique_processed: int = stats["unique_processed"]
    duplicated_dropped: int = stats["duplicated_dropped"]
    topics: list[str] = stats["topics"]

    assert received == 100
    assert unique_processed == 70
    assert duplicated_dropped == 30
    assert received == unique_processed + duplicated_dropped

    events_url: str = f"{server_url}/events"
    events_status, events_response = get_request(url=events_url)
    assert events_status == 200

    events_data: dict[str, Any] = loads(events_response or "{}")
    all_events_count: int = events_data["count"]
    all_events: list[dict[str, Any]] = events_data["events"]

    assert all_events_count == unique_processed
    assert len(all_events) == unique_processed

    events_by_topic: dict[str, list[dict[str, Any]]] = {}
    for topic in topics:
        topic_url: str = f"{server_url}/events?topic={topic}"
        topic_status, topic_response = get_request(url=topic_url)
        assert topic_status == 200

        topic_data: dict[str, Any] = loads(topic_response or "{}")
        topic_events: list[dict[str, Any]] = topic_data["events"]
        events_by_topic[topic] = topic_events

    total_events_by_topic: int = sum(len(events) for events in events_by_topic.values())
    assert total_events_by_topic == all_events_count

    all_event_ids: set[str] = {event["event_id"] for event in all_events}
    assert len(all_event_ids) == len(all_events)

    for event in all_events:
        assert "event_id" in event
        assert "topic" in event
        assert "source" in event
        assert "payload" in event
        assert "timestamp" in event
        assert "message" in event["payload"]
        assert "timestamp" in event["payload"]

    events_topics: set[str] = {event["topic"] for event in all_events}
    assert set(topics) == events_topics

    for topic in topics:
        topic_events = events_by_topic[topic]
        for event in topic_events:
            assert event["topic"] == topic

    more_events: list[EventData] = []
    for i in range(50):
        more_events.append(
            {
                "event_id": f"additional-event-{i}",
                "topic": "additional-topic",
                "source": "additional-source",
                "payload": {
                    "message": f"Additional message {i}",
                    "timestamp": "2025-01-01T00:00:00",
                },
                "timestamp": "2025-01-01T00:00:00",
            }
        )

    data = {"events": more_events}
    status, _ = post_request(url, data)
    assert status == 200

    sleep(2)

    stats_status, stats_response = get_request(url=stats_url)
    assert stats_status == 200

    updated_stats: dict[str, Any] = loads(stats_response or "{}")
    updated_received: int = updated_stats["received"]
    updated_unique_processed: int = updated_stats["unique_processed"]
    updated_duplicated_dropped: int = updated_stats["duplicated_dropped"]

    assert updated_received == received + 50
    assert updated_unique_processed == unique_processed + 50
    assert updated_duplicated_dropped == duplicated_dropped

    events_status, events_response = get_request(url=events_url)
    assert events_status == 200

    updated_events_data: dict[str, Any] = loads(events_response or "{}")
    updated_all_events_count: int = updated_events_data["count"]

    assert updated_all_events_count == updated_unique_processed

    stop_server(server)
    cleanup_db(db_path)
