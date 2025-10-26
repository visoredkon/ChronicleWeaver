from asyncio import run
from datetime import datetime
from http.client import HTTPResponse
from os import getenv
from typing import TypeAlias
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from orjson import dumps

EventData: TypeAlias = dict[str, str | dict[str, str]]
RequestData: TypeAlias = dict[str, list[EventData]]


def make_request(url: str, data: RequestData) -> tuple[int | None, str | None]:
    try:
        request: Request = Request(
            url, data=dumps(data), headers={"Content-Type": "application/json"}
        )

        response: HTTPResponse = urlopen(url=request)
        with response:
            status_code: int = response.getcode()
            response_text: str = response.read().decode(encoding="utf-8")

            return status_code, response_text
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        print(f"Request failed: {e}")

        return None, None


def generate_test_events(
    count: int = 10, duplicate_ratio: float = 0.1
) -> list[EventData]:
    events: list[EventData] = []

    for i in range(int(count * (1 - duplicate_ratio))):
        events.append(
            {
                "event_id": f"publisher-event-{i}",
                "topic": "publisher-topic",
                "source": "publisher-service",
                "payload": {
                    "message": f"Message from publisher {i}",
                    "timestamp": datetime.now().isoformat(),
                },
                "timestamp": datetime.now().isoformat(),
            }
        )

    for i in range(int(count * duplicate_ratio)):
        if events:
            original: EventData = events[i % len(events)]
            events.append(
                {
                    "event_id": original["event_id"],
                    "topic": original["topic"],
                    "source": original["source"],
                    "payload": {
                        "message": f"Duplicate message {i}",
                        "timestamp": datetime.now().isoformat(),
                    },
                    "timestamp": datetime.now().isoformat(),
                }
            )

    return events


async def main() -> None:
    host: str = getenv("AGGREGATOR_HOST", default="localhost")
    port: int = int(getenv("AGGREGATOR_PORT", default="8000"))
    url: str = f"http://{host}:{port}/publish"

    print(f"Publisher starting stress test, will send 5000 events to {url}")

    try:
        events: list[EventData] = generate_test_events(count=5000, duplicate_ratio=0.2)

        data: dict[str, list[EventData]] = {"events": events}
        status, response = make_request(url, data)

        print(f"Stress test: Published {len(events)} events - Status: {status}")

        if status != 200:
            print(f"Error response: {response}")
    except Exception as e:
        print(f"Failed to publish batch: {e}")


if __name__ == "__main__":
    run(main=main())
