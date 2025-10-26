from datetime import datetime
from http.client import HTTPResponse
from os import environ, remove
from os.path import exists
from subprocess import DEVNULL, Popen
from time import sleep
from typing import Any, TypeAlias
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from orjson import dumps

EventData: TypeAlias = dict[str, str | dict[str, str]]


def generate_test_events(
    count: int = 10, duplicate_ratio: float = 0.1
) -> list[EventData]:
    events: list[EventData] = []

    for i in range(int(count * (1 - duplicate_ratio))):
        events.append(
            {
                "event_id": f"event-{i}",
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
                        "message": original["payload"]["message"],  # pyright: ignore[reportArgumentType]
                        "timestamp": original["payload"]["timestamp"],  # pyright: ignore[reportArgumentType]
                    },
                    "timestamp": original["timestamp"],
                }
            )

    return events


def start_server(db_path: str, port: str = "8000") -> Popen[bytes]:
    env: dict[str, str] = environ.copy()
    env["APP_PORT"] = port
    env["DEDUPLICATION_DB_PATH"] = f".{db_path}"

    server: Popen[bytes] = Popen[bytes](
        [
            "uv",
            "run",
            "python",
            "-m",
            "src.aggregator.app.main",
        ],
        env=env,
        stdout=DEVNULL,
        stderr=DEVNULL,
    )

    sleep(2)
    return server


def stop_server(server: Popen[bytes]) -> None:
    server.terminate()
    _ = server.wait()


def get_request(url: str) -> tuple[int | None, str | None]:
    try:
        request: Request = Request(url)

        response: HTTPResponse = urlopen(url=request)
        with response:
            status_code: int = response.getcode()
            response_text: str = response.read().decode(encoding="utf-8")

            return status_code, response_text
    except Exception as e:
        print(f"Request failed: {e}")

        return None, None


def post_request(url: str, data: dict[str, Any]) -> tuple[int | None, str | None]:
    try:
        request: Request = Request(
            url, data=dumps(data), headers={"Content-Type": "application/json"}
        )

        response: HTTPResponse = urlopen(url=request)
        with response:
            status_code: int = response.getcode()
            response_text: str = response.read().decode("utf-8")

            return status_code, response_text
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception as e:
        print(f"Request failed: {e}")

        return None, None


def cleanup_db(db_path: str) -> None:
    db_path = f".{db_path}"

    if exists(path=db_path):
        remove(path=db_path)
