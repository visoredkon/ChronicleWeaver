from subprocess import Popen
from time import sleep, time
from typing import Any, Literal, LiteralString

from orjson import loads

from utils.testing import EventData

from .testing import (
    cleanup_db,
    generate_test_events,
    get_request,
    post_request,
    start_server,
    stop_server,
)


def run_benchmark() -> None:
    port: str = "8001"
    base_url: LiteralString = f"http://localhost:{port}"

    db_path: str = "benchmark.db"

    total_events: int = 5000
    duplicate_ratio: float = 0.2

    cleanup_db(db_path)
    server: Popen[bytes] = start_server(db_path, port)

    events: list[EventData] = generate_test_events(total_events, duplicate_ratio)
    expected_unique: int = int(total_events * (1 - duplicate_ratio))
    batch_size: int = 10

    print("\n" + "=" * 70)
    print("BENCHMARK CONFIGURATION".center(70))
    print("=" * 70)
    print(f"  Total Events        : {total_events:,}")
    print(f"  Duplicate Ratio     : {duplicate_ratio:.1%}")
    print(f"  Expected Unique     : {expected_unique:,}")
    print(f"  Batch Size          : {batch_size}")
    print("=" * 70 + "\n")

    publish_start: float = time()

    total_batches: int = (len(events) + batch_size - 1) // batch_size

    print("Phase 1: Publishing Events")
    print("-" * 70)
    for i in range(0, len(events), batch_size):
        batch: list[EventData] = events[i : i + batch_size]
        status_code, _ = post_request(url=f"{base_url}/publish", data={"events": batch})

        if status_code != 200:
            print(
                f"\n[ERROR] Failed to publish batch {i // batch_size + 1}: Status {status_code}"
            )
            stop_server(server)
            cleanup_db(db_path)
            return

        current_batch: int = i // batch_size + 1
        progress: float = (current_batch / total_batches) * 100
        bar_length: int = 40
        filled: int = int(bar_length * current_batch / total_batches)
        bar: LiteralString = "█" * filled + "░" * (bar_length - filled)
        print(
            f"\r  [{bar}] {progress:.1f}% ({current_batch}/{total_batches} batches)",
            end="",
            flush=True,
        )

    publish_end: float = time()
    publish_time: float = publish_end - publish_start
    publish_throughput: float = total_events / publish_time if publish_time > 0 else 0
    print(f"\n  Completed in {publish_time:.3f}s ({publish_throughput:.2f} events/s)\n")

    previous_processed: int = 0
    processing_start: float = time()

    print("Phase 2: Processing Events")
    print("-" * 70)
    while True:
        status, stats_response = get_request(url=f"{base_url}/stats")

        if status == 200 and stats_response:
            stats: dict = loads(stats_response)
            current_processed: int = stats["received"]

            progress = (current_processed / total_events) * 100
            bar_length = 40
            filled = int(bar_length * current_processed / total_events)
            bar = "█" * filled + "░" * (bar_length - filled)
            print(
                f"\r  [{bar}] {progress:.1f}% ({current_processed}/{total_events} events)",
                end="",
                flush=True,
            )

            if current_processed == total_events:
                break

            if current_processed == previous_processed:
                sleep(0.5)
            else:
                previous_processed = current_processed
                sleep(0.1)
        else:
            sleep(0.5)

    processing_end: float = time()
    processing_time: float = processing_end - processing_start
    total_time: float = publish_time + processing_time
    print(f"\n  Completed in {processing_time:.3f}s\n")

    status, stats_response = get_request(url=f"{base_url}/stats")
    if status == 200 and stats_response:
        stats = loads(stats_response)

        received: int = stats["received"]
        unique_processed: int = stats["unique_processed"]
        duplicated_dropped: int = stats["duplicated_dropped"]

        status, events_response = get_request(
            url=f"{base_url}/events?topic=publisher-topic"
        )
        if status == 200 and events_response:
            events_data = loads(events_response)
            actual_unique: int = events_data["count"]

            throughput: float | Literal[0] = (
                total_events / total_time if total_time > 0 else 0
            )
            processing_rate: float | Literal[0] = (
                unique_processed / processing_time if processing_time > 0 else 0
            )
            duplicate_rate: Any | Literal[0] = (
                duplicated_dropped / received if received > 0 else 0
            )
            avg_latency: float | Literal[0] = (
                processing_time / unique_processed if unique_processed > 0 else 0
            )
            accuracy: bool = (unique_processed == expected_unique) and (
                actual_unique == expected_unique
            )

            width = 70
            print("\n" + "=" * width)
            print("EVALUATION METRICS".center(width))
            print("=" * width)

            print("\n" + "─" * width)
            print("THROUGHPUT ANALYSIS")
            print("─" * width)
            print(f"  Total Events Sent          : {total_events:,}")
            print(f"  Events Received            : {received:,}")
            print(f"  Unique Events Processed    : {unique_processed:,}")
            print(f"  Duplicate Events Dropped   : {duplicated_dropped:,}")
            print(f"  Overall Throughput         : {throughput:.2f} events/s")
            print(f"  Processing Throughput      : {processing_rate:.2f} events/s")
            print(f"  Publishing Throughput      : {publish_throughput:.2f} events/s")

            print("\n" + "─" * width)
            print("LATENCY ANALYSIS")
            print("─" * width)
            print(f"  Publish Latency            : {publish_time:.3f}s")
            print(f"  Processing Latency         : {processing_time:.3f}s")
            print(f"  Total Latency              : {total_time:.3f}s")
            print(f"  Average Per-Event Latency  : {avg_latency * 1000:.2f}ms")

            print("\n" + "─" * width)
            print("DUPLICATE RATE ANALYSIS")
            print("─" * width)
            print(f"  Input Duplicate Ratio      : {duplicate_ratio:.2%}")
            print(f"  Detected Duplicate Rate    : {duplicate_rate:.2%}")
            print(f"  Expected Unique Events     : {expected_unique:,}")
            print(f"  Actual Unique Events       : {actual_unique:,}")
            print(
                f"  Deduplication Accuracy     : {(1 - abs(actual_unique - expected_unique) / expected_unique) * 100:.2f}%"
            )

            print("\n" + "─" * width)
            print("DATA INTEGRITY")
            print("─" * width)
            accuracy_symbol = "[PASS]" if accuracy else "[FAIL]"
            print(f"  Validation Status          : {accuracy_symbol}")
            if accuracy:
                print(
                    f"  All {expected_unique:,} unique events processed without data loss"
                )
            else:
                print(
                    f"  Data loss detected: Expected {expected_unique:,}, Got {actual_unique:,}"
                )

            print("\n" + "=" * width + "\n")
        else:
            print("\n[ERROR] Failed to retrieve events from aggregator")
    else:
        print("\n[ERROR] Failed to retrieve statistics from aggregator")

    stop_server(server)
    cleanup_db(db_path)


if __name__ == "__main__":
    run_benchmark()
