"""Synthetic clickstream producer for demo 09.

Emits JSON click events to the `clicks` Kafka topic at ~10 events
per second. Press Ctrl+C to stop.

Run (in one terminal):
    python examples/09_streaming_clickstream/producer.py

While this runs, start the pipeline in another terminal:
    make demo-streaming-pipeline
"""

from __future__ import annotations

import argparse
import json
import random
import time
from datetime import UTC, datetime

# `confluent-kafka` is the standard Python client; `kafka-python` also
# works. Either is fine — this demo uses confluent-kafka because it
# ships with the ematix-flow dev dependencies.
from confluent_kafka import Producer

URLS = [
    "/", "/about", "/pricing", "/blog", "/docs",
    "/blog/launch", "/blog/v0.3", "/docs/quickstart",
]
REFERRERS = ["google.com", "twitter.com", "direct", "github.com", None]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--num-events", type=int, default=0,
        help="stop after N events (default: 0 = unbounded, Ctrl+C to stop)",
    )
    ap.add_argument(
        "--seq-start", type=int, default=0,
        help="starting seq_id (default 0) — lets restart-aware tests "
             "verify no-gap delivery across consumer restarts",
    )
    ap.add_argument(
        "--rate", type=float, default=10.0,
        help="events per second (default 10)",
    )
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": "localhost:9092"})
    if args.num_events:
        print(f"producing {args.num_events} events to topic=clicks "
              f"(seq_id {args.seq_start}..{args.seq_start + args.num_events - 1})")
    else:
        print("producing to topic=clicks (Ctrl+C to stop)")
    sleep_s = max(1.0 / args.rate, 0.0)
    try:
        i = 0
        while True:
            if args.num_events and i >= args.num_events:
                break
            event = {
                # Monotonic sequence ID — restart-safety tests use this
                # to assert no-gap delivery. Each event carries a
                # unique seq_id; downstream targets can dedupe on it.
                "seq_id": args.seq_start + i,
                "user_id": random.randint(1, 100),
                "url": random.choice(URLS),
                # ISO-8601 without timezone offset — keeps the
                # Arrow → Postgres TIMESTAMP cast in pipeline.py simple.
                "event_ts": datetime.now(UTC).replace(tzinfo=None).isoformat(timespec="microseconds"),
                "referrer": random.choice(REFERRERS),
            }
            p.produce("clicks", json.dumps(event).encode("utf-8"))
            i += 1
            if i % 20 == 0:
                p.flush(1.0)
                print(f"  produced {i} events")
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        print(f"\nstopped after {i} events")
    finally:
        p.flush(5.0)


if __name__ == "__main__":
    main()
