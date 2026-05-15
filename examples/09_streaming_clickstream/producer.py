"""Synthetic clickstream producer for demo 09.

Emits JSON click events to the `clicks` Kafka topic at ~10 events
per second. Press Ctrl+C to stop.

Run (in one terminal):
    python examples/09_streaming_clickstream/producer.py

While this runs, start the pipeline in another terminal:
    make demo-streaming-pipeline
"""

from __future__ import annotations

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
    p = Producer({"bootstrap.servers": "localhost:9092"})
    print("producing to topic=clicks (Ctrl+C to stop)")
    try:
        i = 0
        while True:
            event = {
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
            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"\nstopped after {i} events")
    finally:
        p.flush(5.0)


if __name__ == "__main__":
    main()
