"""Seed 3 parquet files of synthetic events into MinIO's
`ematix-demo` bucket under the prefix `events/`.

Files are named UUIDv7-ish (time-ordered) so the streaming
object-store source's lexicographic high-water mark advances
naturally. Re-running this is safe — files have unique keys.

Run:
    python examples/11_s3_parquet_to_postgres/seed.py
"""

from __future__ import annotations

import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from io import BytesIO

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

ENDPOINT = "http://localhost:9000"
BUCKET = "ematix-demo"
PREFIX = "events/"

EVENT_TYPES = ["page_view", "click", "signup", "purchase", "logout"]


def _batch(n: int, base_ts: datetime) -> pa.Table:
    return pa.table(
        {
            "event_id": [random.randint(10**11, 10**12 - 1) for _ in range(n)],
            "user_id": [random.randint(1, 5000) for _ in range(n)],
            "event_type": [random.choice(EVENT_TYPES) for _ in range(n)],
            "payload": [f'{{"v":{random.randint(1,99)}}}' for _ in range(n)],
            "event_ts": [
                base_ts + timedelta(seconds=i) for i in range(n)
            ],
        }
    )


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    base = datetime.now(UTC)
    for i in range(3):
        table = _batch(200, base + timedelta(minutes=i))
        buf = BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        # Time-ordered key so the object-store streaming source's
        # high-water mark sees files in insert order.
        key = f"{PREFIX}{int(time.time_ns())}-{uuid.uuid4().hex[:8]}.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        print(f"  uploaded s3://{BUCKET}/{key}  ({table.num_rows} rows)")
        time.sleep(0.01)  # ensure distinct ns timestamps
    print(f"done — 3 files in s3://{BUCKET}/{PREFIX}")


if __name__ == "__main__":
    main()
