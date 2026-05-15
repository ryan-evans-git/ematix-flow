"""S3 (MinIO) → Postgres streaming pipeline.

Watches `s3://ematix-demo/events/` for new parquet files (sorted
lexicographically by key, with a high-water mark). Each new file's
rows land in `analytics.events` in Postgres.

This is the **`ObjectStoreS3Connection` with a custom `endpoint`**
path — pointed at MinIO instead of AWS, with MinIO's local
credentials. No AWS account required.

Run:
    python examples/11_s3_parquet_to_postgres/pipeline.py
"""

from __future__ import annotations

from ematix_flow import (
    ObjectStoreS3Connection,
    PostgresConnection,
    register_connection,
    run_streaming_pipeline,
)


def main() -> None:
    src = ObjectStoreS3Connection(
        name="minio_events",
        endpoint="http://localhost:9000",
        bucket="ematix-demo",
        region="us-east-1",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        format="parquet",
    )
    tgt = PostgresConnection(
        name="warehouse",
        url="postgres://postgres:postgres@localhost:5432/postgres",
    )
    register_connection(src)
    register_connection(tgt)

    # `source_query` is the object-key prefix to drain. The
    # streaming source watches this prefix, lists new files since
    # the last high-water mark on each tick, decodes each parquet
    # file's row groups, and ships the resulting Arrow batches to
    # the target's INSERT path.
    run_streaming_pipeline(
        name="s3-events-to-pg",
        source=src,
        source_query="events/",
        target=tgt,
        target_table=("analytics", "events"),
    )


if __name__ == "__main__":
    main()
