# Demo 11 — S3 (MinIO) parquet → Postgres

Drain parquet files from an S3-compatible bucket into a Postgres
table — using **MinIO** as the object store, so no AWS account
needed.

**Shape:** MinIO bucket `ematix-demo/events/*.parquet` → streaming
object-store source → `analytics.events` in Postgres.

The streaming source tracks a **lexicographic high-water mark** over
object keys. Restarts resume from the last fully-processed file via
a checkpoint snapshot. Idempotent: re-running the seed script + the
pipeline doesn't duplicate rows beyond what's actually new.

## Run

From the repo root, with `make up` already done:

```sh
# 1. Initialize the target table.
make demo-s3-init

# 2. Seed 3 parquet files into the bucket (600 rows total).
make demo-s3-seed

# 3. Start the pipeline. It picks up the 3 files and inserts.
make demo-s3-pipeline
```

## Watch it in action

```sh
# Row count after the first drain:
docker exec -it ematix-flow-pg psql -U postgres -c \
  "SELECT count(*), max(event_ts) FROM analytics.events"

# Browse MinIO's web console at http://localhost:9001
# (login: minioadmin / minioadmin) — see the bucket + files.
```

Re-run `make demo-s3-seed` while the pipeline is running — it adds
3 more files. The streaming source picks them up within
`idle_pause_ms` and you'll see the row count climb by another 600.

## What's happening under the hood

- **`ObjectStoreS3Connection` with `endpoint="http://localhost:9000"`**:
  the same S3 source used for production AWS S3, but pointed at
  MinIO. Same code path on real S3 — just swap the endpoint for
  `https://s3.<region>.amazonaws.com` and use real IAM credentials.
- **Streaming high-water mark**: the source tracks a
  `last_seen_object_key` string, written to a `StateStore` snapshot.
  Each tick it `LIST`s the prefix and filters to keys
  lexicographically greater than the mark. UUIDv7-style time-ordered
  keys keep this efficient.
- **Format auto-detection**: `format="parquet"` tells the source
  how to decode each file. CSV / JSON / ORC are also supported via
  the same connection, just change `format=`.
- **At-least-once semantics**: the high-water mark only advances
  after the Postgres `INSERT` commits. If `flow consume` crashes
  mid-file, the next run re-reads from the last fully-committed
  file's key.

## Reset

```sh
# Empty the bucket:
docker exec ematix-flow-minio mc rm -r --force /data/ematix-demo

# Empty the Postgres table:
docker exec ematix-flow-pg psql -U postgres -c "TRUNCATE analytics.events"
```
