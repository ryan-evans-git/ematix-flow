# CDC mirror to Delta Lake (Phase Δ.X1)

End-to-end demo: Postgres source → Debezium → Kafka →
ematix-flow → **local Delta Lake table**. INSERT / UPDATE /
DELETE on the source table propagate to the Delta target by way
of `[transform.cdc]` and `Backend::run_cdc` on the
`DeltaBackend`.

This is the Δ.X1 sibling of [`examples/cdc-debezium`](../cdc-debezium/),
which has the same source pipeline but lands rows in a Postgres
mirror. The two demos can run side-by-side — they use different
Kafka host ports (9094 vs. 9095) and different Compose project
names so the containers don't collide.

Plan: [`docs/PHASE_DELTA_CDC_PLAN.md`](../../docs/PHASE_DELTA_CDC_PLAN.md).
User guide section: [`docs/USER_GUIDE.md`](../../docs/USER_GUIDE.md#cdc-source-mode-δ).

## What this directory contains

| File | Purpose |
|---|---|
| `docker-compose.yml` | 3-service stack (`pg-source`, `kafka`, `connect`) — no `pg-target` |
| `seed-source.sql` | DDL + 2 initial rows on the source; sets `REPLICA IDENTITY FULL` |
| `register-connector.json` | Debezium connector config posted to the Connect REST API |
| `register-connector.sh` | Idempotent registration helper (waits for Connect to come up) |
| `pipeline.toml` | ematix-flow consumer — Kafka source, **Delta-local target**, `[transform.cdc]` |

The Delta target lives on the host filesystem at
`/tmp/ematix-flow-cdc-delta/customers`. The deltalake crate
creates the directory + the `_delta_log/` on the first batch's
MERGE; you don't need to set up a database container.

## Why this example exists

The Δ.X1 plan called for *every* SQL target ematix-flow supports
to have a CDC `run_cdc` impl, but Postgres was the only one with
a runnable end-to-end demo. This closes the gap for Delta — and
locks in (a) that the [target.table].primary_key plumbing landed
in Δ.X1.2 actually works against a real consumer, and (b) that
the single-MERGE-per-batch path collapses correctly under live
event traffic.

## Prerequisites

- Docker + `docker compose`.
- `ematix-flow` installed locally (`pip install ematix-flow`
  puts the `flow` binary on `$PATH`).
- A free local port range: `5435` (source PG), `9095` (Kafka
  external listener), `8084` (Connect REST). All three are
  offset by 1 from the cdc-debezium ports so both demos can run
  simultaneously.
- For *verifying* the target: a tool that reads Delta tables.
  The walkthrough below uses `deltalake-python` (`pip install
  deltalake`) but `duckdb` (`SELECT * FROM
  delta_scan('/tmp/...')`) and `polars`
  (`pl.read_delta('/tmp/...')`) work too.

## Bring up the stack

```sh
docker compose -f examples/cdc-delta/docker-compose.yml up -d
./examples/cdc-delta/register-connector.sh
```

The register script polls the Connect REST API until it
answers, then posts the connector config. First run takes
30–60 s; subsequent `up -d` cycles reuse the cached images.

Verify everything is green:

```sh
# Connector running?
curl -s http://localhost:8084/connectors/pg-source-connector/status \
    | python3 -m json.tool

# Topic created with the snapshot rows?
docker compose -f examples/cdc-delta/docker-compose.yml \
    exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic dbz.public.customers \
    --from-beginning --max-messages 2
```

You should see two snapshot events (`"op": "r"`) for `id=1` and
`id=2`.

## Run the ematix-flow consumer

In another shell:

```sh
flow consume examples/cdc-delta/pipeline.toml --metrics-port 9101
```

The consumer prints log lines as it drains the topic. Within a
second or two the snapshot rows land in the Delta table.

```sh
python3 -c "
from deltalake import DeltaTable
t = DeltaTable('/tmp/ematix-flow-cdc-delta/customers')
print(t.to_pandas().sort_values('id').to_string(index=False))
"
```

You should see Alice + Bob mirrored. The Delta directory looks
like:

```
/tmp/ematix-flow-cdc-delta/customers/
├── _delta_log/
│   └── 00000000000000000000.json
└── part-00000-<uuid>.snappy.parquet
```

## Drive change events

INSERT, UPDATE, and DELETE on the source — every operation
propagates as a separate event:

```sh
COMPOSE="docker compose -f examples/cdc-delta/docker-compose.yml"

# INSERT
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    INSERT INTO public.customers VALUES (3, 'carol@example.com', 'Carol');"

# UPDATE
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    UPDATE public.customers SET name = 'Alice Smith' WHERE id = 1;"

# DELETE
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    DELETE FROM public.customers WHERE id = 2;"

# Verify the mirror — re-read the Delta table from disk
python3 -c "
from deltalake import DeltaTable
t = DeltaTable('/tmp/ematix-flow-cdc-delta/customers')
print(t.to_pandas().sort_values('id').to_string(index=False))
"
```

Final state: `id=1` renamed, `id=2` gone, `id=3` present. The
`_delta_log/` will have one new commit per batch processed by
the consumer.

## What the consumer is doing per batch

1. Polls `dbz.public.customers` from the host's `localhost:9095`
   Kafka listener.
2. Decodes each message JSON into a `RecordBatch`.
3. The streaming runtime sees `[transform.cdc]` is configured →
   routes each batch through `Backend::run_cdc` instead of the
   universal `write_arrow_stream` append path.
4. `cdc::events_from_batch` walks the batch and resolves each
   row to a `CdcEvent { op, key, ts_ms, before, after }`.
5. The **Delta executor (Δ.X1)** is a single
   `DeltaOps::merge` per batch with three branches:
   - `when_matched_delete()` for events with `op = "d"`.
   - `when_matched_update_columns(...)` for `c` / `r` / `u`
     events that find an existing row.
   - `when_not_matched_insert_all()` for `c` / `r` / `u`
     events whose key isn't yet in the table.
   Within-batch dedupe collapses multiple events on the same PK
   to the newest `source.ts_ms` so we don't apply stale
   intermediate states.
6. The MERGE acquires a Delta-table lock for the duration of
   the commit, so concurrent writers are serialized through the
   `_delta_log/` rather than racing.

## Verify idempotency

Stop the consumer (`Ctrl+C`), restart it. The Kafka consumer
group's committed offsets carry over, so the second run picks
up where the first left off. Watch the Prometheus counters for
proof:

```sh
curl -s http://localhost:9101/metrics | grep ematix_streaming_cdc_
```

`ematix_streaming_cdc_idempotent_skipped_total` increments
whenever a redelivered Kafka message lands in the same batch as
its newer counterpart (within-batch dedupe). For *between*-batch
idempotency on Delta — where redelivery in a *different* batch
would re-apply — see the Δ.X1.1 deferred work in
[`docs/PHASE_DELTA_CDC_PLAN.md`](../../docs/PHASE_DELTA_CDC_PLAN.md).
Non-issue when the source preserves per-PK ordering, which
Debezium does by default (events for one PK go to one partition).

## Tear down

```sh
docker compose -f examples/cdc-delta/docker-compose.yml down -v
rm -rf /tmp/ematix-flow-cdc-delta
```

The `-v` flag drops the Compose volumes; the `rm -rf` removes
the Delta files so the next `up -d` starts from a clean slate.

## Troubleshooting

**`Connector status shows FAILED`.** Most often the source
Postgres came up after Connect tried to attach. Restart the
connect container: `docker compose ... restart connect`, then
re-run `register-connector.sh`.

**`flow consume` times out connecting to Kafka.** The pipeline
points at `localhost:9095` (the EXTERNAL listener for this
demo). If you have the cdc-debezium stack also running, that one
uses 9094 — confirm you're targeting the right port.

**`No such file or directory` reading the Delta table.** The
`_delta_log/` doesn't exist until the first MERGE commits. If
you've only just started the consumer, wait a moment — the
snapshot batch (~2 events) commits within seconds.

**Empty rows after INSERT/UPDATE/DELETE.** Confirm via
`kafka-console-consumer` (above) that the topic actually has
the new events. Then check Connect status:
```sh
curl -s http://localhost:8084/connectors/pg-source-connector/status \
    | python3 -m json.tool
```
A FAILED connector after a publication-slot drop is the most
common cause.

## Limitations of this demo

- **Single source table.** Multi-topic CDC pipelines aren't
  shipped — see the cdc-debezium README for the same caveat.
- **Single-node Kafka.** Production needs ≥3 brokers + RF=3 on
  the connector's offset/config/status topics.
- **Local-filesystem Delta only.** S3 + DynamoDB-locked tables
  via `[target] kind = "delta_s3"` work the same way at the
  protocol level — swap the target block, supply the AWS
  credentials, done. Not exercised in this compose because the
  point is to keep the demo runnable on a laptop without cloud
  setup.
- **No between-batch idempotency for Delta.** See
  `ematix_streaming_cdc_idempotent_skipped_total` discussion
  above — Δ.X1.1 closes this gap when needed; not blocking for
  the typical Debezium-with-PK-partitioning case.
