# CDC mirror via Debezium (Phase Δ)

End-to-end demo: Postgres source → Debezium → Kafka →
ematix-flow → Postgres target. INSERT / UPDATE / DELETE on the
source table propagate to the mirror table by way of `[transform.cdc]`.

Plan: [`docs/PHASE_DELTA_CDC_PLAN.md`](../../docs/PHASE_DELTA_CDC_PLAN.md).
User guide section: [`docs/USER_GUIDE.md`](../../docs/USER_GUIDE.md#cdc-source-mode-δ).

## What this directory contains

| File | Purpose |
|---|---|
| `docker-compose.yml` | 4-service stack (`pg-source`, `kafka`, `connect`, `pg-target`) |
| `seed-source.sql` | DDL + 2 initial rows on the source; sets `REPLICA IDENTITY FULL` |
| `seed-target.sql` | DDL on the target — must match the source's column set |
| `register-connector.json` | Debezium connector config posted to the Connect REST API |
| `register-connector.sh` | Idempotent registration helper (waits for Connect to come up) |
| `pipeline.toml` | ematix-flow consumer — Kafka source, Postgres target, `[transform.cdc]` |

## Prerequisites

- Docker + `docker compose`.
- `ematix-flow` installed locally (`pip install ematix-flow` puts the
  `flow` binary on `$PATH`).
- A free local port range: `5433` (source PG), `5434` (target PG),
  `9094` (Kafka external listener), `8083` (Connect REST).

## Bring up the stack

```sh
docker compose -f examples/cdc-debezium/docker-compose.yml up -d
./examples/cdc-debezium/register-connector.sh
```

The register script polls the Connect REST API until it answers,
then posts the connector config. First run takes 30–60 s;
subsequent `up -d` cycles reuse the cached images.

Verify everything is green:

```sh
# Connector running?
curl -s http://localhost:8083/connectors/pg-source-connector/status \
    | python3 -m json.tool

# Topic created with the snapshot rows?
docker compose -f examples/cdc-debezium/docker-compose.yml \
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
flow consume examples/cdc-debezium/pipeline.toml --metrics-port 9100
```

The consumer prints log lines as it drains the topic. Within a
second or two the snapshot rows land in the target:

```sh
docker compose -f examples/cdc-debezium/docker-compose.yml \
    exec pg-target psql -U postgres -d target \
    -c "SELECT * FROM public.customers ORDER BY id;"
```

You should see Alice + Bob mirrored.

## Drive change events

INSERT, UPDATE, and DELETE on the source — every operation
propagates as a separate event:

```sh
COMPOSE="docker compose -f examples/cdc-debezium/docker-compose.yml"

# INSERT
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    INSERT INTO public.customers VALUES (3, 'carol@example.com', 'Carol');"

# UPDATE
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    UPDATE public.customers SET name = 'Alice Smith' WHERE id = 1;"

# DELETE
$COMPOSE exec pg-source psql -U postgres -d source -c "\
    DELETE FROM public.customers WHERE id = 2;"

# Verify the mirror
$COMPOSE exec pg-target psql -U postgres -d target \
    -c "SELECT * FROM public.customers ORDER BY id;"
```

Final state: `id=1` renamed, `id=2` gone, `id=3` present.

## What the consumer is doing per batch

1. Polls `dbz.public.customers` from the host's `localhost:9094` Kafka
   listener.
2. Decodes each message JSON into a `RecordBatch`.
3. The streaming runtime sees `[transform.cdc]` is configured →
   routes each batch through `Backend::run_cdc` instead of the
   universal `write_arrow_stream` append path.
4. `cdc::events_from_batch` walks the batch and resolves each
   row to a `CdcEvent { op, key, ts_ms, before, after }`.
5. The Postgres executor opens a single transaction and, per
   event:
   - **Idempotency gate** (PR 4) — `INSERT … ON CONFLICT DO
     UPDATE WHERE existing.last_seen_ts_ms < EXCLUDED.last_seen_ts_ms
     RETURNING 1` against `ematix_flow.cdc_idempotency`. Empty
     RETURNING = redelivery; skip the data write.
   - **Schema check** (PR 5) — keys in the `after` payload not in
     the target's column set go through the configured
     `schema_evolution` policy (`skip` warns + applies; `fail`
     errors).
   - **Apply** — UPSERT for `c`/`r`, UPDATE for `u`, DELETE for
     `d`. JSON → row coercion uses
     `jsonb_populate_record(NULL::<table>, $1::jsonb)` so types
     coerce identically across all three paths.
6. Single tx commit. Source offsets advance only after target
   write acks (at-least-once), and the gate's strict-monotonic
   check means a redelivery does no work.

## Verify idempotency

Stop the consumer (`Ctrl+C`), restart it. Watch the Prometheus
counters:

```sh
curl -s http://localhost:9100/metrics | grep ematix_streaming_cdc_
```

After a fresh start the consumer re-reads from the configured
group's committed offset; if a few events get redelivered (Kafka
rebalance, etc.), they show up under
`ematix_streaming_cdc_idempotent_skipped_total` instead of being
re-applied.

## Tear down

```sh
docker compose -f examples/cdc-debezium/docker-compose.yml down -v
```

The `-v` flag drops the named volumes too, so the next `up -d`
re-runs the seed SQL.

## Troubleshooting

**Connector status shows `FAILED`.** Most often the source
Postgres came up after Connect tried to attach. Restart the
connect container: `docker compose ... restart connect`, then
re-run `register-connector.sh`.

**`flow consume` times out connecting to Kafka.** The pipeline
points at `localhost:9094` (the EXTERNAL listener). If you
changed the published port in `docker-compose.yml`, update
`pipeline.toml` to match.

**No events landing in the target.** Confirm with
`kafka-console-consumer` (above) that the topic actually has
data. If snapshot events are present but new INSERTs don't show,
check the connector status — it may have stopped consuming after
a transient error.

**Schema mismatch.** If you alter the source table, the
`schema_evolution = "skip"` default means the new column is
silently dropped; switch to `"fail"` for visibility, or update
`seed-target.sql` and re-bring up the stack with `down -v`.

## Limitations of this demo

- Single source table. Debezium can capture many tables in one
  connector — the `table.include.list` would expand and the
  consumer would need one `[transform.cdc]` pipeline per topic
  (or a multi-topic source with a topic→target dispatch layer,
  not yet shipped).
- Single-node Kafka. Production needs ≥3 brokers + replication
  factor 3 on the connector's offset/config/status topics.
- No schema registry. The connector publishes JSON without
  schemas — easy to read, but not space-efficient and not the
  shape you'd want at high throughput. Avro / Protobuf via
  Confluent Schema Registry is supported on the source side
  ([USER_GUIDE](../../docs/USER_GUIDE.md#schema-registry-kafka-avro--protobuf))
  but the demo skips it for clarity.
- Postgres target only. `Backend::run_cdc` ships only the
  Postgres impl today; Delta + DuckDB + MySQL are documented
  follow-ups (`docs/PHASE_DELTA_CDC_PLAN.md` "Phase Δ extensions").
