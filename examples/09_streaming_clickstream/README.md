# Demo 09 — streaming clickstream (Kafka → Postgres)

Watch synthetic click events flow from a Kafka topic into a Postgres
table in real time.

**Shape:** Python producer → Kafka topic `clicks` → `flow consume`
streaming daemon → `analytics.clicks` in Postgres.

The pipeline itself is declared in `pipeline.py` via the typed-Python
`@ematix.streaming_pipeline(...)` decorator; `flow consume --module
pipeline clicks-to-pg` imports the module and runs the registered
pipeline.

## Run

From the repo root, with `make up` already done (`docker compose up
-d` in `examples/`):

```sh
# 1. Initialize the target table (one-time per fresh Postgres).
make demo-streaming-init

# 2. Start the producer in one terminal — emits 10 events/sec.
make demo-streaming-producer

# 3. Start the streaming pipeline in another terminal.
make demo-streaming-pipeline
```

## Watch it in action

```sh
# Live row count (refreshes every 2 sec):
watch -n 2 'docker exec ematix-flow-pg psql -U postgres -c \
  "SELECT count(*) AS rows, max(event_ts) AS latest FROM analytics.clicks"'

# Sample of recent events:
docker exec -it ematix-flow-pg psql -U postgres -c \
  "SELECT user_id, url, event_ts FROM analytics.clicks
   ORDER BY event_ts DESC LIMIT 10"
```

You'll see the row count climb by ~10/sec and the latest event_ts
tick forward each second.

## What's happening under the hood

- **Producer**: a 50-line Python script (`producer.py`) emits random
  click events using the standard `confluent-kafka` library. Nothing
  ematix-flow-specific — just a realistic event stream.
- **Pipeline** (`pipeline.toml`): declares a Kafka source + Postgres
  target. `flow consume` reads the topic in batches, decodes the
  JSON, and writes each batch to `analytics.clicks` in one `INSERT`.
- **At-least-once semantics**: source offsets advance only after the
  target write commits. If `flow consume` crashes mid-batch, no rows
  are lost on restart.
- **Restart-on-error**: the `--restart-on-error` flag wraps the
  consumer in a supervised loop with exponential backoff — schema
  drift in the upstream Avro/JSON triggers a graceful restart that
  re-plans the DataFusion transform.

## Stop everything

```sh
# In each terminal: Ctrl+C.
# Or to teardown the whole stack:
make down
```
