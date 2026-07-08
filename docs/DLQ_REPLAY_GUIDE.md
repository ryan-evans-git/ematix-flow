# Dead-letter queues, replay, and rewind

How a failed record moves through ematix-flow — from the moment a
batch errors to the moment it's redriven (or the whole stream is
rewound past it):

```text
  error policy          DLQ store            replay              rewind
  ────────────          ─────────            ──────              ──────
  on_error = "dlq"  →   topic / table   →    redrive through  →  reset the stream's
  (or write-failure     (browse, lease,      the pipeline's      read position (and
  / late-data DLQ)      park, purge)         OWN transform +     state) to a point
                                             targets             in the past
```

Plan of record: `docs/plans/DLQ_REPLAY.md` ·
PRD: `docs/prds/2026-07-04-dlq-replay.md` ·
UI QA: `docs/qa/DLQ_REPLAY_UI_QA.md`

## 1. Error policy — what dead-letters

Three emission sites route through the same `DeadLetterStore`:

- **Transform errors** under `transform_on_error = "dlq"` — the
  ORIGINAL (pre-transform) input batch dead-letters, one record per
  row.
- **Write failures** when any DLQ opt-in is configured — rows that
  failed to land in a target.
- **Late data** under a window's `late_data = "dlq"` policy — rows
  evicted past their lateness deadline.

Payloads stay in their source wire format (JSON stays JSON, raw
bytes stay raw bytes, Avro/Protobuf re-encode through the source's
schema-registry machinery). Failure metadata — pipeline, stage,
error (truncated at 8 KB), source id, offset snapshot, event time,
failure time, attempt count, payload format — travels out of band
(Kafka headers / table columns), never by envelope-wrapping.

At-least-once ordering is preserved: the DLQ append is acked
BEFORE source offsets commit, so a crash mid-dead-letter
re-delivers rather than loses.

## 2. Choosing the store — `dlq_store`

```python
run_streaming_pipeline(
    ...,
    dead_letter_topic="events-failed",   # topic store target (Kafka sources)
    dlq_store="auto",                    # "auto" (default) | "topic" | "table"
    dlq_max_attempts=3,                  # replay budget; poison records park past it
)
```

TOML equivalents are top-level `dlq_store = "…"` and
`dlq_max_attempts = N`.

- `"auto"` — the historical resolution order: explicit store →
  `dead_letter_topic` + Kafka source → the state store's SQL family
  (a Postgres-checkpointed pipeline gets a Postgres
  `ematix_dlq_records` table for free) → a LOUD in-process SQLite
  fallback (records lost on exit).
- `"topic"` — demand the Kafka topic store. Hard error if the
  pipeline has no Kafka source or no `dead_letter_topic` — never a
  silent fallback.
- `"table"` — demand the table family (browse/lease/park semantics
  a topic can't express), even when a `dead_letter_topic` is set.

Table stores lease safely under concurrency; topic stores keep a
process-local lease and serialize replays per pipeline.

## 3. Replay — redrive through the pipeline

Replay reprocesses dead-lettered records through the pipeline's
OWN transform and targets (never a bypass write). One replay run is
**bounded and single-pass**: it leases the selection once, then
resolves every record —

- **success** → acked (removed) from the store;
- **failure** → re-dead-lettered as a new record with `attempt+1`
  (a still-broken sink never spins a hot loop — the next manual
  replay picks the record up);
- **poison** (`attempt` past the budget) → parked. Parked records
  are excluded from replays until purged; topic stores park into a
  table fallback so their cursor still advances.

Surfaces:

- HTTP: `POST /api/streams/{name}/dlq/replay {"selection": …,
  "max_attempts": …}` — selection is `{"kind":"all"}`,
  `{"kind":"first_n","n":N}`, or `{"kind":"ids","ids":[…]}`. Every
  replay run registers in RunHistory with `kind="replay"` and shows
  in `/api/runs` + the Runs tab.
- Web UI: the `#/streams/{name}/dlq` screen (depth, arrival rates,
  stage breakdown, paged records with payload preview/download,
  Replay / Park / Purge actions).
- Rust: `StreamingPipeline::run_dlq_replay(selection, ReplayOptions)`.

Related inspection endpoints: `GET /api/streams/{name}/dlq`
(depth + rates + stage breakdown), `GET …/dlq/records?status=&page=`
(paged, ≤4 KB payload preview + download link), `POST …/dlq/park`
and `POST …/dlq/purge` (purge demands an explicit selection).

## 4. Rewind — move the whole stream back

When the damage isn't record-shaped (bad deploy, corrupted window,
upstream backfill), rewind the stream itself:

```text
POST /api/streams/{name}/rewind
{"to": {"kind": "timestamp", "ms": 1700000000000},   # or {"kind":"offset","bytes":[…]}
 "confirm_state_reset": true}
```

Orchestration: **gate → resolve → reset → seek**.

- **Gate.** The stream must be stopped (the server 409s while its
  latest run is `running`). A stateful (windowed/session) pipeline
  hard-errors without `confirm_state_reset` — rewinding it clears
  ALL accumulated state, because re-consumed rows would otherwise
  double-count; the web UI backs this with a typed confirmation.
- **Resolve.** Timestamps resolve per source: Kafka via the
  broker's `offsets_for_times` (past-the-tail timestamps map to
  the high watermark — a rewind never replays a partition from
  zero); backends without a timestamp index (Kinesis, SQL) return
  a typed error — rewind those by offset bytes (the same opaque
  blob `offset_snapshot`/`seek_to` round-trip; offset-bytes rewinds
  are single-source only).
- **Reset.** With a state store configured, state-clear +
  offset-write happen in ONE transaction (`StateStore::reset`) — a
  crash mid-rewind can never pair cleared state with stale offsets.
  The in-memory transform state and the pipeline's watermark
  bookkeeping are cleared too.
- **Seek.** Every source's `seek_to` applies the resolved position;
  restart the stream to resume.

Guarantee: rewind-to-T then run produces the same output as a
fresh pipeline started at T (pinned by the Phase 3
rewind-equivalence test).

## 5. Semantics summary

| Concern | Guarantee |
|---|---|
| Dead-letter durability | append acked before source offsets commit (at-least-once) |
| Replay delivery | at-least-once into the targets, same as the pipeline (idempotent-target assumption) |
| Poison containment | `attempt` budget (`dlq_max_attempts`, default 3) → park |
| Concurrent replays | table stores: safe (row leases); topic stores: serialized per pipeline in-process |
| Rewind atomicity | state clear + offset write in one state-store transaction |
| Rewind equivalence | rewind-to-T ≡ fresh-start-at-T on windowed pipelines |
| CDC pipelines | replay typed-rejected (would bypass per-event semantics) — follow-up |
