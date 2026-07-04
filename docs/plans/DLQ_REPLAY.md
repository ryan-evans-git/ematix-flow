# Plan — DLQ management + stream replayability

**PRD:** [docs/prds/2026-07-04-dlq-replay.md](../prds/2026-07-04-dlq-replay.md)
**Status:** Phase 1 in flight (2026-07-04)
**Discipline:** TDD per phase (contract tests written first, shared across
store impls); every phase independently green + shippable; strict CI gates
unchanged. Rust core in `ematix-flow-core`, orchestration/API in
`python/ematix_flow`, UI in `web-ui/`.

## Phase 1 — `DeadLetterStore` trait + universal table store + emission rewire

- `DlqMeta` (pipeline, stage `transform|write|late_data`, error, source_id,
  offset_bytes, event_ts, failed_at, attempt, payload_format) +
  `DlqRecord` (meta + payload bytes) + `trait DeadLetterStore`
  { append, depth, browse(page), take_for_replay(selection, lease),
  ack_replayed, park, purge } — async, object-safe.
- **Contract test suite** written FIRST, parameterized over impls.
- `TableDlq`: SQL family (SQLite for dev, Postgres for prod — same
  connection plumbing as the state store). One `ematix_dlq_records`
  table; lease via `taken_until` column; indices on (pipeline, status).
- `KafkaTopicDlq`: today's format-preserving topic emission upgraded —
  metadata in Kafka HEADERS (payload untouched); browse = bounded peek
  consumer; depth = end-offsets − replay-group committed.
- Rewire the two existing emission sites (transform-error, write-error in
  `streaming.rs`) through the trait. Default resolution: explicit
  `dead_letter_topic` + Kafka source → KafkaTopicDlq; else if
  `on_error="dlq"` or write-DLQ wanted → TableDlq (auto, using the
  configured state store family; in-memory store for tests only).
  At-least-once ordering preserved: append ack BEFORE source offset commit.
- Happy-path cost: zero added work when nothing dead-letters (pin with the
  existing streaming bench/smoke).
- Exit: contract suite green on all impls; integration: fail →
  record-with-meta visible via store API on Kafka + SQLite + Postgres.

## Phase 2 — Replay engine (redrive = reprocess through pipeline)

- `DlqReplaySource`: a bounded source over `take_for_replay` leases
  (selection: all | first N | explicit ids). Feeds the pipeline's OWN
  transform + targets in a bounded run; `on_error` forced to DLQ-with-
  attempt+1; `max_attempts` (default 3) → `park`.
- Replay executions registered in RunHistory as `kind=replay`
  (pipeline-linked, visible in Runs).
- Exit: round-trip integration (fail → fix sink → replay → row present,
  DLQ drained) on both store families; poison-park test; concurrent-replay
  lease test.

## Phase 3 — Rewind (offset/timestamp, atomic state reset)

- Timestamp→offset resolution per backend (Kafka `offsets_for_times`;
  Kinesis AT_TIMESTAMP iterator when its durable offsets land; generic
  offset-bytes path via StateStore for the rest).
- Orchestration: pause → resolve → (stateful: state-clear + offset-write in
  ONE state-store transaction) → seek → resume. `confirm_state_reset`
  required for stateful pipelines, hard error without it.
- Exit: rewind-equivalence integration test (rewind-to-T output ≡
  fresh-start-at-T output, windowed pipeline); stateless rewind test.

## Phase 4 — Python surface + HTTP API

- `run_streaming_pipeline(...)`: `dlq_store="auto"|"topic"|"table"`,
  `dlq_max_attempts`; TOML plumbing + validation mirrors
  `transform_on_error`.
- FastAPI: `GET /api/streams/{name}/dlq` (depth, rates, stage breakdown),
  `GET /api/streams/{name}/dlq/records?status=&page=` (paged, payload
  preview ≤4 KB + download link), `POST .../dlq/replay` {selection},
  `POST .../dlq/park` / `purge` {selection},
  `POST /api/streams/{name}/rewind` {to, confirm_state_reset}.
  Action-gating server-side like `restart_from_step`; bearer-token rules
  unchanged.
- Exit: API tests (fixtures + live store), replay run appears in
  `/api/runs` with `kind=replay`.

## Phase 5 — Web UI

- `#/streams/{name}/dlq`: depth card + arrival sparkline, stage-breakdown
  chips, paged record table (time, stage, error, attempt, offset), payload
  drawer, actions (Replay all / selected / first N; Park; Purge) with
  confirm modals — reusing Workflows/Pipelines modal + badge patterns.
- Stream detail: DLQ depth badge (links to DLQ screen), Rewind control
  (offset/timestamp picker; typed confirmation when stateful).
- Runs tab: `replay` badge + link back to the source DLQ screen.
- mock-api.mjs fixtures for all new endpoints (dev parity).
- Exit: `npm run build` clean; manual QA script in docs/qa/.

## Phase 6 — Docs, changelog, gate sweep

- README + docs page (error-policy → DLQ → replay → rewind lifecycle),
  CHANGELOG entry, `docs/EMAT_FLAGS.md` if any env knobs were added.
- Full gates: fmt/clippy(±features)/workspace tests/parity suites/
  tpch_validate; streaming happy-path perf pin; coverage on new modules.

## Risks / watch-outs

- Kafka header size limits for long error strings (truncate at 8 KB).
- TableDlq payload bloat: BYTEA/BLOB rows for large messages — document
  purge/retention; PRD open question on TTL stands.
- Lease semantics under concurrent replays (contract-tested).
- `streaming.rs` is large and hot — emission rewire must be surgical; no
  behavior change when DLQ unset (pin with existing integration tests).
- The idle-gated 22/22 certification bench run is still armed on this
  machine — heavy build phases will postpone it; it fires in the next
  natural idle window.
