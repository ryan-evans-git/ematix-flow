# Plan — DLQ management + stream replayability

**PRD:** [docs/prds/2026-07-04-dlq-replay.md](../prds/2026-07-04-dlq-replay.md)
**Status:** Phases 1–2 landed (2026-07-04; Phase 2 on `feat/dlq-replay-phase2`); Phase 3 next
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
- **Landed 2026-07-04** (`feat/dlq-store-phase1`): contract suite
  18/18 on TableDlq(SQLite) + 18/18 on TableDlq(Postgres,
  testcontainers); KafkaTopicDlq applicable subset + typed
  `Unsupported` pins green on a real broker; legacy
  `streaming_pipeline_routes_failed_batch_to_dlq` upgraded with
  `emat-dlq-*` header assertions. Phase 1 deviations/notes for
  Phase 2: (a) `take_for_replay` carries an explicit `now_ms`
  parameter (timestamps-passed-in house rule); (b) the lease column
  is named `leased_until` (plan said `taken_until`); (c)
  KafkaTopicDlq lease semantics are group-offset based and
  process-local — concurrent cross-process replays must serialize
  in the replay engine (see `dlq/kafka_topic.rs` module docs).

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
- **Landed 2026-07-04** (`feat/dlq-replay-phase2`), core primitive only —
  RunHistory `kind=replay` registration deferred to Phase 4 as planned
  (the returned `ReplayReport { taken, succeeded, redeadlettered, parked,
  started_at_ms, finished_at_ms }` is its hook). As landed:
  `StreamingPipeline::run_dlq_replay(selection, ReplayOptions
  { max_attempts = 3, lease = 60 s, park_store })` +
  `dlq::replay::DlqReplaySource` (one-shot lease + per-record decode via
  the SAME source decode paths — JSONL/RawBytes shared fns,
  Avro/Protobuf through the primary Kafka source's
  `decode_payloads`). Deviations/notes for Phases 3–4:
  (a) **single-pass semantics** — one `take_for_replay` per run; records
  re-dead-lettered by a run are NOT re-taken by it (no hot loop against a
  still-broken sink), so a poison record parks after `max_attempts`
  *runs*, with `attempt` incremented per run and the parked record
  showing `attempt == max_attempts`;
  (b) **park on a Kafka store** = typed-`Unsupported` fallback: the
  record is appended + parked into `ReplayOptions::park_store`, else the
  state-store SQL family, else a LOUD in-memory SQLite store (cached per
  pipeline via `resolve_park_fallback_store`), and the original IS acked
  so the topic cursor advances;
  (c) topic-store replays are serialized per pipeline via an in-process
  mutex behind the new `DeadLetterStore::replay_requires_serialization`
  hook (cross-process replays can still double-take — at-least-once,
  single-operator assumption documented in `dlq/replay.rs`);
  (d) acks are issued in take order (per-partition offset order on
  Kafka — its group cursor is contiguous); redrives + fallback parks land
  durably BEFORE originals are acked;
  (e) CDC-mode pipelines are typed-rejected by `run_dlq_replay` (replay
  would bypass `run_cdc` per-event semantics) — CDC replay is a
  follow-up;
  (f) `resolve_dlq_store` is now `pub` (Phase 4's depth/browse/park/purge
  API resolves the same store).

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
