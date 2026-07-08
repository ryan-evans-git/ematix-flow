# Plan — DLQ management + stream replayability

**PRD:** [docs/prds/2026-07-04-dlq-replay.md](../prds/2026-07-04-dlq-replay.md)
**Status:** COMPLETE — Phases 1–2 landed 2026-07-04; Phases 3–6 landed
2026-07-08 (`feat/dlq-replay-phases-3-6`)
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
- **Landed 2026-07-08**: `StreamingPipeline::rewind(RewindTarget, confirm_state_reset)`
  = gate → resolve → durable reset → in-memory clear → seek. New
  hooks: `Backend::offsets_for_timestamp` (Kafka via broker
  `offsets_for_times` on an ephemeral probe consumer; past-tail →
  high watermark; typed default error elsewhere — the Kinesis
  typed-Unsupported precedent), `StateStore::reset` (clear ALL
  blobs + REPLACE offsets in one transaction; InMemory + Postgres;
  contract-tested on both), `BatchTransform::{is_stateful,
  clear_state}` (windowed transforms wipe `WindowState` wholesale;
  the pipeline also zeroes watermark bookkeeping). Exit tests:
  `rewind_equivalence_windowed` (run cut mid-stream so an open
  window would double-count if state survived) +
  `stateless_rewind_replays_from_offset`. Deviations/notes:
  (a) **pause/resume is the caller's job** — `rewind` requires the
  consume loop stopped; the Phase 4 API layer enforces it by
  refusing (409) while the stream's latest run is `running`
  (in-core pause machinery wasn't needed for the exit criteria);
  (b) offset-bytes rewinds are **single-source only** (one opaque
  blob can't honestly address N sources) — multi-source pipelines
  rewind by timestamp, resolved per source;
  (c) `RewindTarget::Timestamp` is caller-supplied ms (house
  timestamps-passed-in rule; no clock reads in the path);
  (d) **Kafka rebalance rework** (pre-req): `seek_to`-recovered
  offsets now apply by mutating the assignment TPL before `assign`
  instead of `seek()` from `post_rebalance`, which raced the
  fetcher ("Erroneous state" — the #539 flake). All 24
  broker-gated Kafka tests pass with the rework;
  (e) rewound offsets are durable via `StateStore::reset` when a
  store is configured; a **no-state-store Kafka pipeline rewound
  from another process** (the HTTP API) only updates the API
  process's in-memory backend — committing the rewound offsets to
  the consumer group is a noted follow-up (single-process rewind +
  state-store pipelines are fully covered).

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
- **Landed 2026-07-08**: `dlq_store`/`dlq_max_attempts` kwargs +
  TOML (validated both in Python and at Rust config-load,
  mirroring `transform_on_error`); `DlqStoreMode` on the core
  config — `"topic"`/`"table"` are demands that hard-error when
  unsatisfiable, `"table"` skips the topic rule. New
  `ematix_flow_cli::dlq_ops` layer (operations-only pipeline built
  with the SAME resolution as `run_consume_with`; stats scans are
  capped at 10k records and report `truncated`) exposed through
  pyo3 (`_core.dlq_stats/records/record_by_id/replay/park/purge/
  stream_rewind`, with a per-TOML pipeline cache so in-process
  fallback stores stay coherent across HTTP calls) and consumed by
  `web/dlq.py` + the FastAPI endpoints exactly as specced. Replay
  runs (including failed ones) register as `kind="replay"` with the
  ReplayReport in extras. Deviations/notes:
  (a) **stats scan bound** — stage breakdown + arrival buckets page
  through `browse` (oldest-first) up to 10k records; a deeper DLQ
  reports `truncated = true` rather than stalling the API;
  (b) `dlq_record_by_id` is a bounded browse scan (the trait has no
  point lookup) — fine at DLQ scale, revisit if DLQs grow;
  (c) **park by selection** resolves All/FirstN against the PENDING
  set (leased records are in a replay's custody);
  (d) purge requires an EXPLICIT selection at the HTTP layer — no
  implicit whole-DLQ default on a destructive op;
  (e) the "live store" exit criterion is covered by the CLI crate's
  `dlq_ops` suite (sqlite family, incl. a real redrive into a
  sqlite target) + a pyo3 empty-store smoke; the HTTP layer is
  pinned with fixtures (21 tests: paging, 4 KB preview truncation,
  download, gating, RunHistory registration, bearer rules);
  (f) `run_dlq_replay`'s options now default `max_attempts` from
  the pipeline's configured `dlq_max_attempts` at the ops layer.

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
- **Landed 2026-07-08**: `#/streams/{name}/dlq` screen
  (StreamDlq.svelte) with depth card, per-interval arrival
  sparkline, stage chips, paged record table + payload drawer
  (≤4 KB preview + raw download), Replay all/selected/first-N,
  Park, Purge behind confirm modals reusing the Pipelines modal
  pattern (purge-all requires typing `purge`); Rewind control with
  timestamp/offset picker. mock-api.mjs fixtures for every
  endpoint; `npm run build` clean; QA script at
  `docs/qa/DLQ_REPLAY_UI_QA.md` (first file in docs/qa/).
  Deviations/notes:
  (a) the repo has **no stream-detail screen** — the DLQ depth
  badge lives on the streaming job card (Jobs tab; fetched
  per-stream after the list loads, hidden when the stream isn't
  registered with the API process) and the Rewind control lives on
  the DLQ screen;
  (b) the stateful typed confirmation is **progressive**: the UI
  first submits without `confirm_state_reset`; the server's typed
  400 triggers the type-the-stream-name step, then the retry
  carries the flag (statefulness isn't known client-side up
  front). The mock API emulates the 400 so the flow is
  exercisable offline;
  (c) Runs-tab `replay` badge links back to the stream's DLQ
  screen (RunRecord summaries already carry `kind`).

## Phase 6 — Docs, changelog, gate sweep

- README + docs page (error-policy → DLQ → replay → rewind lifecycle),
  CHANGELOG entry, `docs/EMAT_FLAGS.md` if any env knobs were added.
- Full gates: fmt/clippy(±features)/workspace tests/parity suites/
  tpch_validate; streaming happy-path perf pin; coverage on new modules.
- **Landed 2026-07-08**: `docs/DLQ_REPLAY_GUIDE.md` (lifecycle
  guide, in the mkdocs nav), README DLQ bullet + streaming example
  updated, CHANGELOG Unreleased entry (feature + the #539 seek
  fix). No env knobs were added, so `docs/EMAT_FLAGS.md` is
  untouched. Gate sweep results are recorded in the phase commits;
  the pandas/pyarrow × `_core` interpreter-shutdown segfault that
  blocks a single-process local pytest run on macOS is
  PRE-EXISTING (reproduced on an untouched main checkout) and
  tracked separately.

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
