# PRD — DLQ management + stream replayability (with web-UI support)

- **Date:** 2026-07-04
- **Status:** Draft (decisions below approved by owner 2026-07-04)
- **Owner:** Ryan Evans
- **Related:** `docs/PHASE_39_5_SESSIONS.md` (StateStore), `docs/PHASE_39_4_WINDOWS.md`
  (watermarks/late data), `python/ematix_flow/web/` (UI/API), CHANGELOG `[0.9.0]`+
  streaming phases.

## Problem

Streaming pipelines can already *emit* to a dead-letter queue — transform
errors (`transform_on_error = "dlq"`) and sink-write failures route the
original-format batches to a `dead_letter_topic`, and offsets only commit
after the DLQ ack. But that is where the story ends:

- **The DLQ is a write-only black hole.** Nothing in the product consumes,
  counts, inspects, or drains it. Users cannot see *that* messages were
  dead-lettered (beyond a Prometheus counter), *why* they failed, or *what*
  the payloads were.
- **No replay.** There is no way to re-process dead-lettered messages after
  fixing the cause (bad schema, sink outage, transform bug), and no way to
  rewind a live stream to an earlier offset/timestamp for a broader replay.
  The only recovery today is out-of-band Kafka tooling.
- **DLQ records carry no error metadata.** Batches are routed
  format-preserved (good for replay symmetry) but with no record of the
  failure reason, stage, source offsets, timestamp, or attempt count —
  nothing a UI or an operator can triage from.
- **Kafka-only.** `dead_letter_topic` requires a Kafka source (the DLQ
  producer is the source backend's producer). Kinesis / RabbitMQ / Pub/Sub /
  file sources have no DLQ at all (`transform_on_error = "dlq"` logs a
  warning and effectively drops).
- **No UI.** The web UI (`flow web`) shows streaming throughput and offers
  `resume_from_watermark`, but has no DLQ visibility or replay controls.

From the user's POV: "my stream said `errors: 47` last night — what were
they, why did they fail, and how do I get those 47 rows into my warehouse
now that I fixed the schema?" has no in-product answer.

## Users

- **Primary:** data/platform engineers operating ematix streaming pipelines
  (the people who run `flow web` and get paged when `errors_total` climbs).
- **Secondary:** pipeline authors (the `@ematix.job` writers) configuring
  error policy per stream; support/on-call rotations triaging incidents.

## Goals

1. **See** — every streaming pipeline surfaces its DLQ state in the UI:
   depth, arrival rate, per-stage breakdown (transform vs write vs late-data
   when it lands), and a browsable list of dead-lettered records with
   payload preview + full failure metadata (error, stage, source, offsets,
   event/failure timestamps, attempt count).
2. **Replay (redrive)** — one-click "replay all / selected / first N" that
   re-processes DLQ records **through the pipeline's own transform + sinks**
   (current code and config) as a bounded, observable **replay run** (shows
   up in the Runs tab like any run). Records that fail again return to the
   DLQ with `attempt + 1`; a `max_attempts` guard (default 3) parks
   poison messages instead of looping.
3. **Rewind** — a stream detail control to rewind the pipeline's sources to
   an offset or timestamp. For stateful pipelines (windows/joins/sessions),
   rewind **atomically resets the state store** with the seek, behind an
   explicit typed confirmation in the UI ("this resets window/session
   state"). No silently wrong aggregates, ever.
4. **Every backend** — DLQ management and replay work uniformly across ALL
   streaming sources via a `DeadLetterStore` abstraction: a Kafka-topic
   implementation (upgrading today's behavior with metadata) AND a portable
   table-backed implementation (SQL via the state-store family) that serves
   Kinesis / RabbitMQ / Pub/Sub / file sources — same trait, same UI.
5. **Ease of use** — sane defaults: a pipeline with `on_error = "dlq"` and
   no explicit topic gets the portable table DLQ automatically; the UI
   requires zero extra configuration beyond what `flow web` needs today.

## Non-goals

- **Exactly-once replay.** Semantics stay at-least-once, same as the
  pipeline itself (idempotent-target assumption unchanged). Replay may
  duplicate rows at the sink; that is documented, not "fixed".
- **Versioned state snapshots / time-travel.** Rewind resets state; it does
  not restore historical state as-of the rewind point. (Deferred; see
  out-of-scope.)
- **Broker-level DLQ management** (RabbitMQ `x-dead-letter-exchange`,
  Pub/Sub `dead_letter_policy`): we do not manage broker-native DLQs; the
  app-level store is the managed surface. Broker DLQs remain pass-through
  config.
- **Cross-pipeline DLQ routing** (one pipeline's DLQ feeding another
  pipeline). A DLQ belongs to exactly one pipeline.
- **Editing payloads before replay.** Records replay as-received. (Deferred.)
- **`late_data = "dlq"` for windows/sessions** stays reserved (separate
  write path per PHASE_39_5); the DeadLetterStore trait must be designed so
  it can plug in later without schema change.
- **Multi-user RBAC** on replay/rewind actions (UI auth remains the existing
  all-or-nothing bearer token; per-action roles arrive with Phase 4c auth).

## Success metrics

- A dead-lettered record is **visible in the UI within 30 s** of the failure
  (with error, stage, offsets, payload preview) — measured by integration
  test.
- **Redrive round-trip works on every backend family** in CI: Kafka-topic
  store AND table store (SQLite + Postgres) each pass a
  fail → dead-letter → fix → replay → sink-row-present integration test.
- **Poison-message guard:** a permanently failing record ends parked at
  `max_attempts` with status visible in the UI — never an infinite loop
  (integration test).
- **Rewind correctness:** rewinding a stateful (windowed) pipeline to T
  yields byte-identical sink output to a fresh pipeline started at T
  (integration test) — proving the atomic state-reset semantics.
- **No regression** to existing streaming throughput/latency benchmarks when
  DLQ is idle (the store must add zero cost on the happy path).
- Operator can go from "errors climbing" to "replayed and green" without
  leaving the UI (manual QA scenario).

## Constraints

- **Decided (owner, 2026-07-04):** redrive = reprocess-through-pipeline
  (not re-publish-to-source); rewind = reset-state-with-confirm; scope =
  all backends via abstraction (not Kafka-first).
- **At-least-once everywhere:** DLQ append must ack before source offsets
  commit (already true for the Kafka path — preserve under the trait).
- **Format preservation:** payloads stay in their source format
  (JSON↔JSON, RawBytes↔RawBytes) so replay is symmetric; failure metadata
  travels OUT OF BAND of the payload (Kafka headers / table columns), never
  by envelope-wrapping.
- **Happy-path cost:** zero added per-batch work when nothing dead-letters.
- **Storage reuse:** the table-backed store rides the existing state-store
  connection family (Postgres; SQLite for local dev) — no new infra
  dependency for the default experience.
- **UI conventions:** Svelte SPA + FastAPI `/api/*` patterns
  (`web-ui/src/routes/`, `server.py` action-gating, `api.js` client,
  mock-api fixtures for dev); actions gated server-side like
  `restart_from_step`.
- **TDD discipline** (house rule): every phase lands tests-first; CI gates
  (fmt/clippy/coverage/parity) unchanged.
- **Rust core / Python surface split:** policy + stores + replay engine in
  `ematix-flow-core`; orchestration + API in the Python layer mirrors how
  runs/history work today.

## Design sketch (for the plan, not binding detail)

- `trait DeadLetterStore`: `append(records + DlqMeta)`, `depth()`,
  `browse(page)`, `take_for_replay(selection, lease)`, `ack_replayed()`,
  `park()`, `purge(selection)`. Implementations: `KafkaTopicDlq`
  (payload as-is + metadata in headers; browse via bounded peek consumer),
  `TableDlq` (state-store SQL family; payload bytes + metadata columns —
  the universal default).
- `DlqMeta`: pipeline, stage (`transform|write|late_data`), error string,
  source id, source offset bytes, event_ts, failed_at, attempt,
  payload_format.
- **Replay run**: a bounded `StreamingPipeline` execution whose source is
  the DeadLetterStore selection (lease-based so concurrent replays don't
  double-take), same transform/targets, `on_error` forced to `dlq` with
  attempt+1 and `max_attempts` park. Registered in RunHistory as
  `kind=replay` (visible in Runs tab, linkable from the DLQ screen).
- **Rewind**: `POST /api/streams/{name}/rewind {to: offset|timestamp, confirm_state_reset: bool}` →
  pause → seek via backend (`seek_to` / timestamp resolution) → if stateful:
  state-store clear + offset write in ONE transaction → resume.
- **UI**: new `#/streams/{name}/dlq` screen (depth card, rate sparkline,
  browsable table with payload drawer, replay/park/purge actions with
  modals) + rewind control on the stream detail; Runs tab shows replay runs
  with a `replay` badge.

## Open questions

- DLQ retention: unlimited vs TTL/size cap with parked-overflow policy —
  default proposal: unbounded table with a `purge` action + documented
  retention query; revisit after usage. `TODO: confirm with owner before GA.`
- Payload preview size cap in the UI (proposal: 4 KB truncated preview,
  full download endpoint).
- Should replay runs honor the pipeline's schedule/trigger gates or always
  run immediately? (Proposal: immediate, manual-only.)

## Out-of-scope ideas captured during discussion

- Versioned state snapshots enabling true time-travel rewind without reset.
- Payload editing / transform-patching before replay.
- Automatic redrive policies (e.g., auto-replay on sink recovery, backoff
  schedules) — phase-2 candidate once manual redrive is trusted.
- `late_data = "dlq"` wiring for windows/sessions (trait-ready, separate
  write path).
- Cross-backend DLQ migration tooling.
