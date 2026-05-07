# ematix-flow — Roadmap

What's been shipped, what's left, and the priority order. Compiled
from the deferred sections of every phase plan in `docs/`.

## Status snapshot (2026-05; Π.1 / Π.3 / Π.1.4 audit pass)

**Shipped:**

| Phase block | What | Where |
|---|---|---|
| 0–14 | v0.1 declarative Postgres (decorator API, all 4 strategies, watermarks, run history, scheduling, CLI). | `docs/PRD.md`, `docs/IMPLEMENTATION_PLAN.md` |
| 15–20 | ML feature store (`@ematix.feature_view`, PIT, online MV, training-set builder). | `docs/ML_FEATURE_STORE_PLAN.md` |
| 21–25 | Ergonomics decorator overhaul (`@ematix.table`, `pk()`, `natural_key()`, multi-target). | `docs/ERGONOMICS_PLAN.md` |
| 26–28 | Normalization markers (`trim`, `lower`, `parse_timestamp`, …) + post-load transforms + DataFrame interop. | `docs/NORMALIZATION_TRANSFORMS_PLAN.md` |
| 30 | Backend trait + Postgres reference impl (Arrow IO contract). | `docs/MULTI_BACKEND_PLAN.md` |
| 31–33 | MySQL / SQLite / DuckDB target backends. | `docs/MULTI_BACKEND_PLAN.md` |
| 34 | Object-store targets: Parquet / CSV / ORC / JSONL on local FS + S3. | `docs/MULTI_BACKEND_PLAN.md` |
| 35a–f | Delta Lake target (local + S3) with DataFusion-backed MERGE. | `docs/MULTI_BACKEND_PLAN.md` |
| 36 | Kafka source + target with SASL/PLAIN, SCRAM, mTLS, MSK IAM, Schema-Registry-aware Avro/Protobuf, EOS via transactions. | `docs/MULTI_BACKEND_PLAN.md` |
| 37 | RabbitMQ, GCP Pub/Sub, AWS Kinesis sources + targets. | `docs/MULTI_BACKEND_PLAN.md` |
| 38 | `flow consume` CLI binary + Prometheus `/metrics` + `--restart-on-error` supervisor. | (CLI section of `MULTI_BACKEND_PLAN.md`) |
| Py.1–Py.6 | Python streaming bindings: `run_pipeline`, pyclass wrappers, `ArrowBatchIter`. | (CLI section) |
| 39.1 | `BatchTransform` trait + `LazySqlTransform` (per-batch DataFusion SQL filter / project / cast / lookup-join). | `docs/SQL_TRANSFORMS_PLAN.md` |
| 39.2 | Static lookup tables (`[transform.lookups.<name>]`). | `docs/SQL_TRANSFORMS_PLAN.md` |
| 39.3 | Refreshing lookups (`refresh_interval_ms` per lookup). | `docs/SQL_TRANSFORMS_PLAN.md` |
| 39.4 | Tumbling + hopping windows; 9 aggregators (incl. HLL+ `count_distinct`); `late_data = "drop"` and `"reopen"`; idle-tick emit; multi-source watermark. | `docs/PHASE_39_4_WINDOWS.md` |
| 39.5a | Session windows; durable `StateStore` (Postgres + in-memory) with postcard wire format; per-emit atomic state+offsets commit; `seek_to` on Kafka. | `docs/PHASE_39_5_SESSIONS.md` |
| 39.5b | Keyed time-windowed stream-stream inner join; reuses 39.5a `StateStore`; per-source `BatchContext::source_id` dispatch. | `docs/PHASE_39_5B_JOINS.md` |
| Π.1 | `SchemaRegistryConnection` typed connection; `KafkaConnection.schema_registry=` (instance or registered name); Kafka `payload_format` + `schema_registry_url` plumbed through the streaming TOML emitter (was silently dropped before); `Watermark(lateness_ms=, source_idleness_ms=)` typed-Python knob + `[watermark]` TOML block; `transform_on_error="fail"\|"drop"\|"dlq"` exposed on `run_streaming_pipeline` / `@ematix.streaming_pipeline`. | `docs/UNIFIED_PIPELINE_API.md` |
| Π.3 | `flow consume --module my_pipelines <name>` Python-loading CLI shape; `@ematix.streaming_pipeline` now registers into a process-global name-keyed registry; `render_streaming_pipeline_toml(name)` shared between the runner and the CLI's render path; `flow consume-list --module M` companion. Implemented in the Python `flow` entry point (no PyO3 added to the Rust CLI binary). | `docs/UNIFIED_PIPELINE_API.md` |
| Π.1.4 | Object-store per-format write options end-to-end: `ParquetCompression` enum (`uncompressed`/`snappy`/`gzip`/`zstd`) + `ObjectWriteOptions` struct + `ObjectStoreBackend::with_write_options` builder; CSV `delimiter` + `header` honored on write; CLI TOML fields on `ObjectStoreLocal` / `ObjectStoreS3`; typed-Python `Target(parquet_compression=, csv_delimiter=, csv_header=)` with shape-correctness checks at the boundary. | (CLI section of `MULTI_BACKEND_PLAN.md`) |

**Tests at the time of writing:** 459 core lib + 108 CLI lib + 376 default Python (≈196 testcontainers-gated `@pytest.mark.integration`) + ~80 Rust testcontainers `--ignored`. All green on macOS aarch64. clippy + fmt clean on stable Rust.

---

## What's left

Grouped by priority. Everything below is documented in a phase plan;
this is the consolidated punch list.

### P0 — release / publish

1. ~~Wheel-build CI matrix.~~ **Shipped.** `.github/workflows/release.yml` builds Linux x86_64 (manylinux2014) + macOS aarch64 / x86_64 wheels for Python 3.10–3.13.
2. ~~PyPI trusted publishing.~~ **Shipped.** Same workflow, `publish` job uses `pypa/gh-action-pypi-publish` with the `pypi` GitHub environment + `id-token: write`. One-time PyPI configuration captured in `docs/RELEASE.md`.
3. ~~mkdocs site.~~ **Shipped.** `mkdocs.yml` + Material theme + `.github/workflows/docs.yml` deploys to GitHub Pages on push to main. Strict-mode build clean.
4. ~~`examples/` directory.~~ **Shipped.** `examples/01_append.py` through `examples/08_stream_join.toml` covering every strategy + every Phase 39 shape. `examples/docker-compose.yml` brings up local Kafka + Postgres. CLI tests parse + cross-validate every example TOML.
5. ~~v0.1.0 tag + announcement.~~ **Ready to cut.** `CHANGELOG.md` populated; `docs/RELEASE.md` has the human checklist (PyPI trusted-publisher one-time setup, pre-tag verification, post-publish smoke test). The `git tag v0.1.0 && git push origin v0.1.0` is the user's call.

### P1 — completeness gaps in shipped phases

6. ~~`late_data = "dlq"` for windowed transforms.~~ **Shipped.** `LateDataPolicy::Dlq` stashes past-budget rows into a per-transform buffer; pipeline drains via the new `BatchTransform::take_dlq_rows()` trait method and routes through the existing `dead_letter_topic` Kafka producer. Information-loss caveat: rows are post-SQL-prestage, not raw upstream bytes — documented.
7. ~~P1.7a + P1.7b shipped.~~ **Shipped.** Pub/Sub + RabbitMQ accepted via broker-tracked offsets (manual-ack stream, `seek_to` is a no-op). Kinesis now accepted via per-shard sequence numbers — `KinesisBackend` overrides `seek_to` / `offset_snapshot` with a JSON `KinesisOffsetSnapshotV1 { shards: BTreeMap<String, String> }` wire format; resume plugs into the existing `AfterSequenceNumber` iterator path with no read-path changes. Object stores stay out of scope: `ObjectStoreBackend` isn't a streaming source today (no consumer-loop in `StreamingPipeline`, not exposed in CLI's `SourceConfig`), so there's no surface for `seek_to` until/unless object-store is added as a streaming source variant.
8. ~~Periodic dirty-only checkpoint ticker.~~ **Shipped.** `StreamingPipeline::run` spawns a `tokio::time::interval` task at `[state_store] checkpoint_interval_ms` cadence (default 60s) that drains transform diff + offset snapshots and commits to the store, independently of emit activity. `MissedTickBehavior::Delay` so a backed-up pipeline doesn't fire a tick burst.
9. ~~`count_distinct` in stateful sessions.~~ **Partially shipped.** Exact-mode (`mode = "exact"` with `max_distinct_values_per_group`) is HashSet-backed and round-trips through postcard cleanly. Approximate-mode HLL+ stays unsupported because `HyperLogLogPlus`'s register state is in private fields — needs an upstream change or a fork. CLI rejects the approximate combination at config-load with a message pointing at exact mode.
10. ~~`InMemoryStateStore` loud-warning at config-load.~~ **Shipped.** `tracing::warn!` fires when `kind = "in_memory"` is paired with a session window or stream-stream join. Skipped when no stateful transform is configured so test streams stay clean.
11. ~~End-to-end Postgres + Kafka crash-recovery test for joins.~~ **Shipped.** `join_pipeline_crash_recovers_committed_state` in `tests/integration_pg.rs` exercises the production `pipeline.commit_state(store)` and `pipeline.load_state(store)` paths. **Caught a real bug** during writing: `WindowedAggregateTransform`'s `take_state_commit` / `recover_state` were inherent methods, not trait overrides — production session/join pipelines were silently no-op'ing both commit and recovery. Existing session e2e test passed only because it called inherent methods on the concrete type. Fix moved them into `impl BatchTransform`; new tests exercise the `Arc<dyn BatchTransform>` dispatch path.

### P2 — feature extensions (post-v0.1)

12. ~~Outer joins (LEFT / RIGHT / FULL).~~ **Shipped.** `JoinKind::LeftOuter` / `RightOuter` / `FullOuter` variants. `BufferedRow.matched: bool` flips on first match; `evict_state` returns orphan emits at retention deadline. `on_idle_tick` now produces batches when outer-join orphans evict. `build_emit_batch` materializes `None` sides as NULLs (the schema was already nullable=true).
13. ~~`late_data = "reopen"` for joins.~~ **Shipped.** `JoinLateDataPolicy::Reopen { allowed_lateness_ms }` extends the per-side retention horizon. Late rows admitted within budget can match opposite-side buffer; each (L, R) pair still emits exactly once (the duplicate-emit concern in the design doc is actually a state-store-recovery + Kafka-redelivery case present under any policy — idempotent targets handle at-least-once via the join's downstream dedup key).
14. ~~Asymmetric time windows for joins.~~ **Shipped.** `min_delta_ms` / `max_delta_ms` override the symmetric `time_window_ms`. Per-side retention horizons computed from the signed bounds — left rows evict at `wm > L + max + lateness`, right rows at `wm > R - min + lateness`.
15. ~~Per-row error handling for transforms.~~ **Shipped (batch-granularity).** `[transform] on_error = "fail" | "drop" | "dlq"` policy at the pipeline level. DataFusion executes per-batch, so the granularity is per-batch, not per-row — documented. `Dlq` re-uses the existing `dead_letter_topic` plumbing. Source: `docs/SQL_TRANSFORMS_PLAN.md` open question 2.
16. ~~Lookup schema drift on refresh.~~ **Shipped.** `DataFusionTransform` now captures per-lookup schemas at construction and `refresh_lookup` cross-checks each refresh against the originally-registered shape. Drift fails fast with a clear error pointing at the schema diff, instead of silently swapping in a bad MemTable that errors mid-batch on the next `transform()` call.
17. **Iceberg target backend.** Deferred since `iceberg-rust` 0.x pins arrow 57 vs the workspace's arrow 58. Re-check on `iceberg-rust` 0.6+. Source: `docs/ICEBERG_PLAN.md`.
18. ~~**Unified pipeline API — phases 1 / Π.1 / Π.3 / Π.1.4.**~~ **Shipped.** P2.18: `Source` + `sources=[...]` typed-Python multi-source. P2.18 `Join`: `kind` / `min_delta_ms` / `max_delta_ms` / `late_data="reopen"` + `allowed_lateness_ms`. **Π.1**: `SchemaRegistryConnection` typed connection; Kafka `payload_format` + `schema_registry_url` plumbed through the streaming TOML emitter; `Watermark(...)` knob + `[watermark]` TOML block; `transform_on_error` exposed on the typed-Python surface. **Π.3**: `flow consume --module M name` + `flow consume-list --module M` Python-loading CLI in the Python entry point. **Π.1.4**: object-store per-format options (Parquet compression + CSV delimiter / header) on `Target`. Source: `docs/UNIFIED_PIPELINE_API.md`.
19. ~~**Π.5: deprecate the inline-credentials TOML loader.**~~ **Shipped.** New `PipelineCliConfig::inline_credential_findings()` walks every source/target variant and reports human-readable findings for inline credentials (`postgres://user:pw@...`, RabbitMQ `amqp_url` with userinfo, Kinesis / S3 `access_key_id`/`secret_access_key`). The `flow` binary's `run_consume_cmd` calls it on parse + emits `tracing::warn!` lines with the migration pointer (`flow consume --module M name` + `@ematix.connection`). Silenced by `EMATIX_FLOW_NO_DEPRECATION=1` for CI runs that intentionally use the legacy form. Removal scheduled one minor release after the warning lands.
20. **SR basic-auth Rust plumbing.** Π.1 follow-up: `SchemaRegistryConnection.basic_auth_user / basic_auth_password` are accepted on the dataclass + redact in `repr()` but the streaming TOML emitter raises `NotImplementedError` if they reach the emit step. Wire `SrSettings::new_basic_auth(url, user, password)` through `KafkaBackend::with_schema_registry_basic_auth(...)` and corresponding TOML / Python emit lines.
21. **Kafka SASL / MSK-IAM through the streaming TOML emitter.** `KafkaConnection` dataclass already carries `sasl_plain_*`, `sasl_scram_*`, and `msk_iam_region` fields — currently only consumed by the direct PyO3 `KafkaBackend` constructor, not emitted by `_source_fields` / `_target_fields` for streaming pipelines. Same shape as the Schema-Registry plumbing that Π.1 closed. Hand-written TOML still works in the meantime.

34. **Phase Δ — CDC source mode (Debezium / Maxwell / custom envelope).** Apply change-data-capture events from a Kafka topic to a target table with proper per-op semantics: insert / upsert on `c`+`r`, update by PK on `u`, delete (or soft-delete column flip) on `d`. Configurable via both `[transform.cdc]` TOML **and** the typed `CDC(envelope="debezium")` dataclass on `@ematix.streaming_pipeline` — peer-equivalent paths into the same `CdcConfig` struct + Rust execution path. Tombstone records skipped; idempotency-by-key via the existing Postgres / in-memory `StateStore` (last-seen `source.ts_ms` per PK, so Kafka redelivery doesn't double-apply); best-effort schema-evolution detection (warn + skip unknown columns; ALTER TABLE on the target as a follow-up). Closes the "is this Debezium-friendly?" gap that today's hand-rolled approach (Kafka source + SQL transform splitting `before`/`after` + custom post-load DELETEs) makes awkward + footgunny. Effort: ~3–4 wk. Plan: [`docs/PHASE_DELTA_CDC_PLAN.md`](PHASE_DELTA_CDC_PLAN.md).

### P3 — performance / ops

22. **Columnar buffer storage for joins.** Today's per-row owned scalars work but are heavy at high throughput; switch to single-row `RecordBatch` references with row indices. Profile first. Source: `docs/PHASE_39_5B_JOINS.md`.
23. **Hot-key state-size warning thresholds.** Per design-doc open question; warn at config-load when `max_groups_per_window × estimated_blob_size > X`. Same hooks for sessions + joins. Source: `docs/PHASE_39_5_SESSIONS.md`.
24. **Schema-change handling for refreshing lookups + windows.** Today both surface errors to the supervisor; a more graceful path detects drift + re-plans eagerly. Source: `docs/SQL_TRANSFORMS_PLAN.md` open question 4.
25. **Streaming Parquet writes.** Today's `write_arrow_stream` path buffers every batch in memory before emitting one Parquet file (via `AsyncArrowWriter`), which costs O(file-size) RSS for >GB inputs to S3. `object_store::buffered::BufWriter` would let the writer stream out in 5-MiB multipart-upload chunks. Documented in `objectstore_backend.rs::write_parquet_at_path` since Phase 34a.

### P4 — open design questions (no code yet)

26. **Kafka rebalance + `seek_to`.** Mid-stream rebalance triggers `assign+seek` again with a new partition set; current offsets loaded from `StateStore` may not cover new partitions. Source: `docs/PHASE_39_5_SESSIONS.md` open question.
27. **Windows interaction:** revisit the lookup-schema-drift handling once windows compose with refreshing lookups in production. Source: `docs/SQL_TRANSFORMS_PLAN.md`.
28. **Object-store as a streaming source.** The `ObjectStoreBackend` is batch-only via `read_arrow_stream` today; not exposed through `StreamingPipeline`'s consumer loop nor in CLI's `SourceConfig`. Adding a streaming-source variant would unlock new patterns (incremental Parquet ingest from S3 prefix) and would also be the trigger to land per-file-offset `seek_to` (deferred from P1.7b).

### P5 — Phase Σ: distributed compute (scale SQL like PySpark, footprint like ematix-flow)

Goal: stand alongside PySpark on batch SQL throughput at <20% of PySpark's image size, then extend to distributed streaming. Built on DataFusion (already a workspace dep); no JVM, no shuffle service to install. A→C is a coherent batch-distributed delivery (~7–13 weeks). D is bigger and gated on a build-vs-adopt spike. Per-PR plan + open design questions in [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md).

29. **Σ.A1 — single-node DataFusion baseline + TPC-H harness.** Audit `crates/ematix-flow-core/src/transform.rs` for SQL-surface gaps PySpark users hit (window functions, complex types, `EXPLAIN`/`EXPLAIN ANALYZE`); close must-haves. Wire TPC-H Parquet loaders into `examples/tpch/`. Add `criterion` benches for Q1/Q3/Q6/Q19 at SF=1, committed to `docs/BENCHMARKS.md`. Acceptance: all four queries beat single-node PySpark on the same hardware. Effort: 1–2 wk. Standalone value (better single-node SQL) regardless of B/C/D.

30. **Σ.A2 — SQL dialect selector + translation layer.** User config: `[transform] dialect = "datafusion" | "spark" | "duckdb"` (defaults to `datafusion`, zero-cost passthrough). Translator built on `sqlparser-rs` (already in DataFusion's tree): parse source dialect → AST rewrite → emit DataFusion SQL. Initial scope: function-name remap (`current_timestamp`/`now`, `from_unixtime`, …), `LATERAL VIEW EXPLODE` → `UNNEST`, window-frame syntax, complex-type literals. Diagnostics on unsupported nodes ("function `transform_keys` not yet translated; rewrite as …"). Acceptance: ≥80% of TPC-DS executes under `dialect = "spark"` with documented gaps for the rest. Postgres/Trino dialects on demand. Effort: 2–3 wk. Validates dialect handling on single-node before B distributes it.

31. **Σ.B — distributed-execution backend.** *(Shipped 2026-05-05.)* New `crates/ematix-flow-distributed` library — any ematix-flow process that links it can act as either coordinator or worker (`flow-worker` binary). New `DistributedBackend` peer to in-process DataFusion; selection via `[transform] engine = "datafusion" | "distributed"` + `peers = [...]`. **Connector trait refactor lands here** (one breaking change absorbing all Σ.B + dialect + state-store needs at once — pre-1.0 budget approved). Connectors must serialize over Arrow Flight: connection-pool / SR-cache state instantiated lazily on the executor side from URL config, not shipped. `examples/distributed-cluster/` docker-compose with 3 `flow-worker` peers + parquet volume mount. Originally specced against Apache Ballista; pivoted mid-Σ.B to [`datafusion-distributed`](https://crates.io/crates/datafusion-distributed) when Ballista pinned DataFusion ^52 vs the workspace's DataFusion 53 (rationale + alternatives in `docs/PHASE_SIGMA_B_TRAIT_SPIKE.md`). Acceptance: cluster image ≤150 MB total; full 22-query TPC-H pass at SF=1; mTLS support shipped. Effort: ~6 wk actual.

32. **Σ.C — TPC-H head-to-head vs PySpark.** *(Shipped 2026-05-05.)* DataFusion / ematix-flow-distributed (3 in-process workers) / PySpark `local[*]` on identical M3 Pro / SF=1 (full 22 queries) + SF=10 (representative set). Record wall time + bootstrap CIs; 10 criterion samples + 3 PySpark trials. Output: `docs/BENCHMARKS.md` head-to-head with paste-into-HN TL;DR. Acceptance: DataFusion median ≤ PySpark median across the suite — **cleared comfortably (5.87× geomean at SF=1, 3.3× geomean at SF=10).** Cross-host numbers + the originally-specced `m6i.4xlarge × 4` recipe deferred (no AWS in this project's runway; `infra/` retained for users with cluster access). Effort: ~2 wk actual.

33. **Σ.D — distributed streaming.** *(Spike shipped 2026-05-05; implementation deferred until demand — see `docs/PHASE_SIGMA_D_SPIKE.md`.)* Much bigger than Σ.B — effectively rebuilding Flink in Rust if done from scratch. The week-1 spike evaluated four candidate paths: **Arroyo** (embeddability regressed after the 2025 Cloudflare acquisition), **RisingWave** (sidecar-only via Postgres wire — useful as a future connector backend, not as the engine), **Denormalized** (right architectural shape — embeddable Rust streaming on DataFusion — but pre-1.0 / 0 releases / "actively seeking design partners"), and **DIY on Arrow Flight + the existing `state_store/`** (12–20 weeks of bounded engineering: partitioned-replicated state store, watermark propagation across shuffle, Chandy-Lamport checkpoint coordinator, continuous Arrow Flight shuffle, credit-based backpressure, per-pipeline failure recovery). Recommendation: defer until either a concrete workload requires per-key state larger than one host, Denormalized cuts a 1.0, or someone funds the DIY track. Streaming pipelines in Σ.B stay partition-parallel; Σ.D is what would unlock distributed shuffle for windowed joins / global aggregations across all keys. Effort: 12–20 wk if/when triggered.

**Σ-block sizing summary:**

| Sub-phase | Effort | Calendar | Validates |
|---|---|---|---|
| Σ.A1 | 1 dev | 1–2 wk | DataFusion single-node ≥ PySpark single-node |
| Σ.A2 | 1 dev | 2–3 wk | Spark dialect runs on ematix-flow without rewrites |
| Σ.B | 1 dev | 3–6 wk | Distributed batch SQL works at <150 MB image |
| Σ.C | 1 dev | 1–2 wk | Public-facing benchmark vs PySpark |
| Σ.D | 1 dev | 12–20 wk | Distributed streaming SQL (gated on spike) |
| **A1–C only** | | **~7–13 wk** | Production-ready vs Spark batch |
| **A1–D** | | **~19–33 wk** | Production-ready vs Spark batch + streaming |

Recommendation: ship Σ.A–C first; Σ.D triggers after the batch claim is benchmarked + announced.

### P6 — Phase Ω: orchestration + status UI

Goal: turn ematix-flow from "a library you call from your scheduler"
into a self-hosting orchestrator-lite that handles dependency
ordering, retry policy, and live status visibility — without
bringing in Airflow. The streaming consumer already has a Prometheus
`/metrics` endpoint and `--restart-on-error` supervisor; this phase
generalizes both to batch pipelines and adds a human-facing surface.

Plan + open questions are draft-only at the moment; implementation
gated on resolving the items in **§Open questions** below. Items
sized assuming the open questions land at "smallest reasonable
choice" (in-process state, opt-in persistence reusing `StateStore`,
trait-based alerter).

35. **Ω.1 — Pipeline dependencies (DAG).** Decorator-level
    `depends_on=[upstream_pipeline]` (or `[pipeline.depends_on]` TOML).
    Build a DAG at registration time; reject cycles with a clear error
    pointing at both ends of the cycle. A pipeline's downstream is
    eligible to run only when its declared upstreams' most-recent
    runs succeeded inside a freshness window (`upstream_freshness=`,
    default unbounded). Single-process scope first; cross-process
    dependency negotiation follows in Ω.4 if/when needed. Effort:
    ~1.5 wk.

36. **Ω.2 — Declarative retry policy.** Per-pipeline (and per-step
    once Ω.5 lands) retry config: `retries=N`, `retry_backoff_ms`,
    `retry_max_total_ms`, `retry_on=[ErrorKind, ...]`. Exponential
    backoff with jitter; bounded by both attempt count and total
    wall-time. Generalizes the existing streaming `--restart-on-error`
    supervisor to batch pipelines and exposes the policy
    declaratively rather than as a CLI flag. Effort: ~1 wk.

37. **Ω.3 — Status UI (HTTP server + JSON API).** A built-in server
    on a configurable port (e.g. `flow status --port 8080`) showing
    live pipeline + step status: which pipelines are running /
    queued / blocked-on-dependency / failed; per-step state for the
    active run; last-N runs per pipeline. Same process as the
    runner — no separate daemon. JSON API behind the HTML so external
    dashboards can consume the same data. Defaults to `127.0.0.1`
    bind; explicit `--bind 0.0.0.0` opt-in; no in-tree auth in v1
    (relegate to fronting reverse proxy — same model as the existing
    Prometheus `/metrics`). Effort: ~2 wk.

#### Open questions (gating decisions; no code yet)

- **Ω.Q1 — Run-history persistence (opt-in).** Today's run history
  is per-pipeline in the *target* schema (Postgres only). A unified,
  cross-pipeline "runs" table is a different surface. Choices:
  reuse the existing `StateStore` (Postgres + in-memory backends
  already implemented) and add an `ematix_flow.run_history` table?
  Or add SQLite as a third backend so users without Postgres get
  durability? Retention policy needs a knob (`max_runs` /
  `max_age_days`). Open: opt-in keeps the change non-breaking but
  means the UI's "history" panel is empty by default — is that
  acceptable, or should the UI persist a small in-memory ring
  buffer (last-100) regardless of the persistent backend choice?

- **Ω.Q2 — Restart-from-failure.** Replay a failed run resuming at
  the failed step rather than from the top. Easy mental model, hard
  semantics: requires every step to declare its inputs as a
  deterministic function of upstream outputs (otherwise "restart at
  step 5" can produce different results than "run all 5 steps
  fresh"). For streaming pipelines this is already implicit (offsets
  + StateStore + watermarks). For batch pipelines it needs every
  step to either (a) be idempotent — already true for `merge` /
  `scd2`, *not* true for `append` — or (b) checkpoint intermediate
  Arrow batches to disk. Open: scope restart-from-failure to
  idempotent strategies + streaming, or land step-level batch
  checkpointing as part of this phase?

- **Ω.Q3 — Alert transport surface.** On run failure (and optionally
  success), send an alert via a configurable transport. Candidate
  transports: Slack webhook, generic HTTP webhook, email (SMTP),
  PagerDuty Events API. Open: ship transports in-tree (each adds a
  dep + maintenance surface) or expose an `Alerter` trait + a
  reference webhook impl + a docs page on how to plug in others?
  Recommendation leans trait — keeps the dependency tree minimal
  and matches the `Backend` / `StateStore` extensibility pattern
  already in the codebase.

- **Ω.Q4 — Re-alert on stuck failures.** If a pipeline has been in
  `failed` state for longer than `realert_after=...`, re-fire the
  alert. Coupling: requires the persistent run-history backend
  (Ω.Q1) — there's no place to track "we already alerted at T0"
  without it. Open: do we gate Ω.Q4 entirely on the user opting
  into Ω.Q1, or build a separate small "alert state" table that
  could exist independently of run history?

- **Ω.Q5 — Step-level granularity.** Items 35–37 above are
  pipeline-level. The user-facing pitch ("see the status of
  individual steps of the pipeline") implies step-level too —
  meaning every transform, every emit, every commit ought to be a
  named step the UI can show. That's a meaningful refactor: today's
  pipeline runs are mostly atomic from a status-tracking point of
  view. Open: in v1, expose pipeline-level only and label this as
  follow-up; or land step decomposition + per-step retry / alert
  hooks together? Strong tension between "ship something useful
  fast" and "build the surface users actually asked for."

- **Ω.Q6 — UI-vs-flow-worker overlap.** Phase Σ.B already ships
  `flow-worker` as a peer in the distributed batch mesh. The Ω.3
  status server is a separate concern but shares some plumbing
  (process registry, metrics endpoint). Open: merge the two so
  there's exactly one daemon binary, or keep `flow status` as a
  distinct subcommand?

#### Sizing summary (assuming smallest-choice answers to Q1–Q6)

| Sub-phase | Effort | Calendar | Validates |
|---|---|---|---|
| Ω.1 | 1 dev | 1–2 wk | DAG-aware scheduling without Airflow |
| Ω.2 | 1 dev | ~1 wk | Declarative retries on batch + streaming |
| Ω.3 | 1 dev | 2–3 wk | Built-in pipeline status UI |
| Ω.Q1 | 1 dev | 1–2 wk | Persistent cross-pipeline run history |
| Ω.Q2 | 1 dev | 2–4 wk | Restart-from-failure (size depends on (a) vs (b) above) |
| Ω.Q3 | 1 dev | 1 wk | Alerter trait + reference webhook impl |
| Ω.Q4 | 1 dev | <1 wk | Re-alert on stuck failures (depends on Q1) |
| Ω.Q5 | 1 dev | 3–4 wk | Step-level decomposition (if landed in v1) |
| **Ω.1–3 only** | | **~4–6 wk** | Orchestration + UI without persistence layer |
| **Ω.1–3 + Q1+Q3+Q4** | | **~7–10 wk** | Above + history + alerts + re-alert |
| **Full Ω including Q2 + Q5** | | **~12–18 wk** | Restart-from-failure + step-level UI |

Recommendation: ship **Ω.1 + Ω.2 + Ω.3 + Q3** as the v1 of this
phase (DAG + retries + UI + alerts via Alerter trait). Defer Q1 /
Q2 / Q4 / Q5 to follow-ups; they're all valuable but each adds a
new persistence or refactor surface that's worth its own design
review. Q6 decides itself once Ω.3 starts wiring the status server.

---

## What this roadmap intentionally doesn't cover

- **Spark interop polish.** Already shipped via `[spark]` extra; future
  Spark-specific ergonomics live in their own follow-up.
- **Per-strategy DDL planner extensions.** Documented in
  `docs/MULTI_BACKEND_PLAN.md`; landing as needed per backend.
- **CDC / change-data-capture sources.** Out of scope. Use a Kafka
  Connect / Debezium-style upstream pipeline producing to Kafka, then
  consume with this framework.
