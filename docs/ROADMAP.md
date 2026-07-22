# ematix-flow — Roadmap

What's been shipped, what's left, and the priority order. Compiled
from the deferred sections of every phase plan in `docs/`.

> **Current focus (2026-07-18): the v2 program.** The clean-room native
> engine is merged to `v2`; the active work is the DataFusion-elimination
> cutover, native distribution, and the DataFrame API. Live plan +
> sequencing: **[`plans/V2_PROGRAM.md`](plans/V2_PROGRAM.md)** (target:
> [`V2_TARGET.md`](V2_TARGET.md)). The DF-era `V2_SQL_SURFACE_GAPS.md` and
> `PHASE_V2_S1/2/3` docs are superseded by that program.

## Status snapshot (2026-05-10; v0.2.0 cut)

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
| Δ | **Phase Δ — CDC source mode.** Apply Debezium / Maxwell / custom-envelope CDC events to a target table with per-op semantics + idempotency-by-PK + schema-evolution detection. Per-batch transactional with prepared-statement reuse; `delete_mode = "soft"` flips a column instead of DELETE. Five Prometheus counters under `pipeline=<name>`. `examples/cdc-debezium/` docker-compose stack. | `docs/PHASE_DELTA_CDC_PLAN.md`, `docs/USER_GUIDE.md` |
| Δ.X1 / Δ.X2 | **CDC executors across backends.** Single-MERGE-per-batch on Delta Lake; native per-op executors on MySQL, SQLite, DuckDB with shared idempotency gate + soft-delete + schema-evolution semantics. Δ.X1.1 `_cdc_last_ts` hidden-column path for between-batch idempotency on Delta. | `docs/PHASE_DELTA_CDC_PLAN.md` § "Phase Δ extensions" |
| Π.4b–Π.5 | **Object-store as streaming source** (`last_seen_object_key` high-water-mark; UUIDv7-ordered keys; per-format decoders shared with the read-prefix path). **Π.5** unified streaming-API knobs: scalar `udfs=` + aggregate `aggregate_udfs=` on `run_streaming_pipeline`; inline-credential deprecation warnings on the legacy TOML loader. | `docs/PHASE_39_5_SESSIONS.md`, `docs/UNIFIED_PIPELINE_API.md` |
| UDFs | **Python `@udf` (scalar) + `@udaf` (aggregate).** PyArrow zero-copy per-batch dispatch; scalar wraps a callable, aggregate wraps an Accumulator class with `update_batch`/`merge_batch`/`evaluate`/`state`. Pure-Rust escape hatch when GIL contention dominates. Threaded through `run_streaming_pipeline` to `LazySqlTransform`. | `README.md`, `docs/USER_GUIDE.md` |
| Σ.A1 | **TPC-H single-node baseline + audit.** 22-query Apache Spark TPC-H suite plans + executes cleanly through DataFusion 53 with zero SQL-surface gaps. PySpark head-to-head at SF=1 (5.87× geomean) + SF=10 (3.3× geomean). | `docs/BENCHMARKS.md`, `docs/PHASE_SIGMA_PLAN.md` |
| Σ.A2 | **SQL dialect translator.** `[transform] dialect = "datafusion"\|"spark"\|"duckdb"` selects an `sqlparser-rs`-based AST rewriter that emits DataFusion-compatible SQL. Spark surface: function-name remap + `LATERAL VIEW EXPLODE` → wrap-in-subquery `unnest`. DuckDB surface: function-name remap. **103/103 Spark TPC-DS PASS**, **103/103 DuckDB TPC-DS PASS**. | `crates/ematix-flow-core/src/dialect/`, `docs/PHASE_SIGMA_PLAN.md` |
| Σ.B–Σ.C | **Distributed batch SQL.** `ematix-flow-distributed` crate + `flow-worker` binary; `[transform] engine = "distributed"` selects a peer-distributed `DataFusion` execution via Arrow Flight on top of `datafusion-distributed`. Full 22-query TPC-H clean at SF=1; cluster image ≤150 MB; mTLS shipped. | `docs/PHASE_SIGMA_PLAN.md` |

**Tests at the time of writing (v0.2.0 cut):** 583 core lib + 156 CLI lib + 12 distributed + 403 default Python (≈196 testcontainers-gated `@pytest.mark.integration`) + ~80 Rust testcontainers `--ignored`. All green on macOS aarch64. clippy + fmt clean on stable Rust.

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
20. ~~**SR basic-auth Rust plumbing.**~~ **Shipped.** `_kafka_payload_and_sr_lines` in `python/ematix_flow/streaming.py` emits `schema_registry_basic_auth_{user,password}` for both source and target Kafka blocks; `KafkaConnection` redacts the password in `repr()`. CLI side: `apply_sr_basic_auth` in `crates/ematix-flow-cli/src/lib.rs` wires both fields through `KafkaBackend::with_schema_registry_basic_auth(...)` → `SrSettings::set_basic_authorization` for every Avro / Protobuf encode/decode against the registry. Half-set (one without the other) rejected at Python emit + re-checked in the CLI helper. Coverage: `tests/python/test_phase_pi1_schema_registry.py`.
21. ~~**Kafka SASL / MSK-IAM through the streaming TOML emitter.**~~ **Shipped.** `_kafka_auth_lines` in `python/ematix_flow/streaming.py` emits `sasl_plain_{username,password}`, `sasl_scram_{username,password,mechanism}`, and `msk_iam_region` for both source and target Kafka blocks with the same mutual-exclusion check as the Rust runner. CLI side: `apply_kafka_auth` in `crates/ematix-flow-cli/src/lib.rs` dispatches to `with_sasl_plain` / `with_sasl_scram` / `with_msk_iam`; partial PLAIN / SCRAM combinations are rejected at config-load. Coverage: `tests/python/test_phase_pi_kafka_sasl.py`.

34. **Phase Δ — CDC source mode (Debezium / Maxwell / custom envelope).** Apply change-data-capture events from a Kafka topic to a target table with proper per-op semantics: insert / upsert on `c`+`r`, update by PK on `u`, delete (or soft-delete column flip) on `d`. Configurable via both `[transform.cdc]` TOML **and** the typed `CDC(envelope="debezium")` dataclass on `@ematix.streaming_pipeline` — peer-equivalent paths into the same `CdcConfig` struct + Rust execution path. Tombstone records skipped; idempotency-by-key via the existing Postgres / in-memory `StateStore` (last-seen `source.ts_ms` per PK, so Kafka redelivery doesn't double-apply); best-effort schema-evolution detection (warn + skip unknown columns; ALTER TABLE on the target as a follow-up). Closes the "is this Debezium-friendly?" gap that today's hand-rolled approach (Kafka source + SQL transform splitting `before`/`after` + custom post-load DELETEs) makes awkward + footgunny. Effort: ~3–4 wk. Plan: [`docs/PHASE_DELTA_CDC_PLAN.md`](PHASE_DELTA_CDC_PLAN.md).

### P3 — performance / ops

22. ~~**Columnar buffer storage for joins.**~~ **Shipped (Arc-clone variant).** Profiled first via two new criterion benches — `join_ingest_1000_rows_no_match` (pure buffer, no emit) and `join_steady_state_1000x1000_match_rate` (1000-row left + 1000-row right, ~10× match rate per key, full ingest+emit+materialise). Steady-state path was the hot one: 12× the no-match path because every match did two deep `Vec<ScalarValue>` clones into an `EmittedRow`. The full RecordBatch-reference shape originally specced would have required a state-store wire-format migration (postcard buffer blob is the snapshot format); landed the cheaper Arc-clone variant instead — `BufferedRow.values: Arc<Vec<ScalarValue>>` + `EmittedRow.{left,right}: Option<Arc<Vec<ScalarValue>>>`, with serde's `rc` feature so `Arc<T>` round-trips byte-identically as `T` (no migration). Result: steady-state bench **1.68 ms → 651 µs (−61%)**; no-match bench unchanged (no clones to elide). Source: `docs/PHASE_39_5B_JOINS.md`; benches `crates/ematix-flow-core/benches/transform.rs`.
23. ~~**Hot-key state-size warning thresholds.**~~ **Shipped (window side).** New `crates/ematix-flow-core/src/state_size.rs` exposes `project_window_state_bytes(&WindowConfig)` and `warn_if_exceeds_default(label, projected)`. The estimator is the design-doc-cited "~200 B/state_blob" baseline plus per-feature adders (group-by column-name length, session metadata). Default threshold 1 GiB, override via `EMATIX_FLOW_STATE_SIZE_WARN_BYTES` (set to `0` to silence). Wired into `PipelineCliConfig::from_toml_str` — same shape as the `warn_on_stateful_in_memory_store` hook. Joins also need this but currently lack a declared per-pipeline max-key projection input (`JoinConfig` has no `max_groups`); deferred pending design — see `docs/PHASE_39_5B_JOINS.md` open question. Source: `docs/PHASE_39_5_SESSIONS.md`.
24. ~~**Schema-change handling for refreshing lookups + windows.**~~ **Shipped (lookup side).** `DataFusionTransform::refresh_lookup` now does graceful re-plan on drift: tentatively swaps in the new lookup `MemTable`, re-compiles the SQL via `ctx.sql(...)`, compares the new logical-plan output schema to the cached one — accepts when output shape is unchanged (extra unused columns, join-key type widenings DataFusion can absorb) and rolls back on mismatch (e.g. `SELECT u.*` with new columns, type change on a SELECTed column). The pipeline keeps serving against the OLD lookup contents until the supervisor restarts on the rolled-back drift. `lookup_schemas` moved behind a `Mutex<HashMap>` so the accepted-drift path can update tracked shapes without bouncing the transform. Coverage: 4 new tests (extra-unused-column accepted, output-shape-changing drift rejected with rollback verification, join-key type widening accepted, SELECTed-column type change rejected with rollback verification). Windows already tolerate additive schema changes (per-batch column lookup by name in `windowed.rs`); no further work needed there. Source: `docs/SQL_TRANSFORMS_PLAN.md` open question 4.
25. ~~**Streaming Parquet writes.**~~ **Shipped.** `write_parquet_at_path` now feeds `AsyncArrowWriter` straight into `object_store::buffered::BufWriter` — under the 10 MiB capacity threshold the writer issues one PUT, over it switches to the object store's multipart-upload path with bounded RSS regardless of total file size. `parquet_round_trips_when_payload_exceeds_bufwriter_capacity` (200k rows × ~64 B/row, uncompressed to defeat dict encoding) sanity-checks that the on-disk file lands >10 MiB so the multipart branch is actually exercised by CI. Source: `crates/ematix-flow-core/src/objectstore_backend.rs`.

### P4 — open design questions (no code yet)

26. ~~**Kafka rebalance + `seek_to`.**~~ **Shipped.** Replaced the prior "manual `assign + Offset::Offset(...)` at consumer-acquire time" path with `subscribe()` + `EmatixKafkaContext::post_rebalance`. The recovered per-partition map lives on `KafkaBackend.seek_map: Arc<Mutex<HashMap>>`, cloned into every context built via `build_context()`. On each `Rebalance::Assign(tpl)`, the callback iterates assigned partitions and calls `consumer.seek(...)` for any whose offset is in the map (consumed entries are removed so a later rebalance reads broker-stored committed offsets). Single-worker pipelines are unaffected (initial assign-all rebalance triggers the seek; behavior identical to the old path); multi-worker setups (Σ.D distributed streaming, when it lands) inherit correct partition-reassignment behavior for free. Coverage: 3 new unit tests (`seek_to_populates_shared_seek_map_for_post_rebalance`, `seek_to_replaces_prior_seek_map`, `build_context_clones_share_seek_map_with_backend`) plus updated `load_state_applies_seek_to_on_matching_source` integration assertion. Source: `docs/PHASE_39_5_SESSIONS.md` open question (now marked resolved).
27. **Windows interaction:** revisit the lookup-schema-drift handling once windows compose with refreshing lookups in production. Source: `docs/SQL_TRANSFORMS_PLAN.md`.
28. ~~**Object-store as a streaming source.**~~ **Shipped.** `ObjectStoreBackend.read_arrow_stream` now tracks a `last_seen_object_key` high-water mark (lexicographic over object keys; UUIDv7 file naming gives time-ordered keys). Successive calls return only files whose key sorts strictly greater than the mark; restarts restore the mark via `seek_to(...)` against a `StateStore` snapshot. Format-specific per-file decoders (`decode_parquet_file`, `decode_csv_file`, `decode_jsonl_file`, `decode_orc_file`) extracted from the prior monolithic `read_*_under_prefix` helpers and shared with the new `streaming_read_after` listing+filter+sort path. Wire format: `{ "v": 1, "last_object_key": "..." }` JSON. CLI `SourceConfig` gained `ObjectStoreLocal` / `ObjectStoreS3` variants; `build_one_source` dispatches accordingly. `validate_sources_support_seek_to` accepts both. Inline-credential scanner flags S3 source-side keys. Python `_source_fields` emits source TOML for `ObjectStoreLocalConnection` / `ObjectStoreS3Connection`. Resolves the P1.7b "object-store seek_to" follow-up that was deferred pending this trigger. Coverage: 5 new tests in core (incremental read, seek_to round-trip, snapshot-is-none-until-consumption, garbage bytes rejected, supports_seek_to=true) + 4 new tests in CLI (TOML parsing for both variants, seek_to validator, inline-credential flagging). Source: `docs/PHASE_39_5_SESSIONS.md`.

### P5 — Phase Σ: distributed compute (scale SQL like PySpark, footprint like ematix-flow)

Goal: stand alongside PySpark on batch SQL throughput at <20% of PySpark's image size, then extend to distributed streaming. Built on DataFusion (already a workspace dep); no JVM, no shuffle service to install. A→C is a coherent batch-distributed delivery (~7–13 weeks). D is bigger and gated on a build-vs-adopt spike. Per-PR plan + open design questions in [`docs/PHASE_SIGMA_PLAN.md`](PHASE_SIGMA_PLAN.md).

29. ~~**Σ.A1 — single-node DataFusion baseline + TPC-H harness.**~~ **Shipped 2026-05-05** across four sub-PRs documented in [`docs/BENCHMARKS.md`](BENCHMARKS.md). PR 1: `examples/tpch/{generate.rs,queries/q01..q22.sql}` plus `tpch_smoke` integration test. PR 2: `crates/ematix-flow-core/benches/tpch.rs` with criterion benches for Q1/Q3/Q6/Q19 at SF=1 (M3 Pro baseline committed to BENCHMARKS.md "Σ.A1 PR 2 baseline (2026-05-05)"). PR 3: audit no-op — all four queries plan + execute cleanly through DataFusion 53.1 with **zero SQL-surface gaps**. The Σ.C extension (2026-05-05) hardened this to all 22 official TPC-H queries — `tpch_22_audit` integration test passes 22/22 with no surface gaps. PR 4: PySpark head-to-head — DataFusion wins all four reps by **~4.3× geomean** (Q3: 6.8× faster), comfortably clearing the ≥1.5× acceptance gate. **`EXPLAIN` + `EXPLAIN ANALYZE`** round-trip through `DataFusionTransform` cleanly (locked in by `explain_select_round_trips` / `explain_analyze_select_round_trips` smoke tests in `transform.rs`). Window functions + complex-type literals: covered transitively by the 22-query suite (Q15 / Q22 hit the relevant shapes; all pass). Source: [`docs/PHASE_SIGMA_PLAN.md` Σ.A1](PHASE_SIGMA_PLAN.md#σa1--single-node-datafusion-baseline--tpc-h-harness).

30. ~~**Σ.A2 — SQL dialect selector + translation layer.**~~ **Shipped.** `[transform] dialect = "datafusion" | "spark" | "duckdb"` lands on `PipelineCliConfig` and threads through `streaming_config_with_lookups_*` into `LazySqlTransform` via `crates/ematix-flow-core/src/dialect/{mod,spark,duckdb}.rs`. Translator built on `sqlparser-rs` (already in DataFusion's tree): parse source dialect → `VisitMut` AST walk → emit DataFusion SQL. Spark surface: function-name remap (`ifnull`/`nvl` → `coalesce`, `lcase`/`ucase` → `lower`/`upper`, `instr` → `strpos`, `current_timestamp` → `now`, `array` → `make_array`), `expr(x)` no-op strip, `LATERAL VIEW EXPLODE` → wrap-in-subquery `unnest(...)` (the `CROSS JOIN UNNEST` form trips an `OuterReferenceColumn` planner error in DataFusion 53 — see `examples/df_unnest_probe.rs`). DuckDB surface: function-name remap (`list_value` → `make_array`); both dialects share the closer-to-ANSI Postgres surface so the rewrite table is intentionally narrow. Acceptance gate (≥80% Spark TPC-DS PASS): cleared comfortably — `cargo run --release -p ematix-flow-core --example tpcds_dialect_audit -- spark` reports **103/103 PASS** on the official Apache Spark TPC-DS query set; same audit on DuckDB also **103/103 PASS**. Coverage: `crates/ematix-flow-core/tests/dialect_{spark,duckdb}{,_e2e}.rs` (873 LOC across 4 test files) — pure-AST unit tests + DataFusion-end-to-end planner-roundtrip tests. Postgres/Trino remain on-demand.

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
| Σ.E3 | 1 dev | ~5 wk | Dict-aware execution end-to-end (see `docs/PHASE_SIGMA_E3_DICT_AWARE.md`) — Photon's #1 string-workload lever, scoped 2026-05-15 |
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
gated on the remaining items in **§Decisions and open questions**
below. The first round of decisions has landed (D1: append-only
`RunLog` with local-file + shared backends, lease-based cluster
recovery for idempotent tasks); other questions still open. Items
sized assuming the remaining opens land at "smallest reasonable
choice" (Alerter trait, manual-restart path (a), pipeline-level
granularity in v1).

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

#### Decisions and open questions

The first round of decisions has landed in this draft; remaining
items are still open. Recorded inline below so the trail of "why
the design landed where it did" stays in one place.

- **Ω.D1 — Run-history persistence: append-only `RunLog` with two
  backends (DECIDED).** Mental model is a simple state file: every
  task transition (`started`, `succeeded`, `failed`) is appended as
  a record. On startup, the runtime scans the log; any entry with a
  `started` event but no matching termination is presumed failed
  (the worker process didn't get a chance to write the
  termination). New `RunLog` trait with two reference impls:

  - **`LocalFileRunLog`** — JSONL appended to a local file. Single-
    pod / dev. Crash-recovery does the "presume in-progress = failed"
    sweep on startup. Works on any filesystem; no shared
    dependency. Default for single-process deployments.

  - **`SharedRunLog`** — backed by the existing `StateStore`
    (Postgres reference impl already shipped). Required for
    cluster-aware recovery (next bullet). Same append-only mental
    model, just sitting in a shared table instead of a local file.

  Cluster-aware recovery is the differentiator. When multiple pods
  run the same `flow` daemon and one crashes mid-task, the peers
  must notice and take over rather than mark the task failed. This
  needs a **lease + heartbeat** mechanism on top of `SharedRunLog`:

  - A worker claims a task by writing `started by <worker-id>` with
    a lease `expires_at = now() + lease_ttl`.
  - The worker heartbeats by extending `expires_at` periodically
    (default ~1/3 of `lease_ttl`).
  - Peers see expired leases for tasks still in `started` state and
    re-claim them, writing `resumed by <new-worker-id>`. The task's
    final outcome attributes to whichever worker writes the
    termination record.
  - Hard requirement: only auto-resume tasks declared
    `idempotent=true`. Non-idempotent tasks (e.g. "send an alert
    email") get marked failed instead of resumed. Default depends
    on strategy — `merge` / `scd2` / streaming are safe to
    auto-mark idempotent; `append` is not.

  Reuse the existing Σ.B peer mesh as the discovery substrate
  (workers already know about each other for distributed batch
  SQL); the lease state lives in `SharedRunLog`, not in mesh
  metadata.

  Sub-questions remaining inside this decision:

  - **D1.a — Lease TTL default.** Tradeoff: shorter = faster
    recovery, but heartbeat overhead + risk of legitimate-but-slow
    tasks losing their lease to a peer. Common range: 30s–5min.
    Probably configurable per pipeline with a 60s global default.
  - **D1.b — RunLog rotation / retention.** Append-only files grow.
    Need a knob (`max_age_days` / `max_records` / size-based
    rotation). Retention is also what powers the UI's "history"
    panel — too short and the UI is empty after a week.
  - **D1.c — `LocalFileRunLog` format.** JSONL is simplest + greppable
    + diffable. SQLite is queryable but adds a dep. Lean JSONL.
  - **D1.d — Idempotency declaration surface.** Pipeline-level
    `idempotent=True` decorator field, or auto-derive from the
    declared mode (`merge`/`scd2` → True, `append` → False)? Probably
    auto-derive with an explicit override knob.

- **Ω.Q2 — Restart-from-failure (partly answered by D1).** The Ω.D1
  decision lands the substrate (cluster-aware lease-based resumption
  for idempotent tasks). What's still open is **manual** restart-
  from-failure: a user explicitly asks the runtime to replay a run
  that already terminated as `failed`. For streaming pipelines this
  works for free (offsets + StateStore + watermarks resume the
  consumer where it left off). For batch:

  - **Idempotent strategies (`merge` / `scd2`):** "manual restart"
    is just "run again" — same SQL, same target, same keys. No
    extra infra needed.
  - **Non-idempotent strategies (`append`):** restart-from-failure
    is genuinely hard — re-running appends would duplicate rows.
    Two paths: (a) refuse manual restart for `append` pipelines,
    point users at a `truncate + append` recipe, or (b) land step-
    level batch checkpointing (write intermediate Arrow batches to
    object store, fingerprint inputs, resume from the last
    successful checkpoint).

  Open: take path (a) for v1 (consistent with the auto-recovery
  decision in D1, ships in days) or land (b) as part of this phase
  (~3–4 wk additional)? Recommendation: (a) for v1, (b) as a
  follow-up phase with its own design doc.

- **Ω.Q3 — Alert transport surface.** On run failure (and optionally
  success), send an alert via a configurable transport. Candidate
  transports: Slack webhook, generic HTTP webhook, email (SMTP),
  PagerDuty Events API. Open: ship transports in-tree (each adds a
  dep + maintenance surface) or expose an `Alerter` trait + a
  reference webhook impl + a docs page on how to plug in others?
  Recommendation leans trait — keeps the dependency tree minimal
  and matches the `Backend` / `StateStore` extensibility pattern
  already in the codebase.

- **Ω.Q4 — Re-alert on stuck failures (unblocked by D1).** Now that
  D1 lands a `RunLog` regardless of cluster mode, "we already
  alerted at T0" can be tracked in the same log as another event
  type (`alerted`, with the alert config snapshot). Re-alert
  becomes: "every `realert_after` interval, scan for runs in
  `failed` state whose most-recent `alerted` event is older than
  the threshold, and re-fire." No separate alert-state surface
  needed. Effort: <1 wk on top of D1 + Q3.

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

#### Sizing summary (post-D1; remaining open questions in smallest-choice form)

| Sub-phase | Effort | Calendar | Validates |
|---|---|---|---|
| Ω.1 | 1 dev | 1–2 wk | DAG-aware scheduling without Airflow |
| Ω.2 | 1 dev | ~1 wk | Declarative retries on batch + streaming |
| Ω.3 | 1 dev | 2–3 wk | Built-in pipeline status UI |
| Ω.D1a | 1 dev | ~1 wk | `LocalFileRunLog` (JSONL append + presume-failed-on-restart sweep) |
| Ω.D1b | 1 dev | 2–3 wk | `SharedRunLog` + lease/heartbeat for cluster failover |
| Ω.Q2 (manual restart) | 1 dev | <1 wk | Manual restart-from-failure for idempotent strategies (path (a)) |
| Ω.Q3 | 1 dev | 1 wk | Alerter trait + reference webhook impl |
| Ω.Q4 | 1 dev | <1 wk | Re-alert on stuck failures (rides on D1) |
| Ω.Q5 | 1 dev | 3–4 wk | Step-level decomposition (if landed in v1) |
| **Single-pod v1** (Ω.1–3 + D1a + Q2(a) + Q3 + Q4) | | **~7–9 wk** | Full orchestration + UI + history + alerts on a single host |
| **Cluster v1** (above + D1b) | | **~10–12 wk** | Above + cluster-aware lease-based recovery |
| **+ step-level UI** (Q5) | | **~13–16 wk** | All the above + per-step status / per-step retry hooks |
| **+ batch step-checkpointing** (Q2 path b) | | **~17–20 wk** | Restart-from-failure works for `append` too |

Recommendation: ship the **single-pod v1** as the first cut (Ω.1–3
+ `LocalFileRunLog` + path (a) restart + Alerter trait + re-alert).
That's the standalone orchestrator-lite users can adopt today. The
cluster bits (`SharedRunLog` + lease) follow as Ω.D1b once the
single-pod surface is exercised in production. Q5 (step-level
decomposition) and Q2 path (b) (batch step checkpointing) are
follow-up phases — neither is on the critical path for the asked-
for feature.

---

## What this roadmap intentionally doesn't cover

- **Spark interop polish.** Already shipped via `[spark]` extra; future
  Spark-specific ergonomics live in their own follow-up.
- **Per-strategy DDL planner extensions.** Documented in
  `docs/MULTI_BACKEND_PLAN.md`; landing as needed per backend.
- **CDC / change-data-capture sources.** Out of scope. Use a Kafka
  Connect / Debezium-style upstream pipeline producing to Kafka, then
  consume with this framework.

---

## Post-2026-05-17 AWS campaign — next chapter

The 2026-05-17 SF=1+SF=10 validation campaign produced concrete
evidence (15/22 SF=1 wins vs DuckDB+Polars; Π.15 parallel scaling
3.72× on x86 c7i; SF=10 regression vs DuckDB needs investigation) +
exposed a set of harness bugs that lost the SF=10 BENCHMARKS.md.
That run informs the next several phases.

### Must-fix before the next campaign

- [docs/CAMPAIGN_HARNESS_FIXES.md](CAMPAIGN_HARNESS_FIXES.md) — 15
  concrete bugs/optimisations (stage ordering, BENCHMARKS-per-SF
  upload, GHA prebuilt target, et al). One infra PR ≈ 1 wk.

### New engine phases (drafted 2026-05-17, ready to prioritise)

| Phase | Doc | Effort | Status | What it unlocks |
|---|---|---|---|---|
| **Σ.G** Generic fused aggregate | [PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE.md](PHASE_SIGMA_G_GENERIC_FUSED_AGGREGATE.md) | ~3 wk | Drafted | Retires per-query `InjectFusedQN` rules; "no longer per-query bandaid" |
| **Φ** Vectorised aggregate kernels | [PHASE_PHI_VECTORISED_AGG_KERNELS.md](PHASE_PHI_VECTORISED_AGG_KERNELS.md) | ~9 wk | Drafted | Real wins on non-TPC-H aggregates; backs Σ.G.4 |
| **Π** Aggregate (and join) spilling | [PHASE_PI_AGGREGATE_SPILLING.md](PHASE_PI_AGGREGATE_SPILLING.md) | ~9 wk | Stub | Production-readiness at SF=100+ |
| **Ψ.1** Column stats + cardinality estimator | [PHASE_PSI_COLUMN_STATS_AND_CBO.md](PHASE_PSI_COLUMN_STATS_AND_CBO.md) | ~4 wk | Stub | Σ.G.4's cost-driven dispatch becomes useful |
| **Ψ.2** Full CBO | (same doc) | ~3-6 mo | Stub (defer-decision) | Ship to users with arbitrary schemas |
| **Z** Distributed TPC-H vs Spark | [DISTRIBUTED_TPCH_BENCHMARK_PLAN.md](DISTRIBUTED_TPCH_BENCHMARK_PLAN.md) | ~1 wk α+β impl, ~$2.50 to run | Drafted (incl. `flow worker` CLI spec) | TPC-H fan-out vs Spark/Trino/Daft on EKS |

### Recommended order

1. **Land PR #87** (Σ.E3b emat dict-preserved + parquet v0.9.2 bump) — open, awaiting CI.
2. **Land CAMPAIGN_HARNESS_FIXES PR** — bundles the 15 fixes; trustworthy future campaign runs.
3. **Σ.G** (3 wk) — hygiene + honest "no longer bandaid" claim.
4. **Φ.1-Φ.3 + Π + Ψ.1 in parallel** (~12 wk, probably two engineers):
   - Φ for non-TPC-H aggregate wins.
   - Π for SF=100 production-readiness.
   - Ψ.1 to feed Σ.G.4.
5. **Decision point at T+15 wk:** commit to Ψ.2 (full CBO), or focus on Φ.4-5 (kernel breadth)?
6. **Phase Z distributed** can run alongside any of #3-5 once the `flow worker` CLI subcommand ships.

### Open prioritisation questions (for the post-campaign session)

1. **Π vs Φ ordering** — Π unblocks SF=100 (real users). Φ unblocks non-TPC-H (better positioning). Which first?
2. **Whether Ψ.2 ships at all** — if Σ.G.4 + Ψ.1 give "good enough" planning for target customers, Ψ.2's 3-6 months might be better spent on Φ.4+Φ.5.
3. **Distributed campaign timing** — run α+β as soon as `flow worker` lands, or wait until Φ ships so we benchmark with the best kernels?
4. **Generic vectorised kernels at the *parquet decode* layer too** — should ematix-parquet absorb Φ.5's codegen path, or keep decode + aggregate kernels separate?
