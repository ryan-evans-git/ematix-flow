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

### P3 — performance / ops

22. **Columnar buffer storage for joins.** Today's per-row owned scalars work but are heavy at high throughput; switch to single-row `RecordBatch` references with row indices. Profile first. Source: `docs/PHASE_39_5B_JOINS.md`.
23. **Hot-key state-size warning thresholds.** Per design-doc open question; warn at config-load when `max_groups_per_window × estimated_blob_size > X`. Same hooks for sessions + joins. Source: `docs/PHASE_39_5_SESSIONS.md`.
24. **Schema-change handling for refreshing lookups + windows.** Today both surface errors to the supervisor; a more graceful path detects drift + re-plans eagerly. Source: `docs/SQL_TRANSFORMS_PLAN.md` open question 4.
25. **Streaming Parquet writes.** Today's `write_arrow_stream` path buffers every batch in memory before emitting one Parquet file (via `AsyncArrowWriter`), which costs O(file-size) RSS for >GB inputs to S3. `object_store::buffered::BufWriter` would let the writer stream out in 5-MiB multipart-upload chunks. Documented in `objectstore_backend.rs::write_parquet_at_path` since Phase 34a.

### P4 — open design questions (no code yet)

26. **Kafka rebalance + `seek_to`.** Mid-stream rebalance triggers `assign+seek` again with a new partition set; current offsets loaded from `StateStore` may not cover new partitions. Source: `docs/PHASE_39_5_SESSIONS.md` open question.
27. **Windows interaction:** revisit the lookup-schema-drift handling once windows compose with refreshing lookups in production. Source: `docs/SQL_TRANSFORMS_PLAN.md`.
28. **Object-store as a streaming source.** The `ObjectStoreBackend` is batch-only via `read_arrow_stream` today; not exposed through `StreamingPipeline`'s consumer loop nor in CLI's `SourceConfig`. Adding a streaming-source variant would unlock new patterns (incremental Parquet ingest from S3 prefix) and would also be the trigger to land per-file-offset `seek_to` (deferred from P1.7b).

---

## What this roadmap intentionally doesn't cover

- **Spark interop polish.** Already shipped via `[spark]` extra; future
  Spark-specific ergonomics live in their own follow-up.
- **Per-strategy DDL planner extensions.** Documented in
  `docs/MULTI_BACKEND_PLAN.md`; landing as needed per backend.
- **CDC / change-data-capture sources.** Out of scope. Use a Kafka
  Connect / Debezium-style upstream pipeline producing to Kafka, then
  consume with this framework.
