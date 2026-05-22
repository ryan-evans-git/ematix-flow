//! ematix-flow Rust core.
//!
//! See `docs/PRD.md` and `docs/IMPLEMENTATION_PLAN.md` for the design.

pub mod backend;
pub mod cdc;
pub mod ddl;
pub mod delta_backend;
// Σ.A2 PR 1: SQL dialect translator. Namespaced (`ematix_flow_core::
// dialect::Dialect`) so it doesn't collide with `backend::Dialect`,
// which names backend kinds (Postgres / MySQL / Kafka / …) rather
// than SQL surfaces.
pub mod dialect;
pub mod duckdb_backend;
// Σ.E2: row-group-parallel parquet TableProvider that bypasses
// DataFusion's `ParquetExec → DataSourceExec → RepartitionExec` stack.
// See `fast_parquet.rs` for the day-2/day-3 probe results that
// motivated this.
pub mod fast_parquet;
// Bridge from ematix-parquet kernel output (Vec<T>, Vec<u8> bitmap,
// etc.) to Arrow arrays. Foundation for replacing parquet-rs under
// FastParquetExec. See module docstring for the integration plan.
pub mod ematix_parquet_bridge;
// Phase 2 of the ematix-parquet integration — `TableProvider` +
// `ExecutionPlan` that scan a parquet file via the bridge instead
// of parquet-rs. Supports primitive columns only; non-primitive
// callers continue using `FastParquetTableProvider`.
pub mod ematix_fast_parquet;
// Σ.E5.1: streaming Arrow `RecordBatch` reader over ematix-parquet.
// Emits 65 536-row batches sliced from a per-row-group dict-aware
// decode. Replaces the whole-RG emission of `ematix_parquet_bridge`
// for the Q1-shape workload. See
// `docs/PHASE_SIGMA_E5_PARQUET_RS_ELIMINATION.md` §E5.1.
pub mod emat_arrow_reader;
// Σ.E5.6 scaffold: intra-RG page-streaming column decoders. The
// trait + first concrete impl (Float64) — not yet wired into
// EmatixFastParquetExec. Closes the architectural first-batch latency
// gap diagnosed in Q19. See task #503 and
// `project_q19_root_cause_orchestration.md`.
pub mod emat_page_stream;
// Σ.G.2 first slice: `AggregateSpec` trait. The Q1Spec/Q6Spec impls
// that originally lived here were retired in Σ.G.2f.3 cleanup
// (commit 476d65d). The trait survives as the abstraction shared by
// `FilterSumSpec` and `FilterMultiAggSpec`.
pub mod fused_aggregate;
// Σ.G.2 third slice: the generic operator that the trait was built
// to enable. Wraps any `AggregateSpec` impl as a DataFusion
// `ExecutionPlan`. Reachable via direct construction or via the
// `InjectFusedQ{6,1}Rule` family which build it from raw SQL plans
// (Σ.G.3d retired the intermediate FusedFilterSumExec lift).
pub mod fused_aggregate_exec;
// Σ.G.2e-1: `FilterSumSpec` — first runtime-configured `AggregateSpec`
// impl. JIT-only single-bucket SUM over an AND-chain of (col ⊕ literal)
// clauses. Substrate for `InjectFilterSumRule` (Σ.G.2e-2), which lifts
// any SUM-over-Filter SQL into `FusedAggregateExec<FilterSumSpec>`.
pub mod fused_aggregate_filter_sum;
// Σ.G.2e-2: `InjectFilterSumRule` — SQL-pattern matcher that
// constructs `FusedAggregateExec<FilterSumSpec>` from a SUM-over-Filter
// plan shape. First slice recognises the canonical Q6 shape; future
// slices broaden the matcher to arbitrary column sets.
pub mod fused_aggregate_filter_sum_rule;
// Σ.G.2f.1 (task #480): `FilterMultiAggSpec` — runtime-configured
// group-by + multi-aggregate spec with no data-specific JIT baking.
// Substrate for the Photon-style template-specialization follow-up in
// .2 that retires `InjectFusedQ1Rule` + `Q1Spec`.
pub mod fused_aggregate_filter_multi_agg;
// Σ.G.2f.3 (task #481): `InjectFilterMultiAggRule` — SQL-pattern
// physical optimizer rule that rewrites a multi-aggregate + group-by
// plan shape into a single `FusedAggregateExec<FilterMultiAggSpec>`.
// Group-by-aware counterpart to `InjectFilterSumRule`.
pub mod fused_aggregate_filter_multi_agg_rule;
// Task #610 follow-up: `DedupeAggregateForFloatDeterminism` —
// PhysicalOptimizerRule that detects structurally-identical
// AggregateExec subtrees and forces both to mode=Single to eliminate
// f64 SUM non-determinism (TPC-H Q15's CTE-duplicate shape). SCAFFOLD
// ONLY — not yet wired into any SessionState. See module docs.
pub mod dedupe_aggregate_rule;
// Σ.H.1d.1 (task #552): scaffolding for the parallel numeric-keyed
// `FilterMultiAggSpec`. Hosts `NumericKeyKind` and (future)
// `FilterMultiAggSpecNumeric`. Kept disjoint from the existing
// string-keyed `fused_aggregate_filter_multi_agg` so the Dict /
// Utf8View hot-path codegen is unaffected. See
// `docs/PHASE_SIGMA_H1D_DIAGNOSIS_AND_DESIGN.md` for the binary-cost
// vs exec-cost decomposition that motivated this split.
pub mod fused_aggregate_filter_multi_agg_numeric;
// Σ.D3: cranelift-JIT'd inner loop for the unified fused-aggregate
// operator. Hosts `FusedFilterAggSpec` IR + `FusedFilterAggJit`
// runtime that `FilterSumSpec` and `FilterMultiAggSpec` build on.
pub mod fused_jit;
// fused_jit_rule retired in Σ.F.2 (2026-05-20). The shared
// `AggregateShapeConfig` walker it hosted is replaced by the
// declarative shape catalog (`shape_catalog`); both fused-aggregate
// injection rules now express their patterns as `Shape`s and call
// `Shape::try_match` directly. The remaining JIT-emission substrate
// stayed in `fused_jit.rs`.
// Σ.E3a: `DictFilterExec` — IN-list filter on Dictionary(UInt32, Utf8)
// columns by code membership (no string compare in the hot loop).
// Photon's #1 string-workload pattern; landing the operator standalone
// here, with the matching `PhysicalOptimizerRule` to follow.
pub mod dict_filter;
// Σ.E3a (companion rule): `EnableDictFilterRule` rewrites in-plan
// FilterExec(InList on Dictionary(UInt32, Utf8)) to DictFilterExec.
// Speculative — non-matching plans pass through unchanged.
pub mod dict_filter_rule;
// Σ.E3b.1: `DictGroupCountExec` — single-pass COUNT(*) GROUP BY over
// a Dictionary(UInt32, Utf8|Utf8View) column. Maintains a per-batch
// dict-code → slot lookup so the hot row loop is one array index, one
// counter bump — no hash, no string compare.
pub mod dict_aggregate;
// Σ.E3b.2: `EnableDictGroupCountRule` — PhysicalOptimizerRule that
// rewrites `AggregateExec(FinalPartitioned) → RepartitionExec →
// AggregateExec(Partial)` on a dict group column + COUNT(*) into a
// DictGroupCountExec. Speculative; non-matching plans pass through.
pub mod dict_aggregate_rule;
// Σ.K.2 (2026-05-21): query-shape-aware dict-arrival routing. Pre-
// planning helper that walks a LogicalPlan and decides per-table
// whether `with_dict_preservation(true)` is a net win, based on the
// query's downstream operators (GROUP BY shape vs LIKE / multi-agg).
// Lives outside the physical optimiser to avoid the LLVM-codegen
// perturbation cost recorded in optimizer-codegen-sensitivity.
pub mod dict_routing;
// Σ.L.2 (2026-05-21): adaptive runtime workload feedback. Persists
// per-shape probe outcomes + per-query observability (selectivity,
// hash collision rate) to a SQLite file (~/.ematix/workload.db by
// default). The optimiser consults this before speculating; after
// ~100 queries on a workload, decisions converge and the probe runs
// at most once per new shape.
pub mod workload_log;
// Σ.L.3 retired (2026-05-22): adaptive predicate reordering experiment
// rejected by triangulation bench. Kernel-bench 0.64× on isolated Q06
// didn't translate to wall-time at SF=1 or SF=10. See
// `project_sigma_l3c_deleted.md` for the full bench-driven rationale.
// Σ.L.4 (2026-05-21): cross-query scan-cache framing. When two
// concurrent queries scan the same (file, row_group, projection,
// filter), they share a single decoded handle instead of decoding
// twice. Scaffolding lands here; full scan-path integration is a
// follow-up bite.
pub mod scan_cache;
// Σ.L.5 (2026-05-21): workload-aware parquet write tuning. Reads
// Σ.L.2's workload.db, emits recommendations for row-group size,
// sort keys, bloom columns, dict columns, and compression codec.
// Scaffolding lands here; CLI + actual rewrite are follow-ups.
pub mod write_tuner;
// Σ.M (2026-05-21): SQL → PhysicalPlan cache. Keyed by canonicalised
// SQL string + schema-version. Photon does this for prepared statements
// only; we do it for ad-hoc SQL. Massive win on benchmark/dashboard
// workloads where queries repeat.
pub mod plan_cache;
// Σ.J.2 (2026-05-21): cross-stage bloom filter for distributed joins.
// Build-side worker computes a bloom over join keys; ships it via
// Flight metadata to probe-side workers; probe-side skips rows before
// shipping. Cuts cross-stage row volume by 10-100× on selective joins.
pub mod bloom;
// Σ.N (2026-05-21): Robin Hood vectorized hash table for aggregates.
// Open-addressing with Robin Hood eviction (probe-distance equalisation).
// Photon's signature operator; nobody ships this in OSS DataFusion.
// Tonight ships the data structure + tests; operator integration is a
// follow-up bite to avoid the optimizer-codegen-sensitivity tax.
pub mod robin_hood_agg;
// Σ.N.d (2026-05-22): planner rule that auto-installs
// RobinHoodAggregateExec on AggregateExec(Final/Partial) sub-plans
// with a single Int64 GROUP BY + COUNT(*). NOT installed in default
// chain — opt-in only via install_robin_hood_rule(state_builder) to
// avoid the codegen tax recorded in optimizer-codegen-sensitivity.
pub mod robin_hood_agg_rule;
// Σ.J.2.b.vi (2026-05-22): per-request PhysicalOptimizerRule that
// walks the plan, finds EmatixFastParquetExec scans whose
// `<table>.<col>` uuid matches a build-side bloom in ContextBlooms,
// and wraps them in BloomFilterExec. Closes the probe-side half of
// the distributed bloom flow; build-side emitter is Σ.J.2.b.vii.
pub mod context_bloom_rule;
// Σ.O (2026-05-21): in-memory LRU cache of decoded RecordBatch by
// (file_path, row_group_idx, projection_hash). Avoids re-decoding
// the same parquet column chunks across queries in the same session.
// Multi-query workloads (dashboards, repeated benchmarks) reuse
// decoded batches instead of re-decoding.
pub mod parquet_decode_cache;
pub mod hash;
pub mod join;
// Σ.E5 (2026-05-19): Photon-style vectorized LIKE pattern matcher
// using memchr::memmem. Used by emat's BridgeFilter for byte_array
// substring predicates (and available as a standalone utility).
pub mod glue_schema_registry;
pub mod kafka_backend;
pub mod kinesis_backend;
pub mod like_matcher;
pub mod meta;
pub mod mysql_backend;
// Task #556 / #559: callback registry that lets Rust backends invoke
// embedding-language code (Python via PyO3) without taking a hard
// PyO3 dependency. Glue Schema Registry decode + future warehouse
// pipeline execution both route through this.
pub mod objectstore_backend;
pub mod pg;
pub mod py_callbacks;
// Task #559 final slice: Rust-side invoker for warehouse-pipeline
// callbacks. Uses py_callbacks to dispatch a @ematix.warehouse_pipeline
// function by name without subprocess overhead.
pub mod warehouse_executor;
// Task #481: one-call setup helpers (`with_optimizer_rules` +
// `register_dict_aware_parquet`) that activate the dict-aware fast
// path without callers having to memorise the rule chain + the
// `with_dict_preservation(true)` opt-in.
pub mod preset;
pub mod pubsub_backend;
pub mod rabbitmq_backend;
pub mod session_blob;
// Σ.F (task #543): declarative shape catalog substrate. The matcher
// AST + named-capture try_match. Replaces the per-rule plan walkers
// (dict_filter_rule / dict_aggregate_rule / the two
// fused_aggregate_filter_*_rule) once the catalog dispatcher lands.
pub mod shape_catalog;
pub mod spec;
pub mod sqlite_backend;
pub mod state_size;
pub mod state_store;
pub mod strategy;
pub mod streaming;
// Σ.E4a: hardware topology discovery. Single source of truth for
// NUMA node count / core count; consumed by Σ.E4b (NUMA-local alloc)
// and Σ.E4c (node-partitioned hash execs). Today's implementation is
// a single-node stub — hwloc2 backend lands in Σ.E4a.2.
pub mod topology;
pub mod transform;
pub mod types;
pub mod windowed;

// Test-only TPC-H mini-fixture generator. Builds a tiny synthetic
// dataset in a process-scoped tempdir on first use so the existing
// integration tests can run in CI without `examples/tpch/data/sf1`
// populated. See `test_support.rs` for the resolution-order contract
// and the SF=1 cardinality gate.
#[cfg(test)]
pub(crate) mod test_support;

pub use backend::{Backend, BackendError, Dialect, ObjectFormat, PostgresBackend, StreamingKind};
pub use delta_backend::DeltaBackend;
pub use duckdb_backend::DuckDBBackend;
pub use kafka_backend::KafkaBackend;
pub use kinesis_backend::KinesisBackend;
pub use mysql_backend::MySQLBackend;
pub use objectstore_backend::ObjectStoreBackend;
pub use pubsub_backend::PubSubBackend;
pub use rabbitmq_backend::RabbitMQBackend;
pub use spec::{Mode, PipelineSpec, SourceSpec, SpecError, TargetSpec, normalize_json};
pub use sqlite_backend::SQLiteBackend;
pub use types::{ColumnSpec, ColumnType, TableSpec, normalize_table_json};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
