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
// Σ.D1: `FusedFilterSumExec` physical operator for the simple
// `Aggregate(SUM) over Filter(predicate)` plan shape — closes the
// Q6 gap vs Polars (1.0 ms hand-written / 1.9 ms Polars / 5.96 ms
// today's DataFusion). See `fused.rs` header and issue #44.
pub mod fused;
// Σ.G.2 first slice: `AggregateSpec` trait + `Q6Spec` + `Q1Spec`.
// Both per-shape gates pass (#92, #93) — the trait dispatch matches
// hand-written perf within 1 % on synthetic batches.
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
// Σ.D2: `FusedFilterMultiAggExec` — single-pass fused filter +
// multi-aggregate + group-by physical operator. Day-1 prototype
// (`examples/tpch_q1_tune.rs`) showed 3.08 ms on Q1 SF=1 / 14
// threads vs DataFusion's 25.61 ms MemTable / 47.65 ms parquet
// (15.5× faster) and Polars MemTable 35.2 ms (11.4× faster).
// See `fused_multi_agg.rs` header and issue #45.
pub mod fused_multi_agg;
// Σ.D3: cranelift-JIT'd inner loop for the unified fused-aggregate
// operator. See `fused_jit.rs` header and issue #45. Day-1 scaffold:
// JIT'd Q6 predicate evaluator that hits the same kernel shape as
// Σ.D1's hard-coded operator from a data-driven input. The full
// generic IR emitter (any predicate AST, any agg spec, any group-by
// shape) builds on this scaffold.
pub mod fused_jit;
// Σ.D4 + Σ.D5: `FusedPostJoinExec` — single-pass fused aggregate
// over the output of a join. Wraps the Q3/Q5/Q14 day-1 prototype
// kernels (5-8× faster than DataFusion's per-aggregate dispatch on
// the agg step; modest end-to-end shift because these queries are
// JOIN-bound). Substrate for the future cranelift-JIT-generalized
// operator. See `fused_post_join.rs` header and issues #51, #52.
pub mod fused_post_join;
// Σ.D3 phase D: `PhysicalOptimizerRule` that auto-routes hand-coded
// FusedFilterSumExec / FusedFilterMultiAggExec / FusedPostJoinExec(Q14)
// instances to their Cranelift-JIT'd variants. Walks the physical plan
// tree with `transform_up` and swaps each match in place. Full SQL-
// pattern detection (extracting predicate constants from arbitrary
// PhysicalExpr ASTs to inject a fused exec where none was constructed)
// is documented as a follow-up in the module header.
pub mod fused_jit_rule;
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
pub mod hash;
pub mod join;
pub mod kafka_backend;
pub mod kinesis_backend;
pub mod meta;
pub mod mysql_backend;
pub mod objectstore_backend;
pub mod pg;
// Task #481: one-call setup helpers (`with_optimizer_rules` +
// `register_dict_aware_parquet`) that activate the dict-aware fast
// path without callers having to memorise the rule chain + the
// `with_dict_preservation(true)` opt-in.
pub mod preset;
pub mod pubsub_backend;
pub mod rabbitmq_backend;
pub mod session_blob;
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
