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
// Σ.Q L2: swap LeftSemi/LeftAnti join sides when the probe side has an
// AggregateExec (i.e., is bounded-small) but the build side doesn't.
// Fixes Q18 SF=10 build-on-60M-rows inversion. See module docs.
pub mod swap_semi_join_build_rule;
// Σ.Q.L10: logical-plan rewrite — push a LeftSemi join down past
// Inner joins so it filters its target table directly, eliminating
// the giant intermediate that gets semi-filtered at the top of the
// plan. Closes the structural gap to DuckDB on Q18-style queries
// where an IN-subquery decorrelates into a LeftSemi placed above
// every join.
pub mod push_down_left_semi_rule;
// Σ.Q.M: synthesise LeftSemi joins above Inner joins where one side is
// a filtered dim and the other is a fact table — the static analogue
// of DuckDB's dynamic-filter propagation. Composes with Σ.Q.L10 which
// then pushes the synthesised LeftSemi down to wrap the fact scan.
pub mod synthetic_left_semi_rule;
// Σ.P.1: session-scoped subquery CSE. SharedSubtreeExec wraps a
// duplicated subtree; both consumers share an Arc<CachedBatches> so
// the first execute() computes and the rest replay. Backing storage
// for DedupeAggregateForFloatDeterminism's CSE rewrite path.
pub mod shared_subtree_exec;
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
// Σ.T (2026-05-25): selectivity-first inner-join reorder. Pre-plan
// walker pattern (same precedent as dict_routing — explicit caller
// invocation, no PhysicalOptimizerRule, no codegen tax). Targets
// Q05 / Q08 SF=10 where DataFusion 53.1 has no cost-based join
// reorder and falls back to FROM-clause order. See
// `docs/PHASE_SIGMA_T_JOIN_REORDER.md`.
pub mod join_reorder;
// Σ.U (2026-05-26): push a filter as LeftSemi into an Aggregate's
// input scan. Targets Q17 SF=10 where DF decorrelates the
// correlated `(SELECT 0.2*AVG(l_quantity) WHERE l_partkey=outer.p_partkey)`
// into an Aggregate-over-full-lineitem joined back to filtered
// part, but the agg still runs on all 60M rows producing 10.8M
// groups when only ~200 partkeys actually matter.
pub mod agg_filter_pushdown;
// Σ.AE (2026-05-26): late physical-optimizer rule that surgically
// drops redundant FilterExec conjuncts when the underlying
// EmatixFastParquetExec's BridgeFilter has already evaluated them
// exactly. Spike for the "filter double-decode" pattern documented in
// docs/PI_16_Q06_PROFILE.md. Opt-in via `EMAT_DROP_REDUNDANT_FILTER=1`.
pub mod drop_redundant_filter_rule;
// Σ.AD (2026-05-25): pre-plan walker that pushes small filtered-dim
// Inner joins (nation/region with name filters) down adjacent to their
// FK table's scan inside an Inner-join chain. Targets Q07 where
// `Inner Join(supplier.s_nationkey = n1.n_nationkey)` currently sits at
// the top of the tree above `supplier ⋈ lineitem`. The pre-Σ.AD plan
// broadcasts full 100K supplier; the post-Σ.AD plan filters supplier
// to ~8K via the nation join BEFORE the fact-fact broadcast.
pub mod dim_join_pushdown;
// Σ.AN.1 (2026-05-28): physical optimizer rule that boosts the
// partition count of RepartitionExec(Hash) → AggregateExec(FinalPartitioned)
// pipelines whose agg output cardinality exceeds the per-partition
// L3-fit budget. Targets Q18 SF=10's 15M-group SUM agg that
// blows L3 at the default 14-partition layout. Opt-in via
// EMAT_AGG_PARTITION_BOOST=1.
pub mod agg_partition_boost;
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
// L9.HashSet (2026-05-24): exact i64 membership set for the L9 runtime
// bloom sideband path. Used when the join build is small enough that
// an exact open-addressing table outperforms the probabilistic bloom
// (Q17 SF=10: 17.2 ns/probe bloom → 1.3 ns/probe i64_set; +0 FPs).
pub mod i64_set;
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
// Σ.Q.L1b (2026-05-23): SUM(f64) GROUP BY i64 operator + planner rule.
// Sibling to robin_hood_agg's COUNT(*) variant; targets Q18 SF=10's
// FinalPartitioned sum(l_quantity) GROUP BY l_orderkey at 15M card.
pub mod robin_hood_sum_f64_exec;
// Σ.R.2 (2026-05-24): AVG(f64) GROUP BY i64 operator + opt-in rule.
// Sister to robin_hood_sum_f64_exec; targets Q17 SF=10's
// FinalPartitioned avg(l_quantity) GROUP BY l_partkey at ~2M card
// where the Σ.Q closeout profile pinned GroupValuesPrimitive::intern
// at 21.6% self time.
pub mod robin_hood_avg_f64_exec;
// Σ.J.2.b.vi (2026-05-22): per-request PhysicalOptimizerRule that
// walks the plan, finds EmatixFastParquetExec scans whose
// `<table>.<col>` uuid matches a build-side bloom in ContextBlooms,
// and wraps them in BloomFilterExec. Closes the probe-side half of
// the distributed bloom flow; build-side emitter is Σ.J.2.b.vii.
pub mod context_bloom_rule;
// Σ.Q.L4′ (2026-05-23): per-request PhysicalOptimizerRule that pushes
// blooms INTO EmatixFastParquetExec scans as I64InBloom BridgeFilter
// predicates instead of wrapping them in BloomFilterExec. Saves decode
// work on probe-side rows whose join key isn't in the build set.
pub mod inbloom_scan_pushdown_rule;
// Σ.Q.L4′ slice 3 (2026-05-23): single-node bloom emitter. Walks a
// LogicalPlan, pre-executes Inner-equijoin build sides, returns a
// `column_uuid → BloomFilter` map ready to wrap in ContextBlooms +
// install via EnableInBloomScanPushdownRule. Local sibling of
// ematix-flow-distributed::bloom_emitter::emit_build_side_blooms.
pub mod local_bloom_emitter;
// Σ.Q.L9 (2026-05-23): runtime sideband channel for mid-query plan
// adaptation. Producer (BuildSideBloomEmitterExec) writes blooms as a
// side-effect of HashJoinExec's build phase; consumer
// (EmatixFastParquetExec on the probe side) reads at execute() time.
pub mod bridge_filter_sideband;
// Σ.Q.L9 slice 2 (2026-05-23): pass-through wrapper exec that observes
// batches flowing from a HashJoinExec build side, accumulates the i64
// join-key column into a BloomFilter (one local per partition,
// union-merged at completion), and publishes an I64InBloom
// ColumnPredicate to a BridgeFilterSideband shared with the probe scan.
pub mod build_side_bloom_emitter_exec;
// Σ.Q.L9 slice 3 (2026-05-23): planner rule that threads a runtime
// sideband between HashJoinExec's build child (wrapped with
// BuildSideBloomEmitterExec) and the probe-side EmatixFastParquetExec
// (via with_runtime_sideband). Opt-in via install_runtime_bloom_sideband_rule.
pub mod runtime_bloom_sideband_rule;
// Σ.S.B (2026-05-24): plan-time FK-chain detection helper shared by
// the cascading-L9 prototype and the general rule. Pure-fn stem
// extraction + plan walker that surfaces candidate EmatixFastParquetExec
// scans by column-name stem. The cascading rule layers join-path
// confirmation on top of this — the helper itself only proposes
// candidates, the rule commits.
pub mod fk_chain;
// Σ.S.B (2026-05-24): cascading-L9 rule. Opt-in superset of the
// non-cascading L9 base rule: per HashJoinExec, finds extra
// FK-chained scans in the probe subtree via fk_chain, then publishes
// the build-side bloom to each via a per-scan sideband (build runs
// once; bloom Arc shared). Install via install_cascading_bloom_rule.
pub mod runtime_bloom_cascading_rule;
// Σ.AJ.1 Lever B POC (2026-05-27): plan-wide broadcast of bloom
// emitters to sibling same-parquet-path scans. Specifically targets
// Q17's correlated AVG-subquery shape where the subquery's lineitem
// scan can use the part-filter bloom but isn't reachable from the
// HashJoin-local probe-subtree walk that the cascading rule uses.
// Opt-in via EMAT_L9_BROADCAST_SIBLINGS=1; default OFF. Unsafe for
// Q21-shape (correlated subqueries with different join semantics).
pub mod broadcast_sibling_blooms_rule;
// Σ.U.A (2026-05-24): Apache-Impala-style lane-parallel filter-sum
// kernel. Generalises the splash-bloom pattern (see `bloom.rs`) to
// the "M predicate clauses + SUM-of-product" shape that covers Q06,
// Q14, Q19, and future predicates of the same family. Branchless,
// fixed-size lane arrays, LLVM auto-vectorises to NEON on aarch64
// and SSE2/AVX2 on x86_64 — one source-of-truth, two architectures.
pub mod lane_filter_sum_kernel;
// Σ.O (2026-05-21): in-memory LRU cache of decoded RecordBatch by
// (file_path, row_group_idx, projection_hash). Avoids re-decoding
// the same parquet column chunks across queries in the same session.
// Multi-query workloads (dashboards, repeated benchmarks) reuse
// decoded batches instead of re-decoding.
pub mod hash;
pub mod join;
pub mod parquet_decode_cache;
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
