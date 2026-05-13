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
// Σ.D1: `FusedFilterSumExec` physical operator for the simple
// `Aggregate(SUM) over Filter(predicate)` plan shape — closes the
// Q6 gap vs Polars (1.0 ms hand-written / 1.9 ms Polars / 5.96 ms
// today's DataFusion). See `fused.rs` header and issue #44.
pub mod fused;
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
// Σ.D3 phase E: fully-fused Q14 scan + filter + join + agg in one
// operator. Owns both inputs (lineitem + part); part becomes a
// direct-indexed promo bitmap; lineitem streams through parallel
// shards filtering + probing + accumulating in one pass. Built to
// beat Polars's 12.5 ms Q14 SF=1 number — the post-join-only fusion
// in `fused_post_join.rs::Q14` only saved ~0.9 ms; the JOIN+scan
// dominated. See module header for the algorithm.
pub mod fused_q14_full;
// Σ.D3 phase D: `PhysicalOptimizerRule` that auto-routes hand-coded
// FusedFilterSumExec / FusedFilterMultiAggExec / FusedPostJoinExec(Q14)
// instances to their Cranelift-JIT'd variants. Walks the physical plan
// tree with `transform_up` and swaps each match in place. Full SQL-
// pattern detection (extracting predicate constants from arbitrary
// PhysicalExpr ASTs to inject a fused exec where none was constructed)
// is documented as a follow-up in the module header.
pub mod fused_jit_rule;
pub mod hash;
pub mod join;
pub mod kafka_backend;
pub mod kinesis_backend;
pub mod meta;
pub mod mysql_backend;
pub mod objectstore_backend;
pub mod pg;
pub mod pubsub_backend;
pub mod rabbitmq_backend;
pub mod session_blob;
pub mod spec;
pub mod sqlite_backend;
pub mod state_size;
pub mod state_store;
pub mod strategy;
pub mod streaming;
pub mod transform;
pub mod types;
pub mod windowed;

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
