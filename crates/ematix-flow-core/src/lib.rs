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
// Σ.D3: cranelift-JIT'd inner loop for the unified fused-aggregate
// operator. See `fused_jit.rs` header and issue #45. Day-1 scaffold:
// JIT'd Q6 predicate evaluator that hits the same kernel shape as
// Σ.D1's hard-coded operator from a data-driven input. The full
// generic IR emitter (any predicate AST, any agg spec, any group-by
// shape) builds on this scaffold.
pub mod fused_jit;
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
