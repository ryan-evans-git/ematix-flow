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
// Σ.D4 + Σ.D5: `FusedPostJoinExec` — single-pass fused aggregate
// over the output of a join. Wraps the Q3/Q5/Q14 day-1 prototype
// kernels (5-8× faster than DataFusion's per-aggregate dispatch on
// the agg step; modest end-to-end shift because these queries are
// JOIN-bound). Substrate for the future cranelift-JIT-generalized
// operator. See `fused_post_join.rs` header and issues #51, #52.
pub mod fused_post_join;
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
