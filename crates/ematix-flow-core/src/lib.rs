//! ematix-flow Rust core.
//!
//! See `docs/PRD.md` and `docs/IMPLEMENTATION_PLAN.md` for the design.

pub mod ddl;
pub mod pg;
pub mod spec;
pub mod types;

pub use spec::{Mode, PipelineSpec, SourceSpec, SpecError, TargetSpec, normalize_json};
pub use types::{ColumnSpec, ColumnType, TableSpec, normalize_table_json};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
