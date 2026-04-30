//! ematix-flow Rust core.
//!
//! See `docs/PRD.md` and `docs/IMPLEMENTATION_PLAN.md` for the design.

pub mod spec;

pub use spec::{Mode, PipelineSpec, SourceSpec, SpecError, TargetSpec, normalize_json};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
