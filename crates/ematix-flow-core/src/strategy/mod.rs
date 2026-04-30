//! Phase 5+: load strategies. Each strategy produces an executable plan
//! against a target Postgres given a source query/table.

pub mod append;
pub mod truncate;

pub use append::{AppendPlan, augment_with_metadata, plan_same_db_append};
pub use truncate::{TruncatePlan, plan_truncate_replace};
