//! `ematix-flow-hash-join` — Σ.T V5 L13 custom hash-join kernel.
//!
//! ## Why a sibling crate
//!
//! Per `project_optimizer_codegen_sensitivity.md`, every new
//! optimizer rule or rule-walked operator in `ematix-flow-core` has
//! paid 5-8% geomean tax from LLVM codegen perturbation **before
//! the new code does any work**. Σ.K.2 / Σ.H.1d / Σ.K.A all hit
//! this wall. The mitigation that's worked (`ematix-parquet`,
//! Σ.N.f.3 RobinHood substrate at the kernel layer) is to ship
//! perf-sensitive code in its own crate so the codegen tax stays
//! local to the consumer that actually calls it.
//!
//! This crate is **pure kernel** — no DataFusion or Arrow dep. The
//! DataFusion `ExecutionPlan` wrapper (`EmatixHashJoinExec`) and
//! the pre-planning helper that swaps it in for shape-matched
//! plans (`pick_emat_hash_join`) live in `ematix-flow-core` and
//! land in Story 2.5.
//!
//! ## OQ-L13-A resolution: sibling op, not outright replacement
//!
//! V2 §2.3 originally said "replace `HashJoinExec`"; V5 §1 Tier 1
//! and CURRENT.md updated that to "wrapper that swaps in for
//! `HashJoinExec` when shape matches." We follow the
//! Σ.N.f.3 (RobinHoodAggregateExec) pattern: the new op exists, a
//! pre-planning helper picks the route, the existing DataFusion op
//! stays as the fallback. Lower blast radius; preserves
//! DataFusion compatibility on any shape we haven't validated.
//!
//! ## Shape this crate targets
//!
//! - Inner join, `i64 = i64` equi-key.
//! - Build side ingests as a stream of batches (one or many), each
//!   batch carries an absolute `row_idx_base` so multi-batch builds
//!   preserve the caller's row numbering.
//! - Probe side streams batches and emits `(probe_row_idx,
//!   build_row_idx)` pairs per match.
//! - Duplicates allowed on both sides (chained per-key, one heap
//!   allocation upfront for the entire chain pool).
//! - NULL keys never match (Inner-join NULL semantics).
//!
//! ## Out of scope (in this crate)
//!
//! - LeftOuter / RightOuter / FullOuter / Semi / Anti — Story 2.5's
//!   shape-matcher only swaps the kernel in on Inner; everything
//!   else stays on stock `HashJoinExec`.
//! - Non-i64 key types. Generic-over-`K: Hash + Eq + Copy` is the
//!   next monomorphisation (i32, u32, Date32, then dictionary-int
//!   codes for string FKs once Σ.K.2 dict-routing's Σ.L workload-
//!   log integration lands per Phase 3).
//! - Multi-column composite keys. The optimiser projects composite
//!   keys to a single i64 hash before this crate sees them; that's
//!   a Story 2.5 concern.
//! - Bloom emission (Story 2.3) lives in its own module when 2.3
//!   lands. Build-side selection (Story 2.2) lives in
//!   [`build_side`]; skew detection + overflow partition (Story 2.4)
//!   lives in [`skew`].

pub mod build_side;
pub mod skew;
pub mod table;

pub use build_side::{BuildSide, BuildSideReason, SideStats, StatsSource, choose};
pub use skew::{OverflowTable, SkewAnalysis, observe as observe_skew};
pub use table::{ProbeMatch, RobinHoodHashJoinI64Table};
