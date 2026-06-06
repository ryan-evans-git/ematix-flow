//! `ematix-flow-push` — PV.1 morsel-driven PUSH pipeline kernel.
//!
//! ## Why a sibling crate
//!
//! Per `project_optimizer_codegen_sensitivity.md`, every new
//! optimizer rule / rule-walked operator added to `ematix-flow-core`
//! has paid a 5-8% geomean codegen tax on *unrelated* queries before
//! doing any work (Σ.K.2 / Σ.H.1d / Σ.K.A). The mitigation that has
//! worked — `ematix-parquet`, `ematix-flow-hash-join`, the Σ.N.f.3
//! RobinHood substrate — is to ship perf-sensitive code in its own
//! crate so the tax stays local to the consumer that calls it. This
//! crate is **pure kernel** — no DataFusion, no Arrow. The
//! `EmatPushPipelineExec` `ExecutionPlan` glue lands in
//! `ematix-flow-core` in PV.2.
//!
//! ## What it does (the measured lever)
//!
//! Phase-0 (`examples/pv1_fused_q08_pipeline.rs`) showed the
//! Q08/Q07/Q10/Q17/Q20 SF=10 loss to DuckDB is NOT decode (ematix
//! decodes faster; payload Snappy decompress is unavoidable+shared)
//! — it is the PULL model materialising a 60M-row intermediate and
//! `take`-gathering at every join boundary. A fused, RG-parallel,
//! push loop that compacts ONLY at its output measured **−16% vs the
//! pull pipeline, beating DuckDB**. This crate is the reusable kernel
//! for that loop: a `Morsel` carrying decoded columns + a deferred
//! `Selection`, `PushOperator`s that narrow the selection (or attach
//! a probe payload) without compacting, and an adaptive
//! [`ProbeStructure`] selector.
//!
//! ## The load-bearing finding (ADR §1.3 finding 2)
//!
//! The win EXISTS ONLY when the materialisation removal is paired
//! with a fast, domain-aware probe. The same fused loop with a naive
//! `HashSet` over the 60M-row probe side measured **+22% (a loss)**;
//! with a dense bitset keyed on the bounded FK domain it measured
//! **−16%**. So [`ProbeStructure`] selection (`probe::choose`) is the
//! #1 surface, not a knob — see `probe.rs`.

pub mod agg;
pub mod morsel;
pub mod ops;
pub mod probe;

pub use agg::I64SumF64;
pub use morsel::{Column, ColumnData, Morsel, Selection};
pub use ops::{FilterPush, Pred, ProbePush, ProjectPush, PushOperator, Sink};
pub use probe::{DENSE_BITSET_MAX_DOMAIN, DENSE_PAYLOAD_MAX_DOMAIN, ProbeStructure, choose};

/// Errors a push operator or sink can raise. Kept minimal — the
/// kernel is total over well-typed inputs; mismatches are bugs, not
/// recoverable conditions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushError {
    /// A column index referenced an absent column.
    BadColumn(usize),
    /// A key column was not the integer type the probe expects.
    KeyTypeMismatch,
    /// Sink-specific failure with a message.
    Sink(String),
}

impl std::fmt::Display for PushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushError::BadColumn(i) => write!(f, "bad column index {i}"),
            PushError::KeyTypeMismatch => write!(f, "key column type mismatch"),
            PushError::Sink(s) => write!(f, "sink error: {s}"),
        }
    }
}
impl std::error::Error for PushError {}

/// Drive `source` morsels through the `ops` chain into `terminal`,
/// **push-style**: each operator calls the next inline, so no
/// intermediate morsel is materialised between operators — the whole
/// point of the pipeline. The only compaction is whatever `terminal`
/// chooses to do at its boundary.
///
/// All Phase-1 operators are pipelined (no breakers in the chain; the
/// build side is frozen into the [`ProbeStructure`]s before the run,
/// and the aggregate is the `terminal`), so `finish` is a no-op here.
pub fn run_pipeline(
    ops: &mut [Box<dyn PushOperator>],
    source: impl IntoIterator<Item = Morsel>,
    terminal: &mut dyn Sink,
) -> Result<(), PushError> {
    for m in source {
        let mut chain = OpChainSink { ops, terminal };
        chain.emit(m)?;
    }
    Ok(())
}

/// A [`Sink`] that *is* the remainder of an operator chain: emitting a
/// morsel into it runs the first remaining operator, whose downstream
/// is a `OpChainSink` over the rest. `split_first_mut` keeps the
/// borrows disjoint (operator vs its downstream), so no `RefCell`.
struct OpChainSink<'a> {
    ops: &'a mut [Box<dyn PushOperator>],
    terminal: &'a mut dyn Sink,
}

impl Sink for OpChainSink<'_> {
    fn emit(&mut self, m: Morsel) -> Result<(), PushError> {
        match self.ops.split_first_mut() {
            None => self.terminal.emit(m),
            Some((first, rest)) => {
                let mut next = OpChainSink {
                    ops: rest,
                    terminal: self.terminal,
                };
                first.push(m, &mut next)
            }
        }
    }
}
