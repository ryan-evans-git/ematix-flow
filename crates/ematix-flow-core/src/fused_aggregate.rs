//! Σ.G.2 first slice: `AggregateSpec` trait.
//!
//! The Q1/Q6 per-query `*Spec` impls that originally lived here were
//! retired in Σ.G.2f.3 cleanup (2026-05-19) along with their
//! injection rules — see [[feedback-no-tpch-hardcoding]]. The trait
//! survives as the abstraction shared by `FilterSumSpec` and
//! `FilterMultiAggSpec`, which are the only remaining `AggregateSpec`
//! implementations.
//!
//! `process_batch` is invoked once per `RecordBatch` with a `&mut Acc`
//! so the implementation can keep arbitrary per-shape accumulator
//! state without box-allocations or trait dispatch *inside* the loop.
//! Trait dispatch is at the batch boundary, not the row boundary —
//! that's the key property that lets the unified path stay
//! competitive with hand-written operators.
//!
//! `finalize` runs once per partition shard at end-of-input, turning
//! the accumulator state into a `RecordBatch` matching
//! `output_schema()`.
//!
//! `validate_input_schema` is called once at operator construction to
//! catch type / column-name mismatches before any data flows.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;

/// Per-shape descriptor for the unified fused aggregate path.
pub trait AggregateSpec: Send + Sync + std::fmt::Debug + 'static {
    /// Per-shard accumulator state. Must be `Default` so the
    /// operator can spin one up per parallel shard before draining
    /// batches into it.
    type Accumulator: Default + Send;

    /// Process one input batch into the accumulator.
    ///
    /// Implementations should keep this method monomorphic over the
    /// batch column types (cache the `.values()` slices once, then
    /// loop on raw indices) so LLVM auto-vectorises the loop body.
    fn process_batch(&self, batch: &RecordBatch, acc: &mut Self::Accumulator) -> DfResult<()>;

    /// Convert the merged accumulator state to the operator's
    /// output `RecordBatch`. Called once per execute() at end-of-
    /// input after merging per-shard accumulators.
    fn finalize(&self, acc: Self::Accumulator) -> DfResult<RecordBatch>;

    /// Merge two per-shard accumulators. Called when the operator
    /// shards batches across rayon workers; each shard produces an
    /// `Accumulator`, then we left-fold-merge them before
    /// `finalize`.
    fn merge(&self, left: Self::Accumulator, right: Self::Accumulator) -> Self::Accumulator;

    /// The schema this operator emits. Captured at construction so
    /// the property table doesn't have to ask the spec per call.
    fn output_schema(&self) -> SchemaRef;

    /// Verify the child plan's schema has the columns + types this
    /// spec expects. Failure here surfaces as a Plan error at
    /// operator construction.
    fn validate_input_schema(&self, schema: &SchemaRef) -> DfResult<()>;
}
