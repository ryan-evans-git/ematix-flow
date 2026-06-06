//! HJ.3 — Arrow bridge for the L13 RobinHood hash-join kernel.
//!
//! Turns Arrow `RecordBatch`es into the pure-kernel
//! `RobinHoodHashJoinI64Table` (build + probe over i64 key slices) and
//! gathers the emitted `(probe_row, build_row)` matches back into an
//! Arrow output `RecordBatch` via the `take` kernel.
//!
//! Scope (v1): **Inner** join, single i64 (or i64-widenable: Int32/
//! Date32) equi-key, build = LEFT (matches DataFusion's HashJoinExec
//! convention + SwapSemiJoinBuildSideRule). The `EmatixHashJoinExec`
//! operator (next) wraps this; the pre-plan rule swaps it in only on
//! this validated shape and leaves every other join on stock DataFusion.
//!
//! WHY this lives in flow-core, not the kernel crate: the kernel is
//! deliberately Arrow/DataFusion-free (codegen-sensitivity isolation,
//! see `project_optimizer_codegen_sensitivity.md`). The Arrow glue is
//! the consumer and belongs here.

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
// take/concat via datafusion's re-exported arrow (arrow-select is only a
// dev-dependency of flow-core; this keeps the bridge in non-test builds).
use datafusion::arrow::compute::{concat_batches, take};
use ematix_flow_hash_join::{ProbeMatch, RobinHoodHashJoinI64Table, TaggedJoinI64U32};
use std::sync::atomic::{AtomicU64, Ordering};

/// HJ.4 fire counters (observability for the wall A/B): how many build sides
/// were resolved to the SIMD-tag table vs the chained RobinHood table. Read by
/// `examples/hj4_q08_wall_ab.rs` to confirm which kernel ran.
pub static TAG_BUILDS: AtomicU64 = AtomicU64::new(0);
pub static RH_BUILDS: AtomicU64 = AtomicU64::new(0);

/// HJ.4 opt-in (in addition to `EMAT_HASH_JOIN=1`): prefer the SIMD-tag probe
/// table for key-UNIQUE build sides (e.g. a dimension PK). Falls back to the
/// chained RobinHood table when the build has duplicate keys.
fn tag_probe_enabled() -> bool {
    std::env::var("EMAT_HJ_TAG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// The resolved probe structure: SIMD-tag (unique build) or chained RobinHood.
enum ProbeTable {
    RobinHood(RobinHoodHashJoinI64Table),
    Tag(TaggedJoinI64U32),
}

/// Source of an output column: a column index on the build side or the
/// probe side. The operator builds this from the join's output schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinColumn {
    Build(usize),
    Probe(usize),
}

/// Extract an i64 key vector + optional per-row validity from an Arrow
/// column, widening Int32 → i64. Returns `None` for unsupported key
/// types (caller then declines the swap and stays on stock HashJoin).
fn key_as_i64(col: &ArrayRef) -> Option<(Vec<i64>, Option<Vec<bool>>)> {
    let nulls = col.nulls().map(|nb| {
        (0..col.len())
            .map(|i| nb.is_valid(i))
            .collect::<Vec<bool>>()
    });
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Some((a.values().to_vec(), nulls))
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Some((a.values().iter().map(|&v| v as i64).collect(), nulls))
    } else {
        None
    }
}

/// Built hash table + retained build-side rows, ready to probe.
pub struct EmatHashJoiner {
    table: ProbeTable,
    /// Concatenated build side, indexed by the kernel's `build_row_idx`.
    build: RecordBatch,
    probe_key_idx: usize,
    output: Vec<JoinColumn>,
    output_schema: SchemaRef,
}

impl EmatHashJoiner {
    /// Build the table from the (already collected) build-side batches.
    /// `build_key_idx` / `probe_key_idx` index the join key column on
    /// each side; `output` maps each output column to its source.
    pub fn try_build(
        build_batches: &[RecordBatch],
        build_key_idx: usize,
        probe_key_idx: usize,
        output: Vec<JoinColumn>,
        output_schema: SchemaRef,
    ) -> Result<Self, String> {
        if build_batches.is_empty() {
            return Err("empty build side".into());
        }
        let schema = build_batches[0].schema();
        let build = if build_batches.len() == 1 {
            build_batches[0].clone()
        } else {
            concat_batches(&schema, build_batches)
                .map_err(|e| format!("concat build batches: {e}"))?
        };
        let (keys, nulls) =
            key_as_i64(build.column(build_key_idx)).ok_or("unsupported build key type")?;
        // HJ.4: prefer the SIMD-tag table when opted in AND the build is
        // key-unique (try_build returns None on a duplicate key → fall back).
        let table = if tag_probe_enabled() {
            match TaggedJoinI64U32::try_build(&keys, nulls.as_deref(), 0) {
                Some(t) => {
                    TAG_BUILDS.fetch_add(1, Ordering::Relaxed);
                    ProbeTable::Tag(t)
                }
                None => {
                    let mut t = RobinHoodHashJoinI64Table::with_capacity(keys.len());
                    t.insert_batch(&keys, nulls.as_deref(), 0);
                    RH_BUILDS.fetch_add(1, Ordering::Relaxed);
                    ProbeTable::RobinHood(t)
                }
            }
        } else {
            let mut t = RobinHoodHashJoinI64Table::with_capacity(keys.len());
            t.insert_batch(&keys, nulls.as_deref(), 0);
            RH_BUILDS.fetch_add(1, Ordering::Relaxed);
            ProbeTable::RobinHood(t)
        };
        Ok(Self {
            table,
            build,
            probe_key_idx,
            output,
            output_schema,
        })
    }

    /// Number of distinct build keys (for diagnostics / gating).
    pub fn build_keys(&self) -> usize {
        match &self.table {
            ProbeTable::RobinHood(t) => t.len(),
            ProbeTable::Tag(t) => t.len(),
        }
    }

    /// Probe one batch, returning the gathered Inner-join output batch.
    pub fn probe(&self, probe: &RecordBatch) -> Result<RecordBatch, String> {
        let (keys, nulls) =
            key_as_i64(probe.column(self.probe_key_idx)).ok_or("unsupported probe key type")?;
        let mut matches: Vec<ProbeMatch> = Vec::with_capacity(keys.len());
        match &self.table {
            ProbeTable::RobinHood(t) => t.probe_batch(&keys, nulls.as_deref(), 0, &mut matches),
            ProbeTable::Tag(t) => t.probe_batch(&keys, nulls.as_deref(), 0, &mut matches),
        }

        let probe_idx = UInt32Array::from_iter_values(matches.iter().map(|m| m.probe_row_idx));
        let build_idx = UInt32Array::from_iter_values(matches.iter().map(|m| m.build_row_idx));

        let cols: Vec<ArrayRef> = self
            .output
            .iter()
            .map(|jc| match jc {
                JoinColumn::Build(i) => take(self.build.column(*i), &build_idx, None),
                JoinColumn::Probe(i) => take(probe.column(*i), &probe_idx, None),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("take/gather: {e}"))?;

        RecordBatch::try_new(self.output_schema.clone(), cols)
            .map_err(|e| format!("assemble output batch: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn i64_batch(name: &str, vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vals))],
        )
        .unwrap()
    }

    #[test]
    fn inner_join_i64_matches_naive() {
        // build keys 10,20,30,20 ; probe keys 20,30,40,10,20
        let build = i64_batch("bk", vec![10, 20, 30, 20]);
        let probe = i64_batch("pk", vec![20, 30, 40, 10, 20]);
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("bk", DataType::Int64, false),
            Field::new("pk", DataType::Int64, false),
        ]));
        let j = EmatHashJoiner::try_build(
            std::slice::from_ref(&build),
            0,
            0,
            vec![JoinColumn::Build(0), JoinColumn::Probe(0)],
            out_schema,
        )
        .unwrap();
        let out = j.probe(&probe).unwrap();
        // p=20→{b1,b3}, p=30→{b2}, p=40→{}, p=10→{b0}, p=20→{b1,b3} = 6 rows
        assert_eq!(out.num_rows(), 6, "inner-join cardinality");
        let bk = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let pk = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..out.num_rows() {
            assert_eq!(bk.value(i), pk.value(i), "every emitted pair has bk==pk");
        }
    }

    #[test]
    fn null_keys_never_match() {
        let build = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("bk", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let probe = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![
                Some(1),
                None,
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        let out_schema = Arc::new(Schema::new(vec![Field::new("bk", DataType::Int64, true)]));
        let j = EmatHashJoiner::try_build(
            std::slice::from_ref(&build),
            0,
            0,
            vec![JoinColumn::Build(0)],
            out_schema,
        )
        .unwrap();
        let out = j.probe(&probe).unwrap();
        // Only 1↔1 and 3↔3 match; NULLs never match → 2 rows.
        assert_eq!(out.num_rows(), 2);
    }
}
