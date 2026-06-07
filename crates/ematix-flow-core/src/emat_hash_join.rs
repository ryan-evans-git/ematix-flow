//! HJ.3 — Arrow bridge for the L13 RobinHood hash-join kernel.
//!
//! Turns Arrow `RecordBatch`es into the pure-kernel
//! `RobinHoodHashJoinI64Table` (build + probe over i64 key slices) and
//! gathers the emitted `(probe_row, build_row)` matches back into an
//! Arrow output `RecordBatch` via the `take`/`interleave` kernels.
//!
//! Scope (v1): **Inner** join, single i64 (or i64-widenable: Int32/
//! Date32) equi-key, build = LEFT (matches DataFusion's HashJoinExec
//! convention + SwapSemiJoinBuildSideRule). The `EmatixHashJoinExec`
//! operator (next) wraps this; the pre-plan rule swaps it in only on
//! this validated shape and leaves every other join on stock DataFusion.
//!
//! SF100.7 (#312): the build payload is kept **un-concatenated** as the
//! original `Vec<RecordBatch>`. Only the i64 join keys are gathered into
//! one contiguous slice (cheap — 8 B/row) to drive the kernel; wide
//! payload columns are gathered at probe time with the `interleave`
//! kernel via a global-row-index → `(batch, local-row)` map. This drops
//! the `concat_batches` 2× transient peak + full wide-string memcpy that
//! made the no-shuffle build non-viable at SF=100 (5.73M-row / 3.2 GB
//! build). Single-batch builds keep the byte-identical `take` fast path.
//!
//! WHY this lives in flow-core, not the kernel crate: the kernel is
//! deliberately Arrow/DataFusion-free (codegen-sensitivity isolation,
//! see `project_optimizer_codegen_sensitivity.md`). The Arrow glue is
//! the consumer and belongs here.

use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
// take/interleave via datafusion's re-exported arrow (arrow-select is only a
// dev-dependency of flow-core; this keeps the bridge in non-test builds).
use datafusion::arrow::compute::{interleave, take};
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
    /// Build side, kept **un-concatenated** (SF100.7 / #312). The kernel's
    /// emitted `build_row_idx` is a global index across all batches; it maps
    /// back to `(batch, local-row)` via `build_offsets`.
    build_batches: Vec<RecordBatch>,
    /// `build_offsets[k]` = global row index of the first row of
    /// `build_batches[k]`; the trailing entry is the total build row count.
    /// Ascending, so a global index resolves to its batch by `partition_point`.
    build_offsets: Vec<u32>,
    probe_key_idx: usize,
    output: Vec<JoinColumn>,
    output_schema: SchemaRef,
}

impl EmatHashJoiner {
    /// Build the table from the (already collected) build-side batches.
    /// `build_key_idx` / `probe_key_idx` index the join key column on
    /// each side; `output` maps each output column to its source.
    ///
    /// Concat-free: only the i64 keys are gathered into one slice to drive
    /// the kernel; the wide payload batches are retained as-is and gathered
    /// per probe via `interleave`.
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
        // Gather ONLY the i64 keys (cheap, 8 B/row) into one contiguous slice
        // + build the global row-offset map. The wide payload is never copied.
        let total: usize = build_batches.iter().map(|b| b.num_rows()).sum();
        let mut keys: Vec<i64> = Vec::with_capacity(total);
        let mut per_batch_nulls: Vec<Option<Vec<bool>>> = Vec::with_capacity(build_batches.len());
        let mut offsets: Vec<u32> = Vec::with_capacity(build_batches.len() + 1);
        let mut acc: u32 = 0;
        let mut any_null = false;
        for b in build_batches {
            offsets.push(acc);
            let (k, n) = key_as_i64(b.column(build_key_idx)).ok_or("unsupported build key type")?;
            acc = acc
                .checked_add(k.len() as u32)
                .ok_or("build side exceeds u32::MAX rows")?;
            keys.extend_from_slice(&k);
            any_null |= n.is_some();
            per_batch_nulls.push(n);
        }
        offsets.push(acc);
        // Materialise a combined validity slice only if some batch had nulls;
        // batches without nulls contribute all-valid.
        let nulls: Option<Vec<bool>> = if any_null {
            let mut v = Vec::with_capacity(total);
            for (b, n) in build_batches.iter().zip(per_batch_nulls.iter()) {
                match n {
                    Some(nn) => v.extend_from_slice(nn),
                    None => v.extend(std::iter::repeat(true).take(b.num_rows())),
                }
            }
            Some(v)
        } else {
            None
        };

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
            // Cheap: clones Arc-backed columns, not the underlying buffers.
            build_batches: build_batches.to_vec(),
            build_offsets: offsets,
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

    /// Number of retained (un-concatenated) build batches. SF100.7 diagnostic:
    /// `> 1` means the concat-free `interleave` gather path is active.
    pub fn build_batch_count(&self) -> usize {
        self.build_batches.len()
    }

    /// Map a global build row index to `(batch, local-row)`.
    #[inline]
    fn locate(&self, global: u32) -> (usize, usize) {
        // `build_offsets` is ascending with a trailing total → the batch is the
        // last offset `<=` global.
        let k = self.build_offsets.partition_point(|&o| o <= global) - 1;
        (k, (global - self.build_offsets[k]) as usize)
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

        // Build-side gather. One build batch → plain `take` (byte-identical to
        // the pre-#312 path; keeps small / SF=10 joins on the hot path). Many
        // batches → `interleave` by `(batch, local-row)` so the wide payload is
        // never concatenated.
        let single = self.build_batches.len() == 1;
        let build_take_idx: Option<UInt32Array> =
            single.then(|| UInt32Array::from_iter_values(matches.iter().map(|m| m.build_row_idx)));
        let build_pairs: Option<Vec<(usize, usize)>> = (!single).then(|| {
            matches
                .iter()
                .map(|m| self.locate(m.build_row_idx))
                .collect()
        });

        let cols: Vec<ArrayRef> = self
            .output
            .iter()
            .map(|jc| match jc {
                JoinColumn::Probe(i) => take(probe.column(*i), &probe_idx, None),
                JoinColumn::Build(i) => {
                    if single {
                        take(
                            self.build_batches[0].column(*i),
                            build_take_idx.as_ref().unwrap(),
                            None,
                        )
                    } else {
                        let arrays: Vec<&dyn Array> = self
                            .build_batches
                            .iter()
                            .map(|b| b.column(*i).as_ref())
                            .collect();
                        interleave(&arrays, build_pairs.as_ref().unwrap())
                    }
                }
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
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn i64_batch(name: &str, vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vals))],
        )
        .unwrap()
    }

    /// key (i64) + payload (Utf8) build/probe batch.
    fn kv_batch(keys: Vec<i64>, payload: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("v", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(StringArray::from(payload)),
            ],
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
        assert_eq!(
            j.build_batch_count(),
            1,
            "single-batch build → take fast path"
        );
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

    #[test]
    fn multi_batch_build_concat_free_gather() {
        // SF100.7 (#312): 3 build batches exercise the concat-free interleave
        // path + the global-row-idx → (batch, local) map across batch
        // boundaries, incl. a duplicate key (20) spanning batch0 and batch2.
        let b0 = kv_batch(vec![10, 20], vec!["a0", "a1"]); // global 0,1
        let b1 = kv_batch(vec![30], vec!["b0"]); // global 2
        let b2 = kv_batch(vec![40, 20], vec!["c0", "c1"]); // global 3,4
        let probe = kv_batch(vec![20, 40, 30, 10], vec!["p0", "p1", "p2", "p3"]);
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, false), // build payload (wide col)
            Field::new("k", DataType::Int64, false), // build key
            Field::new("pk", DataType::Int64, false), // probe key
        ]));
        let j = EmatHashJoiner::try_build(
            &[b0, b1, b2],
            0,
            0,
            vec![
                JoinColumn::Build(1),
                JoinColumn::Build(0),
                JoinColumn::Probe(0),
            ],
            out_schema,
        )
        .unwrap();
        assert_eq!(
            j.build_batch_count(),
            3,
            "concat-free retains all build batches (no concat_batches)"
        );
        let out = j.probe(&probe).unwrap();
        // matches: 20→{a1,c1}, 40→{c0}, 30→{b0}, 10→{a0} = 5 rows
        assert_eq!(out.num_rows(), 5, "inner-join cardinality across batches");
        let v = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bk = out.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let pk = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..out.num_rows() {
            assert_eq!(bk.value(i), pk.value(i), "build key == probe key");
        }
        // Cross-batch payloads must resolve to the correct source batch.
        let got: std::collections::HashSet<(String, i64)> = (0..out.num_rows())
            .map(|i| (v.value(i).to_string(), bk.value(i)))
            .collect();
        assert!(
            got.contains(&("a1".to_string(), 20)),
            "batch0 dup-key payload"
        );
        assert!(
            got.contains(&("c1".to_string(), 20)),
            "batch2 dup-key payload"
        );
        assert!(got.contains(&("c0".to_string(), 40)), "batch2 payload");
        assert!(got.contains(&("b0".to_string(), 30)), "batch1 payload");
        assert!(got.contains(&("a0".to_string(), 10)), "batch0 payload");
    }
}
