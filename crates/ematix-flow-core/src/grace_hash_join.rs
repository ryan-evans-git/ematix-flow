//! Σ.SP Phase 1 — `GraceHashJoinExec`: a grace-partitioned Inner hash
//! join for builds the in-memory `HashJoinExec` cannot afford.
//!
//! DF 53's `HashJoinExec` cannot spill: an oversized build either rides
//! the page-cache margin to a kernel OOM (unbounded pool) or deadlocks
//! under a cap (the refuted 0.7×RAM blanket cap — see
//! docs/plans/MEMORY_BUDGET.md). Upstream spilling is still a
//! design-stage proposal (apache/datafusion#17267, checked 2026-07-11
//! against DataFusion 54). This operator is the classic answer:
//!
//! 1. **Partition phase** — stream BOTH inputs once, hash the join
//!    keys, and scatter rows across `k` Arrow-IPC spill files per side
//!    (`DiskManager` temp files, RAII-cleaned). Memory is O(k write
//!    buffers), independent of input size.
//! 2. **Pair phase** — join spill pair `i` with a stock in-memory
//!    `HashJoinExec` (CollectLeft — each pair is sized to fit by the
//!    demotion rule's choice of `k`), streaming its output onward
//!    before the next pair starts. Peak memory ≈ one pair's build.
//!
//! Rows with equal join keys land in the same pair by construction, so
//! the union of pair joins IS the join — the parity tests pin this
//! against the stock operator, duplicate keys and multi-key `on`
//! included.
//!
//! Scope (Phase 1): `JoinType::Inner`, no filter, no projection —
//! exactly the shape the demotion rule targets first. Semi/anti (the
//! Q21 shape) are Phase 2; recursion for a still-oversized pair is
//! Phase 1.5 (until then the demotion rule sizes `k` from the grounded
//! estimate with headroom). See docs/plans/SPILLABLE_JOIN.md.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use ahash::RandomState;
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::compute::take_record_batch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader as IpcFileReader;
use datafusion::arrow::ipc::writer::FileWriter as IpcFileWriter;
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{DataFusionError, JoinType, NullEquality, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::joins::utils::JoinOn;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures_util::StreamExt;

/// Fixed hash seeds for the scatter phase. DELIBERATELY distinct from
/// DataFusion's own join hash seeds — if the pair-phase `HashJoinExec`
/// hashed with the same function, every row in pair `i` would collide
/// into the same buckets and the build degenerates to a linked-list
/// scan. (Recursion, when it lands, bumps these per depth.)
const SCATTER_SEEDS: (u64, u64, u64, u64) = (0x5eed_5ca7, 0x7e11_ea57, 0x9e37_79b9, 0x0dd_ba11);

/// Grace-partitioned Inner equi-join. See module docs.
pub struct GraceHashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    /// Spill fan-out per side. Chosen by the demotion rule from the
    /// grounded build estimate; ≥ 1.
    num_spill_partitions: usize,
    props: Arc<PlanProperties>,
}

impl fmt::Debug for GraceHashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraceHashJoinExec")
            .field("on", &self.on)
            .field("num_spill_partitions", &self.num_spill_partitions)
            .finish()
    }
}

impl GraceHashJoinExec {
    /// Inner join only (Phase 1). `num_spill_partitions` is clamped to
    /// ≥ 1; `on` must be non-empty column-pair equi-keys.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        num_spill_partitions: usize,
    ) -> Result<Self> {
        if on.is_empty() {
            return Err(DataFusionError::Plan(
                "GraceHashJoinExec: empty join keys".into(),
            ));
        }
        // Inner output schema = left fields ⊕ right fields — the stock
        // HashJoinExec's Inner/no-projection shape, which the pair
        // phase reproduces exactly.
        let fields: Vec<Field> = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .map(|f| f.as_ref().clone())
            .collect();
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            left,
            right,
            on,
            num_spill_partitions: num_spill_partitions.max(1),
            props,
        })
    }

    pub fn num_spill_partitions(&self) -> usize {
        self.num_spill_partitions
    }
}

impl DisplayAs for GraceHashJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "GraceHashJoinExec: join_type=Inner, k={}, on={:?}",
            self.num_spill_partitions, self.on
        )
    }
}

impl ExecutionPlan for GraceHashJoinExec {
    fn name(&self) -> &str {
        "GraceHashJoinExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut it = children.into_iter();
        let (Some(l), Some(r), None) = (it.next(), it.next(), it.next()) else {
            return Err(DataFusionError::Internal(
                "GraceHashJoinExec: expected exactly 2 children".into(),
            ));
        };
        Ok(Arc::new(Self::try_new(
            l,
            r,
            self.on.clone(),
            self.num_spill_partitions,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "GraceHashJoinExec: single output partition, got {partition}"
            )));
        }
        let left = Arc::clone(&self.left);
        let right = Arc::clone(&self.right);
        let on = self.on.clone();
        let k = self.num_spill_partitions;
        let schema = self.schema();
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            let left_keys: Vec<_> = on.iter().map(|(l, _)| Arc::clone(l)).collect();
            let right_keys: Vec<_> = on.iter().map(|(_, r)| Arc::clone(r)).collect();
            let l_parts = scatter_side(&left, &left_keys, k, &context).await?;
            let r_parts = scatter_side(&right, &right_keys, k, &context).await?;
            for i in 0..k {
                // Inner join: a pair with either side empty emits nothing.
                let (Some(lf), Some(rf)) = (&l_parts[i], &r_parts[i]) else {
                    continue;
                };
                let l_batches = read_spill(lf)?;
                let r_batches = read_spill(rf)?;
                let l_mem = MemorySourceConfig::try_new_exec(&[l_batches], left.schema(), None)?;
                let r_mem = MemorySourceConfig::try_new_exec(&[r_batches], right.schema(), None)?;
                let pair = HashJoinExec::try_new(
                    l_mem,
                    r_mem,
                    on.clone(),
                    None,
                    &JoinType::Inner,
                    None,
                    PartitionMode::CollectLeft,
                    NullEquality::NullEqualsNothing,
                    false,
                )?;
                let mut out = pair.execute(0, Arc::clone(&context))?;
                while let Some(batch) = out.next().await {
                    if tx.send(batch).await.is_err() {
                        // Receiver dropped (LIMIT etc.) — stop producing.
                        return Ok(());
                    }
                }
            }
            Ok(())
        });
        Ok(builder.build())
    }
}

/// Stream every partition of `input`, scatter rows across `k` IPC spill
/// files by `hash(keys) % k`. Returns one optional spill file per
/// scatter partition (`None` = no rows landed there). Memory: O(k)
/// write buffers; the files are `DiskManager` temp files (RAII-cleaned
/// when the returned handles drop).
async fn scatter_side(
    input: &Arc<dyn ExecutionPlan>,
    key_exprs: &[Arc<dyn datafusion::physical_expr::PhysicalExpr>],
    k: usize,
    context: &Arc<TaskContext>,
) -> Result<Vec<Option<RefCountedTempFile>>> {
    let schema = input.schema();
    let random_state = RandomState::with_seeds(
        SCATTER_SEEDS.0,
        SCATTER_SEEDS.1,
        SCATTER_SEEDS.2,
        SCATTER_SEEDS.3,
    );
    let disk = context.runtime_env().disk_manager.clone();
    let mut writers: Vec<Option<(RefCountedTempFile, IpcFileWriter<std::fs::File>)>> =
        (0..k).map(|_| None).collect();
    let mut hash_buf: Vec<u64> = Vec::new();
    for p in 0..input.output_partitioning().partition_count() {
        let mut stream = input.execute(p, Arc::clone(context))?;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let keys: Vec<_> = key_exprs
                .iter()
                .map(|e| {
                    e.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<_>>()?;
            hash_buf.clear();
            hash_buf.resize(batch.num_rows(), 0);
            create_hashes(&keys, &random_state, &mut hash_buf)?;
            let mut indices: Vec<Vec<u32>> = vec![Vec::new(); k];
            for (row, h) in hash_buf.iter().enumerate() {
                indices[(*h % k as u64) as usize].push(row as u32);
            }
            for (i, idx) in indices.iter().enumerate() {
                if idx.is_empty() {
                    continue;
                }
                let sub = take_record_batch(&batch, &UInt32Array::from(idx.clone()))?;
                let w = match &mut writers[i] {
                    Some((_, w)) => w,
                    slot @ None => {
                        let tmp = disk.create_tmp_file("grace-hash-join")?;
                        let file = std::fs::File::create(tmp.path())?;
                        let w = IpcFileWriter::try_new(file, schema.as_ref())?;
                        &mut slot.insert((tmp, w)).1
                    }
                };
                w.write(&sub)?;
            }
        }
    }
    writers
        .into_iter()
        .map(|slot| match slot {
            Some((tmp, mut w)) => {
                w.finish()?;
                Ok(Some(tmp))
            }
            None => Ok(None),
        })
        .collect()
}

/// Read every batch of an IPC spill file back.
fn read_spill(file: &RefCountedTempFile) -> Result<Vec<RecordBatch>> {
    let f = std::fs::File::open(file.path())?;
    let reader = IpcFileReader::try_new(f, None)?;
    reader
        .into_iter()
        .map(|b| b.map_err(DataFusionError::from))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionContext;

    // Repo trap (de-flake 23078a3a): fixture columns must not end in
    // `key` — columns here are `ident`, `ref_a`, `tag`, `payload`.

    /// Left: dims (ident 0..n_left, tag "t<i%7>").
    fn left_batches(n: usize) -> (SchemaRef, Vec<RecordBatch>) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("ident", DataType::Int64, false),
            Field::new("tag", DataType::Utf8, false),
        ]));
        // Two batches to exercise batch boundaries.
        let mk = |lo: usize, hi: usize| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values((lo..hi).map(|i| i as i64))),
                    Arc::new(StringArray::from_iter_values(
                        (lo..hi).map(|i| format!("t{}", i % 7)),
                    )),
                ],
            )
            .unwrap()
        };
        let mid = n / 2;
        (Arc::clone(&schema), vec![mk(0, mid), mk(mid, n)])
    }

    /// Right: facts (ref_a = i % modulo — DUPLICATE keys — payload i*3).
    fn right_batches(n: usize, modulo: usize) -> (SchemaRef, Vec<RecordBatch>) {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("ref_a", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let mk = |lo: usize, hi: usize| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (lo..hi).map(|i| (i % modulo) as i64),
                    )),
                    Arc::new(Int64Array::from_iter_values((lo..hi).map(|i| i as i64 * 3))),
                ],
            )
            .unwrap()
        };
        let mid = n / 2;
        (Arc::clone(&schema), vec![mk(0, mid), mk(mid, n)])
    }

    fn on_ident_ref_a() -> JoinOn {
        vec![(
            Arc::new(Column::new("ident", 0)) as _,
            Arc::new(Column::new("ref_a", 0)) as _,
        )]
    }

    /// Sorted pretty-printed join output for order-insensitive parity.
    async fn sorted_rows(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<String> {
        let mut all = Vec::new();
        for p in 0..plan.output_partitioning().partition_count() {
            let stream = plan.execute(p, ctx.task_ctx()).unwrap();
            all.extend(collect(stream).await.unwrap());
        }
        let text = pretty_format_batches(&all).unwrap().to_string();
        let mut rows: Vec<String> = text.lines().map(|s| s.to_string()).collect();
        rows.sort();
        rows
    }

    fn stock_join(
        l: Arc<dyn ExecutionPlan>,
        r: Arc<dyn ExecutionPlan>,
        on: JoinOn,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                l,
                r,
                on,
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn mem_exec(schema: SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        MemorySourceConfig::try_new_exec(&[batches], schema, None).unwrap()
    }

    /// THE oracle: grace join == stock join, duplicate keys included,
    /// across scatter fan-outs (k = 1 degenerates to one pair; k = 8
    /// exercises real scattering).
    #[tokio::test(flavor = "multi_thread")]
    async fn parity_with_stock_inner_join_across_k() {
        let ctx = SessionContext::new();
        let (ls, lb) = left_batches(100);
        let (rs, rb) = right_batches(1_000, 60); // ref_a 0..60, heavy dups
        let stock = stock_join(
            mem_exec(Arc::clone(&ls), lb.clone()),
            mem_exec(Arc::clone(&rs), rb.clone()),
            on_ident_ref_a(),
        );
        let expect = sorted_rows(stock, &ctx).await;
        assert!(expect.len() > 4, "oracle join must be non-trivial");
        for k in [1usize, 2, 8] {
            let grace: Arc<dyn ExecutionPlan> = Arc::new(
                GraceHashJoinExec::try_new(
                    mem_exec(Arc::clone(&ls), lb.clone()),
                    mem_exec(Arc::clone(&rs), rb.clone()),
                    on_ident_ref_a(),
                    k,
                )
                .unwrap(),
            );
            let got = sorted_rows(grace, &ctx).await;
            assert_eq!(got, expect, "grace(k={k}) != stock join");
        }
    }

    /// Multi-key equi-join parity (both key columns participate in the
    /// scatter hash — a bug that hashed only the first key would still
    /// pass the single-key test).
    #[tokio::test(flavor = "multi_thread")]
    async fn parity_multi_key_join() {
        let ctx = SessionContext::new();
        // Both sides share (ident, payload%5) shapes: join on two cols.
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("ident", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]));
        let mk = |n: usize, m: i64| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(
                        (0..n).map(|i| (i as i64) % 13),
                    )),
                    Arc::new(Int64Array::from_iter_values((0..n).map(|i| (i as i64) % m))),
                ],
            )
            .unwrap()
        };
        let on: JoinOn = vec![
            (
                Arc::new(Column::new("ident", 0)) as _,
                Arc::new(Column::new("ident", 0)) as _,
            ),
            (
                Arc::new(Column::new("payload", 1)) as _,
                Arc::new(Column::new("payload", 1)) as _,
            ),
        ];
        let stock = stock_join(
            mem_exec(Arc::clone(&schema), vec![mk(200, 5)]),
            mem_exec(Arc::clone(&schema), vec![mk(300, 5)]),
            on.clone(),
        );
        let expect = sorted_rows(stock, &ctx).await;
        let grace: Arc<dyn ExecutionPlan> = Arc::new(
            GraceHashJoinExec::try_new(
                mem_exec(Arc::clone(&schema), vec![mk(200, 5)]),
                mem_exec(Arc::clone(&schema), vec![mk(300, 5)]),
                on,
                4,
            )
            .unwrap(),
        );
        let got = sorted_rows(grace, &ctx).await;
        assert_eq!(got, expect, "multi-key grace != stock");
    }

    /// An empty side yields an empty Inner join (and no spill panic).
    #[tokio::test(flavor = "multi_thread")]
    async fn empty_side_yields_empty_join() {
        let ctx = SessionContext::new();
        let (ls, _) = left_batches(0);
        let (rs, rb) = right_batches(50, 10);
        let grace: Arc<dyn ExecutionPlan> = Arc::new(
            GraceHashJoinExec::try_new(
                mem_exec(Arc::clone(&ls), vec![]),
                mem_exec(rs, rb),
                on_ident_ref_a(),
                4,
            )
            .unwrap(),
        );
        let stream = grace.execute(0, ctx.task_ctx()).unwrap();
        let batches = collect(stream).await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 0, "empty left → empty inner join");
    }

    /// Executing twice works (no consumed state on the exec node) —
    /// trials re-execute the same plan.
    #[tokio::test(flavor = "multi_thread")]
    async fn re_execution_is_stable() {
        let ctx = SessionContext::new();
        let (ls, lb) = left_batches(40);
        let (rs, rb) = right_batches(200, 30);
        let grace: Arc<dyn ExecutionPlan> = Arc::new(
            GraceHashJoinExec::try_new(mem_exec(ls, lb), mem_exec(rs, rb), on_ident_ref_a(), 3)
                .unwrap(),
        );
        let a = sorted_rows(Arc::clone(&grace), &ctx).await;
        let b = sorted_rows(grace, &ctx).await;
        assert_eq!(a, b, "re-execution must reproduce the join");
        assert!(a.len() > 4);
    }
}
