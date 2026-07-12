//! Σ.MG.2 — plan-embedded bloom transport.
//!
//! gRPC headers cap at ~8 KB, so the header path
//! ([`crate::bloom_flight`]) can only ship toy blooms (≤ ~50 K build
//! keys). The blooms that matter — Q07's nation-filtered supplier at
//! SF=100 is ~80 K keys, ~800 K at SF=1000 — ride HERE instead: a
//! [`PhysicalExtensionCodec`] serializes
//! [`BloomFilterExec`](ematix_flow_core::bloom::BloomFilterExec)
//! nodes INSIDE the stage plan protobuf (MB-scale payload limits),
//! registered on both sides via
//! `datafusion_distributed::DistributedExt::with_distributed_user_codec`.
//!
//! ## Coordinator side — [`EmbeddedBloomRule`]
//!
//! Blooms are per-query and produced asynchronously (the emitter
//! pre-executes build sides), but physical optimizer rules are fixed
//! at session build. The bridge is a take-once slot: the execution
//! path fills [`BloomSlot`] right before planning; the rule (which
//! MUST be installed AFTER the mesh-gate/stage-splitter — see below)
//! TAKES the slot and wraps matching scans via the existing
//! [`EnableContextBloomRule`](ematix_flow_core::context_bloom_rule::EnableContextBloomRule)
//! machinery. The network-boundary nodes expose their stage bodies
//! through `children()`/`with_new_children`, so the post-split
//! traversal wraps scans INSIDE the frozen stages and the codec
//! ships the wrap in the stage protobuf.
//!
//! ## Ordering is load-bearing (hang #3, 2026-07-12)
//!
//! Wrapping BEFORE the splitter changes the splitter's topology
//! decisions: datafusion-distributed's plan annotator exempts a LEAF
//! directly under `CoalescePartitionsExec` / `SortPreservingMergeExec`
//! from getting a network boundary, and a bloom-wrapped scan is no
//! longer a leaf. On TPC-H Q02 at SF100 that flipped a CollectLeft
//! build side from a local scan into a remote NetworkCoalesce stage —
//! a shape that deadlocks a real fleet (workers stream their data,
//! end-of-stream never arrives, coordinator parks forever) while a
//! localhost mesh tolerates it. The
//! `post_splitter_bloom_wrap_is_topology_invariant` test pins the
//! invariant: armed vs empty slot must plan an identical topology.
//!
//! Take-once is the correctness property: a bloom prunes rows by a
//! specific build-side key set, so a stale bloom applied to the WRONG
//! query would silently drop rows. An empty slot means no wrapping —
//! missing blooms cost pruning, never correctness. Callers that plan
//! concurrently on one session must serialize fill→plan (the
//! campaign and `DistributedBackend` both do).
//!
//! ## Worker side
//!
//! `flow-worker` registers [`BloomExecCodec`] in its session builder;
//! decoding reconstructs the exec around the child with the bloom
//! bytes from the payload. Workers must roll BEFORE coordinators
//! start emitting plan-embedded blooms (an old worker fails loudly on
//! the unknown extension node — never silently wrong).

use std::sync::{Arc, Mutex};

use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use ematix_flow_core::bloom::{BloomFilter, BloomFilterExec, ContextBlooms};
use ematix_flow_core::context_bloom_rule::EnableContextBloomRule;

/// Marker prefix so the decoder can reject payloads that aren't ours
/// (multiple user codecs may be chained; each must fail cleanly on
/// foreign bytes).
const CODEC_TAG: &[u8; 4] = b"EBL1";

/// `PhysicalExtensionCodec` for [`BloomFilterExec`]: `[tag:4][key_col_idx
/// u64 LE][bloom bytes]`, one child.
#[derive(Debug, Default)]
pub struct BloomExecCodec;

impl PhysicalExtensionCodec for BloomExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if buf.len() < 12 || &buf[0..4] != CODEC_TAG {
            return Err(DataFusionError::Internal(
                "BloomExecCodec: not an EBL1 payload".into(),
            ));
        }
        let key_col_idx = u64::from_le_bytes(buf[4..12].try_into().unwrap()) as usize;
        let bloom = BloomFilter::from_bytes(&buf[12..])
            .map_err(|e| DataFusionError::Internal(format!("BloomExecCodec: {e:?}")))?;
        let [input] = inputs else {
            return Err(DataFusionError::Internal(format!(
                "BloomExecCodec: expected exactly 1 input, got {}",
                inputs.len()
            )));
        };
        Ok(Arc::new(BloomFilterExec::try_new(
            Arc::clone(input),
            key_col_idx,
            Arc::new(bloom),
        )?))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> DfResult<()> {
        let Some(exec) = node.as_any().downcast_ref::<BloomFilterExec>() else {
            return Err(DataFusionError::Internal(
                "BloomExecCodec: not a BloomFilterExec".into(),
            ));
        };
        buf.extend_from_slice(CODEC_TAG);
        buf.extend_from_slice(&(exec.key_col_idx() as u64).to_le_bytes());
        buf.extend_from_slice(&exec.bloom().to_bytes());
        Ok(())
    }
}

/// Take-once, per-query bloom hand-off between the async execution
/// path (which emits blooms) and the sync physical optimizer (which
/// wraps scans). See module docs for the correctness argument.
#[derive(Debug, Clone, Default)]
pub struct BloomSlot {
    inner: Arc<Mutex<Option<ContextBlooms>>>,
}

impl BloomSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Arm the slot for the NEXT planned query.
    pub fn fill(&self, blooms: std::collections::HashMap<String, Arc<BloomFilter>>) {
        *self.inner.lock().expect("bloom slot poisoned") = Some(ContextBlooms::new(blooms));
    }

    /// Disarm (e.g. a query that emitted nothing must not inherit the
    /// previous query's blooms — same invariant as the header path's
    /// always-set semantics).
    pub fn clear(&self) {
        *self.inner.lock().expect("bloom slot poisoned") = None;
    }

    fn take(&self) -> Option<ContextBlooms> {
        self.inner.lock().expect("bloom slot poisoned").take()
    }
}

/// Coordinator-side physical rule: takes the slot and wraps matching
/// scans with `BloomFilterExec`. Install AFTER the adaptive mesh
/// gate: the wrap must happen on the post-split plan so the stage
/// topology is already frozen (pre-split wrapping defeated the
/// splitter's leaf exemption and deadlocked the fleet — see the
/// module docs' "Ordering is load-bearing" section).
#[derive(Debug)]
pub struct EmbeddedBloomRule {
    slot: BloomSlot,
}

impl EmbeddedBloomRule {
    pub fn new(slot: BloomSlot) -> Self {
        Self { slot }
    }
}

impl PhysicalOptimizerRule for EmbeddedBloomRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let Some(blooms) = self.slot.take() else {
            return Ok(plan);
        };
        EnableContextBloomRule::new(blooms).optimize(plan, config)
    }

    fn name(&self) -> &str {
        "ematix_embedded_bloom"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::displayable;
    use datafusion_proto::physical_plan::AsExecutionPlan;
    use datafusion_proto::protobuf::PhysicalPlanNode;

    fn small_bloom(keys: &[i64]) -> BloomFilter {
        let mut b = BloomFilter::with_capacity(keys.len().max(1), 12);
        for k in keys {
            b.insert_i64(*k);
        }
        b
    }

    /// A real parquet-backed arrow scan (file `t.parquet` → table
    /// stem `t`, the identity the uuid match needs — MemTable scans
    /// have no file identity and can never match a bloom).
    async fn parquet_scan(
        ctx: &datafusion::prelude::SessionContext,
    ) -> (tempfile::TempDir, Arc<dyn ExecutionPlan>) {
        use datafusion::arrow::array::{Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::parquet::arrow::ArrowWriter;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("t.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..100i64).collect::<Vec<_>>()))],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, None).unwrap();
        w.write(&rb).unwrap();
        w.close().unwrap();
        ctx.register_parquet("t", path.to_str().unwrap(), Default::default())
            .await
            .unwrap();
        let plan = ctx
            .sql("SELECT k FROM t")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        (tmp, plan)
    }

    /// Encode → decode round-trips the exec: same key column, and the
    /// decoded bloom answers membership identically.
    #[tokio::test(flavor = "multi_thread")]
    async fn codec_round_trips_bloom_exec() {
        let ctx = datafusion::prelude::SessionContext::new();
        let (_tmp, scan) = parquet_scan(&ctx).await;
        let bloom = small_bloom(&[7, 42, 99]);
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(BloomFilterExec::try_new(scan, 0, Arc::new(bloom)).unwrap());

        let codec = BloomExecCodec;
        let node =
            PhysicalPlanNode::try_from_physical_plan(Arc::clone(&exec), &codec).expect("encode");
        let task_ctx = ctx.task_ctx();
        let decoded = node
            .try_into_physical_plan(&task_ctx, &codec)
            .expect("decode");
        let d = decoded
            .as_any()
            .downcast_ref::<BloomFilterExec>()
            .expect("decoded BloomFilterExec");
        assert_eq!(d.key_col_idx(), 0);
        assert!(d.bloom().might_contain_i64(42), "inserted key survives");
        assert!(
            !d.bloom().might_contain_i64(1_000_003),
            "absent key still absent (same bits)"
        );
    }

    /// Foreign payloads are rejected loudly (codec chaining safety).
    #[tokio::test(flavor = "multi_thread")]
    async fn codec_rejects_foreign_bytes() {
        let ctx = datafusion::prelude::SessionContext::new();
        let (_tmp, scan) = parquet_scan(&ctx).await;
        let task_ctx = ctx.task_ctx();
        let err = BloomExecCodec.try_decode(b"NOPEnope", &[scan], &task_ctx);
        assert!(err.is_err());
    }

    /// Σ.MG.2 hang #3 regression (2026-07-12, Q02 SF100): bloom
    /// insertion must NOT change the distributed stage topology.
    ///
    /// Pre-fix, `EmbeddedBloomRule` ran BEFORE the stage splitter.
    /// Wrapping a scan in `BloomFilterExec` defeated the splitter's
    /// leaf exemption (datafusion-distributed `plan_annotator.rs`
    /// skips the network boundary below `CoalescePartitionsExec` /
    /// `SortPreservingMergeExec` only when the child is a LEAF), so a
    /// CollectLeft build side that planned as a local scan became a
    /// remote `NetworkCoalesceExec` stage — a topology that
    /// deadlocked the real 4-node fleet on Q02: workers streamed
    /// their data, end-of-stream never arrived, coordinator parked
    /// forever. Localhost meshes tolerate the shape, so only a fleet
    /// (or this plan-shape assertion) catches it.
    ///
    /// The rule therefore runs AFTER the gate/splitter and wraps
    /// scans inside the already-frozen stages: planning the same
    /// query with an armed slot vs an empty slot must produce an
    /// IDENTICAL network topology, with blooms present only as
    /// in-place wrappers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn post_splitter_bloom_wrap_is_topology_invariant() {
        use crate::mesh_gate::{AdaptiveMeshGateRule, MeshGateConfig};
        use datafusion::arrow::array::{Int64Array, RecordBatch};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use datafusion_distributed::DistributedExt;

        // Multi-file tables in the parted naming convention
        // (`<table>-NNNN.parquet`). The Q02 shape that flipped needs
        // a CollectLeft BUILD side that is a FILTERED multi-partition
        // scan: the planner coalesces the build input
        // (`CoalescePartitionsExec → FilterExec → scan`), the
        // annotator sees a non-leaf child under the coalesce and
        // gives that subtree its own NetworkCoalesce stage — the
        // stage the blooms must wrap INSIDE OF. `fact` (3 parts,
        // hard-filtered below) is that build side; `dim` (2 parts,
        // 5000 rows each) is the big probe side. Columns follow the
        // fixture rules: never `*key`.
        fn write_parts(dir: &Path, table: &str, col_a: &str, col_b: &str, parts: usize, rows: i64) {
            let tdir = dir.join(table);
            std::fs::create_dir_all(&tdir).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new(col_a, DataType::Int64, false),
                Field::new(col_b, DataType::Int64, false),
            ]));
            for p in 0..parts {
                let base = (p as i64) * 10_000;
                let rb = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from((base..base + rows).collect::<Vec<i64>>())),
                        Arc::new(Int64Array::from(
                            (0..rows).map(|v| v % 50).collect::<Vec<i64>>(),
                        )),
                    ],
                )
                .unwrap();
                let f = std::fs::File::create(tdir.join(format!("{table}-{:04}.parquet", p + 1)))
                    .unwrap();
                let mut w = ArrowWriter::try_new(f, schema.clone(), None).unwrap();
                w.write(&rb).unwrap();
                w.close().unwrap();
            }
        }
        use std::path::Path;
        let tmp = tempfile::tempdir().unwrap();
        write_parts(tmp.path(), "dim", "ident", "val", 2, 5000);
        write_parts(tmp.path(), "fact", "ref_a", "payload", 3, 200);

        // Session shape = the campaign's distributed arm post-fix:
        // gate FIRST (forced ON → the inner stage splitter always
        // runs), bloom wrap AFTER — on the split plan.
        let slot = BloomSlot::new();
        let resolver = crate::StaticWorkerResolver {
            urls: (1..=3)
                .map(|i| url::Url::parse(&format!("http://127.0.0.1:{}", 50050 + i)).unwrap())
                .collect(),
        };
        let builder = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_collect_statistics(true)
                    .with_target_partitions(4),
            )
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(AdaptiveMeshGateRule::new(
                MeshGateConfig::forced(),
            )))
            .with_physical_optimizer_rule(Arc::new(EmbeddedBloomRule::new(slot.clone())))
            .with_distributed_user_codec(BloomExecCodec)
            .with_distributed_worker_resolver(resolver)
            .with_distributed_files_per_task(1)
            .expect("files_per_task = 1 is valid");
        let ctx = SessionContext::new_with_state(builder.build());
        for t in ["dim", "fact"] {
            ctx.register_parquet(t, tmp.path().join(t).to_str().unwrap(), Default::default())
                .await
                .unwrap();
        }
        // The hard filter (payload = 49 keeps ~2% of fact) makes the
        // filtered fact the CollectLeft BUILD side; the planner
        // coalesces it and the annotator stages the coalesced
        // subtree (non-leaf child under CoalescePartitionsExec) —
        // the exact Q02 mechanism. GROUP BY adds the hash-shuffle
        // path for good measure.
        let sql = "SELECT d.ident, count(*) FROM dim d \
                   JOIN fact f ON d.ident = f.ref_a WHERE f.payload = 49 \
                   GROUP BY d.ident";
        let plan_once = || async {
            let p = ctx
                .sql(sql)
                .await
                .unwrap()
                .create_physical_plan()
                .await
                .unwrap();
            format!("{}", displayable(p.as_ref()).indent(true))
        };

        // Leg A: slot empty → no blooms, the splitter's native topology.
        slot.clear();
        let bare = plan_once().await;
        // Leg B: slot armed for both tables → blooms wrap in place.
        let mut blooms = std::collections::HashMap::new();
        blooms.insert("dim.ident".to_string(), Arc::new(small_bloom(&[1, 2, 3])));
        blooms.insert("fact.ref_a".to_string(), Arc::new(small_bloom(&[1, 2, 3])));
        slot.fill(blooms);
        let bloomed = plan_once().await;

        // Vacuity guards: the splitter must have engaged, and the
        // armed leg must actually wrap something.
        assert!(
            bare.contains("NetworkCoalesceExec") || bare.contains("NetworkShuffleExec"),
            "fixture must produce a real network stage:\n{bare}"
        );
        assert!(
            !bare.contains("BloomFilterExec") && bloomed.contains("BloomFilterExec"),
            "armed slot must wrap scans (and only the armed leg):\nbare:\n{bare}\nbloomed:\n{bloomed}"
        );

        // THE invariant: identical stage topology. Compare the
        // ordered list of topology-bearing lines (indentation
        // stripped — an in-place wrapper legitimately shifts child
        // indentation, and that is the ONLY change it may make).
        // `contains`, not `starts_with`: the renderer prefixes
        // boundary nodes with "[Stage N] => ", and keeping the full
        // line pins stage NUMBERING and task counts too.
        fn topology(rendered: &str) -> Vec<&str> {
            rendered
                .lines()
                .map(str::trim_start)
                .filter(|l| {
                    [
                        "DistributedExec",
                        "NetworkCoalesceExec",
                        "NetworkShuffleExec",
                        "NetworkBroadcastExec",
                        "PartitionIsolatorExec",
                        "DataSourceExec",
                    ]
                    .iter()
                    .any(|p| l.contains(p))
                })
                .collect()
        }
        assert_eq!(
            topology(&bare),
            topology(&bloomed),
            "bloom insertion changed the stage topology\nbare:\n{bare}\nbloomed:\n{bloomed}"
        );

        // Shipping property: at least one bloom sits INSIDE a network
        // stage (below a network boundary node) — that is the wrap the
        // codec serializes to workers.
        let mut boundary_indent: Option<usize> = None;
        let mut in_stage_bloom = false;
        for line in bloomed.lines() {
            let indent = line.len() - line.trim_start().len();
            let t = line.trim_start();
            if t.contains("NetworkCoalesceExec") || t.contains("NetworkShuffleExec") {
                boundary_indent = Some(indent);
            } else if let Some(b) = boundary_indent {
                if indent <= b {
                    boundary_indent = None;
                } else if t.starts_with("BloomFilterExec") {
                    in_stage_bloom = true;
                    break;
                }
            }
        }
        assert!(
            in_stage_bloom,
            "no BloomFilterExec inside a network stage — blooms would never ship:\n{bloomed}"
        );
    }

    /// The slot is take-once: the first optimize wraps, the second
    /// (same slot, next query) sees nothing — a query can never
    /// inherit the previous query's blooms.
    #[tokio::test(flavor = "multi_thread")]
    async fn slot_is_take_once() {
        let ctx = datafusion::prelude::SessionContext::new();
        let (_tmp, scan) = parquet_scan(&ctx).await;
        let slot = BloomSlot::new();
        let mut blooms = std::collections::HashMap::new();
        blooms.insert("t.k".to_string(), Arc::new(small_bloom(&[1, 2, 3])));
        slot.fill(blooms);
        let rule = EmbeddedBloomRule::new(slot.clone());
        let cfg = ConfigOptions::default();

        let first = rule.optimize(Arc::clone(&scan), &cfg).expect("first");
        let first_txt = format!("{}", displayable(first.as_ref()).indent(true));
        assert!(
            first_txt.contains("BloomFilterExec"),
            "armed slot must wrap the scan:\n{first_txt}"
        );

        let second = rule.optimize(Arc::clone(&scan), &cfg).expect("second");
        assert!(
            Arc::ptr_eq(&scan, &second),
            "slot must be empty on the next query (take-once)"
        );
    }
}
