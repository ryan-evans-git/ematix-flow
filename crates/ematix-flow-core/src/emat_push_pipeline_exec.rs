//! PV.3 — `EmatPushPipelineExec`: a DataFusion `ExecutionPlan` that fuses a
//! `scan → (filter/project) → N FK-reduction joins → Partial-agg-input`
//! fragment into one push pass, materialising a `RecordBatch` ONLY at its
//! output boundary (the −16% Q08 lever, ADR §1.3).
//!
//! ## PV.3 generalisation (vs the PV.2 hand-built Q08 node)
//!
//! PV.2 hardcoded Q08's emit and took pre-built `KernelProbes`. PV.3 makes
//! the node **spec-driven** so a recognizer can construct it from a matched
//! plan fragment (no TPC-H hardcoding in the shipped path):
//!
//! - `probe_input` — the fact scan, projected to the columns the fused loop
//!   reads (keys + measure inputs).
//! - `builds: Vec<BuildBinding>` — each dimension reduction, bound to a fact
//!   FK column. Its [`ProbeStructure`] is either supplied pre-built
//!   ([`BuildSource::Prebuilt`], for perf harnesses) or a dim **subplan**
//!   ([`BuildSource::Plan`]) collected once via `OnceCell` and fed to
//!   [`choose`] — the same build-once-share-across-probe-partitions pattern
//!   as `EmatixHashJoinExec`.
//! - `emit: Vec<EmitCol>` — how each OUTPUT column is produced (a fact
//!   passthrough, the `price*(1-disc)` revenue idiom, or an i64 build
//!   payload). The output `schema` is **authoritative** — the recognizer
//!   passes the matched projection's exact output schema so the node can
//!   splice under the existing stock aggregate unchanged.
//!
//! ## Correctness invariants (architect review, PV.3)
//!
//! - **C-R3 (NULL FK):** the dense `.values()` hot path ignores Arrow
//!   validity, so the fuse loop checks each probe FK column's null bit and
//!   **drops** a row whose FK is NULL (inner-join semantics: NULL never
//!   matches). A build key that is NULL is dropped at build time.
//! - **C-R4 (non-unique payload):** a payload-carrying build MUST be key
//!   unique (a dimension PK). If [`choose`] routes it to a multi-match
//!   structure, the node **errors loudly at build** rather than silently
//!   keeping one payload — the recognizer must only bind PK-unique payload
//!   joins. Loud failure ≫ silent wrong sum (this node is default-OFF).
//! - Membership builds may be non-unique (dups are irrelevant to present/
//!   absent), so they never error.

use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int64Builder};
use arrow_array::{Array, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use ematix_flow_push::{ProbeStructure, choose};
use futures_util::{StreamExt, TryFutureExt, TryStreamExt};
use tokio::sync::OnceCell;

/// PV.3b profiling counters (nanoseconds, opt-in). `BUILD_NANOS` = wall time of
/// the once-only dim-probe construction (`build_structure` over all bindings);
/// `PROBE_NANOS` = summed `fuse_batch` CPU across probe partitions. Reset + read
/// by `examples/pv3b_q08_profile.rs` to split the fused cost build-vs-probe.
pub static BUILD_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PROBE_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// PV.4.0: summed wall time of the concurrent fact-decode task across probe
/// partitions (overlap path only). Lets the spike confirm the build hides under
/// the decode rather than serializing before it.
pub static DECODE_NANOS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// PV.4.0 spike gate: returns `Some(buffer_morsels)` when `EMAT_PV4_OVERLAP=1`,
/// reading the per-partition decode-ahead bound from `EMAT_PV4_BUFFER`
/// (default 64). Read ONCE at planning time by `FusedProbePlanner` — never on
/// the hot path. Unset → `None` → the byte-identical serial build-then-probe
/// path. (The bound caps in-flight fact bytes: a morsel of the Q08 fact
/// projection is ~328 KB, so 64 × target_partitions ≈ a few hundred MB.)
pub fn pv4_overlap_from_env() -> Option<usize> {
    match std::env::var("EMAT_PV4_OVERLAP") {
        Ok(v) if v == "1" || v.eq_ignore_ascii_case("true") => {
            let buf = std::env::var("EMAT_PV4_BUFFER")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(64);
            Some(buf.max(1))
        }
        _ => None,
    }
}

/// Where a binding's [`ProbeStructure`] comes from.
pub enum BuildSource {
    /// Already collected + chosen (perf harnesses, tests).
    Prebuilt(Arc<ProbeStructure>),
    /// A dimension subplan collected once at execute time and fed to
    /// [`choose`]. `key_col`/`payload_col` index the subplan's OUTPUT.
    Plan {
        plan: Arc<dyn ExecutionPlan>,
        key_col: usize,
        payload_col: Option<usize>,
    },
}

/// One dimension reduction, bound to a fact (probe) FK column.
pub struct BuildBinding {
    pub source: BuildSource,
    /// Column index in the PROBE (fact) input carrying the FK to test.
    pub probe_fk_col: usize,
    /// Require the build key to be UNIQUE at build time. Set when replacing
    /// an INNER join with a membership reduction: an Inner join multiplies a
    /// probe row by the number of build matches, but a membership filter
    /// emits it once — so the swap is only value-equivalent if the build is
    /// key-unique (a dimension PK). LeftSemi joins don't multiply, so they
    /// set `false`. Payload builds are always checked for uniqueness
    /// regardless (a non-unique payload is ambiguous). Violation → loud error.
    pub require_unique: bool,
}

impl BuildBinding {
    /// Does this binding recover an i64 payload (inner join carrying a build
    /// column), vs pure membership (semijoin)?
    fn carries_payload(&self) -> bool {
        match &self.source {
            BuildSource::Prebuilt(ps) => matches!(
                ps.as_ref(),
                ProbeStructure::DensePayload { .. }
                    | ProbeStructure::HashTable(_)
                    | ProbeStructure::HashMultiTable(_)
            ),
            BuildSource::Plan { payload_col, .. } => payload_col.is_some(),
        }
    }
}

/// How one output column is produced from a surviving fused row.
pub enum EmitCol {
    /// Pass a fact (probe) column through unchanged (Int64/Float64).
    ProbeColumn { col: usize },
    /// The TPC-H revenue idiom `price * (1 - discount)` over two fact f64
    /// columns → Float64.
    ProbeRevenue { price_col: usize, disc_col: usize },
    /// The i64 payload recovered from build `build_idx` for this row.
    BuildPayload { build_idx: usize },
}

pub struct EmatPushPipelineExec {
    probe_input: Arc<dyn ExecutionPlan>,
    builds: Arc<Vec<BuildBinding>>,
    emit: Arc<Vec<EmitCol>>,
    /// Authoritative output schema (recognizer passes the matched
    /// projection's exact schema so the node splices under the stock agg).
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Resolved structures (one per binding), built once across probe
    /// partitions. `Prebuilt` bindings clone their Arc; `Plan` bindings are
    /// collected + `choose`n here.
    built: Arc<OnceCell<Arc<Vec<Arc<ProbeStructure>>>>>,
    /// PV.4.0: when `true`, decode the fact CONCURRENTLY with the dim build
    /// (bounded morsel channel) instead of after it. Default `false` (serial,
    /// byte-identical to PV.3b). Set by the planner from `EMAT_PV4_OVERLAP`.
    overlap: bool,
    /// PV.4.0: per-partition decode-ahead bound (morsels) for the overlap path.
    overlap_buf: usize,
}

impl EmatPushPipelineExec {
    pub fn new(
        probe_input: Arc<dyn ExecutionPlan>,
        builds: Vec<BuildBinding>,
        emit: Vec<EmitCol>,
        output_schema: SchemaRef,
    ) -> Self {
        assert_eq!(
            emit.len(),
            output_schema.fields().len(),
            "emit spec must have one entry per output field"
        );
        let eq = EquivalenceProperties::new(output_schema.clone());
        // The fused fragment is per-partition like the scan it replaces.
        let partitioning = probe_input.output_partitioning().clone();
        let properties = Arc::new(PlanProperties::new(
            eq,
            partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            probe_input,
            builds: Arc::new(builds),
            emit: Arc::new(emit),
            schema: output_schema,
            properties,
            built: Arc::new(OnceCell::new()),
            overlap: false,
            overlap_buf: 0,
        }
    }

    /// PV.4.0: enable the overlap execution path with a per-partition bounded
    /// decode buffer of `buf` morsels (decode the fact concurrently with the
    /// dim build). Off by default; the planner calls this when
    /// [`pv4_overlap_from_env`] is `Some`.
    pub fn with_overlap(mut self, buf: usize) -> Self {
        self.overlap = true;
        self.overlap_buf = buf.max(1);
        self
    }

    /// Plan children in stable order: `[probe_input, <each Plan build's plan>]`.
    /// Prebuilt builds contribute no child.
    fn plan_build_children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.builds
            .iter()
            .filter_map(|b| match &b.source {
                BuildSource::Plan { plan, .. } => Some(plan),
                BuildSource::Prebuilt(_) => None,
            })
            .collect()
    }
}

impl std::fmt::Debug for EmatPushPipelineExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EmatPushPipelineExec(builds={}, emit={})",
            self.builds.len(),
            self.emit.len()
        )
    }
}

impl DisplayAs for EmatPushPipelineExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let kinds: Vec<&str> = self
            .builds
            .iter()
            .map(|b| {
                if b.carries_payload() {
                    "payload"
                } else {
                    "member"
                }
            })
            .collect();
        write!(
            f,
            "EmatPushPipelineExec: builds=[{}], out_cols={}",
            kinds.join(","),
            self.emit.len()
        )
    }
}

impl ExecutionPlan for EmatPushPipelineExec {
    fn name(&self) -> &str {
        "EmatPushPipelineExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut v = vec![&self.probe_input];
        v.extend(self.plan_build_children());
        v
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let expected = 1 + self.plan_build_children().len();
        if children.len() != expected {
            return Err(DataFusionError::Internal(format!(
                "EmatPushPipelineExec expects {expected} children, got {}",
                children.len()
            )));
        }
        let new_probe = children[0].clone();
        // Re-thread the new Plan children back into the bindings in order.
        let mut next = children[1..].iter();
        let mut builds = Vec::with_capacity(self.builds.len());
        for b in self.builds.iter() {
            let source = match &b.source {
                BuildSource::Prebuilt(ps) => BuildSource::Prebuilt(ps.clone()),
                BuildSource::Plan {
                    key_col,
                    payload_col,
                    ..
                } => BuildSource::Plan {
                    plan: next.next().expect("child count checked above").clone(),
                    key_col: *key_col,
                    payload_col: *payload_col,
                },
            };
            builds.push(BuildBinding {
                source,
                probe_fk_col: b.probe_fk_col,
                require_unique: b.require_unique,
            });
        }
        let emit = self.emit.iter().map(clone_emit).collect();
        let node = Self::new(new_probe, builds, emit, self.schema.clone());
        let node = if self.overlap {
            node.with_overlap(self.overlap_buf)
        } else {
            node
        };
        Ok(Arc::new(node))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Claim the probe partition's stream SYNCHRONOUSLY at execute time. If
        // the probe input is a `RepartitionExec`, each output partition must
        // call `execute(p)` up-front (before any awaiting) — otherwise the
        // `OnceCell` build below delays these calls, the partitions stampede
        // the repartition channels out of order, and it trips its internal
        // "partition not used yet" invariant (intermittent panic).
        let probe_stream = self.probe_input.execute(partition, ctx.clone())?;
        let built = self.built.clone();
        let builds = self.builds.clone();
        let emit = self.emit.clone();
        let out_schema = self.schema.clone();
        let stream_schema = self.schema.clone();
        let build_ctx = ctx;

        // ---- PV.4.0 overlap path ----------------------------------------------
        // The serial path below awaits the dim build BEFORE the probe stream is
        // polled, so the (~90 ms) build is a serial prefix in front of the
        // (~140 ms) fact decode — PV.3b proved that prefix IS the +53%. Here a
        // spawned decoder pushes fact morsels into a bounded channel CONCURRENTLY
        // with the build, so the build hides under the decode; backpressure at
        // `overlap_buf` morsels bounds the in-flight bytes. The fuse still awaits
        // the build (it needs every probe structure), but the DECODE does not.
        if self.overlap {
            let cap = self.overlap_buf.max(1);
            let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(cap);
            let mut ps = probe_stream;
            tokio::spawn(async move {
                let __dt0 = std::time::Instant::now();
                while let Some(item) = ps.next().await {
                    if tx.send(item).await.is_err() {
                        break; // consumer dropped (e.g. LIMIT) — stop decoding.
                    }
                }
                DECODE_NANOS.fetch_add(
                    __dt0.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            });

            let fut = async move {
                let structures = resolve_structures(&built, &builds, &build_ctx).await?;
                // Drain the (already partly-decoded) channel, fusing each morsel.
                let out = futures_util::stream::unfold(
                    (rx, structures, builds, emit, out_schema),
                    |(mut rx, structures, builds, emit, out_schema)| async move {
                        let item = rx.recv().await?; // None → stream end
                        let res = match item {
                            Ok(rb) => {
                                let __pt0 = std::time::Instant::now();
                                let r = fuse_batch(&rb, &structures, &builds, &emit, &out_schema);
                                PROBE_NANOS.fetch_add(
                                    __pt0.elapsed().as_nanos() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                r
                            }
                            Err(e) => Err(e),
                        };
                        Some((res, (rx, structures, builds, emit, out_schema)))
                    },
                );
                Ok::<_, DataFusionError>(out)
            };
            let s = fut.try_flatten_stream();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, s)));
        }

        // ---- Default (serial) path: build all dims, THEN map the probe stream.
        let fut = async move {
            let structures = resolve_structures(&built, &builds, &build_ctx).await?;
            let mapped = probe_stream.map(move |rb| {
                let rb = rb?;
                let __pt0 = std::time::Instant::now();
                let r = fuse_batch(&rb, &structures, &builds, &emit, &out_schema);
                PROBE_NANOS.fetch_add(
                    __pt0.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                r
            });
            Ok::<_, DataFusionError>(mapped)
        };

        let s = fut.try_flatten_stream();
        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, s)))
    }
}

fn clone_emit(e: &EmitCol) -> EmitCol {
    match e {
        EmitCol::ProbeColumn { col } => EmitCol::ProbeColumn { col: *col },
        EmitCol::ProbeRevenue {
            price_col,
            disc_col,
        } => EmitCol::ProbeRevenue {
            price_col: *price_col,
            disc_col: *disc_col,
        },
        EmitCol::BuildPayload { build_idx } => EmitCol::BuildPayload {
            build_idx: *build_idx,
        },
    }
}

/// Read an i64 key at row `i` from an Int64/Int32 column, or `None` if NULL
/// or the column is an unsupported type.
#[inline]
fn key_at(arr: &dyn Array, i: usize) -> Option<i64> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        Some(a.value(i))
    } else {
        arr.as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(i) as i64)
    }
}

/// Resolve all probe structures ONCE, shared across probe partitions via the
/// `OnceCell`. Used by both the serial and the PV.4.0 overlap path. The wall
/// time is recorded in [`BUILD_NANOS`].
async fn resolve_structures(
    built: &OnceCell<Arc<Vec<Arc<ProbeStructure>>>>,
    builds: &[BuildBinding],
    ctx: &Arc<TaskContext>,
) -> DfResult<Arc<Vec<Arc<ProbeStructure>>>> {
    let s = built
        .get_or_try_init(|| async {
            let __bt0 = std::time::Instant::now();
            let mut out: Vec<Arc<ProbeStructure>> = Vec::with_capacity(builds.len());
            for b in builds.iter() {
                match &b.source {
                    BuildSource::Prebuilt(ps) => out.push(ps.clone()),
                    BuildSource::Plan {
                        plan,
                        key_col,
                        payload_col,
                    } => {
                        let ps =
                            build_structure(plan, *key_col, *payload_col, b.require_unique, ctx)
                                .await?;
                        out.push(Arc::new(ps));
                    }
                }
            }
            BUILD_NANOS.fetch_add(
                __bt0.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            Ok::<_, DataFusionError>(Arc::new(out))
        })
        .await?;
    Ok(s.clone())
}

/// Collect a `Plan` build's batches and pick its [`ProbeStructure`].
/// Skips rows whose key (or, for payload builds, payload) is NULL — they
/// cannot participate in an inner join. Errors if a payload build is not
/// key-unique (C-R4).
async fn build_structure(
    plan: &Arc<dyn ExecutionPlan>,
    key_col: usize,
    payload_col: Option<usize>,
    require_unique: bool,
    ctx: &Arc<TaskContext>,
) -> DfResult<ProbeStructure> {
    let nparts = plan.output_partitioning().partition_count();
    // Collect every partition CONCURRENTLY (one spawned task each). A serial
    // `for p in 0..nparts { execute(p).await }` loop drove each partition to
    // completion before the next — serialising the entire dim-subquery decode +
    // joins onto a single task, which dominated the in-plan fused cost at SF=10
    // (PV.3b: +86% vs stock with the serial loop). Stock runs all partitions in
    // parallel; the build must too.
    let mut tasks = Vec::with_capacity(nparts);
    for p in 0..nparts {
        let plan = plan.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = plan.execute(p, ctx)?;
            let mut keys: Vec<i64> = Vec::new();
            let mut payloads: Vec<i64> = Vec::new();
            while let Some(b) = s.try_next().await? {
                let kcol = b.column(key_col);
                let pcol = payload_col.map(|c| b.column(c));
                for i in 0..b.num_rows() {
                    let Some(k) = key_at(kcol.as_ref(), i) else {
                        continue;
                    };
                    match pcol {
                        None => keys.push(k),
                        Some(pc) => {
                            let Some(pv) = key_at(pc.as_ref(), i) else {
                                continue;
                            };
                            keys.push(k);
                            payloads.push(pv);
                        }
                    }
                }
            }
            Ok::<(Vec<i64>, Vec<i64>), DataFusionError>((keys, payloads))
        }));
    }
    let mut keys: Vec<i64> = Vec::new();
    let mut payloads: Vec<i64> = Vec::new();
    for t in tasks {
        let (k, p) = t
            .await
            .map_err(|e| DataFusionError::Internal(format!("build collect join error: {e}")))??;
        keys.extend(k);
        payloads.extend(p);
    }
    // C-R4 / multiplicity: an Inner-join membership reduction is only
    // value-equivalent to the join if the build is key-unique. Check it
    // explicitly (payload builds get a second guard below via `choose`).
    if require_unique {
        let mut seen = std::collections::HashSet::with_capacity(keys.len());
        for &k in &keys {
            if !seen.insert(k) {
                return Err(DataFusionError::Internal(
                    "EmatPushPipelineExec: build side is not key-unique but the \
                     join semantics (Inner) require uniqueness — refusing to \
                     change row multiplicity"
                        .into(),
                ));
            }
        }
    }
    let ps = choose(&keys, payload_col.map(|_| payloads.as_slice()));
    if payload_col.is_some() && ps.may_multi_match() {
        return Err(DataFusionError::Internal(
            "EmatPushPipelineExec: payload-carrying build is not key-unique \
             (recognizer must bind only PK-unique payload joins) — refusing to \
             silently drop matches"
                .into(),
        ));
    }
    Ok(ps)
}

/// A probe FK column resolved to its concrete type ONCE (before the row loop)
/// so the hot path does a direct slice load + null-bit test per row — never a
/// per-row `downcast_ref`/column lookup (that cost erased the fused win).
enum FkCol<'a> {
    I64(&'a Int64Array),
    I32(&'a Int32Array),
    Unsupported,
}

impl FkCol<'_> {
    #[inline]
    fn key(&self, i: usize) -> Option<i64> {
        match self {
            FkCol::I64(a) => (!a.is_null(i)).then(|| a.value(i)),
            FkCol::I32(a) => (!a.is_null(i)).then(|| a.value(i) as i64),
            FkCol::Unsupported => None,
        }
    }
}

fn resolve_fk(arr: &dyn Array) -> FkCol<'_> {
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        FkCol::I64(a)
    } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        FkCol::I32(a)
    } else {
        FkCol::Unsupported
    }
}

/// One fused push pass over a probe batch: for each row, test every build's
/// FK (NULL FK → drop; membership absent → drop; payload absent → drop),
/// and append the emit columns for survivors. Column accessors are resolved
/// ONCE before the loop (no per-row downcast).
fn fuse_batch(
    rb: &RecordBatch,
    structures: &[Arc<ProbeStructure>],
    builds: &[BuildBinding],
    emit: &[EmitCol],
    out_schema: &SchemaRef,
) -> DfResult<RecordBatch> {
    let n = rb.num_rows();
    // Hoist ALL per-build metadata + column accessors out of the row loop.
    let payload_flags: Vec<bool> = builds.iter().map(|b| b.carries_payload()).collect();
    let fk_cols: Vec<FkCol> = builds
        .iter()
        .map(|b| resolve_fk(rb.column(b.probe_fk_col).as_ref()))
        .collect();
    let payload_builds = payload_flags.iter().filter(|&&p| p).count();

    // Pass 1: survivors + per-payload-build captured payloads (survivor order).
    let mut survivors: Vec<u32> = Vec::with_capacity(n / 64 + 16);
    let mut payload_cols: Vec<Vec<i64>> = vec![Vec::new(); payload_builds];
    let mut row_pay: Vec<i64> = Vec::with_capacity(payload_builds);

    'row: for i in 0..n {
        row_pay.clear();
        for bi in 0..builds.len() {
            let Some(key) = fk_cols[bi].key(i) else {
                continue 'row; // C-R3: NULL FK never matches.
            };
            let ps = &structures[bi];
            if payload_flags[bi] {
                match ps.payload(key) {
                    Some(p) => row_pay.push(p),
                    None => continue 'row,
                }
            } else if !ps.contains(key) {
                continue 'row;
            }
        }
        survivors.push(i as u32);
        for (slot, &p) in row_pay.iter().enumerate() {
            payload_cols[slot].push(p);
        }
    }

    // Pass 2: build each output column from survivors per its EmitCol.
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(emit.len());
    let mut next_payload_slot = 0usize;
    // Map each BuildPayload's build_idx → its payload-column slot.
    let payload_slot_of: Vec<Option<usize>> = {
        let mut slot = 0usize;
        builds
            .iter()
            .map(|b| {
                if b.carries_payload() {
                    let s = slot;
                    slot += 1;
                    Some(s)
                } else {
                    None
                }
            })
            .collect()
    };

    for (e, field) in emit.iter().zip(out_schema.fields()) {
        let arr: arrow_array::ArrayRef = match e {
            EmitCol::ProbeColumn { col } => {
                build_passthrough(rb.column(*col), &survivors, field.data_type())?
            }
            EmitCol::ProbeRevenue {
                price_col,
                disc_col,
            } => {
                let price = f64col(rb.column(*price_col), *price_col)?;
                let disc = f64col(rb.column(*disc_col), *disc_col)?;
                let mut b = Float64Builder::with_capacity(survivors.len());
                for &s in &survivors {
                    let s = s as usize;
                    b.append_value(price.value(s) * (1.0 - disc.value(s)));
                }
                Arc::new(b.finish())
            }
            EmitCol::BuildPayload { build_idx } => {
                let slot = payload_slot_of
                    .get(*build_idx)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "EmitCol::BuildPayload build_idx={build_idx} is not a payload build"
                        ))
                    })?;
                debug_assert_eq!(slot, next_payload_slot);
                next_payload_slot += 1;
                let vals = &payload_cols[slot];
                let mut b = Int64Builder::with_capacity(vals.len());
                for &v in vals {
                    b.append_value(v);
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(arr);
    }

    RecordBatch::try_new(out_schema.clone(), arrays)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn f64col(a: &arrow_array::ArrayRef, idx: usize) -> DfResult<&Float64Array> {
    a.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| DataFusionError::Internal(format!("col {idx} not Float64")))
}

/// Gather `src[survivors]` into a new array of `want` type (Int64/Float64),
/// preserving nulls.
fn build_passthrough(
    src: &arrow_array::ArrayRef,
    survivors: &[u32],
    want: &DataType,
) -> DfResult<arrow_array::ArrayRef> {
    match want {
        DataType::Int64 => {
            let a = src
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("passthrough want Int64".into()))?;
            let mut b = Int64Builder::with_capacity(survivors.len());
            for &s in survivors {
                let s = s as usize;
                if a.is_null(s) {
                    b.append_null();
                } else {
                    b.append_value(a.value(s));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let a = src
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("passthrough want Float64".into()))?;
            let mut b = Float64Builder::with_capacity(survivors.len());
            for &s in survivors {
                let s = s as usize;
                if a.is_null(s) {
                    b.append_null();
                } else {
                    b.append_value(a.value(s));
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => Err(DataFusionError::Internal(format!(
            "EmatPushPipelineExec passthrough unsupported output type {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_schema::{Field, Schema};
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn i64a(v: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(v))
    }
    fn f64a(v: Vec<f64>) -> ArrayRef {
        Arc::new(Float64Array::from(v))
    }

    /// A 1-partition in-memory scan over a single batch (a fake scan/dim).
    async fn mem(
        ctx: &SessionContext,
        schema: SchemaRef,
        batch: RecordBatch,
    ) -> Arc<dyn ExecutionPlan> {
        let mt = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        mt.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    /// Membership reduction (part-like) + revenue emit, the clean de-risk
    /// shape. Fact has 5 rows; build keeps partkeys {10, 30}. Rows whose
    /// l_partkey ∉ {10,30} (and the NULL-FK row) must drop.
    #[tokio::test]
    async fn membership_plus_revenue_drops_nonmembers_and_null_fk() {
        // Fact: (l_partkey, l_extendedprice, l_discount)
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("l_partkey", DataType::Int64, true),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let fact = RecordBatch::try_new(
            fact_schema.clone(),
            vec![
                i64a(vec![Some(10), Some(20), Some(30), None, Some(10)]),
                f64a(vec![100.0, 200.0, 300.0, 400.0, 500.0]),
                f64a(vec![0.0, 0.1, 0.5, 0.0, 0.2]),
            ],
        )
        .unwrap();

        // Build (part): p_partkey {10, 30} — membership only.
        let dim_schema = Arc::new(Schema::new(vec![Field::new(
            "p_partkey",
            DataType::Int64,
            false,
        )]));
        let dim =
            RecordBatch::try_new(dim_schema.clone(), vec![i64a(vec![Some(10), Some(30)])]).unwrap();

        let ctx = SessionContext::new();
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "revenue",
            DataType::Float64,
            false,
        )]));
        let exec = Arc::new(EmatPushPipelineExec::new(
            mem(&ctx, fact_schema, fact).await,
            vec![BuildBinding {
                source: BuildSource::Plan {
                    plan: mem(&ctx, dim_schema, dim).await,
                    key_col: 0,
                    payload_col: None,
                },
                probe_fk_col: 0,
                require_unique: true,
            }],
            vec![EmitCol::ProbeRevenue {
                price_col: 1,
                disc_col: 2,
            }],
            out_schema,
        ));

        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let total: f64 = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .sum();
        // Survivors: row0 (pk10) 100*1.0=100, row2 (pk30) 300*0.5=150,
        // row4 (pk10) 500*0.8=400. row1(pk20) dropped, row3(NULL) dropped.
        assert!((total - 650.0).abs() < 1e-9, "revenue sum {total} != 650");
    }

    /// Inner join carrying an i64 payload (orders→year-like) + a fact
    /// passthrough. Verifies payload recovery aligns with survivors.
    #[tokio::test]
    async fn payload_build_recovers_aligned_values() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        // orderkeys 1,2,3,4 ; only 2,4 survive (in the dim) with years 1995,1996.
        let fact = RecordBatch::try_new(
            fact_schema.clone(),
            vec![
                i64a(vec![Some(1), Some(2), Some(3), Some(4)]),
                f64a(vec![10.0, 20.0, 30.0, 40.0]),
                f64a(vec![0.0, 0.0, 0.0, 0.0]),
            ],
        )
        .unwrap();
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_year", DataType::Int64, false),
        ]));
        let dim = RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                i64a(vec![Some(2), Some(4)]),
                i64a(vec![Some(1995), Some(1996)]),
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("o_year", DataType::Int64, false),
            Field::new("volume", DataType::Float64, false),
        ]));
        let exec = Arc::new(EmatPushPipelineExec::new(
            mem(&ctx, fact_schema, fact).await,
            vec![BuildBinding {
                source: BuildSource::Plan {
                    plan: mem(&ctx, dim_schema, dim).await,
                    key_col: 0,
                    payload_col: Some(1),
                },
                probe_fk_col: 0,
                require_unique: false,
            }],
            vec![
                EmitCol::BuildPayload { build_idx: 0 },
                EmitCol::ProbeRevenue {
                    price_col: 1,
                    disc_col: 2,
                },
            ],
            out_schema,
        ));
        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let b = &batches[0];
        assert_eq!(b.num_rows(), 2, "only orderkeys 2,4 survive");
        let years = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let vols = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        // row order = survivor order = orderkey 2 (year1995,vol20), 4 (year1996,vol40)
        assert_eq!(years.values(), &[1995, 1996]);
        assert_eq!(vols.values(), &[20.0, 40.0]);
    }

    /// C-R4: a payload build that is NOT key-unique must error loudly, not
    /// silently keep one match.
    #[tokio::test]
    async fn nonunique_payload_build_errors() {
        let fact_schema = Arc::new(Schema::new(vec![Field::new("fk", DataType::Int64, false)]));
        let fact = RecordBatch::try_new(fact_schema.clone(), vec![i64a(vec![Some(5)])]).unwrap();
        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("p", DataType::Int64, false),
        ]));
        // key 5 appears twice with different payloads → multi-match.
        let dim = RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                i64a(vec![Some(5), Some(5)]),
                i64a(vec![Some(100), Some(200)]),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        let out_schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Int64, false)]));
        let exec = Arc::new(EmatPushPipelineExec::new(
            mem(&ctx, fact_schema, fact).await,
            vec![BuildBinding {
                source: BuildSource::Plan {
                    plan: mem(&ctx, dim_schema, dim).await,
                    key_col: 0,
                    payload_col: Some(1),
                },
                probe_fk_col: 0,
                require_unique: false,
            }],
            vec![EmitCol::BuildPayload { build_idx: 0 }],
            out_schema,
        ));
        let err = collect(exec, ctx.task_ctx()).await.err();
        assert!(
            err.map(|e| e.to_string().contains("not key-unique"))
                .unwrap_or(false),
            "non-unique payload build must error"
        );
    }

    /// Multiplicity guard: an Inner-join membership reduction with a NON-unique
    /// build must error (Inner multiplies; membership would silently dedup).
    #[tokio::test]
    async fn nonunique_membership_with_require_unique_errors() {
        let fact_schema = Arc::new(Schema::new(vec![Field::new("fk", DataType::Int64, false)]));
        let fact = RecordBatch::try_new(fact_schema.clone(), vec![i64a(vec![Some(7)])]).unwrap();
        // dim key 7 appears twice → an Inner join would emit the fact row twice.
        let dim_schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let dim =
            RecordBatch::try_new(dim_schema.clone(), vec![i64a(vec![Some(7), Some(7)])]).unwrap();
        let out_schema = Arc::new(Schema::new(vec![Field::new("fk", DataType::Int64, false)]));
        let ctx = SessionContext::new();
        let exec = Arc::new(EmatPushPipelineExec::new(
            mem(&ctx, fact_schema, fact).await,
            vec![BuildBinding {
                source: BuildSource::Plan {
                    plan: mem(&ctx, dim_schema, dim).await,
                    key_col: 0,
                    payload_col: None,
                },
                probe_fk_col: 0,
                require_unique: true,
            }],
            vec![EmitCol::ProbeColumn { col: 0 }],
            out_schema,
        ));
        let err = collect(exec, ctx.task_ctx()).await.err();
        assert!(
            err.map(|e| e.to_string().contains("not key-unique"))
                .unwrap_or(false),
            "non-unique membership with require_unique must error"
        );
    }

    /// PV.4.0: the overlap execution path (fact decoded concurrently with the
    /// dim build, drained through a bounded channel) must be byte-identical to
    /// the serial path. Same shape as
    /// `membership_plus_revenue_drops_nonmembers_and_null_fk`, with
    /// `.with_overlap(2)` — the result must still be 650.0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlap_path_matches_serial() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("l_partkey", DataType::Int64, true),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
        ]));
        let fact = RecordBatch::try_new(
            fact_schema.clone(),
            vec![
                i64a(vec![Some(10), Some(20), Some(30), None, Some(10)]),
                f64a(vec![100.0, 200.0, 300.0, 400.0, 500.0]),
                f64a(vec![0.0, 0.1, 0.5, 0.0, 0.2]),
            ],
        )
        .unwrap();
        let dim_schema = Arc::new(Schema::new(vec![Field::new(
            "p_partkey",
            DataType::Int64,
            false,
        )]));
        let dim =
            RecordBatch::try_new(dim_schema.clone(), vec![i64a(vec![Some(10), Some(30)])]).unwrap();

        let ctx = SessionContext::new();
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "revenue",
            DataType::Float64,
            false,
        )]));
        let exec = Arc::new(
            EmatPushPipelineExec::new(
                mem(&ctx, fact_schema, fact).await,
                vec![BuildBinding {
                    source: BuildSource::Plan {
                        plan: mem(&ctx, dim_schema, dim).await,
                        key_col: 0,
                        payload_col: None,
                    },
                    probe_fk_col: 0,
                    require_unique: true,
                }],
                vec![EmitCol::ProbeRevenue {
                    price_col: 1,
                    disc_col: 2,
                }],
                out_schema,
            )
            .with_overlap(2),
        );

        let batches = collect(exec, ctx.task_ctx()).await.unwrap();
        let total: f64 = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .sum();
        assert!(
            (total - 650.0).abs() < 1e-9,
            "overlap revenue sum {total} != 650 (must match serial path)"
        );
    }
}
