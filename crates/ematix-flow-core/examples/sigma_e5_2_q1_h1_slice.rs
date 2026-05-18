//! Σ.E5.2 H1 isolation: re-bench EmatixFast Q1 SQL with each
//! per-RG RecordBatch sliced into 65_536-row chunks downstream of
//! `EmatixFastParquetExec.execute()`. If the gap closes materially
//! when batches are chunked, H1 (per-RG vs streaming batch emission)
//! is confirmed.
//!
//! Implementation: a thin `SlicingExec` wraps the `ExecutionPlan`
//! returned by `EmatixFastParquetTableProvider::scan` and reads its
//! stream, yielding 65_536-row slices instead of whole-RG batches.
//! It's a wrapper, not a fix; the source provider is untouched.

use std::any::Any;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use datafusion::common::Result as DfResult;


use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures_util::Stream;
use futures_util::StreamExt;

use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const TARGET_BATCH: usize = 65_536;
const TRIALS: usize = 21;
const WARMUPS: usize = 3;

const Q1_SQL: &str = "
    SELECT
        l_returnflag, l_linestatus,
        sum(l_quantity) AS sum_qty,
        sum(l_extendedprice) AS sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        avg(l_quantity) AS avg_qty,
        avg(l_extendedprice) AS avg_price,
        avg(l_discount) AS avg_disc,
        count(*) AS count_order
    FROM lineitem
    WHERE l_shipdate <= DATE '1998-09-02'
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
";

fn data_path() -> String {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match std::env::var("TPCH_DATA_DIR") {
        Ok(s) => format!("{s}/lineitem.parquet"),
        Err(_) => manifest.parent().unwrap().parent().unwrap()
            .join("examples/tpch/data/sf1/lineitem.parquet")
            .to_string_lossy().into_owned(),
    }
}

// ---- Slicing wrapper plan -------------------------------------------------

#[derive(Debug)]
struct SlicingExec {
    inner: Arc<dyn ExecutionPlan>,
    target: usize,
    properties: Arc<PlanProperties>,
}
impl SlicingExec {
    fn new(inner: Arc<dyn ExecutionPlan>, target: usize) -> Self {
        let parts = inner.properties().partitioning.partition_count();
        let eq = EquivalenceProperties::new(inner.schema());
        let properties = Arc::new(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(parts),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { inner, target, properties }
    }
}
impl DisplayAs for SlicingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SlicingExec(target={})", self.target)
    }
}
impl ExecutionPlan for SlicingExec {
    fn name(&self) -> &str { "SlicingExec" }
    fn as_any(&self) -> &dyn Any { self }
    fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.inner] }
    fn with_new_children(self: Arc<Self>, mut c: Vec<Arc<dyn ExecutionPlan>>) -> DfResult<Arc<dyn ExecutionPlan>> {
        let inner = c.pop().unwrap();
        Ok(Arc::new(SlicingExec::new(inner, self.target)))
    }
    fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> DfResult<SendableRecordBatchStream> {
        let s = self.inner.execute(partition, ctx)?;
        let schema = self.inner.schema();
        Ok(Box::pin(SliceStream::new(s, schema, self.target)))
    }
}

struct SliceStream {
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    target: usize,
    current: Option<RecordBatch>,
    pos: usize,
}
impl SliceStream {
    fn new(inner: SendableRecordBatchStream, schema: SchemaRef, target: usize) -> Self {
        Self { inner, schema, target, current: None, pos: 0 }
    }
}
impl Stream for SliceStream {
    type Item = DfResult<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.current.is_some() {
                let b_rows = self.current.as_ref().unwrap().num_rows();
                let remaining = b_rows - self.pos;
                if remaining > 0 {
                    let n = remaining.min(self.target);
                    let slice = self.current.as_ref().unwrap().slice(self.pos, n);
                    self.pos += n;
                    if self.pos >= b_rows {
                        self.current = None;
                        self.pos = 0;
                    }
                    return Poll::Ready(Some(Ok(slice)));
                } else {
                    self.current = None;
                    self.pos = 0;
                }
            }
            match self.inner.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(b))) => {
                    if b.num_rows() <= self.target {
                        return Poll::Ready(Some(Ok(b)));
                    }
                    self.current = Some(b);
                    self.pos = 0;
                }
            }
        }
    }
}
impl RecordBatchStream for SliceStream {
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}

// ---- Wrapper table provider -----------------------------------------------

#[derive(Debug)]
struct SlicingProvider {
    inner: Arc<EmatixFastParquetTableProvider>,
    target: usize,
}
#[async_trait::async_trait]
impl TableProvider for SlicingProvider {
    fn as_any(&self) -> &dyn Any { self }
    fn schema(&self) -> SchemaRef { self.inner.schema() }
    fn table_type(&self) -> TableType { TableType::Base }
    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DfResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(Arc::new(SlicingExec::new(inner, self.target)))
    }
}

fn median(times: &mut [f64]) -> f64 {
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}
fn stdev(times: &[f64], mean: f64) -> f64 {
    let n = times.len();
    let var = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    var.sqrt()
}

async fn bench(label: &str, ctx: &SessionContext) -> f64 {
    for _ in 0..WARMUPS {
        let _ = ctx.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
    }
    let mut times = Vec::with_capacity(TRIALS);
    for _ in 0..TRIALS {
        let s = Instant::now();
        let _ = ctx.sql(Q1_SQL).await.unwrap().collect().await.unwrap();
        times.push(s.elapsed().as_secs_f64() * 1000.0);
    }
    let med = median(&mut times);
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let sd = stdev(&times, mean);
    println!("  {label:<54}  median {med:>6.2} ms ± {sd:>5.2}");
    med
}

async fn build_ctx_emat(path: &str, dict: bool, slice: bool) -> SessionContext {
    let cfg = SessionConfig::new().with_target_partitions(14);
    let state = SessionStateBuilder::new().with_config(cfg).with_default_features().build();
    let ctx = SessionContext::new_with_state(state);
    let base = EmatixFastParquetTableProvider::try_new(path).unwrap();
    let base = if dict { base.with_dict_preservation(true) } else { base };
    if slice {
        let prov = SlicingProvider { inner: Arc::new(base), target: TARGET_BATCH };
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
    } else {
        ctx.register_table("lineitem", Arc::new(base)).unwrap();
    }
    ctx
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let path = data_path();
    println!("==> Σ.E5.2 H1 test: 65_536-row slicing on EmatixFast emission");
    println!("==> data: {path}");
    println!("==> {TRIALS}-trial median after {WARMUPS} warm-ups; 14 partitions; rule OFF\n");

    let ctx_u_off = build_ctx_emat(&path, false, false).await;
    let ctx_u_on  = build_ctx_emat(&path, false, true).await;
    let ctx_d_off = build_ctx_emat(&path, true,  false).await;
    let ctx_d_on  = build_ctx_emat(&path, true,  true).await;

    println!("--- EmatixFast (Utf8) ---");
    let u_off = bench("baseline (1 batch per RG)", &ctx_u_off).await;
    let u_on  = bench("with 65_536-row slicing", &ctx_u_on).await;
    println!();
    println!("--- EmatixFast (Dict) ---");
    let d_off = bench("baseline (1 batch per RG)", &ctx_d_off).await;
    let d_on  = bench("with 65_536-row slicing", &ctx_d_on).await;

    println!();
    println!("--- H1 deltas ---");
    println!("  Utf8 : slice − baseline = {:+.2} ms ({:+.1}%)",
        u_on - u_off, 100.0 * (u_on - u_off) / u_off);
    println!("  Dict : slice − baseline = {:+.2} ms ({:+.1}%)",
        d_on - d_off, 100.0 * (d_on - d_off) / d_off);
}
