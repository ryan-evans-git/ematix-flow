//! Σ.E2: `FastParquetTableProvider` — a row-group-parallel parquet
//! TableProvider that bypasses DataFusion's `ParquetExec → DataSource
//! Exec → RepartitionExec` wrapper stack.
//!
//! ### Background
//!
//! Day-2 / day-3 probes (see `examples/parquet_rs_*.rs`) decomposed
//! the 1.45× Polars-vs-DataFusion gap on Q6 4-col:
//!
//! ```text
//!   parquet-rs single-thread (tuned, batch_size=65536):  54.88 ms
//!   parquet-rs 6-thread, 1 thread per row group:         14.75 ms  ← beats Polars
//!   Polars     14-thread:                                15.08 ms
//!   DataFusion 14-thread (default ParquetExec):          21.87 ms
//! ```
//!
//! Conclusion: parquet-rs is not the bottleneck. The DataFusion wrapper
//! layer adds the entire gap. A custom TableProvider that:
//!   - reports `Partitioning::UnknownPartitioning(num_row_groups)` so
//!     the planner skips RepartitionExec
//!   - spawns one task per row group
//!   - reads at batch_size=65536 (sweet spot from the probe sweep)
//!
//! ...matches or beats Polars without a polars-io dep.
//!
//! ### Scope (v1)
//!
//! - Single parquet file (one path). Multi-file support is a v2 follow-up.
//! - Column projection: yes (passed through to parquet-rs `ProjectionMask`).
//! - Filter pushdown: NO. parquet-rs's `with_row_filter` is broken for
//!   cheap predicates (21% slower on Q6 vs no filter despite 52× row
//!   reduction — see `examples/parquet_rs_late_mat.rs`). DataFusion's
//!   `FilterExec` runs post-decode the same as today.
//! - Limit pushdown: NO. v2.

use std::any::Any;
use std::fs::File;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::schema::types::SchemaDescriptor;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::{self};

/// Per-probe sweet spot — see `examples/parquet_rs_granular.rs`.
const DEFAULT_BATCH_SIZE: usize = 65_536;

/// `TableProvider` over a single parquet file. Construction reads the
/// file footer once to cache the Arrow schema and the row-group count;
/// `scan()` returns a [`FastParquetExec`] whose partition count equals
/// the number of row groups.
#[derive(Debug, Clone)]
pub struct FastParquetTableProvider {
    path: String,
    schema: SchemaRef,
    num_row_groups: usize,
    parquet_schema: Arc<SchemaDescriptor>,
}

impl FastParquetTableProvider {
    /// Open `path`, read the parquet metadata, cache schema + row group
    /// count. Fails if the file is missing or unreadable.
    pub fn try_new(path: impl Into<String>) -> DfResult<Self> {
        let path: String = path.into();
        let file = File::open(&path).map_err(|e| {
            DataFusionError::External(
                format!("FastParquetTableProvider: cannot open `{path}`: {e}").into(),
            )
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            DataFusionError::External(
                format!("FastParquetTableProvider: parquet open failed: {e}").into(),
            )
        })?;
        let schema = builder.schema().clone();
        let num_row_groups = builder.metadata().num_row_groups();
        let parquet_schema: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        Ok(Self {
            path,
            schema,
            num_row_groups,
            parquet_schema,
        })
    }

    /// File path this provider scans.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Number of row groups in the parquet file (= number of output
    /// partitions when no projection prunes empty row groups).
    pub fn num_row_groups(&self) -> usize {
        self.num_row_groups
    }
}

#[async_trait]
impl TableProvider for FastParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Build the projected output schema. parquet-rs uses leaf-column
        // indices via `ProjectionMask::leaves`; for flat (non-nested)
        // schemas — which is what TPC-H is — leaf indices match field
        // indices, so the field projection here also describes the
        // leaf projection used at execute() time.
        let projected_indices: Vec<usize> = match projection {
            Some(p) => p.clone(),
            None => (0..self.schema.fields().len()).collect(),
        };
        let projected_fields: Vec<_> = projected_indices
            .iter()
            .map(|&i| self.schema.field(i).clone())
            .collect();
        let projected_schema: SchemaRef =
            Arc::new(datafusion::arrow::datatypes::Schema::new(projected_fields));

        let exec = FastParquetExec::try_new(
            self.path.clone(),
            projected_schema,
            projected_indices,
            self.num_row_groups,
            self.parquet_schema.clone(),
        )?;
        Ok(Arc::new(exec))
    }
}

/// `ExecutionPlan` over a parquet file with `num_row_groups` partitions.
/// Each partition decodes exactly one row group, single-threaded, at
/// `batch_size = 65_536`. The planner picks up the partitioning from
/// `PlanProperties` and (because we report a fixed partition count)
/// will not wrap us in a `RepartitionExec`.
#[derive(Debug)]
pub struct FastParquetExec {
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    num_row_groups: usize,
    parquet_schema: Arc<SchemaDescriptor>,
    properties: Arc<PlanProperties>,
}

impl FastParquetExec {
    pub fn try_new(
        path: String,
        schema: SchemaRef,
        projection: Vec<usize>,
        num_row_groups: usize,
        parquet_schema: Arc<SchemaDescriptor>,
    ) -> DfResult<Self> {
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(num_row_groups.max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            path,
            schema,
            projection,
            num_row_groups,
            parquet_schema,
            properties,
        })
    }
}

impl DisplayAs for FastParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "FastParquetExec(path={}, row_groups={}, projection={:?})",
            self.path, self.num_row_groups, self.projection,
        )
    }
}

impl ExecutionPlan for FastParquetExec {
    fn name(&self) -> &str {
        "FastParquetExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition >= self.num_row_groups {
            return Err(DataFusionError::Internal(format!(
                "FastParquetExec: partition {partition} out of range (num_row_groups={})",
                self.num_row_groups
            )));
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            build_partition_stream(
                self.path.clone(),
                self.projection.clone(),
                self.parquet_schema.clone(),
                partition,
            ),
        )))
    }
}

/// Build the per-partition stream that decodes one row group on a
/// blocking worker and yields its RecordBatches one at a time.
fn build_partition_stream(
    path: String,
    projection: Vec<usize>,
    parquet_schema: Arc<SchemaDescriptor>,
    row_group: usize,
) -> impl futures_util::Stream<Item = DfResult<RecordBatch>> + Send + 'static {
    use futures_util::StreamExt;
    let fut = async move {
        tokio::task::spawn_blocking(move || -> DfResult<Vec<RecordBatch>> {
            let file = File::open(&path).map_err(|e| {
                DataFusionError::External(
                    format!("FastParquetExec: open `{path}` failed: {e}").into(),
                )
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                DataFusionError::External(format!("FastParquetExec: builder failed: {e}").into())
            })?;
            let mask = ProjectionMask::leaves(&parquet_schema, projection.iter().copied());
            let reader = builder
                .with_projection(mask)
                .with_row_groups(vec![row_group])
                .with_batch_size(DEFAULT_BATCH_SIZE)
                .build()
                .map_err(|e| {
                    DataFusionError::External(
                        format!("FastParquetExec: reader build failed: {e}").into(),
                    )
                })?;
            let mut out = Vec::new();
            for batch in reader {
                out.push(batch.map_err(|e| {
                    DataFusionError::External(
                        format!("FastParquetExec: batch decode failed: {e}").into(),
                    )
                })?);
            }
            Ok(out)
        })
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("FastParquetExec: blocking join failed: {e}"))
        })?
    };
    stream::once(fut).flat_map(|res| {
        let items: Vec<DfResult<RecordBatch>> = match res {
            Ok(v) => v.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        };
        futures_util::stream::iter(items)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Resolve the SF=1 data dir, walking up from CARGO_MANIFEST_DIR
    /// (`crates/ematix-flow-core`) to the workspace root.
    fn sf1_dir() -> Option<PathBuf> {
        if let Ok(env) = std::env::var("TPCH_DATA_DIR") {
            let p = PathBuf::from(env);
            return p.exists().then_some(p);
        }
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let p = manifest.parent()?.parent()?.join("examples/tpch/data/sf1");
        p.exists().then_some(p)
    }

    fn lineitem_parquet() -> Option<String> {
        sf1_dir().map(|d| d.join("lineitem.parquet").to_string_lossy().into_owned())
    }

    #[test]
    fn provider_caches_schema_and_row_group_count() {
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let prov = FastParquetTableProvider::try_new(path).expect("open");
        assert_eq!(prov.schema().fields().len(), 16, "lineitem has 16 cols");
        assert_eq!(prov.num_row_groups(), 6, "SF=1 lineitem has 6 row groups");
        assert_eq!(
            prov.schema().field(0).name(),
            "l_orderkey",
            "first col is l_orderkey"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn scan_returns_one_partition_per_row_group() {
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        let state = ctx.state();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        assert_eq!(
            exec.properties().partitioning.partition_count(),
            6,
            "one partition per row group"
        );
        assert_eq!(exec.schema().fields().len(), 16);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn count_star_matches_row_total() {
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        let df = ctx.sql("SELECT count(*) AS c FROM lineitem").await.unwrap();
        let batches = df.collect().await.unwrap();
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 6_001_215, "SF=1 lineitem row count");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn projection_decodes_only_requested_columns() {
        // Verifying behavior (not perf): a `SELECT l_orderkey` returns
        // batches with exactly one column matching the requested name.
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        let df = ctx
            .sql("SELECT l_orderkey FROM lineitem LIMIT 5")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "l_orderkey");
    }

    /// Q6 plan-level check. With `FastParquetTableProvider` the result
    /// must match DataFusion's default register_parquet path to within
    /// floating-point tolerance.
    #[tokio::test(flavor = "multi_thread")]
    async fn q6_result_matches_default_provider() {
        let Some(dir) = sf1_dir() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        use datafusion::prelude::SessionContext;
        let parquet = dir.join("lineitem.parquet").to_string_lossy().into_owned();

        let sql = "SELECT \
            sum(l_extendedprice * l_discount) AS revenue \
            FROM lineitem \
            WHERE l_shipdate >= DATE '1994-01-01' \
              AND l_shipdate <  DATE '1995-01-01' \
              AND l_discount BETWEEN 0.05 AND 0.07 \
              AND l_quantity <  24";

        // Reference: default DataFusion parquet path.
        let ctx_ref = SessionContext::new();
        ctx_ref
            .register_parquet("lineitem", &parquet, Default::default())
            .await
            .unwrap();
        let r_ref = ctx_ref.sql(sql).await.unwrap().collect().await.unwrap();
        let v_ref = r_ref[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        // Our provider.
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(parquet).unwrap();
        ctx.register_table("lineitem", Arc::new(prov)).unwrap();
        let r = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let v = r[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
            .unwrap()
            .value(0);

        let rel_err = ((v - v_ref) / v_ref).abs();
        assert!(
            rel_err < 1e-10,
            "Q6 revenue mismatch: ours={v}, ref={v_ref} (rel_err={rel_err:e})"
        );
    }
}
