//! Phase 2: `EmatixFastParquetExec` + `EmatixFastParquetTableProvider`.
//!
//! Alternate `TableProvider` for parquet files that decodes columns via
//! the `ematix_parquet_bridge` (which dispatches through ematix-parquet
//! kernels — NEON-fused bw=12/17 unpack, Snappy buffer reuse, bitmap-
//! driven sparse gather) instead of parquet-rs's
//! `ParquetRecordBatchReader`.
//!
//! Shape mirrors [`crate::fast_parquet::FastParquetExec`]:
//!   - Row-group-parallel via `Partitioning::UnknownPartitioning(N)`,
//!     where N = `min(num_row_groups, target_partitions).max(1)`.
//!   - Each partition opens the file once, decodes its assigned RGs
//!     sequentially in a `spawn_blocking` worker, yields one
//!     `RecordBatch` per RG over an mpsc channel.
//!   - No filter pushdown, no row-group pruning — the bridge handles
//!     the hot decode path; Phase 3 will add Phase 5-style
//!     bitmap-first predicate eval.
//!
//! Phase 2 supports primitive columns only (INT32, INT64, DOUBLE,
//! Date32). BYTE_ARRAY / Utf8(View) / nested types error out at
//! `try_new`. Callers that need those use the existing
//! [`crate::fast_parquet::FastParquetTableProvider`].

use std::any::Any;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Statistics;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};

use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::ematix_parquet_bridge::{
    decode_column_chunk_f64, decode_column_chunk_i32, decode_column_chunk_i64,
};

/// `TableProvider` that scans a single parquet file using the
/// ematix-parquet bridge for column decode.
#[derive(Debug)]
pub struct EmatixFastParquetTableProvider {
    path: String,
    schema: SchemaRef,
    num_row_groups: usize,
    num_rows: usize,
}

impl EmatixFastParquetTableProvider {
    /// Open the parquet file, validate that every column is one of the
    /// primitive types the bridge supports, and build the Arrow
    /// schema. Errors immediately if any column is unsupported so
    /// callers don't discover this mid-scan.
    pub fn try_new(path: impl Into<String>) -> DfResult<Self> {
        let path = path.into();
        let file = File::open(&path).map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetTableProvider: open `{path}`: {e}").into(),
            )
        })?;
        // Use parquet-rs to extract the Arrow schema. The bridge
        // operates on raw column chunks; we still need parquet-rs
        // to translate parquet types into Arrow types for the
        // RecordBatch schema we'll build.
        let opts = ArrowReaderOptions::new();
        let meta = ArrowReaderMetadata::load(&file, opts).map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetTableProvider: load metadata: {e}").into(),
            )
        })?;
        let schema = meta.schema().clone();

        // Validate: all columns must be primitive types the bridge
        // can decode. Anything else, defer to FastParquetTableProvider.
        for field in schema.fields() {
            match field.data_type() {
                DataType::Int32 | DataType::Int64 | DataType::Float64 | DataType::Date32 => {}
                other => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "EmatixFastParquetTableProvider: column `{}` has type {other:?}; bridge supports Int32/Int64/Float64/Date32 only — use FastParquetTableProvider",
                        field.name()
                    )));
                }
            }
        }

        let reader = SerializedFileReader::new(File::open(&path).map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetTableProvider: reopen `{path}`: {e}").into(),
            )
        })?)
        .map_err(|e| {
            DataFusionError::External(
                format!("EmatixFastParquetTableProvider: SerializedFileReader: {e}").into(),
            )
        })?;
        let fmd = reader.metadata().file_metadata();
        let num_rows = fmd.num_rows() as usize;
        let num_row_groups = reader.metadata().num_row_groups();

        Ok(Self {
            path,
            schema,
            num_row_groups,
            num_rows,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn num_row_groups(&self) -> usize {
        self.num_row_groups
    }
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[async_trait::async_trait]
impl TableProvider for EmatixFastParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Phase 2 supports projection but not predicate pushdown.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Unsupported; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let projected_schema: Schema = self.schema.project(&projection)?;
        let projected_schema: SchemaRef = Arc::new(projected_schema);

        // Partition assignment: round-robin RGs across
        // min(num_rgs, target_partitions) partitions.
        let target_partitions = state.config_options().execution.target_partitions;
        let num_rgs = self.num_row_groups;
        let num_partitions = num_rgs.min(target_partitions).max(1);
        let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
        for rg in 0..num_rgs {
            assignments[rg % num_partitions].push(rg);
        }

        Ok(Arc::new(EmatixFastParquetExec::try_new(
            self.path.clone(),
            projected_schema,
            projection,
            assignments,
            self.num_rows,
        )?))
    }
}

/// `ExecutionPlan` produced by [`EmatixFastParquetTableProvider`].
#[derive(Debug)]
pub struct EmatixFastParquetExec {
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    assignments: Vec<Vec<usize>>,
    num_rows: usize,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl EmatixFastParquetExec {
    pub fn try_new(
        path: String,
        schema: SchemaRef,
        projection: Vec<usize>,
        assignments: Vec<Vec<usize>>,
        num_rows: usize,
    ) -> DfResult<Self> {
        let eq_props = EquivalenceProperties::new(schema.clone());
        let properties = Arc::new(PlanProperties::new(
            eq_props,
            Partitioning::UnknownPartitioning(assignments.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            path,
            schema,
            projection,
            assignments,
            num_rows,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for EmatixFastParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rgs: usize = self.assignments.iter().map(|a| a.len()).sum();
        write!(
            f,
            "EmatixFastParquetExec(path={}, partitions={}, row_groups={}, projection={:?})",
            self.path,
            self.assignments.len(),
            total_rgs,
            self.projection,
        )
    }
}

impl ExecutionPlan for EmatixFastParquetExec {
    fn name(&self) -> &str {
        "EmatixFastParquetExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
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
        let row_groups = self
            .assignments
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let path = self.path.clone();
        let projection = self.projection.clone();
        let schema = self.schema.clone();
        let baseline = BaselineMetrics::new(&self.metrics, partition);

        let stream = build_partition_stream(path, schema.clone(), projection, row_groups, baseline);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Statistics> {
        let rows = match partition {
            Some(p) if p < self.assignments.len() => {
                // Rough per-partition estimate: total / num_partitions.
                self.num_rows / self.assignments.len().max(1)
            }
            None => self.num_rows,
            _ => 0,
        };
        let mut s = Statistics::new_unknown(&self.schema);
        s.num_rows = datafusion::common::stats::Precision::Inexact(rows);
        Ok(s)
    }
}

/// Per-partition decode worker. Walks its assigned RGs sequentially,
/// emits one RecordBatch per RG over an mpsc channel.
fn build_partition_stream(
    path: String,
    schema: SchemaRef,
    projection: Vec<usize>,
    row_groups: Vec<usize>,
    baseline: BaselineMetrics,
) -> futures_util::stream::BoxStream<'static, DfResult<RecordBatch>> {
    use futures_util::StreamExt;

    if row_groups.is_empty() {
        return futures_util::stream::iter(Vec::<DfResult<RecordBatch>>::new()).boxed();
    }

    let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(8);
    let path_buf = PathBuf::from(path);

    tokio::task::spawn_blocking(move || {
        for rg in row_groups {
            let batch_result = decode_one_rg(&path_buf, rg, &schema, &projection);
            if tx.blocking_send(batch_result).is_err() {
                return; // consumer dropped
            }
        }
    });

    let stream = futures_util::stream::unfold((rx, baseline), |(mut rx, baseline)| async move {
        let timer = baseline.elapsed_compute().timer();
        let item = rx.recv().await;
        drop(timer);
        if let Some(Ok(ref batch)) = item {
            baseline.record_output(batch.num_rows());
        }
        item.map(|i| (i, (rx, baseline)))
    });
    stream.boxed()
}

/// Decode one row group into a `RecordBatch` matching `schema`. Each
/// projected column is dispatched on Arrow data type to the
/// appropriate bridge function.
fn decode_one_rg(
    path: &std::path::Path,
    rg: usize,
    schema: &SchemaRef,
    projection: &[usize],
) -> DfResult<RecordBatch> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(projection.len());
    for (out_idx, &col_idx) in projection.iter().enumerate() {
        let field = schema.field(out_idx);
        let arr: Arc<dyn arrow_array::Array> = match field.data_type() {
            DataType::Int32 => {
                decode_column_chunk_i32(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            DataType::Date32 => {
                // Date32 is INT32 physically. Bridge returns Int32Array;
                // re-wrap as Date32Array.
                let i32_arr = decode_column_chunk_i32(path, rg, col_idx)?;
                let vals: Vec<i32> = i32_arr.values().to_vec();
                Arc::new(arrow_array::Date32Array::from(vals))
            }
            DataType::Int64 => {
                decode_column_chunk_i64(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            DataType::Float64 => {
                decode_column_chunk_f64(path, rg, col_idx)? as Arc<dyn arrow_array::Array>
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "EmatixFastParquetExec: unsupported column type {other:?} for `{}`",
                    field.name()
                )));
            }
        };
        columns.push(arr);
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
        DataFusionError::External(
            format!("EmatixFastParquetExec: RecordBatch::try_new: {e}").into(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    fn lineitem_path() -> Option<String> {
        let s = match std::env::var("TPCH_DATA_DIR") {
            Ok(s) => format!("{s}/lineitem.parquet"),
            Err(_) => "examples/tpch/data/sf1/lineitem.parquet".into(),
        };
        if std::path::Path::new(&s).exists() {
            Some(s)
        } else {
            None
        }
    }

    #[tokio::test]
    async fn count_rows_via_provider() {
        let Some(path) = lineitem_path() else {
            eprintln!("skipping: SF=1 lineitem not present");
            return;
        };
        // lineitem has BYTE_ARRAY columns (l_returnflag, l_linestatus,
        // etc.) so the full table can't go through Emat alone yet.
        // Probe with a single-column file? At minimum verify try_new
        // rejects unsupported types cleanly.
        let r = EmatixFastParquetTableProvider::try_new(path);
        match r {
            Ok(_) => panic!("expected lineitem to be rejected (BYTE_ARRAY cols)"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("bridge supports") || msg.contains("BYTE_ARRAY") || msg.contains("Utf8"),
                    "unexpected error: {msg}"
                );
            }
        }
    }

    /// Build a small primitive-only parquet file in memory, register
    /// it via the provider, run `SELECT COUNT(*)` through DataFusion,
    /// confirm row count.
    #[tokio::test]
    async fn end_to_end_simple_count() {
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::SerializedFileWriter;
        use parquet::schema::types::Type as PType;
        use parquet::basic::{Compression, Repetition, Type as PhysicalType};
        use parquet::column::writer::ColumnWriter;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Schema: a (i32), b (i64), c (f64).
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        PType::primitive_type_builder("a", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("b", PhysicalType::INT64)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        PType::primitive_type_builder("c", PhysicalType::DOUBLE)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build(),
        );
        let file = File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();
        // a
        let a: Vec<i32> = (0..1000).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&a, None, None).unwrap();
        }
        col.close().unwrap();
        // b
        let b: Vec<i64> = (0..1000i64).map(|i| i * 100).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&b, None, None).unwrap();
        }
        col.close().unwrap();
        // c
        let c: Vec<f64> = (0..1000).map(|i| (i as f64) * 1.5).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::DoubleColumnWriter(t) = col.untyped() {
            t.write_batch(&c, None, None).unwrap();
        }
        col.close().unwrap();
        rg.close().unwrap();
        writer.close().unwrap();

        let provider =
            EmatixFastParquetTableProvider::try_new(path.to_string_lossy().to_string()).unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let df = ctx.sql("SELECT COUNT(*), SUM(a), SUM(b), SUM(c) FROM t").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        let b0 = &batches[0];
        let count = b0
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_a = b0
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_b = b0
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0);
        let sum_c = b0
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 1000);
        assert_eq!(sum_a, (0..1000i64).sum::<i64>());
        assert_eq!(sum_b, (0..1000i64).map(|i| i * 100).sum::<i64>());
        let expected_c: f64 = (0..1000).map(|i| (i as f64) * 1.5).sum();
        assert!((sum_c - expected_c).abs() < 1e-6);
    }
}
