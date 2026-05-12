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
use datafusion::common::stats::Precision;
use datafusion::common::{
    ColumnStatistics, DataFusionError, Result as DfResult, ScalarValue, Statistics,
};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::{
    ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use datafusion::parquet::schema::types::SchemaDescriptor;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures_util::stream::StreamExt;

/// Per-probe sweet spot — see `examples/parquet_rs_granular.rs`. The
/// Σ.E2 follow-up batch-size sweep (8K/16K/32K/65K across SF=1 and
/// SF=10) confirmed 65K is the right default: Q13 strongly prefers it
/// (+27% vs +18% at 8K), Q19's small advantage at 8K became moot once
/// the Utf8View root cause was fixed.
const DEFAULT_BATCH_SIZE: usize = 65_536;

/// Replace `Utf8`/`LargeUtf8`/`Binary`/`LargeBinary` fields with their
/// `*View` equivalents, leaving everything else untouched. Σ.E2 root-
/// cause fix for Q01 SF=10: DataFusion's default parquet reader emits
/// `Utf8View` for string columns, which lets downstream FilterExec /
/// AggregatePartial use SIMD-optimised kernels over the 16-byte inline
/// view layout. parquet-rs's `with_schema` hint promotes types
/// per-field (see parquet-58.1.0 src/arrow/schema/primitive.rs apply
/// _hint), so all we need to do is hand it a schema where the strings
/// are already declared as views.
fn promote_to_view_types(
    schema: &datafusion::arrow::datatypes::Schema,
) -> datafusion::arrow::datatypes::Schema {
    use datafusion::arrow::datatypes::{DataType, Field};
    let promoted: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_type = match f.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
                DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
                other => other.clone(),
            };
            Field::new(f.name(), new_type, f.is_nullable()).with_metadata(f.metadata().clone())
        })
        .collect();
    datafusion::arrow::datatypes::Schema::new_with_metadata(promoted, schema.metadata().clone())
}


/// Aggregate per-row-group parquet statistics into a file-level
/// `ColumnStatistics`. Returns one entry per Arrow field in `schema`,
/// in field order.
///
/// We support primitive types where the parquet `Statistics` variant
/// maps directly to a single Arrow `ScalarValue`: Int32, Int64,
/// Float32, Float64, Boolean, plus Arrow's Date32 (parquet stores it
/// as an Int32 with logical type Date). For other types (Utf8,
/// nested, etc.) we emit `Precision::Absent`, matching DataFusion's
/// `ColumnStatistics::new_unknown()`.
///
/// Aggregation rules:
///   - `null_count`: sum across row groups (Exact iff every RG had Exact)
///   - `min_value`:  min across row groups
///   - `max_value`:  max across row groups
fn aggregate_column_statistics(
    meta: &datafusion::parquet::file::metadata::ParquetMetaData,
    arrow_schema: &datafusion::arrow::datatypes::Schema,
) -> Vec<ColumnStatistics> {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::parquet::file::statistics::Statistics as PqStats;

    let num_fields = arrow_schema.fields().len();
    let mut out: Vec<ColumnStatistics> = (0..num_fields)
        .map(|_| ColumnStatistics::new_unknown())
        .collect();

    // For each leaf column, fold its row-group stats into one
    // ColumnStatistics. We match leaf order = field order for the
    // flat TPC-H schemas this provider targets.
    for (col_idx, field) in arrow_schema.fields().iter().enumerate() {
        if col_idx >= meta.row_group(0).num_columns() {
            continue; // shouldn't happen, but defensive
        }
        let arrow_ty = field.data_type();
        let mut null_count: Option<i64> = Some(0);
        let mut min_sv: Option<ScalarValue> = None;
        let mut max_sv: Option<ScalarValue> = None;
        let mut any_missing = false;

        for rg_idx in 0..meta.num_row_groups() {
            let col = meta.row_group(rg_idx).column(col_idx);
            let Some(stats) = col.statistics() else {
                any_missing = true;
                null_count = None;
                continue;
            };
            // Per-column null count: parquet exposes via `null_count_opt`.
            if let (Some(nc), Some(curr)) = (stats.null_count_opt(), null_count) {
                null_count = Some(curr.saturating_add(nc as i64));
            } else {
                null_count = None;
            }

            // Extract typed min/max if both ends are present.
            let (rg_min, rg_max) = match (stats, arrow_ty) {
                (PqStats::Int32(s), DataType::Int32) => match (s.min_opt(), s.max_opt()) {
                    (Some(&lo), Some(&hi)) => (
                        Some(ScalarValue::Int32(Some(lo))),
                        Some(ScalarValue::Int32(Some(hi))),
                    ),
                    _ => (None, None),
                },
                (PqStats::Int32(s), DataType::Date32) => match (s.min_opt(), s.max_opt()) {
                    (Some(&lo), Some(&hi)) => (
                        Some(ScalarValue::Date32(Some(lo))),
                        Some(ScalarValue::Date32(Some(hi))),
                    ),
                    _ => (None, None),
                },
                (PqStats::Int64(s), DataType::Int64) => match (s.min_opt(), s.max_opt()) {
                    (Some(&lo), Some(&hi)) => (
                        Some(ScalarValue::Int64(Some(lo))),
                        Some(ScalarValue::Int64(Some(hi))),
                    ),
                    _ => (None, None),
                },
                (PqStats::Float(s), DataType::Float32) => match (s.min_opt(), s.max_opt()) {
                    (Some(&lo), Some(&hi)) => (
                        Some(ScalarValue::Float32(Some(lo))),
                        Some(ScalarValue::Float32(Some(hi))),
                    ),
                    _ => (None, None),
                },
                (PqStats::Double(s), DataType::Float64) => match (s.min_opt(), s.max_opt()) {
                    (Some(&lo), Some(&hi)) => (
                        Some(ScalarValue::Float64(Some(lo))),
                        Some(ScalarValue::Float64(Some(hi))),
                    ),
                    _ => (None, None),
                },
                _ => (None, None),
            };

            match (rg_min, &min_sv) {
                (Some(v), None) => min_sv = Some(v),
                (Some(v), Some(curr)) if v < *curr => min_sv = Some(v),
                _ => {}
            }
            match (rg_max, &max_sv) {
                (Some(v), None) => max_sv = Some(v),
                (Some(v), Some(curr)) if v > *curr => max_sv = Some(v),
                _ => {}
            }
        }

        let null_precision = match (null_count, any_missing) {
            (Some(n), false) => Precision::Exact(n.max(0) as usize),
            _ => Precision::Absent,
        };
        out[col_idx] = ColumnStatistics {
            null_count: null_precision,
            max_value: max_sv.map(Precision::Exact).unwrap_or(Precision::Absent),
            min_value: min_sv.map(Precision::Exact).unwrap_or(Precision::Absent),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
            byte_size: Precision::Absent,
        };
    }

    out
}

/// `TableProvider` over a single parquet file. Construction reads the
/// file footer once to cache the Arrow schema, row-group count, and
/// total row count; `scan()` returns a [`FastParquetExec`] whose
/// partition count is `target_partitions` from the session config
/// (which lets DataFusion match it to its hash-join parallelism
/// without inserting a `RoundRobinBatch` repartition on top).
#[derive(Debug, Clone)]
pub struct FastParquetTableProvider {
    path: String,
    schema: SchemaRef,
    num_row_groups: usize,
    num_rows: usize,
    parquet_schema: Arc<SchemaDescriptor>,
    /// One entry per column in `schema` (field order). Populated from
    /// parquet row-group stats at construction so we don't pay the
    /// metadata-read cost on every scan.
    column_stats: Arc<Vec<ColumnStatistics>>,
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
        // Hand the planner (and downstream operators) a schema that
        // declares string/binary columns as their `*View` form so kernel
        // selection matches DataFusion's default parquet path. This
        // schema is also passed to parquet-rs at read time via
        // `ArrowReaderOptions::with_schema`, which actually emits
        // StringViewArray/BinaryViewArray.
        let schema: SchemaRef = Arc::new(promote_to_view_types(builder.schema()));
        let meta = builder.metadata();
        let num_row_groups = meta.num_row_groups();
        // num_rows is a global property of the file; parquet metadata
        // exposes it as i64. Saturating-cast in case (a corrupt file
        // could declare a negative count; we treat that as zero).
        let num_rows = meta.file_metadata().num_rows().max(0) as usize;
        let parquet_schema: Arc<SchemaDescriptor> = builder.parquet_schema().clone().into();
        let column_stats = Arc::new(aggregate_column_statistics(meta, &schema));
        Ok(Self {
            path,
            schema,
            num_row_groups,
            num_rows,
            parquet_schema,
            column_stats,
        })
    }

    /// File path this provider scans.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Number of row groups in the parquet file. We round-robin these
    /// across `target_partitions` worker partitions at scan time.
    pub fn num_row_groups(&self) -> usize {
        self.num_row_groups
    }

    /// Total rows across all row groups, as declared in the parquet
    /// footer. Exposed via [`ExecutionPlan::statistics`] so the planner
    /// can size hash tables and pick join build sides correctly.
    pub fn num_rows(&self) -> usize {
        self.num_rows
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
        state: &dyn Session,
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

        // Partition count = min(num_row_groups, target_partitions).
        //
        // Why not `target_partitions` directly: when a parquet file
        // has fewer row groups than CPUs (e.g. SF=1 lineitem has 6
        // row groups; M3 Pro has 14 cores), reporting 14 partitions
        // means 8 are empty. parquet-rs row-group readers can't be
        // sub-divided cheaply, so the work is unbalanced — measured
        // as a catastrophic regression in the day-5 bench (Q01 went
        // from +8% to -48% just from this change).
        //
        // Why not `num_row_groups` directly (v1): if the file has
        // more row groups than CPUs, we'd over-shard.
        //
        // The downside: when num_row_groups < target_partitions,
        // DataFusion still adds a RoundRobinBatch above us. v3 work
        // can split row groups into byte ranges to remove that.
        let target_partitions = state.config().options().execution.target_partitions;
        let num_partitions = self.num_row_groups.min(target_partitions).max(1);

        // partition i gets row groups {i, i+N, i+2N, …} ∩ [0, num_row_groups).
        let mut assignments: Vec<Vec<usize>> = vec![Vec::new(); num_partitions];
        for rg in 0..self.num_row_groups {
            assignments[rg % num_partitions].push(rg);
        }

        // Project the column-statistics vector to match the projected
        // schema. ColumnStatistics are positional in field order.
        let projected_col_stats: Vec<ColumnStatistics> = projected_indices
            .iter()
            .map(|&i| self.column_stats[i].clone())
            .collect();

        let exec = FastParquetExec::try_new(
            self.path.clone(),
            projected_schema,
            self.schema.clone(),
            projected_indices,
            assignments,
            self.num_rows,
            projected_col_stats,
            self.parquet_schema.clone(),
        )?;
        Ok(Arc::new(exec))
    }
}

/// `ExecutionPlan` over a parquet file. Each output partition handles
/// a (possibly empty) list of row groups; partitions read at
/// `batch_size = 65_536`. Partition count is set at scan time to
/// match `target_partitions` so DataFusion's planner doesn't need to
/// add a `RoundRobinBatch` repartition on top.
#[derive(Debug)]
pub struct FastParquetExec {
    path: String,
    schema: SchemaRef,
    /// Full (unprojected) promoted Arrow schema for the parquet file.
    /// Threaded into `ArrowReaderOptions::with_schema` so parquet-rs
    /// emits `Utf8View`/`BinaryView` arrays per the provider's schema
    /// hint. parquet-rs requires `with_schema` to cover every parquet
    /// column, not just the projected ones, so we keep the full schema.
    full_schema: SchemaRef,
    projection: Vec<usize>,
    /// For partition `i`, `assignments[i]` lists which parquet row
    /// groups it reads. Empty assignments produce empty streams.
    assignments: Vec<Vec<usize>>,
    /// Total row count across the file (from parquet metadata).
    num_rows: usize,
    /// Per-column file-level stats, aligned to `schema` field order.
    /// Populated from parquet row-group statistics.
    column_stats: Vec<ColumnStatistics>,
    parquet_schema: Arc<SchemaDescriptor>,
    properties: Arc<PlanProperties>,
    /// Per-partition decode metrics. Σ.E2 follow-up: shipped initially
    /// with `metrics=[]` which made Q01 SF=10 undiagnosable in EXPLAIN
    /// ANALYZE — we couldn't see whether the gap was in file open,
    /// row-group decode, or batch hand-off. Surface matches DataFusion's
    /// `DataSourceExec`: `output_rows`, `elapsed_compute`, `output_batches`,
    /// `bytes_scanned`, `time_elapsed_opening`, `time_elapsed_processing`.
    metrics: ExecutionPlanMetricsSet,
}

impl FastParquetExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        path: String,
        schema: SchemaRef,
        full_schema: SchemaRef,
        projection: Vec<usize>,
        assignments: Vec<Vec<usize>>,
        num_rows: usize,
        column_stats: Vec<ColumnStatistics>,
        parquet_schema: Arc<SchemaDescriptor>,
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
            full_schema,
            projection,
            assignments,
            num_rows,
            column_stats,
            parquet_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// Per-partition handles into the shared `ExecutionPlanMetricsSet`.
/// Constructed once per `execute()` call and threaded into the
/// blocking decode worker + the consumer-side stream.
struct PartitionMetrics {
    baseline: BaselineMetrics,
    output_batches: Count,
    bytes_scanned: Count,
    time_opening: Time,
    time_processing: Time,
}

impl PartitionMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            // Use typed builders where DataFusion provides them
            // (`output_batches` is a reserved MetricValue variant —
            // a plain `counter("output_batches", …)` panics at
            // aggregation time with a type mismatch). The free-form
            // `bytes_scanned` reuses the name DataSourceExec uses so
            // EXPLAIN ANALYZE output reads consistently.
            output_batches: MetricBuilder::new(metrics).output_batches(partition),
            bytes_scanned: MetricBuilder::new(metrics).counter("bytes_scanned", partition),
            time_opening: MetricBuilder::new(metrics).subset_time("time_elapsed_opening", partition),
            time_processing: MetricBuilder::new(metrics)
                .subset_time("time_elapsed_processing", partition),
        }
    }
}

impl DisplayAs for FastParquetExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let total_rgs: usize = self.assignments.iter().map(|a| a.len()).sum();
        write!(
            f,
            "FastParquetExec(path={}, partitions={}, row_groups={}, projection={:?})",
            self.path,
            self.assignments.len(),
            total_rgs,
            self.projection,
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
        let row_groups = self.assignments.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "FastParquetExec: partition {partition} out of range (num_partitions={})",
                self.assignments.len()
            ))
        })?;
        let pm = PartitionMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            build_partition_stream(
                self.path.clone(),
                self.projection.clone(),
                self.parquet_schema.clone(),
                self.full_schema.clone(),
                row_groups.clone(),
                pm,
            ),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DfResult<Statistics> {
        // Expose num_rows + per-column min/max/null_count from parquet
        // metadata so the planner can:
        //   1. Pick the right join build side (Q12 EXPLAIN: planner
        //      picked 1.5M-row orders instead of 30K-row post-filter
        //      lineitem before stats were exposed).
        //   2. Estimate filter selectivity against column min/max
        //      (e.g. `l_shipdate >= '1994' AND < '1995'` over min=
        //      '1992' max='1998' → ~14% selectivity, vs the default
        //      blind ~10% guess).
        let mut stats = Statistics::new_unknown(&self.schema);
        let n = match partition {
            None => self.num_rows,
            Some(i) => {
                if i >= self.assignments.len() {
                    return Err(DataFusionError::Internal(format!(
                        "FastParquetExec::partition_statistics: partition {i} out of range"
                    )));
                }
                let denom = self.assignments.len().max(1);
                self.num_rows / denom
            }
        };
        stats.num_rows = Precision::Exact(n);
        stats.column_statistics = self.column_stats.clone();
        Ok(stats)
    }
}

/// Build the per-partition stream that decodes a list of row groups on
/// a blocking worker and yields each `RecordBatch` as it decodes. An
/// empty `row_groups` list produces an empty stream.
///
/// v4-streaming: prior impl returned a `Vec<RecordBatch>` from
/// `spawn_blocking`, so the partition's downstream operators saw a
/// burst-after-silence pattern. The Σ.E2 SF=10 audit showed this added
/// 1.7–2.5× to downstream `RepartitionExec.fetch_time`. Switching to a
/// bounded mpsc channel + `blocking_send` lets each batch flow as it
/// emerges; the bounded buffer also gives natural backpressure when the
/// consumer is slower than the decoder.
fn build_partition_stream(
    path: String,
    projection: Vec<usize>,
    parquet_schema: Arc<SchemaDescriptor>,
    full_schema: SchemaRef,
    row_groups: Vec<usize>,
    pm: PartitionMetrics,
) -> impl futures_util::Stream<Item = DfResult<RecordBatch>> + Send + 'static {
    // Empty fast-path: don't spawn a worker just to do nothing.
    if row_groups.is_empty() {
        return futures_util::stream::iter(Vec::<DfResult<RecordBatch>>::new()).left_stream();
    }

    // Channel capacity bounds how far the decoder can run ahead of
    // consumption. 8 = enough buffering to absorb downstream stutter
    // (RepartitionExec batching, hash-table grow events) without
    // starving the consumer, small enough to bound peak memory.
    let (tx, rx) = tokio::sync::mpsc::channel::<DfResult<RecordBatch>>(8);

    // The blocking decoder needs its own handles for the metrics it
    // records (open/processing time, batch count, bytes). The consumer
    // side keeps the rest (baseline elapsed_compute + output_rows).
    let bytes_scanned = pm.bytes_scanned.clone();
    let output_batches = pm.output_batches.clone();
    let time_opening = pm.time_opening.clone();
    let time_processing = pm.time_processing.clone();

    tokio::task::spawn_blocking(move || {
        let send_err = |tx: &tokio::sync::mpsc::Sender<DfResult<RecordBatch>>,
                        e: DataFusionError| {
            let _ = tx.blocking_send(Err(e));
        };

        let open_timer = time_opening.timer();

        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                send_err(
                    &tx,
                    DataFusionError::External(
                        format!("FastParquetExec: open `{path}` failed: {e}").into(),
                    ),
                );
                return;
            }
        };
        // `with_schema` lets parquet-rs honour our Utf8View/BinaryView
        // promotion: at column-decode time it sees the hint and routes
        // the column through the view-emitting array reader (see
        // parquet-58.1.0 src/arrow/array_reader/byte_view_array.rs).
        // The hint covers the full file schema, not just the projection.
        let options = ArrowReaderOptions::new().with_schema(full_schema);
        let builder = match ParquetRecordBatchReaderBuilder::try_new_with_options(file, options) {
            Ok(b) => b,
            Err(e) => {
                send_err(
                    &tx,
                    DataFusionError::External(
                        format!("FastParquetExec: builder failed: {e}").into(),
                    ),
                );
                return;
            }
        };
        let mask = ProjectionMask::leaves(&parquet_schema, projection.iter().copied());
        let mut reader = match builder
            .with_projection(mask)
            .with_row_groups(row_groups)
            .with_batch_size(DEFAULT_BATCH_SIZE)
            .build()
        {
            Ok(r) => r,
            Err(e) => {
                send_err(
                    &tx,
                    DataFusionError::External(
                        format!("FastParquetExec: reader build failed: {e}").into(),
                    ),
                );
                return;
            }
        };
        // Stop counting "opening" time once we have a live reader; the
        // remaining time inside the loop is decode work.
        drop(open_timer);

        loop {
            let timer = time_processing.timer();
            let next = reader.next();
            drop(timer);
            let Some(batch_res) = next else { break };
            let item = batch_res.map_err(|e| {
                DataFusionError::External(
                    format!("FastParquetExec: batch decode failed: {e}").into(),
                )
            });
            if let Ok(ref b) = item {
                output_batches.add(1);
                bytes_scanned.add(b.get_array_memory_size());
            }
            // blocking_send returns Err only when the receiver dropped —
            // the consumer doesn't want more data, so stop decoding.
            if tx.blocking_send(item).is_err() {
                return;
            }
        }
    });

    // Adapt `Receiver` to `Stream` without pulling in `tokio-stream`.
    // Wrap the recv so each poll's wall-clock counts toward
    // `elapsed_compute` and per-batch rows are reported via baseline.
    let baseline = pm.baseline;
    futures_util::stream::unfold((rx, baseline), |(mut rx, baseline)| async move {
        let timer = baseline.elapsed_compute().timer();
        let item = rx.recv().await;
        drop(timer);
        if let Some(Ok(ref batch)) = item {
            baseline.record_output(batch.num_rows());
        }
        item.map(|i| (i, (rx, baseline)))
    })
    .right_stream()
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
    async fn scan_partition_count_is_min_of_rgs_and_target() {
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        use datafusion::prelude::{SessionConfig, SessionContext};
        let prov = FastParquetTableProvider::try_new(path).unwrap();

        // Case 1: target=4, num_row_groups=6 → partitions=4 (target wins).
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let state = ctx.state();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        assert_eq!(exec.properties().partitioning.partition_count(), 4);

        // Case 2: target=32, num_row_groups=6 → partitions=6 (RGs wins).
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(32));
        let state = ctx.state();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        assert_eq!(exec.properties().partitioning.partition_count(), 6);

        // Statistics propagation: SF=1 lineitem = 6,001,215 rows.
        let stats = exec.partition_statistics(None).unwrap();
        match stats.num_rows {
            datafusion::common::stats::Precision::Exact(n) => {
                assert_eq!(n, 6_001_215, "exact row count from parquet metadata")
            }
            _ => panic!("expected Exact num_rows from FastParquetExec"),
        }
    }

    /// Verify column min/max statistics flow through to the
    /// ExecutionPlan. Required for the planner to estimate filter
    /// selectivity (Q12 / Q19 regression cause in v2a).
    #[tokio::test(flavor = "multi_thread")]
    async fn column_statistics_min_max_populated() {
        use datafusion::common::ScalarValue;
        use datafusion::common::stats::Precision;
        use datafusion::prelude::SessionContext;
        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx = SessionContext::new();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        let state = ctx.state();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        let stats = exec.partition_statistics(None).unwrap();
        assert_eq!(stats.column_statistics.len(), 16, "16 lineitem cols");

        // l_quantity is column 4, Float64. TPC-H lineitem quantities
        // are integers 1-50 stored as doubles, so min=1, max=50.
        let qty = &stats.column_statistics[4];
        match &qty.min_value {
            Precision::Exact(ScalarValue::Float64(Some(v))) => {
                assert!((*v - 1.0).abs() < 1e-9, "l_quantity min = 1, got {v}")
            }
            other => panic!("expected Float64 min for l_quantity, got {other:?}"),
        }
        match &qty.max_value {
            Precision::Exact(ScalarValue::Float64(Some(v))) => {
                assert!((*v - 50.0).abs() < 1e-9, "l_quantity max = 50, got {v}")
            }
            other => panic!("expected Float64 max for l_quantity, got {other:?}"),
        }

        // l_shipdate is column 10, Date32. TPC-H ship dates span
        // roughly 1992-01-02 through 1998-12-01.
        let ship = &stats.column_statistics[10];
        match &ship.min_value {
            Precision::Exact(ScalarValue::Date32(Some(_))) => {}
            other => panic!("expected Date32 min for l_shipdate, got {other:?}"),
        }
        match &ship.max_value {
            Precision::Exact(ScalarValue::Date32(Some(_))) => {}
            other => panic!("expected Date32 max for l_shipdate, got {other:?}"),
        }
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

    /// v4-streaming: per-partition stream must yield batches incrementally
    /// (one per parquet read), not as a single burst after all row groups
    /// have decoded.
    ///
    /// Why: with the old `spawn_blocking → Vec<RecordBatch>` shape, the
    /// downstream `FilterExec` / `RepartitionExec` sat idle until every
    /// row group in the partition's assignment finished decoding, then
    /// processed a burst. The Σ.E2 SF=10 EXPLAIN ANALYZE attributed
    /// 1.7–2.5× of the loss-query slowdown to this head-of-line block
    /// (e.g. Q10 lineitem `RepartitionExec.fetch_time` was 3.07s vs DF
    /// default's 1.22s — 2.5× higher).
    ///
    /// The behavioral invariant: time-to-first-batch must be much smaller
    /// than total-decode-time when a partition has many batches. We pin a
    /// loose ratio (0.5) so the assertion is robust to host noise.
    #[tokio::test(flavor = "multi_thread")]
    async fn build_partition_stream_yields_batches_incrementally() {
        use datafusion::execution::TaskContext;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures_util::StreamExt;
        use std::time::Instant;

        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        // target_partitions=1 forces all 6 RGs into one partition, so
        // a Vec-materializing impl waits for all of them before yielding.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let state = ctx.state();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, task_ctx).unwrap();

        let start = Instant::now();
        let _first = stream
            .next()
            .await
            .expect("at least one batch from non-empty file")
            .expect("first batch decodes ok");
        let first_batch_time = start.elapsed();

        let mut count = 1usize;
        while let Some(b) = stream.next().await {
            b.expect("batch decodes ok");
            count += 1;
        }
        let total_time = start.elapsed();

        // SF=1 lineitem = 6,001,215 rows / 65,536 batch_size ≈ 92 batches.
        // Allow generous slack for batch-size rounding at RG boundaries.
        assert!(
            (50..=200).contains(&count),
            "expected ~92 batches from SF=1 lineitem, got {count}"
        );

        // Streaming property: first batch must arrive well before the
        // last one. A Vec-materializing impl would fail this with ratio
        // ≈ 1.0; the streaming impl typically hits ratio < 0.1.
        let ratio = first_batch_time.as_secs_f64() / total_time.as_secs_f64();
        assert!(
            ratio < 0.5,
            "first batch took {first_batch_time:?} but full scan {total_time:?} \
             (ratio={ratio:.3}) — partition stream is not yielding incrementally"
        );
    }

    /// Σ.E2 root-cause fix: `Utf8` columns on the wire should surface
    /// as `Utf8View` (StringView), matching DataFusion's default
    /// parquet reader. The diagnostic `fast_parquet_array_diff` example
    /// showed Q01's 31% SF=10 regression was caused by string columns
    /// going through legacy Utf8 kernels in downstream FilterExec and
    /// AggregatePartial; DF emits StringView, so its kernels are
    /// SIMD-optimised over the 16-byte inline view layout.
    ///
    /// The fix is to convert Utf8 → Utf8View in the schema we hand to
    /// parquet-rs's `ArrowReaderOptions::with_schema`. Same for Binary
    /// → BinaryView. For TPC-H lineitem this only affects
    /// l_returnflag, l_linestatus, l_shipmode, l_shipinstruct,
    /// l_comment, but those columns drive Q01's grouping cost.
    #[tokio::test(flavor = "multi_thread")]
    async fn string_columns_decoded_as_utf8view() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::DataType;
        use datafusion::execution::TaskContext;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures_util::StreamExt;

        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let state = ctx.state();
        let prov = FastParquetTableProvider::try_new(path).unwrap();

        // Schema-level: provider must advertise Utf8View for string cols
        // so DataFusion's planner picks the SIMD-friendly kernel paths.
        // lineitem field 8 = l_returnflag, field 9 = l_linestatus.
        let schema = prov.schema();
        for i in [8usize, 9] {
            assert!(
                matches!(schema.field(i).data_type(), DataType::Utf8View),
                "field {i} ({:?}) should be Utf8View, got {:?}",
                schema.field(i).name(),
                schema.field(i).data_type()
            );
        }

        // Runtime-level: actual decoded array must be StringViewArray.
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = exec.execute(0, task_ctx).unwrap();
        let batch = stream
            .next()
            .await
            .expect("at least one batch")
            .expect("batch decodes ok");
        let rf_arr = batch.column(8);
        assert!(
            rf_arr.as_string_view_opt().is_some(),
            "l_returnflag must downcast to StringViewArray, got {:?}",
            rf_arr.data_type()
        );
    }

    /// Σ.E2 follow-up: `FastParquetExec` must expose decode metrics so
    /// EXPLAIN ANALYZE can attribute time to the scan. Without these
    /// (we shipped with `metrics=[]`), Q01 SF=10's 31% regression is
    /// undiagnosable — we cannot see whether the gap is in parquet open,
    /// row-group decode, or batch hand-off.
    ///
    /// Minimum required surface:
    ///   - `elapsed_compute` (wall-clock time spent in poll_next)
    ///   - `output_rows` and `output_batches`
    ///   - `bytes_scanned` (sum of RecordBatch memory across all partitions)
    ///   - `time_elapsed_opening` (file open + reader build)
    ///   - `time_elapsed_processing` (cumulative time inside reader.next())
    #[tokio::test(flavor = "multi_thread")]
    async fn execution_plan_exposes_decode_metrics() {
        use datafusion::execution::TaskContext;
        use datafusion::prelude::{SessionConfig, SessionContext};
        use futures_util::StreamExt;

        let Some(path) = lineitem_parquet() else {
            eprintln!("TPC-H SF=1 data not generated; skipping test");
            return;
        };
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        let state = ctx.state();
        let prov = FastParquetTableProvider::try_new(path).unwrap();
        let exec = prov.scan(&state, None, &[], None).await.unwrap();
        let n_partitions = exec.properties().partitioning.partition_count();

        let task_ctx = Arc::new(TaskContext::default());
        let mut total_rows = 0usize;
        for p in 0..n_partitions {
            let mut s = exec.execute(p, task_ctx.clone()).unwrap();
            while let Some(b) = s.next().await {
                let batch = b.expect("batch decodes ok");
                total_rows += batch.num_rows();
            }
        }
        assert_eq!(total_rows, 6_001_215, "all SF=1 lineitem rows scanned");

        let metrics = exec
            .metrics()
            .expect("FastParquetExec should expose metrics() once instrumented");
        let names: Vec<String> = metrics
            .iter()
            .map(|m| m.value().name().to_string())
            .collect();

        let elapsed = metrics
            .elapsed_compute()
            .expect("elapsed_compute aggregated across partitions");
        assert!(elapsed > 0, "elapsed_compute should be > 0 after a scan");
        let rows = metrics.output_rows().expect("output_rows populated");
        assert_eq!(rows as usize, 6_001_215);

        for required in [
            "time_elapsed_opening",
            "time_elapsed_processing",
            "bytes_scanned",
            "output_batches",
        ] {
            assert!(
                names.iter().any(|n| n == required),
                "FastParquetExec metrics missing `{required}`; have: {names:?}"
            );
        }
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
