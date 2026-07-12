//! Σ.SC P3W — the SQL path onto sidecar indexes.
//!
//! [`try_sidecar_lookup`] is the plan-time hook the fast-parquet
//! provider calls per part: when the query's pushed-down filters
//! contain `col = <int literal>` on a column covered by a sorted-i64
//! sidecar index, every projected column is codec-materializable, and
//! the P3 selectivity gate approves, the part's scan is replaced by a
//! [`SidecarLookupExec`] (or, when the key is provably outside the
//! part's footer bounds, an empty relation — the multi-part
//! range-prune win: an orderkey-contiguous parted table answers a
//! point lookup from ONE part's index while every other part prunes
//! to empty without touching a data page).
//!
//! Correctness posture: the provider's filter pushdown stays
//! `Inexact`, so DataFusion re-applies the eq (and any other filters)
//! above this exec — the index result never has to be trusted blindly.
//! Under `EMAT_EXACT_PUSHDOWN=1` the provider claims exactness for
//! pushed filters it would have evaluated itself; the sidecar path
//! answers ONLY the eq, so it stands down entirely in that mode.
//!
//! Off-switch: `EMAT_SIDECAR_SQL` (tri-state, default ON). Inert
//! wherever no `.emx-idx` sidecar file exists next to the part — which
//! is every deployment that never ran `flow index build`, so shipped
//! defaults remain benched defaults.

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use crate::flags;
use crate::sidecar_index::{
    SidecarEqDecision, sidecar_eq_decision, sidecar_materializable, sidecar_path,
    sidecar_rows_where_eq,
};

/// `col = <int literal>` (either orientation) → `(column, key)`.
fn int_eq_shape(f: &Expr) -> Option<(String, i64)> {
    let Expr::BinaryExpr(be) = f else { return None };
    if be.op != Operator::Eq {
        return None;
    }
    let (c, lit) = match (be.left.as_ref(), be.right.as_ref()) {
        (Expr::Column(c), Expr::Literal(lit, _)) => (c, lit),
        (Expr::Literal(lit, _), Expr::Column(c)) => (c, lit),
        _ => return None,
    };
    let key = match lit {
        ScalarValue::Int64(Some(v)) => *v,
        ScalarValue::Int32(Some(v)) => *v as i64,
        _ => return None,
    };
    Some((c.name.clone(), key))
}

/// Extract the FIRST `col = <int literal>` from the pushed filter
/// list; other filters may coexist (they re-apply above under inexact
/// pushdown).
fn extract_int_eq(filters: &[Expr]) -> Option<(String, i64)> {
    filters.iter().find_map(int_eq_shape)
}

/// Should the provider accept pushdown for `expr` PURELY so the
/// sidecar path can see it? Int-eq predicates are not BridgeFilter
/// shapes (the scan kernels never consume them), so historically they
/// stayed in DataFusion's FilterExec and never reached `scan()`. When
/// a sidecar file sits next to the part, claim `Inexact` for the
/// int-eq shape: the scan-side hook then gets its chance, the eq
/// re-applies above either way, and deployments without sidecar files
/// keep byte-identical plans (the existence check gates everything).
pub(crate) fn is_sidecar_pushable(expr: &Expr, source_path: &Path) -> bool {
    flags::tri_state("EMAT_SIDECAR_SQL").unwrap_or(true)
        && !flags::present("EMAT_EXACT_PUSHDOWN")
        && int_eq_shape(expr).is_some()
        && sidecar_path(source_path).exists()
}

/// Plan-time hook (see module docs). `Ok(None)` ⇒ the caller builds
/// its normal scan.
pub(crate) fn try_sidecar_lookup(
    source_path: &Path,
    table_schema: &SchemaRef,
    projection: &[usize],
    filters: &[Expr],
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    if !flags::tri_state("EMAT_SIDECAR_SQL").unwrap_or(true)
        || flags::present("EMAT_EXACT_PUSHDOWN")
    {
        return Ok(None);
    }
    // Cheapest check first: no sidecar file, no path.
    if !sidecar_path(source_path).exists() {
        return Ok(None);
    }
    let Some((col, key)) = extract_int_eq(filters) else {
        return Ok(None);
    };
    let out_schema: SchemaRef = Arc::new(table_schema.project(projection)?);
    if !out_schema
        .fields()
        .iter()
        .all(|f| sidecar_materializable(f.data_type()))
    {
        return Ok(None);
    }
    match sidecar_eq_decision(source_path, &col, key) {
        SidecarEqDecision::Scan => Ok(None),
        SidecarEqDecision::EmptyProven => Ok(Some(Arc::new(
            datafusion::physical_plan::empty::EmptyExec::new(out_schema),
        ))),
        SidecarEqDecision::Lookup { index_name } => Ok(Some(Arc::new(SidecarLookupExec::new(
            source_path.to_path_buf(),
            index_name,
            col,
            key,
            projection.to_vec(),
            out_schema,
        )))),
    }
}

/// One-partition leaf exec answering `WHERE <col> = key` from a sorted
/// sidecar index (see module docs).
#[derive(Debug)]
pub struct SidecarLookupExec {
    source_path: PathBuf,
    index_name: String,
    col: String,
    key: i64,
    ordinals: Vec<usize>,
    out_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl SidecarLookupExec {
    fn new(
        source_path: PathBuf,
        index_name: String,
        col: String,
        key: i64,
        ordinals: Vec<usize>,
        out_schema: SchemaRef,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&out_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            source_path,
            index_name,
            col,
            key,
            ordinals,
            out_schema,
            properties,
        }
    }
}

impl DisplayAs for SidecarLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SidecarLookupExec(col={}, key={}, index={}, file={})",
            self.col,
            self.key,
            self.index_name,
            self.source_path.display()
        )
    }
}

impl ExecutionPlan for SidecarLookupExec {
    fn name(&self) -> &str {
        "SidecarLookupExec"
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
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "SidecarLookupExec has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SidecarLookupExec: invalid partition {partition}"
            )));
        }
        let path = self.source_path.clone();
        let index_name = self.index_name.clone();
        let key = self.key;
        let ordinals = self.ordinals.clone();
        let schema = Arc::clone(&self.out_schema);
        let stream_schema = Arc::clone(&self.out_schema);
        let once = futures_util::stream::once(async move {
            tokio::task::spawn_blocking(move || {
                sidecar_rows_where_eq(&path, &index_name, key, &ordinals, schema)
            })
            .await
            .map_err(|e| DataFusionError::Execution(format!("sidecar lookup task: {e}")))?
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(stream_schema, once)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ematix_fast_parquet_multi::EmatixFastParquetMultiTableProvider;
    use crate::sidecar_build::build_sorted_sidecar;
    use datafusion::arrow::array::{Array, Int64Array};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext, col, lit};
    use ematix_parquet_codec::write::{ColumnData, write_table_to_path};
    use ematix_parquet_format::types::CompressionCodec;

    /// Two-part fixture with DISJOINT sorted ident ranges per part —
    /// the parted-table shape whose point lookups should answer from
    /// ONE part's index while the other part range-prunes to empty.
    /// `ident` values stride by 3 (bounds 0..3*rows) so the P3
    /// uniform-model estimate (1/width) stays far under the 0.05 gate.
    fn write_two_parts(dir: &Path, with_sidecars: bool) -> Vec<String> {
        let mut paths = Vec::new();
        for (p, base) in [(1u32, 0i64), (2u32, 100_000i64)] {
            let ident: Vec<i64> = (0..1000).map(|i| base + i * 3).collect();
            let val: Vec<i64> = (0..1000).map(|i| base + i * 10).collect();
            let path = dir.join(format!("t-{p:04}.parquet"));
            write_table_to_path(
                &path,
                &[
                    ("ident", ColumnData::I64(&ident)),
                    ("val", ColumnData::I64(&val)),
                ],
                CompressionCodec::Uncompressed,
            )
            .expect("write part");
            if with_sidecars {
                build_sorted_sidecar(&path, "idx_ident", "ident", None).expect("build sidecar");
            }
            paths.push(path.to_string_lossy().into_owned());
        }
        paths
    }

    async fn point_query(
        paths: Vec<String>,
        sql: &str,
    ) -> (String, Vec<datafusion::arrow::array::RecordBatch>) {
        let ctx =
            SessionContext::new_with_config(SessionConfig::new().with_collect_statistics(true));
        let provider = EmatixFastParquetMultiTableProvider::try_new_files(paths).unwrap();
        ctx.register_table("t", Arc::new(provider)).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let rendered = format!("{}", displayable(plan.as_ref()).indent(true));
        let batches = df.collect().await.unwrap();
        (rendered, batches)
    }

    fn i64_values(batches: &[datafusion::arrow::array::RecordBatch]) -> Vec<i64> {
        let mut out = Vec::new();
        for b in batches {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 col");
            for i in 0..a.len() {
                out.push(a.value(i));
            }
        }
        out.sort_unstable();
        out
    }

    /// The headline: a parted point lookup answers from the covering
    /// part's index, the non-covering part proves empty from footer
    /// bounds, and the result matches the scan answer exactly.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sql_point_lookup_answers_from_sidecar_across_parts() {
        let dir = tempfile::tempdir().unwrap();
        let paths = write_two_parts(dir.path(), true);
        // ident = 42 lives in part 1 (bounds 0..2997); part 2's bounds
        // start at 100_000 → EmptyProven without a data-page read.
        let sql = "SELECT val FROM t WHERE ident = 42";
        let (plan, batches) = point_query(paths.clone(), sql).await;
        assert!(
            plan.contains("SidecarLookupExec"),
            "covered point lookup must plan the sidecar exec:\n{plan}"
        );
        assert!(
            plan.contains("EmptyExec"),
            "non-covering part must range-prune to empty:\n{plan}"
        );
        // ident=42 = base 0 + i*3 with i=14 → val = 140.
        assert_eq!(i64_values(&batches), vec![140]);

        // Oracle: identical rows without sidecars (pure scan).
        let dir2 = tempfile::tempdir().unwrap();
        let paths2 = write_two_parts(dir2.path(), false);
        let (plan2, batches2) = point_query(paths2, sql).await;
        assert!(!plan2.contains("SidecarLookupExec"));
        assert_eq!(i64_values(&batches2), i64_values(&batches));
    }

    /// Widened-type fixture (0.17.3 lazy materializers): `ident` (i64,
    /// indexed) plus Int32/Date32/Utf8 projection columns. Written via
    /// parquet-rs because the codec test writer has no DATE annotation
    /// route; dictionary off ⇒ PLAIN pages, the encoding every masked
    /// decoder supports.
    fn write_typed_part(dir: &Path, with_sidecar: bool) -> String {
        use datafusion::parquet::basic::{
            Compression, ConvertedType, Repetition, Type as PhysicalType,
        };
        use datafusion::parquet::column::writer::ColumnWriter;
        use datafusion::parquet::data_type::ByteArray;
        use datafusion::parquet::file::properties::WriterProperties;
        use datafusion::parquet::file::writer::SerializedFileWriter;
        use datafusion::parquet::schema::types::Type as PType;

        let prim = |name: &str, pt: PhysicalType, ct: ConvertedType| {
            Arc::new(
                PType::primitive_type_builder(name, pt)
                    .with_repetition(Repetition::REQUIRED)
                    .with_converted_type(ct)
                    .build()
                    .unwrap(),
            )
        };
        let schema = Arc::new(
            PType::group_type_builder("schema")
                .with_fields(vec![
                    prim("ident", PhysicalType::INT64, ConvertedType::NONE),
                    prim("small", PhysicalType::INT32, ConvertedType::NONE),
                    prim("day", PhysicalType::INT32, ConvertedType::DATE),
                    prim("tag", PhysicalType::BYTE_ARRAY, ConvertedType::UTF8),
                ])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::UNCOMPRESSED)
                .set_dictionary_enabled(false)
                .build(),
        );
        let path = dir.join("typed-0001.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut rg = writer.next_row_group().unwrap();

        let ident: Vec<i64> = (0..1000).map(|i| i * 3).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int64ColumnWriter(t) = col.untyped() {
            t.write_batch(&ident, None, None).unwrap();
        }
        col.close().unwrap();

        let small: Vec<i32> = (0..1000).map(|i| i * 7).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&small, None, None).unwrap();
        }
        col.close().unwrap();

        let day: Vec<i32> = (0..1000).map(|i| 19_000 + i).collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::Int32ColumnWriter(t) = col.untyped() {
            t.write_batch(&day, None, None).unwrap();
        }
        col.close().unwrap();

        let tag: Vec<ByteArray> = (0..1000)
            .map(|i| ByteArray::from(format!("tag-{i:04}").into_bytes()))
            .collect();
        let mut col = rg.next_column().unwrap().unwrap();
        if let ColumnWriter::ByteArrayColumnWriter(t) = col.untyped() {
            t.write_batch(&tag, None, None).unwrap();
        }
        col.close().unwrap();

        rg.close().unwrap();
        writer.close().unwrap();

        if with_sidecar {
            build_sorted_sidecar(&path, "idx_ident", "ident", None).expect("build sidecar");
        }
        path.to_string_lossy().into_owned()
    }

    /// 0.17.3 widening: Int32/Date32/Utf8 projections answer from the
    /// sidecar and match the pure-scan oracle exactly (Date32 renders
    /// as a real date, proving the wrapper, not just the bytes).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn widened_projections_answer_from_sidecar() {
        let fmt = |bs: &[datafusion::arrow::array::RecordBatch]| {
            datafusion::arrow::util::pretty::pretty_format_batches(bs)
                .unwrap()
                .to_string()
        };
        let dir = tempfile::tempdir().unwrap();
        let indexed = write_typed_part(dir.path(), true);
        let sql = "SELECT small, day, tag FROM t WHERE ident = 42";
        let (plan, batches) = point_query(vec![indexed], sql).await;
        assert!(
            plan.contains("SidecarLookupExec"),
            "widened projection must stay on the sidecar path:\n{plan}"
        );
        // ident=42 → row 14 → small=98, day=19014 (2022-01-22), tag-0014.
        let got = fmt(&batches);
        assert!(
            got.contains("98") && got.contains("2022-01-22") && got.contains("tag-0014"),
            "unexpected sidecar rows:\n{got}"
        );

        let dir2 = tempfile::tempdir().unwrap();
        let plain = write_typed_part(dir2.path(), false);
        let (plan2, batches2) = point_query(vec![plain], sql).await;
        assert!(!plan2.contains("SidecarLookupExec"), "{plan2}");
        assert_eq!(fmt(&batches2), got, "sidecar answer must equal scan oracle");
    }

    /// No sidecar file ⇒ the hook is inert (plan and rows unchanged).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_sidecar_files_scan_normally() {
        let dir = tempfile::tempdir().unwrap();
        let paths = write_two_parts(dir.path(), false);
        let (plan, batches) = point_query(paths, "SELECT val FROM t WHERE ident = 300000").await;
        assert!(!plan.contains("SidecarLookupExec"), "{plan}");
        assert!(i64_values(&batches).is_empty());
    }

    /// A projected type the codec can't materialize (F64) stands the
    /// sidecar path down — exact results via the normal scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unmaterializable_projection_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let ident: Vec<i64> = (0..1000).map(|i| i * 3).collect();
        let price: Vec<f64> = (0..1000).map(|i| i as f64 * 1.5).collect();
        let path = dir.path().join("t-0001.parquet");
        write_table_to_path(
            &path,
            &[
                ("ident", ColumnData::I64(&ident)),
                ("price", ColumnData::F64(&price)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        build_sorted_sidecar(&path, "idx_ident", "ident", None).unwrap();
        let (plan, batches) = point_query(
            vec![path.to_string_lossy().into_owned()],
            "SELECT price FROM t WHERE ident = 42",
        )
        .await;
        assert!(
            !plan.contains("SidecarLookupExec"),
            "F64 projection must fall back to scan:\n{plan}"
        );
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    /// A wide-selectivity key (narrow footer width) trips the P3 gate
    /// — scan, not index.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wide_selectivity_scans() {
        let dir = tempfile::tempdir().unwrap();
        // 1000 rows over ident 0..9 → est 0.1 > 0.05 default gate.
        let ident: Vec<i64> = (0..1000).map(|i| i % 10).collect();
        let val: Vec<i64> = (0..1000).collect();
        let path = dir.path().join("t-0001.parquet");
        write_table_to_path(
            &path,
            &[
                ("ident", ColumnData::I64(&ident)),
                ("val", ColumnData::I64(&val)),
            ],
            CompressionCodec::Uncompressed,
        )
        .unwrap();
        build_sorted_sidecar(&path, "idx_ident", "ident", None).unwrap();
        let (plan, batches) = point_query(
            vec![path.to_string_lossy().into_owned()],
            "SELECT val FROM t WHERE ident = 4",
        )
        .await;
        assert!(
            !plan.contains("SidecarLookupExec"),
            "0.1 estimated selectivity must scan:\n{plan}"
        );
        assert_eq!(i64_values(&batches).len(), 100);
    }

    /// `extract_int_eq` accepts both orientations and skips non-eq.
    #[test]
    fn extract_int_eq_shapes() {
        let eq = col("ident").eq(lit(42i64));
        let eq_flipped = lit(7i64).eq(col("ident"));
        let gt = col("ident").gt(lit(1i64));
        assert_eq!(
            extract_int_eq(&[gt.clone(), eq]),
            Some(("ident".to_string(), 42))
        );
        assert_eq!(
            extract_int_eq(&[eq_flipped]),
            Some(("ident".to_string(), 7))
        );
        assert_eq!(extract_int_eq(&[gt]), None);
    }
}
