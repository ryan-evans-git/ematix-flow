//! Phase Z WorkUnit — the serialised job spec sent to an ephemeral
//! worker (K8s Job pod, Lambda invocation, or a local
//! `flow run-shard` invocation for testing).
//!
//! Different from the long-running [`crate::DistributedBackend`]
//! model in this crate: the WorkUnit pattern targets *one-shot*
//! workers that consume a single shard of a query, write a partial
//! result to S3, and exit. Tracked in
//! [`docs/DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`].
//!
//! Schema version: `v1` (signalled by `$schema` field). Future
//! additions to `Query` discriminants or `Input` shapes should
//! preserve backwards compatibility for v1; breaking changes
//! bump to `v2`.

use serde::{Deserialize, Serialize};

/// Top-level WorkUnit. Deserialised from JSON the worker reads on
/// stdin, from a file (via `--work-unit-file`), or that
/// `LambdaExecutor` / `K8sJobExecutor` writes into the event /
/// ConfigMap respectively.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkUnit {
    /// Schema pin — `"https://ematix-flow/work-unit.v1.json"`.
    /// Coordinators MUST set this; workers MUST validate it.
    #[serde(rename = "$schema", default = "WorkUnit::default_schema")]
    pub schema: String,

    /// Stable identifier for this unit. The worker echoes it in
    /// its metrics-JSON output so the coordinator can correlate.
    pub id: String,

    /// What to execute.
    pub query: Query,

    /// Where the input data lives.
    pub input: Input,

    /// Where the worker writes its partial result.
    pub output: Output,

    /// Optional per-unit execution knobs. All have sensible
    /// defaults so the coordinator can omit this whole block.
    #[serde(default)]
    pub execution: Execution,
}

impl WorkUnit {
    pub fn default_schema() -> String {
        "https://ematix-flow/work-unit.v1.json".to_string()
    }

    /// Validate schema pin matches v1 expectations. Returns
    /// `Err` if the schema is unknown — let the caller decide
    /// whether to refuse or attempt best-effort.
    pub fn validate_schema(&self) -> Result<(), String> {
        if self.schema == Self::default_schema() {
            Ok(())
        } else {
            Err(format!(
                "unsupported WorkUnit schema {:?}; this build understands {:?}",
                self.schema,
                Self::default_schema()
            ))
        }
    }
}

/// What query to run. The `kind` discriminator is forward-compat —
/// future variants (`sql`, `udf`, etc.) get added here without
/// breaking the v1 schema, since serde's `tag` field falls into
/// the `#[serde(other)]` arm when unknown.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Query {
    /// One of the 22 TPC-H queries. `id` is canonical "Q01".."Q22".
    Tpch { id: String },
    /// Arbitrary SQL, executed verbatim by the worker against the tables its
    /// `Input` registers. Added for Σ.SC I.3: an [`Input::IcebergScan`] is not
    /// bound to the TPC-H catalog, so its units carry their own SQL. Additive
    /// to v1 — old JSON (`kind: "tpch"`) decodes unchanged.
    Sql { sql: String },
    // Future: Udf { ... }, etc.
}

impl Query {
    /// Short label echoed in [`WorkUnitMetrics::query`] — the TPC-H id
    /// (`"Q14"`) or the literal `"sql"`. Deliberately NOT the SQL text:
    /// metrics lines are parsed/grepped by coordinators and dashboards, and a
    /// multi-line query would mangle them.
    pub fn label(&self) -> &str {
        match self {
            Query::Tpch { id } => id,
            Query::Sql { .. } => "sql",
        }
    }
}

/// Where the input data lives.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Input {
    /// Parquet files organised by table under a common prefix.
    /// Optional `row_group_range` per table partitions the work
    /// across workers.
    ParquetPartition {
        /// S3 (`s3://bucket/prefix`) or local (`file:///path`) URI.
        uri_prefix: String,
        /// Table names that must be readable under the prefix.
        tables: Vec<String>,
        /// Optional row-group sharding per table. Maps table name
        /// to `(start, end_exclusive)`. Tables absent from this
        /// map are read in full.
        #[serde(default)]
        row_group_range: std::collections::BTreeMap<String, (u32, u32)>,
    },
    /// A **pre-pruned** Iceberg scan. The coordinator (built
    /// `--features iceberg`) has already walked the table's manifest and
    /// run the conservative `(min,max)` summary prune
    /// (`ematix_flow_core::iceberg_scan`), so this carries the *explicit*
    /// surviving files — no prefix listing on the worker. Deliberately
    /// **iceberg-free**: plain strings + i64 only, so the worker never
    /// links iceberg-rust and the wire schema is stable.
    IcebergScan {
        /// Table these files belong to (for multi-table queries).
        table: String,
        /// The index the coordinator pruned on. The worker uses it for the
        /// per-file sidecar lookup on the [`IcebergScanTarget::Indexed`]
        /// files.
        index_name: String,
        /// The predicate that was pruned on, replayed by the worker as the
        /// sidecar lookup (and re-applied on the full-scan files). `None` for
        /// a pure enumeration fan-out (no prunable predicate — every file
        /// survives and is full-scanned). `#[serde(default)]` keeps v1 JSON
        /// with a predicate object decoding unchanged.
        #[serde(default)]
        predicate: Option<IcebergPredicate>,
        /// Surviving files, post manifest-prune, each tagged index-readable
        /// vs must-full-scan. Mirrors
        /// `ematix_flow_core::iceberg_scan::IcebergScanPlan`.
        targets: Vec<IcebergScanTarget>,
    },
}

/// The predicate an [`Input::IcebergScan`] was pruned on, in a wire-stable,
/// iceberg-free form. INT64 only for now — matches the sidecar read-side
/// primitive (`ematix_flow_core::sidecar_index`); other physical types follow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IcebergPredicate {
    /// `WHERE <indexed_col> = key`.
    Eq { key: i64 },
    /// `WHERE <indexed_col> BETWEEN low AND high`, either bound open (`null`).
    Range {
        #[serde(default)]
        low: Option<i64>,
        #[serde(default)]
        high: Option<i64>,
    },
}

/// One surviving file in an [`Input::IcebergScan`], tagged with how the worker
/// should read it. The iceberg-free wire twin of
/// `ematix_flow_core::iceberg_scan::ScanTarget`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "read", rename_all = "snake_case")]
pub enum IcebergScanTarget {
    /// Survived the prune and has a covering sidecar: the worker opens
    /// `sidecar_uri` and does an index lookup + masked decode of `data_uri`.
    Indexed {
        data_uri: String,
        sidecar_uri: String,
    },
    /// Survived the prune with no covering sidecar: the worker **must**
    /// full-scan `data_uri` and re-apply the predicate (the manifest prune is
    /// conservative, not exact — dropping this would lose rows).
    FullScan { data_uri: String },
}

impl IcebergScanTarget {
    /// The source Parquet URI, regardless of read strategy — what the worker
    /// opens through the ematix codec either way. Mirrors
    /// `ematix_flow_core::iceberg_scan::ScanTarget::data_uri`.
    pub fn data_uri(&self) -> &str {
        match self {
            IcebergScanTarget::Indexed { data_uri, .. }
            | IcebergScanTarget::FullScan { data_uri } => data_uri,
        }
    }
}

/// Where the worker writes its partial result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Output {
    /// Single Arrow IPC stream at the given URI. S3
    /// (`s3://bucket/key`) or local (`file:///path`).
    ArrowIpc { uri: String },
}

/// Optional execution knobs. All defaults; coordinator can omit
/// the whole block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execution {
    /// Worker-side thread count. Defaults to number of CPUs.
    #[serde(default)]
    pub threads: Option<usize>,
    /// Engage `EmatixFastParquetTableProvider::with_dict_preservation`.
    /// Defaults to `false` (2026-06-21): dict-preservation HARD-ERRORS on
    /// standard parquet whose writer PLAIN-falls-back a high-card string
    /// column ("dict-preserved read: data page is PLAIN-encoded"), breaking
    /// Q09/Q10/Q13-class queries. It is only safe on all-dict (ematix-written)
    /// data, so it is opt-in. (Was `true` — a latent correctness bug.)
    #[serde(default)]
    pub with_dict_preservation: bool,
    /// Engage late-materialization on FastParquet. Defaults to
    /// `true` (Σ.E5a standard).
    #[serde(default = "default_true")]
    pub with_late_mat: bool,
    /// Engage Π.14 adaptive predicate dispatch. Defaults to `true`.
    #[serde(default = "default_true")]
    pub with_adaptive_predicate: bool,
}

impl Default for Execution {
    fn default() -> Self {
        Self {
            threads: None,
            // false: dict-preservation hard-errors on PLAIN-fallback string
            // columns in standard parquet (see field doc) — opt-in only.
            with_dict_preservation: false,
            with_late_mat: true,
            with_adaptive_predicate: true,
        }
    }
}

fn default_true() -> bool {
    true
}

/// Structured metrics the worker emits on stdout as a single JSON
/// line on successful exit. Coordinators parse this verbatim. Logs
/// go to stderr (`tracing` formatter), so stdout stays clean.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkUnitMetrics {
    /// Echo of [`WorkUnit::id`] for correlation.
    pub work_unit_id: String,
    /// Echo of the query id (e.g. `"Q14"`).
    pub query: String,
    /// Wall-clock time in millseconds for the whole worker run
    /// (from process start to result write).
    pub wall_ms: u64,
    /// Optional sub-timings; populate as we wire up the real
    /// execution path. Keeping the schema permissive on v1.
    #[serde(default)]
    pub decode_ms: Option<u64>,
    #[serde(default)]
    pub exec_ms: Option<u64>,
    #[serde(default)]
    pub output_write_ms: Option<u64>,
    /// Rows scanned from the input.
    #[serde(default)]
    pub rows_in: Option<u64>,
    /// Rows in the output partial result.
    #[serde(default)]
    pub rows_out: Option<u64>,
    /// Final output location.
    pub output_uri: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: the canonical work-unit JSON from
    /// [`docs/DISTRIBUTED_TPCH_BENCHMARK_PLAN.md`] round-trips
    /// through serde.
    #[test]
    fn work_unit_round_trip_v1() {
        let json = r#"{
            "$schema": "https://ematix-flow/work-unit.v1.json",
            "id": "wu-7a3f",
            "query": {"kind": "tpch", "id": "Q14"},
            "input": {
                "kind": "parquet_partition",
                "uri_prefix": "s3://bucket/tpch/sf10",
                "tables": ["lineitem", "part"],
                "row_group_range": {"lineitem": [0, 21]}
            },
            "output": {
                "kind": "arrow_ipc",
                "uri": "s3://bucket/results/run-c4f8/q14-shard-3.arrow"
            },
            "execution": {
                "threads": 4,
                "with_dict_preservation": true,
                "with_late_mat": true,
                "with_adaptive_predicate": true
            }
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse");
        wu.validate_schema().expect("schema ok");
        assert_eq!(wu.id, "wu-7a3f");
        match &wu.query {
            Query::Tpch { id } => assert_eq!(id, "Q14"),
            other => panic!("expected Tpch, got {other:?}"),
        }
        // Re-serialise + re-parse must yield identical.
        let s = serde_json::to_string(&wu).unwrap();
        let wu2: WorkUnit = serde_json::from_str(&s).unwrap();
        assert_eq!(wu, wu2);
    }

    /// Minimal WorkUnit — only required fields. Execution defaults
    /// must fill in.
    #[test]
    fn work_unit_minimal_uses_execution_defaults() {
        let json = r#"{
            "id": "wu-minimal",
            "query": {"kind": "tpch", "id": "Q01"},
            "input": {
                "kind": "parquet_partition",
                "uri_prefix": "file:///tmp/sf1",
                "tables": ["lineitem"]
            },
            "output": {"kind": "arrow_ipc", "uri": "file:///tmp/out.arrow"}
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse");
        // Schema default applied
        assert_eq!(wu.schema, WorkUnit::default_schema());
        // Execution defaults: dict_preservation OFF (PLAIN-fallback safety),
        // late_mat + adaptive_predicate ON, threads unset.
        assert!(
            !wu.execution.with_dict_preservation,
            "dict_preservation must default OFF — true hard-errors on PLAIN-fallback string columns"
        );
        assert!(wu.execution.with_late_mat);
        assert!(wu.execution.with_adaptive_predicate);
        assert_eq!(wu.execution.threads, None);
        // No row_group_range for an unsharded run
        match &wu.input {
            Input::ParquetPartition {
                row_group_range, ..
            } => {
                assert!(row_group_range.is_empty());
            }
            other => panic!("expected ParquetPartition, got {other:?}"),
        }
    }

    /// A pre-pruned Iceberg scan round-trips through JSON, and the tagged
    /// target/predicate payloads decode to the right variants.
    #[test]
    fn iceberg_scan_input_round_trip() {
        let json = r#"{
            "id": "wu-ice-1",
            "query": {"kind": "tpch", "id": "Q06"},
            "input": {
                "kind": "iceberg_scan",
                "table": "lineitem",
                "index_name": "idx_l_shipdate",
                "predicate": {"op": "range", "low": 8766, "high": 9131},
                "targets": [
                    {"read": "indexed", "data_uri": "s3://b/lineitem/p0.parquet", "sidecar_uri": "s3://b/lineitem/p0.parquet.idx"},
                    {"read": "full_scan", "data_uri": "s3://b/lineitem/p1.parquet"}
                ]
            },
            "output": {"kind": "arrow_ipc", "uri": "file:///out.arrow"}
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse iceberg_scan");
        match &wu.input {
            Input::IcebergScan {
                table,
                index_name,
                predicate,
                targets,
            } => {
                assert_eq!(table, "lineitem");
                assert_eq!(index_name, "idx_l_shipdate");
                assert_eq!(
                    *predicate,
                    Some(IcebergPredicate::Range {
                        low: Some(8766),
                        high: Some(9131),
                    })
                );
                assert_eq!(targets.len(), 2);
                assert_eq!(
                    targets[0],
                    IcebergScanTarget::Indexed {
                        data_uri: "s3://b/lineitem/p0.parquet".into(),
                        sidecar_uri: "s3://b/lineitem/p0.parquet.idx".into(),
                    }
                );
                assert_eq!(
                    targets[1],
                    IcebergScanTarget::FullScan {
                        data_uri: "s3://b/lineitem/p1.parquet".into(),
                    }
                );
            }
            other => panic!("expected IcebergScan, got {other:?}"),
        }
        // Full serialize → deserialize identity.
        let s = serde_json::to_string(&wu).unwrap();
        let wu2: WorkUnit = serde_json::from_str(&s).unwrap();
        assert_eq!(wu, wu2);
    }

    /// Σ.SC I.3: an `iceberg_scan` may omit `predicate` entirely — a pure
    /// enumeration fan-out (the coordinator listed the snapshot's files but
    /// had no prunable predicate). Parses to `None` via `#[serde(default)]`,
    /// which is also the v1 additive-compat proof: pre-existing JSON with a
    /// predicate object still decodes (see `iceberg_scan_input_round_trip`).
    #[test]
    fn iceberg_scan_without_predicate_parses_none() {
        let json = r#"{
            "id": "wu-ice-nopred",
            "query": {"kind": "tpch", "id": "Q01"},
            "input": {
                "kind": "iceberg_scan",
                "table": "lineitem",
                "index_name": "idx_id",
                "targets": [
                    {"read": "full_scan", "data_uri": "file:///t/p0.parquet"}
                ]
            },
            "output": {"kind": "arrow_ipc", "uri": "file:///out.arrow"}
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse no-predicate scan");
        match &wu.input {
            Input::IcebergScan { predicate, .. } => assert!(predicate.is_none()),
            other => panic!("expected IcebergScan, got {other:?}"),
        }
    }

    /// Σ.SC I.3: the `sql` query variant the enum was designed to grow — the
    /// worker executes the carried SQL verbatim. Iceberg scans are not bound
    /// to the TPC-H catalog, so the oracle tests (and real users) need it.
    #[test]
    fn sql_query_round_trips_and_labels() {
        let json = r#"{
            "id": "wu-sql-1",
            "query": {"kind": "sql", "sql": "SELECT id, val FROM t WHERE id = 5"},
            "input": {
                "kind": "iceberg_scan",
                "table": "t",
                "index_name": "idx_id",
                "predicate": {"op": "eq", "key": 5},
                "targets": [
                    {"read": "full_scan", "data_uri": "file:///t/p0.parquet"}
                ]
            },
            "output": {"kind": "arrow_ipc", "uri": "file:///out.arrow"}
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse sql query");
        match &wu.query {
            Query::Sql { sql } => assert_eq!(sql, "SELECT id, val FROM t WHERE id = 5"),
            other => panic!("expected Sql, got {other:?}"),
        }
        // Metrics echo label: TPC-H units echo their id; SQL units echo "sql".
        assert_eq!(wu.query.label(), "sql");
        assert_eq!(Query::Tpch { id: "Q14".into() }.label(), "Q14");
        let s = serde_json::to_string(&wu).unwrap();
        let wu2: WorkUnit = serde_json::from_str(&s).unwrap();
        assert_eq!(wu, wu2);
    }

    /// Σ.SC I.3: serialize → deserialize → serialize is BYTE-identical for an
    /// IcebergScan unit. Coordinators may hash/dedupe serialized WorkUnits
    /// (retry idempotency), so re-encoding must not reorder or reshape fields.
    #[test]
    fn iceberg_scan_serialization_is_byte_stable() {
        let wu = WorkUnit {
            schema: WorkUnit::default_schema(),
            id: "wu-ice-stable".into(),
            query: Query::Tpch { id: "Q06".into() },
            input: Input::IcebergScan {
                table: "lineitem".into(),
                index_name: "idx_id".into(),
                predicate: Some(IcebergPredicate::Range {
                    low: Some(8766),
                    high: None,
                }),
                targets: vec![
                    IcebergScanTarget::Indexed {
                        data_uri: "s3://b/p0.parquet".into(),
                        sidecar_uri: "s3://b/p0.parquet.idx".into(),
                    },
                    IcebergScanTarget::FullScan {
                        data_uri: "s3://b/p1.parquet".into(),
                    },
                ],
            },
            output: Output::ArrowIpc {
                uri: "file:///out.arrow".into(),
            },
            execution: Execution::default(),
        };
        let s1 = serde_json::to_string(&wu).unwrap();
        let wu2: WorkUnit = serde_json::from_str(&s1).unwrap();
        let s2 = serde_json::to_string(&wu2).unwrap();
        assert_eq!(
            s1, s2,
            "re-encoding an IcebergScan unit must be byte-stable"
        );
        assert_eq!(wu, wu2);
    }

    /// An open-ended range (`high` omitted) defaults the missing bound to
    /// `None`, and eq predicates tag as `eq`.
    #[test]
    fn iceberg_predicate_shapes() {
        let eq: IcebergPredicate = serde_json::from_str(r#"{"op":"eq","key":42}"#).unwrap();
        assert_eq!(eq, IcebergPredicate::Eq { key: 42 });
        let open: IcebergPredicate = serde_json::from_str(r#"{"op":"range","low":100}"#).unwrap();
        assert_eq!(
            open,
            IcebergPredicate::Range {
                low: Some(100),
                high: None,
            }
        );
    }

    /// Unknown schema is reported, not silently accepted.
    #[test]
    fn work_unit_rejects_unknown_schema() {
        let json = r#"{
            "$schema": "https://ematix-flow/work-unit.v999.json",
            "id": "wu-future",
            "query": {"kind": "tpch", "id": "Q01"},
            "input": {
                "kind": "parquet_partition",
                "uri_prefix": "file:///tmp",
                "tables": ["t"]
            },
            "output": {"kind": "arrow_ipc", "uri": "file:///out"}
        }"#;
        let wu: WorkUnit = serde_json::from_str(json).expect("parse");
        assert!(wu.validate_schema().is_err());
    }

    /// Metrics-JSON round-trip — the format the worker emits on
    /// stdout for the coordinator to parse.
    #[test]
    fn work_unit_metrics_round_trip() {
        let m = WorkUnitMetrics {
            work_unit_id: "wu-7a3f".into(),
            query: "Q14".into(),
            wall_ms: 423,
            decode_ms: Some(287),
            exec_ms: Some(136),
            output_write_ms: None,
            rows_in: Some(6_001_215),
            rows_out: Some(1),
            output_uri: "s3://bucket/results/run-c4f8/q14-shard-3.arrow".into(),
        };
        let s = serde_json::to_string(&m).unwrap();
        let m2: WorkUnitMetrics = serde_json::from_str(&s).unwrap();
        assert_eq!(m, m2);
    }
}
