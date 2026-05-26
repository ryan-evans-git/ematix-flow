//! Phase Z α: `flow run-shard` execute path.
//!
//! Takes a parsed [`WorkUnit`], registers the input parquet files
//! as `EmatixFastParquetTableProvider` instances on a fresh
//! `SessionContext`, executes the query, and writes the partial
//! result as Arrow IPC to the output URI.
//!
//! Scope of α:
//! - `file://` URIs only (S3 lands in α.2 via `object_store`)
//! - TPC-H query catalog embedded via `include_str!` (Q01-Q22)
//! - `row_group_range` is honored advisorily (we don't yet thread
//!   it into the parquet provider; full integration in α.3)
//! - Single-process execution; per-shard fanout is the
//!   coordinator's job
//!
//! Error model mirrors the CLI exit codes documented on
//! `Commands::RunShard`: `Config` → exit 2, `Execution` → exit 3,
//! `Output` → exit 4.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_ipc::writer::FileWriter as ArrowIpcWriter;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use ematix_flow_core::ematix_fast_parquet::EmatixFastParquetTableProvider;
use ematix_flow_core::preset;
use ematix_flow_distributed::work_unit::{Input, Output, Query, WorkUnit, WorkUnitMetrics};

#[derive(Debug, thiserror::Error)]
pub enum RunShardError {
    /// Config-level failure: bad spec, unreachable input, unknown
    /// query id. Caller maps to exit code 2.
    #[error("config error: {0}")]
    Config(String),

    /// Execution failure mid-run: SQL planning error, DataFusion
    /// error, predicate panic. Caller maps to exit code 3.
    #[error("execution error: {0}")]
    Execution(String),

    /// Output failure: cannot create the destination, Arrow IPC
    /// write error, S3 upload error. Caller maps to exit code 4.
    #[error("output error: {0}")]
    Output(String),
}

impl RunShardError {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Config(_) => 2,
            Self::Execution(_) => 3,
            Self::Output(_) => 4,
        }
    }
}

/// Execute a single shard. Returns the metrics envelope on
/// success; caller serialises to stdout JSON.
pub async fn execute_work_unit(wu: &WorkUnit) -> Result<WorkUnitMetrics, RunShardError> {
    let started = Instant::now();

    // ---- 1. Resolve input prefix to a local path ----
    let (prefix, tables) = match &wu.input {
        Input::ParquetPartition {
            uri_prefix, tables, ..
        } => (uri_prefix.as_str(), tables.as_slice()),
    };
    let prefix_path = uri_to_local_path(prefix)
        .map_err(|e| RunShardError::Config(format!("input.uri_prefix: {e}")))?;
    if !prefix_path.is_dir() {
        return Err(RunShardError::Config(format!(
            "input.uri_prefix {prefix:?} does not resolve to an existing directory ({:?})",
            prefix_path
        )));
    }

    // ---- 2. Resolve query SQL ----
    let sql = match &wu.query {
        Query::Tpch { id } => {
            tpch_sql(id).map_err(|e| RunShardError::Config(format!("query.id: {e}")))?
        }
    };

    // ---- 3. Build session + register tables ----
    //
    // Σ.V (2026-05-26): install the ematix preset rule chain so a
    // CLI shard produces the same plan shape as the bench. Before
    // this change the CLI ran with vanilla DataFusion — losing
    // Σ.Q.L10 LeftSemi pushdown, Σ.Q.L1b RobinHood SUM, Σ.Q.L9
    // bloom sideband, and the Σ.D/Σ.E/Σ.G inject rules.
    let state = preset::with_optimizer_rules(
        SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features(),
    )
    .build();
    let ctx = SessionContext::new_with_state(state);
    let decode_start = Instant::now();
    for table in tables {
        let parquet_path = parquet_path_for_table(&prefix_path, table)
            .map_err(|e| RunShardError::Config(format!("table {table:?}: {e}")))?;
        let mut provider =
            EmatixFastParquetTableProvider::try_new(parquet_path.to_string_lossy().to_string())
                .map_err(|e| RunShardError::Config(format!("open {table:?}: {e}")))?;
        provider = provider.with_dict_preservation(wu.execution.with_dict_preservation);
        provider = provider.with_late_mat(wu.execution.with_late_mat);
        ctx.register_table(table.as_str(), Arc::new(provider))
            .map_err(|e| RunShardError::Config(format!("register {table:?}: {e}")))?;
    }
    let decode_ms = decode_start.elapsed().as_millis() as u64;

    // ---- 4. Execute the query ----
    let exec_start = Instant::now();
    let df = ctx
        .sql(sql)
        .await
        .map_err(|e| RunShardError::Execution(format!("plan: {e}")))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| RunShardError::Execution(format!("collect: {e}")))?;
    let exec_ms = exec_start.elapsed().as_millis() as u64;
    let rows_out: usize = batches.iter().map(|b| b.num_rows()).sum();

    // ---- 5. Write Arrow IPC to output URI ----
    let output_uri = match &wu.output {
        Output::ArrowIpc { uri } => uri.clone(),
    };
    let output_path = uri_to_local_path(&output_uri)
        .map_err(|e| RunShardError::Output(format!("output.uri: {e}")))?;
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| RunShardError::Output(format!("mkdir {parent:?}: {e}")))?;
    }
    let write_start = Instant::now();
    {
        let file = std::fs::File::create(&output_path)
            .map_err(|e| RunShardError::Output(format!("create {output_path:?}: {e}")))?;
        // Empty-result corner: still write a valid Arrow IPC file
        // with the query's logical schema so the coordinator can
        // distinguish "ran but empty" from "didn't run". Use the
        // first batch's schema if any; otherwise build from the df
        // plan. df is consumed by collect() above — fall back to
        // empty schema only when there are truly no rows.
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        let mut writer = ArrowIpcWriter::try_new(file, &schema)
            .map_err(|e| RunShardError::Output(format!("ipc init: {e}")))?;
        for b in &batches {
            writer
                .write(b)
                .map_err(|e| RunShardError::Output(format!("ipc write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| RunShardError::Output(format!("ipc finish: {e}")))?;
    }
    let output_write_ms = write_start.elapsed().as_millis() as u64;

    let wall_ms = started.elapsed().as_millis() as u64;
    Ok(WorkUnitMetrics {
        work_unit_id: wu.id.clone(),
        query: match &wu.query {
            Query::Tpch { id } => id.clone(),
        },
        wall_ms,
        decode_ms: Some(decode_ms),
        exec_ms: Some(exec_ms),
        output_write_ms: Some(output_write_ms),
        rows_in: None, // populated when we wire ExecutionPlan metrics in α.3
        rows_out: Some(rows_out as u64),
        output_uri,
    })
}

/// Resolve a `file:///abs/path` or bare path to a local
/// [`PathBuf`]. `s3://` URIs are reserved for α.2 and produce a
/// config error here.
fn uri_to_local_path(uri: &str) -> Result<PathBuf, String> {
    if let Some(rest) = uri.strip_prefix("file://") {
        Ok(PathBuf::from(rest))
    } else if uri.starts_with("s3://") {
        Err(format!(
            "s3:// URIs not yet supported by `flow run-shard` (α.1); use file:// for now ({uri:?})"
        ))
    } else if uri.starts_with('/') {
        Ok(PathBuf::from(uri))
    } else {
        Err(format!(
            "unsupported URI scheme in {uri:?}; expected file:// or absolute path"
        ))
    }
}

/// Resolve the parquet file path for a given table name under
/// the input prefix. Today: `{prefix}/{table}.parquet`. Future:
/// honor partitioned layouts (`{prefix}/{table}/part-*.parquet`).
fn parquet_path_for_table(prefix: &Path, table: &str) -> Result<PathBuf, String> {
    let candidate = prefix.join(format!("{table}.parquet"));
    if candidate.is_file() {
        Ok(candidate)
    } else {
        Err(format!("{candidate:?} not found"))
    }
}

/// Embedded TPC-H query catalog. The `Q01..Q22` ids map to the
/// canonical SQL shipped under `examples/tpch/queries/`. Embedded
/// at compile time so the worker binary is self-contained — no
/// runtime FS lookup means the same binary works in Lambda /
/// K8s images without baking the queries directory.
fn tpch_sql(id: &str) -> Result<&'static str, String> {
    let normalized = id.to_uppercase();
    Ok(match normalized.as_str() {
        "Q01" => include_str!("../../../examples/tpch/queries/q01.sql"),
        "Q02" => include_str!("../../../examples/tpch/queries/q02.sql"),
        "Q03" => include_str!("../../../examples/tpch/queries/q03.sql"),
        "Q04" => include_str!("../../../examples/tpch/queries/q04.sql"),
        "Q05" => include_str!("../../../examples/tpch/queries/q05.sql"),
        "Q06" => include_str!("../../../examples/tpch/queries/q06.sql"),
        "Q07" => include_str!("../../../examples/tpch/queries/q07.sql"),
        "Q08" => include_str!("../../../examples/tpch/queries/q08.sql"),
        "Q09" => include_str!("../../../examples/tpch/queries/q09.sql"),
        "Q10" => include_str!("../../../examples/tpch/queries/q10.sql"),
        "Q11" => include_str!("../../../examples/tpch/queries/q11.sql"),
        "Q12" => include_str!("../../../examples/tpch/queries/q12.sql"),
        "Q13" => include_str!("../../../examples/tpch/queries/q13.sql"),
        "Q14" => include_str!("../../../examples/tpch/queries/q14.sql"),
        "Q15" => include_str!("../../../examples/tpch/queries/q15.sql"),
        "Q16" => include_str!("../../../examples/tpch/queries/q16.sql"),
        "Q17" => include_str!("../../../examples/tpch/queries/q17.sql"),
        "Q18" => include_str!("../../../examples/tpch/queries/q18.sql"),
        "Q19" => include_str!("../../../examples/tpch/queries/q19.sql"),
        "Q20" => include_str!("../../../examples/tpch/queries/q20.sql"),
        "Q21" => include_str!("../../../examples/tpch/queries/q21.sql"),
        "Q22" => include_str!("../../../examples/tpch/queries/q22.sql"),
        other => {
            return Err(format!(
                "unknown TPC-H query id {other:?}; expected Q01..Q22"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ematix_flow_distributed::work_unit::Execution;
    use std::collections::BTreeMap;

    #[test]
    fn uri_resolution_handles_three_shapes() {
        assert_eq!(
            uri_to_local_path("file:///tmp/data").unwrap(),
            PathBuf::from("/tmp/data")
        );
        assert_eq!(
            uri_to_local_path("/tmp/data").unwrap(),
            PathBuf::from("/tmp/data")
        );
        assert!(uri_to_local_path("s3://bucket/key").is_err());
        assert!(uri_to_local_path("foo://bar").is_err());
    }

    #[test]
    fn tpch_sql_covers_q01_through_q22() {
        for n in 1..=22u32 {
            let id = format!("Q{n:02}");
            let sql = tpch_sql(&id).expect(&id);
            // Every query mentions at least one TPC-H table or
            // the canonical SQL keyword set — sanity check the
            // include_str! actually picked up content.
            assert!(
                sql.contains("select") || sql.contains("SELECT"),
                "Q{n:02} SQL empty or unexpected"
            );
        }
        assert!(tpch_sql("Q99").is_err());
        assert!(tpch_sql("not-a-query").is_err());
    }

    #[test]
    fn tpch_sql_case_insensitive() {
        let upper = tpch_sql("Q14").unwrap();
        let lower = tpch_sql("q14").unwrap();
        assert_eq!(upper, lower);
    }

    /// End-to-end execute on a tiny in-tree fixture if SF=1 lineitem
    /// is present. Skipped otherwise — CI typically doesn't have the
    /// generated TPC-H data on disk. Mainly useful when iterating
    /// locally + before the next AWS campaign run.
    #[tokio::test]
    async fn execute_q06_against_sf1_if_present() {
        // The fixture path matches what `tpch_generate --sf 1` produces.
        // Skip when missing so this test is friendly in CI / fresh
        // checkouts.
        let sf1 =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1");
        if !sf1.join("lineitem.parquet").is_file() {
            eprintln!("skipping: SF=1 fixture not present at {sf1:?}");
            return;
        }
        let out_dir = tempfile::tempdir().unwrap();
        let out_uri = format!(
            "file://{}",
            out_dir.path().join("q06-result.arrow").display()
        );

        let wu = WorkUnit {
            schema: WorkUnit::default_schema(),
            id: "test-wu-q06".into(),
            query: Query::Tpch { id: "Q06".into() },
            input: Input::ParquetPartition {
                uri_prefix: format!("file://{}", sf1.display()),
                tables: vec!["lineitem".into()],
                row_group_range: BTreeMap::new(),
            },
            output: Output::ArrowIpc {
                uri: out_uri.clone(),
            },
            execution: Execution::default(),
        };

        let metrics = execute_work_unit(&wu).await.expect("Q06 execute");
        assert_eq!(metrics.work_unit_id, "test-wu-q06");
        assert_eq!(metrics.query, "Q06");
        assert!(metrics.wall_ms > 0);
        // Q06 produces a single scalar row.
        assert_eq!(metrics.rows_out, Some(1));
        // The output file should now exist + be non-empty.
        let out_path = out_dir.path().join("q06-result.arrow");
        let md = std::fs::metadata(&out_path).expect("output file");
        assert!(md.len() > 0, "Arrow IPC output should be non-empty");
    }
}
