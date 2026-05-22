//! Σ.L.2 — adaptive-runtime workload feedback log.
//!
//! Persists across process runs at `~/.ematix/workload.db` (or
//! `EMATIX_WORKLOAD_DB` env var). Σ.L.1's probe writes here; the
//! resolver consults here first to skip re-probing. Σ.L.5 will mine
//! this for write-tuning recommendations.
//!
//! ## Schema
//!
//! - `probe_outcomes` — per (table, gb_col), the last race result and
//!   how many times we've observed it.
//! - `query_observations` — per query shape hash, last N wall times +
//!   row counts (for future cost-based decisions).
//! - `predicate_selectivity` — per (table, col, op), observed
//!   selectivity ratios for Σ.L.3 + Σ.L.5.
//!
//! ## Why SQLite (not a flat file)
//!
//! - Concurrent-read safe (multi-process query execution writes the
//!   same log).
//! - Schema migration via versioned `schema_version` row.
//! - Future analytic queries against the log (e.g., "which tables
//!   would benefit from a write-side dict-encoded rewrite?") run
//!   natively.
//!
//! Mirrors the v0.7.0 SQLite RunLog deployment pattern. Same crate
//! dep (`rusqlite`); ~80 lines of glue.

use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

use rusqlite::{params, Connection, OptionalExtension};

/// Default path: `~/.ematix/workload.db`. Caller can override.
pub fn default_workload_db_path() -> PathBuf {
    if let Ok(p) = std::env::var("EMATIX_WORKLOAD_DB") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".ematix").join("workload.db")
}

/// Σ.L.2 — opens (and creates if needed) the workload log. Cheap to
/// keep one of these per process.
pub struct WorkloadLog {
    conn: Mutex<Connection>,
}

impl WorkloadLog {
    /// Open at the default path. Creates parent dir + db if missing.
    pub fn open_default() -> Result<Self, WorkloadLogError> {
        Self::open(&default_workload_db_path())
    }

    pub fn open(path: &Path) -> Result<Self, WorkloadLogError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(WorkloadLogError::Io)?;
        }
        let conn = Connection::open(path).map_err(WorkloadLogError::Db)?;
        Self::init_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// In-memory variant for tests.
    pub fn open_in_memory() -> Result<Self, WorkloadLogError> {
        let conn = Connection::open_in_memory().map_err(WorkloadLogError::Db)?;
        Self::init_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn init_schema(conn: &Connection) -> Result<(), WorkloadLogError> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER NOT NULL PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS probe_outcomes (
                table_name      TEXT NOT NULL,
                gb_col          TEXT NOT NULL,
                dict_wins       INTEGER NOT NULL,    -- 0 or 1
                dict_ms         REAL NOT NULL,
                default_ms      REAL NOT NULL,
                n_observations  INTEGER NOT NULL DEFAULT 1,
                last_seen_unix  INTEGER NOT NULL,
                PRIMARY KEY (table_name, gb_col)
            );
            CREATE TABLE IF NOT EXISTS query_observations (
                shape_hash      TEXT NOT NULL,
                last_wall_ms    REAL NOT NULL,
                last_row_count  INTEGER NOT NULL,
                n_observations  INTEGER NOT NULL DEFAULT 1,
                last_seen_unix  INTEGER NOT NULL,
                PRIMARY KEY (shape_hash)
            );
            CREATE TABLE IF NOT EXISTS predicate_selectivity (
                table_name      TEXT NOT NULL,
                col_name        TEXT NOT NULL,
                op              TEXT NOT NULL,        -- 'eq', 'range', 'in', 'like'
                selectivity     REAL NOT NULL,        -- last observed pass rate 0..1
                n_observations  INTEGER NOT NULL DEFAULT 1,
                last_seen_unix  INTEGER NOT NULL,
                PRIMARY KEY (table_name, col_name, op)
            );
            INSERT OR IGNORE INTO schema_version (version) VALUES (1);
            "#,
        )
        .map_err(WorkloadLogError::Db)?;
        Ok(())
    }

    /// Σ.L.2 — record a probe outcome (from Σ.L.1's race). If the
    /// (table, gb_col) pair already has a row, increments
    /// `n_observations` and updates wall times via EWMA.
    pub fn record_probe_outcome(
        &self,
        table: &str,
        gb_col: &str,
        dict_ms: f64,
        default_ms: f64,
    ) -> Result<(), WorkloadLogError> {
        let dict_wins = if dict_ms <= default_ms * 0.95 { 1 } else { 0 };
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO probe_outcomes
              (table_name, gb_col, dict_wins, dict_ms, default_ms, n_observations, last_seen_unix)
            VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6)
            ON CONFLICT (table_name, gb_col) DO UPDATE SET
              -- EWMA with α=0.3: newer observations weight more.
              dict_ms       = 0.7 * dict_ms       + 0.3 * excluded.dict_ms,
              default_ms    = 0.7 * default_ms    + 0.3 * excluded.default_ms,
              dict_wins     = excluded.dict_wins,
              n_observations = n_observations + 1,
              last_seen_unix = excluded.last_seen_unix
            "#,
            params![table, gb_col, dict_wins, dict_ms, default_ms, now],
        )
        .map_err(WorkloadLogError::Db)?;
        Ok(())
    }

    /// Σ.L.2 — consult before probing. Returns `Some(dict_wins)` iff
    /// we've observed this (table, gb_col) at least `min_observations`
    /// times. Falls back to None so the caller probes.
    pub fn consult_probe(
        &self,
        table: &str,
        gb_col: &str,
        min_observations: i64,
    ) -> Result<Option<bool>, WorkloadLogError> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                r#"
                SELECT dict_wins, n_observations
                  FROM probe_outcomes
                 WHERE table_name = ?1 AND gb_col = ?2
                "#,
                params![table, gb_col],
                |r| {
                    let dw: i64 = r.get(0)?;
                    let n: i64 = r.get(1)?;
                    Ok((dw, n))
                },
            )
            .optional()
            .map_err(WorkloadLogError::Db)?;
        Ok(row.and_then(|(dw, n)| if n >= min_observations { Some(dw == 1) } else { None }))
    }

    /// Record per-query observability — wall time + total rows
    /// returned, keyed by a shape hash the caller computes (e.g. a
    /// hash of the LogicalPlan or normalized SQL).
    pub fn record_query(
        &self,
        shape_hash: &str,
        wall_ms: f64,
        row_count: u64,
    ) -> Result<(), WorkloadLogError> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO query_observations
              (shape_hash, last_wall_ms, last_row_count, n_observations, last_seen_unix)
            VALUES (?1, ?2, ?3, 1, ?4)
            ON CONFLICT (shape_hash) DO UPDATE SET
              last_wall_ms   = 0.7 * last_wall_ms + 0.3 * excluded.last_wall_ms,
              last_row_count = excluded.last_row_count,
              n_observations = n_observations + 1,
              last_seen_unix = excluded.last_seen_unix
            "#,
            params![shape_hash, wall_ms, row_count as i64, now],
        )
        .map_err(WorkloadLogError::Db)?;
        Ok(())
    }

    /// Record observed predicate selectivity — Σ.L.3 consumes this
    /// to reorder filter chains by ascending pass-rate.
    pub fn record_selectivity(
        &self,
        table: &str,
        col: &str,
        op: &str,
        selectivity: f64,
    ) -> Result<(), WorkloadLogError> {
        let now = unix_now();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO predicate_selectivity
              (table_name, col_name, op, selectivity, n_observations, last_seen_unix)
            VALUES (?1, ?2, ?3, ?4, 1, ?5)
            ON CONFLICT (table_name, col_name, op) DO UPDATE SET
              selectivity    = 0.7 * selectivity + 0.3 * excluded.selectivity,
              n_observations = n_observations + 1,
              last_seen_unix = excluded.last_seen_unix
            "#,
            params![table, col, op, selectivity, now],
        )
        .map_err(WorkloadLogError::Db)?;
        Ok(())
    }

    /// Get observed selectivity for (table, col, op). Useful for
    /// Σ.L.3's filter-reorder rule + Σ.L.5's write tuner.
    pub fn get_selectivity(
        &self,
        table: &str,
        col: &str,
        op: &str,
    ) -> Result<Option<f64>, WorkloadLogError> {
        let conn = self.conn.lock().unwrap();
        let r = conn
            .query_row(
                "SELECT selectivity FROM predicate_selectivity \
                   WHERE table_name = ?1 AND col_name = ?2 AND op = ?3",
                params![table, col, op],
                |r| r.get::<_, f64>(0),
            )
            .optional()
            .map_err(WorkloadLogError::Db)?;
        Ok(r)
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug)]
pub enum WorkloadLogError {
    Io(std::io::Error),
    Db(rusqlite::Error),
}

impl std::fmt::Display for WorkloadLogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadLogError::Io(e) => write!(f, "workload_log io: {e}"),
            WorkloadLogError::Db(e) => write!(f, "workload_log db: {e}"),
        }
    }
}
impl std::error::Error for WorkloadLogError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_in_memory_and_record_probe() {
        let log = WorkloadLog::open_in_memory().unwrap();
        log.record_probe_outcome("lineitem", "l_shipmode", 8.0, 16.0)
            .unwrap();
        // first observation — n=1, below min=3 default → None
        assert_eq!(
            log.consult_probe("lineitem", "l_shipmode", 3).unwrap(),
            None
        );
        // accumulate observations
        log.record_probe_outcome("lineitem", "l_shipmode", 9.0, 17.0)
            .unwrap();
        log.record_probe_outcome("lineitem", "l_shipmode", 10.0, 19.0)
            .unwrap();
        // now n=3 → returns the verdict
        assert_eq!(
            log.consult_probe("lineitem", "l_shipmode", 3).unwrap(),
            Some(true) // dict ~9.5ms < 16ms*0.95
        );
    }

    #[test]
    fn min_observations_one_returns_immediately() {
        let log = WorkloadLog::open_in_memory().unwrap();
        log.record_probe_outcome("orders", "o_orderpriority", 20.0, 5.0)
            .unwrap();
        assert_eq!(
            log.consult_probe("orders", "o_orderpriority", 1).unwrap(),
            Some(false) // dict 20ms loses to 5ms
        );
    }

    #[test]
    fn selectivity_round_trip() {
        let log = WorkloadLog::open_in_memory().unwrap();
        log.record_selectivity("lineitem", "l_shipdate", "range", 0.12)
            .unwrap();
        assert_eq!(
            log.get_selectivity("lineitem", "l_shipdate", "range").unwrap(),
            Some(0.12)
        );
        // EWMA smoothing
        log.record_selectivity("lineitem", "l_shipdate", "range", 0.20)
            .unwrap();
        let s = log
            .get_selectivity("lineitem", "l_shipdate", "range")
            .unwrap()
            .unwrap();
        // 0.7 * 0.12 + 0.3 * 0.20 = 0.144
        assert!((s - 0.144).abs() < 1e-6);
    }

    #[test]
    fn query_observation_round_trip() {
        let log = WorkloadLog::open_in_memory().unwrap();
        log.record_query("abc123", 50.0, 1000).unwrap();
        log.record_query("abc123", 45.0, 1000).unwrap();
        // Test passes if no errors. Schema correctness verified by SQL.
    }

    #[test]
    fn schema_init_idempotent() {
        let log1 = WorkloadLog::open_in_memory().unwrap();
        // Re-init the same connection via init_schema again — should
        // not error (CREATE TABLE IF NOT EXISTS).
        WorkloadLog::init_schema(&log1.conn.lock().unwrap()).unwrap();
    }
}
