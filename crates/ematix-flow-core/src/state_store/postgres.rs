//! Postgres-backed `StateStore`.
//!
//! Two tables under a configurable schema:
//!
//! - `{schema}.ematix_streaming_state` — one row per `(pipeline, key)`.
//! - `{schema}.ematix_streaming_offsets` — one row per `(pipeline, source_id)`.
//!
//! Both tables are written inside a single transaction in [`commit`]
//! so a partial failure is invisible to a subsequent [`load`]. See
//! `docs/PHASE_39_5_SESSIONS.md` § "Postgres schema" / "Cadence".
//!
//! State blobs and offset bytes are pre-encoded by the caller — the
//! store treats them as opaque `BYTEA`. Postcard / migrations live
//! one layer up.
//!
//! [`commit`]: StateStore::commit
//! [`load`]: StateStore::load

use std::collections::HashMap;

use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio_postgres::{Config as PgConfig, NoTls};

use crate::backend::BackendError;
use crate::pg::PgError;

use super::{CommitSnapshot, RecoveredState, StateStore};

#[derive(Debug)]
pub struct PostgresStateStore {
    pool: Pool,
    /// Postgres schema that owns both tables. Quoted as an identifier
    /// at SQL render time, not interpolated as a literal — avoids
    /// SQL-injection risk if a user supplies an exotic schema name.
    schema: String,
}

impl PostgresStateStore {
    /// Open a pool against `url`. Eagerly validates connectivity so a
    /// bad URL fails at construction time, not first use. Mirrors
    /// [`crate::pg::PgPool::connect`].
    pub async fn connect(url: &str, schema: &str) -> Result<Self, BackendError> {
        let pg_cfg: PgConfig = url.parse().map_err(|e: tokio_postgres::Error| {
            BackendError::Connection(format!("invalid postgres url: {e}"))
        })?;
        let mgr_cfg = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg_cfg, NoTls, mgr_cfg);
        let pool = Pool::builder(mgr)
            .max_size(8)
            .build()
            .map_err(|e| BackendError::Connection(format!("pool: {e}")))?;
        let client = pool
            .get()
            .await
            .map_err(|e| BackendError::Connection(format!("pool: {e}")))?;
        let _: i32 = client
            .query_one("SELECT 1", &[])
            .await
            .map_err(|e| BackendError::from(PgError::Postgres(e)))?
            .get(0);
        drop(client);

        Ok(Self {
            pool,
            schema: schema.to_string(),
        })
    }

    /// Create the two state-store tables if they don't exist.
    /// Idempotent. Caller invokes this once at pipeline start before
    /// the first `load`.
    pub async fn ensure_schema(&self) -> Result<(), BackendError> {
        let schema = quote_ident(&self.schema);
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {schema};
             CREATE TABLE IF NOT EXISTS {schema}.ematix_streaming_state (
                 pipeline_name   TEXT       NOT NULL,
                 group_key       BYTEA      NOT NULL,
                 state_blob      BYTEA      NOT NULL,
                 state_version   INTEGER    NOT NULL,
                 updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                 PRIMARY KEY (pipeline_name, group_key)
             );
             CREATE TABLE IF NOT EXISTS {schema}.ematix_streaming_offsets (
                 pipeline_name   TEXT       NOT NULL,
                 source_id       TEXT       NOT NULL,
                 offset_bytes    BYTEA      NOT NULL,
                 state_version   INTEGER    NOT NULL,
                 updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                 PRIMARY KEY (pipeline_name, source_id)
             );"
        );
        let client = self.client().await?;
        client.batch_execute(&sql).await.map_err(pg_err)?;
        Ok(())
    }

    /// Test-only escape hatch for atomicity tests. Runs an arbitrary
    /// SQL string outside any transaction. Not part of the trait.
    #[doc(hidden)]
    pub async fn raw_execute_for_test(&self, sql: &str) -> Result<u64, BackendError> {
        let client = self.client().await?;
        client.execute(sql, &[]).await.map_err(pg_err)
    }

    async fn client(&self) -> Result<deadpool_postgres::Object, BackendError> {
        self.pool
            .get()
            .await
            .map_err(|e| BackendError::Connection(format!("pool: {e}")))
    }
}

#[async_trait]
impl StateStore for PostgresStateStore {
    async fn load(&self, pipeline: &str) -> Result<RecoveredState, BackendError> {
        let schema = quote_ident(&self.schema);
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        // group_key is the primary key, so the pipeline filter is
        // sargable and there's no point holding a long-running snapshot
        // for the load. We still wrap in a transaction so the two
        // queries see a consistent point in time vs. a concurrent
        // commit.
        let state_rows = tx
            .query(
                &format!(
                    "SELECT group_key, state_blob, state_version
                     FROM {schema}.ematix_streaming_state
                     WHERE pipeline_name = $1"
                ),
                &[&pipeline],
            )
            .await
            .map_err(pg_err)?;

        let offset_rows = tx
            .query(
                &format!(
                    "SELECT source_id, offset_bytes
                     FROM {schema}.ematix_streaming_offsets
                     WHERE pipeline_name = $1"
                ),
                &[&pipeline],
            )
            .await
            .map_err(pg_err)?;

        tx.commit().await.map_err(pg_err)?;

        let mut state_by_key: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(state_rows.len());
        let mut state_version: u32 = 0;
        for row in state_rows {
            let key: Vec<u8> = row.get(0);
            let blob: Vec<u8> = row.get(1);
            let v: i32 = row.get(2);
            state_by_key.insert(key, blob);
            // All rows for a single pipeline share the same
            // state_version (commit writes them in lockstep). Track
            // the max as a defensive read in case some other writer
            // pushes a later version mid-flight.
            state_version = state_version.max(v as u32);
        }

        let mut offsets: HashMap<String, Vec<u8>> = HashMap::with_capacity(offset_rows.len());
        for row in offset_rows {
            let source_id: String = row.get(0);
            let bytes: Vec<u8> = row.get(1);
            offsets.insert(source_id, bytes);
        }

        Ok(RecoveredState {
            state_by_key,
            offsets,
            state_version,
        })
    }

    async fn commit(&self, pipeline: &str, snapshot: CommitSnapshot) -> Result<(), BackendError> {
        let schema = quote_ident(&self.schema);
        let mut client = self.client().await?;
        let tx = client.transaction().await.map_err(pg_err)?;

        let version_i32: i32 = snapshot.state_version as i32;

        // Upserts. ON CONFLICT collapses duplicate keys to the last
        // value in the snapshot, which is the same semantics
        // `InMemoryStateStore` gets from `HashMap::insert`.
        if !snapshot.state_upserts.is_empty() {
            let upsert_sql = format!(
                "INSERT INTO {schema}.ematix_streaming_state
                     (pipeline_name, group_key, state_blob, state_version, updated_at)
                 VALUES ($1, $2, $3, $4, now())
                 ON CONFLICT (pipeline_name, group_key) DO UPDATE
                     SET state_blob = EXCLUDED.state_blob,
                         state_version = EXCLUDED.state_version,
                         updated_at = now()"
            );
            let stmt = tx.prepare(&upsert_sql).await.map_err(pg_err)?;
            for (key, blob) in &snapshot.state_upserts {
                tx.execute(
                    &stmt,
                    &[&pipeline, &key.as_slice(), &blob.as_slice(), &version_i32],
                )
                .await
                .map_err(pg_err)?;
            }
        }

        // Deletes. Unknown-key deletes are silent — see contract test
        // `delete_of_unknown_key_is_ok`.
        if !snapshot.state_deletes.is_empty() {
            let delete_sql = format!(
                "DELETE FROM {schema}.ematix_streaming_state
                 WHERE pipeline_name = $1 AND group_key = $2"
            );
            let stmt = tx.prepare(&delete_sql).await.map_err(pg_err)?;
            for key in &snapshot.state_deletes {
                tx.execute(&stmt, &[&pipeline, &key.as_slice()])
                    .await
                    .map_err(pg_err)?;
            }
        }

        // Offsets. Per-source merge — absent ids retain prior values,
        // so a per-source ticker can commit just its own offset.
        if !snapshot.offsets.is_empty() {
            let offset_sql = format!(
                "INSERT INTO {schema}.ematix_streaming_offsets
                     (pipeline_name, source_id, offset_bytes, state_version, updated_at)
                 VALUES ($1, $2, $3, $4, now())
                 ON CONFLICT (pipeline_name, source_id) DO UPDATE
                     SET offset_bytes = EXCLUDED.offset_bytes,
                         state_version = EXCLUDED.state_version,
                         updated_at = now()"
            );
            let stmt = tx.prepare(&offset_sql).await.map_err(pg_err)?;
            for (source_id, bytes) in &snapshot.offsets {
                tx.execute(
                    &stmt,
                    &[
                        &pipeline,
                        &source_id.as_str(),
                        &bytes.as_slice(),
                        &version_i32,
                    ],
                )
                .await
                .map_err(pg_err)?;
            }
        }

        tx.commit().await.map_err(pg_err)?;
        Ok(())
    }
}

fn pg_err(e: tokio_postgres::Error) -> BackendError {
    BackendError::from(PgError::Postgres(e))
}

/// Render a Postgres identifier safely. Doubles embedded quotes and
/// wraps in `"`s. Schema names come from user config; this avoids
/// SQL injection if someone supplies `public"; DROP TABLE foo; --`.
fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::quote_ident;

    #[test]
    fn quote_ident_doubles_quotes() {
        assert_eq!(quote_ident("public"), "\"public\"");
        assert_eq!(quote_ident("weird\"name"), "\"weird\"\"name\"");
    }
}
