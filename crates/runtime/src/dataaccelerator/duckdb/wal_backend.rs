/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! WAL backend implementation for `DuckDB`.
//!
//! `DuckDB` uses optimistic concurrency: conflicting transactions are aborted at
//! commit time. All three write operations (`atomic_insert`, `atomic_delete`,
//! `atomic_update`) retry the full SELECT → WAL-insert → data-change transaction
//! from scratch on conflict, as required by the spec.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use datafusion::common::{Constraint, Constraints};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::Expr;
use datafusion_table_providers::duckdb::DuckDB;
use datafusion_table_providers::duckdb::write::write_batches_to_table;
use datafusion_table_providers::duckdb::{RelationName, TableDefinition, TableManager};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;

use crate::accelerated_table::write::wal::{
    WalBackend, WalEntry, WalOp, arrow_ipc_to_batches, batches_to_arrow_ipc, extract_pks_ipc,
    sanitize_name,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// WAL backend for `DuckDB`.
///
/// The WAL tables and checkpoint are stored in the same `DuckDB` instance as the
/// accelerator data, so WAL writes and data writes are covered by the same
/// transaction. On conflict (`DuckDB` optimistic concurrency), the full
/// transaction is retried from scratch.
pub(crate) struct DuckDBWalBackend {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    on_conflict: Option<OnConflict>,
    table_name: String,
    primary_keys: Vec<String>,
    schema: SchemaRef,
}

impl DuckDBWalBackend {
    /// Build a `DuckDBWalBackend` from a pool and its table provider.
    ///
    /// The pool is supplied by the caller (`DuckDBAccelerator::wal_backend`) via
    /// `get_shared_pool`, which works for both file-mode and memory-mode DuckDB.
    /// Schema and constraints are read directly from the `accelerator` table provider
    /// without downcasting to engine-internal types.
    /// Returns `None` when WAL is not applicable (no primary keys).
    pub(crate) async fn try_new(
        pool: Arc<DuckDbConnectionPool>,
        source: &dyn crate::dataaccelerator::AccelerationSource,
        accelerator: &Arc<dyn TableProvider>,
    ) -> Option<Self> {
        let schema = accelerator.schema();
        let constraints: &Constraints = accelerator.constraints()?;

        let primary_keys: Vec<String> = constraints
            .iter()
            .filter_map(|c| {
                if let Constraint::PrimaryKey(idxs) = c {
                    Some(idxs.iter().map(|&i| schema.field(i).name().clone()))
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        if primary_keys.is_empty() {
            return None;
        }

        let table_name = source.name().table().to_string();
        let on_conflict = Some(OnConflict::Upsert(ColumnReference::new(
            primary_keys.clone(),
        )));
        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new(&table_name), Arc::clone(&schema))
                .with_constraints(constraints.clone()),
        );

        Some(Self {
            pool,
            table_definition,
            on_conflict,
            table_name,
            primary_keys,
            schema,
        })
    }

    fn wal_table(&self) -> String {
        format!("__spice_wal_{}", sanitize_name(&self.table_name))
    }

    fn wal_cp_table(&self) -> String {
        format!("__spice_wal_cp_{}", sanitize_name(&self.table_name))
    }

    fn wal_seq(&self) -> String {
        format!("__spice_wal_seq_{}", sanitize_name(&self.table_name))
    }

    fn wal_txn_seq(&self) -> String {
        format!("__spice_wal_txn_seq_{}", sanitize_name(&self.table_name))
    }
}

fn is_duckdb_conflict(e: &dyn std::error::Error) -> bool {
    let msg = e.to_string();
    msg.contains("Conflict on") || msg.contains("TransactionContext Error")
}

fn conflict_backoff(attempt: u32) -> Duration {
    Duration::from_millis(10u64.saturating_mul(1u64 << attempt.min(9)))
}

fn filters_to_sql_string(filters: &[Expr]) -> DataFusionResult<Option<String>> {
    if filters.is_empty() {
        return Ok(None);
    }
    datafusion_table_providers::util::dml::filters_to_sql(filters, None)
        .map(Some)
        .map_err(|e| DataFusionError::Execution(format!("WAL: failed to build filter SQL: {e}")))
}

/// Return the names of all base tables that hold data for `table_definition`.
///
/// When DuckDB table providers use partition tables the main table name becomes a VIEW;
/// DELETE/UPDATE must target the underlying base tables directly.  If no internal
/// partition tables exist the main table itself is a base table.
fn writable_base_tables(
    table_definition: &Arc<TableDefinition>,
    tx: &duckdb::Transaction<'_>,
    fallback: &str,
) -> Result<Vec<String>, BoxError> {
    let internal = table_definition
        .list_internal_tables(tx)
        .map_err(|e| Box::new(e) as BoxError)?;
    if internal.is_empty() {
        Ok(vec![fallback.to_string()])
    } else {
        Ok(internal
            .into_iter()
            .map(|(name, _)| name.to_string())
            .collect())
    }
}

fn resolve_append_table(
    table_definition: &Arc<TableDefinition>,
    tx: &duckdb::Transaction<'_>,
) -> Result<TableManager, BoxError> {
    let base = TableManager::new(Arc::clone(table_definition))
        .with_internal(false)
        .map_err(|e| Box::new(e) as BoxError)?;
    let internal = table_definition
        .list_internal_tables(tx)
        .map_err(|e| Box::new(e) as BoxError)?;
    Ok(if let Some((latest, _)) = internal.last() {
        TableManager::from_table_name(Arc::clone(table_definition), latest.clone())
    } else {
        base
    })
}

/// SELECT affected PKs before a DELETE or UPDATE; returns Arrow IPC bytes, or `None` if no rows.
fn resolve_pks_before_change_arrow(
    tx: &duckdb::Transaction<'_>,
    table_name: &str,
    primary_keys: &[String],
    filter_sql: Option<&str>,
) -> Result<Option<Vec<u8>>, BoxError> {
    let pk_select = primary_keys
        .iter()
        .map(|k| format!(r#""{k}""#))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = if let Some(wh) = filter_sql {
        format!(r#"SELECT {pk_select} FROM "{table_name}" WHERE {wh}"#)
    } else {
        format!(r#"SELECT {pk_select} FROM "{table_name}""#)
    };

    let mut stmt = tx.prepare(&sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(None);
    }

    Ok(Some(batches_to_arrow_ipc(&batches)?))
}

/// After an UPDATE, read the new row state for the affected PKs and return Arrow IPC bytes.
fn read_rows_by_pks_arrow(
    tx: &duckdb::Transaction<'_>,
    table_name: &str,
    primary_keys: &[String],
    pks_ipc: &[u8],
) -> Result<Vec<u8>, BoxError> {
    if pks_ipc.is_empty() {
        return Ok(Vec::new());
    }

    let where_clause = build_where_clause_from_ipc(primary_keys, pks_ipc)?;
    let sql = format!(r#"SELECT * FROM "{table_name}" WHERE {where_clause}"#);
    let mut stmt = tx.prepare(&sql)?;
    let batches: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return Ok(Vec::new());
    }

    batches_to_arrow_ipc(&batches)
}

/// Build a SQL WHERE clause from Arrow IPC PK bytes for use in `SELECT * ... WHERE <clause>`.
fn build_where_clause_from_ipc(primary_keys: &[String], ipc: &[u8]) -> Result<String, BoxError> {
    let batches = arrow_ipc_to_batches(ipc)?;
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;

    if primary_keys.len() == 1 {
        let pk_col = &primary_keys[0];
        let col_idx = batch.schema().index_of(pk_col)?;
        let arr = batch.column(col_idx);
        let values = (0..batch.num_rows())
            .map(|row| arrow_value_to_sql_literal(arr.as_ref(), row))
            .collect::<Vec<_>>()
            .join(", ");
        return Ok(format!(r#""{pk_col}" IN ({values})"#));
    }

    let clauses = (0..batch.num_rows())
        .map(|row| {
            let parts = primary_keys
                .iter()
                .map(|pk| {
                    let col_idx = batch.schema().index_of(pk).unwrap_or(0);
                    let v = arrow_value_to_sql_literal(batch.column(col_idx).as_ref(), row);
                    format!(r#""{pk}" = {v}"#)
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({parts})")
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    Ok(clauses)
}

/// Convert a single Arrow array element to a SQL literal for use in WHERE clauses.
fn arrow_value_to_sql_literal(arr: &dyn arrow::array::Array, row: usize) -> String {
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        LargeStringArray, StringArray, StringViewArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };

    if arr.is_null(row) {
        return "NULL".to_string();
    }

    macro_rules! int_lit {
        ($ty:ty) => {
            if let Some(a) = arr.as_any().downcast_ref::<$ty>() {
                return a.value(row).to_string();
            }
        };
    }
    int_lit!(Int8Array);
    int_lit!(Int16Array);
    int_lit!(Int32Array);
    int_lit!(Int64Array);
    int_lit!(UInt8Array);
    int_lit!(UInt16Array);
    int_lit!(UInt32Array);
    int_lit!(UInt64Array);
    int_lit!(Float32Array);
    int_lit!(Float64Array);

    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return if a.value(row) { "TRUE" } else { "FALSE" }.to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        let s = a.value(row).replace('\'', "''");
        return format!("'{s}'");
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        let s = a.value(row).replace('\'', "''");
        return format!("'{s}'");
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringViewArray>() {
        let s = a.value(row).replace('\'', "''");
        return format!("'{s}'");
    }

    // Fallback for date/timestamp and other types: use arrow display formatter.
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    let opts = FormatOptions::default();
    ArrayFormatter::try_new(arr, &opts)
        .ok()
        .and_then(|fmt| fmt.value(row).try_to_string().ok())
        .map_or("NULL".to_string(), |s| {
            format!("'{}'", s.replace('\'', "''"))
        })
}

#[async_trait]
impl WalBackend for DuckDBWalBackend {
    fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    async fn initialize(&self) -> Result<(), BoxError> {
        let seq = self.wal_seq();
        let txn_seq = self.wal_txn_seq();
        let wal = self.wal_table();
        let cp = self.wal_cp_table();
        let pool = Arc::clone(&self.pool);

        tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
            let mut conn = pool.connect_sync()?;
            let duckdb_conn =
                DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
            let tx = duckdb_conn.conn.transaction()?;

            tx.execute_batch(&format!(
                r#"
                CREATE SEQUENCE IF NOT EXISTS "{seq}";
                CREATE SEQUENCE IF NOT EXISTS "{txn_seq}";
                CREATE TABLE IF NOT EXISTS "{wal}" (
                    seq        BIGINT PRIMARY KEY DEFAULT nextval('"{seq}"'),
                    txn_id     BIGINT NOT NULL DEFAULT -1,
                    op         VARCHAR NOT NULL,
                    pks        BLOB NOT NULL,
                    new_values BLOB,
                    written_at TIMESTAMPTZ DEFAULT now()
                );
                ALTER TABLE "{wal}" ADD COLUMN IF NOT EXISTS txn_id BIGINT DEFAULT -1;
                CREATE TABLE IF NOT EXISTS "{cp}" (
                    last_delivered_seq BIGINT NOT NULL
                );
                "#
            ))?;

            tx.execute(
                &format!(r#"INSERT INTO "{cp}" SELECT -1 WHERE (SELECT COUNT(*) FROM "{cp}") = 0"#),
                [],
            )?;

            tx.commit()?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    async fn pending_count(&self) -> Result<i64, BoxError> {
        let cp_table = self.wal_cp_table();
        let wal_table = self.wal_table();
        let pool = Arc::clone(&self.pool);

        tokio::task::spawn_blocking(move || -> Result<i64, BoxError> {
            let mut conn = Arc::clone(&pool).connect_sync()?;
            let duckdb_conn =
                DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
            let last_seq: i64 = {
                let mut stmt = duckdb_conn
                    .conn
                    .prepare(&format!(r#"SELECT last_delivered_seq FROM "{cp_table}""#))?;
                let mut rows = stmt.query([])?;
                rows.next()?
                    .map(|r| r.get::<usize, i64>(0))
                    .transpose()?
                    .unwrap_or(-1)
            };
            let mut stmt = duckdb_conn.conn.prepare(&format!(
                r#"SELECT COUNT(*) FROM "{wal_table}" WHERE seq > {last_seq}"#
            ))?;
            let mut rows = stmt.query([])?;
            Ok(rows
                .next()?
                .map(|r| r.get::<usize, i64>(0))
                .transpose()?
                .unwrap_or(0))
        })
        .await?
    }

    async fn atomic_insert(&self, batches: Vec<RecordBatch>) -> Result<(), BoxError> {
        let wal_table = self.wal_table();
        let txn_seq = self.wal_txn_seq();
        let table_definition = Arc::clone(&self.table_definition);
        let on_conflict = self.on_conflict.clone();
        let schema = Arc::clone(&self.schema);
        let primary_keys = self.primary_keys.clone();
        let pool = Arc::clone(&self.pool);

        let mut attempt: u32 = 0;
        loop {
            let wal_table = wal_table.clone();
            let txn_seq = txn_seq.clone();
            let table_definition = Arc::clone(&table_definition);
            let on_conflict = on_conflict.clone();
            let schema = Arc::clone(&schema);
            let primary_keys = primary_keys.clone();
            let batches = batches.clone();
            let pool = Arc::clone(&pool);

            let res = tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
                let mut conn = pool.connect_sync()?;
                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
                let tx = duckdb_conn.conn.transaction()?;

                // Allocate one txn_id for this entire DML operation.
                let txn_id: i64 = {
                    let mut stmt =
                        tx.prepare(&format!(r#"SELECT nextval('"{txn_seq}"')"#))?;
                    let mut rows = stmt.query([])?;
                    rows.next()?
                        .map(|r| r.get::<usize, i64>(0))
                        .transpose()?
                        .unwrap_or(0)
                };

                // Write each batch as a separate WAL row, all sharing the same txn_id.
                for batch in &batches {
                    let pks_ipc = extract_pks_ipc(&[batch.clone()], &primary_keys)?;
                    let ipc_bytes = batches_to_arrow_ipc(&[batch.clone()])?;
                    tx.execute(
                        &format!(
                            r#"INSERT INTO "{wal_table}" (txn_id, op, pks, new_values) VALUES (?, ?, ?, ?)"#
                        ),
                        duckdb::params![txn_id, WalOp::Insert.as_str(), pks_ipc, ipc_bytes],
                    )?;
                }

                let append_table = resolve_append_table(&table_definition, &tx)
                    .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
                write_batches_to_table(&append_table, &tx, schema, batches, on_conflict.as_ref())
                    .map_err(|e: datafusion::error::DataFusionError| {
                        Box::new(std::io::Error::other(e.to_string())) as BoxError
                    })?;

                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(|e| -> BoxError {
                Box::new(std::io::Error::other(format!("WAL insert panicked: {e}")))
            })?;

            match res {
                Ok(()) => return Ok(()),
                Err(e) if is_duckdb_conflict(&*e) => {
                    tracing::debug!(table = %self.table_name, attempt, "WAL insert: conflict, retrying");
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn atomic_delete(&self, filters: &[Expr]) -> Result<u64, BoxError> {
        let filter_sql = filters_to_sql_string(filters)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
        let wal_table = self.wal_table();
        let txn_seq = self.wal_txn_seq();
        let table_name = self.table_name.clone();
        let primary_keys = self.primary_keys.clone();
        let pool = Arc::clone(&self.pool);
        let table_definition = Arc::clone(&self.table_definition);

        let mut attempt: u32 = 0;
        loop {
            let wal_table = wal_table.clone();
            let txn_seq = txn_seq.clone();
            let table_name = table_name.clone();
            let primary_keys = primary_keys.clone();
            let filter_sql = filter_sql.clone();
            let pool = Arc::clone(&pool);
            let table_definition = Arc::clone(&table_definition);

            let res = tokio::task::spawn_blocking(move || -> Result<u64, BoxError> {
                let mut conn = pool.connect_sync()?;
                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
                let tx = duckdb_conn.conn.transaction()?;

                let Some(pks_ipc) = resolve_pks_before_change_arrow(
                    &tx,
                    &table_name,
                    &primary_keys,
                    filter_sql.as_deref(),
                )?
                else {
                    tx.rollback()?;
                    return Ok(0);
                };

                let txn_id: i64 = {
                    let mut stmt =
                        tx.prepare(&format!(r#"SELECT nextval('"{txn_seq}"')"#))?;
                    let mut rows = stmt.query([])?;
                    rows.next()?
                        .map(|r| r.get::<usize, i64>(0))
                        .transpose()?
                        .unwrap_or(0)
                };

                tx.execute(
                    &format!(
                        r#"INSERT INTO "{wal_table}" (txn_id, op, pks, new_values) VALUES (?, ?, ?, NULL)"#
                    ),
                    duckdb::params![txn_id, WalOp::Delete.as_str(), pks_ipc],
                )?;

                // DELETE must target base tables; the main table name may be a VIEW when
                // DuckDB uses internal partition tables after a Full refresh.
                let base_tables = writable_base_tables(&table_definition, &tx, &table_name)?;
                let mut count: usize = 0;
                for bt in &base_tables {
                    let sql = if let Some(ref wh) = filter_sql {
                        format!(r#"DELETE FROM "{bt}" WHERE {wh}"#)
                    } else {
                        format!(r#"DELETE FROM "{bt}""#)
                    };
                    count += tx.execute(&sql, [])?;
                }

                tx.commit()?;
                Ok(count as u64)
            })
            .await
            .map_err(|e| -> BoxError {
                Box::new(std::io::Error::other(format!("WAL delete panicked: {e}")))
            })?;

            match res {
                Ok(count) => return Ok(count),
                Err(e) if is_duckdb_conflict(&*e) => {
                    tracing::debug!(table = %self.table_name, attempt, "WAL delete: conflict, retrying");
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn atomic_update(
        &self,
        assignments: &[(String, Expr)],
        filters: &[Expr],
    ) -> Result<u64, BoxError> {
        let filter_sql = filters_to_sql_string(filters)
            .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
        let set_clause =
            datafusion_table_providers::util::dml::assignments_to_sql(assignments, None)
                .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as BoxError)?;
        let wal_table = self.wal_table();
        let txn_seq = self.wal_txn_seq();
        let table_name = self.table_name.clone();
        let primary_keys = self.primary_keys.clone();
        let pool = Arc::clone(&self.pool);
        let table_definition = Arc::clone(&self.table_definition);

        let mut attempt: u32 = 0;
        loop {
            let wal_table = wal_table.clone();
            let txn_seq = txn_seq.clone();
            let table_name = table_name.clone();
            let primary_keys = primary_keys.clone();
            let filter_sql = filter_sql.clone();
            let set_clause = set_clause.clone();
            let pool = Arc::clone(&pool);
            let table_definition = Arc::clone(&table_definition);

            let res = tokio::task::spawn_blocking(move || -> Result<u64, BoxError> {
                let mut conn = pool.connect_sync()?;
                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
                let tx = duckdb_conn.conn.transaction()?;

                // 1. Resolve affected PKs BEFORE the update
                let Some(pks_ipc) = resolve_pks_before_change_arrow(
                    &tx,
                    &table_name,
                    &primary_keys,
                    filter_sql.as_deref(),
                )?
                else {
                    tx.rollback()?;
                    return Ok(0);
                };

                // 2. Apply the UPDATE across all base tables (the main table name may be a VIEW
                //    when DuckDB uses internal partition tables after a Full refresh).
                let base_tables = writable_base_tables(&table_definition, &tx, &table_name)?;
                let mut count: usize = 0;
                for bt in &base_tables {
                    let sql = if let Some(ref wh) = filter_sql {
                        format!(r#"UPDATE "{bt}" SET {set_clause} WHERE {wh}"#)
                    } else {
                        format!(r#"UPDATE "{bt}" SET {set_clause}"#)
                    };
                    count += tx.execute(&sql, [])?;
                }

                // 3. Read new row state for WAL replay (from the view, which reflects all partitions)
                let new_values_ipc =
                    read_rows_by_pks_arrow(&tx, &table_name, &primary_keys, &pks_ipc)?;

                // 4. Allocate txn_id and write WAL entry with final row state
                let txn_id: i64 = {
                    let mut stmt =
                        tx.prepare(&format!(r#"SELECT nextval('"{txn_seq}"')"#))?;
                    let mut rows = stmt.query([])?;
                    rows.next()?
                        .map(|r| r.get::<usize, i64>(0))
                        .transpose()?
                        .unwrap_or(0)
                };

                tx.execute(
                    &format!(
                        r#"INSERT INTO "{wal_table}" (txn_id, op, pks, new_values) VALUES (?, ?, ?, ?)"#
                    ),
                    duckdb::params![txn_id, WalOp::Update.as_str(), pks_ipc, new_values_ipc],
                )?;

                tx.commit()?;
                Ok(count as u64)
            })
            .await
            .map_err(|e| -> BoxError {
                Box::new(std::io::Error::other(format!("WAL update panicked: {e}")))
            })?;

            match res {
                Ok(count) => return Ok(count),
                Err(e) if is_duckdb_conflict(&*e) => {
                    tracing::debug!(table = %self.table_name, attempt, "WAL update: conflict, retrying");
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn next_pending_group(&self) -> BoxStream<'static, Result<WalEntry, BoxError>> {
        let pool = Arc::clone(&self.pool);
        let wal_table = self.wal_table();
        let cp_table = self.wal_cp_table();

        let (sender, receiver) = mpsc::channel::<Result<WalEntry, BoxError>>(32);

        tokio::task::spawn_blocking(move || {
            let res: Result<(), BoxError> = (|| {
                let mut conn = pool.connect_sync()?;
                let duckdb_conn =
                    DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
                let tx = duckdb_conn.conn.transaction()?;

                let last_seq: i64 = {
                    let mut stmt =
                        tx.prepare(&format!(r#"SELECT last_delivered_seq FROM "{cp_table}""#))?;
                    let mut rows = stmt.query([])?;
                    rows.next()?
                        .map(|r| r.get::<usize, i64>(0))
                        .transpose()?
                        .unwrap_or(-1)
                };

                // Fetch rows belonging to the next undelivered txn_id group.
                // Chosen as the group with the smallest min(seq) among pending txn_ids.
                let sql = format!(
                    r#"SELECT seq, txn_id, op, pks, new_values
                       FROM "{wal_table}"
                       WHERE seq > {last_seq}
                         AND txn_id = (
                           SELECT txn_id FROM "{wal_table}"
                           WHERE seq > {last_seq}
                           GROUP BY txn_id
                           ORDER BY min(seq)
                           LIMIT 1
                         )
                       ORDER BY seq ASC"#
                );

                let mut stmt = tx.prepare(&sql)?;
                let mut rows = stmt.query([])?;

                while let Some(row) = rows.next()? {
                    let op_str: String = row.get(2)?;
                    let Some(op) = WalOp::from_str(&op_str) else {
                        tracing::warn!(
                            "Unknown WAL op '{}' at seq {}, skipping",
                            op_str,
                            row.get::<usize, i64>(0).unwrap_or(0)
                        );
                        continue;
                    };
                    let entry = WalEntry {
                        seq: row.get(0)?,
                        txn_id: row.get(1)?,
                        op,
                        pks_ipc: row.get(3)?,
                        new_values: row.get(4)?,
                    };
                    if sender.blocking_send(Ok(entry)).is_err() {
                        break; // receiver dropped
                    }
                }

                tx.rollback()?;
                Ok(())
            })();

            if let Err(e) = res {
                let _ = sender.blocking_send(Err(e));
            }
        });

        Box::pin(futures::stream::unfold(receiver, |mut rx| async move {
            rx.recv().await.map(|item| (item, rx))
        }))
    }

    async fn advance_checkpoint(&self, seq: i64) -> Result<(), BoxError> {
        let cp_table = self.wal_cp_table();
        let wal_table = self.wal_table();
        let pool = Arc::clone(&self.pool);

        tokio::task::spawn_blocking(move || -> Result<(), BoxError> {
            let mut conn = pool.connect_sync()?;
            let duckdb_conn =
                DuckDB::duckdb_conn(&mut conn).map_err(|e| Box::new(e) as BoxError)?;
            let tx = duckdb_conn.conn.transaction()?;
            tx.execute(
                &format!(r#"UPDATE "{cp_table}" SET last_delivered_seq = {seq}"#),
                [],
            )?;
            tx.execute(
                &format!(r#"DELETE FROM "{wal_table}" WHERE seq <= {seq}"#),
                [],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await?
    }
}
