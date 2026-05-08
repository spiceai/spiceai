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

//! A federated DuckDB table writer that bypasses the acceleration-oriented
//! write path in `datafusion-table-providers`, which was designed for acceleration tables and:
//! 1. Does not support multi-part table references, flattening them into a single quoted identifier (by design)
//! 2. Has acceleration-specific logic for managing internal tables, views, indexes, and other optimizations
//!
//! Write mechanism:
//! 1. Bridges async `RecordBatchStream` to sync via an mpsc channel
//! 2. Registers a temporary Arrow scan view (`register_arrow_scan_view`)
//! 3. Executes `INSERT INTO <table> SELECT * FROM <temp_view>` in a transaction
//! 4. Drops the temporary view and commits (or rolls back on error)

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, SchemaExt};
use datafusion::datasource::TableProvider;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion::sql::TableReference;
use datafusion_table_providers::duckdb::DuckDB;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use futures::StreamExt;
use snafu::prelude::*;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get DuckDB connection: {source}"))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to begin transaction: {source}"))]
    TransactionFailed { source: duckdb::Error },

    #[snafu(display("Failed to commit transaction: {source}"))]
    CommitFailed { source: duckdb::Error },

    #[snafu(display("Failed to register Arrow scan view: {source}"))]
    RegisterScanViewFailed { source: duckdb::Error },

    #[snafu(display("Failed to execute INSERT: {source}"))]
    InsertFailed { source: duckdb::Error },

    #[snafu(display("Failed to drop temporary view: {source}"))]
    DropViewFailed { source: duckdb::Error },

    #[snafu(display("Failed to downcast DuckDB connection"))]
    DowncastFailed,
}

/// A federated DuckDB table writer that properly handles multi-part table references.
///
/// Unlike the acceleration-oriented `DuckDBTableWriter` in `datafusion-table-providers`, this writer:
/// - Stores `TableReference` directly (no `RelationName` flattening)
/// - Generates SQL with `to_quoted_string()` for proper multi-part quoting
/// - Skips internal table management, schema validation, and index management
pub struct DuckDbFederatedTableWriter {
    read_provider: Arc<dyn TableProvider>,
    pool: Arc<DuckDbConnectionPool>,
    table_reference: TableReference,
    write_lock: Arc<Mutex<()>>,
}

impl Debug for DuckDbFederatedTableWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDbFederatedTableWriter")
            .field("table_reference", &self.table_reference)
            .finish()
    }
}

impl DuckDbFederatedTableWriter {
    /// Create a new `DuckDbFederatedTableWriter`.
    ///
    /// - `read_provider`: The underlying read-only `TableProvider` (for `scan` delegation).
    /// - `pool`: `DuckDB` connection pool with the `ducklake` catalog already ATTACHed.
    /// - `table_reference`: Fully-qualified table reference (e.g. `Full("catalog", "schema", "table")`).
    #[must_use]
    pub fn new(
        read_provider: Arc<dyn TableProvider>,
        pool: Arc<DuckDbConnectionPool>,
        table_reference: TableReference,
        write_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            read_provider,
            pool,
            table_reference,
            write_lock,
        }
    }
}

#[async_trait]
impl TableProvider for DuckDbFederatedTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        let sink = DuckDbFederatedDataSink {
            pool: Arc::clone(&self.pool),
            table_reference: self.table_reference.clone(),
            schema: self.schema(),
            write_lock: Arc::clone(&self.write_lock),
            insert_op: op,
        };

        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)) as _)
    }
}

struct DuckDbFederatedDataSink {
    pool: Arc<DuckDbConnectionPool>,
    table_reference: TableReference,
    schema: SchemaRef,
    write_lock: Arc<Mutex<()>>,
    insert_op: InsertOp,
}

impl Debug for DuckDbFederatedDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDbFederatedDataSink")
            .field("table_reference", &self.table_reference)
            .finish()
    }
}

impl DisplayAs for DuckDbFederatedDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDbFederatedDataSink")
    }
}

#[async_trait]
impl DataSink for DuckDbFederatedDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let _write_guard = self.write_lock.lock().await;

        let pool = Arc::clone(&self.pool);
        let table_ref_quoted = self.table_reference.to_quoted_string();
        let insert_op = self.insert_op;
        let schema = data.schema();

        // Channel to bridge async RecordBatch stream → sync DuckDB write
        let (batch_tx, batch_rx) = mpsc::channel::<RecordBatch>(100);

        // Signal that the async stream completed successfully and it is safe to commit.
        // Without this, the blocking side cannot distinguish a clean end-of-stream
        // from a dropped sender (e.g. stream error after partial data).
        let (commit_tx, commit_rx) = oneshot::channel::<()>();

        let write_handle: JoinHandle<datafusion::common::Result<u64>> = tokio::task::spawn_blocking(
            move || {
                let mut db_conn = pool
                    .connect_sync()
                    .map_err(|e| DataFusionError::External(e))?;

                let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let stream = FFI_ArrowArrayStream::new(Box::new(SyncRecordBatchReceiver::new(
                    batch_rx,
                    Arc::clone(&schema),
                )));

                let current_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .as_millis();

                // `register_arrow_scan_view` creates a view in the connection's
                // default catalog (does not support qualified names). The INSERT might
                // target different attached database (e.g. `ducklake`). DuckDB
                // does not allow writes to multiple databases in a single transaction:
                //
                //   "TransactionContext Error: Attempting to write to database
                //    'ducklake' in a transaction that has already modified database
                //    'memory' - a single transaction can only write to a single attached database."
                //
                // Approach:
                //  1. Register the view outside our explicit transaction (auto-commits to the default catalog).
                //  2. Open a transaction for the INSERT (writes only to the target).
                //  3. Commit or roll back.
                //  4. Drop the view (best-effort).
                let view_name = format!("__scan_{current_ts}");
                duckdb_conn
                    .conn
                    .register_arrow_scan_view(&view_name, &stream)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                // Transaction scoped in a block — see explanation above.
                let result = {
                    let tx = duckdb_conn
                        .conn
                        .transaction()
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let inner_result = (|| {
                        if insert_op == InsertOp::Overwrite {
                            let delete_sql = format!("DELETE FROM {table_ref_quoted}");
                            tracing::debug!("{delete_sql}");
                            tx.execute(&delete_sql, [])
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        }

                        let insert_sql = format!(
                            r#"INSERT INTO {table_ref} SELECT * FROM "{view_name}""#,
                            table_ref = table_ref_quoted,
                        );
                        tracing::debug!("{insert_sql}");

                        let rows = tx
                            .execute(&insert_sql, [])
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;

                        Ok::<usize, DataFusionError>(rows)
                    })();

                    match inner_result {
                        Ok(rows) => {
                            // Only commit if the async side confirmed the stream completed
                            // successfully.
                            commit_rx.blocking_recv().map_err(|_| {
                                DataFusionError::Execution(format!(
                                    "Stream terminated before all data was sent to {table_ref_quoted}; rolling back"
                                ))
                            })?;
                            tx.commit()
                                .map_err(|e| DataFusionError::External(Box::new(e)))?;
                            Ok(rows as u64)
                        }
                        Err(e) => {
                            // Transaction automatically rolls back on drop.
                            Err(e)
                        }
                    }
                };

                // Best-effort cleanup: drop the temporary view after the transaction is resolved.
                let _ = duckdb_conn
                    .conn
                    .execute(&format!(r#"DROP VIEW IF EXISTS "{view_name}""#), []);

                result
            },
        );

        // Feed batches from the async stream into the sync channel.
        // If the stream yields an error, we must still await the blocking task
        // so it can roll back the transaction and clean up the temporary view.
        let mut stream_error: Option<DataFusionError> = None;
        while let Some(batch) = data.next().await {
            match batch {
                Ok(batch) => {
                    if batch_tx.send(batch).await.is_err() {
                        break; // Receiver dropped, write task failed
                    }
                }
                Err(e) => {
                    stream_error = Some(e);
                    break;
                }
            }
        }
        drop(batch_tx); // Signal end of stream to the blocking side

        if stream_error.is_none() {
            // All batches sent successfully — tell the blocking side it is safe to commit.
            let _ = commit_tx.send(());
        }
        // else: commit_tx is dropped without sending, so blocking_recv returns
        // Err and the transaction rolls back.

        // Always await the blocking task to ensure cleanup completes before
        // releasing the write lock.
        let write_result = write_handle
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if let Some(e) = stream_error {
            return Err(e);
        }

        write_result
    }
}

/// Bridges an async `mpsc::Receiver<RecordBatch>` to a synchronous `RecordBatchReader`
/// for use with DuckDB's `register_arrow_scan_view`.
struct SyncRecordBatchReceiver {
    rx: mpsc::Receiver<RecordBatch>,
    schema: SchemaRef,
}

impl SyncRecordBatchReceiver {
    fn new(rx: mpsc::Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        Self { rx, schema }
    }
}

impl Iterator for SyncRecordBatchReceiver {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.blocking_recv().map(Ok)
    }
}

impl RecordBatchReader for SyncRecordBatchReceiver {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::memory::MemoryStream;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn get_mem_pool() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory()
                .expect("should create in-memory DuckDB connection pool"),
        )
    }

    /// Creates a test table and seeds it with initial rows.
    fn setup_table(pool: &Arc<DuckDbConnectionPool>, table_name: &str) {
        let mut conn = Arc::clone(pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .execute_batch(&format!(
                r#"
                CREATE TABLE "{table_name}" (id BIGINT, name VARCHAR);
                INSERT INTO "{table_name}" VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
                "#,
            ))
            .expect("should create and seed table");
    }

    fn row_count(pool: &Arc<DuckDbConnectionPool>, table_name: &str) -> i64 {
        let mut conn = Arc::clone(pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .query_row(
                &format!(r#"SELECT COUNT(1) FROM "{table_name}""#),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("should get count")
    }

    fn make_batches(ids: Vec<i64>, names: Vec<&str>) -> Vec<RecordBatch> {
        vec![
            RecordBatch::try_new(
                test_schema(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .expect("should create record batch"),
        ]
    }

    async fn write_via_sink(
        pool: &Arc<DuckDbConnectionPool>,
        table_ref: TableReference,
        insert_op: InsertOp,
        batches: Vec<RecordBatch>,
    ) -> u64 {
        let schema = batches[0].schema();
        let sink = DuckDbFederatedDataSink {
            pool: Arc::clone(pool),
            table_reference: table_ref,
            schema: Arc::clone(&schema),
            write_lock: Arc::new(Mutex::new(())),
            insert_op,
        };

        let stream =
            Box::pin(MemoryStream::try_new(batches, schema, None).expect("should create stream"));

        Arc::new(sink)
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("should write all")
    }

    #[tokio::test]
    async fn test_append_inserts_rows() {
        let pool = get_mem_pool();
        setup_table(&pool, "test_table");

        let rows_written = write_via_sink(
            &pool,
            TableReference::bare("test_table"),
            InsertOp::Append,
            make_batches(vec![4, 5], vec!["dave", "eve"]),
        )
        .await;

        assert_eq!(rows_written, 2);
        assert_eq!(row_count(&pool, "test_table"), 5);
    }

    #[tokio::test]
    async fn test_overwrite_replaces_rows() {
        let pool = get_mem_pool();
        setup_table(&pool, "test_table");
        assert_eq!(row_count(&pool, "test_table"), 3);

        let rows_written = write_via_sink(
            &pool,
            TableReference::bare("test_table"),
            InsertOp::Overwrite,
            make_batches(vec![10, 20], vec!["x", "y"]),
        )
        .await;

        assert_eq!(rows_written, 2);
        assert_eq!(row_count(&pool, "test_table"), 2);
    }

    #[tokio::test]
    async fn test_append_to_empty_table() {
        let pool = get_mem_pool();
        let mut conn = Arc::clone(&pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .execute_batch(r#"CREATE TABLE "empty_tbl" (id BIGINT, name VARCHAR)"#)
            .expect("should create table");

        let rows_written = write_via_sink(
            &pool,
            TableReference::bare("empty_tbl"),
            InsertOp::Append,
            make_batches(vec![1], vec!["only"]),
        )
        .await;

        assert_eq!(rows_written, 1);
        assert_eq!(row_count(&pool, "empty_tbl"), 1);
    }

    #[tokio::test]
    async fn test_overwrite_empty_table() {
        let pool = get_mem_pool();
        let mut conn = Arc::clone(&pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .execute_batch(r#"CREATE TABLE "empty_tbl" (id BIGINT, name VARCHAR)"#)
            .expect("should create table");

        let rows_written = write_via_sink(
            &pool,
            TableReference::bare("empty_tbl"),
            InsertOp::Overwrite,
            make_batches(vec![1, 2], vec!["a", "b"]),
        )
        .await;

        assert_eq!(rows_written, 2);
        assert_eq!(row_count(&pool, "empty_tbl"), 2);
    }

    #[tokio::test]
    async fn test_multipart_table_reference() {
        let pool = get_mem_pool();

        // DuckDB in-memory default catalog is "memory", default schema is "main"
        let mut conn = Arc::clone(&pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .execute_batch(r#"CREATE TABLE "main"."multi_test" (id BIGINT, name VARCHAR)"#)
            .expect("should create table");

        let rows_written = write_via_sink(
            &pool,
            TableReference::partial("main", "multi_test"),
            InsertOp::Append,
            make_batches(vec![1, 2, 3], vec!["a", "b", "c"]),
        )
        .await;

        assert_eq!(rows_written, 3);
        assert_eq!(row_count(&pool, "multi_test"), 3);
    }

    #[tokio::test]
    async fn test_append_then_overwrite() {
        let pool = get_mem_pool();
        setup_table(&pool, "test_table");

        // Append 2 rows → total 5
        write_via_sink(
            &pool,
            TableReference::bare("test_table"),
            InsertOp::Append,
            make_batches(vec![4, 5], vec!["dave", "eve"]),
        )
        .await;
        assert_eq!(row_count(&pool, "test_table"), 5);

        // Overwrite with 1 row → total 1
        write_via_sink(
            &pool,
            TableReference::bare("test_table"),
            InsertOp::Overwrite,
            make_batches(vec![99], vec!["only"]),
        )
        .await;
        assert_eq!(row_count(&pool, "test_table"), 1);

        // Append again → total 3
        write_via_sink(
            &pool,
            TableReference::bare("test_table"),
            InsertOp::Append,
            make_batches(vec![100, 101], vec!["more1", "more2"]),
        )
        .await;
        assert_eq!(row_count(&pool, "test_table"), 3);
    }

    #[tokio::test]
    async fn test_special_characters_in_names() {
        let pool = get_mem_pool();

        // Create a schema and table with special characters (spaces, dots, reserved words)
        let mut conn = Arc::clone(&pool).connect_sync().expect("should connect");
        let duckdb_conn = DuckDB::duckdb_conn(&mut conn).expect("should get DuckDB connection");
        duckdb_conn
            .conn
            .execute_batch(
                r#"
                CREATE SCHEMA "my schema.v2";
                CREATE TABLE "my schema.v2"."my.table" (id BIGINT, name VARCHAR);
                "#,
            )
            .expect("should create schema and table");

        // Append with special characters in schema and table name
        let rows_written = write_via_sink(
            &pool,
            TableReference::partial("my schema.v2", "my.table"),
            InsertOp::Append,
            make_batches(vec![1, 2], vec!["hello", "world"]),
        )
        .await;

        assert_eq!(rows_written, 2);

        let count: i64 = {
            let mut c = Arc::clone(&pool).connect_sync().expect("should connect");
            let dc = DuckDB::duckdb_conn(&mut c).expect("should get DuckDB connection");
            dc.conn
                .query_row(
                    r#"SELECT COUNT(1) FROM "my schema.v2"."my.table""#,
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("should get count")
        };
        assert_eq!(count, 2);

        // Overwrite with special characters
        let rows_written = write_via_sink(
            &pool,
            TableReference::partial("my schema.v2", "my.table"),
            InsertOp::Overwrite,
            make_batches(vec![99], vec!["replaced"]),
        )
        .await;

        assert_eq!(rows_written, 1);

        let count: i64 = {
            let mut c = Arc::clone(&pool).connect_sync().expect("should connect");
            let dc = DuckDB::duckdb_conn(&mut c).expect("should get DuckDB connection");
            dc.conn
                .query_row(
                    r#"SELECT COUNT(1) FROM "my schema.v2"."my.table""#,
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("should get count")
        };
        assert_eq!(count, 1);
    }
}
