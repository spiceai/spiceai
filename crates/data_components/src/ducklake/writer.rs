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

//! A federated `DuckDB` table writer that bypasses the acceleration-oriented
//! write path in `datafusion-table-providers`, which was designed for acceleration tables and:
//! 1. Does not support multi-part table references, flattening them into a single quoted identifier (by design)
//! 2. Has acceleration-specific logic for managing internal tables, views, indexes, and other optimizations
//!
//! Write mechanism:
//! 1. Bridges async `RecordBatchStream` to sync via an mpsc channel
//! 2. Bulk ingests into a temporary staging table through ADBC
//! 3. Executes `INSERT INTO <table> SELECT * FROM temp.main.<stage>` in the same transaction
//! 4. Drops the staging table and commits (or rolls back on error)

use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{DuckLakePool, execute_sql};
use adbc_core::options::{IngestMode, OptionConnection, OptionStatement};
use adbc_core::{Connection as _, Optionable as _, Statement as _};
use arrow::array::{Array as _, RecordBatch, RecordBatchReader};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::TableReference;
use datafusion::common::{Constraints, DataFusionError, SchemaExt};
use datafusion::datasource::TableProvider;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use datafusion_federation::FederatedTableProviderAdaptor;
use futures::StreamExt;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;

/// Process-wide counter to guarantee unique temporary staging table names.
static STAGE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A federated `DuckDB` table writer.
pub struct DuckDbFederatedTableWriter {
    read_provider: Arc<dyn TableProvider>,
    pool: Arc<DuckLakePool>,
    table_reference: TableReference,
    write_lock: Arc<Mutex<()>>,
}

impl Debug for DuckDbFederatedTableWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDbFederatedTableWriter")
            .field("table_reference", &self.table_reference)
            .finish_non_exhaustive()
    }
}

impl DuckDbFederatedTableWriter {
    /// Create a writable `TableProvider` that preserves federation pushdown.
    ///
    /// Extracts the `FederatedTableSource` from `read_provider` (if present)
    /// and re-wraps the writer in a `FederatedTableProviderAdaptor` so the
    /// federation optimizer can still detect it.
    #[must_use]
    pub fn create(
        read_provider: Arc<dyn TableProvider>,
        pool: Arc<DuckLakePool>,
        table_reference: &TableReference,
        write_lock: Arc<Mutex<()>>,
    ) -> Arc<dyn TableProvider> {
        let fed_source = read_provider
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .map(|adaptor| Arc::clone(&adaptor.source));

        let writer: Arc<dyn TableProvider> = Arc::new(Self {
            read_provider,
            pool,
            table_reference: table_reference.clone(),
            write_lock,
        });

        if let Some(source) = fed_source {
            tracing::debug!(%table_reference, "Created federated writable DuckDB table provider");
            Arc::new(FederatedTableProviderAdaptor::new_with_provider(
                source, writer,
            ))
        } else {
            tracing::debug!(%table_reference, "Created non-federated writable DuckDB table provider");
            writer
        }
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for DuckDbFederatedTableWriter {
    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.read_provider.constraints()
    }

    fn table_type(&self) -> TableType {
        self.read_provider.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.read_provider.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.read_provider.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.read_provider.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.read_provider.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.read_provider.statistics()
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

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> DFResult<datafusion::catalog::ScanResult> {
        self.read_provider.scan_with_args(state, args).await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if op == InsertOp::Replace {
            return Err(DataFusionError::NotImplemented(
                "REPLACE INTO is not supported".to_string(),
            ));
        }

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

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.read_provider.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.read_provider.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.read_provider.truncate(state).await
    }

    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: datafusion::common::DFSchemaRef,
        on: datafusion::prelude::Expr,
        clauses: Vec<datafusion::logical_expr::dml::MergeIntoClause>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .merge_into(state, source, merge_schema, on, clauses)
            .await
    }
}

struct DuckDbFederatedDataSink {
    pool: Arc<DuckLakePool>,
    table_reference: TableReference,
    schema: SchemaRef,
    write_lock: Arc<Mutex<()>>,
    insert_op: InsertOp,
}

impl Debug for DuckDbFederatedDataSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDbFederatedDataSink")
            .field("table_reference", &self.table_reference)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckDbFederatedDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDbFederatedDataSink")
    }
}

#[async_trait]
impl DataSink for DuckDbFederatedDataSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let _write_guard = Arc::clone(&self.write_lock).lock_owned().await;

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

        let write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                // Keep the write lock in the blocking task too: cancelling the
                // async producer must not release it before rollback completes.
                let _write_guard = _write_guard;
                let mut connection = pool.connect().map_err(DataFusionError::External)?;
                let connection = &mut connection;
                connection
                    .set_option(OptionConnection::AutoCommit, "false".into())
                    .map_err(|source| DataFusionError::External(Box::new(source)))?;
                let stage_id = STAGE_COUNTER.fetch_add(1, Ordering::Relaxed);
                let stage = format!("__spice_stage_{stage_id}");
                let result = (|| -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
                    let mut ingest = connection.new_statement()?;
                    ingest.set_option(OptionStatement::TargetTable, stage.clone().into())?;
                    ingest.set_option(OptionStatement::Temporary, "true".into())?;
                    ingest.set_option(OptionStatement::IngestMode, IngestMode::Create.into())?;
                    ingest.bind_stream(Box::new(SyncRecordBatchReceiver::new(batch_rx, schema)))?;
                    ingest.execute_update()?;
                    drop(ingest);
                    // Channel closure alone cannot distinguish a successful stream
                    // from cancellation or an input error after partial ingestion.
                    commit_rx
                        .blocking_recv()
                        .map_err(|_| super::Error::StreamTerminated {
                            table: table_ref_quoted.clone(),
                        })?;
                    if insert_op == InsertOp::Overwrite {
                        execute_sql(connection, &format!("DELETE FROM {table_ref_quoted}"))?;
                    }
                    // DuckDB's ADBC update count may be unknown. Count the staged
                    // rows explicitly rather than acknowledging an inferred count.
                    let mut count = connection.new_statement()?;
                    count.set_sql_query(format!("SELECT count(*) FROM temp.main.\"{stage}\""))?;
                    let mut reader = count.execute()?;
                    let batch = reader.next().ok_or(super::Error::Metadata {
                        message: "DuckDB returned no staged row count",
                    })??;
                    if batch.num_rows() != 1
                        || batch.num_columns() != 1
                        || batch.column(0).is_null(0)
                    {
                        return Err(super::Error::Metadata {
                            message: "DuckDB returned an invalid staged row count",
                        }
                        .into());
                    }
                    let counts = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .ok_or(super::Error::Metadata {
                            message: "DuckDB returned an invalid staged row count",
                        })?;
                    let rows = u64::try_from(counts.value(0))?;
                    drop(reader);
                    drop(count);
                    execute_sql(
                        connection,
                        &format!(
                            "INSERT INTO {table_ref_quoted} SELECT * FROM temp.main.\"{stage}\""
                        ),
                    )?;
                    execute_sql(connection, &format!("DROP TABLE temp.main.\"{stage}\""))?;
                    connection.commit()?;
                    Ok(rows)
                })();
                if result.is_err() {
                    connection
                        .rollback()
                        .map_err(|source| DataFusionError::External(Box::new(source)))?;
                }
                result.map_err(DataFusionError::External)
            });

        // Feed batches from the async stream into the sync channel.
        // If the stream yields an error, we must still await the blocking task
        // so it can roll back the transaction and clean up the temporary staging table.
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
        } else {
            drop(commit_tx);
        }

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
/// for ADBC bulk ingestion.
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
