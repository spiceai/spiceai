use std::time::{SystemTime, UNIX_EPOCH};
use std::{any::Any, fmt, sync::Arc};

use crate::duckdb::DuckDB;
use crate::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use crate::util::{
    constraints,
    on_conflict::OnConflict,
    retriable_error::{check_and_mark_retriable_error, to_retriable_data_write_error},
};
use arrow::array::RecordBatchReader;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use arrow_schema::ArrowError;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{Constraints, SchemaExt};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{metrics::MetricsSet, DisplayAs, DisplayFormatType, ExecutionPlan},
};
use duckdb::Transaction;
use futures::StreamExt;
use snafu::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use super::creator::{TableDefinition, TableManager, ViewCreator};
use super::write_settings::DuckDBWriteSettings;
use super::{to_datafusion_error, RelationName};

/// A callback handler that is invoked after data has been successfully written to a DuckDB table
/// but before the transaction is committed.
///
/// The handler executes within the same transaction as the data write operation. This means:
/// - Any operations performed by the handler are atomic with the data write
/// - If the handler returns an error, both the handler's operations and the data write are rolled back
/// - The handler has access to the newly written data for queries and validations
pub type WriteCompletionHandler = Arc<
    dyn Fn(&Transaction<'_>, &TableManager, &SchemaRef, u64) -> datafusion::common::Result<()>
        + Send
        + Sync
        + 'static,
>;

// checking schemas are equivalent is disabled because it incorrectly marks single-level list fields are different when the name of the field is different
// e.g. List(Field { name: 'a', data_type: Int32 }) != List(Field { name: 'b', data_type: Int32 })
// but, in this case, they are actually equivalent because the field name does not matter for the schema.
// related: https://github.com/apache/arrow-rs/issues/6733#issuecomment-2482582556
const SCHEMA_EQUIVALENCE_ENABLED: bool = false;

#[derive(Default)]
pub struct DuckDBTableWriterBuilder {
    read_provider: Option<Arc<dyn TableProvider>>,
    pool: Option<Arc<DuckDbConnectionPool>>,
    on_conflict: Option<OnConflict>,
    table_definition: Option<Arc<TableDefinition>>,
    on_data_written: Option<WriteCompletionHandler>,
    write_settings: Option<DuckDBWriteSettings>,
}

impl DuckDBTableWriterBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_read_provider(mut self, read_provider: Arc<dyn TableProvider>) -> Self {
        self.read_provider = Some(read_provider);
        self
    }

    #[must_use]
    pub fn with_pool(mut self, pool: Arc<DuckDbConnectionPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    #[must_use]
    pub fn set_on_conflict(mut self, on_conflict: Option<OnConflict>) -> Self {
        self.on_conflict = on_conflict;
        self
    }

    #[must_use]
    pub fn with_table_definition(mut self, table_definition: TableDefinition) -> Self {
        self.table_definition = Some(Arc::new(table_definition));
        self
    }

    #[must_use]
    pub fn with_on_data_written(mut self, on_data_written: WriteCompletionHandler) -> Self {
        self.on_data_written = Some(on_data_written);
        self
    }

    #[must_use]
    pub fn with_write_settings(mut self, write_settings: DuckDBWriteSettings) -> Self {
        self.write_settings = Some(write_settings);
        self
    }

    /// Builds a `DuckDBTableWriter` from the provided configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the required fields are missing:
    /// - `read_provider`
    /// - `pool`
    /// - `table_definition`
    pub fn build(self) -> super::Result<DuckDBTableWriter> {
        let Some(read_provider) = self.read_provider else {
            return Err(super::Error::MissingReadProvider);
        };

        let Some(pool) = self.pool else {
            return Err(super::Error::MissingPool);
        };

        let Some(table_definition) = self.table_definition else {
            return Err(super::Error::MissingTableDefinition);
        };

        Ok(DuckDBTableWriter {
            read_provider,
            on_conflict: self.on_conflict,
            table_definition,
            pool,
            on_data_written: self.on_data_written,
            write_settings: self.write_settings.unwrap_or_default(),
        })
    }
}

#[derive(Clone)]
pub struct DuckDBTableWriter {
    pub read_provider: Arc<dyn TableProvider>,
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    on_conflict: Option<OnConflict>,
    on_data_written: Option<WriteCompletionHandler>,
    write_settings: DuckDBWriteSettings,
}

impl std::fmt::Debug for DuckDBTableWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTableWriter")
            .field("read_provider", &self.read_provider)
            .field("pool", &self.pool)
            .field("table_definition", &self.table_definition)
            .field("on_conflict", &self.on_conflict)
            .field(
                "on_data_written",
                &self
                    .on_data_written
                    .as_ref()
                    .map_or("None", |_| "Some(callback)"),
            )
            .field("write_settings", &self.write_settings)
            .finish()
    }
}

impl DuckDBTableWriter {
    #[must_use]
    pub fn pool(&self) -> Arc<DuckDbConnectionPool> {
        Arc::clone(&self.pool)
    }

    #[must_use]
    pub fn table_definition(&self) -> Arc<TableDefinition> {
        Arc::clone(&self.table_definition)
    }

    #[must_use]
    pub fn on_conflict(&self) -> Option<&OnConflict> {
        self.on_conflict.as_ref()
    }

    #[must_use]
    pub fn write_settings(&self) -> &DuckDBWriteSettings {
        &self.write_settings
    }

    #[must_use]
    pub fn with_on_data_written_handler(mut self, on_data_written: WriteCompletionHandler) -> Self {
        self.on_data_written = Some(on_data_written);
        self
    }
}

#[async_trait]
impl TableProvider for DuckDBTableWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.read_provider.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.table_definition.constraints()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.read_provider
            .scan(state, projection, filters, limit)
            .await
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut sink = DuckDBDataSink::new(
            Arc::clone(&self.pool),
            Arc::clone(&self.table_definition),
            op,
            self.on_conflict.clone(),
            self.schema(),
        )
        .with_write_settings(self.write_settings.clone());

        if let Some(handler) = &self.on_data_written {
            sink = sink.with_on_data_written_handler(Arc::clone(handler));
        }

        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)) as _)
    }
}

pub(crate) struct DuckDBDataSink {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
    on_data_written: Option<WriteCompletionHandler>,
    write_settings: DuckDBWriteSettings,
}

#[async_trait]
impl DataSink for DuckDBDataSink {
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
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let pool = Arc::clone(&self.pool);
        let table_definition = Arc::clone(&self.table_definition);
        let overwrite = self.overwrite;
        let on_conflict = self.on_conflict.clone();
        let on_data_written = self.on_data_written.clone();
        let write_settings = self.write_settings.clone();

        // Limit channel size to a maximum of 100 RecordBatches queued for cases when DuckDB is slower than the writer stream,
        // so that we don't significantly increase memory usage. After the maximum RecordBatches are queued, the writer stream will wait
        // until DuckDB is able to process more data.
        let (batch_tx, batch_rx): (Sender<RecordBatch>, Receiver<RecordBatch>) = mpsc::channel(100);

        // Since the main task/stream can be dropped or fail, we use a oneshot channel to signal that all data is received and we should commit the transaction
        let (notify_commit_transaction, on_commit_transaction) = tokio::sync::oneshot::channel();

        let schema = data.schema();

        let duckdb_write_handle: JoinHandle<datafusion::common::Result<u64>> =
            tokio::task::spawn_blocking(move || {
                let num_rows = match overwrite {
                    InsertOp::Overwrite => insert_overwrite(
                        pool,
                        &table_definition,
                        batch_rx,
                        on_conflict.as_ref(),
                        on_data_written.as_ref(),
                        on_commit_transaction,
                        schema,
                        &write_settings,
                    )?,
                    InsertOp::Append | InsertOp::Replace => insert_append(
                        pool,
                        &table_definition,
                        batch_rx,
                        on_conflict.as_ref(),
                        on_data_written.as_ref(),
                        on_commit_transaction,
                        schema,
                        &write_settings,
                    )?,
                };

                Ok(num_rows)
            });

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            if let Some(constraints) = self.table_definition.constraints() {
                constraints::validate_batch_with_constraints(
                    vec![batch.clone()],
                    constraints,
                    &crate::util::constraints::UpsertOptions::default(),
                )
                .await
                .context(super::ConstraintViolationSnafu)
                .map_err(to_datafusion_error)?;
            }

            if let Err(send_error) = batch_tx.send(batch).await {
                match duckdb_write_handle.await {
                    Err(join_error) => {
                        return Err(DataFusionError::Execution(format!(
                            "Error writing to DuckDB: {join_error}"
                        )));
                    }
                    Ok(Err(datafusion_error)) => {
                        return Err(datafusion_error);
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unable to send RecordBatch to DuckDB writer: {send_error}"
                        )))
                    }
                };
            }
        }

        if notify_commit_transaction.send(()).is_err() {
            return Err(DataFusionError::Execution(
                "Unable to send message to commit transaction to DuckDB writer.".to_string(),
            ));
        };

        // Drop the sender to signal the receiver that no more data is coming
        drop(batch_tx);

        match duckdb_write_handle.await {
            Ok(result) => result,
            Err(e) => Err(DataFusionError::Execution(format!(
                "Error writing to DuckDB: {e}"
            ))),
        }
    }
}

impl DuckDBDataSink {
    pub(crate) fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            pool,
            table_definition,
            overwrite,
            on_conflict,
            schema,
            on_data_written: None,
            write_settings: DuckDBWriteSettings::default(),
        }
    }

    #[must_use]
    pub fn with_on_data_written_handler(mut self, handler: WriteCompletionHandler) -> Self {
        self.on_data_written = Some(handler);
        self
    }

    #[must_use]
    pub fn with_write_settings(mut self, write_settings: DuckDBWriteSettings) -> Self {
        self.write_settings = write_settings;
        self
    }
}

impl std::fmt::Debug for DuckDBDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

impl DisplayAs for DuckDBDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBDataSink")
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_append(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<RecordBatch>,
    on_conflict: Option<&OnConflict>,
    on_data_written: Option<&WriteCompletionHandler>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: SchemaRef,
    write_settings: &DuckDBWriteSettings,
) -> datafusion::common::Result<u64> {
    let mut db_conn = pool
        .connect_sync()
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    let append_table = TableManager::new(Arc::clone(table_definition))
        .with_internal(false)
        .map_err(to_retriable_data_write_error)?;

    let should_have_indexes = !append_table.indexes_vec().is_empty();
    let has_indexes = !append_table
        .current_indexes(&tx)
        .map_err(to_retriable_data_write_error)?
        .is_empty();
    let is_empty_table = append_table
        .get_row_count(&tx)
        .map_err(to_retriable_data_write_error)?
        == 0;
    let should_apply_indexes = should_have_indexes && !has_indexes && is_empty_table;

    let append_table_schema = append_table
        .current_schema(&tx)
        .map_err(to_retriable_data_write_error)?;

    if SCHEMA_EQUIVALENCE_ENABLED && !schema.equivalent_names_and_types(&append_table_schema) {
        return Err(DataFusionError::Execution(
            "Schema of the append table does not match the schema of the new append data."
                .to_string(),
        ));
    }

    tracing::debug!(
        "Append load for {table_name}",
        table_name = append_table.table_name()
    );
    let num_rows = write_to_table(
        &append_table,
        &tx,
        Arc::clone(&schema),
        batch_rx,
        on_conflict,
    )
    .map_err(to_retriable_data_write_error)?;

    if let Some(callback) = on_data_written {
        callback(&tx, &append_table, &schema, num_rows)?;
    }

    if write_settings.recompute_statistics_on_write {
        execute_analyze_sql(&tx, &append_table.table_name().to_string());
    }

    on_commit_transaction
        .try_recv()
        .map_err(to_retriable_data_write_error)?;

    tx.commit()
        .context(super::UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_datafusion_error)?;

    // apply indexes if new table
    if should_apply_indexes {
        tracing::debug!(
            "Load for table {table_name} complete, applying constraints and indexes.",
            table_name = append_table.table_name()
        );

        append_table
            .create_indexes(&tx)
            .map_err(to_retriable_data_write_error)?;
    }

    let primary_keys_match = append_table
        .verify_primary_keys_match(&append_table, &tx)
        .map_err(to_retriable_data_write_error)?;
    let indexes_match = append_table
        .verify_indexes_match(&append_table, &tx)
        .map_err(to_retriable_data_write_error)?;

    if !primary_keys_match {
        return Err(DataFusionError::Execution(
            "Primary keys do not match between the new table and the existing table.\nEnsure primary key configuration is the same as the existing table, or manually migrate the table.".to_string(),
        ));
    }

    if !indexes_match {
        return Err(DataFusionError::Execution(
            "Indexes do not match between the new table and the existing table.\nEnsure index configuration is the same as the existing table, or manually migrate the table.".to_string(),
        ));
    }

    tx.commit()
        .context(super::UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    Ok(num_rows)
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn insert_overwrite(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<RecordBatch>,
    on_conflict: Option<&OnConflict>,
    on_data_written: Option<&WriteCompletionHandler>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: SchemaRef,
    write_settings: &DuckDBWriteSettings,
) -> datafusion::common::Result<u64> {
    let cloned_pool = Arc::clone(&pool);
    let mut db_conn = pool
        .connect_sync()
        .context(super::DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    let new_table = TableManager::new(Arc::clone(table_definition))
        .with_internal(true)
        .map_err(to_retriable_data_write_error)?;

    new_table
        .create_table(cloned_pool, &tx)
        .map_err(to_retriable_data_write_error)?;

    let existing_tables = new_table
        .list_other_internal_tables(&tx)
        .map_err(to_retriable_data_write_error)?;
    let base_table = new_table
        .base_table(&tx)
        .map_err(to_retriable_data_write_error)?;
    let last_table = match (existing_tables.last(), base_table.as_ref()) {
        (Some(internal_table), Some(base_table)) => {
            return Err(DataFusionError::Execution(
                format!("Failed to insert data for DuckDB - both an internal table and definition base table were found.\nManual table migration is required - delete the table '{internal_table}' or '{base_table}' and try again.",
                internal_table = internal_table.0.table_name(),
                base_table = base_table.table_name())));
        }
        (Some((table, _)), None) | (None, Some(table)) => Some(table),
        (None, None) => None,
    };

    if let Some(last_table) = last_table {
        let should_have_indexes = !last_table.indexes_vec().is_empty();
        let has_indexes = !last_table
            .current_indexes(&tx)
            .map_err(to_retriable_data_write_error)?
            .is_empty();
        let is_empty_table = last_table
            .get_row_count(&tx)
            .map_err(to_retriable_data_write_error)?
            == 0;
        let should_apply_indexes = should_have_indexes && !has_indexes && is_empty_table;

        let last_table_schema = last_table
            .current_schema(&tx)
            .map_err(to_retriable_data_write_error)?;
        let new_table_schema = new_table
            .current_schema(&tx)
            .map_err(to_retriable_data_write_error)?;

        if SCHEMA_EQUIVALENCE_ENABLED
            && !new_table_schema.equivalent_names_and_types(&last_table_schema)
        {
            return Err(DataFusionError::Execution(
                "Schema does not match between the new table and the existing table.".to_string(),
            ));
        }

        if !should_apply_indexes {
            // compare indexes and primary keys
            let primary_keys_match = new_table
                .verify_primary_keys_match(last_table, &tx)
                .map_err(to_retriable_data_write_error)?;
            let indexes_match = new_table
                .verify_indexes_match(last_table, &tx)
                .map_err(to_retriable_data_write_error)?;

            if !primary_keys_match {
                return Err(DataFusionError::Execution(
                    "Primary keys do not match between the new table and the existing table.\nEnsure primary key configuration is the same as the existing table, or manually migrate the table."
                        .to_string(),
                ));
            }

            if !indexes_match {
                return Err(DataFusionError::Execution(
                    "Indexes do not match between the new table and the existing table.\nEnsure index configuration is the same as the existing table, or manually migrate the table.".to_string(),
                ));
            }
        }
    }

    tracing::debug!("Initial load for {}", new_table.table_name());
    let num_rows = write_to_table(&new_table, &tx, Arc::clone(&schema), batch_rx, on_conflict)
        .map_err(to_retriable_data_write_error)?;

    on_commit_transaction
        .try_recv()
        .map_err(to_retriable_data_write_error)?;

    if let Some(base_table) = base_table {
        base_table
            .delete_table(&tx)
            .map_err(to_retriable_data_write_error)?;
    }

    new_table
        .create_view(&tx)
        .map_err(to_retriable_data_write_error)?;

    if let Some(callback) = on_data_written {
        callback(&tx, &new_table, &schema, num_rows)?;
    }

    if write_settings.recompute_statistics_on_write {
        execute_analyze_sql(&tx, &new_table.table_name().to_string());
    }

    tx.commit()
        .context(super::UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    tracing::debug!(
        "Load for table {table_name} complete, applying constraints and indexes.",
        table_name = new_table.table_name()
    );

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(super::UnableToBeginTransactionSnafu)
        .map_err(to_datafusion_error)?;

    for (table, _) in existing_tables {
        table
            .delete_table(&tx)
            .map_err(to_retriable_data_write_error)?;
    }

    // Apply constraints and indexes.
    new_table
        .create_indexes(&tx)
        .map_err(to_retriable_data_write_error)?;

    tx.commit()
        .context(super::UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    Ok(num_rows)
}

#[allow(clippy::doc_markdown)]
/// Writes a stream of ``RecordBatch``es to a DuckDB table.
fn write_to_table(
    table: &TableManager,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    data_batches: Receiver<RecordBatch>,
    on_conflict: Option<&OnConflict>,
) -> datafusion::common::Result<u64> {
    let stream = FFI_ArrowArrayStream::new(Box::new(RecordBatchReaderFromStream::new(
        data_batches,
        schema,
    )));

    let current_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(super::UnableToGetSystemTimeSnafu)
        .map_err(to_datafusion_error)?
        .as_millis();

    let view_name = format!("__scan_{}_{current_ts}", table.table_name());
    tx.register_arrow_scan_view(&view_name, &stream)
        .context(super::UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    let view = ViewCreator::from_name(RelationName::new(view_name));
    let rows = view
        .insert_into(table, tx, on_conflict)
        .map_err(to_datafusion_error)?;
    view.drop(tx).map_err(to_datafusion_error)?;

    Ok(rows as u64)
}

/// Executes an ANALYZE statement to update query optimizer statistics.
/// This helps DuckDB's query planner generate better execution plans, especially after large data changes.
/// https://duckdb.org/docs/stable/sql/statements/analyze
///
/// Errors are logged but do not fail the operation since statistics updates are non-critical.
pub fn execute_analyze_sql(tx: &Transaction, table_name: &str) {
    // DuckDB doesn't support parameterized table names in the ANALYZE statement
    let analyze_sql = format!(r#"ANALYZE "{table_name}""#);
    tracing::debug!("Executing analyze SQL: {analyze_sql}");
    match tx.prepare(&analyze_sql) {
        Ok(mut stmt) => match stmt.execute([]) {
            Ok(_) => {
                tracing::debug!("Analyze SQL executed for table '{table_name}'");
            }
            Err(e) => {
                tracing::error!("Failed to execute analyze SQL for table '{table_name}': {e}");
            }
        },
        Err(e) => {
            tracing::error!("Failed to prepare analyze SQL for table '{table_name}': {e}");
        }
    }
}

struct RecordBatchReaderFromStream {
    stream: Receiver<RecordBatch>,
    schema: SchemaRef,
}

impl RecordBatchReaderFromStream {
    fn new(stream: Receiver<RecordBatch>, schema: SchemaRef) -> Self {
        Self { stream, schema }
    }
}

impl Iterator for RecordBatchReaderFromStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stream.blocking_recv().map(Ok)
    }
}

impl RecordBatchReader for RecordBatchReaderFromStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod test {
    use arrow::array::{Int64Array, StringArray};
    use datafusion::physical_plan::memory::MemoryStream;

    use super::*;
    use crate::{
        duckdb::creator::tests::{get_basic_table_definition, get_mem_duckdb, init_tracing},
        util::{column_reference::ColumnReference, indexes::IndexType},
    };

    #[tokio::test]
    async fn test_write_to_table_overwrite_without_previous_table() {
        // Test scenario: Write to a table with overwrite mode without a previous table
        // Expected behavior: Data sink creates a new internal table, writes data to it, and creates a view with the table definition name

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            table_definition.schema(),
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");
        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 1);

        let table_name = internal_tables.pop().expect("should have a table").0;

        let rows = tx
            .query_row(&format!("SELECT COUNT(1) FROM {table_name}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("to get count");
        assert_eq!(rows, 2);

        // expect a view to be created with the table definition name
        let view_rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {view_name}",
                    view_name = table_definition.name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        assert_eq!(view_rows, 2);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_to_table_overwrite_with_previous_base_table() {
        // Test scenario: Write to a table with overwrite mode with a previous base table
        // Expected behavior: Data sink creates a new internal table, writes data to it.
        // Before creating the view, the base table needs to get dropped as we need to create a view with the same name.

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        // make an existing table to overwrite
        let overwrite_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table");

        overwrite_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        tx.execute(
            &format!(
                "INSERT INTO {table_name} VALUES (3, 'c')",
                table_name = overwrite_table.table_name()
            ),
            [],
        )
        .expect("to insert");

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            table_definition.schema(),
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");
        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 1);

        let table_name = internal_tables.pop().expect("should have a table").0;

        let rows = tx
            .query_row(&format!("SELECT COUNT(1) FROM {table_name}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("to get count");
        assert_eq!(rows, 2);

        let table_creator =
            TableManager::from_table_name(Arc::clone(&table_definition), table_name);
        let base_table = table_creator.base_table(&tx).expect("to get base table");

        assert!(base_table.is_none()); // base table should get deleted

        // expect a view to be created with the table definition name
        let view_rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {view_name}",
                    view_name = table_definition.name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        assert_eq!(view_rows, 2);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_to_table_overwrite_with_previous_internal_table() {
        // Test scenario: Write to a table with overwrite mode with a previous base table
        // Expected behavior: Data sink creates a new internal table, writes data to it.
        // Before creating the view, the base table needs to get dropped as we need to create a view with the same name.

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let table_definition = get_basic_table_definition();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        // make an existing table to overwrite
        let overwrite_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(true)
            .expect("to create table");

        overwrite_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        tx.execute(
            &format!(
                "INSERT INTO {table_name} VALUES (3, 'c')",
                table_name = overwrite_table.table_name()
            ),
            [],
        )
        .expect("to insert");

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            table_definition.schema(),
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");
        let mut internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 1);

        let table_name = internal_tables.pop().expect("should have a table").0;

        let rows = tx
            .query_row(&format!("SELECT COUNT(1) FROM {table_name}"), [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("to get count");
        assert_eq!(rows, 2);

        // expect a view to be created with the table definition name
        let view_rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {view_name}",
                    view_name = table_definition.name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        assert_eq!(view_rows, 2);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_to_table_append_with_previous_table() {
        // Test scenario: Write to a table with append mode with a previous table
        // Expected behavior: Data sink appends data to the existing table. No new internal table should be created.
        // The existing table is re-used.

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let table_definition = get_basic_table_definition();

        // make an existing table to append from
        let append_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table");

        append_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        tx.execute(
            &format!(
                "INSERT INTO {table_name} VALUES (3, 'c')",
                table_name = append_table.table_name()
            ),
            [],
        )
        .expect("to insert");

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Append,
            None,
            table_definition.schema(),
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 0);

        let base_table = append_table
            .base_table(&tx)
            .expect("to get base table")
            .expect("should have a base table");

        let rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {table_name}",
                    table_name = base_table.table_name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");
        assert_eq!(rows, 3);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_to_table_append_with_previous_table_needs_indexes() {
        // Test scenario: Write to a table with append mode with a previous table
        // Expected behavior: Data sink appends data to the existing table. No new internal table should be created.
        // The existing table is re-used.

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(
                    vec![(
                        ColumnReference::try_from("id").expect("valid column ref"),
                        IndexType::Enabled,
                    )]
                    .into_iter()
                    .collect(),
                ),
        );

        // make an existing table to append from
        let append_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table");

        append_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // don't apply indexes, and leave the table empty to simulate a new table from TableProviderFactory::create()

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Append,
            None,
            table_definition.schema(),
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let internal_tables = table_definition
            .list_internal_tables(&tx)
            .expect("to list internal tables");
        assert_eq!(internal_tables.len(), 0);

        let base_table = append_table
            .base_table(&tx)
            .expect("to get base table")
            .expect("should have a base table");

        let rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {table_name}",
                    table_name = base_table.table_name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");
        assert_eq!(rows, 2);

        // at this point, indexes should be applied
        let indexes = append_table.current_indexes(&tx).expect("to get indexes");
        assert_eq!(indexes.len(), 1);

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_completion_custom_handler() {
        // Test scenario: Verify that the on_data_written callback signature is correct and callback is called during write_all
        // Expected behavior: The callback is executed, callback can delete data as part of append operation

        //Custom callback that removes records with id > 1
        let callback: WriteCompletionHandler = Arc::new(
            |tx: &Transaction<'_>,
             table_manager: &TableManager,
             schema: &SchemaRef,
             num_rows: u64| {
                assert!(!schema.fields().is_empty(), "Schema should have fields");
                assert_eq!(table_manager.table_name().to_string(), "test_table");
                assert_eq!(num_rows, 2, "Should have written 2 rows");
                let table_name = table_manager.table_name();
                tx.execute(&format!("DELETE FROM {table_name} WHERE id > 1"), [])
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to delete rows with id > 1: {e}"
                        ))
                    })?;
                Ok(())
            },
        );

        let _guard = init_tracing(None);
        let pool = get_mem_duckdb();

        let cloned_pool = Arc::clone(&pool);
        let mut conn = cloned_pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let table_definition = Arc::new(
            TableDefinition::new(RelationName::new("test_table"), Arc::clone(&schema))
                .with_indexes(
                    vec![(
                        ColumnReference::try_from("id").expect("valid column ref"),
                        IndexType::Enabled,
                    )]
                    .into_iter()
                    .collect(),
                ),
        );

        // make an existing table to append from
        let append_table = TableManager::new(Arc::clone(&table_definition))
            .with_internal(false)
            .expect("to create table");

        append_table
            .create_table(Arc::clone(&pool), &tx)
            .expect("to create table");

        // don't apply indexes, and leave the table empty to simulate a new table from TableProviderFactory::create()

        tx.commit().expect("to commit");

        let duckdb_sink = DuckDBDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Append,
            None,
            table_definition.schema(),
        )
        .with_on_data_written_handler(callback);
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // id, name
        // 1, "a"
        // 2, "b"
        let batches = vec![RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .expect("should create a record batch")];

        let stream = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let tx = duckdb.conn.transaction().expect("to begin transaction");

        let base_table = append_table
            .base_table(&tx)
            .expect("to get base table")
            .expect("should have a base table");

        let rows = tx
            .query_row(
                &format!(
                    "SELECT COUNT(1) FROM {table_name}",
                    table_name = base_table.table_name()
                ),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("to get count");

        // Must be single row with id = 1 only
        assert_eq!(rows, 1);

        tx.rollback().expect("to rollback");
    }
}
