/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_trait::async_trait;
use datafusion::common::{Constraints, utils::quote_identifier};
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, metrics::MetricsSet},
};
use datafusion_table_providers::duckdb::{DuckDB, TableDefinition};
use datafusion_table_providers::sql::db_connection_pool::dbconnection::duckdbconn::DuckDbConnection;
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::constraints::get_primary_keys_from_constraints;
use datafusion_table_providers::util::indexes::IndexType;
use datafusion_table_providers::util::on_conflict::OnConflict;
use datafusion_table_providers::util::retriable_error::{
    check_and_mark_retriable_error, to_retriable_data_write_error,
};
use duckdb::Transaction;
use futures::StreamExt;
use snafu::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::dataaccelerator::UpsertOptions;
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::dataaccelerator::partitioned_duckdb::tables_mode::insert::BatchPartitioner;
use crate::dataaccelerator::partitioned_duckdb::tables_mode::partition_buffer::{
    PartitionBufferConfig, PartitionBufferFactory, PartitionData,
};
use crate::dataaccelerator::upsert_dedup::deduplicate_batch;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DuckDBWriteSettings {
    recompute_statistics_on_write: bool,
}

impl DuckDBWriteSettings {
    #[must_use]
    pub(crate) fn from_params(params: &HashMap<String, String>) -> Self {
        let recompute_statistics_on_write = params
            .get("recompute_statistics_on_write")
            .is_none_or(|value| !value.eq_ignore_ascii_case("false"));

        Self {
            recompute_statistics_on_write,
        }
    }
}

impl Default for DuckDBWriteSettings {
    fn default() -> Self {
        Self {
            recompute_statistics_on_write: true,
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to connect to database: {source}"))]
    DbConnectionPool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to commit transaction: {source}"))]
    UnableToCommitTransaction { source: duckdb::Error },

    #[snafu(display("Unable to begin duckdb transaction: {source}"))]
    UnableToBeginTransaction { source: duckdb::Error },

    #[snafu(display("Failed to register Arrow scan view for DuckDB ingestion: {source}"))]
    UnableToRegisterArrowScanView { source: duckdb::Error },

    #[snafu(display(
        "Failed to register Arrow scan view to build DuckDB table creation statement: {source}"
    ))]
    UnableToRegisterArrowScanViewForTableCreation { source: duckdb::Error },

    #[snafu(display("Unable to create duckdb table: {source}"))]
    UnableToCreateDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to drop duckdb table: {source}"))]
    UnableToDropDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to query data from the duckdb table: {source}"))]
    UnableToQueryData { source: duckdb::Error },

    #[snafu(display("Unable to create index on duckdb table: {source}"))]
    UnableToCreateIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to rollback transaction: {source}"))]
    UnableToRollbackTransaction { source: duckdb::Error },

    #[snafu(display("Failed to get system time since epoch: {source}"))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Failed to get elapsed time: {source}"))]
    UnableToGetElapsedTime { source: std::time::SystemTimeError },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation {
        source: datafusion_table_providers::util::constraints::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

static TEMP_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_name_suffix() -> Result<u64> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context(UnableToGetSystemTimeSnafu)?;
    let nanos = u64::try_from(duration.as_nanos()).map_or(u64::MAX, |value| value);
    Ok(nanos.saturating_add(TEMP_NAME_COUNTER.fetch_add(1, Ordering::Relaxed)))
}

#[derive(Clone)]
/// A `DataFusion` sink that writes partitioned data to separate `DuckDB` tables.
///
/// This struct implements the `DataSink` trait, buffering and writing incoming record batches
/// into `DuckDB` tables according to partitioning logic. Each partition is written to its own
/// `DuckDB` table.
pub struct DuckDBPartitionedDataSink {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    upsert_options: UpsertOptions,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
    schema: SchemaRef,
    partitioner: Arc<BatchPartitioner>,
    write_settings: DuckDBWriteSettings,
    partition_buffer_config: PartitionBufferConfig,
}

#[async_trait]
impl DataSink for DuckDBPartitionedDataSink {
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
        let indexes = self.indexes.clone();
        let constraints = self.constraints.clone();
        let write_settings = self.write_settings;

        let (batch_tx, batch_rx): (
            Sender<(String, PartitionData)>,
            Receiver<(String, PartitionData)>,
        ) = mpsc::channel(10);

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
                        on_commit_transaction,
                        &schema,
                        constraints.clone(),
                        &indexes,
                        &write_settings,
                    )?,
                    InsertOp::Append | InsertOp::Replace => insert_append(
                        pool,
                        &table_definition,
                        batch_rx,
                        on_conflict.as_ref(),
                        on_commit_transaction,
                        &schema,
                        constraints.clone(),
                        &indexes,
                        &write_settings,
                    )?,
                };

                Ok(num_rows)
            });

        let mut partition_buffer = PartitionBufferFactory::create_buffer(
            &self.partition_buffer_config,
            batch_tx,
            Arc::clone(&self.schema),
            &self.table_definition.name().to_string(),
        )?;

        let partitioner = Arc::clone(&self.partitioner);

        let upsert_options = self.upsert_options.clone();

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            let batches = partitioner.partition_batch(&batch)?;

            for (partition_name, batch) in batches {
                let partition_batches = if let Some(constraints) = &self.constraints {
                    let deduped_batch = deduplicate_batch(&batch, constraints, &upsert_options)?;
                    // `validate_batch_with_constraints` now takes an owned `Vec<RecordBatch>` plus
                    // an `&datafusion_table_providers::util::constraints::UpsertOptions`
                    // (datafusion-table-providers `sgrebnov/spiceai-53`). Map our local
                    // (field-identical) `UpsertOptions` to that type; the call is a constraint check
                    // (dedup already applied above) so its returned batches are discarded.
                    let tp_upsert_options =
                        datafusion_table_providers::util::constraints::UpsertOptions::default()
                            .with_remove_duplicates(upsert_options.remove_duplicates)
                            .with_last_write_wins(upsert_options.last_write_wins);
                    datafusion_table_providers::util::constraints::validate_batch_with_constraints(
                        vec![deduped_batch.clone()],
                        constraints,
                        &tp_upsert_options,
                    )
                    .await
                    .context(ConstraintViolationSnafu)
                    .map_err(to_datafusion_error)?;
                    vec![deduped_batch]
                } else {
                    vec![batch]
                };

                if let Err(send_error) = partition_buffer
                    .process(partition_name, partition_batches)
                    .await
                {
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
                            )));
                        }
                    };
                }
            }
        }

        // Flush buffers and drop the sender to signal the receiver that no more data is coming
        if let Err(send_error) = partition_buffer.finish().await {
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
                    )));
                }
            };
        }

        if notify_commit_transaction.send(()).is_err() {
            return Err(DataFusionError::Execution(
                "Unable to send message to commit transaction to DuckDB writer.".to_string(),
            ));
        }

        match duckdb_write_handle.await {
            Ok(result) => result,
            Err(e) => Err(DataFusionError::Execution(format!(
                "Error writing to DuckDB: {e}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
struct PartitionTableManager {
    definition_name: String,
    table_name: String,
    schema: SchemaRef,
    constraints: Option<Constraints>,
    indexes: Vec<(ColumnReference, IndexType)>,
}

impl PartitionTableManager {
    fn new(
        definition_name: String,
        schema: SchemaRef,
        constraints: Option<Constraints>,
        indexes: Vec<(ColumnReference, IndexType)>,
    ) -> Self {
        Self {
            table_name: definition_name.clone(),
            definition_name,
            schema,
            constraints,
            indexes,
        }
    }

    fn from_table_name(
        definition_name: String,
        table_name: String,
        schema: SchemaRef,
        constraints: Option<Constraints>,
        indexes: Vec<(ColumnReference, IndexType)>,
    ) -> Self {
        Self {
            definition_name,
            table_name,
            schema,
            constraints,
            indexes,
        }
    }

    fn with_internal(mut self, is_internal: bool) -> Result<Self> {
        if is_internal {
            self.table_name = self.generate_internal_name()?;
        }
        Ok(self)
    }

    fn definition_name(&self) -> &str {
        &self.definition_name
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn generate_internal_name(&self) -> Result<String> {
        let suffix = unique_name_suffix()?;
        Ok(format!(
            "__data_{table_name}_{suffix}",
            table_name = self.definition_name
        ))
    }

    fn create_table(&self, pool: Arc<DuckDbConnectionPool>, tx: &Transaction<'_>) -> Result<()> {
        let mut db_conn = pool.connect_sync().context(DbConnectionPoolSnafu)?;
        let duckdb_conn =
            DuckDB::duckdb_conn(&mut db_conn).map_err(|source| Error::DbConnectionPool {
                source: Box::new(source),
            })?;

        let mut create_stmt = self.get_table_create_statement(duckdb_conn)?;
        let primary_keys = self
            .constraints
            .as_ref()
            .map(|constraints| get_primary_keys_from_constraints(constraints, &self.schema))
            .unwrap_or_default();

        if !primary_keys.is_empty() && !create_stmt.contains("PRIMARY KEY") {
            let primary_key_clause = format!(", PRIMARY KEY ({}));", primary_keys.join(", "));
            create_stmt = create_stmt.replace(");", &primary_key_clause);
        }

        tx.execute(&create_stmt, [])
            .context(UnableToCreateDuckDBTableSnafu)?;
        Ok(())
    }

    fn get_table_create_statement(&self, duckdb_conn: &mut DuckDbConnection) -> Result<String> {
        let tx = duckdb_conn
            .conn
            .transaction()
            .context(UnableToBeginTransactionSnafu)?;
        let empty_batch = RecordBatch::new_empty(Arc::clone(&self.schema));
        let record_batch_reader = arrow::array::RecordBatchIterator::new(
            vec![empty_batch].into_iter().map(Ok),
            Arc::clone(&self.schema),
        );
        let stream = FFI_ArrowArrayStream::new(Box::new(record_batch_reader));

        let suffix = unique_name_suffix()?;
        let view_name = format!(
            "__scan_{}_{suffix}",
            sanitize_identifier_fragment(self.table_name())
        );
        tx.register_arrow_scan_view(&view_name, &stream)
            .context(UnableToRegisterArrowScanViewForTableCreationSnafu)?;

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {view_name}",
            table_name = quote_identifier(self.table_name()),
            view_name = quote_identifier(&view_name)
        );
        tx.execute(&sql, [])
            .context(UnableToCreateDuckDBTableSnafu)?;

        let create_stmt = tx
            .query_row(
                "SELECT sql FROM duckdb_tables() WHERE table_name = ?",
                [self.table_name()],
                |row| row.get::<usize, String>(0),
            )
            .context(UnableToQueryDataSnafu)?;

        let create_stmt = create_stmt.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
        tx.rollback().context(UnableToRollbackTransactionSnafu)?;

        Ok(create_stmt)
    }

    fn list_other_internal_tables(&self, tx: &Transaction<'_>) -> Result<Vec<(Self, u64)>> {
        let pattern = format!("__data_{}%", self.definition_name());
        let mut stmt = tx
            .prepare("SELECT table_name FROM duckdb_tables() WHERE table_name LIKE ?")
            .context(UnableToQueryDataSnafu)?;
        let mut rows = stmt.query([pattern]).context(UnableToQueryDataSnafu)?;

        let mut tables = Vec::new();
        while let Some(row) = rows.next().context(UnableToQueryDataSnafu)? {
            let table_name: String = row.get(0).context(UnableToQueryDataSnafu)?;
            if table_name == self.table_name {
                continue;
            }
            let Some(inner_name) = table_name.strip_prefix("__data_") else {
                continue;
            };
            let Some((inner_table_name, timestamp)) = inner_name.rsplit_once('_') else {
                continue;
            };
            if inner_table_name != self.definition_name() {
                continue;
            }
            let Ok(timestamp) = timestamp.parse::<u64>() else {
                continue;
            };
            tables.push((
                Self::from_table_name(
                    self.definition_name.clone(),
                    table_name,
                    Arc::clone(&self.schema),
                    self.constraints.clone(),
                    self.indexes.clone(),
                ),
                timestamp,
            ));
        }

        tables.sort_by_key(|left| left.1);
        Ok(tables)
    }

    fn delete_table(&self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute(
            &format!(
                "DROP TABLE IF EXISTS {}",
                quote_identifier(self.table_name())
            ),
            [],
        )
        .context(UnableToDropDuckDBTableSnafu)?;
        Ok(())
    }

    fn create_view(&self, tx: &Transaction<'_>) -> Result<()> {
        if self.table_name == self.definition_name {
            return Ok(());
        }

        tx.execute(
            &format!(
                "CREATE OR REPLACE VIEW {base_table} AS SELECT * FROM {internal_table}",
                base_table = quote_identifier(self.definition_name()),
                internal_table = quote_identifier(self.table_name())
            ),
            [],
        )
        .context(UnableToCreateDuckDBTableSnafu)?;
        Ok(())
    }

    fn current_indexes(&self, tx: &Transaction<'_>) -> Result<HashSet<String>> {
        let mut stmt = tx
            .prepare("SELECT index_name FROM duckdb_indexes() WHERE table_name = ?")
            .context(UnableToQueryDataSnafu)?;
        let mut rows = stmt
            .query([self.table_name()])
            .context(UnableToQueryDataSnafu)?;

        let mut indexes = HashSet::new();
        while let Some(row) = rows.next().context(UnableToQueryDataSnafu)? {
            indexes.insert(
                row.get::<usize, String>(0)
                    .context(UnableToQueryDataSnafu)?,
            );
        }
        Ok(indexes)
    }

    fn create_indexes(&self, tx: &Transaction<'_>) -> Result<()> {
        for (index_index, (columns, index_type)) in self.indexes.iter().enumerate() {
            let unique = if *index_type == IndexType::Unique {
                "UNIQUE "
            } else {
                ""
            };
            let index_name = self.index_name(index_index, columns);
            let columns = columns
                .iter()
                .map(quote_identifier)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "CREATE {unique}INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})",
                index_name = quote_identifier(&index_name),
                table_name = quote_identifier(self.table_name())
            );
            tx.execute(&sql, [])
                .context(UnableToCreateIndexOnDuckDBTableSnafu)?;
        }
        Ok(())
    }

    fn index_name(&self, index_index: usize, columns: &ColumnReference) -> String {
        let columns = columns.iter().collect::<Vec<_>>().join("_");
        format!(
            "idx_{}_{}_{}",
            sanitize_identifier_fragment(self.table_name()),
            index_index,
            sanitize_identifier_fragment(&columns)
        )
    }
}

fn sanitize_identifier_fragment(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect()
}

fn execute_analyze_sql(tx: &Transaction<'_>, table_name: &str) {
    let sql = format!("ANALYZE {}", quote_identifier(table_name));
    if let Err(error) = tx.execute(&sql, []) {
        tracing::warn!("Failed to analyze DuckDB table {table_name}: {error}");
    }
}

impl DuckDBPartitionedDataSink {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        upsert_options: UpsertOptions,
        constraints: Option<Constraints>,
        indexes: Vec<(ColumnReference, IndexType)>,
        schema: SchemaRef,
        partitioner: Arc<BatchPartitioner>,
    ) -> Self {
        Self {
            pool,
            table_definition,
            overwrite,
            on_conflict,
            upsert_options,
            constraints,
            indexes,
            schema,
            partitioner,
            write_settings: DuckDBWriteSettings::default(),
            partition_buffer_config: PartitionBufferConfig::default(),
        }
    }

    /// Sets the write settings for controlling `DuckDB` write behavior.
    ///
    /// # Arguments
    /// * `write_settings` - `DuckDB` write settings including ANALYZE control
    #[must_use]
    pub fn with_write_settings(mut self, write_settings: DuckDBWriteSettings) -> Self {
        self.write_settings = write_settings;
        self
    }

    pub fn with_partition_buffer_config(mut self, buffer_config: PartitionBufferConfig) -> Self {
        self.partition_buffer_config = buffer_config;
        self
    }
}

impl std::fmt::Debug for DuckDBPartitionedDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBPartitionedDataSink")
    }
}

impl DisplayAs for DuckDBPartitionedDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DuckDBPartitionedDataSink")
    }
}

#[expect(clippy::too_many_arguments, clippy::trivially_copy_pass_by_ref)]
fn insert_overwrite(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<(String, PartitionData)>,
    on_conflict: Option<&OnConflict>,
    on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: &SchemaRef,
    constraints: Option<Constraints>,
    indexes: &[(ColumnReference, IndexType)],
    write_settings: &DuckDBWriteSettings,
) -> datafusion::common::Result<u64> {
    let cloned_pool = Arc::clone(&pool);
    let mut db_conn = pool
        .connect_sync()
        .context(DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    // Snapshot all existing partition tables (main views) before writing new data,
    // so we can later drop any views and internal tables that are not present in the latest refresh.
    let mut candidates_to_drop =
        get_existing_partition_tables(&tx, table_definition, schema, constraints.clone(), indexes)?;

    tracing::debug!("Initial load for {}", table_definition.name());
    let (num_rows, tables) = write_to_tables(
        table_definition,
        &tx,
        schema,
        constraints,
        indexes,
        batch_rx,
        on_conflict,
        &cloned_pool,
        true,
    )
    .map_err(to_retriable_data_write_error)?;

    on_commit_transaction
        .blocking_recv()
        .map_err(to_retriable_data_write_error)?;

    for new_table in &tables {
        new_table
            .create_view(&tx)
            .map_err(to_retriable_data_write_error)?;

        // Delete old internal tables for this partitioned table
        new_table
            .list_other_internal_tables(&tx)
            .map_err(to_retriable_data_write_error)?
            .into_iter()
            .try_for_each(|(old_table, _)| {
                old_table
                    .delete_table(&tx)
                    .map_err(to_retriable_data_write_error)
            })?;

        if write_settings.recompute_statistics_on_write {
            execute_analyze_sql(&tx, new_table.table_name());
        }

        // partition still exists so should NOT be deleted
        candidates_to_drop.remove(new_table.definition_name());
    }

    // Drop obsolete partition tables that no longer exist after the latest full refresh.
    for view in candidates_to_drop.values() {
        drop_partition_view(view, &tx)?;
    }

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    tracing::debug!(
        "Load for table {table_name} complete, applying constraints and indexes.",
        table_name = table_definition.name()
    );

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)
        .map_err(to_datafusion_error)?;

    // Apply constraints and indexes. Since we create new internal tables for each full refresh,
    // we need to apply indexes after each refresh.
    for new_table in &tables {
        new_table
            .create_indexes(&tx)
            .map_err(to_retriable_data_write_error)?;
    }

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    Ok(num_rows)
}

#[expect(clippy::too_many_arguments, clippy::trivially_copy_pass_by_ref)]
fn insert_append(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<(String, PartitionData)>,
    on_conflict: Option<&OnConflict>,
    on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: &SchemaRef,
    constraints: Option<Constraints>,
    indexes: &[(ColumnReference, IndexType)],
    write_settings: &DuckDBWriteSettings,
) -> datafusion::common::Result<u64> {
    let cloned_pool = Arc::clone(&pool);
    let mut db_conn = pool
        .connect_sync()
        .context(DbConnectionPoolSnafu)
        .map_err(to_retriable_data_write_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_retriable_data_write_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    tracing::debug!(
        "Append load for {table_name}",
        table_name = table_definition.name()
    );

    let (num_rows, tables) = write_to_tables(
        table_definition,
        &tx,
        schema,
        constraints,
        indexes,
        batch_rx,
        on_conflict,
        &cloned_pool,
        false,
    )
    .map_err(to_retriable_data_write_error)?;

    if write_settings.recompute_statistics_on_write {
        for table in &tables {
            execute_analyze_sql(&tx, table.table_name());
        }
    }

    on_commit_transaction
        .blocking_recv()
        .map_err(to_retriable_data_write_error)?;

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    tracing::debug!(
        "Load for table {table_name} complete, applying constraints and indexes.",
        table_name = table_definition.name()
    );

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)
        .map_err(to_datafusion_error)?;

    // During append refresh, we only need to create indexes on new partition tables,
    // so we check if the table has any existing indexes and only create indexes if it doesn't have any.
    for new_table in &tables {
        let has_indexes = !new_table
            .current_indexes(&tx)
            .map_err(to_retriable_data_write_error)?
            .is_empty();

        // Add logic to verify that existing indexes match required configuration
        // https://github.com/spiceai/spiceai/issues/7590
        if has_indexes {
            continue;
        }

        new_table
            .create_indexes(&tx)
            .map_err(to_retriable_data_write_error)?;
    }

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    Ok(num_rows)
}

#[expect(clippy::too_many_arguments, clippy::needless_pass_by_value)]
fn write_to_tables(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,
    schema: &SchemaRef,
    constraints: Option<Constraints>,
    indexes: &[(ColumnReference, IndexType)],
    mut data_batches: Receiver<(String, PartitionData)>,
    on_conflict: Option<&OnConflict>,
    pool: &Arc<DuckDbConnectionPool>,
    with_internal: bool,
) -> datafusion::common::Result<(u64, Vec<Arc<PartitionTableManager>>)> {
    let mut total_rows = 0u64;

    let start_main = SystemTime::now();

    // Track which partitions have already been created to avoid duplicate table creation and return back
    let mut created_partitions: HashMap<String, Arc<PartitionTableManager>> = HashMap::new();

    tracing::debug!(
        "Starting partitioned table writes for {}",
        table_definition.name()
    );

    while let Some((partition, data)) = data_batches.blocking_recv() {
        let start = SystemTime::now();

        // Check if partition table already exists or create it
        let partition_table = if let Some(existing_table) = created_partitions.get(&partition) {
            Arc::clone(existing_table)
        } else {
            // Create new partition table
            let partition_table_name = format!("{partition}/{}", table_definition.name());
            let partition_table = Arc::new(
                PartitionTableManager::new(
                    partition_table_name,
                    Arc::clone(schema),
                    constraints.clone(),
                    indexes.to_vec(),
                )
                .with_internal(with_internal)
                .map_err(to_datafusion_error)?,
            );

            partition_table
                .create_table(Arc::clone(pool), tx)
                .map_err(to_datafusion_error)?;

            created_partitions.insert(partition.clone(), Arc::clone(&partition_table));
            partition_table
        };

        let rows_written = match data {
            PartitionData::Batches(records) => write_data_chunk_to_table(
                &partition_table,
                tx,
                Arc::clone(schema),
                records,
                on_conflict,
            )?,
            PartitionData::ParquetFile(file_path) => {
                write_parquet_file_to_table(&partition_table, tx, &file_path)?
            }
        };

        total_rows += rows_written;

        let elapsed = start
            .elapsed()
            .context(UnableToGetElapsedTimeSnafu)
            .map_err(to_datafusion_error)?;
        let secs = elapsed.as_secs_f64();
        #[expect(clippy::cast_precision_loss)]
        let rps = if secs > 0.0 {
            (rows_written as f64) / secs
        } else {
            rows_written as f64
        };
        tracing::trace!("Processed {rows_written} rows in {elapsed:?} ({rps:.2} rows/s)");
    }

    let total_elapsed = start_main
        .elapsed()
        .context(UnableToGetElapsedTimeSnafu)
        .map_err(to_datafusion_error)?;

    tracing::debug!(
        "Completed partitioned writes; created {} partition tables, total rows: {}, elapsed time: {:?}",
        created_partitions.len(),
        total_rows,
        total_elapsed
    );

    Ok((total_rows, created_partitions.into_values().collect()))
}

#[expect(clippy::needless_pass_by_value)]
fn write_data_chunk_to_table(
    table: &PartitionTableManager,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    on_conflict: Option<&OnConflict>,
) -> datafusion::common::Result<u64> {
    let batch_reader =
        arrow::array::RecordBatchIterator::new(batches.into_iter().map(Ok), Arc::clone(&schema));
    let stream = FFI_ArrowArrayStream::new(Box::new(batch_reader));

    let suffix = unique_name_suffix().map_err(to_datafusion_error)?;

    let view_name = format!(
        "__scan_{}_{suffix}",
        sanitize_identifier_fragment(table.table_name())
    );

    tx.register_arrow_scan_view(&view_name, &stream)
        .context(UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    let mut insert_sql = format!(
        "INSERT INTO {table_name} SELECT * FROM {view_name}",
        table_name = quote_identifier(table.table_name()),
        view_name = quote_identifier(&view_name)
    );
    if let Some(on_conflict) = on_conflict {
        let on_conflict_sql = on_conflict.build_on_conflict_statement(&schema);
        insert_sql.push(' ');
        insert_sql.push_str(&on_conflict_sql);
    }
    let rows = tx
        .execute(&insert_sql, [])
        .context(UnableToCreateDuckDBTableSnafu)
        .map_err(to_datafusion_error)?;

    tx.execute(
        &format!("DROP VIEW IF EXISTS {}", quote_identifier(&view_name)),
        [],
    )
    .context(UnableToDropDuckDBTableSnafu)
    .map_err(to_datafusion_error)?;

    Ok(rows as u64)
}

/// Inserts data from a Parquet file into a partition table.
///
/// Note: does not currently support conflict resolutions
///
/// # Returns
/// The number of rows inserted from the Parquet file
///
/// # Errors
/// Returns a `DataFusion` error if the SQL execution fails or if the file cannot be read
fn write_parquet_file_to_table(
    table: &PartitionTableManager,
    tx: &Transaction<'_>,
    file_path: &std::path::Path,
) -> datafusion::common::Result<u64> {
    let sql = format!(
        "INSERT INTO {table_name} SELECT * FROM read_parquet(?, hive_partitioning=false)",
        table_name = quote_identifier(table.table_name())
    );

    let file_path_str = file_path.to_string_lossy();
    let rows_written = tx.execute(&sql, [&*file_path_str]).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to insert from parquet file '{}': {e}",
            file_path.display()
        ))
    })? as u64;

    // Clean up the temporary file after successful insertion
    if let Err(e) = std::fs::remove_file(file_path) {
        tracing::warn!(
            "Failed to remove temporary parquet file '{}': {e}",
            file_path.display(),
        );
    }
    Ok(rows_written)
}

/// Gets all existing partition tables for a given base table definition.
#[expect(clippy::needless_pass_by_value)]
fn get_existing_partition_tables(
    tx: &Transaction<'_>,
    base_table_definition: &Arc<TableDefinition>,
    schema: &SchemaRef,
    constraints: Option<Constraints>,
    indexes: &[(ColumnReference, IndexType)],
) -> datafusion::common::Result<HashMap<String, PartitionTableManager>> {
    let base_table_name = base_table_definition.name();

    let pattern = format!("%/{base_table_name}");
    let mut stmt = tx
        .prepare("SELECT table_name FROM information_schema.tables WHERE table_name LIKE ?1")
        .map_err(to_retriable_data_write_error)?;

    let mut existing_partitions = HashMap::new();
    let mut rows = stmt
        .query([&pattern])
        .map_err(to_retriable_data_write_error)?;

    while let Some(row) = rows.next().map_err(to_retriable_data_write_error)? {
        let table_name: String = row.get(0).map_err(to_retriable_data_write_error)?;
        existing_partitions.insert(
            table_name.clone(),
            PartitionTableManager::new(
                table_name,
                Arc::clone(schema),
                constraints.clone(),
                indexes.to_vec(),
            ),
        );
    }

    Ok(existing_partitions)
}

/// Drops a partition view used by full refresh and all its associated internal tables.
///
/// # Arguments
/// * `view` - The partition view to drop. This should be the view itself, not an internal table used by the view.
/// * `tx` - The active `DuckDB` transaction used to execute the drop operations.
///
/// # Errors
/// Returns an error if any internal table or the view cannot be dropped.
fn drop_partition_view(
    view: &PartitionTableManager,
    tx: &Transaction<'_>,
) -> datafusion::common::Result<()> {
    tracing::debug!(
        "Dropping partitioned table {name}",
        name = view.table_name()
    );

    // First drop internal tables
    for (old_table, _) in view
        .list_other_internal_tables(tx)
        .map_err(to_retriable_data_write_error)?
    {
        old_table
            .delete_table(tx)
            .map_err(to_retriable_data_write_error)?;
    }

    tx.execute(
        &format!(
            "DROP VIEW IF EXISTS {}",
            quote_identifier(view.table_name())
        ),
        [],
    )
    .map_err(to_retriable_data_write_error)?;

    Ok(())
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

#[cfg(test)]
mod test {
    use crate::dataaccelerator::partitioned_duckdb::tables_mode::partition_buffer::config::PartitionBufferType;

    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::catalog::TableProviderFactory;
    use datafusion::common::{TableReference, ToDFSchema};
    use datafusion::execution::TaskContext;
    use datafusion::execution::context::SessionContext;
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::col;
    use datafusion_table_providers::duckdb::write::DuckDBTableWriter;
    use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
    use runtime_table_partition::expression::PartitionedBy;
    use std::ops::Deref;
    use std::thread;

    fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory().expect("to get a memory duckdb connection pool"),
        )
    }

    struct TestTableDefinition {
        definition: Arc<TableDefinition>,
        schema: SchemaRef,
    }

    impl TestTableDefinition {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    impl Deref for TestTableDefinition {
        type Target = Arc<TableDefinition>;

        fn deref(&self) -> &Self::Target {
            &self.definition
        }
    }

    fn get_test_schema() -> SchemaRef {
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, false),
        ]))
    }

    async fn get_test_table_definition() -> TestTableDefinition {
        let schema = get_test_schema();
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema))
            .expect("to convert Arrow schema to DataFusion schema");
        let cmd = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            or_replace: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let factory = crate::dataaccelerator::duckdb::create_factory();
        let ctx = SessionContext::new();
        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("to create DuckDB table provider");
        let writer = table_provider
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .expect("DuckDB table provider should be a writer");

        TestTableDefinition {
            definition: writer.table_definition(),
            schema,
        }
    }

    fn make_partition_batch(schema: &SchemaRef, region: &str, ids: &[i64]) -> RecordBatch {
        let id_values: Vec<Option<i64>> = ids.iter().copied().map(Some).collect();
        let region_values: Vec<Option<&str>> = ids.iter().map(|_| Some(region)).collect();

        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(id_values)),
                Arc::new(StringArray::from(region_values)),
            ],
        )
        .expect("should create a record batch")
    }

    fn verify_state_after_write(
        tx: &Transaction<'_>,
        table_definition: &TestTableDefinition,
        target_partitions: &[&str],
        expected_rows_per_partition: i64,
        should_have_internal_tables: bool,
    ) {
        for partition in target_partitions {
            let partition_table_name = format!("{partition}/{}", table_definition.name());
            let partitioned_table = PartitionTableManager::new(
                partition_table_name,
                table_definition.schema(),
                None,
                Vec::new(),
            );

            // Verify that partitioned tables were created (one for each region)
            let mut internal_tables = partitioned_table
                .list_other_internal_tables(tx)
                .expect("to list internal tables");

            if should_have_internal_tables {
                assert_eq!(
                    internal_tables.len(),
                    1,
                    "Expected partitioned internal table to be created"
                );
                let table_name = internal_tables.pop().expect("should have a table").0;

                // Verify that data was written to a partitioned table
                let rows = tx
                    .query_row(
                        &format!("SELECT COUNT(1) FROM \"{}\"", table_name.table_name()),
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .expect("to get count");
                assert_eq!(
                    rows, expected_rows_per_partition,
                    "Expected {expected_rows_per_partition} rows in partitioned table"
                );
            } else {
                assert_eq!(
                    internal_tables.len(),
                    0,
                    "Expected no internal tables for append mode"
                );
            }

            // Verify a view was created for partitioned table
            let view_rows = tx
                .query_row(
                    &format!(
                        "SELECT COUNT(1) FROM \"{view_name}\"",
                        view_name = partitioned_table.definition_name()
                    ),
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("to get count");

            assert_eq!(
                view_rows, expected_rows_per_partition,
                "Expected view to have {expected_rows_per_partition} rows from a partitioned table"
            );
        }
    }

    fn verify_partition_does_not_exist(
        tx: &Transaction<'_>,
        table_definition: &TestTableDefinition,
        partition_name: &str,
        with_internal: bool,
    ) {
        let partition_table_name = format!("{partition_name}/{}", table_definition.name());
        let partitioned_table = PartitionTableManager::new(
            partition_table_name,
            table_definition.schema(),
            None,
            Vec::new(),
        );

        if with_internal {
            let internal_tables = partitioned_table
                .list_other_internal_tables(tx)
                .expect("to list internal tables");

            assert_eq!(
                internal_tables.len(),
                0,
                "Expected no internal tables for partition {partition_name}"
            );
        }

        let main_table_exists_result = tx.query_row(
            "SELECT COUNT(1) FROM information_schema.tables WHERE table_name = ?1",
            [partitioned_table.definition_name().to_string()],
            |row| row.get::<_, i64>(0),
        );

        match main_table_exists_result {
            Ok(count) => assert_eq!(
                count, 0,
                "Expected view or main table for partition {partition_name} to be removed"
            ),
            Err(e) => panic!(
                "Failed to check if main table or view exists for partition {partition_name}: {e}"
            ),
        }
    }

    #[test]
    fn sanitize_identifier_fragment_replaces_partition_separators() {
        assert_eq!(
            sanitize_identifier_fragment("region=us-east-1/test_table"),
            "region_us_east_1_test_table"
        );
    }

    #[test]
    fn unique_name_suffix_does_not_collide_for_fast_calls() {
        let suffixes = (0..256)
            .map(|_| unique_name_suffix().expect("suffix generation should succeed"))
            .collect::<HashSet<_>>();

        assert_eq!(suffixes.len(), 256);
    }

    #[test]
    fn current_indexes_queries_duckdb_indexes_table_function() {
        let pool = get_mem_duckdb();
        let schema = get_test_schema();
        let table_name = "region=us-east-1/test_table";
        let mut conn = pool.connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        tx.execute(
            &format!(
                "CREATE TABLE {} (id BIGINT, region VARCHAR)",
                quote_identifier(table_name)
            ),
            [],
        )
        .expect("to create table");
        tx.execute(
            &format!(
                "CREATE INDEX {} ON {} (id)",
                quote_identifier("idx_current_indexes_test"),
                quote_identifier(table_name)
            ),
            [],
        )
        .expect("to create index");

        let table = PartitionTableManager::new(table_name.to_string(), schema, None, Vec::new());
        let indexes = table.current_indexes(&tx).expect("to query indexes");

        assert!(indexes.contains("idx_current_indexes_test"));
    }

    #[tokio::test]
    async fn test_write_overwrite() {
        // Test scenario:
        // 1. Write to a table with overwrite mode without a previous table
        // 2. Write to the same table again with overwrite mode, simulating an existing table
        // Expected behavior: Data sink creates partitioned tables, writes data to them, and creates views, old internal tables are deleted
        let pool = get_mem_duckdb();

        let table_definition = get_test_table_definition().await;

        // Create partitioner by name - partition by "region" column
        let partitioned_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let partitioner = Arc::new(
            BatchPartitioner::new(
                &partitioned_by.expression,
                table_definition.schema(),
                &partitioned_by,
            )
            .expect("should create partitioner"),
        );

        let duckdb_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            UpsertOptions::default(),
            None,
            Vec::new(),
            table_definition.schema(),
            partitioner,
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // Test data with two different regions to create two partitions
        // id, region
        // 1, "us-east-1"
        // 2, "us-west-1"
        // 3, "us-east-1"
        // 4, "us-west-1"
        let batches = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        verify_state_after_write(
            &tx,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            2,
            true,
        );

        tx.rollback().expect("to rollback");

        // Simulate writing again with overwrite mode, which should delete old internal tables
        // Second batch has 1 row per partition (2 total rows)
        let batches2 = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream2: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches2, table_definition.schema(), None)
                .expect("to get stream"),
        );

        data_sink
            .write_all(stream2, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn2 = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb2 = DuckDB::duckdb_conn(&mut conn2).expect("to get duckdb conn");
        let tx2 = duckdb2.conn.transaction().expect("to begin transaction");

        verify_state_after_write(
            &tx2,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            1,
            true,
        );

        tx2.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_overwrite_partition_removal() {
        // Test scenario:
        // 1. Write to a table with overwrite mode creating two partitions
        // 2. Write to the same table again with overwrite mode but only one partition
        // Expected behavior: Old partition table should be removed, only new partition should exist
        let pool = get_mem_duckdb();

        let table_definition = get_test_table_definition().await;

        // Create partitioner by name - partition by "region" column
        let partitioned_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let partitioner = Arc::new(
            BatchPartitioner::new(
                &partitioned_by.expression,
                table_definition.schema(),
                &partitioned_by,
            )
            .expect("should create partitioner"),
        );

        let duckdb_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            UpsertOptions::default(),
            None,
            Vec::new(),
            table_definition.schema(),
            partitioner,
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // First write: Test data with two different regions to create two partitions
        // id, region
        // 1, "us-east-1"
        // 2, "us-west-1"
        // 3, "us-east-1"
        // 4, "us-west-1"
        let batches1 = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream1: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches1, table_definition.schema(), None)
                .expect("to get stream"),
        );

        data_sink
            .write_all(stream1, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn1 = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb1 = DuckDB::duckdb_conn(&mut conn1).expect("to get duckdb conn");
        let tx1 = duckdb1.conn.transaction().expect("to begin transaction");

        // Verify both partitions were created
        verify_state_after_write(
            &tx1,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            2,
            true,
        );

        tx1.rollback().expect("to rollback");

        // Second write: Only write data for one partition (us-east-1)
        let batches2 = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(10), Some(11)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-east-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream2: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches2, table_definition.schema(), None)
                .expect("to get stream"),
        );

        data_sink
            .write_all(stream2, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn2 = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb2 = DuckDB::duckdb_conn(&mut conn2).expect("to get duckdb conn");
        let tx2 = duckdb2.conn.transaction().expect("to begin transaction");

        // Verify only the us-east-1 partition exists with 2 rows
        verify_state_after_write(&tx2, &table_definition, &["region=us-east-1"], 2, true);

        // Verify that the us-west-1 partition table was removed
        verify_partition_does_not_exist(&tx2, &table_definition, "region=us-west-1", true);

        tx2.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_insert_overwrite_waits_for_commit_signal() {
        let pool = get_mem_duckdb();
        let table_definition = get_test_table_definition().await;
        let schema = table_definition.schema();

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(2);
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();

        let batch = make_partition_batch(&schema, "us-east-1", &[1, 2]);

        batch_tx
            .send((
                "region=us-east-1".to_string(),
                PartitionData::Batches(vec![batch]),
            ))
            .await
            .expect("to send partition batch");
        drop(batch_tx);

        let write_settings = DuckDBWriteSettings::default();

        let handle = thread::spawn({
            let pool = Arc::clone(&pool);
            let table_definition = Arc::clone(&table_definition);
            let schema = Arc::clone(&schema);

            move || {
                insert_overwrite(
                    pool,
                    &table_definition,
                    batch_rx,
                    None,
                    commit_rx,
                    &schema,
                    None,
                    &[],
                    &write_settings,
                )
            }
        });

        commit_tx.send(()).expect("to send commit signal");

        let rows = handle
            .join()
            .expect("insert thread to finish")
            .expect("insert_overwrite to succeed");

        assert_eq!(rows, 2, "expected rows to be written after commit signal");
    }

    #[tokio::test]
    async fn test_insert_append_waits_for_commit_signal() {
        let pool = get_mem_duckdb();
        let table_definition = get_test_table_definition().await;
        let schema = table_definition.schema();

        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(2);
        let (commit_tx, commit_rx) = tokio::sync::oneshot::channel();

        let batch = make_partition_batch(&schema, "us-west-1", &[10, 11, 12]);

        batch_tx
            .send((
                "region=us-west-1".to_string(),
                PartitionData::Batches(vec![batch]),
            ))
            .await
            .expect("to send partition batch");
        drop(batch_tx);

        let write_settings = DuckDBWriteSettings::default();

        let handle = thread::spawn({
            let pool = Arc::clone(&pool);
            let table_definition = Arc::clone(&table_definition);
            let schema = Arc::clone(&schema);

            move || {
                insert_append(
                    pool,
                    &table_definition,
                    batch_rx,
                    None,
                    commit_rx,
                    &schema,
                    None,
                    &[],
                    &write_settings,
                )
            }
        });

        commit_tx.send(()).expect("to send commit signal");

        let rows = handle
            .join()
            .expect("insert thread to finish")
            .expect("insert_append to succeed");

        assert_eq!(rows, 3, "expected rows to be written after commit signal");
    }

    #[tokio::test]
    async fn test_write_append() {
        // Test scenario:
        // 1. Write to a table with append mode without a previous table
        // 2. Write to the same table again with append mode, simulating an existing table
        // Expected behavior: Data sink creates partitioned tables, writes data to them
        let pool = get_mem_duckdb();

        let table_definition = get_test_table_definition().await;

        // Create partitioner by name - partition by "region" column
        let partitioned_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let partitioner = Arc::new(
            BatchPartitioner::new(
                &partitioned_by.expression,
                table_definition.schema(),
                &partitioned_by,
            )
            .expect("should create partitioner"),
        );

        let duckdb_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Append,
            None,
            UpsertOptions::default(),
            None,
            Vec::new(),
            table_definition.schema(),
            partitioner,
        );
        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // Test data with two different regions to create two partitions
        // id, region
        // 1, "us-east-1"
        // 2, "us-west-1"
        // 3, "us-east-1"
        // 4, "us-west-1"
        let batches = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        verify_state_after_write(
            &tx,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            2,
            false,
        );

        tx.rollback().expect("to rollback");

        // Simulate writing again with append mode, which should append data to existing tables
        // Second batch has 1 row per partition (2 total rows)
        let batches2 = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(5), Some(6)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream2: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches2, table_definition.schema(), None)
                .expect("to get stream"),
        );

        data_sink
            .write_all(stream2, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all");

        let mut conn2 = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb2 = DuckDB::duckdb_conn(&mut conn2).expect("to get duckdb conn");
        let tx2 = duckdb2.conn.transaction().expect("to begin transaction");

        tx2.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_write_overwrite_with_parquet_buffer() {
        // Test scenario: Use parquet buffer instead of memory buffer
        // Expected behavior: Data sink creates partitioned tables using parquet files as intermediate storage
        let pool = get_mem_duckdb();
        let table_definition = get_test_table_definition().await;

        // Create partitioner by name - partition by "region" column
        let partitioned_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let partitioner = Arc::new(
            BatchPartitioner::new(
                &partitioned_by.expression,
                table_definition.schema(),
                &partitioned_by,
            )
            .expect("should create partitioner"),
        );

        // Configure parquet buffer with small threshold for testing
        let parquet_buffer_config = PartitionBufferConfig {
            buffer_type: PartitionBufferType::Parquet,
            rows_per_partition_threshold: 1000,
            temp_dir: std::env::temp_dir().join("spice_test_parquet_buffer"),
        };

        let duckdb_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            UpsertOptions::default(),
            None,
            Vec::new(),
            table_definition.schema(),
            partitioner,
        )
        .with_partition_buffer_config(parquet_buffer_config);

        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // Test data with two different regions to create two partitions
        let batches = vec![
            RecordBatch::try_new(
                Arc::clone(&table_definition.schema()),
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3), Some(4)])),
                    Arc::new(StringArray::from(vec![
                        Some("us-east-1"),
                        Some("us-west-1"),
                        Some("us-east-1"),
                        Some("us-west-1"),
                    ])),
                ],
            )
            .expect("should create a record batch"),
        ];

        let stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all with parquet buffer");

        let mut conn = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        verify_state_after_write(
            &tx,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            2,
            true,
        );

        tx.rollback().expect("to rollback");
    }

    #[tokio::test]
    async fn test_parquet_buffer_large_batch() {
        // Test scenario: Large batch that exceeds partition threshold multiple times with parquet buffer
        // Expected behavior: Multiple parquet files created and flushed to DuckDB
        let pool = get_mem_duckdb();
        let table_definition = get_test_table_definition().await;

        let partitioned_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let partitioner = Arc::new(
            BatchPartitioner::new(
                &partitioned_by.expression,
                table_definition.schema(),
                &partitioned_by,
            )
            .expect("should create partitioner"),
        );

        // Configure parquet buffer with very small threshold to force multiple flushes
        let parquet_buffer_config = PartitionBufferConfig {
            buffer_type: PartitionBufferType::Parquet,
            rows_per_partition_threshold: 700,
            temp_dir: std::env::temp_dir().join("spice_test_parquet_large"),
        };

        let duckdb_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&pool),
            Arc::clone(&table_definition),
            InsertOp::Overwrite,
            None,
            UpsertOptions::default(),
            None,
            Vec::new(),
            table_definition.schema(),
            partitioner,
        )
        .with_partition_buffer_config(parquet_buffer_config);

        let data_sink: Arc<dyn DataSink> = Arc::new(duckdb_sink);

        // Create a batch of 1000 records
        let ids: Vec<Option<i64>> = (1..=1000).map(Some).collect();
        let regions: Vec<Option<&str>> = (1..=1000)
            .map(|i| {
                if i % 2 == 0 {
                    Some("us-east-1")
                } else {
                    Some("us-west-1")
                }
            })
            .collect();

        let base_batch = RecordBatch::try_new(
            Arc::clone(&table_definition.schema()),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .expect("should create a record batch");

        // Create multiple copies of the batch to exceed the partition threshold
        let batches = vec![
            base_batch.clone(),
            base_batch.clone(),
            base_batch.clone(),
            base_batch.clone(),
            base_batch,
        ];

        let stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(
            MemoryStream::try_new(batches, table_definition.schema(), None).expect("to get stream"),
        );

        data_sink
            .write_all(stream, &Arc::new(TaskContext::default()))
            .await
            .expect("to write all with parquet buffer");

        let mut conn = Arc::clone(&pool).connect_sync().expect("to connect");
        let duckdb = DuckDB::duckdb_conn(&mut conn).expect("to get duckdb conn");
        let tx = duckdb.conn.transaction().expect("to begin transaction");

        verify_state_after_write(
            &tx,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            2500,
            true,
        );

        tx.rollback().expect("to rollback");
    }
}
