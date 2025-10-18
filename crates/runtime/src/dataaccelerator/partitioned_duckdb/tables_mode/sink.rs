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
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, metrics::MetricsSet},
};
use datafusion_table_providers::duckdb::{
    DuckDB, RelationName, TableDefinition, TableManager, ViewCreator,
};
use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
use datafusion_table_providers::util::on_conflict::OnConflict;
use datafusion_table_providers::util::retriable_error::{
    check_and_mark_retriable_error, to_retriable_data_write_error,
};
use duckdb::Transaction;
use futures::StreamExt;
use snafu::prelude::*;
use std::collections::HashMap;
use std::time::SystemTime;
use std::{any::Any, fmt, sync::Arc};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

use crate::dataaccelerator::partitioned_duckdb::tables_mode::insert::BatchPartitioner;
use crate::dataaccelerator::partitioned_duckdb::tables_mode::partition_buffer::PartitionBuffer;

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

    #[snafu(display("Failed to get system time since epoch: {source}"))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Failed to get elapsed time: {source}"))]
    UnableToGetElapsedTime { source: std::time::SystemTimeError },
}

// Bufferring rows allows for much more efficient writes in DuckDB
// 122_880 represents DuckDB default size of groups of rows - that are stored together at the storage level.
const ROWS_PER_PARTITION_BUFFER: usize = 122_880;

#[derive(Clone)]
pub struct DuckDBPartitionedDataSink {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    overwrite: InsertOp,
    on_conflict: Option<OnConflict>,
    schema: SchemaRef,
    partitioner: Arc<BatchPartitioner>,
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

        let (batch_tx, batch_rx): (
            Sender<(String, Vec<RecordBatch>)>,
            Receiver<(String, Vec<RecordBatch>)>,
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
                    )?,
                    InsertOp::Append | InsertOp::Replace => insert_append(
                        pool,
                        &table_definition,
                        batch_rx,
                        on_conflict.as_ref(),
                        on_commit_transaction,
                        &schema,
                    )?,
                };

                Ok(num_rows)
            });

        // Buffering rows allows for much more efficient writes in DuckDB
        let mut partition_buffer = PartitionBuffer::new(batch_tx, ROWS_PER_PARTITION_BUFFER);

        let partitioner = Arc::clone(&self.partitioner);

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            let batches = partitioner.partition_batch(&batch)?;

            for (partition_name, batch) in batches {
                if let Err(send_error) = partition_buffer.process_batch(partition_name, batch).await
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

        if let Err(send_error) = partition_buffer.flush_all().await {
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

        // Drop the sender to signal the receiver that no more data is coming
        drop(partition_buffer);

        match duckdb_write_handle.await {
            Ok(result) => result,
            Err(e) => Err(DataFusionError::Execution(format!(
                "Error writing to DuckDB: {e}"
            ))),
        }
    }
}

impl DuckDBPartitionedDataSink {
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        overwrite: InsertOp,
        on_conflict: Option<OnConflict>,
        schema: SchemaRef,
        partitioner: Arc<BatchPartitioner>,
    ) -> Self {
        Self {
            pool,
            table_definition,
            overwrite,
            on_conflict,
            schema,
            partitioner,
        }
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

#[allow(clippy::too_many_lines)]
fn insert_overwrite(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: &SchemaRef,
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

    tracing::debug!("Initial load for {}", table_definition.name());
    let (num_rows, tables) = write_to_tables(
        table_definition,
        &tx,
        schema,
        batch_rx,
        on_conflict,
        &cloned_pool,
        true,
    )
    .map_err(to_retriable_data_write_error)?;

    on_commit_transaction
        .try_recv()
        .map_err(to_retriable_data_write_error)?;

    for new_table in &tables {
        new_table
            .create_view(&tx)
            .map_err(to_retriable_data_write_error)?;
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

    // Apply constraints and indexes.
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

fn insert_append(
    pool: Arc<DuckDbConnectionPool>,
    table_definition: &Arc<TableDefinition>,
    batch_rx: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    mut on_commit_transaction: tokio::sync::oneshot::Receiver<()>,
    schema: &SchemaRef,
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

    let (num_rows, _) = write_to_tables(
        table_definition,
        &tx,
        schema,
        batch_rx,
        on_conflict,
        &cloned_pool,
        false,
    )
    .map_err(to_retriable_data_write_error)?;

    on_commit_transaction
        .try_recv()
        .map_err(to_retriable_data_write_error)?;

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_retriable_data_write_error)?;

    Ok(num_rows)
}

fn write_to_tables(
    table_definition: &Arc<TableDefinition>,
    tx: &Transaction<'_>,
    schema: &SchemaRef,
    mut data_batches: Receiver<(String, Vec<RecordBatch>)>,
    on_conflict: Option<&OnConflict>,
    pool: &Arc<DuckDbConnectionPool>,
    with_internal: bool,
) -> datafusion::common::Result<(u64, Vec<Arc<TableManager>>)> {
    let mut total_rows = 0u64;

    let start_main = SystemTime::now();

    // Track which partitions have already been created to avoid duplicate table creation and return back
    let mut created_partitions: HashMap<String, Arc<TableManager>> = HashMap::new();

    tracing::debug!(
        "Starting partitioned table writes for {}",
        table_definition.name()
    );

    while let Some((partition, batch)) = data_batches.blocking_recv() {
        let start = SystemTime::now();
        let batch_size_mb = batch
            .iter()
            .map(arrow::array::RecordBatch::get_array_memory_size)
            .sum::<usize>()
            / (1024 * 1024);

        // Check if partition table already exists or create it
        let partition_table = if let Some(existing_table) = created_partitions.get(&partition) {
            Arc::clone(existing_table)
        } else {
            // Create new partition table
            let partition_table_name = format!("{partition}/{}", table_definition.name());
            let partition_table_def = Arc::new(TableDefinition::new(
                RelationName::new(partition_table_name.clone()),
                Arc::clone(schema),
            ));

            let partition_table = Arc::new(
                TableManager::new(partition_table_def)
                    .with_internal(with_internal)
                    .map_err(table_providers_duckdb_to_datafusion_error)?,
            );

            partition_table
                .create_table(Arc::clone(pool), tx)
                .map_err(table_providers_duckdb_to_datafusion_error)?;

            created_partitions.insert(partition.clone(), Arc::clone(&partition_table));
            partition_table
        };

        let rows_written = write_data_chunk_to_table(
            &partition_table,
            tx,
            Arc::clone(schema),
            batch,
            on_conflict,
        )?;

        total_rows += rows_written;

        let elapsed = start
            .elapsed()
            .context(UnableToGetElapsedTimeSnafu)
            .map_err(to_datafusion_error)?;
        let secs = elapsed.as_secs_f64();
        #[allow(clippy::cast_precision_loss)]
        let rps = if secs > 0.0 {
            (rows_written as f64) / secs
        } else {
            rows_written as f64
        };
        tracing::trace!(
            "Processed {rows_written} rows in {elapsed:?} ({rps:.2} rows/s, memory: {batch_size_mb:.2} MB)"
        );
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

fn write_data_chunk_to_table(
    table: &TableManager,
    tx: &Transaction<'_>,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    on_conflict: Option<&OnConflict>,
) -> datafusion::common::Result<u64> {
    let batch_reader = arrow::array::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    let stream = FFI_ArrowArrayStream::new(Box::new(batch_reader));

    let current_ts = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context(UnableToGetSystemTimeSnafu)
        .map_err(to_datafusion_error)?
        .as_millis();

    let view_name = format!("__scan_{}_{current_ts}", table.table_name());

    tx.register_arrow_scan_view(&view_name, &stream)
        .context(UnableToRegisterArrowScanViewSnafu)
        .map_err(to_datafusion_error)?;

    let view = ViewCreator::from_name(RelationName::new(view_name));
    let rows = view
        .insert_into(table, tx, on_conflict)
        .map_err(table_providers_duckdb_to_datafusion_error)?;

    view.drop(tx)
        .map_err(table_providers_duckdb_to_datafusion_error)?;

    Ok(rows as u64)
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn table_providers_duckdb_to_datafusion_error(
    error: datafusion_table_providers::duckdb::Error,
) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
