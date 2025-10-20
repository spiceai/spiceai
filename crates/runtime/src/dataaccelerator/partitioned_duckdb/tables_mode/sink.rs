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
use datafusion_table_providers::util::constraints::UpsertOptions;
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

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation {
        source: datafusion_table_providers::util::constraints::Error,
    },
}

// Buffering rows allows for much more efficient writes in `DuckDB`
// 122_880 represents DuckDB default size of groups of rows - that are stored together at the storage level.
const ROWS_PER_PARTITION_BUFFER: usize = 122_880;

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

    #[allow(clippy::too_many_lines)]
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

        let upsert_options = self
            .on_conflict
            .as_ref()
            .map_or_else(UpsertOptions::default, |conflict| {
                conflict.get_upsert_options()
            });

        while let Some(batch) = data.next().await {
            let batch = batch.map_err(check_and_mark_retriable_error)?;

            let batches = partitioner.partition_batch(&batch)?;

            for (partition_name, batch) in batches {
                let partition_batches = if let Some(constraints) =
                    self.table_definition.constraints()
                {
                    datafusion_table_providers::util::constraints::validate_batch_with_constraints(
                        vec![batch],
                        constraints,
                        &upsert_options,
                    )
                    .await
                    .context(ConstraintViolationSnafu)
                    .map_err(to_datafusion_error)?
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

/// Creates a new `TableDefinition` for a partition based on an existing table definition.
///
/// This helper function creates a new table definition with the specified name while
/// copying over indexes and constraints from the original table definition.
fn create_partition_table_definition(
    base_table_definition: &TableDefinition,
    partition_table_name: String,
) -> Arc<TableDefinition> {
    let mut partition_table_def = TableDefinition::new(
        RelationName::new(partition_table_name),
        base_table_definition.schema(),
    );

    // Copy indexes and constraints from the original table definition
    let indexes = base_table_definition.indexes();
    if !indexes.is_empty() {
        partition_table_def = partition_table_def.with_indexes(indexes.to_vec());
    }

    if let Some(constraints) = base_table_definition.constraints() {
        partition_table_def = partition_table_def.with_constraints(constraints.clone());
    }

    Arc::new(partition_table_def)
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

    let (num_rows, tables) = write_to_tables(
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
            let partition_table_def =
                create_partition_table_definition(table_definition, partition_table_name);

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

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::RecordBatchStream;
    use datafusion::physical_plan::memory::MemoryStream;
    use datafusion::prelude::col;
    use datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool;
    use runtime_table_partition::expression::PartitionedBy;

    fn get_mem_duckdb() -> Arc<DuckDbConnectionPool> {
        Arc::new(
            DuckDbConnectionPool::new_memory().expect("to get a memory duckdb connection pool"),
        )
    }

    fn get_test_table_definition() -> Arc<TableDefinition> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("region", arrow::datatypes::DataType::Utf8, false),
        ]));

        Arc::new(TableDefinition::new(
            RelationName::new("test_table"),
            schema,
        ))
    }

    fn verify_state_after_write(
        tx: &duckdb::Transaction,
        table_definition: &Arc<TableDefinition>,
        target_partitions: &[&str],
        expected_rows_per_partition: i64,
        should_have_internal_tables: bool,
    ) {
        for partition in target_partitions {
            let partition_table_name = format!("{partition}/{}", table_definition.name());

            let partitioned_table_definition = TableDefinition::new(
                RelationName::new(partition_table_name),
                Arc::clone(&table_definition.schema()),
            );

            // Verify that partitioned tables were created (one for each region)
            let mut internal_tables = partitioned_table_definition
                .list_internal_tables(tx)
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
                        &format!("SELECT COUNT(1) FROM \"{table_name}\"",),
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
                        view_name = partitioned_table_definition.name()
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

    #[tokio::test]
    async fn test_write_overwrite() {
        // Test scenario:
        // 1. Write to a table with overwrite mode without a previous table
        // 2. Write to the same table again with overwrite mode, simulating an existing table
        // Expected behavior: Data sink creates partitioned tables, writes data to them, and creates views, old internal tables are deleted
        let pool = get_mem_duckdb();

        let table_definition = get_test_table_definition();

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
    async fn test_write_append() {
        // Test scenario:
        // 1. Write to a table with append mode without a previous table
        // 2. Write to the same table again with append mode, simulating an existing table
        // Expected behavior: Data sink creates partitioned tables, writes data to them
        let pool = get_mem_duckdb();

        let table_definition = get_test_table_definition();

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

        // After append, each partition should have 3 rows (2 from first batch + 1 from second batch)
        verify_state_after_write(
            &tx2,
            &table_definition,
            &["region=us-east-1", "region=us-west-1"],
            3,
            false,
        );

        tx2.rollback().expect("to rollback");
    }
}
