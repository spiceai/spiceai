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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::DFSchema,
    error::DataFusionError,
    logical_expr::dml::InsertOp,
    physical_expr::create_physical_expr,
    physical_plan::{
        Distribution, ExecutionPlan, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_optimizer_rules::pass_thru::PassThruExec;
use datafusion_table_providers::{
    duckdb::TableDefinition, sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
    util::on_conflict::OnConflict,
};
use futures::StreamExt;
use runtime_table_partition::{
    expression::PartitionedBy,
    insert::{InsertStrategy, PartitionContext, partition_batch},
};

use crate::dataaccelerator::{
    AccelerationSource, partitioned_duckdb::tables_mode::sink::DuckDBPartitionedDataSink,
};

/// Strategy for handling `DuckDB` table-based partition insertions.
#[derive(Debug)]
pub struct DuckDBPartitionedInsertStrategy {
    pool: Arc<DuckDbConnectionPool>,
    table_definition: Arc<TableDefinition>,
    on_conflict: Option<OnConflict>,
    rows_per_partition_buffer: Option<usize>,
}

impl DuckDBPartitionedInsertStrategy {
    #[must_use]
    pub fn new(
        pool: Arc<DuckDbConnectionPool>,
        table_definition: Arc<TableDefinition>,
        on_conflict: Option<OnConflict>,
        source: &dyn AccelerationSource,
    ) -> Self {
        let rows_per_partition_buffer = get_rows_per_partition_buffer(source);

        Self {
            pool,
            table_definition,
            on_conflict,
            rows_per_partition_buffer,
        }
    }

    fn create_infer_partitions_exec(
        input: Arc<dyn ExecutionPlan>,
        ctx: &PartitionContext,
    ) -> Arc<dyn ExecutionPlan> {
        let creator = Arc::clone(&ctx.creator);
        let partitions_lock = Arc::clone(&ctx.partitions);

        let exec = move |input_exec: &Arc<dyn ExecutionPlan>, partition, ctx| {
            let schema = input_exec.schema();
            let input_stream = input_exec.execute(partition, ctx)?;
            let creator = Arc::clone(&creator);
            let partitions_lock = Arc::clone(&partitions_lock);

            let output_stream = futures::stream::unfold(
                (input_stream, creator, partitions_lock, true),
                |(mut input, creator, partitions_lock, mut success)| async move {
                    match input.next().await {
                        Some(Ok(batch)) => {
                            Some((Ok(batch), (input, creator, partitions_lock, success)))
                        }
                        Some(Err(e)) => {
                            success = false;
                            Some((Err(e), (input, creator, partitions_lock, success)))
                        }
                        None => {
                            // Stream completed - re-infer partitions if successful
                            if success {
                                match creator.infer_existing_partitions().await {
                                    Ok(partitions) => {
                                        let partitions_map = partitions
                                            .into_iter()
                                            .map(|p| (p.partition_value.to_string(), p))
                                            .collect::<HashMap<_, _>>();
                                        *partitions_lock.write().await = partitions_map;
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            "Failed to re-infer partitions after insert: {e}"
                                        );
                                    }
                                }
                            }
                            None
                        }
                    }
                },
            );

            Ok(
                Box::pin(RecordBatchStreamAdapter::new(schema, output_stream))
                    as SendableRecordBatchStream,
            )
        };

        Arc::new(
            PassThruExec::new(input, "InferPartitionsExec", exec)
                .with_input_partitioning(Distribution::SinglePartition),
        )
    }
}

fn get_rows_per_partition_buffer(source: &dyn AccelerationSource) -> Option<usize> {
    source
        .acceleration()?
        .params
        .get("duckdb_partitioned_write_flush_threshold")
        .and_then(|v| {
            v.parse::<usize>().ok().or_else(|| {
                tracing::warn!(
                    "Invalid `duckdb_partitioned_write_flush_threshold` parameter '{v}': must be a positive integer"
                );
                None
            })
        })
}

#[async_trait]
impl InsertStrategy for DuckDBPartitionedInsertStrategy {
    async fn execute_insert(
        &self,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
        ctx: &PartitionContext,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = Arc::clone(&ctx.schema);

        let partitioner = Arc::new(BatchPartitioner::new(
            &ctx.partition_by.expression,
            Arc::clone(&schema),
            &ctx.partition_by,
        )?);

        let mut data_sink = DuckDBPartitionedDataSink::new(
            Arc::clone(&self.pool),
            Arc::clone(&self.table_definition),
            insert_op,
            self.on_conflict.clone(),
            schema,
            partitioner,
        );

        // Apply the buffer size configuration if available
        if let Some(buffer_size) = self.rows_per_partition_buffer {
            data_sink = data_sink.with_rows_per_partition_buffer(buffer_size);
        }

        let data_sink_exec = Arc::new(DataSinkExec::new(input, Arc::new(data_sink), None));

        // Wrap with PassThruExec to re-infer partitions after completion
        Ok(Self::create_infer_partitions_exec(data_sink_exec, ctx))
    }
}

/// Partitions Arrow `RecordBatch`es into separate tables based on a `DataFusion` expression.
///
/// `BatchPartitioner` compiles a `DataFusion` logical expression into a physical expression,
/// which is then evaluated for each row in a batch to determine its partition key.
/// The partitioning specification (`PartitionedBy`) provides the partition column name and expression.
/// The `partition_batch` method uses the physical expression to group rows into partitions,
/// returning a map from partition identifier (in hive-style format) to the corresponding `RecordBatch`.
pub(crate) struct BatchPartitioner {
    physical_expr: Arc<dyn datafusion::physical_plan::PhysicalExpr>,
    partitioned_by: PartitionedBy,
}

impl BatchPartitioner {
    pub fn new(
        expr: &Expr,
        schema: SchemaRef,
        partitioned_by: &PartitionedBy,
    ) -> Result<Self, DataFusionError> {
        let df_schema = DFSchema::try_from(schema)?;
        let execution_props = ExecutionProps::new();
        let physical_expr = create_physical_expr(expr, &df_schema, &execution_props)?;
        Ok(Self {
            physical_expr,
            partitioned_by: partitioned_by.clone(),
        })
    }

    /// Partition a `RecordBatch` into multiple batches based on partition keys, returning a `HashMap` where the key is the partition identifier and the value is the `RecordBatch` for that partition.
    pub fn partition_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<HashMap<String, RecordBatch>, DataFusionError> {
        let partitions = partition_batch(batch, self.physical_expr.as_ref())?;

        Ok(partitions
            .into_iter()
            .map(|(partition, (_scalar_value, batch))| {
                // hive-style format
                (format!("{}={partition}", self.partitioned_by.name), batch)
            })
            .collect::<HashMap<_, _>>())
    }
}
