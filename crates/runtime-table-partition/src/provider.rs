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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, RecordBatch, UInt64Array},
        compute::{concat_batches, partition, take},
    },
    catalog::{
        Session, TableProvider,
        memory::{DataSourceExec, MemorySourceConfig},
    },
    common::{Constraints, DFSchema},
    datasource::TableType,
    error::DataFusionError,
    execution::{TaskContext, context::ExecutionProps},
    logical_expr::{ColumnarValue, dml::InsertOp},
    physical_expr::create_physical_expr,
    physical_plan::{ExecutionPlan, execute_stream, union::UnionExec},
    prelude::Expr,
    scalar::ScalarValue,
};
use futures::StreamExt as _;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::{creator::PartitionCreator, expression::validate_scalar_compatibility};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Only a single 'partition_by' expression is supported, but {num_partition_by} were given."
    ))]
    PartitionByViolation { num_partition_by: usize },
    #[snafu(display("Creating partition failed: {source}"))]
    CreatingPartition { source: super::creator::Error },
    #[snafu(display("Validating expressions failed: {source}"))]
    ValidatingExpressions { source: super::expression::Error },
    #[snafu(display("Failed to convert schema to DFSchema: {source}"))]
    SchemaConversion { source: DataFusionError },
    #[snafu(display("Expected array from partition expression, got scalar"))]
    InvalidPartitionExpression,
}

type ScalarValueString = String;

#[derive(Debug)]
pub struct PartitionTableProvider {
    creator: Arc<dyn PartitionCreator>,
    partition_by: Vec<Expr>,
    partitions: RwLock<HashMap<ScalarValueString, Arc<dyn TableProvider>>>,
    schema: SchemaRef,
}

impl PartitionTableProvider {
    /// Creates a new [`PartitionTableProvider`] that partitions the data using
    /// the first expression in `partition_by`.
    ///
    /// # Errors
    /// This function will return an Error when the `partition_by` expression
    /// validation fails.
    pub async fn new(
        creator: Arc<dyn PartitionCreator>,
        partition_by: Vec<Expr>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let num_partition_by = partition_by.len();
        let expr = partition_by
            .first()
            .context(PartitionByViolationSnafu { num_partition_by })?;

        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(SchemaConversionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?
            .into_iter()
            .map(|p| {
                validate_scalar_compatibility(expr, &p.partition_value, &df_schema)?;
                Ok((p.partition_value.to_string(), p.table_provider))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .context(ValidatingExpressionsSnafu)?;

        let partitions = RwLock::new(partitions);

        Ok(Self {
            creator,
            partition_by,
            partitions,
            schema,
        })
    }
}

#[async_trait]
impl TableProvider for PartitionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::Execution(
            "PartitionedTableProvider::scan not implemented yet".to_string(),
        ))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let expr = self.partition_by.first().ok_or_else(|| {
            DataFusionError::Execution("Failed to get first partition expression".to_string())
        })?;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

        let task_ctx = Arc::new(TaskContext::from(state));
        let mut stream = execute_stream(Arc::clone(&input), task_ctx)?;
        let mut execution_plans = Vec::new();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }

            let partition_groups = group_by_partition(expr, &[batch], &df_schema)?;

            for (scalar_value, batch) in partition_groups {
                let partition_key = scalar_value.to_string();
                tracing::info!("Inserting into partition with key: {partition_key}");

                let table_provider = {
                    let partitions = self.partitions.read().await;
                    if let Some(existing_provider) = partitions.get(&partition_key) {
                        tracing::debug!("Using existing partition for key: {partition_key}");
                        Arc::clone(existing_provider)
                    } else {
                        drop(partitions);
                        tracing::debug!("Creating new partition for key: {partition_key}");
                        let new_provider = self
                            .creator
                            .create_partition(scalar_value.clone())
                            .await
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                        let mut partitions = self.partitions.write().await;
                        partitions.insert(partition_key.clone(), Arc::clone(&new_provider));
                        new_provider
                    }
                };

                let mem_exec = DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
                    &[vec![batch]],
                    Arc::clone(&self.schema),
                    None,
                )?));

                let plan = table_provider
                    .insert_into(state, Arc::new(mem_exec), insert_op)
                    .await?;
                execution_plans.push(plan);
            }
        }

        if execution_plans.is_empty() {
            let mem_exec = DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
                &[vec![]],
                Arc::clone(&self.schema),
                None,
            )?));
            Ok(Arc::new(mem_exec))
        } else if execution_plans.len() == 1 {
            #[allow(clippy::unwrap_used)]
            Ok(execution_plans.into_iter().next().unwrap())
        } else {
            Ok(Arc::new(UnionExec::new(execution_plans)))
        }
    }
}

fn group_by_partition(
    expr: &Expr,
    batches: &[RecordBatch],
    df_schema: &DFSchema,
) -> Result<HashMap<ScalarValue, RecordBatch>, DataFusionError> {
    let props = ExecutionProps::new();
    let physical_expr = create_physical_expr(expr, df_schema, &props)?;
    let mut partition_map = HashMap::new();

    for batch in batches.iter().filter(|b| b.num_rows() > 0) {
        let column = physical_expr.evaluate(batch)?;
        let array = match column {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => return Err(Error::InvalidPartitionExpression).boxed()?,
        };

        let partitions = partition(&[Arc::clone(&array)])?;

        for indices in partitions.ranges() {
            if indices.is_empty() {
                continue;
            }
            // Extract scalar value from the first row of the partition
            let indices = indices.collect::<Vec<_>>();
            let scalar = ScalarValue::try_from_array(&array, indices[0])?;
            let partition_batches = partition_map.entry(scalar).or_insert_with(Vec::new);
            // Create a single batch for the partition using indices
            let new_batch = filter_batch_by_indices(batch, &indices)?;
            partition_batches.push(new_batch);
        }
    }

    let mut result = HashMap::with_capacity(partition_map.len());
    for (scalar, batches) in partition_map {
        if batches.is_empty() {
            continue;
        }
        let concat_batch = concat_batches(&batches[0].schema(), &batches)?;
        result.insert(scalar, concat_batch);
    }

    Ok(result)
}

fn filter_batch_by_indices(
    batch: &RecordBatch,
    indices: &[usize],
) -> Result<RecordBatch, DataFusionError> {
    let indices_array = UInt64Array::from_iter_values(indices.iter().map(|&i| i as u64));
    let indices_array = Arc::new(indices_array) as Arc<dyn Array>;
    let columns = batch
        .columns()
        .iter()
        .map(|col| take(col, &indices_array, None))
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| DataFusionError::ArrowError(e, None))
}
