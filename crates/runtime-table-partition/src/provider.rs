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
    arrow::{array::RecordBatch, compute::concat_batches},
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
    physical_plan::{ExecutionPlan, common::collect, execute_stream, union::UnionExec},
    prelude::Expr,
    scalar::ScalarValue,
};
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
        let stream = execute_stream(Arc::clone(&input), task_ctx)?;
        let batches = collect(stream).await?;

        let partition_groups = group_by_partition(expr, &batches, &df_schema)?;

        let mut execution_plans = Vec::new();
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
    let mut partition_map: HashMap<ScalarValue, Vec<RecordBatch>> = HashMap::new();

    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }

        let column = physical_expr.evaluate(batch)?;
        let array = match column {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => return Err(Error::InvalidPartitionExpression).boxed()?,
        };

        for i in 0..array.len() {
            let scalar = ScalarValue::try_from_array(&array, i)?;
            let partition_batches = partition_map.entry(scalar).or_insert_with(Vec::new);
            let new_batch = batch.slice(i, 1);
            partition_batches.push(new_batch);
        }
    }

    let mut result = HashMap::new();
    for (scalar, batches) in partition_map {
        let Some(batch) = batches.first() else {
            continue;
        };
        let schema = batch.schema();
        let concat_batch = concat_batches(&schema, &batches)?;
        result.insert(scalar, concat_batch);
    }

    Ok(result)
}
