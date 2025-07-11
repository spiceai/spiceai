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
    catalog::{Session, TableProvider},
    common::{Constraints, DFSchema},
    datasource::TableType,
    error::DataFusionError,
    logical_expr::{TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::{ExecutionPlan, empty::EmptyExec, limit::GlobalLimitExec, union::UnionExec},
    prelude::Expr,
};
use pruning::prune_partition;
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::{
    Partition, creator::PartitionCreator, expression::validate_scalar_compatibility,
    insert::PartitionerExec,
};

mod pruning;

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
    partition_by: Expr,
    partitions: Arc<RwLock<HashMap<ScalarValueString, Partition>>>,
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
        mut partition_by: Vec<Expr>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let num_partition_by = partition_by.len();
        ensure!(
            num_partition_by == 1,
            PartitionByViolationSnafu { num_partition_by }
        );
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(SchemaConversionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?;

        let partition_by = partition_by
            .pop()
            .context(PartitionByViolationSnafu { num_partition_by })?;

        let partitions = partitions
            .into_iter()
            .map(|p| {
                validate_scalar_compatibility(&partition_by, &p.partition_value, &df_schema)?;
                Ok((p.partition_value.to_string(), p))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .context(ValidatingExpressionsSnafu)?;

        let partitions = Arc::new(RwLock::new(partitions));

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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.creator.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let partitions = self.partitions.read().await;
        let mut plans = Vec::with_capacity(partitions.len());
        for partition in partitions.values() {
            if prune_partition(filters, &self.partition_by, &partition.partition_value) {
                continue;
            }
            let plan = partition
                .table_provider
                .scan(state, projection, filters, limit)
                .await?;
            plans.push(plan);
        }

        let plan = match plans {
            plans if plans.is_empty() => {
                return Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))));
            }
            mut plans if plans.len() == 1 => plans.pop().ok_or_else(|| {
                DataFusionError::Execution("expected an ExecutionPlan".to_string())
            })?,
            plans => Arc::new(UnionExec::new(plans)),
        };

        if let Some(limit) = limit {
            return Ok(Arc::new(GlobalLimitExec::new(plan, limit, None)));
        }

        Ok(plan)
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(PartitionerExec::new(
            input,
            self.partition_by.clone(),
            Arc::clone(&self.creator),
            Arc::clone(&self.partitions),
            insert_op,
            Arc::clone(&self.schema),
        )))
    }
}
