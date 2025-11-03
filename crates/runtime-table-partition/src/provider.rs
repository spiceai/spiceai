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
    common::{Constraints, DFSchema, project_schema},
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
    Partition,
    creator::PartitionCreator,
    expression::{PartitionedBy, validate_scalar_compatibility},
    insert::{DefaultInsertStrategy, InsertStrategy, PartitionContext},
};

pub mod pruning;

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

pub(crate) type ScalarValueString = String;

#[derive(Debug)]
pub struct PartitionTableProvider {
    creator: Arc<dyn PartitionCreator>,
    partition_by: PartitionedBy,
    partitions: Arc<RwLock<HashMap<ScalarValueString, Partition>>>,
    schema: SchemaRef,
    insert_strategy: Arc<dyn InsertStrategy>,
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
        mut partition_by: Vec<PartitionedBy>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let num_partition_by = partition_by.len();
        let partition_by = partition_by
            .pop()
            .context(PartitionByViolationSnafu { num_partition_by })?;
        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(SchemaConversionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?;

        let partitions = partitions
            .into_iter()
            .map(|p| {
                validate_scalar_compatibility(
                    &partition_by.expression,
                    &p.partition_value,
                    &df_schema,
                )?;
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
            insert_strategy: Arc::new(DefaultInsertStrategy),
        })
    }

    /// Sets a custom data insertion strategy for this [`PartitionTableProvider`].
    #[must_use]
    pub fn with_insert_strategy(mut self, insert_strategy: Arc<dyn InsertStrategy>) -> Self {
        self.insert_strategy = insert_strategy;
        self
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
        // Split filters into partition filters (for pruning) and data filters (for partition scans)
        // Partition filters are those that can be evaluated using only the partition expression columns
        let partition_columns = self.partition_by.expression.column_refs();

        // Pre-compute column references for all filters to avoid repeated expression tree traversals
        let filter_columns_cache: Vec<_> =
            filters.iter().map(|filter| filter.column_refs()).collect();

        let (partition_filters, data_filters): (Vec<_>, Vec<_>) = filters
            .iter()
            .cloned()
            .zip(filter_columns_cache.iter())
            .partition(|(_, filter_columns)| {
                // A filter is a partition filter if:
                // 1. It has no column references (constant expression like WHERE true), OR
                // 2. All its column references are in the partition expression columns
                filter_columns.is_empty()
                    || filter_columns
                        .iter()
                        .all(|col| partition_columns.contains(col))
            });

        // Extract just the filters (without the cached column refs)
        let partition_filters: Vec<_> = partition_filters.into_iter().map(|(f, _)| f).collect();
        let data_filters: Vec<_> = data_filters.into_iter().map(|(f, _)| f).collect();

        tracing::debug!(
            "Partition pruning: {} partition filters, {} data filters",
            partition_filters.len(),
            data_filters.len()
        );

        let partitions = self.partitions.read().await;
        let mut plans = Vec::with_capacity(partitions.len());
        for partition in partitions.values() {
            // Use partition filters for pruning
            if prune_partition(
                &partition_filters,
                &self.partition_by.expression,
                &partition.partition_value,
                &self.schema,
            )? {
                continue;
            }
            // Only pass data filters to partition scan (partition filters are redundant after pruning)
            let plan = partition
                .table_provider
                .scan(state, projection, &data_filters, limit)
                .await?;
            plans.push(plan);
        }

        let plan = match plans {
            plans if plans.is_empty() => {
                let projected_schema = project_schema(&self.schema, projection)?;
                return Ok(Arc::new(EmptyExec::new(projected_schema)));
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
        let ctx = PartitionContext {
            creator: Arc::clone(&self.creator),
            partition_by: self.partition_by.clone(),
            partitions: Arc::clone(&self.partitions),
            schema: Arc::clone(&self.schema),
        };

        self.insert_strategy
            .execute_insert(input, insert_op, &ctx)
            .await
    }
}
