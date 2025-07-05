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
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, dml::InsertOp},
    physical_plan::{ExecutionPlan, empty::EmptyExec, limit::GlobalLimitExec, union::UnionExec},
    prelude::Expr,
    scalar::ScalarValue,
};
use snafu::prelude::*;
use tokio::sync::RwLock;

use crate::{
    Partition, creator::PartitionCreator, expression::validate_scalar_compatibility,
    insert::PartitionInsertExec,
};

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
pub struct PartitionTableProvider<ConnectionPool> {
    creator: Arc<dyn PartitionCreator<ConnectionPool = ConnectionPool>>,
    partition_by: Expr,
    partitions: Arc<RwLock<HashMap<ScalarValueString, Partition<ConnectionPool>>>>,
    schema: SchemaRef,
}

impl<ConnectionPool> PartitionTableProvider<ConnectionPool> {
    /// Creates a new [`PartitionTableProvider`] that partitions the data using
    /// the first expression in `partition_by`.
    ///
    /// # Errors
    /// This function will return an Error when the `partition_by` expression
    /// validation fails.
    pub async fn new(
        creator: Arc<dyn PartitionCreator<ConnectionPool = ConnectionPool>>,
        mut partition_by: Vec<Expr>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        let num_partition_by = partition_by.len();
        ensure!(
            num_partition_by == 1,
            PartitionByViolationSnafu { num_partition_by }
        );
        let partition_by = partition_by
            .pop()
            .context(PartitionByViolationSnafu { num_partition_by })?;

        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(SchemaConversionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?
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

    /// Get `ConnectionPool`s for each partition.
    ///
    /// # Errors
    pub async fn get_shared_pools(&self) -> Vec<Arc<ConnectionPool>> {
        self.partitions
            .read()
            .await
            .values()
            .map(|p| Arc::clone(&p.pool))
            .collect()
    }
}

#[async_trait]
impl<ConnectionPool> TableProvider for PartitionTableProvider<ConnectionPool>
where
    ConnectionPool: std::fmt::Debug + Send + Sync + 'static,
{
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
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
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
        Ok(Arc::new(PartitionInsertExec::new(
            input,
            self.partition_by.clone(),
            Arc::clone(&self.creator),
            Arc::clone(&self.partitions),
            insert_op,
            Arc::clone(&self.schema),
        )))
    }
}

/// Determine whether a partition should be pruned from the scan plan based on
/// the query `filters`, the expression that the partition was created from,
/// `partition_by`, and the `partition_value` produced by the `partition_by`
/// `Expr` for this particular partition.
fn prune_partition(filters: &[Expr], partition_by: &Expr, partition_value: &ScalarValue) -> bool {
    for filter in filters {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            right,
            op: Operator::Eq,
        }) = filter
        {
            if left.as_ref() == partition_by {
                if let Expr::Literal(lit) = right.as_ref() {
                    return lit != partition_value;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use datafusion::common::Column;

    use super::*;

    #[test]
    fn test_prune_partition_exact_match() {
        let region_expr = Expr::Column(Column::from_name("region"));
        let partition_value = ScalarValue::Utf8(Some("us-east-1".to_string()));
        let filters = &[region_expr
            .clone()
            .eq(Expr::Literal(partition_value.clone()))];

        let partition_by = region_expr;
        assert!(!prune_partition(filters, &partition_by, &partition_value));

        let partition_value = ScalarValue::Utf8(Some("ap-northeast-2".to_string()));
        assert!(prune_partition(filters, &partition_by, &partition_value));
    }

    #[test]
    #[ignore]
    fn test_prune_partition_range() {
        let column = Expr::Column(Column::from_name("fare_amount"));
        let partition_by = column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(10.0))));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(10.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(9.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .gt(Expr::Literal(ScalarValue::Float64(Some(11.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(prune_partition(filters, &partition_by, &partition_value));

        let filters = &[column
            .clone()
            .lt(Expr::Literal(ScalarValue::Float64(Some(9.0))))];
        let partition_value = ScalarValue::Boolean(Some(true));
        assert!(prune_partition(filters, &partition_by, &partition_value));
        let partition_value = ScalarValue::Boolean(Some(false));
        assert!(!prune_partition(filters, &partition_by, &partition_value));
    }
}
