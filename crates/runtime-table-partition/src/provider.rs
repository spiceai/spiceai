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

use arrow::array::{Array, UInt64Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink};
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, DFSchema, Statistics, project_schema},
    config::ConfigOptions,
    datasource::TableType,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, dml::InsertOp},
    physical_expr::{OrderingRequirements, PhysicalSortExpr},
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PhysicalExpr, PlanProperties,
        collect,
        empty::EmptyExec,
        execution_plan::{CardinalityEffect, InvariantLevel},
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        limit::GlobalLimitExec,
        metrics::MetricsSet,
        projection::ProjectionExec,
        sort_pushdown::SortOrderPushdownResult,
        union::UnionExec,
    },
    prelude::Expr,
};
use pruning::prune_partition;
use snafu::prelude::*;
use tokio::sync::RwLock;
use util::format_datafusion_error;

use crate::{
    Partition,
    creator::PartitionCreator,
    creator::filename::encode_composite_key,
    expression::{PartitionedBy, validate_scalar_compatibility},
    insert::{DefaultInsertStrategy, InsertStrategy, PartitionContext},
};

pub mod pruning;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("At least one 'partition_by' expression is required, but none were given."))]
    PartitionByRequired,
    #[snafu(display("Creating partition failed: {source}"))]
    CreatingPartition { source: super::creator::Error },
    #[snafu(display("Validating expressions failed: {source}"))]
    ValidatingExpressions { source: super::expression::Error },
    #[snafu(display(
        "Failed to convert schema to DFSchema: {}",
        format_datafusion_error(source)
    ))]
    SchemaConversion { source: DataFusionError },
    #[snafu(display("Expected array from partition expression, got scalar"))]
    InvalidPartitionExpression,
    #[snafu(display(
        "Partition has {actual} values but expected {expected} (one per partition_by expression)"
    ))]
    PartitionValueCountMismatch { expected: usize, actual: usize },
}

/// Composite partition key string, used as `HashMap` key.
/// For a single partition expression, this is just the encoded value (e.g., "us-east-1").
/// For multiple partition expressions, this is a path-like string (e.g., "2025/10/15").
pub(crate) type CompositePartitionKey = String;

#[derive(Debug)]
pub struct PartitionTableProvider {
    creator: Arc<dyn PartitionCreator>,
    /// The partition expressions. For hierarchical partitions like
    /// `partition_by: [year, month]`, this contains all expressions in order.
    partition_by: Vec<PartitionedBy>,
    partitions: Arc<RwLock<HashMap<CompositePartitionKey, Partition>>>,
    schema: SchemaRef,
    insert_strategy: Arc<dyn InsertStrategy>,
}

impl PartitionTableProvider {
    /// Checks if a filter expression contains or references any of the partition expressions.
    /// This is used to identify filters that can be used for partition pruning.
    fn filter_contains_any_partition_expr(
        filter: &Expr,
        partition_exprs: &[PartitionedBy],
    ) -> bool {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

        let mut contains = false;
        let _ = filter.apply(|expr| {
            for p in partition_exprs {
                if expr == &p.expression {
                    contains = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        contains
    }

    /// Returns `true` if the filter exclusively compares partition expressions to
    /// literal values using equality, and `prune_partition` can fully resolve the
    /// filter via partition metadata alone.
    ///
    /// Only the shapes that `prune_partition` handles exactly are accepted:
    /// - Single equality: `partition_expr = literal`
    /// - OR-chains of equalities: `partition_expr = lit1 OR partition_expr = lit2`
    /// - AND of partition-expression equalities across different expressions
    ///
    /// Shapes that are NOT accepted (because `prune_partition` cannot fully
    /// resolve them for transform partition expressions like `bucket()`):
    /// - Inequalities: `partition_expr != lit`, `partition_expr > lit`, etc.
    /// - `InList`: `partition_expr IN (lit1, lit2)` or `NOT IN`
    /// - Base-column filters: `org_id = 100`
    fn is_partition_expression_filter(filter: &Expr, partition_exprs: &[PartitionedBy]) -> bool {
        match filter {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq => {
                    let is_partition_expr_vs_literal = |a: &Expr, b: &Expr| -> bool {
                        partition_exprs.iter().any(|p| a == &p.expression)
                            && matches!(b, Expr::Literal(..))
                    };
                    is_partition_expr_vs_literal(left, right)
                        || is_partition_expr_vs_literal(right, left)
                }
                Operator::Or | Operator::And => {
                    Self::is_partition_expression_filter(left, partition_exprs)
                        && Self::is_partition_expression_filter(right, partition_exprs)
                }
                _ => false,
            },
            _ => false,
        }
    }

    /// Creates a new [`PartitionTableProvider`] that partitions the data using
    /// the given partition expressions.
    ///
    /// For hierarchical partitioning (e.g., year/month/day), pass multiple expressions
    /// in the order they should be applied.
    ///
    /// # Errors
    /// This function will return an Error when:
    /// - No partition expressions are provided
    /// - Partition expression validation fails
    /// - Existing partitions have incompatible values
    pub async fn new(
        creator: Arc<dyn PartitionCreator>,
        partition_by: Vec<PartitionedBy>,
        schema: SchemaRef,
    ) -> Result<Self, Error> {
        ensure!(!partition_by.is_empty(), PartitionByRequiredSnafu);

        let df_schema = DFSchema::try_from(Arc::clone(&schema)).context(SchemaConversionSnafu)?;

        let partitions = creator
            .infer_existing_partitions()
            .await
            .context(CreatingPartitionSnafu)?;

        let partitions: Result<HashMap<_, _>, Error> = partitions
            .into_iter()
            .map(|p| {
                // Validate that partition has the correct number of values
                ensure!(
                    p.partition_values.len() == partition_by.len(),
                    PartitionValueCountMismatchSnafu {
                        expected: partition_by.len(),
                        actual: p.partition_values.len(),
                    }
                );

                // Validate each partition value is compatible with its expression
                for (expr, value) in partition_by.iter().zip(p.partition_values.iter()) {
                    validate_scalar_compatibility(&expr.expression, value, &df_schema)
                        .context(ValidatingExpressionsSnafu)?;
                }

                let key = encode_composite_key(&p.partition_values).map_err(|e| {
                    Error::CreatingPartition {
                        source: crate::creator::Error::CreatePartition {
                            source: Box::new(e) as Box<dyn std::error::Error + Send + Sync>,
                        },
                    }
                })?;
                Ok((key, p))
            })
            .collect();

        let partitions = Arc::new(RwLock::new(partitions?));

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

    /// Returns the table providers for all current partitions.
    ///
    /// Callers can use this to apply per-partition operations (e.g., index maintenance)
    /// that are not part of the standard `TableProvider` interface.
    pub async fn partition_table_providers(&self) -> Vec<Arc<dyn TableProvider>> {
        self.partitions
            .read()
            .await
            .values()
            .map(|p| Arc::clone(&p.table_provider))
            .collect()
    }

    /// Collects all partition column references from all partition expressions.
    fn all_partition_columns(&self) -> std::collections::HashSet<&datafusion::common::Column> {
        self.partition_by
            .iter()
            .flat_map(|p| p.expression.column_refs())
            .collect()
    }
    #[must_use]
    pub fn creator(&self) -> &Arc<dyn PartitionCreator> {
        &self.creator
    }

    #[must_use]
    pub fn partition_by(&self) -> &[PartitionedBy] {
        &self.partition_by
    }

    /// Returns the provider for the given partition values, creating the partition if needed.
    ///
    /// # Errors
    ///
    /// Returns a [`DataFusionError`] if the partition key cannot be encoded or if creating
    /// the new partition fails.
    pub async fn get_or_create_partition_provider(
        &self,
        partition_values: Vec<datafusion::scalar::ScalarValue>,
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let partition_key = encode_composite_key(&partition_values).map_err(|e| {
            DataFusionError::Execution(format!("Failed to encode partition key: {e}"))
        })?;

        // retain the write lock across the entire get-or-create instead of holding a .read() which releases the lock and could race
        // with another get-or-create for the same partition key, resulting in duplicate partitions being created
        let mut partitions_lock = self.partitions.write().await;
        if let Some(partition) = partitions_lock.get(&partition_key) {
            return Ok(Arc::clone(&partition.table_provider));
        }

        let partition = self
            .creator
            .create_partition(partition_values)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let provider = Arc::clone(&partition.table_provider);
        partitions_lock.insert(partition_key, partition);
        Ok(provider)
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl TableProvider for PartitionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.creator.constraints()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let creator_results = self.creator.supports_filters_pushdown(filters)?;

        Ok(filters
            .iter()
            .zip(creator_results)
            .map(|(filter, creator_result)| {
                // Filters that directly compare a partition expression to literals
                // (e.g. bucket(50, org_id) = '5') are fully resolved by partition
                // pruning — no row-level FilterExec is needed.
                if Self::is_partition_expression_filter(filter, &self.partition_by) {
                    TableProviderFilterPushDown::Exact
                } else {
                    creator_result
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Split filters into partition filters (for pruning) and data filters (for partition scans)
        // NOTE: Filters can be BOTH partition filters AND data filters for transform partitions
        let partition_columns = self.all_partition_columns();

        // Pre-compute column references for all filters to avoid repeated expression tree traversals
        let filter_columns_cache: Vec<_> =
            filters.iter().map(|filter| filter.column_refs()).collect();

        // Collect partition filters (used for pruning)
        // A filter can prune partitions if it involves ANY of the partition columns
        let partition_filters: Vec<_> = filters
            .iter()
            .cloned()
            .zip(filter_columns_cache.iter())
            .filter_map(|(filter, filter_columns)| {
                // A filter is a partition filter (for pruning) if:
                // 1. It has no column references (constant expression like WHERE true), OR
                // 2. All its column references are in the partition expression columns, OR
                // 3. The filter directly involves any partition expression itself
                if filter_columns.is_empty() {
                    return Some(filter);
                }

                if filter_columns
                    .iter()
                    .all(|col| partition_columns.contains(col))
                {
                    return Some(filter);
                }

                // Check if the filter contains any partition expression
                if Self::filter_contains_any_partition_expr(&filter, &self.partition_by) {
                    return Some(filter);
                }

                None
            })
            .collect();

        // Collect data filters (applied to partition scans)
        // Exclude filters that are simple column filters matching a simple partition expression exactly
        // For example, with partition_by region:
        //   - WHERE region = 'us-east-1' should NOT be a data filter (partition handles it)
        // But with partition_by bucket(3, user_id):
        //   - WHERE user_id = 100 SHOULD be a data filter (partition only determines bucket)
        let simple_partition_cols: std::collections::HashSet<_> = self
            .partition_by
            .iter()
            .filter_map(|p| {
                if let Expr::Column(col) = &p.expression {
                    Some(col)
                } else {
                    None
                }
            })
            .collect();

        let data_filters: Vec<_> = filters
            .iter()
            .zip(filter_columns_cache.iter())
            .filter(|(filter, filter_cols)| {
                // If this filter references only a simple partition column, exclude it
                if filter_cols.len() == 1
                    && let Some(col) = filter_cols.iter().next()
                    && simple_partition_cols.contains(col)
                {
                    return false; // Exclude from data filters
                }
                // Exclude filters that directly compare a partition expression to
                // literal values (e.g. bucket(50, org_id) = '5' OR bucket(50, org_id) = '13').
                // Partition pruning already guarantees only matching partitions are
                // scanned, so re-evaluating the transform per-row is redundant and
                // can be extremely expensive for large datasets.
                if Self::is_partition_expression_filter(filter, &self.partition_by) {
                    return false;
                }
                // For all other cases (transform expressions, multiple columns, etc.), keep as data filter
                true
            })
            .map(|(filter, _)| filter.clone())
            .collect();

        let partitions = self.partitions.read().await;
        let mut plans = Vec::with_capacity(partitions.len());
        for partition in partitions.values() {
            // Check if this partition should be pruned based on ANY partition expression
            let mut should_prune = false;
            for (idx, partition_expr) in self.partition_by.iter().enumerate() {
                // Get the partition value for this expression
                let partition_value = partition.partition_values.get(idx).ok_or_else(|| {
                    DataFusionError::Internal(format!("Partition missing value at index {idx}"))
                })?;

                if prune_partition(
                    &partition_filters,
                    &partition_expr.expression,
                    partition_value,
                    &self.schema,
                )? {
                    should_prune = true;
                    break;
                }
            }

            if should_prune {
                continue;
            }

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
            plans => Arc::new(PartitionedUnionExec::try_new(plans)?),
        };

        if let Some(limit) = limit {
            return Ok(Arc::new(GlobalLimitExec::new(plan, limit, None)));
        }

        Ok(plan)
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(
        &self,
    ) -> Option<std::borrow::Cow<'_, datafusion::logical_expr::LogicalPlan>> {
        None
    }

    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: datafusion::catalog::ScanArgs<'a>,
    ) -> datafusion::error::Result<datafusion::catalog::ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let projection = args.projection().map(<[usize]>::to_vec);
        let limit = args.limit();
        let plan = self
            .scan(state, projection.as_ref(), filters, limit)
            .await?;
        Ok(datafusion::catalog::ScanResult::new(plan))
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        None
    }

    async fn truncate(
        &self,
        _state: &dyn Session,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(
            "PartitionTableProvider does not support truncate".to_string(),
        ))
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

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session_state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Session is not a SessionState in PartitionTableProvider::update".to_string(),
                )
            })?
            .clone();

        let partitions = self.partitions.read().await;
        let partition_list: Vec<_> = partitions.values().cloned().collect();
        drop(partitions);

        let update_sink = Arc::new(PartitionedUpdateSink::new(
            partition_list,
            assignments,
            filters,
            state.task_ctx(),
            session_state,
        ));

        Ok(Arc::new(DeletionExec::new(update_sink)))
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let session_state = state
            .as_any()
            .downcast_ref::<SessionState>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Session is not a SessionState in PartitionTableProvider::delete_from"
                        .to_string(),
                )
            })?
            .clone();

        // Collect all partitions that need deletion
        let partitions = self.partitions.read().await;
        let partition_list: Vec<_> = partitions.values().cloned().collect();
        drop(partitions);

        // Create a deletion sink that will iterate over all partitions
        let deletion_sink = Arc::new(PartitionedDeletionSink::new(
            partition_list,
            filters,
            state.task_ctx(),
            session_state,
        ));

        Ok(Arc::new(DeletionExec::new(deletion_sink)))
    }
}

/// A deletion sink that applies deletion filters to all partitions in a partitioned table.
struct PartitionedDeletionSink {
    partitions: Vec<Partition>,
    filters: Vec<Expr>,
    task_ctx: Arc<TaskContext>,
    session_state: SessionState,
}

impl PartitionedDeletionSink {
    fn new(
        partitions: Vec<Partition>,
        filters: Vec<Expr>,
        task_ctx: Arc<TaskContext>,
        session_state: SessionState,
    ) -> Self {
        Self {
            partitions,
            filters,
            task_ctx,
            session_state,
        }
    }
}

#[async_trait]
impl DeletionSink for PartitionedDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut total_deleted = 0u64;

        for partition in &self.partitions {
            let plan = partition
                .table_provider
                .delete_from(&self.session_state, self.filters.clone())
                .await?;

            let results = collect(plan, Arc::clone(&self.task_ctx)).await?;

            for batch in results {
                if let Some(count_col) = batch.column_by_name("count")
                    && let Some(uint_array) = count_col.as_any().downcast_ref::<UInt64Array>()
                {
                    for i in 0..uint_array.len() {
                        if !uint_array.is_null(i) {
                            total_deleted += uint_array.value(i);
                        }
                    }
                }
            }
        }

        Ok(total_deleted)
    }
}

/// An update sink that applies update operations to all partitions in a partitioned table.
struct PartitionedUpdateSink {
    partitions: Vec<Partition>,
    assignments: Vec<(String, Expr)>,
    filters: Vec<Expr>,
    task_ctx: Arc<TaskContext>,
    session_state: SessionState,
}

impl PartitionedUpdateSink {
    fn new(
        partitions: Vec<Partition>,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
        task_ctx: Arc<TaskContext>,
        session_state: SessionState,
    ) -> Self {
        Self {
            partitions,
            assignments,
            filters,
            task_ctx,
            session_state,
        }
    }
}

#[async_trait]
impl DeletionSink for PartitionedUpdateSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let mut total_updated = 0u64;
        let session_ctx = datafusion::execution::context::SessionContext::new();
        let _state = session_ctx.state();

        for partition in &self.partitions {
            let plan = partition
                .table_provider
                .update(
                    &self.session_state,
                    self.assignments.clone(),
                    self.filters.clone(),
                )
                .await?;

            let results = collect(plan, Arc::clone(&self.task_ctx)).await?;

            for batch in results {
                if let Some(count_col) = batch.column_by_name("count")
                    && let Some(uint_array) = count_col.as_any().downcast_ref::<UInt64Array>()
                {
                    for i in 0..uint_array.len() {
                        if !uint_array.is_null(i) {
                            total_updated += uint_array.value(i);
                        }
                    }
                }
            }
        }

        Ok(total_updated)
    }
}

#[derive(Debug)]
struct PartitionedUnionExec {
    inner_union: Arc<dyn ExecutionPlan>,
}

impl PartitionedUnionExec {
    fn try_new(partitions: Vec<Arc<dyn ExecutionPlan>>) -> Result<Self, DataFusionError> {
        let inner_union = UnionExec::try_new(partitions)?;
        Ok(Self { inner_union })
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for PartitionedUnionExec {
    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "PartitionedUnionExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "PartitionedUnionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner_union.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.inner_union.schema()
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<(), DataFusionError> {
        self.inner_union.check_invariants(check)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner_union.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner_union.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner_union.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner_union.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner_union.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.is_empty() {
            return Err(DataFusionError::Plan(
                "PartitionedUnionExec requires at least one child".to_string(),
            ));
        }

        Ok(Arc::new(PartitionedUnionExec::try_new(children)?))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.inner_union.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner_union.metrics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Statistics, DataFusionError> {
        self.inner_union.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner_union.supports_limit_pushdown()
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner_union.cardinality_effect()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        self.inner_union.try_swapping_with_projection(projection)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription, DataFusionError> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

impl DisplayAs for PartitionedUnionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "PartitionedUnionExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        arrow::array::{Int32Array, StringArray},
        arrow::record_batch::RecordBatch,
        datasource::MemTable,
        logical_expr::{ScalarUDF, expr::ScalarFunction},
        prelude::{col, lit},
        scalar::ScalarValue,
    };

    type PartitionsData = Arc<RwLock<Vec<(ScalarValue, Arc<dyn TableProvider>)>>>;

    #[derive(Debug)]
    struct MockCreator {
        partitions_data: PartitionsData,
    }

    #[async_trait]
    impl PartitionCreator for MockCreator {
        async fn create_partition(
            &self,
            _partition_values: Vec<ScalarValue>,
        ) -> Result<Partition, super::super::creator::Error> {
            unreachable!("create_partition not needed for scan tests")
        }

        async fn infer_existing_partitions(
            &self,
        ) -> Result<Vec<Partition>, super::super::creator::Error> {
            let data = self.partitions_data.read().await;
            Ok(data
                .iter()
                .map(|(val, provider)| Partition {
                    partition_values: vec![val.clone()],
                    table_provider: Arc::clone(provider),
                })
                .collect())
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
        }
    }

    fn create_test_batch(region: &str, ids: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let id_array = Arc::new(Int32Array::from(ids));
        let region_array = Arc::new(StringArray::from(vec![region; id_array.len()]));

        RecordBatch::try_new(schema, vec![id_array, region_array])
            .expect("failed to create test batch")
    }

    #[tokio::test]
    async fn test_scan_with_multiple_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let us_east_batch = create_test_batch("us-east-1", vec![1, 2, 3]);
        let us_west_batch = create_test_batch("us-west-1", vec![4, 5, 6]);

        let partitions_data = vec![
            (
                ScalarValue::Utf8(Some("us-east-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_east_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
            (
                ScalarValue::Utf8(Some("us-west-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_west_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let partition_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan failed");

        // With 2 partitions and no filters, should produce a UnionExec
        assert!(
            plan.as_any().is::<PartitionedUnionExec>(),
            "Expected PartitionedUnionExec for multiple partitions"
        );
    }

    #[tokio::test]
    async fn test_scan_with_partition_pruning() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let us_east_batch = create_test_batch("us-east-1", vec![1, 2, 3]);
        let us_west_batch = create_test_batch("us-west-1", vec![4, 5, 6]);

        let partitions_data = vec![
            (
                ScalarValue::Utf8(Some("us-east-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_east_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
            (
                ScalarValue::Utf8(Some("us-west-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_west_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let partition_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter to only one partition
        let filters = vec![col("region").eq(lit("us-east-1"))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // After pruning to single partition, should not be UnionExec
        assert!(
            !plan.as_any().is::<UnionExec>(),
            "Expected single partition plan (not UnionExec) after pruning"
        );
    }

    #[tokio::test]
    async fn test_scan_with_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let us_east_batch = create_test_batch("us-east-1", vec![1, 2, 3]);

        let partitions_data = vec![(
            ScalarValue::Utf8(Some("us-east-1".to_string())),
            Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![us_east_batch]])
                    .expect("failed to create MemTable"),
            ) as Arc<dyn TableProvider>,
        )];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let partition_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &[], Some(10))
            .await
            .expect("scan failed");

        // With a limit, should wrap in GlobalLimitExec
        assert!(
            plan.as_any().is::<GlobalLimitExec>(),
            "Expected GlobalLimitExec when limit is provided"
        );
    }

    #[tokio::test]
    async fn test_scan_empty_result() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(vec![])),
        });

        let partition_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan failed");

        // No partitions should return EmptyExec
        assert!(
            plan.as_any().is::<EmptyExec>(),
            "Expected EmptyExec when no partitions exist"
        );
    }

    #[tokio::test]
    async fn test_scan_prune_all_partitions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("region", DataType::Utf8, false),
        ]));

        let us_east_batch = create_test_batch("us-east-1", vec![1, 2, 3]);
        let us_west_batch = create_test_batch("us-west-1", vec![4, 5, 6]);

        let partitions_data = vec![
            (
                ScalarValue::Utf8(Some("us-east-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_east_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
            (
                ScalarValue::Utf8(Some("us-west-1".to_string())),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![us_west_batch]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let partition_by = PartitionedBy {
            name: "region".to_string(),
            expression: col("region"),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter that matches no partitions
        let filters = vec![col("region").eq(lit("eu-central-1"))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // All partitions pruned should return EmptyExec
        assert!(
            plan.as_any().is::<EmptyExec>(),
            "Expected EmptyExec when all partitions are pruned"
        );
    }

    #[tokio::test]
    async fn test_scan_with_bucket_partition_expression() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("failed to create batch");

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![15, 25, 35])),
            ],
        )
        .expect("failed to create batch");

        let partitions_data = vec![
            (
                ScalarValue::Int32(Some(0)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch1]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
            (
                ScalarValue::Int32(Some(1)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch2]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        // Create a bucket partition expression: bucket(10, user_id)
        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: bucket_udf,
            args: vec![lit(10i32), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_10_user_id".to_string(),
            expression: partition_expr.clone(),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter using the bucket expression
        let filters = vec![partition_expr.eq(lit(0i32))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // Should prune to single partition
        assert!(
            !plan.as_any().is::<UnionExec>(),
            "Expected single partition plan after pruning with bucket expression"
        );
    }

    #[tokio::test]
    async fn test_scan_bucket_partition_with_base_column_filter() {
        // Test that filters on the base column (used in a transform partition like bucket)
        // are BOTH used for pruning AND passed to the partition scan for data filtering.
        // This prevents data integrity bugs where partition pruning incorrectly filters out data.

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        // Partition 0: contains user_ids that hash to bucket 0
        // Partition 1: contains user_ids that hash to bucket 1
        // We'll create data where multiple user_ids map to the same bucket
        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![100, 200, 300])), // Different user_ids in bucket 0
            ],
        )
        .expect("failed to create batch");

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![150, 250, 350])), // Different user_ids in bucket 1
            ],
        )
        .expect("failed to create batch");

        let partitions_data = vec![
            (
                ScalarValue::Int64(Some(0)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch0]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
            (
                ScalarValue::Int64(Some(1)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch1]])
                        .expect("failed to create MemTable"),
                ) as Arc<dyn TableProvider>,
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        // Partition by bucket(3, user_id)
        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: bucket_udf,
            args: vec![lit(3i64), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_3_user_id".to_string(),
            expression: partition_expr,
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter: WHERE user_id = 100
        // This should:
        // 1. Evaluate bucket(3, 100) to determine which partition to scan
        // 2. Pass user_id = 100 as a data filter to the partition scan
        // 3. NOT incorrectly filter out the data based only on partition pruning
        let filters = vec![col("user_id").eq(lit(100i32))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // Verify the plan structure
        // The important part is that the filter is passed through to the partition scan
        // We can't directly verify the filter was passed, but we can ensure:
        // 1. Only one partition is scanned (pruning worked)
        // 2. The plan is not a UnionExec (single partition)
        assert!(
            !plan.as_any().is::<UnionExec>(),
            "Expected single partition plan after pruning with base column filter"
        );

        // The actual data filtering verification would require executing the plan
        // and checking the results, which is beyond the scope of a unit test.
        // Integration tests should verify that the correct data is returned.
    }

    /// A wrapper around [`MemTable`] that records what filters are passed to
    /// its `scan` method, used to verify that partition-expression filters are
    /// NOT redundantly forwarded as data filters.
    #[derive(Debug)]
    struct FilterTrackingProvider {
        inner: MemTable,
        recorded_filters: Arc<std::sync::Mutex<Vec<Vec<Expr>>>>,
    }

    impl FilterTrackingProvider {
        fn new(inner: MemTable, tracker: Arc<std::sync::Mutex<Vec<Vec<Expr>>>>) -> Self {
            Self {
                inner,
                recorded_filters: tracker,
            }
        }
    }

    #[async_trait]
    impl TableProvider for FilterTrackingProvider {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            self.recorded_filters
                .lock()
                .expect("lock poisoned")
                .push(filters.to_vec());
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    #[tokio::test]
    async fn test_bucket_partition_expr_filter_not_passed_as_data_filter() {
        // Regression test: when a filter directly compares the partition expression
        // to a literal (e.g. `bucket(3, user_id) = 0`), it must NOT be forwarded
        // as a data filter to partition scans.  Doing so forces an expensive
        // per-row hash evaluation that is redundant after partition pruning.

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )
        .expect("failed to create batch");
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(Int32Array::from(vec![15, 25])),
            ],
        )
        .expect("failed to create batch");

        let tracker: Arc<std::sync::Mutex<Vec<Vec<Expr>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let partitions_data: Vec<(ScalarValue, Arc<dyn TableProvider>)> = vec![
            (
                ScalarValue::Int32(Some(0)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch0]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ),
            (
                ScalarValue::Int32(Some(1)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch1]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::clone(&bucket_udf),
            args: vec![lit(10i32), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_10_user_id".to_string(),
            expression: partition_expr.clone(),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter: bucket(10, user_id) = 0  — a partition-expression filter
        let filters = vec![partition_expr.eq(lit(0i32))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let _plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // Only one partition should have been scanned (pruning works)
        let recorded = tracker.lock().expect("lock poisoned");
        assert_eq!(
            recorded.len(),
            1,
            "Expected exactly 1 partition scanned, got {}",
            recorded.len()
        );

        // The partition scan should receive NO filters — the bucket expression
        // filter is fully resolved by pruning and must not be a data filter.
        assert!(
            recorded[0].is_empty(),
            "Expected no data filters passed to partition scan, but got: {:?}",
            recorded[0]
        );
    }

    #[tokio::test]
    async fn test_bucket_partition_or_chain_not_passed_as_data_filter() {
        // Regression test: an OR-chain of bucket partition expression equalities
        // (e.g. `bucket(3, user_id) = 0 OR bucket(3, user_id) = 1`) must NOT be
        // forwarded as a data filter.

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let batch0 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![10])),
            ],
        )
        .expect("failed to create batch");
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![20])),
            ],
        )
        .expect("failed to create batch");
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int32Array::from(vec![30])),
            ],
        )
        .expect("failed to create batch");

        let tracker: Arc<std::sync::Mutex<Vec<Vec<Expr>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let partitions_data: Vec<(ScalarValue, Arc<dyn TableProvider>)> = vec![
            (
                ScalarValue::Int32(Some(0)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch0]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ),
            (
                ScalarValue::Int32(Some(1)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch1]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ),
            (
                ScalarValue::Int32(Some(2)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch2]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ),
        ];

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: Arc::clone(&bucket_udf),
            args: vec![lit(3i32), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_3_user_id".to_string(),
            expression: partition_expr.clone(),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter: bucket(3, user_id) = 0 OR bucket(3, user_id) = 1
        let filters = vec![
            partition_expr
                .clone()
                .eq(lit(0i32))
                .or(partition_expr.eq(lit(1i32))),
        ];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let _plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        // 2 partitions should match (0 and 1), partition 2 pruned.
        let recorded = tracker.lock().expect("lock poisoned");
        assert_eq!(
            recorded.len(),
            2,
            "Expected 2 partitions scanned, got {}",
            recorded.len()
        );

        // Neither scanned partition should receive the bucket filter as a data filter
        for (idx, filters) in recorded.iter().enumerate() {
            assert!(
                filters.is_empty(),
                "Partition {idx}: expected no data filters, got: {filters:?}",
            );
        }
    }

    #[tokio::test]
    async fn test_base_column_filter_still_passed_as_data_filter_for_bucket_partition() {
        // Correctness test: when the filter is on the base column (e.g. user_id = 100)
        // and the partition is bucket(3, user_id), the filter MUST still be forwarded
        // as a data filter — multiple user_ids can hash to the same bucket.

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let tracker: Arc<std::sync::Mutex<Vec<Vec<Expr>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        // Create partitions for all possible bucket values (0, 1, 2) so `user_id = 100`
        // is guaranteed to match one regardless of which bucket the hash maps to.
        let mut partitions_data: Vec<(ScalarValue, Arc<dyn TableProvider>)> = Vec::new();
        for bucket_val in 0..3i32 {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![bucket_val + 1])),
                    Arc::new(Int32Array::from(vec![100 + bucket_val])),
                ],
            )
            .expect("failed to create batch");

            partitions_data.push((
                ScalarValue::Int32(Some(bucket_val)),
                Arc::new(FilterTrackingProvider::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                        .expect("failed to create MemTable"),
                    Arc::clone(&tracker),
                )),
            ));
        }

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(partitions_data)),
        });

        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: bucket_udf,
            args: vec![lit(3i32), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_3_user_id".to_string(),
            expression: partition_expr,
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Filter: user_id = 100 — base column, NOT partition expression
        let filters = vec![col("user_id").eq(lit(100i32))];

        let session_state = datafusion::execution::context::SessionContext::new().state();
        let _plan = provider
            .scan(&session_state, None, &filters, None)
            .await
            .expect("scan failed");

        let recorded = tracker.lock().expect("lock poisoned");
        // At least one partition scanned
        assert!(
            !recorded.is_empty(),
            "Expected at least one partition to be scanned"
        );

        // The data filter `user_id = 100` MUST be present (correctness requirement)
        for (idx, filters) in recorded.iter().enumerate() {
            assert!(
                !filters.is_empty(),
                "Partition {idx}: expected data filter user_id=100 to be passed through",
            );
        }
    }

    #[tokio::test]
    async fn test_supports_filters_pushdown_exact_for_partition_expr_filter() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("user_id", DataType::Int32, false),
        ]));

        let creator = Arc::new(MockCreator {
            partitions_data: Arc::new(RwLock::new(vec![])),
        });

        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: bucket_udf,
            args: vec![lit(3i32), col("user_id")],
        });

        let partition_by = PartitionedBy {
            name: "bucket_3_user_id".to_string(),
            expression: partition_expr.clone(),
        };

        let provider =
            PartitionTableProvider::new(creator, vec![partition_by], Arc::clone(&schema))
                .await
                .expect("failed to create provider");

        // Partition expression filter → Exact
        let partition_filter = partition_expr.eq(lit(0i32));
        // Base column filter → Inexact (delegated to creator)
        let base_filter = col("user_id").eq(lit(100i32));

        let results = provider
            .supports_filters_pushdown(&[&partition_filter, &base_filter])
            .expect("supports_filters_pushdown failed");

        assert_eq!(
            results[0],
            TableProviderFilterPushDown::Exact,
            "Partition expression filter should be Exact"
        );
        assert_eq!(
            results[1],
            TableProviderFilterPushDown::Inexact,
            "Base column filter should be Inexact (delegated to creator)"
        );
    }
}
