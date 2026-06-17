/*
Copyright 2026 The Spice.ai OSS Authors

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

//! [`HttpParamsPushdown`] rewrites `HashJoinExec` (semi-join) over `HttpExec`
//! by emitting a lazy [`HttpWithDeferredParamsExec`] node. The node defers
//! build-side materialization to execution time (fully async), then injects the
//! collected values as HTTP partition filters. This enables `IN (SELECT ...)`
//! subqueries against HTTP datasets to produce one HTTP request per subquery
//! value.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{LargeStringArray, StringArray, StringViewArray};
use datafusion::common::stats::Statistics;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::{
    common::tree_node::{Transformed, TransformedResult, TreeNode},
    config::ConfigOptions,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::expressions::Column,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
        SortOrderPushdownResult,
        coalesce_partitions::CoalescePartitionsExec,
        execution_plan::{
            Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
        },
        expressions::PhysicalSortExpr,
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
        },
        joins::HashJoinExec,
        metrics::MetricsSet,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_expr::JoinType;
use futures::TryStreamExt;

use data_components::http::provider::HttpExec;

/// Maximum number of values to materialize from the build side of the join.
const MAX_MATERIALIZED_VALUES: usize = 20_000;

/// HTTP request parameter column names eligible for pushdown.
const HTTP_REQUEST_PARAM_COLUMNS: &[&str] = &[
    "request_headers",
    "request_path",
    "request_query",
    "request_body",
];

/// A [`PhysicalOptimizerRule`] that detects `HashJoinExec` nodes (`LeftSemi` or
/// `RightSemi`) where one side contains an `HttpExec` and the other side is a
/// small, materializable subquery. Instead of materializing during planning, it
/// emits an [`HttpWithDeferredParamsExec`] node that defers execution to
/// runtime.
#[derive(Debug)]
pub struct HttpParamsPushdown;

impl PhysicalOptimizerRule for HttpParamsPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(try_rewrite_hash_join).data()
    }

    fn name(&self) -> &'static str {
        "HttpParamsPushdown"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Attempt to rewrite a single plan node. Only fires when the node is a
/// `HashJoinExec(LeftSemi | RightSemi)` where one side contains an `HttpExec`
/// and the join key references an HTTP request parameter column.
///
/// `DataFusion` produces semi-joins for `IN (SELECT ...)` subqueries.
/// The optimizer may choose either orientation depending on cost estimates:
/// - `LeftSemi`:  `HttpExec` on left  (probe), subquery on right (build)
/// - `RightSemi`: subquery on left (build/collect), `HttpExec` on right (probe)
///   Returns an error if an unsupported join type (e.g. `Inner`) is used with
///   HTTP request parameter columns, since that would silently produce incorrect
///   results without the pushdown.
fn try_rewrite_hash_join(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
    let Some(join_exec) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(Transformed::no(plan));
    };

    // Determine which side has HttpExec and which is the build side.
    // Also capture the build-side column index so we materialize the correct join key.
    //
    // Only semi-joins are supported. Inner joins are intentionally excluded because
    // the rewrite replaces the entire HashJoinExec with HttpWithDeferredParamsExec
    // which only outputs http_side.schema() — build-side columns are dropped,
    // changing the join output schema and potentially causing incorrect results
    // or downstream operator failures when build-side columns are referenced.
    let (http_side, build_side, col_name, build_col_index) = match join_exec.join_type() {
        JoinType::LeftSemi => {
            let on = join_exec.on();
            if on.len() != 1 {
                return Ok(Transformed::no(plan));
            }
            let (left_col_expr, right_col_expr) = &on[0];
            let Some(left_col) = left_col_expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(right_col) = right_col_expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            let name = left_col.name().to_string();
            if !HTTP_REQUEST_PARAM_COLUMNS.contains(&name.as_str()) {
                return Ok(Transformed::no(plan));
            }
            if !contains_http_exec(join_exec.left()) {
                return Ok(Transformed::no(plan));
            }
            (
                Arc::clone(join_exec.left()),
                Arc::clone(join_exec.right()),
                name,
                right_col.index(),
            )
        }
        JoinType::RightSemi => {
            let on = join_exec.on();
            if on.len() != 1 {
                return Ok(Transformed::no(plan));
            }
            let (left_col_expr, right_col_expr) = &on[0];
            let Some(left_col) = left_col_expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(right_col) = right_col_expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(plan));
            };
            let name = right_col.name().to_string();
            if !HTTP_REQUEST_PARAM_COLUMNS.contains(&name.as_str()) {
                return Ok(Transformed::no(plan));
            }
            if !contains_http_exec(join_exec.right()) {
                return Ok(Transformed::no(plan));
            }
            (
                Arc::clone(join_exec.right()),
                Arc::clone(join_exec.left()),
                name,
                left_col.index(),
            )
        }
        // Detect unsupported join types that reference HTTP request param
        // columns with an HttpExec child — these would silently produce
        // incorrect results without pushdown. Return a clear error so the
        // user can rewrite to `IN (SELECT ...)`.
        _ => {
            if let Some(col_name) = http_join_param_column(join_exec) {
                return Err(DataFusionError::Plan(format!(
                    "JOIN on HTTP request parameter column '{col_name}' is not supported. Use `WHERE {col_name} IN (SELECT ...)` instead."
                )));
            }
            return Ok(Transformed::no(plan));
        }
    };

    tracing::debug!(
        "HttpParamsPushdown: rewriting HashJoinExec for column '{col_name}' into HttpWithDeferredParamsExec"
    );

    let exec = HttpWithDeferredParamsExec::new(http_side, build_side, col_name, build_col_index);
    Ok(Transformed::yes(Arc::new(exec)))
}

/// If the `HashJoinExec` has an `HttpExec` on one side and the join key is an
/// HTTP request parameter column, return the column name. Used to detect
/// unsupported join types that would silently produce incorrect results.
fn http_join_param_column(join_exec: &HashJoinExec) -> Option<String> {
    let on = join_exec.on();
    if on.len() != 1 {
        return None;
    }
    let (left_col_expr, right_col_expr) = &on[0];

    let left_has_http = contains_http_exec(join_exec.left());
    let right_has_http = contains_http_exec(join_exec.right());

    if left_has_http && !right_has_http {
        let col = left_col_expr.downcast_ref::<Column>()?;
        let name = col.name();
        if HTTP_REQUEST_PARAM_COLUMNS.contains(&name) {
            return Some(name.to_string());
        }
    } else if right_has_http && !left_has_http {
        let col = right_col_expr.downcast_ref::<Column>()?;
        let name = col.name();
        if HTTP_REQUEST_PARAM_COLUMNS.contains(&name) {
            return Some(name.to_string());
        }
    }
    None
}

/// Recursively check whether `plan` or any descendant is an `HttpExec`.
fn contains_http_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.downcast_ref::<HttpExec>().is_some() {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| contains_http_exec(child))
}

/// Find the first `HttpExec` in the plan tree and return a reference to it.
fn find_http_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&HttpExec> {
    if let Some(http_exec) = plan.downcast_ref::<HttpExec>() {
        return Some(http_exec);
    }
    for child in plan.children() {
        if let Some(found) = find_http_exec(child) {
            return found.into();
        }
    }
    None
}

/// Walk the plan tree and mark any `HttpExec` nodes with deferred partitions
/// so that EXPLAIN output shows `partitions=deferred` instead of the
/// static template partition list.
fn mark_http_exec_deferred(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let fallback = Arc::clone(&plan);
    plan.transform_down(|node| {
        let Some(http_exec) = node.downcast_ref::<HttpExec>() else {
            return Ok(Transformed::no(node));
        };
        let marked: Arc<dyn ExecutionPlan> = Arc::new(http_exec.clone().with_deferred_partitions());
        Ok(Transformed::yes(marked))
    })
    .data()
    .unwrap_or(fallback)
}

/// A lazy [`ExecutionPlan`] node that defers build-side materialization to
/// execution time. During `execute()`, it:
/// 1. Collects all string values from the build side (fully async).
/// 2. Deduplicates them.
/// 3. Validates against `max_request_partitions`.
/// 4. Replaces the `HttpExec` in the http side with expanded partitions.
/// 5. Executes the rewritten http side and streams results.
#[derive(Debug)]
struct HttpWithDeferredParamsExec {
    /// The subtree containing the `HttpExec` (probe side of the join).
    http_side: Arc<dyn ExecutionPlan>,
    /// The subquery/table to materialize (build side of the join).
    build_side: Arc<dyn ExecutionPlan>,
    /// Which HTTP virtual column to push the materialized values into.
    col_name: String,
    /// Column index in the build side output to materialize values from.
    build_col_index: usize,
    /// Cached plan properties derived from the http side.
    properties: Arc<PlanProperties>,
    /// Metrics populated at execution time for EXPLAIN ANALYZE visibility.
    metrics: ExecutionPlanMetricsSet,
}

impl HttpWithDeferredParamsExec {
    fn new(
        http_side: Arc<dyn ExecutionPlan>,
        build_side: Arc<dyn ExecutionPlan>,
        col_name: String,
        build_col_index: usize,
    ) -> Self {
        // Always report 1 output partition. DataFusion reads properties()
        // at planning time to schedule downstream nodes, but the true
        // partition count isn't known until execute() materializes the build
        // side. We merge all rewritten partitions into one stream at
        // execution time (same pattern as CoalescePartitionsExec).
        let schema = http_side.schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        // Mark the template HttpExec as deferred so EXPLAIN shows
        // "partitions=deferred" instead of the static partition list.
        let http_side = mark_http_exec_deferred(http_side);

        Self {
            http_side,
            build_side,
            col_name,
            build_col_index,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for HttpWithDeferredParamsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HttpWithDeferredParamsExec: deferred_param={}, build_side={}",
            self.col_name,
            self.build_side.name()
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for HttpWithDeferredParamsExec {
    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "HttpWithDeferredParamsExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "HttpWithDeferredParamsExec"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(self.properties().eq_properties.schema())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<(), DataFusionError> {
        check_default_invariants(self, check)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    /// No specific distribution required for either child.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; 2]
    }

    /// No ordering required for either child.
    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        vec![None, None]
    }

    /// Neither child benefits from increased parallelism
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }

    /// Repartitioning is not supported — the true partition count is unknown
    /// until the build side is materialized at execution time.
    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> Result<Arc<Statistics>, DataFusionError> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }

    /// Limit pushdown is not supported — the output stream is assembled
    /// dynamically at execution time.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription, DataFusionError> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(
        &self,
        _state: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>, DataFusionError> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.http_side, &self.build_side]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 2 {
            return Err(DataFusionError::Internal(
                "HttpWithDeferredParamsExec requires exactly 2 children".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.col_name.clone(),
            self.build_col_index,
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let children = self.children().into_iter().cloned().collect();
        self.with_new_children(children)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let http_side = Arc::clone(&self.http_side);
        let build_side = Arc::clone(&self.build_side);
        let col_name = self.col_name.clone();
        let build_col_index = self.build_col_index;
        let schema = Arc::clone(&self.http_side.schema());
        let ctx = Arc::clone(&context);
        let build_side_values =
            MetricBuilder::new(&self.metrics).global_counter("build_side_values");
        let http_partitions =
            MetricBuilder::new(&self.metrics).global_counter("http_exec_partitions");

        let stream = futures::stream::once(async move {
            // 1. Materialize build side (fully async).
            let values = materialize_string_values(&build_side, &ctx, build_col_index).await?;

            build_side_values.add(values.len());

            if values.is_empty() {
                tracing::debug!(
                    "HttpWithDeferredParamsExec: build side produced no values for column '{col_name}', returning empty"
                );
                let empty_stream: SendableRecordBatchStream =
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema,
                        futures::stream::empty(),
                    ));
                return Ok::<SendableRecordBatchStream, DataFusionError>(empty_stream);
            }

            tracing::debug!(
                "HttpWithDeferredParamsExec: materialized {} unique values for column '{col_name}'",
                values.len(),
            );

            // 2. Rewrite HttpExec partitions with materialized values.
            let rewritten = replace_http_exec_with_expanded_params(&http_side, &col_name, &values)?;

            if let Some(http_exec) = find_http_exec(&rewritten) {
                http_partitions.add(http_exec.partitions().len());
            }

            // 3. Merge all rewritten partitions into a single output stream
            //    via CoalescePartitionsExec, which spawns each partition on
            //    the thread pool for true multi-threaded parallelism.
            let merged_plan: Arc<dyn ExecutionPlan> = Arc::new(
                CoalescePartitionsExec::new(rewritten),
            );
            let merged: SendableRecordBatchStream = merged_plan.execute(0, ctx)?;
            Ok(merged)
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.http_side.schema(),
            stream,
        )))
    }
}

/// Execute the build-side plan asynchronously and collect all unique non-null,
/// deduplicated string values from the specified column. NULLs are skipped
/// (SQL `IN` never matches NULL), but empty strings are preserved — they are
/// valid SQL values and downstream `HttpExec`/provider validation decides
/// whether they are acceptable for the target column. Returns an error if the
/// subquery produces more than [`MAX_MATERIALIZED_VALUES`] unique values.
async fn materialize_string_values(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
    col_index: usize,
) -> Result<Vec<String>, DataFusionError> {
    let batches = datafusion::physical_plan::collect(Arc::clone(plan), Arc::clone(context)).await?;

    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for (batch_index, batch) in batches.iter().enumerate() {
        if batch.num_columns() <= col_index {
            return Err(DataFusionError::Internal(format!(
                "HttpWithDeferredParamsExec: build-side batch {batch_index} is missing column index {col_index}; batch has {} columns",
                batch.num_columns()
            )));
        }
        let array = batch.column(col_index);

        let string_iter: Box<dyn Iterator<Item = Option<&str>>> =
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                Box::new(arr.iter())
            } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                Box::new(arr.iter())
            } else if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                Box::new(arr.iter())
            } else {
                return Err(DataFusionError::Internal(format!(
                    "HttpWithDeferredParamsExec: expected string column, got {:?}",
                    array.data_type()
                )));
            };

        for val in string_iter.flatten() {
            if seen.insert(val.to_string()) {
                if values.len() >= MAX_MATERIALIZED_VALUES {
                    return Err(DataFusionError::Plan(format!(
                        "HttpWithDeferredParamsExec: subquery produced more than {MAX_MATERIALIZED_VALUES} unique values, aborting pushdown"
                    )));
                }
                values.push(val.to_string());
            }
        }
    }

    Ok(values)
}

/// Walk the `plan` tree, find the `HttpExec` leaf, and replace it with a new
/// `HttpExec` whose partitions include the materialized values for the given
/// column. Validates against `max_request_partitions`.
fn replace_http_exec_with_expanded_params(
    plan: &Arc<dyn ExecutionPlan>,
    col_name: &str,
    values: &[String],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let col = col_name.to_string();
    let vals: Arc<[String]> = values.into();

    Arc::clone(plan)
        .transform_down(move |node| {
            let Some(http_exec) = node.downcast_ref::<HttpExec>() else {
                return Ok(Transformed::no(node));
            };

            let new_exec = http_exec.with_expanded_params(&col, &vals)?;
            Ok(Transformed::yes(Arc::new(new_exec)))
        })
        .data()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::common::NullEquality;
    use datafusion::config::ConfigOptions;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion_datasource::memory::MemorySourceConfig;
    use reqwest::Client;
    use url::Url;

    use data_components::http::provider::HttpTableProvider;

    /// Build an `HttpExec` with one empty partition.
    ///
    /// Configures `allowed_request_paths` so that
    /// `with_expanded_params("request_path", …)` succeeds for paths
    /// matching `/*`.
    fn make_http_exec() -> Arc<dyn ExecutionPlan> {
        let provider = HttpTableProvider::new(
            Url::parse("http://localhost:9999/api").expect("valid url"),
            Client::new(),
            "json".to_string(),
            false,
        )
        .with_allowed_paths(["/*"])
        .expect("valid path glob");
        let provider = Arc::new(provider);
        let schema: SchemaRef = Arc::new(HttpTableProvider::base_table_schema());
        Arc::new(HttpExec::new(
            schema,
            provider,
            vec![(None, None, None, None)],
            None,
        ))
    }

    /// Build a simple in-memory `ExecutionPlan` producing the given string
    /// column (`col_name`) with the given values.
    fn make_memory_exec(col_name: &str, values: &[Option<&str>]) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            DataType::Utf8,
            true,
        )]));
        let array = StringArray::from(values.to_vec());
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).expect("valid batch");
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("valid memory exec")
    }

    /// Build a `HashJoinExec` (semi-join) for testing the optimizer rule.
    fn make_hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_col: &str,
        left_idx: usize,
        right_col: &str,
        right_idx: usize,
        join_type: JoinType,
    ) -> Arc<dyn ExecutionPlan> {
        let on: Vec<(
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )> = vec![(
            Arc::new(Column::new(left_col, left_idx)),
            Arc::new(Column::new(right_col, right_idx)),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &join_type,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .expect("valid HashJoinExec"),
        )
    }

    // -----------------------------------------------------------------------
    // Optimizer rule rewrite tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_left_semi_rewrite_produces_deferred_exec() {
        // LeftSemi: HttpExec on left, subquery on right
        let http = make_http_exec();
        let build = make_memory_exec("request_headers", &[Some("hdr1"), Some("hdr2")]);

        // request_headers is column index 3 in HttpExec schema, index 0 in build
        let join = make_hash_join(
            http,
            build,
            "request_headers",
            3,
            "request_headers",
            0,
            JoinType::LeftSemi,
        );

        let rule = HttpParamsPushdown;
        let result = rule
            .optimize(join, &ConfigOptions::new())
            .expect("optimize should succeed");

        // The top-level node should be HttpWithDeferredParamsExec
        assert!(
            result
                .downcast_ref::<HttpWithDeferredParamsExec>()
                .is_some(),
            "expected HttpWithDeferredParamsExec, got {}",
            result.name()
        );
        assert_eq!(
            result.children().len(),
            2,
            "should have http_side and build_side children"
        );
    }

    #[test]
    fn test_right_semi_rewrite_produces_deferred_exec() {
        // RightSemi: subquery on left, HttpExec on right
        let build = make_memory_exec("request_path", &[Some("/a"), Some("/b")]);
        let http = make_http_exec();

        // In RightSemi: left_col is from build, right_col is from HttpExec
        let join = make_hash_join(
            build,
            http,
            "request_path",
            0,
            "request_path",
            0,
            JoinType::RightSemi,
        );

        let rule = HttpParamsPushdown;
        let result = rule
            .optimize(join, &ConfigOptions::new())
            .expect("optimize should succeed");

        let deferred = result
            .downcast_ref::<HttpWithDeferredParamsExec>()
            .expect("expected HttpWithDeferredParamsExec");
        assert_eq!(deferred.col_name, "request_path");
    }

    #[test]
    fn test_no_rewrite_for_non_http_param_column() {
        // Join on "content" (not an HTTP request param column) — should not rewrite
        let http = make_http_exec();
        let build = make_memory_exec("content", &[Some("hello")]);

        let join = make_hash_join(
            http,
            build,
            "content",
            4, // content is at index 4 in HttpExec schema
            "content",
            0,
            JoinType::LeftSemi,
        );

        let rule = HttpParamsPushdown;
        let result = rule
            .optimize(Arc::clone(&join), &ConfigOptions::new())
            .expect("optimize should succeed");

        // Should remain a HashJoinExec (no rewrite)
        assert!(
            result.downcast_ref::<HashJoinExec>().is_some(),
            "expected HashJoinExec unchanged, got {}",
            result.name()
        );
    }

    #[test]
    fn test_no_rewrite_without_http_exec() {
        // Both sides are memory — no HttpExec present
        let left = make_memory_exec("request_headers", &[Some("a")]);
        let right = make_memory_exec("request_headers", &[Some("b")]);

        let join = make_hash_join(
            left,
            right,
            "request_headers",
            0,
            "request_headers",
            0,
            JoinType::LeftSemi,
        );

        let rule = HttpParamsPushdown;
        let result = rule
            .optimize(Arc::clone(&join), &ConfigOptions::new())
            .expect("optimize should succeed");

        assert!(
            result.downcast_ref::<HashJoinExec>().is_some(),
            "expected HashJoinExec unchanged when no HttpExec is present"
        );
    }

    #[test]
    fn test_inner_join_on_http_param_returns_error() {
        let http = make_http_exec();
        let build = make_memory_exec("request_headers", &[Some("hdr1")]);

        let join = make_hash_join(
            http,
            build,
            "request_headers",
            3,
            "request_headers",
            0,
            JoinType::Inner,
        );

        let rule = HttpParamsPushdown;
        let _ = rule
            .optimize(join, &ConfigOptions::new())
            .expect_err("inner join on HTTP param should error");
    }

    // -----------------------------------------------------------------------
    // materialize_string_values tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_materialize_filters_nulls_preserves_empties() {
        let plan = make_memory_exec(
            "val",
            &[
                Some("a"),
                None,
                Some(""),
                Some("b"),
                None,
                Some("a"),
                Some(""),
            ],
        );
        let ctx = Arc::new(TaskContext::default());

        let values = materialize_string_values(&plan, &ctx, 0)
            .await
            .expect("materialize should succeed");

        // NULLs are skipped, empty strings are preserved, duplicates are removed
        assert_eq!(values.len(), 3, "expected 3 unique values, got {values:?}");
        assert!(values.contains(&"a".to_string()));
        assert!(values.contains(&"b".to_string()));
        assert!(values.contains(&String::new()));
    }

    #[tokio::test]
    async fn test_materialize_deduplicates() {
        let plan = make_memory_exec(
            "val",
            &[Some("x"), Some("y"), Some("x"), Some("z"), Some("y")],
        );
        let ctx = Arc::new(TaskContext::default());

        let values = materialize_string_values(&plan, &ctx, 0)
            .await
            .expect("materialize should succeed");

        assert_eq!(values.len(), 3, "expected 3 unique values");
    }

    #[tokio::test]
    async fn test_materialize_empty_build_side() {
        let plan = make_memory_exec("val", &[]);
        let ctx = Arc::new(TaskContext::default());

        let values = materialize_string_values(&plan, &ctx, 0)
            .await
            .expect("materialize should succeed");

        assert!(values.is_empty(), "expected empty vec for empty input");
    }

    #[tokio::test]
    async fn test_materialize_all_nulls_returns_empty() {
        let plan = make_memory_exec("val", &[None, None, None]);
        let ctx = Arc::new(TaskContext::default());

        let values = materialize_string_values(&plan, &ctx, 0)
            .await
            .expect("materialize should succeed");

        assert!(values.is_empty(), "all-null input should produce empty vec");
    }

    // -----------------------------------------------------------------------
    // replace_http_exec_with_partitions tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_replace_http_exec_expands_partitions() {
        let http = make_http_exec();
        let values = vec!["/v1".to_string(), "/v2".to_string(), "/v3".to_string()];

        let rewritten = replace_http_exec_with_expanded_params(&http, "request_path", &values)
            .expect("replace should succeed");

        let http_exec = find_http_exec(&rewritten).expect("should contain HttpExec");
        assert_eq!(
            http_exec.partitions().len(),
            3,
            "expected 3 partitions from 1 × 3 values"
        );

        // Verify each partition has the correct path value
        for (i, val) in values.iter().enumerate() {
            assert_eq!(
                http_exec.partitions()[i].0,
                Some(val.clone()),
                "partition {i} should have request_path={val}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // contains_http_exec / find_http_exec utility tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_contains_http_exec_positive() {
        let http = make_http_exec();
        assert!(contains_http_exec(&http), "should find HttpExec directly");

        // Wrapped in CoalescePartitionsExec
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(http));
        assert!(
            contains_http_exec(&wrapped),
            "should find HttpExec inside CoalescePartitionsExec"
        );
    }

    #[test]
    fn test_contains_http_exec_negative() {
        let mem = make_memory_exec("col", &[Some("val")]);
        assert!(
            !contains_http_exec(&mem),
            "should not find HttpExec in memory exec"
        );
    }
}
