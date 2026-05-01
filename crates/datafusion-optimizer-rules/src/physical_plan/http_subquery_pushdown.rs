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

//! [`HttpParamsPushdown`] rewrites `HashJoinExec` (semi-join or inner-join)
//! over `HttpExec` by emitting a lazy [`HttpWithDeferredParamsExec`] node. The node
//! defers build-side materialization to execution time (fully async), then
//! injects the collected values as HTTP partition filters. This enables
//! `IN (SELECT ...)` subqueries and `JOIN` queries against HTTP datasets to
//! produce one HTTP request per subquery/join value.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow::array::{LargeStringArray, StringArray, StringViewArray};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::{
    common::tree_node::{Transformed, TransformedResult, TreeNode},
    config::ConfigOptions,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::expressions::Column,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        coalesce_partitions::CoalescePartitionsExec,
        execution_plan::{Boundedness, EmissionType},
        joins::HashJoinExec,
        metrics::MetricsSet,
        metrics::{ExecutionPlanMetricsSet, MetricBuilder},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_expr::JoinType;
use futures::TryStreamExt;

use data_components::http::provider::HttpExec;

/// Maximum number of values to materialize from the build side of the join.
const MAX_MATERIALIZED_VALUES: usize = 50_000;

/// HTTP virtual column names that can be pushed down as partition filters.
const HTTP_VIRTUAL_COLUMNS: &[&str] = &[
    "request_headers",
    "request_path",
    "request_query",
    "request_body",
];

/// A [`PhysicalOptimizerRule`] that detects `HashJoinExec` nodes (`LeftSemi`,
/// `RightSemi`, or `Inner`) where one side contains an `HttpExec` and the other
/// side is a small, materializable subquery/table. Instead of materializing
/// during planning, it emits an [`HttpWithDeferredParamsExec`] node that defers
/// execution to runtime.
#[derive(Debug)]
pub struct HttpParamsPushdown;

impl PhysicalOptimizerRule for HttpParamsPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| Ok(try_rewrite_hash_join(node)))
            .data()
    }

    fn name(&self) -> &'static str {
        "HttpParamsPushdown"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Attempt to rewrite a single plan node. Only fires when the node is a
/// `HashJoinExec(LeftSemi | RightSemi | Inner)` where one side contains an
/// `HttpExec` and the join key references an HTTP virtual column.
///
/// `DataFusion` may produce different orientations depending on cost estimates:
/// - `LeftSemi`:  `HttpExec` on left  (probe), subquery on right (build)
/// - `RightSemi`: subquery on left (build/collect), `HttpExec` on right (probe)
/// - `Inner`:    either side may have `HttpExec`; the build side is determined
///   by `CollectLeft` mode (left is collected/materialized)
fn try_rewrite_hash_join(plan: Arc<dyn ExecutionPlan>) -> Transformed<Arc<dyn ExecutionPlan>> {
    let Some(join_exec) = plan.as_any().downcast_ref::<HashJoinExec>() else {
        return Transformed::no(plan);
    };

    // Determine which side has HttpExec and which is the build side.
    let (http_side, build_side, col_name) = match join_exec.join_type() {
        JoinType::LeftSemi => {
            let on = join_exec.on();
            if on.len() != 1 {
                return Transformed::no(plan);
            }
            let (left_col_expr, _) = &on[0];
            let Some(left_col) = left_col_expr.as_any().downcast_ref::<Column>() else {
                return Transformed::no(plan);
            };
            let name = left_col.name().to_string();
            if !HTTP_VIRTUAL_COLUMNS.contains(&name.as_str()) {
                return Transformed::no(plan);
            }
            if !contains_http_exec(join_exec.left()) {
                return Transformed::no(plan);
            }
            (
                Arc::clone(join_exec.left()),
                Arc::clone(join_exec.right()),
                name,
            )
        }
        JoinType::RightSemi => {
            let on = join_exec.on();
            if on.len() != 1 {
                return Transformed::no(plan);
            }
            let (_, right_col_expr) = &on[0];
            let Some(right_col) = right_col_expr.as_any().downcast_ref::<Column>() else {
                return Transformed::no(plan);
            };
            let name = right_col.name().to_string();
            if !HTTP_VIRTUAL_COLUMNS.contains(&name.as_str()) {
                return Transformed::no(plan);
            }
            if !contains_http_exec(join_exec.right()) {
                return Transformed::no(plan);
            }
            (
                Arc::clone(join_exec.right()),
                Arc::clone(join_exec.left()),
                name,
            )
        }
        JoinType::Inner => {
            let on = join_exec.on();
            if on.len() != 1 {
                return Transformed::no(plan);
            }
            let (left_col_expr, right_col_expr) = &on[0];

            let left_has_http = contains_http_exec(join_exec.left());
            let right_has_http = contains_http_exec(join_exec.right());

            if left_has_http && !right_has_http {
                let Some(left_col) = left_col_expr.as_any().downcast_ref::<Column>() else {
                    return Transformed::no(plan);
                };
                let name = left_col.name().to_string();
                if !HTTP_VIRTUAL_COLUMNS.contains(&name.as_str()) {
                    return Transformed::no(plan);
                }
                (
                    Arc::clone(join_exec.left()),
                    Arc::clone(join_exec.right()),
                    name,
                )
            } else if right_has_http && !left_has_http {
                let Some(right_col) = right_col_expr.as_any().downcast_ref::<Column>() else {
                    return Transformed::no(plan);
                };
                let name = right_col.name().to_string();
                if !HTTP_VIRTUAL_COLUMNS.contains(&name.as_str()) {
                    return Transformed::no(plan);
                }
                (
                    Arc::clone(join_exec.right()),
                    Arc::clone(join_exec.left()),
                    name,
                )
            } else {
                return Transformed::no(plan);
            }
        }
        _ => return Transformed::no(plan),
    };

    tracing::debug!(
        "HttpParamsPushdown: rewriting HashJoinExec for column '{col_name}' into HttpWithDeferredParamsExec"
    );

    let exec = HttpWithDeferredParamsExec::new(http_side, build_side, col_name);
    Transformed::yes(Arc::new(exec))
}

/// Recursively check whether `plan` or any descendant is an `HttpExec`.
fn contains_http_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().downcast_ref::<HttpExec>().is_some() {
        return true;
    }
    plan.children()
        .iter()
        .any(|child| contains_http_exec(child))
}

/// Find the first `HttpExec` in the plan tree and return a reference to it.
fn find_http_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<&HttpExec> {
    if let Some(http_exec) = plan.as_any().downcast_ref::<HttpExec>() {
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
        let Some(http_exec) = node.as_any().downcast_ref::<HttpExec>() else {
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
    /// Cached plan properties derived from the http side.
    properties: PlanProperties,
    /// Metrics populated at execution time for EXPLAIN ANALYZE visibility.
    metrics: ExecutionPlanMetricsSet,
}

impl HttpWithDeferredParamsExec {
    fn new(
        http_side: Arc<dyn ExecutionPlan>,
        build_side: Arc<dyn ExecutionPlan>,
        col_name: String,
    ) -> Self {
        // Always report 1 output partition. DataFusion reads properties()
        // at planning time to schedule downstream nodes, but the true
        // partition count isn't known until execute() materializes the build
        // side. We merge all rewritten partitions into one stream at
        // execution time (same pattern as CoalescePartitionsExec).
        let schema = http_side.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        // Mark the template HttpExec as deferred so EXPLAIN shows
        // "partitions=deferred" instead of the static partition list.
        let http_side = mark_http_exec_deferred(http_side);

        Self {
            http_side,
            build_side,
            col_name,
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

impl ExecutionPlan for HttpWithDeferredParamsExec {
    fn name(&self) -> &'static str {
        "HttpWithDeferredParamsExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let http_side = Arc::clone(&self.http_side);
        let build_side = Arc::clone(&self.build_side);
        let col_name = self.col_name.clone();
        let schema = Arc::clone(&self.http_side.schema());
        let ctx = Arc::clone(&context);
        let build_side_values =
            MetricBuilder::new(&self.metrics).global_counter("build_side_values");
        let http_partitions =
            MetricBuilder::new(&self.metrics).global_counter("http_exec_partitions");

        let stream = futures::stream::once(async move {
            // 1. Materialize build side (fully async).
            let values = materialize_string_values(&build_side, &ctx).await?;

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
            let rewritten = replace_http_exec_with_partitions(&http_side, &col_name, &values)?;

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

/// Execute the build-side plan asynchronously and collect all unique non-null
/// string values from its first column. Returns an error if the subquery
/// produces more than [`MAX_MATERIALIZED_VALUES`] unique values.
async fn materialize_string_values(
    plan: &Arc<dyn ExecutionPlan>,
    context: &Arc<TaskContext>,
) -> Result<Vec<String>, DataFusionError> {
    let batches = datafusion::physical_plan::collect(Arc::clone(plan), Arc::clone(context)).await?;

    let mut seen = HashSet::new();
    let mut values = Vec::new();
    for batch in &batches {
        if batch.num_columns() == 0 {
            continue;
        }
        let array = batch.column(0);

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
            if !val.is_empty() && seen.insert(val.to_string()) {
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
fn replace_http_exec_with_partitions(
    plan: &Arc<dyn ExecutionPlan>,
    col_name: &str,
    values: &[String],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Pre-validate partition count against the limit.
    if let Some(http_exec) = find_http_exec(plan) {
        let existing_count = http_exec.partitions().len();
        let new_count = existing_count.saturating_mul(values.len());
        if let Some(max) = http_exec.max_request_partitions()
            && new_count > max
        {
            return Err(DataFusionError::Plan(format!(
                "HttpWithDeferredParamsExec: pushdown would create {new_count} partitions (existing {existing_count} x {val_count} values), which exceeds max_request_partitions={max}. Reduce the number of subquery values or increase max_request_partitions.",
                val_count = values.len(),
            )));
        }
    }

    let col = col_name.to_string();
    let vals: Arc<[String]> = values.into();

    Arc::clone(plan)
        .transform_down(move |node| {
            let Some(http_exec) = node.as_any().downcast_ref::<HttpExec>() else {
                return Ok(Transformed::no(node));
            };

            let existing = http_exec.partitions();
            let mut new_partitions = Vec::with_capacity(existing.len() * vals.len());

            for partition in existing {
                for value in vals.iter() {
                    let mut p = partition.clone();
                    match col.as_str() {
                        "request_headers" => p.3 = Some(value.clone()),
                        "request_path" => p.0 = Some(value.clone()),
                        "request_query" => p.1 = Some(value.clone()),
                        "request_body" => p.2 = Some(value.clone()),
                        _ => {}
                    }
                    new_partitions.push(p);
                }
            }

            tracing::debug!(
                "HttpWithDeferredParamsExec: replacing HttpExec with {} partitions (was {})",
                new_partitions.len(),
                existing.len()
            );

            let new_exec = HttpExec::new(
                Arc::clone(http_exec.projected_schema()),
                Arc::clone(http_exec.provider()),
                new_partitions,
                http_exec.limit(),
            );

            Ok(Transformed::yes(Arc::new(new_exec)))
        })
        .data()
}
