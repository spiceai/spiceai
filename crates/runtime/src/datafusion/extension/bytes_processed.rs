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

//! Adds telemetry to leaf nodes (i.e. `TableScans`) to track the number of bytes scanned during query execution.
use crate::request::RequestContext;
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::{
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, stream::RecordBatchStreamAdapter,
    },
    prelude::Expr,
};
use datafusion_federation::FederatedPlanNode;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{
    any::Any,
    collections::HashSet,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::Arc,
};

#[derive(Debug, Default)]
pub struct BytesProcessedOptimizerRule {}

struct BytesProcessedStream {
    inner: SendableRecordBatchStream,
    bytes_processed: u64,
    request_context: Arc<RequestContext>,
}

impl BytesProcessedStream {
    pub fn new(inner: SendableRecordBatchStream, request_context: Arc<RequestContext>) -> Self {
        Self {
            inner,
            bytes_processed: 0,
            request_context,
        }
    }

    fn emit_bytes_processed(&self) {
        crate::metrics::telemetry::track_bytes_processed(
            self.bytes_processed,
            &self.request_context.to_dimensions(),
        );
    }
}

impl Stream for BytesProcessedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                self.bytes_processed += batch.get_array_memory_size() as u64;
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(None) => {
                self.emit_bytes_processed();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

/// Walk over the plan and insert a `BytesProcessedNode` as the parent of any `TableScans` and `FederationNodes`.
///
/// This should be added as an optimizer rule to run after the `PushDownLimit` rule, since it doesn't support pushing
/// down limits for extension nodes.
impl OptimizerRule for BytesProcessedOptimizerRule {
    /// Walk over the plan and insert a `BytesProcessedNode` as the parent of any `TableScans` and `FederationNodes`.
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| match plan {
            LogicalPlan::Extension(extension) => {
                // If the extension is already a BytesProcessedNode, don't add another one.
                if extension
                    .node
                    .as_any()
                    .downcast_ref::<BytesProcessedNode>()
                    .is_some()
                {
                    return Ok(Transformed::new(
                        LogicalPlan::Extension(extension),
                        false,
                        TreeNodeRecursion::Jump, // Don't process any further children of this sub-tree.
                    ));
                }

                let plan_node = extension.node.as_any().downcast_ref::<FederatedPlanNode>();

                if plan_node.is_some() {
                    let bytes_processed =
                        BytesProcessedNode::new(LogicalPlan::Extension(extension.clone()));
                    let ext_node = Extension {
                        node: Arc::new(bytes_processed),
                    };
                    Ok(Transformed::new(
                        LogicalPlan::Extension(ext_node),
                        true,
                        TreeNodeRecursion::Jump,
                    ))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            LogicalPlan::TableScan(table_scan) => {
                let bytes_processed = BytesProcessedNode::new(LogicalPlan::TableScan(table_scan));
                let ext_node = Extension {
                    node: Arc::new(bytes_processed),
                };
                Ok(Transformed::new(
                    LogicalPlan::Extension(ext_node),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &'static str {
        "bytes_processed_optimizer_rule"
    }
}

impl BytesProcessedOptimizerRule {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(PartialOrd)]
pub(crate) struct BytesProcessedNode {
    pub(super) input: LogicalPlan,
}

impl BytesProcessedNode {
    pub(crate) fn new(input: LogicalPlan) -> Self {
        assert!(input.inputs().is_empty(), "should have no inputs");
        Self { input }
    }
}

impl Debug for BytesProcessedNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for BytesProcessedNode {
    fn name(&self) -> &'static str {
        "BytesProcessedNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BytesProcessedNode")
    }

    /// Returns the necessary input columns for this node required to compute
    /// the columns in the output schema
    ///
    /// This is used for projection push-down when `DataFusion` has determined that
    /// only a subset of the output columns of this node are needed by its parents.
    /// This API is used to tell `DataFusion` which, if any, of the input columns are no longer
    /// needed.
    ///
    /// Return `None`, the default, if this information can not be determined.
    /// Returns `Some(_)` with the column indices for each child of this node that are
    /// needed to compute `output_columns`
    fn necessary_children_exprs(&self, output_columns: &[usize]) -> Option<Vec<Vec<usize>>> {
        // Since the input & output schema is the same, output columns require their corresponding index in the input columns.
        Some(vec![output_columns.to_vec()])
    }

    /// A list of output columns (e.g. the names of columns in
    /// `self.schema()`) for which predicates can not be pushed below
    /// this node without changing the output.
    ///
    /// By default, this returns all columns and thus prevents any
    /// predicates from being pushed below this node.
    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // Allow filters for all columns to be pushed down
        HashSet::new()
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Self { input })
    }
}

impl PartialEq<BytesProcessedNode> for BytesProcessedNode {
    fn eq(&self, other: &BytesProcessedNode) -> bool {
        self.input == other.input
    }
}

impl Eq for BytesProcessedNode {}

impl Hash for BytesProcessedNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

pub(crate) struct BytesProcessedExec {
    input_exec: Arc<dyn ExecutionPlan>,
}

impl BytesProcessedExec {
    pub(crate) fn new(input_exec: Arc<dyn ExecutionPlan>) -> Self {
        Self { input_exec }
    }
}

impl std::fmt::Debug for BytesProcessedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BytesProcessedExec")
    }
}

impl DisplayAs for BytesProcessedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "BytesProcessedExec")
            }
        }
    }
}

impl ExecutionPlan for BytesProcessedExec {
    fn name(&self) -> &'static str {
        "BytesProcessedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_exec]
    }

    /// Prevents the introduction of additional `RepartitionExec` and processing input in parallel.
    /// This guarantees that the input is processed as a single stream, preserving the order of the data.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "should have one input");
        let Some(input) = children.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Arc::new(Self { input_exec: input }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let stream = self.input_exec.execute(partition, Arc::clone(&context))?;
        let schema = stream.schema();

        let Some(request_context) = context.session_config().get_extension::<RequestContext>()
        else {
            // This should never happen if all queries are run through the query builder, so if it does its a bug we need to catch in development.
            panic!(
                "The request context was not provided to BytesProcessedExec, report a bug at https://github.com/spiceai/spiceai/issues"
            )
        };

        let bytes_processed_stream = BytesProcessedStream::new(stream, request_context);

        let stream_adapter = RecordBatchStreamAdapter::new(schema, bytes_processed_stream);

        Ok(Box::pin(stream_adapter))
    }
}

#[cfg(test)]
mod tests {
    use crate::datafusion::extension::bytes_processed::BytesProcessedExec;
    use crate::{Runtime, RuntimeBuilder};
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::physical_expr::expressions::col as physical_col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use std::sync::Arc;

    fn make_test_table() -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0i64..10000))],
        )?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    #[allow(clippy::similar_names)]
    #[tokio::test]
    async fn test_preserve_order_pushdown() -> Result<()> {
        let runtime: Runtime = RuntimeBuilder::new().build().await;
        let test_table = make_test_table()?;

        let data_source_exec = test_table
            .scan(&runtime.df.ctx.state(), None, &[], None)
            .await?;

        let sort_exec = SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new_default(physical_col(
                    "id",
                    data_source_exec.schema().as_ref(),
                )?)
                .desc()
                .nulls_last(),
            ]),
            data_source_exec,
        );

        let final_plan: Arc<dyn ExecutionPlan> =
            Arc::new(BytesProcessedExec::new(Arc::new(sort_exec)));

        /*
           At this point `final_plan` is:
           ┌───────────────────────────┐
           │     BytesProcessedExec    │
           │    --------------------   │
           │     BytesProcessedExec    │
           └─────────────┬─────────────┘
           ┌─────────────┴─────────────┐
           │          SortExec         │
           │    --------------------   │
           │    id@0 DESC NULLS LAST   │
           └─────────────┬─────────────┘
           ┌─────────────┴─────────────┐
           │       DataSourceExec      │
           │    --------------------   │
           │        bytes: 80096       │
           │       format: memory      │
           │          rows: 1          │
           └───────────────────────────┘
        */

        // Optimizer is a bag of rules
        let optimizer = PhysicalOptimizer::new();
        let config = runtime.df.ctx.state().config_options().clone();

        // Fold over the default rules to apply the same optimizations DF would at runtime
        let optimized = optimizer
            .rules
            .iter()
            .fold(Arc::clone(&final_plan), |plan, rule| {
                rule.optimize(plan, &config).expect("Must optimize plan")
            });

        // No semantic eq implemented, so this is the easiest way to compare plans
        assert_eq!(
            displayable(final_plan.as_ref()).tree_render().to_string(),
            displayable(optimized.as_ref()).tree_render().to_string()
        );

        Ok(())
    }
}
