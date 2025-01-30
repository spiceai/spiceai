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
use async_stream::stream;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
        DFSchemaRef,
    },
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
    },
    prelude::Expr,
};
use datafusion_federation::FederatedPlanNode;
use futures::StreamExt;
use std::{
    any::Any,
    collections::HashSet,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::request::RequestContext;

#[derive(Debug, Default)]
pub struct BytesProcessedOptimizerRule {}

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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
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
        let mut stream = self.input_exec.execute(partition, Arc::clone(&context))?;
        let schema = stream.schema();

        let Some(request_context) = context.session_config().get_extension::<RequestContext>()
        else {
            // This should never happen if all queries are run through the query builder, so if it does its a bug we need to catch in development.
            panic!("The request context was not provided to BytesProcessedExec, report a bug at https://github.com/spiceai/spiceai/issues")
        };

        let bytes_processed_stream = stream! {
            let mut bytes_processed = 0u64;
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        bytes_processed += batch.get_array_memory_size() as u64;
                        yield Ok(batch)
                    }
                    Err(e) => {
                        yield Err(e);
                    }
                }
            }
            crate::metrics::telemetry::track_bytes_processed(
                bytes_processed,
                &request_context.to_dimensions(),
            );
        };

        let stream_adapter = RecordBatchStreamAdapter::new(schema, bytes_processed_stream);

        Ok(Box::pin(stream_adapter))
    }
}
