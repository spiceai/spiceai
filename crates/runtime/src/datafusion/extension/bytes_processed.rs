/*
Copyright 2024 The Spice.ai OSS Authors

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
        tree_node::{Transformed, TreeNode},
        DFSchemaRef,
    },
    config::ConfigOptions,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore},
    optimizer::AnalyzerRule,
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

use crate::datafusion::query::Protocol;

#[derive(Default)]
pub struct BytesProcessedAnalyzerRule {}

impl AnalyzerRule for BytesProcessedAnalyzerRule {
    /// Walk over the plan and insert a `BytesProcessedNode` as the parent of any `TableScans` and `FederationNodes`.
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let transformed_plan = plan.transform_up(|plan| match plan {
            LogicalPlan::TableScan(table_scan) => {
                let bytes_processed = BytesProcessedNode::new(LogicalPlan::TableScan(table_scan));
                let ext_node = Extension {
                    node: Arc::new(bytes_processed),
                };
                Ok(Transformed::yes(LogicalPlan::Extension(ext_node)))
            }
            LogicalPlan::Extension(extension) => {
                let plan_node = extension.node.as_any().downcast_ref::<FederatedPlanNode>();

                if plan_node.is_some() {
                    let bytes_processed =
                        BytesProcessedNode::new(LogicalPlan::Extension(extension.clone()));
                    let ext_node = Extension {
                        node: Arc::new(bytes_processed),
                    };
                    Ok(Transformed::yes(LogicalPlan::Extension(ext_node)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })?;
        Ok(transformed_plan.data)
    }

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str {
        "bytes_processed_analyzer_rule"
    }
}

impl BytesProcessedAnalyzerRule {
    pub fn new() -> Self {
        Self::default()
    }
}

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
    fn name(&self) -> &str {
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
    fn name(&self) -> &str {
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

        let protocol: Protocol = context
            .session_config()
            .get_extension::<Protocol>()
            .map_or_else(|| Protocol::Internal, |x| *x);

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
            crate::metrics::telemetry::track_bytes_processed(bytes_processed, protocol.as_arc_str());
        };

        let stream_adapter = RecordBatchStreamAdapter::new(schema, bytes_processed_stream);

        Ok(Box::pin(stream_adapter))
    }
}
