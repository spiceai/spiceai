/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{
    any::Any,
    cmp::Ordering,
    collections::HashSet,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    error::Result,
    execution::{SendableRecordBatchStream, SessionState, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, execution_plan::CardinalityEffect,
        stream::RecordBatchStreamAdapter,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
};
use futures::StreamExt;

use crate::snapshot::SnapshotManager;

/// [`OptimizerRule`] that adds a [`SnapshotCreatorNode`] to the acceleration refresh plan.
#[derive(Debug)]
pub struct SnapshotCreatorOptimizerRule {
    snapshot_manager: SnapshotManager,
}

impl SnapshotCreatorOptimizerRule {
    #[must_use]
    pub fn new(snapshot_manager: SnapshotManager) -> Self {
        Self { snapshot_manager }
    }
}

impl OptimizerRule for SnapshotCreatorOptimizerRule {
    fn name(&self) -> &'static str {
        "SnapshotCreatorOptimizerRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut has_creator_node = false;
        plan.apply(|plan| match plan {
            LogicalPlan::Extension(extension) => {
                if extension
                    .node
                    .as_any()
                    .downcast_ref::<SnapshotCreatorNode>()
                    .is_some()
                {
                    has_creator_node = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })?;

        if has_creator_node {
            Ok(Transformed::no(plan))
        } else {
            let new_plan = LogicalPlan::Extension(Extension {
                node: Arc::new(SnapshotCreatorNode::new(
                    plan,
                    self.snapshot_manager.clone(),
                )),
            });
            Ok(Transformed::yes(new_plan))
        }
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotCreatorNode {
    input: LogicalPlan,
    snapshot_manager: SnapshotManager,
}

impl SnapshotCreatorNode {
    #[must_use]
    pub(crate) fn new(input: LogicalPlan, snapshot_manager: SnapshotManager) -> Self {
        Self {
            input,
            snapshot_manager,
        }
    }
}

impl Hash for SnapshotCreatorNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialEq for SnapshotCreatorNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl Eq for SnapshotCreatorNode {}

impl PartialOrd for SnapshotCreatorNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for SnapshotCreatorNode {
    fn name(&self) -> &'static str {
        "SnapshotCreatorNode"
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
        write!(f, "SnapshotCreatorNode")?;
        Ok(())
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

    /// Returns `true` if a limit can be safely pushed down through this
    /// `UserDefinedLogicalNode` node.
    ///
    /// If this method returns `true`, and the query plan contains a limit at
    /// the output of this node, `DataFusion` will push the limit to the input
    /// of this node.
    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Self {
            input,
            snapshot_manager: self.snapshot_manager.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct SnapshotCreatorExtensionPlanner {}

impl SnapshotCreatorExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for SnapshotCreatorExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(snapshot_creator_node) = node.as_any().downcast_ref::<SnapshotCreatorNode>()
        else {
            return Ok(None);
        };

        if logical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "SnapshotCreatorNode should have 1 logical input, got {}",
                logical_inputs.len()
            )));
        }

        if physical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "SnapshotCreatorNode should have 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let physical_input = &physical_inputs[0];
        let exec_plan = Arc::new(SnapshotExec::new(
            Arc::clone(physical_input),
            snapshot_creator_node.snapshot_manager.clone(),
        ));
        Ok(Some(exec_plan))
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotExec {
    input_exec: Arc<dyn ExecutionPlan>,
    snapshot_manager: SnapshotManager,
}

impl SnapshotExec {
    pub(crate) fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        snapshot_manager: SnapshotManager,
    ) -> Self {
        Self {
            input_exec,
            snapshot_manager,
        }
    }
}

impl DisplayAs for SnapshotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SnapshotExec")?;
        write!(f, " location:{}", self.snapshot_manager.snapshots_location)?;
        Ok(())
    }
}

impl ExecutionPlan for SnapshotExec {
    fn name(&self) -> &'static str {
        "SnapshotExec"
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "SnapshotExec requires exactly one input".to_string(),
            ));
        }
        let input = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "SnapshotExec requires exactly one input".to_string(),
            )
        })?;
        Ok(Arc::new(Self {
            input_exec: input,
            snapshot_manager: self.snapshot_manager.clone(),
        }))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.input_exec.schema();
        let _snapshot_manager = Arc::new(self.snapshot_manager.clone());
        let stream = self
            .input_exec
            .execute(partition, Arc::clone(&context))?
            .boxed();

        // snapshot_manager.create_snapshot();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
