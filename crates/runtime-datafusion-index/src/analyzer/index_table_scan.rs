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
    datasource::DefaultTableSource,
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

use crate::{Index, IndexedTableProvider};

/// [`OptimizerRule`] that looks for [`IndexedTableProvider`] nodes and adds an [`IndexTableScanNode`].
#[derive(Debug, Default)]
pub struct IndexTableScanOptimizerRule {}

impl IndexTableScanOptimizerRule {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for IndexTableScanOptimizerRule {
    fn name(&self) -> &'static str {
        "IndexTableScanOptimizerRule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| match plan {
            LogicalPlan::Extension(extension) => {
                if extension
                    .node
                    .as_any()
                    .downcast_ref::<IndexTableScanNode>()
                    .is_some()
                {
                    Ok(Transformed::new(
                        LogicalPlan::Extension(extension),
                        false,
                        TreeNodeRecursion::Jump, // Don't process any further children of this sub-tree.
                    ))
                } else {
                    Ok(Transformed::no(LogicalPlan::Extension(extension)))
                }
            }
            LogicalPlan::TableScan(table_scan) => {
                let Some(default_source) = table_scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                };
                let underlying = Arc::clone(&default_source.table_provider);
                let Some(indexed_table_provider) =
                    underlying.as_any().downcast_ref::<IndexedTableProvider>()
                else {
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                };
                let projected_schema = Arc::clone(&table_scan.projected_schema);

                // Filter to just the indexes that can be served by the projected schema
                let available_indexes: Vec<_> = indexed_table_provider
                    .indexes
                    .iter()
                    .filter(|index| {
                        // Check if all required columns for this index are in the projected schema
                        index
                            .required_columns()
                            .iter()
                            .all(|col| projected_schema.has_column_with_unqualified_name(col))
                    })
                    .cloned()
                    .collect();

                if available_indexes.is_empty() {
                    // No indexes can be served by the projected schema
                    return Ok(Transformed::no(LogicalPlan::TableScan(table_scan)));
                }

                // Create new node with just the available indexes
                let new_node =
                    IndexTableScanNode::new(LogicalPlan::TableScan(table_scan), available_indexes);

                let plan = LogicalPlan::Extension(Extension {
                    node: Arc::new(new_node),
                });

                // We don't need to process the TableScan we just processed, so we jump to the next node.
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Jump))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

#[derive(Debug)]
pub(crate) struct IndexTableScanNode {
    input: LogicalPlan,
    indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexTableScanNode {
    #[must_use]
    pub(crate) fn new(input: LogicalPlan, indexes: Vec<Arc<dyn Index + Send + Sync>>) -> Self {
        Self { input, indexes }
    }
}

impl Hash for IndexTableScanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        for index in &self.indexes {
            index.name().hash(state);
        }
    }
}

impl PartialEq for IndexTableScanNode {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
            && self.indexes.iter().all(|index| {
                other
                    .indexes
                    .iter()
                    .any(|other_index| index.name() == other_index.name())
            })
    }
}

impl Eq for IndexTableScanNode {}

impl PartialOrd for IndexTableScanNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input)
    }
}

impl UserDefinedLogicalNodeCore for IndexTableScanNode {
    fn name(&self) -> &'static str {
        "IndexTableScanNode"
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
        write!(f, "IndexTableScanNode")?;
        for index in &self.indexes {
            write!(f, " index:{}", index.name())?;
        }
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

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Self {
            input,
            indexes: self.indexes.clone(),
        })
    }
}

#[derive(Debug, Default)]
pub struct IndexTableScanExtensionPlanner {}

impl IndexTableScanExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for IndexTableScanExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(index_table_scan_node) = node.as_any().downcast_ref::<IndexTableScanNode>() else {
            return Ok(None);
        };

        if logical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IndexTableScanNode should have 1 logical input, got {}",
                logical_inputs.len()
            )));
        }

        if physical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "IndexTableScanNode should have 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let physical_input = &physical_inputs[0];
        let exec_plan = Arc::new(IndexerExec::new(
            Arc::clone(physical_input),
            index_table_scan_node.indexes.clone(),
        ));
        Ok(Some(exec_plan))
    }
}

#[derive(Debug)]
pub(crate) struct IndexerExec {
    input_exec: Arc<dyn ExecutionPlan>,
    indexes: Vec<Arc<dyn Index + Send + Sync>>,
}

impl IndexerExec {
    pub(crate) fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        indexes: Vec<Arc<dyn Index + Send + Sync>>,
    ) -> Self {
        Self {
            input_exec,
            indexes,
        }
    }
}

impl DisplayAs for IndexerExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexerExec")?;
        for index in &self.indexes {
            if matches!(t, DisplayFormatType::TreeRender) {
                writeln!(f, "index:{}", index.name())?;
            } else {
                write!(f, " index:{}", index.name())?;
            }
        }
        write!(f, " input:")?;
        self.input_exec.fmt_as(t, f)?;
        Ok(())
    }
}

impl ExecutionPlan for IndexerExec {
    fn name(&self) -> &'static str {
        "IndexerExec"
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
                "IndexerExec requires exactly one input".to_string(),
            ));
        }
        let input = children.into_iter().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(
                "IndexerExec requires exactly one input".to_string(),
            )
        })?;
        Ok(Arc::new(Self {
            input_exec: input,
            indexes: self.indexes.clone(),
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
        let indexes = self.indexes.clone();
        let stream = self
            .input_exec
            .execute(partition, Arc::clone(&context))?
            .then(move |batch| {
                let indexes = indexes.clone();
                async move {
                    if let Ok(batch) = batch.as_ref() {
                        futures::future::join_all(
                            indexes
                                .iter()
                                .map(|index| index.compute_index(vec![batch.clone()])),
                        )
                        .await;
                    }
                    batch
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
