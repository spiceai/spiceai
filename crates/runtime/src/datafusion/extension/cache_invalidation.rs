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

//! Cache invalidation optimizer rule for DataFusion
//!
//! Automatically injects cache invalidation logic after successful write operations

use std::{
    any::Any,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::{Arc, Weak},
};

use async_stream::stream;
use async_trait::async_trait;
use cache::Caching;
use datafusion::{
    common::{
        DFSchemaRef,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    error::{DataFusionError, Result},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, stream::RecordBatchStreamAdapter,
    },
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
    sql::TableReference,
};
use futures::StreamExt;

/// [`OptimizerRule`] that detects write operations in a DataFusion logical plan and injects a cache invalidation node [`CacheInvalidationNode`].
///
/// # See also
///
/// - [`CacheInvalidationNode`]: Logical plan node for cache invalidation.
/// - [`CacheInvalidationExec`]: Physical execution plan for cache invalidation.
/// - [`Caching`]: Trait for cache implementations supporting invalidation.
#[derive(Debug, Default)]
pub struct CacheInvalidationOptimizerRule {
    caching: Weak<Caching>,
}

impl CacheInvalidationOptimizerRule {
    #[must_use]
    pub fn new(caching: Weak<Caching>) -> Self {
        Self { caching }
    }
}

impl OptimizerRule for CacheInvalidationOptimizerRule {
    fn name(&self) -> &'static str {
        "cache_invalidation_optimizer_rule"
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
                    .downcast_ref::<CacheInvalidationNode>()
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
            LogicalPlan::Dml(dml) => {
                let table_name = dml.table_name.clone();
                tracing::trace!("Injecting cache invalidation node for table: {table_name}");

                let ext_node = Extension {
                    node: Arc::new(CacheInvalidationNode::new(
                        LogicalPlan::Dml(dml),
                        table_name,
                        self.caching.clone(),
                    )),
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
}

/// Logical plan node that wraps a write operation and signals cache invalidation for the affected table.
/// During physical planning, this node is converted into a [`CacheInvalidationExec`] execution plan,
/// which performs cache invalidation after the write completes successfully.
pub(crate) struct CacheInvalidationNode {
    input: LogicalPlan,
    table: TableReference,
    caching: Weak<Caching>,
}

impl PartialOrd for CacheInvalidationNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.input.partial_cmp(&other.input) {
            Some(std::cmp::Ordering::Equal) => self.table.partial_cmp(&other.table),
            non_eq => non_eq,
        }
    }
}
impl CacheInvalidationNode {
    pub(crate) fn new(input: LogicalPlan, table: TableReference, caching: Weak<Caching>) -> Self {
        Self {
            input,
            table,
            caching,
        }
    }
}

impl Debug for CacheInvalidationNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for CacheInvalidationNode {
    fn name(&self) -> &'static str {
        "CacheInvalidationNode"
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
        write!(f, "CacheInvalidationNode: table={}", self.table)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Self {
            input,
            table: self.table.clone(),
            caching: self.caching.clone(),
        })
    }
}

impl PartialEq<CacheInvalidationNode> for CacheInvalidationNode {
    fn eq(&self, other: &CacheInvalidationNode) -> bool {
        self.input == other.input && self.table == other.table
    }
}

impl Eq for CacheInvalidationNode {}

impl Hash for CacheInvalidationNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.table.hash(state);
    }
}

/// Physical [`ExecutionPlan`] that wraps a write operation and invalidates cache after successful completion.
pub(crate) struct CacheInvalidationExec {
    input_exec: Arc<dyn ExecutionPlan>,
    table: TableReference,
    caching: Weak<Caching>,
}

impl CacheInvalidationExec {
    pub(crate) fn new(
        input_exec: Arc<dyn ExecutionPlan>,
        table: TableReference,
        caching: Weak<Caching>,
    ) -> Self {
        Self {
            input_exec,
            table,
            caching,
        }
    }

    fn invalidate_cache_for_table(table: &TableReference, caching: &Weak<Caching>) {
        if let Some(cache) = caching.upgrade() {
            if let Err(e) = cache.invalidate_for_table(table.clone()) {
                tracing::warn!("Failed to invalidate cache for table {table}: {e}");
            } else {
                tracing::trace!("Successfully invalidated cache for table {table}");
            }
        } else {
            tracing::debug!(
                "Cache reference for table {table} could not be upgraded; cache may have been dropped"
            );
        }
    }
}

impl std::fmt::Debug for CacheInvalidationExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CacheInvalidationExec")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CacheInvalidationExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CacheInvalidationExec: table={}", self.table)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "CacheInvalidationExec")
            }
        }
    }
}

impl ExecutionPlan for CacheInvalidationExec {
    fn name(&self) -> &'static str {
        "CacheInvalidationExec"
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
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "CacheInvalidationExec requires exactly one child".to_string(),
            ));
        }

        let Some(input) = children.into_iter().next() else {
            unreachable!("should have one input");
        };

        Ok(Arc::new(Self::new(
            input,
            self.table.clone(),
            self.caching.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input_stream = self.input_exec.execute(partition, context)?;
        let schema = self.input_exec.schema();

        let table = self.table.clone();
        let caching = self.caching.clone();

        // Create an async stream that wraps the input and handles cache invalidation
        let stream = stream! {
            let mut input_stream = input_stream;
            let mut exec_failed = false;

            while let Some(batch_result) = input_stream.next().await {
                match batch_result {
                    Ok(batch) => yield Ok(batch),
                    Err(e) => {
                        exec_failed = true;
                        yield Err(e);
                    }
                }
            }

            // If wrapped input_exec completed without error, invalidate the cache
            if !exec_failed {
                Self::invalidate_cache_for_table(&table, &caching);
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[derive(Default)]
pub struct CacheInvalidationExtensionPlanner {}

impl CacheInvalidationExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for CacheInvalidationExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &datafusion::execution::context::SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(cache_node) = node.as_any().downcast_ref::<CacheInvalidationNode>() else {
            return Ok(None);
        };

        if logical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "CacheInvalidationNode should have 1 logical input, got {}",
                logical_inputs.len()
            )));
        }

        if physical_inputs.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "CacheInvalidationNode should have 1 physical input, got {}",
                physical_inputs.len()
            )));
        }

        let physical_input = &physical_inputs[0];

        let exec = CacheInvalidationExec::new(
            physical_input.clone(),
            cache_node.table.clone(),
            cache_node.caching.clone(),
        );

        return Ok(Some(Arc::new(exec)));
    }
}
