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

//! Cache invalidation optimizer rule for `DataFusion`
//!
//! Automatically injects cache invalidation logic after successful write operations

use std::{
    collections::HashSet,
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
    error::Result,
    execution::SendableRecordBatchStream,
    logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore},
    optimizer::{OptimizerConfig, OptimizerRule},
    physical_plan::{DisplayFormatType, ExecutionPlan, stream::RecordBatchStreamAdapter},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
    prelude::Expr,
    sql::TableReference,
};
use futures::StreamExt;

use crate::datafusion::extension::pass_thru::PassThruExec;

/// [`OptimizerRule`] that detects write operations in a `DataFusion` logical plan and injects a cache invalidation node [`CacheInvalidationNode`].
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
                        Weak::clone(&self.caching),
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
            unreachable!("should have one input");
        };
        Ok(Self {
            input,
            table: self.table.clone(),
            caching: Weak::clone(&self.caching),
        })
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        // Allow filters for all columns to be pushed down
        HashSet::new()
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
        Some(vec![output_columns.to_vec()])
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

/// Creates physical [`ExecutionPlan`] that wraps a write operation and invalidates cache after successful completion.
fn create_cache_invalidation_exec(
    input: Arc<dyn ExecutionPlan>,
    table: &TableReference,
    caching: Weak<Caching>,
) -> Arc<dyn ExecutionPlan> {
    let table_exec = table.clone();
    let exec = move |input_exec: &Arc<dyn ExecutionPlan>, partition, ctx| {
        let schema = input_exec.schema();
        let input_stream = input_exec.execute(partition, ctx)?;
        let caching = Weak::clone(&caching);
        let table = table_exec.clone();

        let s = stream! {
            let mut input = input_stream;
            let mut ok = true;
            while let Some(item) = input.next().await {
                match item {
                    Ok(b) => yield Ok(b),
                    Err(e) => { ok = false; yield Err(e); }
                }
            }
            if ok {
                invalidate_cache_for_table(&table, &caching);
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, s)) as SendableRecordBatchStream)
    };

    let table_fmt_fn = table.clone();
    let display_fmt_fn = move |t: DisplayFormatType, f: &mut fmt::Formatter| match t {
        DisplayFormatType::Default | DisplayFormatType::Verbose => {
            write!(f, "CacheInvalidationExec: table={table_fmt_fn}")
        }
        DisplayFormatType::TreeRender => {
            write!(f, "table={table_fmt_fn}")
        }
    };

    Arc::new(
        PassThruExec::new(input, "CacheInvalidationExec", exec)
            .with_input_partitioning(datafusion::physical_plan::Distribution::SinglePartition)
            .with_display_fmt_fn(display_fmt_fn),
    )
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

        Ok(Some(create_cache_invalidation_exec(
            Arc::clone(physical_input),
            &cache_node.table,
            Weak::clone(&cache_node.caching),
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        dataaccelerator::AcceleratorEngineRegistry,
        datafusion::{DataFusion, builder::DataFusionBuilder},
        status::RuntimeStatus,
    };

    use arrow::datatypes::{DataType, Field, Schema};
    use cache::{Caching, QueryResultsCacheProvider, result::CacheStatus};
    use data_components::arrow::write::MemTable;
    use futures::TryStreamExt;
    use spicepod::component::caching::SQLResultsCacheConfig;
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_datafusion(cache: Option<Arc<Caching>>) -> Arc<DataFusion> {
        let mut builder = DataFusionBuilder::new(
            RuntimeStatus::new(),
            Arc::new(AcceleratorEngineRegistry::new()),
        );

        // Add cache if provided
        if let Some(cache) = cache {
            builder = builder.with_caching(cache);
        }

        let df = Arc::new(builder.build());

        let mem_table = Arc::new(
            MemTable::try_new(create_test_schema(), vec![]).expect("mem table should be created"),
        );

        df.ctx
            .register_table(
                "test_table",
                Arc::clone(&mem_table) as Arc<dyn crate::datafusion::TableProvider>,
            )
            .expect("table should be registered");

        df.data_writers
            .write()
            .expect("data writers should be acquired")
            .insert("test_table".into());

        df
    }

    async fn execute_sql(
        df: &Arc<DataFusion>,
        query: &str,
        snapshot_name: Option<&str>,
        expected_cache_status: CacheStatus,
    ) {
        let query_result = df
            .query_builder(query)
            .build()
            .run()
            .await
            .expect("to execute query");

        assert_eq!(
            query_result.cache_status,
            expected_cache_status,
            "Unexpected cache status for query: {query}, expected: {expected_cache_status:?}, got: {actual:?}",
            actual = query_result.cache_status
        );

        let data = query_result
            .data
            .try_collect::<Vec<_>>()
            .await
            .expect("to collect data");

        if let Some(name) = snapshot_name {
            let formatted = arrow::util::pretty::pretty_format_batches(&data)
                .expect("to pretty format batches");
            insta::assert_snapshot!(name, formatted);
        }
    }

    #[tokio::test]
    async fn test_insert_with_cache_invalidation() {
        let config = SQLResultsCacheConfig {
            item_ttl: Some("30s".to_string()),
            ..Default::default()
        };
        let results_cache = Arc::new(
            QueryResultsCacheProvider::try_new(&config, Box::new([])).expect("to create cache"),
        );
        let cache = Arc::new(Caching::new().with_results_cache(Arc::clone(&results_cache)));

        let df = create_test_datafusion(Some(cache));

        // activate cache for test query
        execute_sql(
            &df,
            "SELECT * FROM test_table",
            None,
            CacheStatus::CacheMiss,
        )
        .await;
        execute_sql(&df, "SELECT * FROM test_table", None, CacheStatus::CacheHit).await;

        // verify CacheInvalidationNode is correctly added
        execute_sql(
            &df,
            "explain INSERT INTO test_table VALUES (1, 'foo', 42.0)",
            Some("test_insert_with_cache_plan"),
            CacheStatus::CacheDisabled,
        )
        .await;
        // perform insert query and validate cache has been invalidated correctly
        execute_sql(
            &df,
            "INSERT INTO test_table VALUES (1, 'foo', 42.0)",
            None,
            CacheStatus::CacheDisabled,
        )
        .await;
        execute_sql(
            &df,
            "SELECT * FROM test_table",
            Some("test_insert_with_cache_result"),
            CacheStatus::CacheMiss,
        )
        .await;
    }

    #[tokio::test]
    async fn test_insert_cache_disabled() {
        let df = create_test_datafusion(None);
        // verify there is no CacheInvalidationNode
        execute_sql(
            &df,
            "explain INSERT INTO test_table VALUES (1, 'foo', 42.0)",
            Some("test_insert_cache_disabled_plan"),
            CacheStatus::CacheDisabled,
        )
        .await;
    }
}
