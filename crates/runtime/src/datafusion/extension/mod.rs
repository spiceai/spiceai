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

use async_trait::async_trait;
use bytes_processed::{BytesProcessedExec, BytesProcessedNode};
use data_components::delete::get_deletion_provider;
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRewriter},
    error::Result,
    execution::context::{QueryPlanner, SessionState},
    logical_expr::{DmlStatement, Expr, LogicalPlan, UserDefinedLogicalNode, WriteOp},
    physical_plan::ExecutionPlan,
    physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner},
};
use std::sync::Arc;

pub mod bytes_processed;

/// Helper to strip table qualifiers from column references in filter expressions.
/// This is needed because DELETE filter expressions have qualified column names
/// (e.g., "table.column") but when we apply them during deletion, the columns
/// are unqualified (e.g., just "column").
struct UnqualifyColumns;

impl TreeNodeRewriter for UnqualifyColumns {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<datafusion::common::tree_node::Transformed<Expr>> {
        Ok(match expr {
            Expr::Column(col) => {
                // Strip the table qualifier, keep only the column name
                datafusion::common::tree_node::Transformed::yes(Expr::Column(
                    datafusion::common::Column::new_unqualified(col.name),
                ))
            }
            _ => datafusion::common::tree_node::Transformed::no(expr),
        })
    }
}

fn unqualify_filters(filters: Vec<Expr>) -> Result<Vec<Expr>> {
    let mut unqualifier = UnqualifyColumns;
    filters
        .into_iter()
        .map(|f| f.rewrite(&mut unqualifier).map(|t| t.data))
        .collect()
}

#[derive(Default)]
pub struct SpiceQueryPlanner {
    extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
}

impl std::fmt::Debug for SpiceQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiceQueryPlanner")
            .field("extension_planners", &self.extension_planners.len())
            .finish()
    }
}

impl SpiceQueryPlanner {
    #[must_use]
    pub fn new() -> Self {
        SpiceQueryPlanner {
            extension_planners: vec![],
        }
    }

    #[must_use]
    pub fn with_extension_planners(
        mut self,
        planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        self.extension_planners = planners;
        self
    }
}

#[async_trait]
impl QueryPlanner for SpiceQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Handle DML operations (UPDATE, DELETE) before delegating to DefaultPhysicalPlanner
        
        // Handle UPDATE operations  
        if let LogicalPlan::Dml(stmt @ DmlStatement {
            table_name,
            op: WriteOp::Update,
            input,
            ..
        }) = logical_plan
        {
            // Get the table provider through the catalog
            let table_source = session_state
                .catalog_list()
                .catalog(table_name.catalog().unwrap_or("datafusion"))
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Catalog '{}' not found",
                        table_name.catalog().unwrap_or("datafusion")
                    ))
                })?
                .schema(table_name.schema().unwrap_or("public"))
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Schema '{}' not found",
                        table_name.schema().unwrap_or("public")
                    ))
                })?
                .table(table_name.table())
                .await?
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Table '{}' not found",
                        table_name
                    ))
                })?;
            
            // Check if it supports update
            if let Some(update_provider) = data_components::update::get_update_provider(table_source) {
                // Extract filters from the UPDATE statement's input plan
                let filters = match input.as_ref() {
                    LogicalPlan::Filter(filter) => vec![filter.predicate.clone()],
                    _ => vec![],
                };
                
                // Unqualify column references in filters (remove table qualifiers)
                let unqualified_filters = unqualify_filters(filters)?;
                
                // Extract assignments from the DML statement
                // The input plan contains a Projection with the SET expressions
                let mut assignment_map = std::collections::HashMap::new();
                if let LogicalPlan::Projection(projection) = input.as_ref() {
                    let schema = projection.input.schema();
                    for (idx, expr) in projection.expr.iter().enumerate() {
                        if idx < schema.fields().len() {
                            let field = &schema.fields()[idx];
                            assignment_map.insert(field.name().clone(), expr.clone());
                        }
                    }
                }
                
                // Call update on the update provider - it returns the execution plan
                return update_provider
                    .update(session_state, &unqualified_filters, assignment_map)
                    .await;
            }
            
            return datafusion::common::plan_err!(
                "Table '{}' does not support UPDATE operations. \
                Only accelerated tables support UPDATE.",
                table_name
            );
        }
        
        // Handle DELETE operations
        if let LogicalPlan::Dml(DmlStatement {
            table_name,
            op: WriteOp::Delete,
            input,
            ..
        }) = logical_plan
        {
            // Get the table provider through the catalog
            let table_source = session_state
                .catalog_list()
                .catalog(table_name.catalog().unwrap_or("datafusion"))
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Catalog '{}' not found",
                        table_name.catalog().unwrap_or("datafusion")
                    ))
                })?
                .schema(table_name.schema().unwrap_or("public"))
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Schema '{}' not found",
                        table_name.schema().unwrap_or("public")
                    ))
                })?
                .table(table_name.table())
                .await?
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Table '{}' not found",
                        table_name
                    ))
                })?;
            
            // Check if it supports deletion
            if let Some(deletion_provider) = get_deletion_provider(table_source) {
                // Extract filters from the DELETE statement's input plan
                // The input plan contains the filter conditions
                let filters = match input.as_ref() {
                    LogicalPlan::Filter(filter) => vec![filter.predicate.clone()],
                    _ => vec![],
                };
                
                // Unqualify column references in filters (remove table qualifiers)
                let unqualified_filters = unqualify_filters(filters)?;
                
                // Call delete_from on the deletion provider - it returns the execution plan
                return deletion_provider
                    .delete_from(session_state, &unqualified_filters)
                    .await;
            }
            
            return datafusion::common::plan_err!(
                "Table '{}' does not support DELETE operations. \
                Only accelerated tables support DELETE.",
                table_name
            );
        }
        
        // For all other operations, use the default physical planner
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(self.extension_planners.clone());
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

#[derive(Default)]
pub struct SpiceExtensionPlanner {}

impl SpiceExtensionPlanner {
    #[must_use]
    pub fn new() -> Self {
        SpiceExtensionPlanner {}
    }
}

#[async_trait]
impl ExtensionPlanner for SpiceExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // bytes_processed Extension
        let bytes_processed_node = node.as_any().downcast_ref::<BytesProcessedNode>();
        if bytes_processed_node.is_some() {
            assert_eq!(logical_inputs.len(), 1, "should have 1 input");
            assert_eq!(physical_inputs.len(), 1, "should have 1 input");
            let physical_input = &physical_inputs[0];

            let exec_plan = Arc::new(BytesProcessedExec::new(Arc::clone(physical_input)));
            return Ok(Some(exec_plan));
        }

        Ok(None)
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
