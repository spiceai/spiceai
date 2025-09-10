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

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema, JoinConstraint, JoinType},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{Filter, Join, LogicalPlan, Projection, Sort, SortExpr, TableScan},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    sql::TableReference,
};
use runtime_datafusion_index::IndexedTableProvider;

use crate::{SEARCH_SCORE_COLUMN_NAME, index::SearchIndex};

/// Performs a search on a given [`SearchIndex`] and combine with the underlying [`TableProvider`]
/// if required by filters or additional columns in the projection.
#[derive(Debug, Clone)]
pub struct SearchQueryProvider {
    pub search_index: Arc<dyn SearchIndex>,
    pub table_provider: Arc<IndexedTableProvider>,
    pub query: String,
    pub pre_limit: Option<usize>,
}

impl SearchQueryProvider {
    pub fn new(
        search_index: Arc<dyn SearchIndex>,
        table_provider: Arc<IndexedTableProvider>,
        query: String,
        limit: Option<usize>,
    ) -> Self {
        Self {
            search_index,
            table_provider,
            query,
            pre_limit: limit,
        }
    }

    /// Check if the search index alone can satisfy the query (no join with base table needed)
    fn search_index_table_is_sufficient(
        &self,
        search_index_table: &LogicalPlan,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<bool, DataFusionError> {
        use std::collections::HashSet;

        let search_index_columns: HashSet<String> = search_index_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        // Check if projection can be satisfied
        let source_schema = match projection {
            None => self.schema(),
            Some(indices) => {
                let projected = self
                    .schema()
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(e, None))?;
                Arc::new(projected)
            }
        };

        let columns_requested: HashSet<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let has_all_columns = search_index_columns.is_superset(&columns_requested);

        // Check if all filters can be handled by search index
        let handleable_filters = filters
            .iter()
            .filter(|f| {
                let filter_columns = f
                    .column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect::<HashSet<_>>();
                search_index_columns.is_superset(&filter_columns)
            })
            .count();

        Ok(has_all_columns && handleable_filters == filters.len())
    }

    /// Build the underlying table scan, removing search index metadata columns from projection
    fn underlying_table_scan(
        &self,
        filters: &[Expr],
        all_metadata_columns: &[String],
    ) -> Result<LogicalPlan, DataFusionError> {
        // Remove all metadata columns (including any search-specific columns) from projection
        let base_proj =
            (0..self.table_provider.get_underlying().schema().fields().len()).collect::<Vec<_>>();
        let base_proj = projection_without_columns(
            &self.schema().fields,
            all_metadata_columns,
            Some(&base_proj),
        );

        // Get filters that can be pushed down to the base table
        let filter_refs: Vec<_> = filters.iter().collect();
        let supported_filters = self
            .table_provider
            .supports_filters_pushdown(filter_refs.as_slice())?;

        let underlying_filters: Vec<Expr> = filters
            .iter()
            .zip(supported_filters.iter())
            .filter_map(|(f, supp)| {
                use datafusion::logical_expr::TableProviderFilterPushDown;
                if matches!(supp, TableProviderFilterPushDown::Unsupported) {
                    None
                } else {
                    Some(f.clone())
                }
            })
            .collect();

        Ok(LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("base_table"),
            Arc::new(DefaultTableSource::new(
                Arc::clone(&self.table_provider) as Arc<dyn TableProvider>
            )),
            Some(base_proj),
            underlying_filters,
            None,
        )?))
    }

    /// Get all metadata columns that should be excluded from base table projections
    fn all_metadata_columns(&self) -> Vec<String> {
        self.search_index.metadata_columns().all_names()
    }

    /// Get filters that can be handled by the search index
    fn search_index_filters(
        &self,
        search_index_columns: &std::collections::HashSet<String>,
        filters: &[Expr],
    ) -> Vec<Expr> {
        filters
            .iter()
            .filter(|f| {
                let filter_columns = f
                    .column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect::<std::collections::HashSet<_>>();
                search_index_columns.is_superset(&filter_columns)
            })
            .cloned()
            .collect()
    }

    /// Create the search index table scan
    async fn search_index_table(&self, filters: &[Expr]) -> Result<LogicalPlan, DataFusionError> {
        // Get the query table provider from the search index
        let query_table = self
            .search_index
            .query_table_provider(&self.query)
            .await
            .map_err(|e| DataFusionError::External(e))?;

        // Create table scan with filters that can be handled by the search index
        let search_index_columns: std::collections::HashSet<String> = query_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let search_filters = self.search_index_filters(&search_index_columns, filters);

        Ok(LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("search_index"),
            Arc::new(DefaultTableSource::new(query_table)),
            None,
            search_filters,
            self.pre_limit,
        )?))
    }
}

#[async_trait]
impl TableProvider for SearchQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Build schema by combining base table schema with search score column
        let mut fields: Vec<_> = self
            .table_provider
            .schema()
            .fields()
            .iter()
            .cloned()
            .collect();

        // Add search score column
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME.to_string(),
            arrow_schema::DataType::Float64,
            false,
        )));

        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Check primary key constraints
        if self.search_index.primary_fields().is_empty() {
            return Err(DataFusionError::Execution(
                "The search index was created successfully without a primary key.\n\
                Ensure a primary key is available in the dataset source, or specified in the column configuration."
                .to_string(),
            ));
        }

        let search_index_table = self.search_index_table(filters).await?;

        // Check if search index alone is sufficient
        let base_logical_plan: LogicalPlan =
            if self.search_index_table_is_sufficient(&search_index_table, projection, filters)? {
                // Search index can handle everything - no join needed
                if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                    LogicalPlan::Filter(Filter::try_new(filter, search_index_table.into())?)
                } else {
                    search_index_table
                }
            } else {
                // Need to join with base table
                let underlying_table_scan =
                    self.underlying_table_scan(filters, &self.all_metadata_columns())?;

                // Build join conditions based on primary keys
                let join_conditions: Vec<(Column, Column)> = self
                    .search_index
                    .primary_fields()
                    .iter()
                    .map(|field| {
                        (
                            Column::new(
                                Some(TableReference::parse_str("search_index")),
                                field.name(),
                            ),
                            Column::new(
                                Some(TableReference::parse_str("base_table")),
                                field.name(),
                            ),
                        )
                    })
                    .collect();

                let on: Vec<(Expr, Expr)> = join_conditions
                    .into_iter()
                    .map(|(left, right)| (Expr::Column(left), Expr::Column(right)))
                    .collect();

                // Build join schema
                let join_schema = search_index_table
                    .schema()
                    .join(underlying_table_scan.schema())?;

                // Separate filters for pre-join and post-join
                let search_index_columns: std::collections::HashSet<String> = search_index_table
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();

                let pre_join_filters = self.search_index_filters(&search_index_columns, filters);

                let join = LogicalPlan::Join(Join {
                    left: Arc::new(search_index_table),
                    right: Arc::new(underlying_table_scan),
                    join_type: JoinType::Left,
                    join_constraint: JoinConstraint::On,
                    on,
                    filter: pre_join_filters.into_iter().reduce(Expr::and),
                    schema: join_schema.into(),
                    null_equals_null: false,
                });

                // For now, use the join schema directly
                // TODO: Implement proper deduplication of primary key columns
                let deduped_schema = join.schema().clone();

                LogicalPlan::Projection(Projection::try_new(
                    deduped_schema
                        .iter()
                        .map(|(tbl, f)| match tbl {
                            Some(tbl_ref) => {
                                Expr::Column(Column::new(Some(tbl_ref.clone()), f.name()))
                            }
                            None => Expr::Column(Column::new_unqualified(f.name())),
                        })
                        .collect(),
                    Arc::new(join),
                )?)
            };

        // Add sorting by search score (descending)
        let sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(
                Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                false, // descending
                true,  // nulls_first
            )],
            input: Arc::new(base_logical_plan),
            fetch: limit,
        });

        // Final projection to match requested schema
        let schema_proj: SchemaRef = match projection {
            None => self.schema(),
            Some(idx) => {
                let projected = self
                    .schema()
                    .project(idx)
                    .map_err(|e| DataFusionError::ArrowError(e, None))?;
                Arc::new(projected)
            }
        };

        let final_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(sort),
            Arc::new(DFSchema::from_unqualified_fields(
                schema_proj.fields().clone(),
                HashMap::default(),
            )?),
        ));

        // Convert logical plan to execution plan
        let session_ctx = datafusion::prelude::SessionContext::new();
        let exec_state = session_ctx.state();

        exec_state.create_physical_plan(&final_proj).await
    }
}

/// Helper function to remove columns from a projection
fn projection_without_columns(
    table_fields: &arrow_schema::Fields,
    columns: &[String],
    projection: Option<&Vec<usize>>,
) -> Vec<usize> {
    let base_projection = projection
        .cloned()
        .unwrap_or_else(|| (0..table_fields.len()).collect());

    let columns_to_remove: std::collections::HashSet<_> = columns.iter().collect();

    base_projection
        .into_iter()
        .filter(|&idx| {
            if let Some(field) = table_fields.get(idx) {
                !columns_to_remove.contains(&field.name().to_string())
            } else {
                true
            }
        })
        .collect()
}
