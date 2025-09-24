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

use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, Cast, Filter, Join, LogicalPlan, Operator, Projection, Sort, SortExpr,
        SubqueryAlias, TableProviderFilterPushDown, TableScan, expr::Alias,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, array_element, substring},
    scalar::ScalarValue,
    sql::TableReference,
};

use crate::{
    SEARCH_MATCH_COLUMN_NAME, SEARCH_SCORE_COLUMN_NAME,
    chunking::{ChunkedSearchIndex, is_chunked},
    index::SearchIndex,
};

/// Performs a search on a given [`SearchIndex`] and combine with the underlying [`TableProvider`]
/// if required by filters or additional columns in the projection.
#[derive(Debug, Clone)]
pub struct SearchQueryProvider {
    pub search_index: Arc<dyn SearchIndex>,
    pub table_provider: Arc<dyn TableProvider>,
    pub query: String,
    pub pre_limit: Option<usize>,
}

impl SearchQueryProvider {
    pub fn new(
        search_index: Arc<dyn SearchIndex>,
        table_provider: Arc<dyn TableProvider>,
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
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(projected)
            }
        };

        let columns_requested: HashSet<String> = source_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let has_all_columns = search_index_columns.is_superset(&columns_requested);
        if !has_all_columns {
            // Early exit.
            return Ok(false);
        }

        // Check if all filters can be handled by search index
        let all_filters_can_be_done = filters.iter().all(|f| {
            let filter_columns = f
                .column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>();
            search_index_columns.is_superset(&filter_columns)
        });

        Ok(all_filters_can_be_done)
    }

    /// Build the underlying table scan, removing search index metadata columns from projection
    fn underlying_table_scan(
        &self,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        all_metadata_columns: &[String],
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut base_proj = projection_without_columns(
            &self.schema().fields,
            &[
                all_metadata_columns,
                &[
                    SEARCH_SCORE_COLUMN_NAME.to_string(),
                    SEARCH_MATCH_COLUMN_NAME.to_string(),
                    ChunkedSearchIndex::chunking_offset_col(
                        self.search_index.search_column().as_str(),
                    ),
                ],
            ]
            .concat(),
            projection,
        );
        base_proj.sort_unstable(); // Deterministic LogicalPlans

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

    /// Get filters that can be handled by the search index
    fn search_index_filters(
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
            .map_err(DataFusionError::External)?;

        // Create table scan with filters that can be handled by the search index
        let search_index_columns: std::collections::HashSet<String> = query_table
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let search_filters = Self::search_index_filters(&search_index_columns, filters);

        Ok(LogicalPlan::TableScan(TableScan::try_new(
            TableReference::parse_str("search_index"),
            Arc::new(DefaultTableSource::new(query_table)),
            None,
            search_filters,
            self.pre_limit,
        )?))
    }

    #[allow(clippy::too_many_lines)] // Removed in `https://github.com/spiceai/spiceai/issues/7242`.
    fn join_with_base(
        &self,
        projection: Option<&Vec<usize>>,
        search_index_table: LogicalPlan,
        filters: &[Expr],
    ) -> Result<LogicalPlan, DataFusionError> {
        // Add subquery so that we can uniquely identify columns between search index and underlying table scan.
        let search_index_proj = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            search_index_table.into(),
            TableReference::parse_str("search_index"),
        )?);

        let primary_key_fields = self.search_index.primary_fields();
        let primary_key_projection: Vec<usize> = primary_key_fields
            .iter()
            .filter_map(|f| self.schema().index_of(f.name()).ok())
            .collect();

        let table_proj: Option<Vec<_>> = projection.map(|proj| {
            let mut p = proj.clone().into_iter().collect::<HashSet<_>>();
            // Ensure primary keys are retrieved from underlying table.
            for pp in primary_key_projection {
                p.insert(pp);
            }

            // Remove 'match'. Not in base table, calculated from offsets and search column.
            if let Some((idx, _)) = self.schema().column_with_name(SEARCH_MATCH_COLUMN_NAME) {
                let _ = p.remove(&idx);
            }

            let search_index_cols = search_index_proj
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<HashSet<_>>();
            // These columns may be needed to handle filters in the JOIN.
            // Don't add any that will be in search index.
            let filter_cols = filters
                .iter()
                .flat_map(|f| {
                    let filter_cols = f
                        .column_refs()
                        .iter()
                        .map(|c| c.name())
                        .collect::<HashSet<_>>();
                    filter_cols
                        .difference(&search_index_cols)
                        .copied()
                        .collect::<Vec<_>>()
                })
                .filter_map(|c| self.schema().index_of(c).ok())
                .collect::<Vec<_>>();
            p.extend(filter_cols);

            if let Some((idx, _)) = self.schema().column_with_name(
                format!("{}_embedding", self.search_index.search_column()).as_str(),
            ) {
                let _ = p.remove(&idx);
            }

            p.into_iter().collect()
        });

        // Need to join with base table
        let underlying_table_scan = self.underlying_table_scan(
            table_proj.as_ref(),
            filters,
            &self.search_index.metadata_columns().all_names(),
        )?;

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
                    Column::new(Some(TableReference::parse_str("base_table")), field.name()),
                )
            })
            .collect();

        let on: Vec<(Expr, Expr)> = join_conditions
            .into_iter()
            .map(|(left, right)| (Expr::Column(left), Expr::Column(right)))
            .collect();

        // Build join schema
        let join_schema = search_index_proj
            .schema()
            .join(underlying_table_scan.schema())?;

        let primary_key_column_names: std::collections::HashSet<String> = primary_key_fields
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let (post_join_filters, pre_join_filters): (Vec<Expr>, Vec<Expr>) =
            filters.iter().cloned().partition(|f| {
                f.column_refs()
                    .iter()
                    .any(|col| primary_key_column_names.contains(col.name()))
            });

        let join = LogicalPlan::Join(Join {
            left: Arc::new(search_index_proj),
            right: Arc::new(underlying_table_scan),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            on,
            filter: pre_join_filters.into_iter().reduce(Expr::and),
            schema: join_schema.into(),
            null_equality: NullEquality::NullEqualsNothing,
        });

        let deduped_join_proj_exprs: Vec<_> = join
            .schema()
            .iter()
            .filter(|(tbl, f)| {
                !(primary_key_column_names.contains(f.name())
                    && tbl.is_some_and(|t| *t == TableReference::parse_str("base_table")))
            })
            .map(|(tbl, field_ref)| match tbl {
                Some(table_ref) => {
                    Expr::Column(Column::new(Some(table_ref.clone()), field_ref.name()))
                }
                None => Expr::Column(Column::new(None::<TableReference>, field_ref.name())),
            })
            .collect();

        let proj =
            LogicalPlan::Projection(Projection::try_new(deduped_join_proj_exprs, join.into())?);

        if let Some(filter) = post_join_filters.into_iter().reduce(Expr::and) {
            Ok(LogicalPlan::Filter(Filter::try_new(filter, proj.into())?))
        } else {
            Ok(proj)
        }
    }

    fn match_column_index(&self) -> Option<usize> {
        self.schema()
            .column_with_name(SEARCH_MATCH_COLUMN_NAME)
            .map(|(i, _)| i)
    }

    pub fn add_match_column(
        &self,
        projection: Option<&Vec<usize>>,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, DataFusionError> {
        // If projection doesn't include/need the 'match' column, early exit.
        if !is_chunked(&self.search_index)
            || projection
                .is_some_and(|proj| self.match_column_index().is_none_or(|i| !proj.contains(&i)))
        {
            return Ok(input);
        }
        let mut initial: Vec<_> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect();

        let first = array_element(
            Expr::Column(Column::new_unqualified(
                ChunkedSearchIndex::chunking_offset_col(self.search_index.search_column().as_str()),
            )),
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
        );
        let second = array_element(
            Expr::Column(Column::new_unqualified(
                ChunkedSearchIndex::chunking_offset_col(self.search_index.search_column().as_str()),
            )),
            Expr::Literal(ScalarValue::Int64(Some(2)), None),
        );

        // substring(search_column, chunk_offset[1], chunk_offset[2] - chunk_offset[1]) as 'match'
        let substr = Expr::Cast(Cast::new(
            Box::new(substring(
                Expr::Column(Column::new_unqualified(self.search_index.search_column())),
                first.clone(),
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(second),
                    Operator::Minus,
                    Box::new(first),
                )),
            )),
            DataType::Utf8,
        ));

        initial.push(Expr::Alias(Alias::new(
            substr,
            None::<TableReference>,
            "match",
        )));

        Ok(LogicalPlan::Projection(Projection::try_new(
            initial,
            input.into(),
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

        if is_chunked(&self.search_index) {
            fields.extend([
                Arc::new(Field::new(
                    ChunkedSearchIndex::chunking_offset_col(
                        self.search_index.search_column().as_str(),
                    ),
                    DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 2),
                    false,
                )),
                Arc::new(Field::new(
                    SEARCH_MATCH_COLUMN_NAME.to_string(),
                    arrow_schema::DataType::Utf8,
                    false,
                )),
            ]);

            if let Some(vector_index) = Arc::clone(&self.search_index).as_vector_index() {
                fields.push(Arc::new(Field::new(
                    ChunkedSearchIndex::embedding_col(self.search_index.search_column().as_str()),
                    DataType::new_fixed_size_list(
                        DataType::Float32,
                        vector_index.dimension(),
                        false,
                    ),
                    true,
                )));
            }
        }

        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        let base_table = self.table_provider.supports_filters_pushdown(filters)?;

        // For vector index, any filter that is only on vector index will be marked as support (if not supported by engine, we will manually apply thereafter).
        let search_index_columns: HashSet<String> = self
            .search_index
            .metadata_columns()
            .filterable()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let search_index: Vec<_> = filters
            .iter()
            .map(|f| {
                let filter_columns = f
                    .column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();
                if search_index_columns.is_superset(&filter_columns) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect();

        // If one of the two has a pushdown threshold, it can be used.
        Ok(base_table
            .iter()
            .zip(search_index.iter())
            .map(|(a, b)| match (a, b) {
                (TableProviderFilterPushDown::Exact, _)
                | (_, TableProviderFilterPushDown::Exact) => TableProviderFilterPushDown::Exact,
                (TableProviderFilterPushDown::Inexact, _)
                | (_, TableProviderFilterPushDown::Inexact) => TableProviderFilterPushDown::Inexact,
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if self.search_index.primary_fields().is_empty() {
            return Err(DataFusionError::Execution(
                "The search index was created without a primary key.\n\
                Ensure a primary key is available in the dataset source, or specified in the column configuration."
                .to_string(),
            ));
        }
        let search_index_table = self.search_index_table(filters).await?;

        let inner_proj: Option<Vec<_>> = projection.cloned().map(|proj| {
            let Some(match_idx) = self.match_column_index() else {
                return proj;
            };
            if !proj.contains(&match_idx) {
                return proj;
            }
            let mut proj2 = proj.clone();
            if let Some(search_idx) = self
                .schema()
                .column_with_name(self.search_index.search_column().as_str())
                .map(|(i, _)| i)
                && !proj2.contains(&search_idx)
            {
                proj2.push(search_idx);
            }
            proj2
        });

        // Check if search index alone is sufficient
        let base_logical_plan: LogicalPlan = if self.search_index_table_is_sufficient(
            &search_index_table,
            inner_proj.as_ref(),
            filters,
        )? {
            // Search index can handle everything - no join needed
            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                LogicalPlan::Filter(Filter::try_new(filter, search_index_table.into())?)
            } else {
                search_index_table
            }
        } else {
            self.join_with_base(inner_proj.as_ref(), search_index_table, filters)?
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

        let with_columns = self.add_match_column(inner_proj.as_ref(), sort)?;

        // Final projection to match requested schema
        let schema_proj: SchemaRef = match projection {
            None => self.schema(),
            Some(idx) => {
                let projected = self
                    .schema()
                    .project(idx)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                Arc::new(projected)
            }
        };

        let final_proj = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(with_columns),
            Arc::new(DFSchema::from_unqualified_fields(
                schema_proj.fields().clone(),
                HashMap::default(),
            )?),
        ));

        state.create_physical_plan(&final_proj).await
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
