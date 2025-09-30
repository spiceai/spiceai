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

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Column, DFSchema, DFSchemaRef, JoinConstraint, JoinType, NullEquality},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{
        BinaryExpr, Cast, Filter, Join, Limit, LogicalPlan, Operator, Projection, Sort, SortExpr,
        SubqueryAlias, TableProviderFilterPushDown, TableScan, expr::Alias,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, array_element, col, substring},
    scalar::ScalarValue,
    sql::TableReference,
};

use crate::{
    SEARCH_MATCH_COLUMN_NAME, SEARCH_SCORE_COLUMN_NAME, chunking::ChunkedSearchIndex,
    index::SearchIndex,
};

/// Performs a search on a given [`SearchIndex`] and combine with the underlying [`TableProvider`]
/// if required by filters or additional columns in the projection.
#[derive(Debug, Clone)]
pub struct SearchQueryProvider {
    pub search_index_query: Arc<LogicalPlan>,
    pub table_provider: Arc<dyn TableProvider>,
    pub search_column: String,
    pub primary_key: Vec<String>,
    pub pre_limit: Option<usize>,
}

impl SearchQueryProvider {
    pub fn new(
        search_index_query: Arc<LogicalPlan>,
        table_provider: Arc<dyn TableProvider>,
        search_column: String,
        primary_key: Vec<String>,
        pre_limit: Option<usize>,
    ) -> Self {
        Self {
            search_index_query,
            table_provider,
            search_column,
            primary_key,
            pre_limit,
        }
    }

    pub fn try_from_index(
        search_index: &Arc<dyn SearchIndex>,
        table_provider: Arc<dyn TableProvider>,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let search_index_query = search_index.query_table_provider(query)?;
        Ok(Self::new(
            search_index_query,
            table_provider,
            search_index.search_column(),
            search_index
                .primary_fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            limit,
        ))
    }

    /// Build the underlying table scan, removing search index metadata columns from projection
    fn underlying_table_scan(
        &self,
        columns: Vec<String>,
        filters: &[Expr],
        search_index_schema: &DFSchemaRef,
    ) -> Result<LogicalPlan, DataFusionError> {
        let mut base_table_cols: HashSet<String> = columns.into_iter().collect();
        base_table_cols.remove(SEARCH_MATCH_COLUMN_NAME);
        for f in search_index_schema.fields() {
            base_table_cols.remove(f.name());
        }
        base_table_cols.extend(self.primary_key.clone());

        // Also include any columns needed for filters on base table.
        base_table_cols.extend(columns_missing_from(filters, search_index_schema));
        let base_table_cols: Vec<_> = base_table_cols.into_iter().collect();
        let mut base_proj =
            projection_from_columns(&self.table_provider.schema(), &base_table_cols);
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

        let schema = self.schema();
        let column_names: Vec<String> = match projection {
            None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            Some(proj) => schema
                .project(proj)?
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };

        let underlying_table_scan =
            self.underlying_table_scan(column_names, filters, search_index_proj.schema())?;

        // Build join conditions based on primary keys
        let on: Vec<(Expr, Expr)> = self
            .primary_key
            .iter()
            .map(|pk| {
                (
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("search_index")),
                        pk,
                    )),
                    Expr::Column(Column::new(
                        Some(TableReference::parse_str("base_table")),
                        pk,
                    )),
                )
            })
            .collect();

        // Build join schema
        let join_schema = search_index_proj
            .schema()
            .join(underlying_table_scan.schema())?;

        let primary_key_column_names: std::collections::HashSet<String> =
            self.primary_key.iter().cloned().collect();

        // Can pushdown all filters except those on PKs (since these PK Expr will be unqualified, DF will find them ambigious).
        let join_filters: Vec<Expr> = filters
            .iter()
            .filter(|&f| {
                f.column_refs()
                    .iter()
                    .any(|col| !primary_key_column_names.contains(col.name()))
            })
            .cloned()
            .collect();

        let join = LogicalPlan::Join(Join {
            left: Arc::new(search_index_proj),
            right: Arc::new(underlying_table_scan),
            join_type: JoinType::Left,
            join_constraint: JoinConstraint::On,
            on,
            filter: join_filters.into_iter().reduce(Expr::and),
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

        // Apply all filters after JOIN. This is to ensure that if a filter is pushed onto RHS,
        // LHS (i.e. from search index) doesn't return row violating filter.
        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
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
        let search_col = self.search_column.as_str();
        // If projection doesn't include/need the 'match' column, early exit.
        // Or if its not a chunked search query (doesn't have offsets in schema).
        let match_not_required = projection
            .is_some_and(|proj| self.match_column_index().is_none_or(|i| !proj.contains(&i)));
        let chunked_search_field = self
            .schema()
            .column_with_name(ChunkedSearchIndex::chunking_offset_col(search_col).as_str())
            .is_some();
        if match_not_required || !chunked_search_field {
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
                ChunkedSearchIndex::chunking_offset_col(search_col),
            )),
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
        );
        let second = array_element(
            Expr::Column(Column::new_unqualified(
                ChunkedSearchIndex::chunking_offset_col(search_col),
            )),
            Expr::Literal(ScalarValue::Int64(Some(2)), None),
        );

        // substring(search_column, chunk_offset[1], chunk_offset[2] - chunk_offset[1]) as 'match'
        let substr = Expr::Cast(Cast::new(
            Box::new(substring(
                col(search_col),
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

        // Ensure filters do not reference column not in search index.
        Ok(columns_missing_from(filters, search_index_table.schema()).is_empty())
    }
}

#[async_trait]
impl TableProvider for SearchQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut fields_map = self
            .search_index_query
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), Arc::clone(f)))
            .collect::<HashMap<String, FieldRef>>();

        // Only add if key not in search index (we chose search index columns in `scan` afterall).
        for f in self.table_provider.schema().fields() {
            if !fields_map.contains_key(f.name()) {
                fields_map.insert(f.name().clone(), Arc::clone(f));
            }
        }

        // Add `match` only if its a chunked search field.
        if fields_map.contains_key(&ChunkedSearchIndex::chunking_offset_col(
            self.search_column.as_str(),
        )) && fields_map.contains_key(&self.search_column)
        {
            fields_map.insert(
                SEARCH_MATCH_COLUMN_NAME.to_string(),
                Arc::new(Field::new(
                    SEARCH_MATCH_COLUMN_NAME.to_string(),
                    arrow_schema::DataType::Utf8,
                    false,
                )),
            );
        }

        let mut fields = fields_map.values().cloned().collect::<Vec<_>>();
        fields.sort_unstable();
        Arc::new(Schema::new(fields))
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // Like `ViewTable`, a filter is added on `scan` when needed
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let search_index_table = LogicalPlan::Limit(Limit {
            skip: None,
            fetch: self
                .pre_limit
                .map(|l| Box::new(Expr::Literal(ScalarValue::UInt32(Some(l as u32)), None))),
            input: LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::clone(&self.search_index_query),
                TableReference::parse_str("search_index"),
            )?)
            .into(),
        });

        // Ensure that if we need `match`, we get underlying search column.
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
                .column_with_name(self.search_column.as_str())
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
            // Pushdown indexes to search index
            let search_index = if let Some(filter) =
                exprs_supported(filters, search_index_table.schema())
                    .iter()
                    .cloned()
                    .reduce(Expr::and)
            {
                LogicalPlan::Filter(Filter::try_new(filter, search_index_table.into())?)
            } else {
                search_index_table
            };
            self.join_with_base(inner_proj.as_ref(), search_index, filters)?
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

// Convert to index projection for all unqualified column names. If c in `cols` is not in schema, it is ignored.
fn projection_from_columns(schema: &SchemaRef, cols: &[String]) -> Vec<usize> {
    cols.iter()
        .filter_map(|c| Some(schema.column_with_name(c.as_str())?.0))
        .collect()
}

// Return the unqualified names of columns missing from those referenced by in `expr`.
fn columns_missing_from(expr: &[Expr], schema: &DFSchemaRef) -> Vec<String> {
    let schema_cols = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();

    expr.iter()
        .flat_map(|e| {
            let filter_cols = e
                .column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>();
            filter_cols
                .difference(&schema_cols)
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

// Returns all expr in exprs that are supported by the `schema`.
fn exprs_supported(exprs: &[Expr], schema: &DFSchemaRef) -> Vec<Expr> {
    let schema_cols = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<HashSet<_>>();

    exprs
        .iter()
        .filter(|e| {
            e.column_refs()
                .iter()
                .map(|c| c.name().to_string())
                .collect::<HashSet<_>>()
                .is_subset(&schema_cols)
        })
        .cloned()
        .collect::<Vec<_>>()
}
