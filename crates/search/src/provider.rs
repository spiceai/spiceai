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
    common::{Column, Constraint, Constraints, DFSchemaRef, JoinType},
    datasource::{DefaultTableSource, TableType},
    error::DataFusionError,
    logical_expr::{
        LogicalPlan, LogicalPlanBuilder, Operator, SortExpr, TableProviderFilterPushDown,
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, array_element, binary_expr, cast, col, ident, lit, substring},
    sql::TableReference,
};
use datafusion_expr::select_expr::SelectExpr;
use futures::future::BoxFuture;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{
    SEARCH_MATCH_COLUMN_NAME, SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, chunking::ChunkedSearchIndex},
};

/// Tracks the original UDTF invocation that produced this `SearchQueryProvider`.
///
/// This is used for serialization during distributed query execution with Ballista.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UdtfSource {
    /// Created by `text_search(tbl, query, [col], [limit], [include_score])`
    TextSearch {
        table: String,
        query: String,
        column: Option<String>,
        limit: Option<usize>,
        include_score: Option<bool>,
    },
    /// Created by `vector_search(tbl, query, [col], [limit], [include_score])`
    VectorSearch {
        table: String,
        query: String,
        column: Option<String>,
        limit: Option<usize>,
        include_score: Option<bool>,
    },
}

/// Performs a search on a given [`SearchIndex`] and combine with the underlying [`TableProvider`]
/// if required by filters or additional columns in the projection.
#[derive(Clone)]
pub struct SearchQueryProvider {
    pub search_index_query: Arc<LogicalPlan>,
    pub table_provider: Arc<dyn TableProvider>,
    pub search_column: String,
    pub primary_key: Vec<String>,
    pub constraints: Option<Constraints>,
    pub pre_limit: Option<usize>,
    /// Optional callback invoked before a table scan is performed.
    ///
    /// This callback can be used to perform custom actions (such as logging, metrics, or side effects)
    /// immediately before the provider executes a scan operation. The callback is asynchronous and
    /// will be awaited before the scan proceeds. If `None`, no callback is invoked.
    pub scan_callback: Option<Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>>,
    /// Tracks the original UDTF invocation for distributed serialization.
    ///
    /// This is set when the provider is created via a UDTF like `text_search()` or `vector_search()`.
    /// It enables `SpiceLogicalCodec` to serialize and reconstruct this provider on remote executors.
    pub udtf_source: Option<UdtfSource>,
}

impl std::fmt::Debug for SearchQueryProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchQueryProvider")
            .field("search_index_query", &self.search_index_query)
            .field("table_provider", &self.table_provider)
            .field("search_column", &self.search_column)
            .field("primary_key", &self.primary_key)
            .field("pre_limit", &self.pre_limit)
            .field("udtf_source", &self.udtf_source)
            .finish_non_exhaustive()
    }
}

impl SearchQueryProvider {
    pub fn new(
        search_index_query: Arc<LogicalPlan>,
        table_provider: Arc<dyn TableProvider>,
        search_column: String,
        primary_key: Vec<String>,
        pre_limit: Option<usize>,
    ) -> Self {
        let mut slf = Self {
            search_index_query,
            table_provider,
            search_column,
            primary_key,
            pre_limit,
            scan_callback: None,
            constraints: None,
            udtf_source: None,
        };

        // Create `constraints` based on [`Self::schema`]
        slf.constraints = Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            slf.schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if slf.primary_key.contains(f.name()) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect(),
        )]));
        slf
    }

    /// `func` will be called at the beginning of any [`Self::scan`].
    #[must_use]
    pub fn call_on_scan(
        mut self,
        func: Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>,
    ) -> Self {
        self.scan_callback = Some(func);
        self
    }

    /// Sets the UDTF source for distributed serialization.
    #[must_use]
    pub fn with_udtf_source(mut self, source: UdtfSource) -> Self {
        self.udtf_source = Some(source);
        self
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

        // Include columns for all filters.
        let before_final_filter: Vec<String> = filters
            .iter()
            .flat_map(|f| {
                f.column_refs()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect::<Vec<_>>()
            })
            // Sort for deterministic LogicalPlans
            .collect::<HashSet<String>>()
            .union(&base_table_cols)
            .cloned()
            .collect::<Vec<String>>()
            .into_iter()
            .sorted()
            .collect();

        let mut scan = LogicalPlanBuilder::scan(
            "base_table",
            Arc::new(DefaultTableSource::new(
                Arc::clone(&self.table_provider) as Arc<dyn TableProvider>
            )),
            Some(projection_from_columns(
                &self.table_provider.schema(),
                &before_final_filter,
            )),
        )?;

        if let Some(f) = self.base_table_filters(filters)? {
            scan = scan.filter(f)?;
        }

        // Only return columns 1. asked for in projection or 2. Needed by filters but not in search schema.
        // Previous projection `before_final_filter` included all columns needed by filters.
        base_table_cols.extend(columns_missing_from(filters, search_index_schema));
        scan.project(
            base_table_cols
                .iter()
                .map(|c| SelectExpr::Expression(ident(c)))
                .sorted_by_key(ToString::to_string), // Sort for deterministic LogicalPlans
        )?
        .build()
    }

    // Get filters that can be pushed down to the base table
    fn base_table_filters(&self, filters: &[Expr]) -> Result<Option<Expr>, DataFusionError> {
        let filter_refs: Vec<_> = filters.iter().collect();
        let supported_filters = self
            .table_provider
            .supports_filters_pushdown(filter_refs.as_slice())?;

        Ok(filters
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
            .reduce(Expr::and))
    }

    fn join_with_base(
        &self,
        projection: Option<&Vec<usize>>,
        search_index_table: LogicalPlanBuilder,
        filters: &[Expr],
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let schema = self.schema();
        let search_index_schema = Arc::clone(search_index_table.schema());
        let projection_column_names: Vec<String> = match projection {
            None => schema.fields().iter().map(|f| f.name().clone()).collect(),
            Some(proj) => schema
                .project(proj)?
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        };
        let primary_key_column_names: std::collections::HashSet<String> =
            self.primary_key.iter().cloned().collect();

        let mut bldr = search_index_table
            // Add subquery so that we can uniquely identify columns between search index and underlying table scan.
            .alias("search_index")?
            .join(
                self.underlying_table_scan(projection_column_names, filters, &search_index_schema)?,
                JoinType::Left,
                self.primary_key
                    .iter()
                    .map(|pk| {
                        (
                            Column::new(Some(TableReference::parse_str("search_index")), pk),
                            Column::new(Some(TableReference::parse_str("base_table")), pk),
                        )
                    })
                    .collect(),
                // Can pushdown all filters except those on PKs (since these PK Expr will be unqualified, DF will find them ambigious).
                filters
                    .iter()
                    .filter(|&f| {
                        f.column_refs()
                            .iter()
                            .any(|col| !primary_key_column_names.contains(col.name()))
                    })
                    .cloned()
                    .reduce(Expr::and),
            )?;
        let join_schema = Arc::clone(bldr.schema());

        bldr = bldr.project(
            join_schema
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
                }),
        )?;

        // Apply all filters after JOIN. This is to ensure that if a filter is pushed onto RHS,
        // LHS (i.e. from search index) doesn't return row violating filter.
        if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
            bldr = bldr.filter(filter)?;
        }

        Ok(bldr)
    }

    fn match_column_index(&self) -> Option<usize> {
        self.schema()
            .column_with_name(SEARCH_MATCH_COLUMN_NAME)
            .map(|(i, _)| i)
    }

    pub fn add_match_column(
        &self,
        projection: Option<&Vec<usize>>,
        input: LogicalPlanBuilder,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let search_col = self.search_column.as_str();
        let search_offset = ChunkedSearchIndex::chunking_offset_col(search_col);
        // If projection doesn't include/need the 'match' column, early exit.
        // Or if its not a chunked search query (doesn't have offsets in schema).
        let match_not_required = projection
            .is_some_and(|proj| self.match_column_index().is_none_or(|i| !proj.contains(&i)));
        let chunked_search_field = self
            .schema()
            .column_with_name(search_offset.as_str())
            .is_some();
        if match_not_required || !chunked_search_field {
            return Ok(input);
        }

        let first = array_element(col(&search_offset), lit(1));
        let second = array_element(col(&search_offset), lit(2));

        let input_with_match: Vec<Expr> = [
            input
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect(),
            vec![
                // cast(
                //   substring(
                //      search_column, chunk_offset[1], chunk_offset[2] - chunk_offset[1]),
                //   ),
                //  'Utf8') as '_match'
                cast(
                    substring(
                        col(search_col),
                        array_element(col(search_offset), lit(1)),
                        binary_expr(second, Operator::Minus, first),
                    ),
                    DataType::Utf8,
                )
                .alias(SEARCH_MATCH_COLUMN_NAME),
            ],
        ]
        .concat()
        .into_iter()
        .collect();

        input.project(input_with_match)
    }

    fn search_index_table_is_sufficient(
        &self,
        search_index_schema: &DFSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<bool, DataFusionError> {
        let search_index_columns: HashSet<String> = search_index_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
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
        Ok(columns_missing_from(filters, search_index_schema).is_empty())
    }
}

#[async_trait]
impl TableProvider for SearchQueryProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
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

        // Add `match` only if its a chunked search field (chunking offsets must be from this search index).
        if self
            .search_index_query
            .schema()
            .has_column_with_unqualified_name(&ChunkedSearchIndex::chunking_offset_col(
                self.search_column.as_str(),
            ))
            && fields_map.contains_key(&self.search_column)
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if let Some(ref callback) = self.scan_callback {
            callback().await;
        }

        // Final schema to match requested projection
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

        // Inner projection to ensure that if we need `match`, we get underlying search column.
        let inner_proj: Option<Vec<_>> = projection.cloned().map(|proj| {
            let Some(match_idx) = self.match_column_index() else {
                return proj;
            };
            if !proj.contains(&match_idx) {
                return proj;
            }
            let mut proj2 = proj;
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

        // Build search index base plan WITHOUT pre_limit so that filters can be added
        // below the limit. DataFusion cannot push filters past a Limit node, so adding
        // filters after the limit prevents them from reaching the underlying search index
        // (e.g. S3VectorsQueryExec), causing both worse performance and incorrect results
        // (top-K-then-filter vs top-K-of-filtered).
        let search_base = LogicalPlanBuilder::new_from_arc(Arc::clone(&self.search_index_query))
            .alias("search_index")?;

        let just_use_index = self.search_index_table_is_sufficient(
            &Arc::clone(self.search_index_query.schema()),
            inner_proj.as_ref(),
            filters,
        )?;
        let search_lp = match (just_use_index, filters.iter().cloned().reduce(Expr::and)) {
            (true, None) => search_base.limit(0, self.pre_limit)?.limit(0, limit)?,
            (true, Some(filter)) => search_base
                .filter(filter)?
                .limit(0, self.pre_limit)?
                .limit(0, limit)?,
            (false, _) => {
                // Add supported filters BEFORE the pre_limit so they can be pushed down
                // into the search index scan by DataFusion's PushDownFilter optimizer.
                let search_index = if let Some(filter) =
                    exprs_supported(filters, search_base.schema())
                        .iter()
                        .cloned()
                        .reduce(Expr::and)
                {
                    search_base.filter(filter)?.limit(0, self.pre_limit)?
                } else {
                    search_base.limit(0, self.pre_limit)?
                };

                self.join_with_base(inner_proj.as_ref(), search_index, filters)?
            }
        }
        .sort_with_limit(
            {
                let mut sort_exprs = vec![SortExpr::new(
                    Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                    false, // descending
                    false, // nulls_last (null scores should rank lowest, consistent with other search sort sites)
                )];
                sort_exprs.extend(self.primary_key.iter().map(|pk| {
                    SortExpr::new(
                        Expr::Column(Column::new_unqualified(pk)),
                        true, // ascending
                        true, // nulls_first
                    )
                }));
                sort_exprs
            },
            limit,
        )?;

        // Add final
        let final_plan = self
            .add_match_column(inner_proj.as_ref(), search_lp)?
            .project(
                schema_proj
                    .fields()
                    .into_iter()
                    .map(|f| ident(f.name().clone())),
            )?
            .build()?;
        state.create_physical_plan(&final_plan).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use arrow::record_batch::RecordBatch;
    use datafusion::{datasource::empty::EmptyTable, prelude::SessionContext};

    /// A table provider that records the filters passed to its `scan()` method.
    /// This lets us verify that filters are pushed down through the logical plan
    /// into the underlying table provider's scan.
    #[derive(Debug)]
    struct FilterRecordingProvider {
        schema: SchemaRef,
        recorded_filters: Arc<Mutex<Vec<Vec<Expr>>>>,
    }

    impl FilterRecordingProvider {
        fn new(schema: SchemaRef) -> (Arc<Self>, Arc<Mutex<Vec<Vec<Expr>>>>) {
            let filters = Arc::new(Mutex::new(Vec::new()));
            (
                Arc::new(Self {
                    schema,
                    recorded_filters: Arc::clone(&filters),
                }),
                filters,
            )
        }
    }

    #[async_trait]
    impl TableProvider for FilterRecordingProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn table_type(&self) -> TableType {
            TableType::Base
        }
        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
            Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
        }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            filters: &[Expr],
            _limit: Option<usize>,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            self.recorded_filters
                .lock()
                .expect("lock")
                .push(filters.to_vec());
            // Return a valid empty plan
            Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                Arc::clone(&self.schema),
            )))
        }
    }

    fn build_search_query_provider(
        search_index_provider: Arc<dyn TableProvider>,
        base_table_provider: Arc<dyn TableProvider>,
        pre_limit: Option<usize>,
    ) -> SearchQueryProvider {
        let search_index_query = LogicalPlanBuilder::scan(
            "search_index_inner",
            Arc::new(DefaultTableSource::new(search_index_provider)),
            None,
        )
        .expect("scan builder")
        .build()
        .expect("build");

        SearchQueryProvider::new(
            Arc::new(search_index_query),
            base_table_provider,
            "pk".to_string(),
            vec!["pk".to_string()],
            pre_limit,
        )
    }

    /// Regression test for https://github.com/spiceai/spiceai/issues/10149
    ///
    /// Verifies that filters passed to `SearchQueryProvider::scan()` are pushed
    /// down to the underlying search index table provider when a `pre_limit` is set.
    ///
    /// Previously, the pre_limit was added to the logical plan BEFORE filters,
    /// preventing DataFusion's `PushDownFilter` from pushing filters past the
    /// `Limit` node into the inner `TableScan`. This caused filters to never
    /// reach the search index (e.g. `S3VectorsQueryExec`), producing both worse
    /// performance and incorrect results (top-K-then-filter vs top-K-of-filtered).
    #[tokio::test]
    async fn filters_pushed_down_with_pre_limit_index_only() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
            Field::new("status", DataType::Utf8, true),
        ]));

        let (search_provider, recorded_filters) = FilterRecordingProvider::new(Arc::clone(&schema));

        // Base table only has pk; search index has all columns → just_use_index = true
        let base_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Utf8, false)]));
        let base_table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(base_schema));

        let sqp = build_search_query_provider(
            search_provider,
            base_table,
            Some(50), // pre_limit that previously blocked filter pushdown
        );

        let ctx = SessionContext::new();
        let state = ctx.state();

        let filter = col("status").eq(lit("active"));
        let result = sqp.scan(&state, None, &[filter.clone()], Some(10)).await;
        assert!(result.is_ok(), "scan failed: {:?}", result.err());

        let filters = recorded_filters.lock().expect("lock");
        assert!(
            !filters.is_empty(),
            "inner provider scan() was never called"
        );
        assert!(
            filters
                .iter()
                .any(|scan_filters| scan_filters.contains(&filter)),
            "filters were NOT pushed down to the search index table provider; \
             they are stuck above the pre_limit Limit node (issue #10149)"
        );
    }

    /// Same as above but for the join path (just_use_index = false), which is
    /// the path described in issue #10149 where filters couldn't pass through
    /// the Left Join into the inner search index.
    #[tokio::test]
    async fn filters_pushed_down_with_pre_limit_join_path() {
        let search_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
            Field::new("status", DataType::Utf8, true),
        ]));

        let (search_provider, recorded_filters) =
            FilterRecordingProvider::new(Arc::clone(&search_schema));

        // Base table has extra column "data" not in search index → just_use_index = false
        let base_schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, true),
            Field::new("status", DataType::Utf8, true),
        ]));
        let base_batch = RecordBatch::new_empty(Arc::clone(&base_schema));
        let base_table: Arc<dyn TableProvider> = Arc::new(
            datafusion::datasource::MemTable::try_new(base_schema, vec![vec![base_batch]])
                .expect("MemTable"),
        );

        let sqp = build_search_query_provider(
            search_provider,
            base_table,
            Some(50), // pre_limit
        );

        let ctx = SessionContext::new();
        let state = ctx.state();

        // Filter on "status" which IS in the search index schema,
        // so exprs_supported() will include it for pushdown
        let filter = col("status").eq(lit("active"));
        let result = sqp.scan(&state, None, &[filter.clone()], Some(10)).await;
        assert!(result.is_ok(), "scan failed: {:?}", result.err());

        let filters = recorded_filters.lock().expect("lock");
        assert!(
            !filters.is_empty(),
            "inner provider scan() was never called"
        );
        assert!(
            filters.iter().any(|scan_filters| scan_filters
                .iter()
                .any(|pushed_filter| pushed_filter == &filter)),
            "filters were NOT pushed down to the search index table provider in \
             the join path; they are stuck above the pre_limit (issue #10149)"
        );
    }

    /// Verify that scan works correctly with no pre_limit (filters should
    /// still be pushed down regardless).
    #[tokio::test]
    async fn filters_pushed_down_without_pre_limit() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
            Field::new("status", DataType::Utf8, true),
        ]));

        let (search_provider, recorded_filters) = FilterRecordingProvider::new(Arc::clone(&schema));

        let base_schema = Arc::new(Schema::new(vec![Field::new("pk", DataType::Utf8, false)]));
        let base_table: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(base_schema));

        let sqp = build_search_query_provider(
            search_provider,
            base_table,
            None, // no pre_limit
        );

        let ctx = SessionContext::new();
        let state = ctx.state();

        let filter = col("status").eq(lit("active"));
        let result = sqp.scan(&state, None, &[filter.clone()], Some(10)).await;
        assert!(result.is_ok(), "scan failed: {:?}", result.err());

        let filters = recorded_filters.lock().expect("lock");
        assert!(
            !filters.is_empty(),
            "inner provider scan() was never called"
        );
        assert!(
            filters.iter().any(|scan_filters| !scan_filters.is_empty()),
            "filters should be pushed down even without a pre_limit"
        );
    }
}
