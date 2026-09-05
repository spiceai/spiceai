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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
        /// Encoded global BM25 collection statistics
        /// ([`crate::generation::text_search::GlobalBm25Stats`], JSON) a
        /// distributed search scores against. `None` scores with the local
        /// partition's statistics.
        #[serde(default)]
        global_stats: Option<String>,
        /// The partition's Tantivy reader generation observed while gathering
        /// `global_stats`, set alongside it. `None` when `global_stats` is.
        #[serde(default)]
        expected_generation: Option<u64>,
    },
    /// Created by `vector_search(tbl, query, [col], [limit], [include_score], [distance_metric => "cosine" | "l2"])`
    VectorSearch {
        table: String,
        query: String,
        column: Option<String>,
        limit: Option<usize>,
        include_score: Option<bool>,
        /// Distance metric name ("cosine" or "l2"). `None` = default (cosine).
        distance_metric: Option<String>,
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
    /// When `false`, the [`SEARCH_SCORE_COLUMN_NAME`] column is projected out of
    /// both the advertised schema and the scan result. When `true` (default),
    /// the score column is exposed so callers can order/inspect results.
    pub include_score: bool,
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
    /// Mirrors [`SearchIndex::supports_distributed_global_stats`] for the index
    /// this provider was built from. `false` unless constructed via
    /// [`SearchQueryProvider::try_from_index`] with a capable index.
    pub supports_distributed_global_stats: bool,
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
            include_score: true,
            scan_callback: None,
            constraints: None,
            udtf_source: None,
            supports_distributed_global_stats: false,
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

    /// When set to `false`, the advertised schema and scan output exclude the
    /// internal score column ([`SEARCH_SCORE_COLUMN_NAME`]).
    #[must_use]
    pub fn with_include_score(mut self, include_score: bool) -> Self {
        self.include_score = include_score;
        // Schema field ordering depends on `include_score` (the `_score`
        // column is removed when `false`), and stored constraints reference
        // positional indices into the advertised schema. Recompute so PK
        // indices stay consistent with the current `schema()`.
        self.constraints = Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            self.schema()
                .fields()
                .iter()
                .enumerate()
                .filter_map(|(i, f)| {
                    if self.primary_key.contains(f.name()) {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect(),
        )]));
        self
    }

    pub fn try_from_index(
        search_index: &Arc<dyn SearchIndex>,
        table_provider: Arc<dyn TableProvider>,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let search_index_query = search_index.query_table_provider(query)?;
        let mut provider = Self::new(
            search_index_query,
            table_provider,
            search_index.search_column(),
            search_index
                .primary_fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            limit,
        );
        provider.supports_distributed_global_stats =
            search_index.supports_distributed_global_stats();
        Ok(provider)
    }

    /// The tighter of [`Self::pre_limit`] (the `limit` argument the caller gave
    /// `vector_search()` / `text_search()`) and any limit `DataFusion` pushed into the scan,
    /// or `None` when neither side asked for one.
    ///
    /// This has to bound the provider's *output*, not just its search-index input:
    /// [`Self::join_with_base`] joins on [`Self::primary_key`], and nothing requires the base
    /// table to hold exactly one row per key, so the join can emit more rows than it consumed.
    ///
    /// The bound is a row count, and rows sharing a key tie under the sort below (`_score`
    /// then primary key), so which of them survives is unspecified — and `N` rows can
    /// represent fewer than `N` distinct hits. [`Self::new`] separately advertises that key as
    /// a `PrimaryKey` constraint it has not verified. #13289 tracks both.
    fn effective_limit(pre_limit: Option<usize>, limit: Option<usize>) -> Option<usize> {
        [pre_limit, limit].into_iter().flatten().min()
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
                // The base table decides which rows exist, so a hit whose primary key is
                // not there is a stale index entry rather than a result. An outer join
                // would emit it with the base-table columns NULL-padded and a real
                // `_score`, turning index staleness into a row the dataset does not have.
                JoinType::Inner,
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
        match_required_by_filter: bool,
    ) -> Result<LogicalPlanBuilder, DataFusionError> {
        let search_col = self.search_column.as_str();
        let search_offset = ChunkedSearchIndex::chunking_offset_col(search_col);
        // If projection doesn't include/need the 'match' column, early exit.
        // Or if its not a chunked search query (doesn't have offsets in schema).
        let match_not_required = !match_required_by_filter
            && projection
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
                // Stored offsets are 0-based character offsets; DataFusion
                // `substring` is 1-based and character-counted, so the start
                // position is `chunk_offset[1] + 1` and the length is
                // `chunk_offset[2] - chunk_offset[1]`. See issue #11269.
                // cast(
                //   substring(
                //      search_column, chunk_offset[1] + 1, chunk_offset[2] - chunk_offset[1]),
                //   ),
                //  'Utf8') as '_match'
                cast(
                    substring(
                        ident(search_col),
                        binary_expr(first.clone(), Operator::Plus, lit(1)),
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

        // When `include_score = false`, drop the internal score column from the
        // advertised schema so callers of `SELECT * FROM text_search(..., include_score => false)`
        // don't see it.
        if !self.include_score {
            fields_map.remove(SEARCH_SCORE_COLUMN_NAME);
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
        // `_match` is synthesized after the search index and base table are joined, so it
        // cannot be applied by either input scan. Keep its predicate above this provider.
        Ok(filters
            .iter()
            .map(|filter| {
                if expr_references_match_column(filter) {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    // Like `ViewTable`, a filter is added on `scan` when needed.
                    TableProviderFilterPushDown::Exact
                }
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
        if let Some(ref callback) = self.scan_callback {
            callback().await;
        }

        // `_match` is synthesized below, after the search index and base table are joined.
        // Do not pass predicates that reference it to either input plan.
        let (match_filters, input_filters): (Vec<Expr>, Vec<Expr>) = filters
            .iter()
            .cloned()
            .partition(expr_references_match_column);
        let match_required_by_filter = !match_filters.is_empty();

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
            if !match_required_by_filter && !proj.contains(&match_idx) {
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
            &input_filters,
        )?;
        let mut search_lp = match (
            just_use_index,
            input_filters.iter().cloned().reduce(Expr::and),
        ) {
            (true, None) => search_base.limit(0, self.pre_limit)?.limit(0, limit)?,
            (true, Some(filter)) => search_base
                .filter(filter)?
                .limit(0, self.pre_limit)?
                .limit(0, limit)?,
            (false, _) => {
                // Add supported filters BEFORE the pre_limit so they can be pushed down
                // into the search index scan by DataFusion's PushDownFilter optimizer.
                let search_index = if let Some(filter) =
                    exprs_supported(&input_filters, search_base.schema())
                        .iter()
                        .cloned()
                        .reduce(Expr::and)
                {
                    search_base.filter(filter)?.limit(0, self.pre_limit)?
                } else {
                    search_base.limit(0, self.pre_limit)?
                };

                self.join_with_base(inner_proj.as_ref(), search_index, &input_filters)?
            }
        };

        search_lp =
            self.add_match_column(inner_proj.as_ref(), search_lp, match_required_by_filter)?;
        if let Some(filter) = match_filters.into_iter().reduce(Expr::and) {
            search_lp = search_lp.filter(filter)?;
        }

        let search_lp = search_lp.sort_with_limit(
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
            Self::effective_limit(self.pre_limit, limit),
        )?;

        // Add final
        let final_plan = search_lp
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

fn expr_references_match_column(expr: &Expr) -> bool {
    expr.column_refs()
        .iter()
        .any(|column| column.name() == SEARCH_MATCH_COLUMN_NAME)
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
    use arrow::array::{
        Array, FixedSizeListArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A base table of `(id, content, extra)` rows. `extra` is deliberately absent from the
    /// search index so a `SELECT *` cannot be served by the index alone and the join under
    /// test is actually planned.
    fn base_table_of(
        rows: &[(i64, &str, &str)],
    ) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(_, content, _)| *content)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, extra)| *extra).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    /// Base table: the source of truth, one row per `id`.
    fn base_table() -> Result<Arc<dyn TableProvider>, DataFusionError> {
        base_table_of(&[(1, "dog elephant", "a"), (2, "cat", "b")])
    }

    /// A base table holding **two** rows for `id = 1`, so an inner join on `id` emits more
    /// rows than the search index handed it. This is in-contract: in production the join key
    /// is whatever the index declared through `SearchIndex::primary_fields()`, and no
    /// uniqueness check stands between that declaration and the join.
    fn base_table_with_repeated_key() -> Result<Arc<dyn TableProvider>, DataFusionError> {
        base_table_of(&[
            (1, "dog elephant", "a1"),
            (1, "dog elephant", "a2"),
            (2, "cat", "b"),
        ])
    }

    /// A provider over [`base_table_with_repeated_key`], with `pre_limit` carrying the
    /// caller's `vector_search(.., N)` argument.
    fn provider_over_repeated_key(
        hits: &[(i64, f64)],
        pre_limit: Option<usize>,
    ) -> Result<SearchQueryProvider, DataFusionError> {
        Ok(SearchQueryProvider::new(
            search_index_plan(hits)?,
            base_table_with_repeated_key()?,
            "content".to_string(),
            vec!["id".to_string()],
            pre_limit,
        ))
    }

    /// Run `sql` against [`provider_over_repeated_key`] registered as `searched`.
    async fn search_repeated_key(
        hits: &[(i64, f64)],
        pre_limit: Option<usize>,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "searched",
            Arc::new(provider_over_repeated_key(hits, pre_limit)?),
        )?;
        ctx.sql(sql).await?.collect().await
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// A search index holding one entry per `(id, score)` pair. An `id` absent
    /// from [`base_table`] stands in for an entry the source row no longer backs.
    fn search_index_plan(hits: &[(i64, f64)]) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    hits.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["dog elephant"; hits.len()])),
                Arc::new(Float64Array::from(
                    hits.iter().map(|(_, score)| *score).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let index: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);

        Ok(Arc::new(
            LogicalPlanBuilder::scan(
                "search_index_source",
                Arc::new(DefaultTableSource::new(index)),
                None,
            )?
            .build()?,
        ))
    }

    fn chunked_search_index_plan() -> Result<Arc<LogicalPlan>, DataFusionError> {
        let offset_field = Arc::new(Field::new("item", DataType::Int32, false));
        let offset_column = ChunkedSearchIndex::chunking_offset_col("content");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
            Field::new(
                offset_column,
                DataType::FixedSizeList(Arc::clone(&offset_field), 2),
                false,
            ),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, true),
        ]));
        let offsets = FixedSizeListArray::try_new(
            offset_field,
            2,
            Arc::new(Int32Array::from(vec![0, 3])),
            None,
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["dog elephant"])),
                Arc::new(offsets),
                Arc::new(Float64Array::from(vec![0.5])),
            ],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let index: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);

        Ok(Arc::new(
            LogicalPlanBuilder::scan(
                "chunked_search_index_source",
                Arc::new(DefaultTableSource::new(index)),
                None,
            )?
            .build()?,
        ))
    }

    /// `SELECT id, extra` over a search of [`base_table`] whose index holds `hits`.
    async fn search_ids_and_extra(
        hits: &[(i64, f64)],
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let provider = SearchQueryProvider::new(
            search_index_plan(hits)?,
            base_table()?,
            "content".to_string(),
            vec!["id".to_string()],
            None,
        );

        let ctx = SessionContext::new();
        ctx.register_table("searched", Arc::new(provider))?;
        ctx.sql("SELECT id, extra FROM searched ORDER BY id")
            .await?
            .collect()
            .await
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id column is Int64");
                (0..b.num_rows()).map(|i| col.value(i)).collect::<Vec<_>>()
            })
            .collect()
    }

    /// Regression test for #12089: an unfiltered search must not surface an index
    /// entry whose primary key is absent from the base table. Under an outer join
    /// the stale `id = 999` entry came back with `extra` NULL and a real `_score` —
    /// a row the dataset does not contain — and because it scored highest it was
    /// the first result a top-k search would spend a slot on.
    #[tokio::test]
    async fn a_hit_with_no_base_row_is_dropped() -> Result<(), DataFusionError> {
        let batches = search_ids_and_extra(&[(1, 0.5), (999, 0.98)]).await?;

        assert_eq!(
            ids(&batches),
            vec![1],
            "only the hit backed by a base-table row should be returned"
        );

        // The surviving row must carry real base-table values, not NULL padding.
        let extra = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("extra column is Utf8");
        assert!(
            !extra.is_null(0),
            "base-table columns must be populated for a live hit"
        );
        assert_eq!(extra.value(0), "a");

        Ok(())
    }

    /// The join must not drop live hits: every base row the index knows about is
    /// still returned, so the fix cannot pass by filtering too aggressively.
    #[tokio::test]
    async fn every_hit_backed_by_a_base_row_survives() -> Result<(), DataFusionError> {
        let batches = search_ids_and_extra(&[(1, 0.5), (2, 0.25)]).await?;

        assert_eq!(
            ids(&batches),
            vec![1, 2],
            "both live hits should be returned"
        );

        Ok(())
    }

    /// Regression test for #12233: `_match` exists only after the provider joins
    /// chunked search results with the base table, so its predicate must not be
    /// planned against the base-table scan.
    #[tokio::test]
    async fn match_filter_is_applied_after_match_column_is_synthesized()
    -> Result<(), DataFusionError> {
        let provider = SearchQueryProvider::new(
            chunked_search_index_plan()?,
            base_table()?,
            "content".to_string(),
            vec!["id".to_string()],
            None,
        );
        let ctx = SessionContext::new();
        ctx.register_table("searched", Arc::new(provider))?;

        let batches = ctx
            .sql("SELECT id, extra FROM searched WHERE _match LIKE '%dog%'")
            .await?
            .collect()
            .await?;

        assert_eq!(ids(&batches), vec![1]);
        Ok(())
    }

    /// Regression test for #13274: the `limit` argument to `vector_search()` /
    /// `text_search()` bounds the provider's output, not just how many entries it
    /// reads from the search index. The limit was applied only to the search-index
    /// input, so the join with the base table emitted `2` rows for a requested `1`.
    #[tokio::test]
    async fn a_requested_limit_survives_a_join_that_fans_out() -> Result<(), DataFusionError> {
        let batches = search_repeated_key(
            &[(1, 0.9), (2, 0.5)],
            Some(1),
            "SELECT id, extra FROM searched",
        )
        .await?;

        assert_eq!(
            row_count(&batches),
            1,
            "a search asked for 1 row must return at most 1 row, however many base-table rows the join finds for that key"
        );

        Ok(())
    }

    /// The rows kept under the limit must be the highest-ranked ones. Capping the
    /// output is only correct if it drops the *worst* rows: keeping both `id = 1`
    /// rows and dropping the better-scoring `id = 2` would satisfy a count-only
    /// assertion while returning the wrong results.
    #[tokio::test]
    async fn the_rows_kept_under_the_limit_are_the_highest_scoring() -> Result<(), DataFusionError>
    {
        let batches = search_repeated_key(
            &[(1, 0.5), (2, 0.98)],
            Some(2),
            "SELECT id, extra FROM searched ORDER BY id",
        )
        .await?;

        assert_eq!(
            ids(&batches),
            vec![1, 2],
            "the top-scoring hit must keep its slot; the surplus row of the lower-scoring key is the one dropped"
        );

        Ok(())
    }

    /// Every combination of the rule, asserted where the provider actually decides it. The
    /// end-to-end form of the tighter-SQL-limit direction proves nothing on its own: the outer
    /// `LIMIT` bounds the result whatever this provider returns.
    #[test]
    fn the_effective_limit_is_the_tighter_of_the_two() {
        let effective = SearchQueryProvider::effective_limit;

        assert_eq!(
            effective(Some(5), Some(1)),
            Some(1),
            "a tighter limit from the SQL plan wins"
        );
        assert_eq!(
            effective(Some(1), Some(5)),
            Some(1),
            "the caller's search argument wins over a looser SQL limit"
        );
        assert_eq!(
            effective(Some(3), None),
            Some(3),
            "the caller's search argument applies with no SQL limit at all"
        );
        assert_eq!(
            effective(None, Some(3)),
            Some(3),
            "a SQL limit applies with no search argument"
        );
        assert_eq!(
            effective(None, None),
            None,
            "neither side asked for a limit, so none is invented"
        );
    }

    /// The caller's argument bounds the output end to end, where only this provider
    /// can enforce it: a looser SQL limit leaves the surplus join rows to it.
    #[tokio::test]
    async fn the_search_argument_wins_over_a_looser_sql_limit() -> Result<(), DataFusionError> {
        let batches = search_repeated_key(
            &[(1, 0.9), (2, 0.5)],
            Some(1),
            "SELECT id, extra FROM searched LIMIT 5",
        )
        .await?;

        assert_eq!(row_count(&batches), 1);
        Ok(())
    }

    /// The cap must not be invented: with no limit on either side every joined row
    /// is still returned, including the surplus rows of a repeated key.
    #[tokio::test]
    async fn an_unlimited_search_still_returns_every_joined_row() -> Result<(), DataFusionError> {
        let batches = search_repeated_key(
            &[(1, 0.9), (2, 0.5)],
            None,
            "SELECT id, extra FROM searched",
        )
        .await?;

        assert_eq!(
            row_count(&batches),
            3,
            "two rows for id = 1 plus one for id = 2"
        );

        Ok(())
    }

    /// The plan-shape half of #13274, and the acceptance criterion stated directly: the
    /// requested limit must survive as a fetch *above* the join, not only on the search-index
    /// side beneath it. It is asserted on the plan rather than on a row count because the
    /// property does not depend on how much the join fans out — the sibling tests above cover
    /// the one fan-out shape this module can build, while a deployment can multiply rows for
    /// reasons a unit test cannot reproduce.
    ///
    /// Deliberately shape-locked, so it is coupled to `DataFusion`'s rendered operator names.
    /// The SQL must carry no outer `LIMIT`: that would plant a `fetch=` of its own above the
    /// join and the assertion would pass with the fix reverted, so the fetch value is pinned
    /// to `pre_limit` to make such a change fail rather than go quiet.
    #[tokio::test]
    async fn the_fetch_is_planned_above_the_join() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "searched",
            Arc::new(provider_over_repeated_key(&[(1, 0.9), (2, 0.5)], Some(1))?),
        )?;

        let plan = ctx
            .sql("SELECT id, extra FROM searched")
            .await?
            .create_physical_plan()
            .await?;
        let rendered = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();

        let indent = |line: &str| line.len() - line.trim_start().len();
        let (join_line, join_indent) = rendered
            .lines()
            .enumerate()
            .find_map(|(i, line)| line.contains("HashJoinExec").then(|| (i, indent(line))))
            .unwrap_or_else(|| {
                panic!("the base-table join is what the limit has to survive:\n{rendered}")
            });

        assert!(
            rendered
                .lines()
                .take(join_line)
                .any(|line| line.contains("fetch=1") && indent(line) < join_indent),
            "the requested limit of 1 must be planned as a fetch above the join, not only below it:\n{rendered}"
        );

        Ok(())
    }
}
