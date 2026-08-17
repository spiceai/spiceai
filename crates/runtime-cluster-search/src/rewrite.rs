/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Analyzer rule that rewrites a `text_search(...)` scan over a multi-node
//! accelerated table into a single distributed plan.
//!
//! On a single node, `text_search` reads one local Tantivy index and scores with
//! that index's collection statistics. In a distributed acceleration, each
//! executor holds only its partition's index, so a BM25 score from one executor
//! is not comparable with another's — a naive top-N merge across executors is
//! only approximate.
//!
//! This rule expresses an exact distributed search as one plan:
//!
//! ```text
//! Projection: <original scan columns>
//!   Extension: DistributedSearch{ table, query, column, fetch, executors }
//!     input = Aggregate: group_by=[term], aggr=[SUM(doc_freq), SUM(total_num_docs), SUM(total_num_tokens)]
//!               Union[ TableScan(text_search_stats(...) @ executor-i) ]
//! ```
//!
//! The `Aggregate` over `text_search_stats` legs gathers the global statistics (a
//! distributed sum of the additive per-partition statistics). The
//! [`DistributedSearchExec`](crate::exec::DistributedSearchExec)
//! drains that child, then scores each executor's partition with the global
//! statistics and merges the comparable results.

use std::{fmt::Write as _, sync::Arc};

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    common::{
        DFSchema, Result,
        tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    functions_aggregate::expr_fn::sum,
    logical_expr::{
        Expr, Extension, LogicalPlan, LogicalPlanBuilder, TableScan, col,
        utils::{conjunction, split_conjunction_owned},
    },
    optimizer::AnalyzerRule,
    sql::{
        TableReference,
        unparser::{Unparser, dialect::PostgreSqlDialect},
    },
};

use data_components::flightsql::{FlightSQLTable, FlightSqlClient};
use flight_client::cookie::CookieStore;
use runtime_cluster::ExecutorRegistry;
use search::SEARCH_SCORE_COLUMN_NAME;
use search::generation::text_search::GlobalBm25Stats;
use search::generation::text_search::bm25_stats::{
    STATS_DOC_FREQ_COLUMN, STATS_TERM_COLUMN, STATS_TOTAL_NUM_DOCS_COLUMN,
    STATS_TOTAL_NUM_TOKENS_COLUMN,
};
use search::provider::{SearchQueryProvider, UdtfSource};

use crate::exec::{DistributedExecutor, DistributedSearchParams};
use crate::node::DistributedSearchNode;

/// Decides whether a searched table should be distributed across executors.
///
/// The condition — the table is a multi-node accelerated table — is tested with
/// a type that lives in the runtime crate (`AcceleratedTable`), above this crate.
/// The runtime injects the check as this closure so the rule can live here
/// without depending upward. Returns `true` to distribute the search.
pub type SearchDistributionGate = Arc<dyn Fn(&Arc<dyn TableProvider>) -> bool + Send + Sync>;

/// Rewrites `text_search` scans over multi-node accelerated tables into a
/// distributed-search plan. Registered on the scheduler only (it needs the
/// executor registry).
pub struct DistributedSearchRewrite {
    registry: Arc<ExecutorRegistry>,
    should_distribute: SearchDistributionGate,
}

impl std::fmt::Debug for DistributedSearchRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedSearchRewrite")
            .finish_non_exhaustive()
    }
}

impl DistributedSearchRewrite {
    #[must_use]
    pub fn new(registry: Arc<ExecutorRegistry>, should_distribute: SearchDistributionGate) -> Self {
        Self {
            registry,
            should_distribute,
        }
    }

    /// The distributed rewrite of one `text_search` table scan, or `None` when
    /// the scan is not a distributable search (not a `SearchQueryProvider`, not
    /// a text search, or not over an accelerated table).
    ///
    /// `filter_predicate` is the predicate of a `Filter` node directly wrapping
    /// `scan`, if any. Conjuncts that reference only the search result schema are
    /// pushed into each executor's scored query as a `WHERE` clause (applied by
    /// the executor's own `SearchQueryProvider::scan`, before its internal
    /// top-N cutoff — the same "filter before limit" guarantee the
    /// non-distributed path already enforces); any remaining conjuncts are
    /// returned in [`RewrittenScan::remaining_filter`] for the caller to
    /// re-apply above the rewritten plan.
    fn rewrite_scan(
        &self,
        scan: &TableScan,
        filter_predicate: Option<&Expr>,
    ) -> Result<Option<RewrittenScan>> {
        let Some(default_source) = scan.source.downcast_ref::<DefaultTableSource>() else {
            return Ok(None);
        };
        let Some(search_provider) = default_source
            .table_provider
            .downcast_ref::<SearchQueryProvider>()
        else {
            return Ok(None);
        };
        // Clone the source so the fields are owned (edition-2024 match ergonomics
        // binds them by value here); they are small strings and options.
        let Some(UdtfSource::TextSearch {
            table,
            query,
            column,
            limit,
            ..
        }) = search_provider.udtf_source.clone()
        else {
            return Ok(None);
        };

        // Only distribute when the searched table is an accelerated (multi-node)
        // table. A non-accelerated search runs locally, unchanged.
        if !(self.should_distribute)(&search_provider.table_provider) {
            return Ok(None);
        }

        // Only a local Tantivy-backed index can score against externally
        // supplied collection statistics — a `text_search` over a compound or
        // Elasticsearch-backed index accepts `global_stats` but ignores it, so
        // distributing would silently merge incomparable local scores. Run
        // those backends through the existing (non-distributed-exact) scan.
        if !search_provider.supports_distributed_global_stats {
            return Ok(None);
        }

        let base_ref = TableReference::parse_str(&table);
        let executors = self.registry.resolve_search_executors(&base_ref);
        if executors.is_empty() {
            // No live executor covers the table's partitions; produce no rows
            // rather than silently scoring against an empty scheduler index.
            return Ok(Some(RewrittenScan {
                plan: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(&scan.projected_schema),
                }),
                remaining_filter: filter_predicate.cloned(),
            }));
        }

        let from_table_sql = base_ref.to_quoted_string();

        // The search result schema the executors return and the operator merges,
        // always including `_score` so the merge can rank by it even when the
        // caller projected it away (the outer projection drops it again).
        let merge_schema = merge_schema_with_score(&search_provider.schema());

        // Split the surrounding filter (if any) into conjuncts that reference
        // only the search result schema — pushed into each executor's scored
        // query as a `WHERE` clause — and everything else, left for the caller
        // to re-apply above the rewritten plan. The statistics query is never
        // filtered: BM25 collection statistics are whole-partition, exactly
        // like the non-distributed Tantivy path, whose statistics are unaffected
        // by any `WHERE` predicate.
        let (filter_sql, remaining_filter) = match filter_predicate {
            Some(predicate) => split_pushable_filter(predicate, &merge_schema),
            None => (None, None),
        };

        let stats_plan = Self::build_stats_plan(
            &base_ref,
            &from_table_sql,
            &query,
            column.as_deref(),
            &executors,
        )?;

        let params = DistributedSearchParams {
            from_table_sql,
            query,
            column,
            primary_key: search_provider.primary_key.clone(),
            fetch: limit,
            skip: 0,
            filter_sql,
        };

        let node = DistributedSearchNode::new(
            stats_plan,
            Arc::new(DFSchema::try_from(merge_schema.as_ref().clone())?),
            params,
            executors
                .into_iter()
                .map(|(id, client)| DistributedExecutor { id, client })
                .collect(),
        );

        // Project back to the scan's advertised schema (drops `_score` when the
        // caller passed `include_score => false`). Identity when the schemas match.
        let plan = LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(
            scan.projected_schema
                .fields()
                .iter()
                .map(|f| col(f.name().clone())),
        )?
        .build()?;

        Ok(Some(RewrittenScan {
            plan,
            remaining_filter,
        }))
    }

    /// Build the global-statistics aggregation: a `SUM ... GROUP BY term` over a
    /// union of one `text_search_stats(...)` Flight SQL leg per executor.
    fn build_stats_plan(
        base_ref: &TableReference,
        from_table_sql: &str,
        query: &str,
        column: Option<&str>,
        executors: &[(String, FlightSqlClient)],
    ) -> Result<LogicalPlan> {
        let stats_schema = GlobalBm25Stats::stats_schema();
        let from_function = stats_from_function(from_table_sql, query, column);

        let mut legs = executors.iter().map(|(executor_id, client)| {
            stats_leg(
                executor_id,
                client.clone(),
                base_ref,
                Arc::clone(&stats_schema),
                from_function.clone(),
            )
        });

        let Some(first) = legs.next().transpose()? else {
            return Err(DataFusionError::Internal(
                "distributed search: no executor legs to gather statistics".to_string(),
            ));
        };
        let mut builder = LogicalPlanBuilder::new(first);
        for leg in legs {
            builder = builder.union(leg?)?;
        }

        builder
            .aggregate(
                vec![col(STATS_TERM_COLUMN)],
                vec![
                    sum(col(STATS_DOC_FREQ_COLUMN)).alias(STATS_DOC_FREQ_COLUMN),
                    sum(col(STATS_TOTAL_NUM_DOCS_COLUMN)).alias(STATS_TOTAL_NUM_DOCS_COLUMN),
                    sum(col(STATS_TOTAL_NUM_TOKENS_COLUMN)).alias(STATS_TOTAL_NUM_TOKENS_COLUMN),
                ],
            )?
            .build()
    }
}

impl AnalyzerRule for DistributedSearchRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down(|plan| {
            // A `Filter` directly wrapping the target scan carries predicates
            // that would otherwise be applied only after the distributed merge
            // (after each executor's local top-N cutoff), silently dropping rows
            // that belong in the filtered top-N. Match this shape so pushable
            // conjuncts reach each executor's query before its cutoff.
            if let LogicalPlan::Filter(filter) = &plan
                && let LogicalPlan::TableScan(scan) = filter.input.as_ref()
            {
                return match self.rewrite_scan(scan, Some(&filter.predicate))? {
                    Some(rewritten) => Ok(Transformed::new(
                        rewrap_remaining_filter(rewritten)?,
                        true,
                        TreeNodeRecursion::Jump,
                    )),
                    None => Ok(Transformed::no(plan)),
                };
            }
            let LogicalPlan::TableScan(scan) = &plan else {
                return Ok(Transformed::no(plan));
            };
            match self.rewrite_scan(scan, None)? {
                // Jump so the injected Flight SQL / aggregate subtree is not
                // re-examined by this rule (federation handles it afterward).
                Some(rewritten) => Ok(Transformed::new(
                    rewrap_remaining_filter(rewritten)?,
                    true,
                    TreeNodeRecursion::Jump,
                )),
                None => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "DistributedSearchRewrite"
    }
}

/// The result of rewriting one distributable `text_search` scan: the replacement
/// plan, plus any filter conjuncts that could not be pushed into the executors'
/// queries and must still be applied above it.
struct RewrittenScan {
    plan: LogicalPlan,
    remaining_filter: Option<Expr>,
}

/// Re-wrap `rewritten.plan` in a `Filter` over its unpushed conjuncts, if any.
fn rewrap_remaining_filter(rewritten: RewrittenScan) -> Result<LogicalPlan> {
    match rewritten.remaining_filter {
        Some(predicate) => LogicalPlanBuilder::new(rewritten.plan)
            .filter(predicate)?
            .build(),
        None => Ok(rewritten.plan),
    }
}

/// Split `predicate` into the conjuncts that reference only `schema`'s columns
/// (and can be unparsed to SQL) — safe to push into an executor's scored query —
/// and everything else, re-combined into a single remaining predicate.
fn split_pushable_filter(predicate: &Expr, schema: &SchemaRef) -> (Option<String>, Option<Expr>) {
    let unparser = Unparser::new(&PostgreSqlDialect {});
    let mut pushed_sql: Vec<String> = Vec::new();
    let mut remaining: Vec<Expr> = Vec::new();

    for conjunct in split_conjunction_owned(predicate.clone()) {
        let references_known_columns = conjunct
            .column_refs()
            .iter()
            .all(|c| schema.column_with_name(c.name()).is_some());

        if references_known_columns {
            if let Ok(ast) = unparser.expr_to_sql(&conjunct) {
                pushed_sql.push(ast.to_string());
                continue;
            }
            // Cannot render this conjunct as SQL; leave it for the caller to
            // apply above the merge instead of dropping it.
        }
        remaining.push(conjunct);
    }

    let filter_sql = (!pushed_sql.is_empty()).then(|| pushed_sql.join(" AND "));
    (filter_sql, conjunction(remaining))
}

/// One executor's statistics leg: a Flight SQL scan whose FROM source is a
/// `text_search_stats(...)` UDTF call, returning that executor's local statistics.
fn stats_leg(
    executor_id: &str,
    client: FlightSqlClient,
    base_ref: &TableReference,
    stats_schema: SchemaRef,
    from_function: String,
) -> Result<LogicalPlan> {
    let provider = Arc::new(
        FlightSQLTable::create_with_schema(
            "flightsql",
            executor_id,
            client,
            base_ref.clone(),
            stats_schema,
            Arc::new(CookieStore::new()),
        )
        .with_from_function(from_function),
    );
    LogicalPlanBuilder::scan(
        base_ref.clone(),
        Arc::new(DefaultTableSource::new(provider)),
        None,
    )?
    .build()
}

/// The `text_search_stats(<table>, '<query>'[, "<column>"])` FROM-clause SQL.
fn stats_from_function(from_table_sql: &str, query: &str, column: Option<&str>) -> String {
    let mut args = format!("{from_table_sql}, {}", sql_string_literal(query));
    if let Some(column) = column {
        let _ = write!(args, ", {}", quote_identifier(column));
    }
    format!("text_search_stats({args})")
}

/// The search result schema plus a `_score` column when absent, so the operator
/// can always rank by score before the outer projection trims columns.
fn merge_schema_with_score(schema: &SchemaRef) -> SchemaRef {
    if schema.column_with_name(SEARCH_SCORE_COLUMN_NAME).is_some() {
        return Arc::clone(schema);
    }
    let mut fields: Vec<Arc<Field>> = schema.fields().iter().map(Arc::clone).collect();
    fields.push(Arc::new(Field::new(
        SEARCH_SCORE_COLUMN_NAME,
        DataType::Float64,
        false,
    )));
    Arc::new(Schema::new(fields))
}

/// Render `s` as a single-quoted SQL string literal, doubling embedded quotes.
fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Render `name` as a double-quoted SQL identifier, doubling embedded quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_from_function_renders_table_query_and_column() {
        let sql = stats_from_function("\"spice\".\"public\".\"docs\"", "hello world", Some("body"));
        assert_eq!(
            sql,
            "text_search_stats(\"spice\".\"public\".\"docs\", 'hello world', \"body\")"
        );
    }

    #[test]
    fn stats_from_function_omits_absent_column_and_escapes_quotes() {
        let sql = stats_from_function("\"docs\"", "it's a test", None);
        assert_eq!(sql, "text_search_stats(\"docs\", 'it''s a test')");
    }

    #[test]
    fn merge_schema_appends_score_when_missing() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let merged = merge_schema_with_score(&schema);
        assert!(merged.column_with_name(SEARCH_SCORE_COLUMN_NAME).is_some());
        assert_eq!(merged.fields().len(), 2);
    }

    #[test]
    fn merge_schema_keeps_existing_score() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(SEARCH_SCORE_COLUMN_NAME, DataType::Float64, false),
        ]));
        let merged = merge_schema_with_score(&schema);
        assert_eq!(merged.fields().len(), 2);
    }
}
