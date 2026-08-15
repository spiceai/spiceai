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
//! [`DistributedSearchExec`](super::distributed_search::DistributedSearchExec)
//! drains that child, then scores each executor's partition with the global
//! statistics and merges the comparable results.

use std::sync::Arc;

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
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, TableScan, col},
    optimizer::AnalyzerRule,
    sql::TableReference,
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

use crate::cluster::accelerated_partition_provider::is_accelerated_table_provider;
use crate::cluster::datafusion::distributed_search::{
    DistributedExecutor, DistributedSearchNode, DistributedSearchParams,
};

/// Rewrites `text_search` scans over multi-node accelerated tables into a
/// distributed-search plan. Registered on the scheduler only (it needs the
/// executor registry).
#[derive(Debug)]
pub struct DistributedSearchRewrite {
    registry: Arc<ExecutorRegistry>,
}

impl DistributedSearchRewrite {
    #[must_use]
    pub fn new(registry: Arc<ExecutorRegistry>) -> Self {
        Self { registry }
    }

    /// The distributed rewrite of one `text_search` table scan, or `None` when
    /// the scan is not a distributable search (not a `SearchQueryProvider`, not
    /// a text search, or not over an accelerated table).
    fn rewrite_scan(&self, scan: &TableScan) -> Result<Option<LogicalPlan>> {
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
        if !is_accelerated_table_provider(&search_provider.table_provider) {
            return Ok(None);
        }

        let base_ref = TableReference::parse_str(&table);
        let executors = self.registry.resolve_search_executors(&base_ref);
        if executors.is_empty() {
            // No live executor covers the table's partitions; produce no rows
            // rather than silently scoring against an empty scheduler index.
            return Ok(Some(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(&scan.projected_schema),
                },
            )));
        }

        let from_table_sql = base_ref.to_quoted_string();

        // The search result schema the executors return and the operator merges,
        // always including `_score` so the merge can rank by it even when the
        // caller projected it away (the outer projection drops it again).
        let merge_schema = merge_schema_with_score(&search_provider.schema());

        let stats_plan = self.build_stats_plan(
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
        let projected = LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(
            scan.projected_schema
                .fields()
                .iter()
                .map(|f| col(f.name().clone())),
        )?
        .build()?;

        Ok(Some(projected))
    }

    /// Build the global-statistics aggregation: a `SUM ... GROUP BY term` over a
    /// union of one `text_search_stats(...)` Flight SQL leg per executor.
    fn build_stats_plan(
        &self,
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
            let LogicalPlan::TableScan(scan) = &plan else {
                return Ok(Transformed::no(plan));
            };
            match self.rewrite_scan(scan)? {
                // Jump so the injected Flight SQL / aggregate subtree is not
                // re-examined by this rule (federation handles it afterward).
                Some(rewritten) => Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump)),
                None => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "DistributedSearchRewrite"
    }
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
        args.push_str(&format!(", {}", quote_identifier(column)));
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
