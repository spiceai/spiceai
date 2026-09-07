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

//! A user-defined table function (UDTF) that gathers local BM25 collection
//! statistics for a full-text search query on one partition of a distributed
//! accelerated table.
//!
//! `text_search_stats(tbl: TableReference, query: &str, column: Option<str>)`
//!
//! The output has one row per analyzed query term:
//!  - `term` (Utf8): the tokenized and stemmed query term.
//!  - `doc_freq` (UInt64): the number of documents on this partition that
//!    contain the term.
//!  - `total_num_docs` (UInt64): the partition's document count `N`.
//!  - `total_num_tokens` (UInt64): the partition's total token count in the
//!    search field.
//!
//! The scheduler runs this UDTF on every executor (over the intra-cluster Flight
//! SQL channel), then sums the rows with `SUM ... GROUP BY term` into the global
//! statistics used to score. See [`crate::full_text_udtf`] for the scored query.

use std::sync::{Arc, Weak};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::TableReference;

use search::generation::text_search::index::FullTextDatabaseIndex;

use crate::full_text_udtf::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::table_provider_explorer::TableProviderExplorer;
use crate::udtf::table_ref_from_column_expr;
use runtime_query_engine::query_engine::QueryEngine;

/// SQL name of the statistics-gathering UDTF.
pub static TEXT_SEARCH_STATS_UDTF_NAME: &str = "text_search_stats";

/// Parsed arguments of a `text_search_stats(...)` call.
#[derive(Debug, Clone, PartialEq)]
struct StatsArgs {
    tbl: TableReference,
    query: String,
    column: Option<String>,
}

#[derive(Debug)]
pub struct TextSearchStatsTableFunc<E: TableProviderExplorer> {
    df: Weak<dyn QueryEngine>,
    explorer: E,
}

impl<E: TableProviderExplorer> TextSearchStatsTableFunc<E> {
    #[must_use]
    pub fn new(df: Weak<dyn QueryEngine>, explorer: E) -> Self {
        Self { df, explorer }
    }

    fn parse_args(args: &[Expr]) -> DataFusionResult<StatsArgs> {
        let mut args = args.iter();

        let Some(Expr::Column(c)) = args.next() else {
            return Err(DataFusionError::Plan(
                "text_search_stats: first argument must be a table reference.".to_string(),
            ));
        };
        let tbl = table_ref_from_column_expr(c)
            .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
            .into();

        let Some(Expr::Literal(ScalarValue::Utf8(Some(query)), _)) = args.next() else {
            return Err(DataFusionError::Plan(
                "text_search_stats: second argument must be a query string.".to_string(),
            ));
        };

        let column = match args.next() {
            None => None,
            Some(
                Expr::Column(Column { name, .. }) | Expr::Literal(ScalarValue::Utf8(Some(name)), _),
            ) => Some(name.clone()),
            Some(other) => {
                return Err(DataFusionError::Plan(format!(
                    "text_search_stats: third argument must be a column name, but got {other:?}."
                )));
            }
        };

        Ok(StatsArgs {
            tbl,
            query: query.clone(),
            column,
        })
    }
}

impl<E: TableProviderExplorer + 'static> TableFunctionImpl for TextSearchStatsTableFunc<E> {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = Self::parse_args(args)?;

        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan(
                "text_search_stats: DataFusion instance has been dropped.".to_string(),
            )
        })?;

        let Some(table_provider) = df.get_table_sync(&args.tbl) else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not exist.",
                args.tbl
            )));
        };

        // Distributed full-text search is over local Tantivy indexes on
        // accelerated partitions, so this UDTF resolves only that index type.
        let (fts_indexes, _) = self
            .explorer
            .find_index::<FullTextDatabaseIndex>(&table_provider)
            .filter(|(indexes, _)| !indexes.is_empty())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Table '{}' does not have a full text search index.",
                    args.tbl
                ))
            })?;

        // Choose the index carrying the requested column, or the sole index.
        let fts_index: &FullTextDatabaseIndex = if let Some(ref requested) = args.column {
            fts_indexes
                .iter()
                .copied()
                .find(|idx| idx.search_fields.contains(requested))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Table '{}' does not have a full text search index on '{requested}'.",
                        args.tbl
                    ))
                })?
        } else if fts_indexes.len() == 1 {
            fts_indexes[0]
        } else {
            return Err(DataFusionError::Plan(format!(
                "text_search_stats on table '{}' needs a column argument: it has {} full text search columns.",
                args.tbl,
                fts_indexes.len()
            )));
        };

        let column = match args.column.as_ref() {
            Some(c) => c.clone(),
            None => fts_index.search_fields.first().cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Table '{}' has no full text search column.",
                    args.tbl
                ))
            })?,
        };

        let field_index = fts_index
            .full_text_search_field_index(&column)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // The local index read (per-term document frequency plus the collection
        // totals) is a small in-memory lookup, so it runs at planning time here,
        // mirroring how `text_search` opens its searcher during planning.
        let stats = field_index
            .local_bm25_stats(&args.query)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let batch = stats
            .to_record_batch(field_index.generation_id())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let schema: SchemaRef = batch.schema();
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}
