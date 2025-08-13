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

//! A user-defined table function (UDTF) for performing full text search on a preexisting table that has an associated [`crate::datafusion::indexes::full_text::FullTextIndex`] in [`DataFusion`].
//!
//! `text_search(tbl`: `TableReference`, query: &str, col: Option<str>, limit: Option<usize>, `include_score`: Option<bool>)
//!
//! - tbl: Table to perform full text search upon. If the table does not support it (i.e. no index), and empty table is returned.
//! - query: Query to perform full text search against.
//! - col: If provided, use this column to compare vector search results against.
//! - limit:
//! - `include_score` (default true): If false, do not return `score` in the table projection.
//!
//! The schema of the resultant table will be: `schema(tbl) ∪ {score}`, where:
//!  - `score` (f32): The similarity score of the row with the request `query`.

use std::sync::{Arc, Weak};

use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use futures::FutureExt;
use runtime_datafusion_index::IndexedTableProvider;
use search::generation::text_search::{
    index::FullTextDatabaseIndex, udtf::TextSearchIndexProvider,
};

use crate::{
    datafusion::DataFusion,
    request::{AsyncMarker, RequestContext},
    search::util::{find_concrete_table_provider, table_ref_from_column_expr},
};

pub static TEXT_SEARCH_UDTF_NAME: &str = "text_search";

#[derive(Debug, PartialEq, Clone)]
pub struct TextSearchTableFuncArgs {
    pub tbl: TableReference,
    pub query: String,

    pub column: Option<String>,
    pub limit: Option<usize>,
    pub include_score: Option<bool>,
}

impl TextSearchTableFuncArgs {
    // Find column to perform full text search upon. Use either column specified in
    // [`TextSearchTableFuncArgs`] or if there is only one column in `search_fields`.
    fn column(&self, search_fields: &[String]) -> datafusion::error::Result<String> {
        let TextSearchTableFuncArgs { column, tbl, .. } = &self;
        let col: String = if let Some(col) = column {
            if !search_fields.contains(col) {
                return Err(DataFusionError::Internal(format!(
                    "User function 'text_search' is called on table '{tbl}' that does not have a full text search index on '{col}' column. Index is on column(s): {}.",
                    search_fields.join(", ")
                )));
            }
            col.clone()
        } else {
            let mut fields = search_fields.iter();

            match (fields.next(), fields.next()) {
                (Some(field), None) => field.clone(),
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "User function 'text_search' is called on table '{tbl}' that has {} full text search columns. Must call 'text_search' with column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`.",
                        search_fields.len()
                    )));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "User function 'text_search' is called on table '{tbl}' that has no associated full text search index."
                    )));
                }
            }
        };
        Ok(col)
    }
}

#[derive(Debug)]
pub struct TextSearchTableFunc {
    // This needs to be a weak reference because the DataFusion instance contains the SessionContext which contains this UDTF.
    df: Weak<DataFusion>,
}

impl TextSearchTableFunc {
    #[must_use]
    pub fn new(df: Weak<DataFusion>) -> Self {
        Self { df }
    }
}

impl TextSearchTableFunc {
    fn parse_args(args: &[Expr]) -> DataFusionResult<TextSearchTableFuncArgs> {
        let mut args = args.iter();

        let tbl = args.next();
        let Some(Expr::Column(c)) = tbl else {
            return Err(DataFusionError::Plan(format!(
                "First argument must be a table reference, but got a different expression: {tbl:?}."
            )));
        };
        let tbl_ref = table_ref_from_column_expr(c);

        let query = args.next();
        let Some(Expr::Literal(ScalarValue::Utf8(Some(q)))) = query else {
            return Err(DataFusionError::Plan(format!(
                "Second argument must be a query string, but got {query:?}."
            )));
        };

        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments, provides defaults
            (None, None, None) => (None, None, Some(true)),

            // Single argument cases
            (Some(Expr::Column(Column { name: col, .. })), None, None) => {
                (Some(col.clone()), None, Some(true))
            }
            (Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))), None, None) => {
                (None, Some(*limit), Some(true))
            }
            (Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))), None, None) => {
                (None, None, Some(*include_score))
            }

            // 2 of 3 arguments. When user provides two of three arguments, they must still be in correct order (i.e. no limit before column)
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                None,
            ) => (Some(col.clone()), Some(*limit), Some(true)),
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (Some(col.clone()), None, Some(*include_score)),
            (
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (None, Some(*limit), Some(*include_score)),

            // All three arguments provided
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
            ) => (Some(col.clone()), Some(*limit), Some(*include_score)),

            // Invalid argument combinations
            (a, b, c) => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid arguments: ({tbl_ref:?}, {q}, {a:?}, {b:?}, {c:?}. Expected (table, query, [column, limit, include_score])."
                )));
            }
        };
        Ok(TextSearchTableFuncArgs {
            tbl: tbl_ref,
            query: q.to_string(),
            column,
            limit: limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            include_score,
        })
    }
}

impl TableFunctionImpl for TextSearchTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        async {
            let request_context = RequestContext::current(AsyncMarker::new().await);
            telemetry::track_text_search(&request_context.to_dimensions());
        }
        .now_or_never();

        let args = Self::parse_args(args)?;

        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan("An unexpected error occurred when calling text_search(). Report an issue on GitHub: https://github.com/spiceai/spiceai/issues.\nDetails: DataFusion instance has been dropped.".to_string())
        })?;

        let Some(table_provider) = df.get_table_sync(&args.tbl) else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not exist.",
                args.tbl.clone()
            )));
        };

        let index_table_provider = find_concrete_table_provider::<IndexedTableProvider>(
            &table_provider,
        )
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Table '{}' does not have a full text search index.",
                args.tbl.clone()
            ))
        })?;

        let Some(fts_index) = index_table_provider.get_index::<FullTextDatabaseIndex>() else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not have a full text search index.",
                args.tbl.clone()
            )));
        };

        let column = args.column(&fts_index.search_fields)?;
        Ok(Arc::new(TextSearchIndexProvider {
            query: args.query.clone(),
            column,
            pre_limit: args.limit,
            index: fts_index.clone(),
            underlying: table_provider,
        }))
    }
}
