use std::sync::Arc;

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
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};

use crate::datafusion::DataFusion;

// fn text_search(tbl: TableReference, query: &str, col: Option<str>, limit: Option<usize>, include_score: Option<bool>)
// ```
// - tbl: Table to perform full text search upon. If the table does not support it (i.e. no index), and empty table is returned.
// - query: Query to perform full text search against.
// - col: If provided, use this column to compare vector search results against.
// - limit:
// - include_score (default true): If false, do not return `score` in the table projection.

// The schema of the resultant table will be: `schema(tbl) ∪ {score}`, where:
//  - `score` (f32): The similarity score of the row with the request `query`.
#[derive(Debug)]
pub struct TextSearchTableFuncArgs {
    tbl: TableReference,
    query: String,
    column: Option<String>,
    limit: Option<usize>,
    include_score: Option<bool>,
}

#[derive(Debug)]
pub struct TextSearchTableFunc {
    df: Arc<DataFusion>,
}

impl TextSearchTableFunc {
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }
}

impl TextSearchTableFunc {
    fn parse_args(args: &[Expr]) -> DataFusionResult<TextSearchTableFuncArgs> {
        let mut args = args.iter();

        // TODO: Check if table will be parsed as column expr.
        let tbl = args.next();
        let Some(Expr::Column(Column {
            relation: None,
            name: table_name,
            ..
        })) = tbl
        else {
            return DataFusionError::Plan(format!(
                "First argument must be a table reference, but got a different expression: {tbl:?}."
            ));
        };

        let query = args.next();
        let Some(Expr::Literal(ScalarValue::Utf8(Some(q)))) = query else {
            return DataFusionError::Plan(format!(
                "Second argument must be a query string, but got {query:?}."
            ));
        };

        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments, provides defaults
            (None, None, None) => (None, None, Some(true)),

            // Single argument cases
            (Some(Expr::Literal(ScalarValue::Utf8(Some(col)))), None, None) => {
                (Some(col), None, Some(true))
            }
            (Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))), None, None) => {
                (None, Some(*limit as usize), Some(true))
            }
            (Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))), None, None) => {
                (None, None, Some(include_score))
            }

            // 2 of 3 arguments. When user provides two of three arguments, they must still be in correct order (i.e. no limit before column)
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                None,
            ) => (Some(col), Some(*limit as usize), Some(true)),
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (Some(col), None, Some(include_score)),
            (
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (None, Some(*limit as usize), Some(include_score)),

            // All three arguments provided
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
            ) => (Some(col), Some(*limit as usize), Some(include_score)),

            // Invalid argument combinations
            (a, b, c) => {
                return Err(DataFusionError::Plan(format!(
                    "Invalid arguments: ({table_name}, {q}, {a:?}, {b:?}, {c:?}. Expected (table, query, [column, limit, include_score])."
                )));
            }
        };
        Ok(TextSearchTableFuncArgs {
            tbl: table_name.into(),
            query: q.to_string(),
            column,
            limit,
            include_score,
        })
    }
}

impl TableFunctionImpl for TextSearchTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = Self::parse_args(args)?;

        if !self.df.table_exists(args.tbl.clone()) {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not exist.",
                args.tbl
            )));
        };

        // Perform the full text search and return the results.
        let results = tbl.full_text_search(
            &args.query,
            args.column.as_deref(),
            args.limit,
            args.include_score.unwrap_or(true),
        )?;

        Ok(results)
    }
}
