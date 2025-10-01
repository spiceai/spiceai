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

use arrow_schema::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl};

use moka::future::FutureExt;
use search::{
    generation::text_search::index::FullTextDatabaseIndex, index::SearchIndex,
    provider::SearchQueryProvider,
};
use std::any::Any;
use std::sync::LazyLock;
use std::sync::{Arc, Weak};

use crate::request::{AsyncMarker, RequestContext};
use crate::{
    datafusion::DataFusion,
    embeddings::udtf::parse_limit_scalar,
    search::util::{find_index_in_table_provider, table_ref_from_column_expr, to_column_expr},
};

pub static TEXT_SEARCH_UDTF_NAME: &str = "text_search";
pub static TEXT_SEARCH_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

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

    fn scalar_invocation_error<T>() -> Result<T, DataFusionError> {
        exec_err!("{TEXT_SEARCH_UDTF_NAME} does not support scalar invocation.")
    }
}

impl TextSearchTableFunc {
    pub(crate) fn to_expr(args: &TextSearchTableFuncArgs) -> Vec<Expr> {
        let mut expr = vec![
            Expr::Column(to_column_expr(&args.tbl)),
            Expr::Literal(ScalarValue::Utf8(Some(args.query.clone())), None),
        ];

        if let Some(col) = args.column.as_ref() {
            expr.push(Expr::Column(Column::new_unqualified(col)));
        }

        if let Some(limit) = args.limit {
            expr.push(Expr::Literal(
                ScalarValue::UInt64(Some(u64::try_from(limit).unwrap_or(u64::MAX))),
                None,
            ));
        }

        if let Some(include_score) = args.include_score {
            expr.push(Expr::Literal(
                ScalarValue::Boolean(Some(include_score)),
                None,
            ));
        }

        expr
    }

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
        let Some(Expr::Literal(ScalarValue::Utf8(Some(q)), None)) = query else {
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
            (Some(Expr::Literal(scalar, None)), None, None) => {
                if let ScalarValue::Boolean(Some(include_score)) = *scalar {
                    (None, None, Some(include_score))
                } else {
                    (None, Some(parse_limit_scalar(scalar)?), Some(true))
                }
            }

            // 2 of 3 arguments. When user provides two of three arguments, they must still be in correct order (i.e. no limit before column)
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(scalar, None)),
                None,
            ) => {
                if let ScalarValue::Boolean(Some(include_score)) = *scalar {
                    (Some(col.clone()), None, Some(include_score))
                } else {
                    (
                        Some(col.clone()),
                        Some(parse_limit_scalar(scalar)?),
                        Some(true),
                    )
                }
            }
            (
                Some(Expr::Literal(scalar, None)),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)), None)),
                None,
            ) => (
                None,
                Some(parse_limit_scalar(scalar)?),
                Some(*include_score),
            ),

            // All three arguments provided
            (
                Some(Expr::Column(Column { name: col, .. })),
                Some(Expr::Literal(scalar, None)),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)), None)),
            ) => (
                Some(col.clone()),
                Some(parse_limit_scalar(scalar)?),
                Some(*include_score),
            ),

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

        let mut fts_indexes =
            find_index_in_table_provider::<FullTextDatabaseIndex>(&table_provider)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Table '{}' does not have a full text search index.",
                        args.tbl.clone()
                    ))
                })?
                .0;

        let Some(fts_index) = fts_indexes.pop() else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not have a full text search index.",
                args.tbl.clone()
            )));
        };

        // Select single column if needed.
        let column = args.column(&fts_index.search_fields)?;
        let mut fts_index = fts_index.clone();
        fts_index.search_fields = vec![column];

        Ok(Arc::new(
            SearchQueryProvider::try_from_index(
                &(Arc::new(fts_index) as Arc<dyn SearchIndex>),
                table_provider,
                args.query.as_str(),
                args.limit,
            )?
            .call_on_scan(Arc::new(|| {
                async {
                    let request_context = RequestContext::current(AsyncMarker::new().await);
                    telemetry::track_text_search(&request_context.to_dimensions());
                }
                .boxed()
            })),
        ))
    }
}

impl ScalarUDFImpl for TextSearchTableFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        TEXT_SEARCH_UDTF_NAME
    }

    fn signature(&self) -> &Signature {
        &TEXT_SEARCH_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Self::scalar_invocation_error()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Self::scalar_invocation_error()
    }
}
