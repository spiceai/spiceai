use std::{any::Any, sync::Arc};

use arrow_schema::{Field, Schema, SchemaRef};
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
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::ExecutionPlan,
    prelude::Expr,
    scalar::ScalarValue,
    sql::TableReference,
};
use search::{SEARCH_SCORE_COLUMN_NAME, generation::text_search::FullTextSearchTable};

use crate::{
    datafusion::DataFusion,
    search::{full_text::table::TableWithFullText, util::find_concrete_table_provider},
};

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

    // For now: force user to specify
    primary_key: String,
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
            return Err(DataFusionError::Plan(format!(
                "First argument must be a table reference, but got a different expression: {tbl:?}."
            )));
        };

        let query = args.next();
        let Some(Expr::Literal(ScalarValue::Utf8(Some(q)))) = query else {
            return Err(DataFusionError::Plan(format!(
                "Second argument must be a query string, but got {query:?}."
            )));
        };

        let pk = args.next();
        let Some(Expr::Column(Column {
            relation: None,
            name: pk,
            ..
        })) = pk
        else {
            return Err(DataFusionError::Plan(format!(
                "Third argument (for now) must be the primary key, but got a different expression: {tbl:?}."
            )));
        };

        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments, provides defaults
            (None, None, None) => (None, None, Some(true)),

            // Single argument cases
            (Some(Expr::Literal(ScalarValue::Utf8(Some(col)))), None, None) => {
                (Some(col.clone()), None, Some(true))
            }
            (Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))), None, None) => {
                (None, Some(*limit as usize), Some(true))
            }
            (Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))), None, None) => {
                (None, None, Some(*include_score))
            }

            // 2 of 3 arguments. When user provides two of three arguments, they must still be in correct order (i.e. no limit before column)
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                None,
            ) => (Some(col.clone()), Some(*limit as usize), Some(true)),
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (Some(col.clone()), None, Some(*include_score)),
            (
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
                None,
            ) => (None, Some(*limit as usize), Some(*include_score)),

            // All three arguments provided
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
            ) => (
                Some(col.clone()),
                Some(*limit as usize),
                Some(*include_score),
            ),

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
            primary_key: pk.clone(),
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
                args.tbl.clone()
            )));
        };

        Ok(Arc::new(TextSearchTableProvider {
            df: self.df.clone(),
            args,
        }))
    }
}

#[derive(Debug)]
struct TextSearchTableProvider {
    df: Arc<DataFusion>,
    args: TextSearchTableFuncArgs,
}

#[async_trait::async_trait]
impl TableProvider for TextSearchTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // This is a simplification for now.
        Arc::new(Schema::new(vec![
            Field::new(
                self.args.primary_key.clone(),
                arrow_schema::DataType::Utf8,
                false,
            ),
            Field::new(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                arrow_schema::DataType::Float64,
                false,
            ),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let TextSearchTableFuncArgs {
            tbl,
            query,
            primary_key,
            column,
            limit: args_limit,
            include_score,
        } = &self.args;
        let Some(table_provider) = self.df.get_table(&tbl).await else {
            return Err(DataFusionError::Internal(format!(
                "TODO, need to return empty exec instead"
            )));
        };

        let Some(fts) = find_concrete_table_provider::<TableWithFullText>(&table_provider).await
        else {
            return Err(DataFusionError::Internal(format!(
                "TODO, need to return empty exec instead"
            )));
        };
        let col: String = if let Some(col) = column {
            if !fts.search_fields.contains(col) {
                return Err(DataFusionError::Internal(format!(
                    "TODO, need to return empty exec instead"
                )));
            };
            col.clone()
        } else {
            let mut fields = fts.search_fields.iter();
            let z = match (fields.next(), fields.next()) {
                (Some(field), None) => field.clone(),
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "TODO, need to return empty exec instead"
                    )));
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "TODO, need to return empty exec instead"
                    )));
                }
            };
            z
        };
        let Some(index) = fts.index_as_full_text(col.as_str()).ok() else {
            return Err(DataFusionError::Internal(format!(
                "TODO, need to return empty exec instead"
            )));
        };

        let tbl = FullTextSearchTable::new(index, query.clone()).minimal_schema();
        tbl.scan(state, projection, filters, limit.or(*args_limit))
            .await
    }
}
