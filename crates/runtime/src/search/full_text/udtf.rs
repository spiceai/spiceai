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

use std::{any::Any, sync::Arc};

use arrow_schema::{Field, Schema, SchemaRef};
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
use search::{SEARCH_SCORE_COLUMN_NAME, generation::text_search::table::FullTextSearchTable};

use crate::{
    datafusion::{DataFusion, indexes::full_text::FullTextDatabaseIndex},
    search::{full_text::table::TableWithFullText, util::find_concrete_table_provider},
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

        let (column, limit, include_score) = match (args.next(), args.next(), args.next()) {
            // No arguments, provides defaults
            (None, None, None) => (None, None, Some(true)),

            // Single argument cases
            (Some(Expr::Literal(ScalarValue::Utf8(Some(col)))), None, None) => {
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
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                None,
            ) => (Some(col.clone()), Some(*limit), Some(true)),
            (
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
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
                Some(Expr::Literal(ScalarValue::Utf8(Some(col)))),
                Some(Expr::Literal(ScalarValue::UInt64(Some(limit)))),
                Some(Expr::Literal(ScalarValue::Boolean(Some(include_score)))),
            ) => (Some(col.clone()), Some(*limit), Some(*include_score)),

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
            limit: limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
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
        }

        let fts_index = match self.df.get_full_text_index(&args.tbl) {
            Ok(Some(fts_index)) => fts_index,
            Ok(None) => {
                return Err(DataFusionError::Plan(format!(
                    "UDTF {TEXT_SEARCH_UDTF_NAME} requires the table '{}' to have a full text search index, but it does not.",
                    args.tbl
                )));
            }
            Err(_) => {
                return Err(DataFusionError::Internal(
                    "An internal issue occurred retrieving the text search".into(),
                ));
            }
        };

        Ok(Arc::new(TextSearchUDTFProvider {
            df: Arc::clone(&self.df),
            args,
            index: fts_index,
        }))
    }
}

/// The [`TableProvider`] produced from the [`TEXT_SEARCH_UDTF_NAME`] UDTF.
///
/// Importantly, [`TextSearchUDTFProvider`] relies on [`FullTextUDTFAnalyzerRule`] because, by itself, [`TextSearchUDTFProvider`] does not have all the fields it claims to in its schema (see [`TextSearchUDTFProvider::schema`]).
#[derive(Debug, Clone)]
pub(super) struct TextSearchUDTFProvider {
    df: Arc<DataFusion>,
    pub args: TextSearchTableFuncArgs,
    pub index: FullTextDatabaseIndex,
}

impl TextSearchUDTFProvider {
    // Find column to perform full text search upon. Use either column specified in
    // [`TextSearchTableFuncArgs`] or if index has one column.
    fn column(&self, fts: &TableWithFullText) -> datafusion::error::Result<String> {
        let TextSearchTableFuncArgs { column, tbl, .. } = &self.args;
        let col: String = if let Some(col) = column {
            if !fts.search_fields.contains(col) {
                return Err(DataFusionError::Internal(format!(
                    "User function 'text_search' is called on table '{tbl}' that does not have a full text search index on '{col}' column. Index is on column(s): {}.",
                    fts.search_fields.join(", ")
                )));
            }
            col.clone()
        } else {
            let mut fields = fts.search_fields.iter();

            match (fields.next(), fields.next()) {
                (Some(field), None) => field.clone(),
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "User function 'text_search' is called on table '{tbl}' that has {} full text search columns. Must call 'text_search' with column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`.",
                        fts.search_fields.len()
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

    // Convert projection relative to [`TextSearchUDTFProvider`] (i.e. base schema + 'score'), to the schema of the underlying full text search index.
    fn convert_projection(
        projection: Option<&Vec<usize>>,
        search_index_schema: &SchemaRef,
    ) -> Result<Vec<usize>, DataFusionError> {
        let proj = match projection {
            Some(proj) => {
                let fields: Vec<_> = search_index_schema
                    .project(proj)
                    .map_err(DataFusionError::from)?
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();
                search_index_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| {
                        if fields.contains(f.name()) {
                            Some(i)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            }
            None => (0..search_index_schema.fields().len()).collect(),
        };
        Ok(proj)
    }
}

#[async_trait::async_trait]
impl TableProvider for TextSearchUDTFProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // The schema of [`TextSearchUDTFProvider`] is the underlying [`TableProvider`] (see `self.index.underlying`) augmented with the additional column [`SEARCH_SCORE_COLUMN_NAME`].
    //
    // **Note**: [`TextSearchUDTFProvider`] may not have all fields it claims to have in the schema because the underlying [`FullTextDatabaseIndex`] (in reality the [`search::generation::text_search::FullTextSearchIndex`]) will not have all fields.
    //
    // When used via [`TextSearchTableFunc`], [`TextSearchUDTFProvider`] relies on [`FullTextUDTFAnalyzerRule`] to resolve queries correctly (joining on the underlying table (see `self.args.tbl`)).
    fn schema(&self) -> SchemaRef {
        let mut fields: Vec<_> = self
            .index
            .underlying
            .schema()
            .fields()
            .iter()
            .cloned()
            .collect();
        fields.push(Arc::new(Field::new(
            SEARCH_SCORE_COLUMN_NAME.to_string(),
            arrow_schema::DataType::Float64,
            false,
        )));
        Arc::new(Schema::new(fields))
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
            limit: args_limit,
            ..
        } = &self.args;

        let Some(table_provider) = self.df.get_table(tbl).await else {
            return Err(DataFusionError::Internal(format!(
                "The table '{tbl}' does not exist"
            )));
        };

        let Some(fts) = find_concrete_table_provider::<TableWithFullText>(&table_provider).await
        else {
            return Err(DataFusionError::Internal(format!(
                "The table '{tbl}' does not have an associated full text search index."
            )));
        };

        let col = self.column(&fts)?;

        let Some(index) = fts.index_as_full_text(col.as_str()).ok() else {
            // This shouldn't be reachable as we checked `col` above. Instead of `unreachable!`, provide user friendly error.
            return Err(DataFusionError::Internal(format!(
                "User function 'text_search' is called on table '{tbl}'. Unexpectedly, text search cannot be performed on '{col}' column. Report an issue on GitHub: https://github.com/spiceai/spiceai/issues."
            )));
        };

        let search_index_table = FullTextSearchTable::new(index, query.clone());
        let underlying_projection =
            Self::convert_projection(projection, &search_index_table.schema())?;

        search_index_table
            .scan(
                state,
                Some(&underlying_projection),
                filters,
                limit.or(*args_limit),
            )
            .await
    }
}
