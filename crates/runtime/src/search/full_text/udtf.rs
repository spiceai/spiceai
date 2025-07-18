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

use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, Weak},
};

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
use runtime_datafusion_index::IndexedTableProvider;
use search::{
    SEARCH_SCORE_COLUMN_NAME,
    generation::text_search::{
        DEFAULT_BATCH_SIZE, FullTextSearchFieldIndex, exec::FullTextSearchExec,
        tantivy_to_arrow_type,
    },
};

use crate::{
    datafusion::DataFusion,
    search::{
        full_text::index::FullTextDatabaseIndex,
        util::{find_concrete_table_provider, table_ref_from_column_expr},
    },
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

        Ok(Arc::new(TextSearchUDTFProvider {
            args,
            index: fts_index.clone(),
            underlying: index_table_provider.get_underlying(),
        }))
    }
}

/// The [`TableProvider`] produced from the [`TEXT_SEARCH_UDTF_NAME`] UDTF.
///
/// Importantly, [`TextSearchUDTFProvider`] relies on [`FullTextUDTFAnalyzerRule`] because, by itself, [`TextSearchUDTFProvider`] does not have all the fields it claims to in its schema (see [`TextSearchUDTFProvider::schema`]).
#[derive(Debug, Clone)]
pub(super) struct TextSearchUDTFProvider {
    pub args: TextSearchTableFuncArgs,
    pub index: FullTextDatabaseIndex,
    underlying: Arc<dyn TableProvider>,
}

impl TextSearchUDTFProvider {
    // Find column to perform full text search upon. Use either column specified in
    // [`TextSearchTableFuncArgs`] or if index has one column.
    fn column(&self) -> datafusion::error::Result<String> {
        let TextSearchTableFuncArgs { column, tbl, .. } = &self.args;
        let col: String = if let Some(col) = column {
            if !self.index.search_fields.contains(col) {
                return Err(DataFusionError::Internal(format!(
                    "User function 'text_search' is called on table '{tbl}' that does not have a full text search index on '{col}' column. Index is on column(s): {}.",
                    self.index.search_fields.join(", ")
                )));
            }
            col.clone()
        } else {
            let mut fields = self.index.search_fields.iter();

            match (fields.next(), fields.next()) {
                (Some(field), None) => field.clone(),
                (Some(_), Some(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "User function 'text_search' is called on table '{tbl}' that has {} full text search columns. Must call 'text_search' with column parameter, e.g. `text_search(\"my table\", 'my query', my_search_col)`.",
                        self.index.search_fields.len()
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
        &self,
        projection: Option<&Vec<usize>>,
        search_index_schema: &SchemaRef,
    ) -> Result<Vec<usize>, DataFusionError> {
        let proj = match projection {
            Some(proj) => {
                let fields: Vec<_> = self
                    .schema()
                    .project(proj)
                    .map_err(DataFusionError::from)?
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect();

                // Need to preserve order of projection.
                // Map name of fields above, in order, to the indices within the search index.
                let index_fields: HashMap<String, usize> = search_index_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (f.name().clone(), i))
                    .collect();

                fields
                    .iter()
                    .filter_map(|f| index_fields.get(f).copied())
                    .collect::<Vec<usize>>()
            }
            None => (0..search_index_schema.fields().len()).collect(),
        };
        Ok(proj)
    }

    /// Return the indices of the primary key in the schema.
    pub fn primary_key_projection(&self) -> Vec<usize> {
        let schema = self.schema();

        self.index
            .primary_key
            .iter()
            .filter_map(|pk| {
                let (idx, _) = schema.column_with_name(pk)?;
                Some(idx)
            })
            .collect()
    }

    fn search_field_index_schema(field_index: &FullTextSearchFieldIndex) -> SchemaRef {
        let tantivy_schema = &field_index.search_schema;

        let fields = field_index
            .all_columns()
            .iter()
            .filter_map(|field_name| {
                let (data_type, nullable) = if let Some(f) = field_index.get_type_hint(field_name) {
                    (f.data_type().clone(), f.is_nullable())
                } else {
                    let f = tantivy_schema.get_field(field_name).ok()?;
                    let entry = tantivy_schema.get_field_entry(f);
                    (tantivy_to_arrow_type(entry.field_type())?, false)
                };
                Some(Field::new(field_name, data_type, nullable))
            })
            .chain([Field::new(
                SEARCH_SCORE_COLUMN_NAME,
                arrow::datatypes::DataType::Float64,
                false,
            )])
            .collect::<Vec<_>>();

        Arc::new(Schema::new(fields))
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
        let mut fields: Vec<_> = self.underlying.schema().fields().iter().cloned().collect();
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
        _state: &dyn Session,
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

        let col = self.column()?;

        let Some(field_index) = self
            .index
            .full_text_search_field_index(col.as_str())
            .await
            .ok()
        else {
            // This shouldn't be reachable as we checked `col` above. Instead of `unreachable!`, provide user friendly error.
            return Err(DataFusionError::Internal(format!(
                "User function 'text_search' is called on table '{tbl}'. Unexpectedly, text search cannot be performed on '{col}' column. Report an issue on GitHub: https://github.com/spiceai/spiceai/issues."
            )));
        };

        let search_field_index_schema = Self::search_field_index_schema(&field_index);
        let underlying_projection =
            self.convert_projection(projection, &search_field_index_schema)?;

        Ok(Arc::new(
            FullTextSearchExec::try_new(
                field_index,
                query.clone(),
                search_field_index_schema,
                Some(&underlying_projection),
                filters.to_vec(),
                limit.or(*args_limit).unwrap_or(DEFAULT_BATCH_SIZE),
            )
            .map_err(|e| DataFusionError::ArrowError(e, None))?,
        ))
    }
}
