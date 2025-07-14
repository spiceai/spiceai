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
//! A user-defined table function (UDTF) for performing vector search on a preexisting table that has an embedding configured on at least one of its columns.
//!
//! `vector_search(tbl`: `TableReference`, query: &str, col: Option<str>, limit: Option<usize>, `include_score`: Option<bool>)
//!
//! - tbl: Table to perform full text search upon. If the table does not support it (i.e. no index), an empty table is returned.
//! - query: Query to perform full text search against.
//! - col: If provided, use this column to compare vector search results against.
//! - limit:
//! - `include_score` (default true): If false, do not return `score` in the table projection.
//!
//! The schema of the resultant table will be: `schema(tbl) ∪ {score}`, where:
//!  - `score` (f32): The similarity score of the row with the request `query`.
//!  - `value` (UTF8): The subset of the column most relevant. For non-chunked embedding columns, `value` is the entire value.
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
};

use arrow::{array::FixedSizeListArray, datatypes::Float32Type};
use arrow_schema::{Field, Schema, SchemaRef};
use async_openai::types::EmbeddingInput;
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::Column,
    datasource::{DefaultTableSource, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{
        BinaryExpr, LogicalPlan, Operator, Projection, Sort, SortExpr, TableScan,
        expr::{Alias, ScalarFunction},
    },
    physical_plan::ExecutionPlan,
    prelude::{Expr, lit},
    scalar::ScalarValue,
    sql::TableReference,
};
use itertools::Itertools;
use runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME;
use search::SEARCH_SCORE_COLUMN_NAME;
use snafu::ResultExt;

use crate::{
    datafusion::DataFusion,
    embedding_col,
    embeddings::table::{EmbeddingColumnConfig, EmbeddingTable},
    model::EmbeddingModelStore,
    search::util::find_concrete_table_provider,
};
use tokio::sync::RwLock;

pub static VECTOR_SEARCH_UDTF_NAME: &str = "vector_search";

#[derive(Debug, PartialEq, Clone)]
pub struct VectorSearchTableFuncArgs {
    pub tbl: TableReference,
    pub query: String,

    pub column: Option<String>,
    pub limit: Option<usize>,
    pub include_score: Option<bool>,
}

impl VectorSearchTableFuncArgs {
    /// Check [`Self::column`] is valid, attempt to pick a default, and retrieve the associated [`EmbeddingColumnConfig`].
    fn get_column_and_config(
        &self,
        embedded_columns: &HashMap<String, EmbeddingColumnConfig>,
    ) -> DataFusionResult<(String, EmbeddingColumnConfig)> {
        let cfg = self
            .column
            .as_ref()
            .and_then(|c| embedded_columns.get(c))
            .cloned();
        match (self.column.as_deref(), cfg) {
            (Some(col), Some(cfg)) => Ok((col.to_string(), cfg)),
            (Some(col), None) => Err(DataFusionError::Internal(format!(
                "User function 'vector_search' is called on table '{}' that does not have a embedding index on '{col}' column. Index is on column(s): {}.",
                self.tbl,
                embedded_columns
                    .keys()
                    .collect::<Vec<_>>()
                    .iter()
                    .join(", ")
            ))),
            (None, _) => {
                if embedded_columns.len() > 1 {
                    return Err(DataFusionError::Internal(format!(
                        "User function 'vector_search' is called on table '{}' that has {} vector search columns. Must call 'vector_search' with column parameter, e.g. `vector_search(\"my table\", 'my query', my_embedded_col)`.",
                        self.tbl,
                        embedded_columns.len()
                    )));
                }
                if let Some((col, cfg)) = embedded_columns.iter().next() {
                    Ok((col.clone(), cfg.clone()))
                } else {
                    Err(DataFusionError::Internal(format!(
                        "User function 'vector_search' is called on table '{}' that has no associated full text search index.",
                        self.tbl,
                    )))
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct VectorSearchTableFunc {
    // This needs to be a weak reference because the DataFusion instance contains the SessionContext which contains this UDTF.
    df: Weak<DataFusion>,
}

impl VectorSearchTableFunc {
    #[must_use]
    pub fn new(df: Weak<DataFusion>) -> Self {
        Self { df }
    }
}

impl VectorSearchTableFunc {
    #[must_use]
    pub fn to_expr(args: &VectorSearchTableFuncArgs) -> Vec<Expr> {
        let mut expr = vec![
            Expr::Column(Column::new_unqualified(args.tbl.to_string())),
            Expr::Literal(ScalarValue::Utf8(Some(args.query.clone()))),
        ];

        if let Some(col) = args.column.as_ref() {
            expr.push(Expr::Column(Column::new_unqualified(col)));
        }
        if let Some(limit) = args.limit {
            expr.push(Expr::Literal(ScalarValue::UInt64(Some(limit as u64))));
        }
        if let Some(include_score) = args.include_score {
            expr.push(Expr::Literal(ScalarValue::Boolean(Some(include_score))));
        }
        expr
    }

    fn parse_args(args: &[Expr]) -> DataFusionResult<VectorSearchTableFuncArgs> {
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
                    "Invalid arguments: ({table_name}, {q}, {a:?}, {b:?}, {c:?}. Expected (table, query, [column, limit, include_score])."
                )));
            }
        };
        Ok(VectorSearchTableFuncArgs {
            tbl: table_name.into(),
            query: q.to_string(),
            column,
            limit: limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            include_score,
        })
    }
}

impl TableFunctionImpl for VectorSearchTableFunc {
    fn call(&self, args: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let args = Self::parse_args(args)?;
        let df = self.df.upgrade().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "An unexpected error occurred when calling {VECTOR_SEARCH_UDTF_NAME}(). Report an issue on GitHub: https://github.com/spiceai/spiceai/issues.\nDetails: DataFusion instance has been dropped."
            ))
        })?;
        let Some(table_provider) = df.get_table_sync(&args.tbl) else {
            return Err(DataFusionError::Plan(format!(
                "Table '{}' does not exist.",
                args.tbl.clone()
            )));
        };

        let embedding_table_provider =
            find_concrete_table_provider::<EmbeddingTable>(&table_provider).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Table '{}' does not have an embedding index.",
                    args.tbl.clone()
                ))
            })?;

        let (col, _) = args.get_column_and_config(&embedding_table_provider.embedded_columns)?;
        if embedding_table_provider.is_chunked(col.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "Chunked columns (i.e. '{col}' in '{}') are not yet supported by '{VECTOR_SEARCH_UDTF_NAME}()'",
                args.tbl.clone()
            )));
        }
        Ok(Arc::new(VectorSearchUDTFProvider {
            args,
            underlying: Arc::clone(&table_provider),
            embedded_columns: embedding_table_provider.embedded_columns.clone(),
            embedding_models: Arc::clone(&embedding_table_provider.embedding_models),
        }))
    }
}

/// The [`TableProvider`] produced from the [`VECTOR_SEARCH_UDTF_NAME`] UDTF.
#[derive(Debug, Clone)]
pub(super) struct VectorSearchUDTFProvider {
    pub args: VectorSearchTableFuncArgs,
    underlying: Arc<dyn TableProvider>,
    embedded_columns: HashMap<String, EmbeddingColumnConfig>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl VectorSearchUDTFProvider {
    /// Embed the query argument and convert to [`Float32Array`].
    async fn vector(
        &self,
        col: &str,
        cfg: &EmbeddingColumnConfig,
    ) -> Result<FixedSizeListArray, Box<dyn std::error::Error + Send + Sync>> {
        let models = self.embedding_models.read().await;
        let Some(embedding_model) = models.get(&cfg.model_name) else {
            return Err(Box::from(format!(
                "Column '{col}' in '{}' requires '{}' embedding model, but is not available.",
                self.args.tbl, cfg.model_name
            )));
        };
        let mut resp = embedding_model
            .embed(EmbeddingInput::String(self.args.query.clone()))
            .await
            .boxed()?;
        let Some(v) = resp.pop() else {
            return Err(Box::from(format!(
                "Embedding model '{}' produced no embedding for the query '{}'.",
                cfg.model_name,
                self.args.query.clone()
            )));
        };
        let Ok(size) = i32::try_from(v.len()) else {
            return Err(Box::from(format!(
                "Embedding vector size '{}' is greater that 32-bit integer.",
                v.len()
            )));
        };

        Ok(
            FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                vec![Some(v.into_iter().map(Some).collect::<Vec<_>>())],
                size,
            ),
        )
    }
}

/// Create a new [`SchemaRef`] with the additional fields specified.
///
/// If a new field is already in [`SchemaRef`], it will be ignored.
pub(super) fn append_fields(schema: &SchemaRef, new_fields: Vec<Arc<Field>>) -> SchemaRef {
    let existing_names: HashSet<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut all_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

    for field in new_fields {
        if !existing_names.contains(field.name().as_str()) {
            all_fields.push(field);
        }
    }

    Arc::new(Schema::new(all_fields))
}

#[async_trait::async_trait]
impl TableProvider for VectorSearchUDTFProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        append_fields(
            &self.underlying.schema(),
            vec![Arc::new(Field::new(
                SEARCH_SCORE_COLUMN_NAME.to_string(),
                arrow_schema::DataType::Float64,
                false,
            ))],
        )
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (col, cfg) = self.args.get_column_and_config(&self.embedded_columns)?;

        let query_vector = self
            .vector(&col, &cfg)
            .await
            .map_err(DataFusionError::External)?;

        let Some(cosine_distance_udf) = state
            .scalar_functions()
            .get(COSINE_DISTANCE_UDF_NAME)
            .cloned()
        else {
            return Err(DataFusionError::Execution(format!(
                "UDF '{COSINE_DISTANCE_UDF_NAME}' is required to perform {VECTOR_SEARCH_UDTF_NAME}, but it is not defined."
            )));
        };

        // TODO: eventually this will need to be a join on underlying, and auxiliary table.
        let scan = LogicalPlan::TableScan(TableScan::try_new(
            self.args.tbl.clone(),
            Arc::new(DefaultTableSource::new(Arc::clone(&self.underlying))),
            None,
            filters.to_vec(),
            None,
        )?);

        let mut base_expr: Vec<Expr> = self
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(i, f)| {
                // `SEARCH_SCORE_COLUMN_NAME` not a simple projection, constructed below.
                if f.name() == SEARCH_SCORE_COLUMN_NAME {
                    return None;
                }
                // Check it is in projection
                if projection.is_none() || projection.is_some_and(|proj| proj.contains(&i)) {
                    Some(Expr::Column(Column::from_name(f.name())))
                } else {
                    None
                }
            })
            .collect();

        base_expr.push(Expr::Alias(Alias {
            expr: Box::from(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(lit(1.0)),
                Operator::Minus,
                Box::new(Expr::ScalarFunction(ScalarFunction {
                    func: cosine_distance_udf,
                    args: vec![
                        Expr::Literal(ScalarValue::FixedSizeList(Arc::new(query_vector))),
                        Expr::Column(Column::from_name(embedding_col!(col))),
                    ],
                })),
            ))),
            relation: None,
            name: SEARCH_SCORE_COLUMN_NAME.to_string(),
            metadata: None,
        }));

        let proj = LogicalPlan::Projection(Projection::try_new(base_expr, Arc::new(scan))?);
        let sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(
                Expr::Column(Column::from_name(SEARCH_SCORE_COLUMN_NAME)),
                false,
                false,
            )],
            input: Arc::new(proj),
            fetch: limit,
        });

        state.create_physical_plan(&sort).await
    }
}
