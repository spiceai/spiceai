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
//! - tbl: Table to perform full text search upon. If the table does not support it (i.e. no index), and empty table is returned.
//! - query: Query to perform full text search against.
//! - col: If provided, use this column to compare vector search results against.
//! - limit:
//! - `include_score` (default true): If false, do not return `score` in the table projection.
//!
//! The schema of the resultant table will be: `schema(tbl) ∪ {score}`, where:
//!  - `score` (f32): The similarity score of the row with the request `query`.
//!  - `value` (UTF8): The subset of the column most relevant. For non-chunked embedding columns, `value` is the entire value.

use arrow::{array::FixedSizeListArray, datatypes::Float32Type};
use arrow_schema::{DataType, Field, SchemaRef};
use async_openai::types::EmbeddingInput;
use datafusion::common::exec_err;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::{ColumnarValue, Signature, Volatility};
use datafusion::{
    catalog::{Session, TableFunctionImpl, TableProvider},
    common::Column,
    datasource::{DefaultTableSource, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::{Operator, SortExpr, expr::ScalarFunction},
    physical_plan::ExecutionPlan,
    prelude::{Expr, lit},
    scalar::ScalarValue,
    sql::TableReference,
};

use datafusion_expr::{
    LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDFImpl, binary_expr, col, ident,
};
use futures::FutureExt;
use itertools::Itertools;
#[cfg(feature = "models")]
use runtime_datafusion_udfs::embed::EMBED_UDF_NAME;
#[cfg(not(feature = "models"))]
const EMBED_UDF_NAME: &str = "embed";
use search::generation::CandidateGeneration;
use search::generation::util::get_primary_keys;
use std::{
    any::Any,
    cmp::min,
    collections::HashMap,
    sync::{Arc, LazyLock, Weak},
};

use runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME;
use search::{
    SEARCH_SCORE_COLUMN_NAME, generation::util::append_fields, index::SearchIndex,
    provider::SearchQueryProvider,
};
use snafu::ResultExt;

use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use crate::search::candidate::vector::ChunkedNonIndexVectorGeneration;
use crate::{
    datafusion::DataFusion,
    embedding_col,
    embeddings::table::{EmbeddingColumnConfig, EmbeddingTable},
    model::EmbeddingModelStore,
    search::util::{
        find_concrete_table_provider, find_index_in_table_provider, table_ref_from_column_expr,
        to_column_expr,
    },
};
use runtime_request_context::{AsyncMarker, RequestContext};
#[cfg(feature = "s3_vectors")]
use search::index::s3_vectors::S3Vector;

use search::index::chunking::ChunkedSearchIndex;
use tokio::sync::RwLock;

pub static VECTOR_SEARCH_UDTF_NAME: &str = "vector_search";

pub static VECTOR_SEARCH_SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

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
                    return Err(DataFusionError::Plan(format!(
                        "User function 'vector_search' is called on table '{}' that has {} vector search columns. Must call 'vector_search' with column parameter, e.g. `vector_search(\"my table\", 'my query', my_embedded_col)`.",
                        self.tbl,
                        embedded_columns.len()
                    )));
                }
                if let Some((col, cfg)) = embedded_columns.iter().next() {
                    Ok((col.clone(), cfg.clone()))
                } else {
                    Err(DataFusionError::Plan(format!(
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

    // store a pointer to use for Hash/Eq since UDTF impls require this trait bound but we cannot feasibly make `DataFusion` implement them.
    df_ptr: u64,

    explicit_pks: HashMap<TableReference, Vec<String>>,
}

impl PartialEq for VectorSearchTableFunc {
    fn eq(&self, other: &Self) -> bool {
        self.df_ptr == other.df_ptr && self.explicit_pks == other.explicit_pks
    }
}

impl Eq for VectorSearchTableFunc {}

impl std::hash::Hash for VectorSearchTableFunc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.df_ptr.hash(state);
    }
}

pub fn parse_limit_scalar(scalar: &ScalarValue) -> Result<u64, DataFusionError> {
    match scalar {
        ScalarValue::Int64(Some(limit)) => u64::try_from(*limit).map_err(|_| {
            DataFusionError::Plan(format!(
                "Limit argument must be a non-negative integer, but got {limit}."
            ))
        }),
        ScalarValue::UInt64(Some(limit)) => Ok(*limit),
        ScalarValue::Utf8(Some(limit_str)) => limit_str.parse::<u64>().map_err(|_| {
            DataFusionError::Plan(format!(
                "Limit argument must be a non-negative integer, but got '{limit_str}'."
            ))
        }),
        _ => Err(DataFusionError::Plan(format!(
            "Limit argument must be a non-negative integer, but got {scalar}."
        ))),
    }
}

impl VectorSearchTableFunc {
    #[must_use]
    pub fn new(df: Weak<DataFusion>, explicit_pks: HashMap<TableReference, Vec<String>>) -> Self {
        let ptr = df.as_ptr().addr() as u64;
        Self {
            df,
            explicit_pks,
            df_ptr: ptr,
        }
    }

    fn scalar_invocation_error<T>() -> Result<T, DataFusionError> {
        exec_err!("{VECTOR_SEARCH_UDTF_NAME} does not support scalar invocation.")
    }
}

impl VectorSearchTableFunc {
    #[must_use]
    pub fn to_expr(args: &VectorSearchTableFuncArgs) -> Vec<Expr> {
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

    fn parse_args(args: &[Expr]) -> DataFusionResult<VectorSearchTableFuncArgs> {
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
        Ok(VectorSearchTableFuncArgs {
            tbl: tbl_ref
                .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
                .into(),
            query: q.to_string(),
            column,
            limit: limit.map(|l| usize::try_from(l).unwrap_or(usize::MAX)),
            include_score,
        })
    }

    #[cfg(feature = "s3_vectors")]
    fn index_based_vector_table(
        tbl: &Arc<dyn TableProvider>,
        args: &VectorSearchTableFuncArgs,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let mut vector_indexes: Vec<Arc<dyn SearchIndex>> = match (
            find_index_in_table_provider::<S3Vector>(tbl),
            find_index_in_table_provider::<ChunkedSearchIndex>(tbl),
        ) {
            (_, Some((chunked_index, _))) => chunked_index
                .into_iter()
                .map(|c| Arc::new(c.clone()) as Arc<dyn SearchIndex>)
                .collect::<Vec<_>>(),
            (Some((vector_index, _)), None) => vector_index
                .into_iter()
                .map(|c| Arc::new(c.clone()) as Arc<dyn SearchIndex>)
                .collect::<Vec<_>>(),
            (None, None) => return Ok(None),
        };

        let vector_index_opt = if let Some(col) = &args.column {
            vector_indexes
                .into_iter()
                .find(|idx| *idx.search_column() == *col)
        } else {
            if vector_indexes.len() > 1 {
                return Err(DataFusionError::Internal(format!(
                    "User function 'vector_search' is called on table '{}' that has {} vector search columns. Must call 'vector_search' with column parameter, e.g. `vector_search(\"my table\", 'my query', my_embedded_col)`.",
                    args.tbl,
                    vector_indexes.len()
                )));
            }
            vector_indexes.pop()
        };
        let Some(vector_index) = vector_index_opt else {
            return Ok(None);
        };

        Ok(Some(Arc::new(
            SearchQueryProvider::try_from_index(
                &vector_index,
                Arc::clone(tbl),
                args.query.as_str(),
                args.limit,
            )?
            .call_on_scan(Arc::new(|| {
                async {
                    let request_context = RequestContext::current(AsyncMarker::new().await);
                    telemetry::track_vector_search(&request_context.to_dimensions());
                }
                .boxed()
            })),
        )))
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

        // For table with a vector engine, use it.
        #[cfg(feature = "s3_vectors")]
        if let Some(table_provider) = Self::index_based_vector_table(&table_provider, &args)? {
            return Ok(table_provider);
        }

        // If an embedding column is defined, fallback to JIT or.
        let embedding_table_provider =
            find_concrete_table_provider::<EmbeddingTable>(&table_provider).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Table '{}' does not have an embedding index.",
                    args.tbl.clone()
                ))
            })?;

        let (col, _) = args.get_column_and_config(&embedding_table_provider.embedded_columns)?;
        if embedding_table_provider.is_chunked(col.as_str()) {
            let state = df.ctx.state();
            let Some(embed_udf) = state.scalar_functions().get(EMBED_UDF_NAME) else {
                return Err(DataFusionError::Plan(format!(
                    "'{VECTOR_SEARCH_UDTF_NAME}()' requires missing UDF: '{EMBED_UDF_NAME}'",
                )));
            };

            // Unsafe: worse case is metric without dimensions.
            let dimensions = unsafe { RequestContext::current_sync().to_dimensions() };
            telemetry::track_vector_search(&dimensions);
            let pks = self
                .explicit_pks
                .get(&args.tbl)
                .cloned()
                .or_else(|| get_primary_keys(&table_provider).ok());

            let table = ChunkedNonIndexVectorGeneration::new(
                &table_provider,
                &args.tbl,
                embed_udf,
                embedding_table_provider
                    .get_embedding_model_used_by(&col)
                    .unwrap_or_default(),
                pks.unwrap_or_default(),
                &col,
            )
            .search(args.query)?;
            return alias_value_to_match(Arc::clone(&table));
        }

        Ok(Arc::new(VectorSearchUDTFProvider {
            args,
            underlying: Arc::clone(&table_provider),
            embedded_columns: embedding_table_provider.embedded_columns.clone(),
            embedding_models: Arc::clone(&embedding_table_provider.embedding_models),
        }))
    }
}

/// This is a stub implementation, so that we can nest UDTF function invocations
impl ScalarUDFImpl for VectorSearchTableFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        VECTOR_SEARCH_UDTF_NAME
    }

    fn signature(&self) -> &Signature {
        &VECTOR_SEARCH_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Self::scalar_invocation_error()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        Self::scalar_invocation_error()
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

    /// Determine whether and how to pick between
    ///   1. The query-provided limit (i.e. passed through in the SQL/Logical plan)
    ///   2. The limit provided in `vector_search` args
    fn limit_to_use(&self, limit: Option<usize>) -> usize {
        match (self.args.limit, limit) {
            (Some(l), None) | (None, Some(l)) => l,
            (None, None) => 1000, // Default limit when none specified

            // Equivalent to using always using pre_limit, unless `limit` < `pre_limit`.
            (Some(a), Some(b)) => min(a, b),
        }
    }
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
        let request_context = RequestContext::current(AsyncMarker::new().await);
        telemetry::track_vector_search(&request_context.to_dimensions());
        let (embed_col, cfg) = self.args.get_column_and_config(&self.embedded_columns)?;

        let query_vector = self
            .vector(&embed_col, &cfg)
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
        let mut scan = LogicalPlanBuilder::scan(
            self.args.tbl.clone(),
            Arc::new(DefaultTableSource::new(Arc::clone(&self.underlying))),
            None,
        )?;

        if let Some(f) = filters.iter().cloned().reduce(Expr::and) {
            scan = scan.filter(f)?;
        }

        let search_field_index = self
            .schema()
            .index_of(SEARCH_SCORE_COLUMN_NAME)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        let mut final_expr: Vec<Expr> = self
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
                    Some(ident(f.name()))
                } else {
                    None
                }
            })
            .collect();
        let mut base_expr = final_expr.clone();

        base_expr.push(
            binary_expr(
                lit(1.0),
                Operator::Minus,
                Expr::ScalarFunction(ScalarFunction {
                    func: cosine_distance_udf,
                    args: vec![
                        lit(ScalarValue::FixedSizeList(Arc::new(query_vector))),
                        ident(embedding_col!(embed_col)),
                    ],
                }),
            )
            .alias(SEARCH_SCORE_COLUMN_NAME),
        );

        // only include score in the projection if it is requested.
        // Otherwise, if the query is `SELECT a FROM vector_search(...)`, it will fail because we supplied too many columns in the response!
        if projection.is_none() || projection.is_some_and(|proj| proj.contains(&search_field_index))
        {
            final_expr.push(col(SEARCH_SCORE_COLUMN_NAME));
        }

        let final_plan = scan
            .project(base_expr)?
            .sort(vec![SortExpr::new(
                Expr::Column(Column::from_name(SEARCH_SCORE_COLUMN_NAME)),
                false,
                false,
            )])?
            .limit(0, Some(self.limit_to_use(limit)))?
            // wrap the score calculation in a subquery before final projection, to avoid collapsing away the score calculation.
            .alias("tbl")?
            .project(final_expr)?
            .build()?;

        state.create_physical_plan(&final_plan).await
    }
}

/// Create a new [`TableProvider`] where columns named `value` are aliased to `match`.
///
/// This is used in chunked table providers which expose 'value' for [`CandidateGeneration`], but match in [`VECTOR_SEARCH_UDTF_NAME`] UDTF.
fn alias_value_to_match(
    tbl: Arc<dyn TableProvider>,
) -> Result<Arc<dyn TableProvider>, DataFusionError> {
    let bldr = LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(tbl)), None)?;
    let cols = Arc::clone(bldr.schema())
        .columns()
        .into_iter()
        .map(|c| {
            if c.name() == "value" {
                Expr::Column(c).alias("match")
            } else {
                Expr::Column(c)
            }
        })
        .collect::<Vec<Expr>>();
    Ok(Arc::new(ViewTable::new(bldr.project(cols)?.build()?, None)))
}
