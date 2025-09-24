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
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, JoinType, Result, ScalarValue, exec_err};
use datafusion::datasource::TableType;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext, coalesce, make_array, md5};
use datafusion_expr::{ExprFunctionExt, ExprSchemable, col, lit};
use itertools::Itertools;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

pub static RRF_UDF_NAME: &str = "rrf";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation {
    doc_section: DocSection::default(),
    description: "Merge and rank several search queries into a single result set solely considering the order and not score of the input search queries".to_string(),
    syntax_example: "rrf(query_1, query_2, ..., k)".to_string(),
    sql_example: None,
    arguments: Some(vec![
        (
            "query...".to_string(),
            "Inline text_search or vector_search UDTF invocations".to_string(),
        ),
        ("k".to_string(), "RRF smoothing parameter".to_string()),
    ]),
    alternative_syntax: None,
    related_udfs: Some(vec!["text_search".to_string(), "vector_search".to_string()]),
}
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

#[derive(Debug, Default)]
struct ReciprocalRankFusionArgs {
    pub search_udtf_exprs: Vec<Expr>,
    pub k: f64,
    pub join_key: Option<Expr>,
}

impl ReciprocalRankFusionArgs {
    /// Constructs `ReciprocalRankFusionArgs` from an rrf UDTF invocation, which is a `TableScan` node
    /// that looks like this...
    /// ```text
    /// TableScan: rrf(text_search(wiki_a_potion, Utf8("apple")), vector_search(wiki_a_potion, Utf8("apple")))
    /// ```
    /// ...into a neat struct of subquery expressions and an optional user-provided smoothing parameter.
    ///
    /// # Arguments
    /// * `args` - A slice of `Expr` containing search UDTF invocations and an optional `k` parameter
    ///
    /// # Returns
    /// * `Ok(ReciprocalRankFusionArgs)` - Successfully parsed arguments
    /// * `Err` - If fewer than 2 search queries are provided or if unparsing fails
    pub fn from_udtf_exprs(args: &[Expr]) -> Result<ReciprocalRankFusionArgs> {
        let mut search_udtfs: Vec<Expr> = vec![];
        let mut k_argument: Option<f64> = None;
        let mut join_pk_argument: Option<Expr> = None;

        for expr in args {
            match expr {
                e @ Expr::ScalarFunction(_) => search_udtfs.push(e.clone()),
                Expr::Literal(ScalarValue::Float64(Some(k)), ..) if k_argument.is_none() => {
                    k_argument = Some(*k);
                }
                Expr::Column(c) if join_pk_argument.is_none() => {
                    join_pk_argument = Some(col(c.name.clone()));
                }
                // Show a useful error for the rest
                other_expr => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "{RRF_UDF_NAME} does not yet support {other_expr} arguments."
                    )));
                }
            }
        }

        if search_udtfs.len() < 2 {
            return Err(DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} needs at least 2 search queries to fuse results."
            )));
        }

        Ok(Self {
            search_udtf_exprs: search_udtfs,
            k: k_argument.unwrap_or(60.0),
            join_key: join_pk_argument,
        })
    }
}

impl Debug for ReciprocalRankFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReciprocalRankFusion")
    }
}

pub struct ReciprocalRankFusion {
    pub session_context: Arc<SessionContext>,
    df: Option<DataFrame>,
}

// TODO: DF support for nested UDTF calls without ScalarUDF "hack"
impl ReciprocalRankFusion {
    #[must_use]
    pub fn from_ctx(session_context: &Arc<SessionContext>) -> Self {
        Self {
            session_context: Arc::clone(session_context),
            df: None,
        }
    }

    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    #[must_use]
    pub fn with_df(mut self, df: DataFrame) -> Self {
        self.df = Some(df);
        self
    }

    fn scalar_stub_error<T>() -> Result<T, DataFusionError> {
        exec_err!(
            "{RRF_UDF_NAME} is a table function with a scalar stub. Please call as a table function."
        )
    }

    // Given arguments to n search calls: execute searches, generate row IDs, rank by score, JOIN,
    // then finally re-rank and sort fused results
    fn rerank_and_fuse_df(&self, args: &ReciprocalRankFusionArgs) -> Result<DataFrame> {
        let subquery_dfs = self.prepare_and_execute_subqueries(args)?;

        let score_expr = (0..subquery_dfs.len())
            .map(|i| {
                lit(1.0f64)
                    / (lit(args.k)
                        + coalesce(vec![col(format!("search_{i}.rank")), lit(f64::INFINITY)]))
            })
            .reduce(|a, b| a + b);

        let score_expr = if let Some(score_expr) = score_expr {
            score_expr.alias("fused_score")
        } else {
            return exec_err!("{RRF_UDF_NAME} unable to compute fused_score");
        };

        // Create column expressions for final projection
        let mut columns: Vec<Expr> = vec![score_expr];
        columns.extend(subquery_dfs[0].schema().columns().iter().filter_map(|c| {
            match c.name.as_str() {
                "__spice_rrf_row_id" | "rank" | "score" => None,
                // TODO: do we want the embedding in the final projection?
                other if other.ends_with("_embedding") => None,
                other => Some(
                    coalesce(
                        (0..subquery_dfs.len())
                            .map(|i| col(format!("search_{i}.{other}")))
                            .collect(),
                    )
                    .alias(other),
                ),
            }
        }));

        // Join DFs together, apply final projection, and sort by the new fused score
        let mut join_err: Option<DataFusionError> = None;
        let maybe_joined = subquery_dfs.into_iter().reduce(|a, b| {
            let joined = match args.join_key.clone().map(|e| e.qualified_name()) {
                Some((_, join_key)) => Self::fold_join(a, b, &join_key),
                None => Self::fold_join(a, b, "__spice_rrf_row_id"),
            };

            // No way to short circuit reduce, so we will surface the error at the end
            match joined {
                Ok(joined) => joined,
                Err(e) => {
                    join_err = Some(e);
                    self.session_context
                        .read_empty()
                        .unwrap_or_else(|_| unreachable!("must be able to make an empty DataFrame"))
                }
            }
        });

        if let Some(error) = join_err {
            return Err(error);
        }

        if let Some(joined) = maybe_joined {
            tracing::trace!("{RRF_UDF_NAME} made reranked & fused DF for: {args:?}");

            joined
                .select(columns)?
                .distinct()?
                .sort(vec![col("fused_score").sort(false, false)])
        } else {
            exec_err!("{RRF_UDF_NAME}: Unable to join result sets")
        }
    }

    // Given RRF args with unparsed search udtf exprs, turn each subquery into a DF,
    // add a hashed row ID, rank it, then give it an alias of `search_{i_in_argv}`
    fn prepare_and_execute_subqueries(
        &self,
        args: &ReciprocalRankFusionArgs,
    ) -> Result<Vec<DataFrame>> {
        tracing::trace!("{RRF_UDF_NAME} preparing subqueries for: {:?}", args);

        let search_dfs: Vec<DataFrame> = args
            .search_udtf_exprs
            .iter()
            .map(|expr| {
                let Expr::ScalarFunction(sf) = expr else {
                    unreachable!("Must be a scalar function node")
                };
                self.session_context
                    .table_function(sf.name())
                    .and_then(|udtf| udtf.create_table_provider(&sf.args))
                    .and_then(|provider| self.session_context.read_table(provider))
            })
            .collect::<Result<Vec<_>>>()?;

        let prepared_dfs: Vec<DataFrame> = search_dfs
            .into_iter()
            .enumerate()
            .map(|(i, df)| {
                // Ensure that all projections have a score column
                if !df.schema().has_column_with_unqualified_name("score") {
                    return exec_err!(
                        "{RRF_UDF_NAME}: Query at position {i} does not have a `score` column."
                    );
                }

                let df_with_id = match args.join_key {
                    Some(_) => Ok(df),
                    None => Self::with_rrf_rowid(df),
                };

                df_with_id
                    .and_then(Self::with_rank)
                    .and_then(|df| df.alias(&format!("search_{i}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(prepared_dfs)
    }

    // Given a DF with overlapping unqualified names (as produced by JOIN), where column values
    // are equivalent, return the first (arbitrary) qualified name.
    fn first_qualified_field(df: &DataFrame, name: &str) -> Result<String> {
        df.schema()
            .qualified_fields_with_unqualified_name(name)
            .first()
            .and_then(|(maybe_table_reference, f)| {
                maybe_table_reference.map(|tr| format!("{}.{}", tr.table(), &f.name()))
            })
            .ok_or(DataFusionError::Execution(format!(
                "{RRF_UDF_NAME}: Cannot resolve column {name} when fusing results"
            )))
    }

    // Reduces 2 or more search subquery DFs into a single one
    fn fold_join(a: DataFrame, b: DataFrame, join_key: &str) -> Result<DataFrame> {
        let id_a = Self::first_qualified_field(&a, join_key)?;
        let id_b = Self::first_qualified_field(&b, join_key)?;

        a.join(b, JoinType::Full, &[&id_a], &[&id_b], None)
    }

    // Window and rank a search subquery by its `score` field, exposing a `rank` column
    fn with_rank(df: DataFrame) -> Result<DataFrame> {
        let rank_expr = row_number()
            .order_by(vec![col("score").sort(false, false)])
            .build()?
            .alias("rank");

        df.window(vec![rank_expr])
    }

    // Create an internal row ID by hashing all pieces of the row
    fn with_rrf_rowid(df: DataFrame) -> Result<DataFrame> {
        let bin_columns: Vec<Expr> = df
            .schema()
            .columns()
            .iter()
            .sorted_by_key(|c| c.name())
            // Don't hash embeddings or scores
            .filter_map(|c| match c.name() {
                "score" => None,
                name if name.ends_with("_embedding") => None,
                name => Some(col(name).cast_to(&DataType::Utf8, df.schema())),
            })
            .collect::<Result<Vec<_>>>()?;

        let rrf_row_id = md5(make_array(bin_columns).cast_to(&DataType::Utf8, df.schema())?);
        df.with_column("__spice_rrf_row_id", rrf_row_id)
    }
}

/// This is only implemented as a documentation stub, so that we show up in `SHOW FUNCTIONS`
impl ScalarUDFImpl for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        RRF_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Self::scalar_stub_error()
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Self::scalar_stub_error()
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&*DOCUMENTATION)
    }
}

impl TableFunctionImpl for ReciprocalRankFusion {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let rrf_args = ReciprocalRankFusionArgs::from_udtf_exprs(args)?;
        let rerank_and_fuse_df = self.rerank_and_fuse_df(&rrf_args)?;
        Ok(Arc::new(
            ReciprocalRankFusion::from_ctx(&self.session_context).with_df(rerank_and_fuse_df),
        ))
    }
}

#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        match self.df.as_ref() {
            Some(df) => Arc::clone(df.schema().inner()),
            None => panic!("ReciprocalRankFusion schema is not set. This is a bug in Spice.ai"),
        }
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(ref df) = self.df {
            let mut df = df.clone();

            if let Some(filter) = filters.iter().cloned().reduce(Expr::and) {
                df = df.filter(filter)?;
            }

            if let Some(projection) = projection {
                df = df.select(
                    self.schema()
                        .project(projection)?
                        .fields
                        .iter()
                        .map(|f| col(f.name())),
                )?;
            }

            df.limit(0, limit)?.create_physical_plan().await
        } else {
            exec_err!("ReciprocalRankFusion could not create physical plan")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Runtime;
    use crate::builder::RuntimeBuilder;
    use crate::datafusion::query::QueryBuilder;
    use crate::datafusion::udf::register_udfs;
    use crate::embeddings::table::EmbeddingColumnConfig;
    use crate::embeddings::table::EmbeddingTable;
    use crate::request::{Protocol, RequestContext};
    use crate::search::rrf::ReciprocalRankFusionArgs;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use arrow::array::{FixedSizeListArray, as_string_array};
    use arrow::record_batch::RecordBatch;
    use async_graphql::futures_util::TryStreamExt;
    use async_openai::types::EmbeddingInput;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::common::cast::as_float64_array;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use llms::embeddings::Embed;
    use llms::model2vec::Model2Vec;
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};
    use tokio::sync::RwLock;

    pub static TEST_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
        LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

    fn make_test_table(test_data: &[&str]) -> Result<Arc<dyn TableProvider>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
            Field::new(
                "content_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 64),
                true,
            ),
        ]));

        let mut embedded_columns = HashMap::new();
        embedded_columns.insert(
            "content".to_string(),
            EmbeddingColumnConfig {
                model_name: "test_model".to_string(),
                vector_size: 64,
                in_base_table: true,
                chunker: None,
            },
        );

        let embedding_model = Arc::new(
            Model2Vec::from_params(
                "minishlab/potion-base-2M",
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .expect("Must make embedding model"),
        );

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from_iter_values(
                    0i64..i64::try_from(test_data.len()).expect("Must cast"),
                )),
                Arc::new(StringArray::from_iter_values(test_data.iter())),
                Arc::new(FixedSizeListArray::from_iter_primitive::<
                    arrow::datatypes::Float32Type,
                    _,
                    _,
                >(
                    test_data.iter().map(|s| {
                        embedding_model
                            .embed_sync(EmbeddingInput::String((*s).to_string()))
                            .map(|e| e[0].iter().map(|f| Some(*f)).collect::<Vec<Option<_>>>())
                            .ok()
                    }),
                    64,
                )),
            ],
        )?;

        let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
        let mut embedding_model_store: HashMap<String, Arc<dyn Embed>> = HashMap::new();
        embedding_model_store.insert("test_model".to_string(), embedding_model);

        Ok(Arc::new(EmbeddingTable {
            base_table: mem_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(embedding_model_store)),
        }))
    }

    async fn make_test_runtime() -> Result<Runtime> {
        let rt = RuntimeBuilder::new().build().await;
        rt.df
            .ctx
            .state()
            .config_mut()
            .set_extension(Arc::clone(&TEST_REQUEST_CONTEXT));

        let test_table = make_test_table(&[
            "banana yellow curved fruit",
            "orange citrus round juicy",
            "apple fruit sweet red crispy",
        ])?;
        rt.df
            .ctx
            .register_table("foo", test_table)
            .expect("Failed to register foo table");

        register_udfs(&rt);
        Ok(rt)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries() {
        let runtime = make_test_runtime()
            .await
            .expect("Failed to create test runtime");

        let query =
            "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'), id, 600.0)";
        let query = QueryBuilder::new(query, runtime.datafusion()).build();
        let results = query
            .run()
            .await
            .expect("Must run query")
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect("Must collect results");

        let content = as_string_array(
            results[0]
                .column_by_name("content")
                .expect("Must have content column"),
        );
        assert_eq!(content.value(0), "apple fruit sweet red crispy");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_score_computation() {
        let runtime = make_test_runtime()
            .await
            .expect("Failed to create test runtime");

        let empty_table = make_test_table(&[]).expect("Failed to create empty table");
        runtime
            .df
            .ctx
            .register_table("bar", empty_table)
            .expect("Failed to register bar table");

        let query_empty_red =
            "select * from rrf(vector_search(bar, 'empty'), vector_search(foo, 'red'), id, 600.0)";
        let query_empty_red = QueryBuilder::new(query_empty_red, runtime.datafusion()).build();
        let query_empty_red_results = query_empty_red
            .run()
            .await
            .expect("Must run query")
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect("Must collect results");

        let query_empty_red_content = as_float64_array(
            query_empty_red_results[0]
                .column_by_name("fused_score")
                .expect("Must have score column"),
        )
        .expect("Must be f64[]");

        let query_empty_red_score = query_empty_red_content.value(0);

        let query_red_empty =
            "select * from rrf(vector_search(foo, 'red'), vector_search(bar, 'empty'), id, 600.0)";
        let query_red_empty = QueryBuilder::new(query_red_empty, runtime.datafusion()).build();
        let query_red_empty_results = query_red_empty
            .run()
            .await
            .expect("Must run query")
            .data
            .try_collect::<Vec<RecordBatch>>()
            .await
            .expect("Must collect results");

        let query_red_empty_content = as_float64_array(
            query_red_empty_results[0]
                .column_by_name("fused_score")
                .expect("Must have score column"),
        )
        .expect("Must be f64[]");

        let query_red_empty_score = query_red_empty_content.value(0);

        let score_diff = (query_red_empty_score - query_empty_red_score).abs();
        assert!(score_diff < 0.0001f64);
    }

    fn stub_scalar_function(name: &str) -> Expr {
        let stub_udf = create_udf(
            name,
            vec![DataType::Utf8; 0],
            DataType::Utf8,
            Volatility::Stable,
            Arc::new(|_| {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                    "stub".to_string(),
                ))))
            }),
        );

        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(stub_udf), vec![]))
    }

    #[test]
    fn test_parse_argument_exprs() {
        // Empty call
        let empty_args = ReciprocalRankFusionArgs::from_udtf_exprs(&[]);
        assert!(empty_args.is_err());
        assert_eq!(
            empty_args.err().map(|e| e.to_string()),
            Some(
                "Error during planning: rrf needs at least 2 search queries to fuse results."
                    .to_string()
            )
        );

        // Call with at least 2 arguments, but one of them overrides k only
        let one_search_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(&[
            stub_scalar_function("one_search_with_k"),
            lit(1337.0f64),
        ]);
        assert!(one_search_with_k.is_err());
        assert_eq!(
            one_search_with_k.err().map(|e| e.to_string()),
            Some(
                "Error during planning: rrf needs at least 2 search queries to fuse results."
                    .to_string()
            )
        );

        // Call with many searches
        let mut many_search_exprs: Vec<_> = (0..100)
            .map(|i| stub_scalar_function(&format!("fn_{i}")))
            .collect::<Vec<_>>();

        let many_searches = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_searches.is_ok());
        assert_eq!(
            many_searches
                .expect("Must make args")
                .search_udtf_exprs
                .len(),
            100
        );

        // Call with many searches + k override
        many_search_exprs.push(lit(1337.0f64));
        let many_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_with_k.is_ok());

        let many_with_k = many_with_k.expect("Must make args");
        assert_eq!(many_with_k.search_udtf_exprs.len(), 100);
        // assert_eq!(many_with_k.k, 1337.0f64);

        // Call with many searches + k override + join key specified
        many_search_exprs.push(col("hello"));
        let many_with_k_and_column = ReciprocalRankFusionArgs::from_udtf_exprs(&many_search_exprs);
        assert!(many_with_k_and_column.is_ok());

        let many_with_k_and_column = many_with_k_and_column.expect("Must make args");
        assert_eq!(many_with_k_and_column.search_udtf_exprs.len(), 100);
        // assert_eq!(many_with_k_and_column.k, 1337.0f64);
        assert_eq!(many_with_k_and_column.join_key, Some(col("hello")));
    }
}
