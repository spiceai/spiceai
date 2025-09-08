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
use datafusion::common::{exec_err, DataFusionError, JoinType, Result, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    ColumnarValue, DocSection, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{coalesce, make_array, sha224, DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::Expr as SqlExpr;
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion::sql::unparser::Unparser;
use datafusion_expr::{col, lit, ExprFunctionExt, ExprSchemable, UserDefinedLogicalNode};
use futures::future::join_all;
use itertools::Itertools;
use logos::internal::CallbackResult;
use std::any::Any;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Handle;
use tokio::task;

pub static RRF_UDF_NAME: &str = "rrf";
pub static DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| Documentation {
    doc_section: DocSection::default(),
    description: "Merge and re-rank several search queries into one result set".to_string(),
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
});

pub static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::variadic_any(Volatility::Stable));

#[derive(Debug, Default)]
struct ReciprocalRankFusionArgs {
    pub search_udtf_exprs: Vec<SqlExpr>,
    pub k: f64,
}

impl ReciprocalRankFusionArgs {
    /// Constructs `ReciprocalRankFusionArgs` from an rrf UDTF invocation, which is a TableScan node
    /// that looks like this...
    /// ```
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
        // Find user-provided smoothing param if provided
        let k = if let Some(Expr::Literal(ScalarValue::Float64(Some(k)), ..)) = args.last() {
            *k
        } else {
            // The original RRF paper uses 60 as its default smoothing parameter
            60.0
        };

        // Unparse UDTF invocations
        let unparser = Unparser::new(&DefaultDialect {});
        let search_udtf_exprs: Vec<SqlExpr> = args
            .iter()
            .filter_map(|expr| match expr {
                e @ Expr::ScalarFunction(_) => Some(unparser.expr_to_sql(&e)),
                // Leave the k-override literals alone
                Expr::Literal(ScalarValue::Float64(_), ..) => None,
                // Show a useful error for the rest
                other_expr => Some(Err(DataFusionError::NotImplemented(format!(
                    "{RRF_UDF_NAME} does not yet support {other_expr} arguments."
                )))),
            })
            .collect::<Result<Vec<_>>>()?;

        if search_udtf_exprs.len() < 2 {
            return Err(DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} needs at least 2 search queries to fuse results."
            )));
        }

        Ok(Self {
            search_udtf_exprs,
            k,
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
    pub fn from_ctx(session_context: Arc<SessionContext>) -> Self {
        Self {
            session_context,
            df: None,
        }
    }

    #[must_use]
    pub fn as_any(&self) -> &dyn Any {
        self
    }

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

        let score_expr = coalesce(
            (0..subquery_dfs.len())
                .map(|i| {
                    lit(1.0f64)
                        / (lit(args.k)
                            + coalesce(vec![col(format!("search_{}.rank", i)), lit(f64::INFINITY)]))
                })
                .collect(),
        )
        .alias("fused_score");

        // Create column expressions for final projection
        let mut columns: Vec<Expr> = vec![score_expr];
        columns.extend(subquery_dfs[0].schema().columns().iter().filter_map(|c| {
            match c.name.as_str() {
                "__spice_rrf_row_id" | "rank" | "score" => None,
                // TODO: do we want the embedding in the final projection?
                other if other.contains("embedding") => None,
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
        subquery_dfs
            .into_iter()
            .reduce(|a, b| Self::fold_join(a, b).unwrap())
            .expect("Must have joined DF")
            .select(columns)?
            .distinct()?
            .sort(vec![col("fused_score").sort(false, false)])
    }

    // Given RRF args with unparsed search udtf exprs, turn each subquery into a DF,
    // add a hashed row ID, rank it, then give it an alias of `search_{i_in_argv}`
    fn prepare_and_execute_subqueries(
        &self,
        args: &ReciprocalRankFusionArgs,
    ) -> Result<Vec<DataFrame>> {
        let search_df_queries: Vec<_> = args
            .search_udtf_exprs
            .iter()
            .map(|sqlexpr| format!("select * from {}", sqlexpr.to_string()))
            .collect::<Vec<_>>();

        let search_df_futures: Vec<_> = search_df_queries
            .iter()
            .map(|sql| self.session_context.sql(sql))
            .collect();

        let search_dfs: Vec<DataFrame> = task::block_in_place(move || {
            Handle::current()
                .block_on(join_all(search_df_futures))
                .into_iter()
                .collect::<Result<Vec<_>>>()
        })?
        .into_iter()
        .enumerate()
        .map(|(i, df)| {
            Self::with_rrf_rowid(df)
                .and_then(Self::with_rank)
                .and_then(|df| df.alias(&format!("search_{i}")))
        })
        .collect::<Result<Vec<_>>>()?;

        // Ensure that all projections have a score column
        for (i, df) in search_dfs.iter().enumerate() {
            if !df.schema().has_column_with_unqualified_name("score") {
                return exec_err!(
                    "{RRF_UDF_NAME}: Query at position {i} does not have a `score` column."
                );
            }
        }

        Ok(search_dfs)
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
                "{RRF_UDF_NAME}: Cannot resolve {name} when fusing results"
            )))
    }

    // Reduces 2 or more search subquery DFs into a single one
    fn fold_join(a: DataFrame, b: DataFrame) -> Result<DataFrame> {
        let id_a = Self::first_qualified_field(&a, "__spice_rrf_row_id")?;
        let id_b = Self::first_qualified_field(&b, "__spice_rrf_row_id")?;

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
                name if name.contains("embedding") => None,
                name => Some(col(name).cast_to(&DataType::Utf8, df.schema())),
            })
            .collect::<Result<Vec<_>>>()?;

        let rrf_row_id = sha224(make_array(bin_columns).cast_to(&DataType::Utf8, df.schema())?);
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
            ReciprocalRankFusion::from_ctx(Arc::clone(&self.session_context))
                .with_df(rerank_and_fuse_df),
        ))
    }
}

#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(
            self.df
                .as_ref()
                .expect("ReciprocalRankFusion must have a schema")
                .schema()
                .inner(),
        )
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.df {
            Some(ref df) => df.clone().create_physical_plan().await,
            None => exec_err!("ReciprocalRankFusion could not create physical plan"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::RuntimeBuilder;
    use crate::datafusion::udf::register_udfs;
    use crate::embeddings::table::EmbeddingColumnConfig;
    use crate::embeddings::table::EmbeddingTable;
    use crate::search::rrf::ReciprocalRankFusionArgs;
    use crate::Runtime;
    use arrow::array::Int64Array;
    use arrow::array::StringArray;
    use arrow::array::{as_string_array, ArrayAccessor, FixedSizeListArray};
    use arrow::record_batch::RecordBatch;
    use async_openai::types::EmbeddingInput;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::catalog::MemTable;
    use datafusion::catalog::TableProvider;
    use datafusion::common::Result;
    use datafusion::logical_expr::lit;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::{create_udf, ColumnarValue, Volatility};
    use datafusion::scalar::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use llms::embeddings::Embed;
    use llms::model2vec::Model2Vec;
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock};
    use tokio::sync::RwLock;

    pub static TEST_DATA: LazyLock<Vec<&str>> = LazyLock::new(|| {
        vec![
            "banana yellow curved fruit",
            "orange citrus round juicy",
            "apple fruit sweet red crispy",
        ]
    });

    fn make_test_table() -> Result<Arc<dyn TableProvider>> {
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
                Arc::new(Int64Array::from_iter_values((0i64..TEST_DATA.len() as i64))),
                Arc::new(StringArray::from_iter_values(TEST_DATA.iter())),
                Arc::new(FixedSizeListArray::from_iter_primitive::<
                    arrow::datatypes::Float32Type,
                    _,
                    _,
                >(
                    TEST_DATA.iter().map(|s| {
                        embedding_model
                            .embed_sync(EmbeddingInput::String(s.to_string()))
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
        register_udfs(&rt);

        let test_table = make_test_table()?;

        rt.df
            .ctx
            .register_table("foo", test_table)
            .expect("Failed to register foo table");

        Ok(rt)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_fuse_queries() {
        let runtime = make_test_runtime()
            .await
            .expect("Failed to create test runtime");

        // Should match row containing "apple"
        let query = "select * from rrf(vector_search(foo, 'crispy'), vector_search(foo, 'red'))";
        let ctx = Arc::clone(&runtime.df.ctx);

        let df = ctx.sql(query).await.expect("Must parse query");
        let results = df.collect().await.expect("Must collect results");

        let content = as_string_array(
            results[0]
                .column_by_name("content")
                .expect("Must have content column"),
        );
        assert_eq!(content.value(0), TEST_DATA[2]);
    }

    // fn extract_udtf_args_from_sqlexpr(udtf_name: &str, sql: &str) -> Option<TableFunctionArgs> {
    //     use datafusion::sql::parser::{DFParser, DFParserBuilder};
    //     use datafusion::sql::parser::{Statement};
    //     use datafusion_expr::sqlparser::ast::{Statement as SQLStatement, SetExpr, TableFactor, TableFunctionArgs};
    //     use datafusion::sql::sqlparser::dialect::GenericDialect;
    //     use datafusion::sql::sqlparser::ast::{visit_expressions, visit_relations, visit_statements};
    //     let mut parser = DFParserBuilder::new(sql).build().expect("Must parse query");
    //     let statements = parser.parse_statements().expect("Must parse statements");
    //
    //     if let Statement::Statement(sql_statement) = &statements[0] {
    //         if let SQLStatement::Query(query) = &**sql_statement {
    //             if let SetExpr::Select(select) = &*query.body {
    //
    //                 match &select.from[0].relation {
    //                     TableFactor::Table { name, args, .. } if name.to_string() == udtf_name => {
    //                         return args.clone();
    //                     }
    //                     _ => None::<TableFunctionArgs>,
    //                 };
    //             };
    //         };
    //     };
    //
    //     None::<TableFunctionArgs>
    // }

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
            empty_args.err().unwrap().to_string(),
            "Error during planning: rrf needs at least 2 search queries to fuse results."
        );

        // Call with at least 2 arguments, but one of them overrides k only
        let one_search_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(&[
            stub_scalar_function("one_search_with_k"),
            lit(1337.0f64),
        ]);
        assert!(one_search_with_k.is_err());
        assert_eq!(
            one_search_with_k.err().unwrap().to_string(),
            "Error during planning: rrf needs at least 2 search queries to fuse results."
        );

        // Call with many searches
        let mut many_search_exprs: Vec<_> = (0..100)
            .map(|i| stub_scalar_function(&format!("fn_{i}")))
            .collect::<Vec<_>>();

        let many_searches = ReciprocalRankFusionArgs::from_udtf_exprs(
            &many_search_exprs,
        );
        assert!(many_searches.is_ok());
        assert_eq!(many_searches.unwrap().search_udtf_exprs.len(), 100);

        // Call with many searches + k override
        many_search_exprs.push(lit(1337.0f64));
        let many_with_k = ReciprocalRankFusionArgs::from_udtf_exprs(
            &many_search_exprs,
        );
        assert!(many_with_k.is_ok());

        let many_with_k = many_with_k.unwrap();
        assert_eq!(many_with_k.search_udtf_exprs.len(), 100);
        assert_eq!(many_with_k.k, 1337.0f64);
    }
}
