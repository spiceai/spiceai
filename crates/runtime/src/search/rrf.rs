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
use datafusion::prelude::{DataFrame, SessionContext, coalesce, make_array, sha224};
use datafusion::sql::sqlparser::ast::Expr as SqlExpr;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion_expr::{ExprFunctionExt, ExprSchemable, UserDefinedLogicalNode, col, lit};
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
            .map(|expr| match expr {
                e @ Expr::ScalarFunction(_) => unparser.expr_to_sql(&e),
                other_expr => Err(DataFusionError::NotImplemented(format!(
                    "{RRF_UDF_NAME} does not yet support {other_expr} arguments."
                ))),
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
    use crate::Runtime;
    use crate::builder::RuntimeBuilder;
    use crate::datafusion::udf::register_udfs;
    use std::sync::Arc;

    async fn test_runtime() -> datafusion::common::Result<Runtime> {
        use crate::embeddings::table::EmbeddingTable;
        use crate::model::EmbeddingModelStore;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::datasource::MemTable;
        use std::collections::HashMap;
        use std::sync::Arc;
        use tokio::sync::RwLock;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
            Field::new(
                "content_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 32),
                true,
            ),
        ]));

        let mut embedded_columns = HashMap::new();
        embedded_columns.insert(
            "content".to_string(),
            crate::embeddings::table::EmbeddingColumnConfig {
                model_name: "test_model".to_string(),
                vector_size: 32,
                in_base_table: true,
                chunker: None,
            },
        );

        let mem_table = Arc::new(MemTable::try_new(schema, vec![])?);
        let embedding_table = Arc::new(EmbeddingTable {
            base_table: mem_table,
            embedded_columns,
            embedding_models: Arc::new(RwLock::new(EmbeddingModelStore::default())),
        });

        let rt = RuntimeBuilder::new().build().await;
        register_udfs(&rt);

        rt.df
            .ctx
            .register_table("foo", embedding_table)
            .expect("Failed to register foo table");

        Ok(rt)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rrf_rewriting() {
        let runtime = test_runtime().await.expect("Failed to create test runtime");
        let query = "select * from rrf(vector_search(foo, 'bar'), vector_search(foo, 'bar'))";
        let ctx = Arc::clone(&runtime.df.ctx);

        let df = ctx.sql(query).await.expect("Must parse query");
        let df_schema = Arc::clone(&df.schema().inner());
        let plan = df.into_optimized_plan().unwrap();

        println!("plan: {plan}");
        println!("plan schema {}", plan.schema());
        println!("df schema {}", *df_schema);
    }
}
