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
use datafusion::prelude::{coalesce, DataFrame, SessionContext};
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion::sql::unparser::Unparser;
use datafusion_expr::{col, lit, ExprFunctionExt, UserDefinedLogicalNode};
use futures::future::join_all;
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

/// A no-op UDTF detected by an Optimizer that subsequently implements RRF using plain SQL
pub struct ReciprocalRankFusion {
    pub session_context: Arc<SessionContext>,
    df: Option<DataFrame>,
}

impl Debug for ReciprocalRankFusion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReciprocalRankFusion")
    }
}

impl ReciprocalRankFusion {
    pub fn from_ctx(session_context: Arc<SessionContext>) -> Self {
        Self {
            session_context,
            df: None
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

    fn default_error<T>() -> Result<T, DataFusionError> {
        exec_err!("This is a bug! {RRF_UDF_NAME} should be rewritten by an optimizer rule.")
    }

    fn args_to_df(&self, args: &[Expr]) -> Result<DataFrame> {
        let args: Vec<_> = args.iter().cloned().collect();
        // Find user-provided smoothing param if provided
        let k = if let Some(Expr::Literal(ScalarValue::UInt64(Some(k)), ..)) = args.last() {
            *k
        } else {
            // Supposedly the best magic number
            60
        };

        // Unparse UDTF invocations
        // TODO: DF support for nested UDTF calls without ScalarUDF "hack"
        let unparser = Unparser::new(&DefaultDialect {});
        let search_udtf_invocations: Vec<String> = args
            .iter()
            .filter_map(|expr| match expr {
                // TODO: score is "Spice-standard", but id is not
                e @ Expr::ScalarFunction(_) => unparser
                    .expr_to_sql(&e)
                    .map(|e| format!("select * from {e}"))
                    .ok(),
                _ => None,
            })
            .collect();

        if search_udtf_invocations.len() < 2 {
            return Err(DataFusionError::Plan(format!(
                "{RRF_UDF_NAME} called with less than 2 search queries."
            )));
        }

        let search_df_futures: Vec<_> = search_udtf_invocations
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
            .map(|(i, df)| Self::ranked_and_aliased_df_projection(df, i))
            .collect::<Result<Vec<_>>>()?;

        // TODO: assumes homogenous projections, and is frankly insane
        let mut columns: Vec<_> = search_dfs[0].schema().columns().iter().filter_map(|c| match c.name.as_str() {
            "id" | "rank" | "score" => None,
            other if other.contains("embedding") => None,
            other => Some(
                coalesce((0..search_dfs.len()).map(|i| col(format!("search_{i}.{other}"))).collect())
                    .alias(other)
            ),
        }).collect();

        let id_expr = coalesce(
            (0..search_dfs.len())
                .map(|i| col(format!("search_{i}.id")))
                .collect(),
        )
            .alias("id");
        let score_expr = coalesce(
            (0..search_dfs.len())
                .map(|i| {
                    coalesce(vec![
                        lit(1u64) / (lit(k) + col(format!("search_{}.rank", i))),
                        lit(0u64),
                    ])
                })
                .collect(),
        )
            .alias("fused_score");

        columns.insert(0, id_expr);
        columns.insert(1, score_expr);

        search_dfs
            .into_iter()
            .reduce(|a, b| {
                a.join(b, JoinType::Full, &["id"], &["id"], None)
                    .expect("Must join")
            })
            .expect("Must have joined DF")
            .select(columns)?
            .sort(vec![col("fused_score").sort(false, false)])
    }

    fn ranked_and_aliased_df_projection(df: DataFrame, index: usize) -> Result<DataFrame> {
        let rank_expr = row_number()
            .order_by(vec![col("score").sort(false, false)])
            .build()?
            .alias("rank");

        df.window(vec![rank_expr])?
            .alias(&format!("search_{index}"))
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
        Self::default_error()
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        Self::default_error()
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&*DOCUMENTATION)
    }
}

impl TableFunctionImpl for ReciprocalRankFusion {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let rrf = ReciprocalRankFusion::from_ctx(Arc::clone(&self.session_context));
        let df = rrf.args_to_df(args)?;
        Ok(Arc::new(rrf.with_df(df)))
    }
}

#[async_trait]
impl TableProvider for ReciprocalRankFusion {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(self.df.as_ref().expect("ReciprocalRankFusion must have a schema").schema().inner())
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
    use crate::Runtime;
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
