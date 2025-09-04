use crate::search::rrf::udf::{RRF_UDF_NAME, ReciprocalRankFusion};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{AnalyzerRule, OptimizerConfig, OptimizerRule};
use datafusion::prelude::{DataFrame, coalesce};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion_expr::{ExprFunctionExt, JoinType, col, lit};
use futures::future::join_all;
use tokio::runtime::Handle;
use tokio::task;

#[derive(Debug, Default)]
pub struct ReciprocalRankUDFRewriteRule {}

impl ReciprocalRankUDFRewriteRule {
    fn rrf_to_plan(plan: &ReciprocalRankFusion) -> Result<Transformed<LogicalPlan>> {
        // Find user-provided smoothing param if provided
        let k = if let Some(Expr::Literal(ScalarValue::Int64(Some(k)), ..)) = &plan.args.last() {
            *k
        } else {
            // Supposedly the best magic number
            60
        };

        // Unparse UDTF invocations
        // TODO: DF support for nested UDTF calls without ScalarUDF "hack"
        let unparser = Unparser::new(&DefaultDialect {});
        let search_udtf_invocations: Vec<String> = plan
            .args
            .iter()
            .filter_map(|expr| match expr {
                // TODO: score is "Spice-standard", but id is not
                e @ Expr::ScalarFunction(_) => unparser
                    .expr_to_sql(&e)
                    .map(|e| format!("select id, score from {e}"))
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
            .map(|sql| plan.session_context.sql(sql))
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
                        lit(1.0) / (lit(k) + col(format!("search_{}.rank", i))),
                        lit(0.0),
                    ])
                })
                .collect(),
        )
        .alias("fused_score");

        let joined_df = search_dfs
            .into_iter()
            .reduce(|a, b| {
                a.join(b, JoinType::Full, &["id"], &["id"], None)
                    .expect("Must join")
            })
            .expect("Must have joined DF")
            .select(vec![id_expr, score_expr])?
            .sort(vec![col("fused_score").sort(false, false)])?;

        joined_df.into_optimized_plan().map(Transformed::yes)
    }

    fn ranked_and_aliased_df_projection(df: DataFrame, index: usize) -> Result<DataFrame> {
        let rank_expr = row_number()
            .order_by(vec![col("score").sort(false, true)])
            .build()?
            .alias("rank");

        df.window(vec![rank_expr])?
            .alias(&format!("search_{index}"))
    }
}

impl AnalyzerRule for ReciprocalRankUDFRewriteRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_down(|node| {
            if let LogicalPlan::TableScan(scan) = &node {
                // Although the casting is ugly, this makes it unambiguous that this is our RRF node
                scan.source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                    .and_then(|dts| {
                        dts.table_provider
                            .as_any()
                            .downcast_ref::<ReciprocalRankFusion>()
                    })
                    .map(Self::rrf_to_plan)
                    .unwrap_or(Ok(Transformed::no(node)))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|tp| tp.data)
    }

    fn name(&self) -> &'static str {
        "rrf_rewrite"
    }
}

#[cfg(test)]
mod tests {
    use crate::Runtime;
    use crate::builder::RuntimeBuilder;
    use crate::datafusion::udf::register_udfs;
    use crate::search::rrf::optimizer::ReciprocalRankUDFRewriteRule;
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
        rt.df
            .ctx
            .add_analyzer_rule(Arc::new(ReciprocalRankUDFRewriteRule::default()));
        Ok(rt)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rrf_rewriting() {
        let runtime = test_runtime().await.expect("Failed to create test runtime");
        let query = "select * from rrf(vector_search(foo, 'bar'), vector_search(foo, 'bar'))";
        let ctx = Arc::clone(&runtime.df.ctx);

        let df = ctx.sql(query).await.expect("Must parse query");
        let plan = df.into_optimized_plan().unwrap();
        println!("{plan}");
    }
}
