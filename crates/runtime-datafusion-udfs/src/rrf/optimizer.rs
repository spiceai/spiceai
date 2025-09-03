use crate::rrf::udf::ReciprocalRankFusion;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

#[derive(Debug, Default)]
pub struct ReciprocalRankUDFRewriteRule {}

impl OptimizerRule for ReciprocalRankUDFRewriteRule {
    fn name(&self) -> &'static str {
        "rrf_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>, DataFusionError> {
        // Although the casting is ugly, this makes it unambiguous that this is our RRF node
        let should_rewrite = plan.exists(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                let should_rewrite = scan
                    .source
                    .as_any()
                    .downcast_ref::<DefaultTableSource>()
                    .and_then(|dts| {
                        dts.table_provider
                            .as_any()
                            .downcast_ref::<ReciprocalRankFusion>()
                    })
                    .is_some();
                Ok(should_rewrite)
            } else {
                Ok(false)
            }
        })?;

        if !should_rewrite {
            return Ok(Transformed::no(plan));
        }

        println!("got plan {plan}");
        Ok(Transformed::no(plan))
    }
}

#[cfg(test)]
mod tests {
    use crate::rrf::optimizer::ReciprocalRankUDFRewriteRule;
    use crate::rrf::udf::ReciprocalRankFusion;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn test_context() -> SessionContext {
        let ctx = SessionContext::default();
        ctx.register_udf(ReciprocalRankFusion::default().into());
        ctx.register_udtf("rrf", Arc::new(ReciprocalRankFusion::default()));
        ctx.add_optimizer_rule(Arc::new(ReciprocalRankUDFRewriteRule::default()));
        ctx
    }

    #[tokio::test]
    async fn test_rrf_rewriting() {
        let query = "select * from rrf(query_a, query_b)";
        let ctx = test_context();

        let df = ctx.sql(query).await.expect("Must parse query");
        let plan = df.into_optimized_plan().unwrap();
        println!("plan {plan}");
    }
}
