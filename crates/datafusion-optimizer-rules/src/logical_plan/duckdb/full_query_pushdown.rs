use crate::logical_plan::duckdb::is_plan_supported;
use crate::logical_plan::duckdb::logical_pushdown_node::DuckDBLogicalPlanPushdownNode;
use datafusion::common::DataFusionError;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_expr::LogicalPlan;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, LazyLock};

pub static FULL_QUERY_PUSHDOWN_NAME: &str = "DuckDBFullQueryPushdown";

static FULL_QUERY_PUSHDOWN_SCHEMA_METADATA: LazyLock<HashMap<String, String>> =
    LazyLock::new(|| HashMap::from([(FULL_QUERY_PUSHDOWN_NAME.to_string(), "true".to_string())]));

#[derive(Debug)]
pub struct DuckDBFullQueryPushdown {}

impl DuckDBFullQueryPushdown {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }
}

impl OptimizerRule for DuckDBFullQueryPushdown {
    fn name(&self) -> &'static str {
        "DuckDBFullQueryPushdown"
    }

    // This rule does its own recursion
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let mut supported = true;
        let _ = plan.apply(|p| {
            supported = is_plan_supported(p);

            if supported {
                Ok(TreeNodeRecursion::Continue)
            } else {
                Ok(TreeNodeRecursion::Stop)
            }
        })?;

        println!("plan supported? {supported}");

        if supported {
            Ok(Transformed::new(
                DuckDBLogicalPlanPushdownNode::from_input_plan_with_metadata(
                    plan,
                    FULL_QUERY_PUSHDOWN_SCHEMA_METADATA.clone(),
                ),
                true,
                TreeNodeRecursion::Stop
            ))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
