use crate::logical_plan::duckdb::aggregate_pushdown::DuckDBAggregatePushdownNode;
use crate::physical_plan::duckdb::aggregate_pushdown::DuckDBAggregatePushdownMarkerExec;
use async_trait::async_trait;
use datafusion::common::{Result, plan_err};
use datafusion::execution::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use std::sync::Arc;

pub struct DuckDBLogicalExtensionPlanner {}

impl DuckDBLogicalExtensionPlanner {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(DuckDBLogicalExtensionPlanner {})
    }
}

#[async_trait]
impl ExtensionPlanner for DuckDBLogicalExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(logical_marker) = node.as_any().downcast_ref::<DuckDBAggregatePushdownNode>() {
            if physical_inputs.len() != 1 {
                return plan_err!("DuckDBAggregatePushdownNode expects exactly one input");
            }
            Ok(Some(DuckDBAggregatePushdownMarkerExec::new(
                logical_marker.input_plan.clone(),
                Arc::clone(&physical_inputs[0]),
            )))
        } else {
            Ok(None)
        }
    }
}
