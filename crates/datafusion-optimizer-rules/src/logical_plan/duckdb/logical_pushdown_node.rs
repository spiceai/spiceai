use datafusion::common::{DFSchema, DFSchemaRef, plan_err};
use datafusion_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

/// A generic marker denoting that the logical plan can be directly serialized into
/// a raw DuckSqlExec physical execution node
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DuckDBLogicalPlanPushdownNode {
    pub input_plan: LogicalPlan,
    schema: DFSchemaRef,
}

impl PartialOrd for DuckDBLogicalPlanPushdownNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input_plan.partial_cmp(&other.input_plan)
    }
}

impl DuckDBLogicalPlanPushdownNode {
    #[must_use]
    pub fn new(input: LogicalPlan) -> Arc<Self> {
        Arc::new(Self {
            schema: Arc::new(input.schema().as_ref().clone()),
            input_plan: input,
        })
    }

    #[must_use]
    pub fn new_with_metadata(input: LogicalPlan, metadata: HashMap<String, String>) -> Arc<Self> {
        let meta_schema =
            DFSchema::new_with_metadata(vec![], metadata.clone()).expect("Must make empty schema");
        let mut input_plan_schema = input.schema().as_ref().clone();
        input_plan_schema.merge(&meta_schema);

        Arc::new(Self {
            input_plan: input,
            schema: Arc::new(input_plan_schema),
        })
    }

    #[must_use]
    pub fn from_input_plan(plan: LogicalPlan) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: DuckDBLogicalPlanPushdownNode::new(plan),
        })
    }

    #[must_use]
    pub fn from_input_plan_with_metadata(
        plan: LogicalPlan,
        metadata: HashMap<String, String>,
    ) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: DuckDBLogicalPlanPushdownNode::new_with_metadata(plan, metadata),
        })
    }
}

impl UserDefinedLogicalNodeCore for DuckDBLogicalPlanPushdownNode {
    fn name(&self) -> &'static str {
        "DuckDBLogicalPushdownNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.input_plan.expressions()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DuckDBLogicalPushdownNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 1 {
            return plan_err!("DuckDBLogicalPushdownNode expects exactly one input");
        }
        Ok(DuckDBLogicalPlanPushdownNode {
            input_plan: inputs[0].clone(),
            schema: self.schema.clone(),
        })
    }
}
