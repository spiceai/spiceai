use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Rewriting plans generally involves changing one or more specific plan nodes. This
/// uses the plan node's address to provide a unique key for each plan node, which is
/// comparable and hashable.
///
/// Plan node pointers are decomposed to their address part to make *explicit* that they
/// are solely to be used as keys and NEVER to be dereferenced. This also avoids the issue of
/// lifetimes when storing refs.
///
/// It is the user's responsibility to understand that the refs collected in the visitor
/// belong to a single allocation of a plan.
///
/// This is a useful feature to have for more complicated plan transformations, where
/// it makes sense to implement as discrete steps:
/// - Detect whether the plan can be optimized, collect context
/// - Build a new plan
/// - Replace specific parts of a plan
///
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct PlanNodeKey {
    plan_addr: usize,
}

impl From<&dyn ExecutionPlan> for PlanNodeKey {
    fn from(plan: &dyn ExecutionPlan) -> Self {
        let p: *const dyn ExecutionPlan = plan;
        PlanNodeKey {
            plan_addr: p.addr(),
        }
    }
}

impl From<&Arc<dyn ExecutionPlan>> for PlanNodeKey {
    fn from(plan: &Arc<dyn ExecutionPlan>) -> Self {
        Self::from(plan.as_ref())
    }
}
