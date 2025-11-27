//! Utility functions to enable federation support for scalar functions.
//!
//! Helpful for implementing [`SQLExecutor::can_execute_plan`], when federating Datafusion Scalar UDFs with a `SQLExecutor` that may not support them.

use std::sync::Arc;

use datafusion::{
    catalog::Session,
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::DataFusionError,
    logical_expr::{
        expr::{AggregateFunction, ScalarFunction},
        AggregateUDF, Expr, LogicalPlan, ScalarUDF, WindowFunctionDefinition, WindowUDF,
    },
};

/// Returns whether any [`ScalarFunction`], [`AggregateFunction`] or [`WindowFunction`]s in the [`LogicalPlan`] are unsupported.
///
/// # Arguments
/// * `plan` - The logical plan to check for scalar functions
/// * `supports` - The support policy (allow-list or deny-list)
///
/// # Returns
/// * `Ok(true)` if there are unsupported scalar functions in the plan
/// * `Ok(false)` if all scalar functions are supported
/// * `Err(DataFusionError)` if an error occurs during traversal
pub fn contains_unsupported_functions(
    plan: &LogicalPlan,
    sup: &FunctionSupport,
) -> Result<bool, DataFusionError> {
    plan.exists(|plan| {
        Ok(plan.expressions().into_iter().any(|expr| {
            let mut found_unsupported = false;
            let _ = expr.apply(|expr| {
                if sup.supports(expr) {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    found_unsupported = true;
                    Ok(TreeNodeRecursion::Stop)
                }
            });
            found_unsupported
        }))
    })
}

#[derive(Clone, Debug)]
pub struct FunctionSupport {
    scalar: Option<FunctionRestriction>,
    window: Option<FunctionRestriction>,
    aggregate: Option<FunctionRestriction>,
}

impl FunctionSupport {
    pub fn new(
        scalar: Option<FunctionRestriction>,
        window: Option<FunctionRestriction>,
        aggregate: Option<FunctionRestriction>,
    ) -> Self {
        Self {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn deny_all_from(sess: &dyn Session) -> Self {
        let scalar = Some(FunctionRestriction::Deny(
            sess.scalar_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let window = Some(FunctionRestriction::Deny(
            sess.window_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let aggregate = Some(FunctionRestriction::Deny(
            sess.aggregate_functions()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        ));

        FunctionSupport {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn support_all_from(sess: &dyn Session) -> Self {
        let scalar = Some(FunctionRestriction::Allow(
            sess.scalar_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let window = Some(FunctionRestriction::Allow(
            sess.window_functions().keys().cloned().collect::<Vec<_>>(),
        ));
        let aggregate = Some(FunctionRestriction::Allow(
            sess.aggregate_functions()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
        ));

        FunctionSupport {
            scalar,
            window,
            aggregate,
        }
    }

    pub fn supports(&self, expr: &Expr) -> bool {
        let mut supports = true;
        let _ = expr.apply(|e| {
            let support_child = match e {
                Expr::ScalarFunction(ScalarFunction { func, .. }) => self.supports_scalar(func),
                Expr::AggregateFunction(AggregateFunction { func, .. }) => {
                    self.supports_aggregate(func)
                }
                Expr::WindowFunction(wind) => match &wind.fun {
                    WindowFunctionDefinition::AggregateUDF(func) => self.supports_aggregate(func),
                    WindowFunctionDefinition::WindowUDF(func) => self.supports_window(func),
                },
                _ => true,
            };
            if !support_child {
                supports = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        supports
    }

    pub fn supports_window(&self, fnc: &Arc<WindowUDF>) -> bool {
        self.window
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
    pub fn supports_scalar(&self, fnc: &Arc<ScalarUDF>) -> bool {
        self.scalar
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
    pub fn supports_aggregate(&self, fnc: &Arc<AggregateUDF>) -> bool {
        self.aggregate
            .as_ref()
            .map(|restriction| restriction.supports(&fnc.name().to_string()))
            .unwrap_or(true)
    }
}

#[derive(Clone, Debug)]
pub enum FunctionRestriction {
    Allow(Vec<String>),
    Deny(Vec<String>),
}

impl FunctionRestriction {
    fn supports(&self, name: &String) -> bool {
        match self {
            Self::Allow(allowed) => allowed.contains(name),
            Self::Deny(denied) => !denied.contains(name),
        }
    }
}
