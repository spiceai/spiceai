use crate::concrete;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, DataFusionError};
use datafusion::datasource::{TableProvider, source_as_provider};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{Expr, Extension, LogicalPlan, TableScan, UserDefinedLogicalNodeCore};
use datafusion_federation::FederatedTableProviderAdaptor;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

// https://duckdb.org/docs/stable/sql/functions/aggregates
// https://datafusion.apache.org/user-guide/sql/aggregate_functions.html
static SUPPORTED_AGG_FUNCTIONS_LIST: [&str; 30] = [
    // Basic aggregates
    "avg",
    "count",
    "max",
    "min",
    "sum",
    // Bitwise aggregates
    "bit_and",
    "bit_or",
    "bit_xor",
    // Boolean aggregates
    "bool_and",
    "bool_or",
    // String aggregates
    "string_agg",
    // Statistical aggregates
    "corr",
    "covar_pop",
    "covar_samp",
    "median",
    "stddev_pop",
    "stddev_samp",
    "var_pop",
    "var_samp",
    // Regression aggregates
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    // Percentile/quantile aggregates
    "quantile_cont",
    // Approximate aggregates
    "approx_percentile_cont",
];

static SUPPORTED_AGG_FUNCTIONS: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from(SUPPORTED_AGG_FUNCTIONS_LIST));

/// This looks for opportunities in the expressed logical plan to push down aggregates
/// directly into the SQL execution for DuckDB accelerated table providers (as indicated by `spice.accelerator`).
///
/// Schema metadata was chosen to "tag" scans in order to avoid a dependency on the runtime crate and
/// concrete adapter types.
#[derive(Debug)]
pub struct DuckDBAggregateLogicalPushdown {}

impl DuckDBAggregateLogicalPushdown {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {})
    }

    fn is_duckdb_provider(scan: &TableScan) -> Result<bool> {
        let provider = source_as_provider(&scan.source)?;
        let Some(fed_adapter) = concrete!(provider, FederatedTableProviderAdaptor) else {
            return Ok(false);
        };

        Ok(matches!(
            fed_adapter
                .schema()
                .metadata
                .get("spice.accelerator")
                .map(|s| s.as_str()),
            Some("duckdb")
        ))
    }

    /// If this aggregate's root scan is from a DuckDB accelerated source, with supported expressions,
    /// wrap it in a marker node for pushdown rewriting during physical planning
    fn try_mark_pushdown(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        // Find an aggregate node
        let LogicalPlan::Aggregate(agg) = plan else {
            return Ok(None);
        };

        // Validate its agg expressions to make sure they are supported
        for expr in &agg.aggr_expr {
            match expr {
                Expr::AggregateFunction(AggregateFunction { func, .. })
                    if SUPPORTED_AGG_FUNCTIONS.contains(func.name()) =>
                {
                    continue;
                }
                _ => return Ok(None),
            }
        }

        // Scan its children to ensure that there is a unary chain to an accelerated
        // DuckDB provider
        let mut found = false;

        let _ = plan.apply(|p| match p {
            LogicalPlan::TableScan(table_scan) if Self::is_duckdb_provider(&table_scan)? => {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            }
            other if other.inputs().len() > 1 => Ok(TreeNodeRecursion::Stop),
            _ => Ok(TreeNodeRecursion::Continue),
        })?;

        if found {
            Ok(Some(LogicalPlan::Extension(Extension {
                node: DuckDBAggregatePushdownNode::new(plan.clone()),
            })))
        } else {
            Ok(None)
        }
    }

    /// Try to find a unary path to a marker node from the current node, then swap it
    fn try_percolate_marker_node(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let with_erased_marker = plan.clone().transform_down(|p| {
            if p.inputs().len() > 1 {
                return Ok(Transformed::new(p, false, TreeNodeRecursion::Stop));
            }

            let LogicalPlan::Extension(ref ext) = p else {
                return Ok(Transformed::no(p));
            };

            let Some(marker) = ext
                .node
                .as_any()
                .downcast_ref::<DuckDBAggregatePushdownNode>()
            else {
                return Ok(Transformed::no(p));
            };

            Ok(Transformed::new(
                marker.input_plan.clone(),
                true,
                TreeNodeRecursion::Jump,
            ))
        })?;

        if with_erased_marker.transformed {
            Ok(Some(LogicalPlan::Extension(Extension {
                node: DuckDBAggregatePushdownNode::new(with_erased_marker.data),
            })))
        } else {
            Ok(None)
        }
    }
}

impl OptimizerRule for DuckDBAggregateLogicalPushdown {
    fn name(&self) -> &str {
        "DuckDBAggregatePushdownOptimizerRule"
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
        // Mark all eligible nodes for DuckDB agg pushdown
        let maybe_marked_agg = plan.transform_down(|p| {
            match &p {
                // Skip already marked subtrees
                LogicalPlan::Extension(ext)
                    if concrete!(ext.node, DuckDBAggregatePushdownNode).is_some() =>
                {
                    return Ok(Transformed::new(p, false, TreeNodeRecursion::Jump));
                }
                _ => { /* no-op */ }
            };

            if let Some(marked_for_pushdown) = Self::try_mark_pushdown(&p)? {
                Ok(Transformed::new(
                    marked_for_pushdown,
                    true,
                    TreeNodeRecursion::Jump,
                ))
            } else {
                Ok(Transformed::no(p))
            }
        })?;

        // If we didn't rewrite, bail out early
        if !maybe_marked_agg.transformed {
            return Ok(maybe_marked_agg);
        }

        // Try to push as much of the physical plan under the pushdown marker as possible
        let rewritten_plan = maybe_marked_agg.data;
        rewritten_plan.transform_down(|p| {
            if let Some(percolated) = Self::try_percolate_marker_node(&p)? {
                Ok(Transformed::new(percolated, true, TreeNodeRecursion::Jump))
            } else {
                Ok(Transformed::no(p))
            }
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DuckDBAggregatePushdownNode {
    pub input_plan: LogicalPlan,
}

impl PartialOrd for DuckDBAggregatePushdownNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input_plan.partial_cmp(&other.input_plan)
    }
}

impl DuckDBAggregatePushdownNode {
    pub fn new(input: LogicalPlan) -> Arc<Self> {
        Arc::new(Self { input_plan: input })
    }
}

impl UserDefinedLogicalNodeCore for DuckDBAggregatePushdownNode {
    fn name(&self) -> &str {
        "DuckDBAggregatePushdownNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input_plan]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input_plan.schema()
    }

    fn expressions(&self) -> Vec<datafusion_expr::Expr> {
        self.input_plan.expressions()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DuckDBAggregatePushdownNode")
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "DuckDBAggregatePushdownNode is unary");
        Ok(DuckDBAggregatePushdownNode {
            input_plan: inputs[0].clone(),
        })
    }
}
