use datafusion::datasource::source_as_provider;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::{Expr, LogicalPlan, Projection, TableScan};
use std::collections::HashSet;
use std::sync::LazyLock;

pub mod aggregate_pushdown;
pub mod full_query_pushdown;
pub mod logical_pushdown_node;
pub mod planner;

pub(crate) const SPICE_ACCELERATOR_METADATA_KEY: &str = "spice.accelerator";

// https://duckdb.org/docs/stable/sql/functions/aggregates
// https://datafusion.apache.org/user-guide/sql/aggregate_functions.html
static SUPPORTED_AGG_FUNCTIONS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
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
    ])
});

static SPICE_UNSUPPORTED_UDF: LazyLock<HashSet<&str>> = LazyLock::new(|| {
    HashSet::from([
        "rand",
        "bucket",
        "cosine_distance",
        "truncate_scalar",
        "embed",
        "ai",
        "digest_many",
    ])
});

pub fn is_duckdb_provider(scan: &TableScan) -> datafusion::common::Result<bool> {
    let provider = source_as_provider(&scan.source)?;

    Ok(matches!(
        provider
            .schema()
            .metadata
            .get(SPICE_ACCELERATOR_METADATA_KEY)
            .map(String::as_str),
        Some("duckdb")
    ))
}

pub fn is_projection_supported(proj: &Projection) -> bool {
    proj.expr.iter().all(|e| match e {
        Expr::ScalarFunction(f) => !SPICE_UNSUPPORTED_UDF.contains(f.name()),
        _ => true,
    })
}

pub fn is_plan_supported(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => is_aggregate_plan_supported(&plan),
        LogicalPlan::TableScan(scan) => is_duckdb_provider(scan).unwrap_or(false),
        LogicalPlan::Projection(proj) => is_projection_supported(proj),
        LogicalPlan::DescribeTable(_) => false,
        LogicalPlan::Analyze(_) => false,
        LogicalPlan::Extension(_) => false,
        _ => true,
    }
}

pub fn is_aggregate_plan_supported(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Aggregate(agg) = plan else {
        return false;
    };

    agg.aggr_expr.iter().all(|e| match e {
        Expr::AggregateFunction(AggregateFunction { func, .. }) => {
            SUPPORTED_AGG_FUNCTIONS.contains(func.name())
        }
        _ => false,
    })
}
