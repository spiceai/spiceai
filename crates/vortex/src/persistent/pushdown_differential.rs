// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2024-2026 The Spice.ai OSS Authors

//! Differential tests for filter and projection pushdown into the Vortex scan.
//!
//! Every query runs twice over the same rows: once over a Vortex table, where the
//! filter and projection may be converted and evaluated by Vortex, and once over
//! an in-memory `DataFusion` table, which shares none of that code. The answers
//! have to match row for row. A query `DataFusion` fails has to fail over the
//! Vortex table too, rather than return rows.

use datafusion::arrow::util::pretty::pretty_format_batches;

use super::tests::{insert_rows, physical_plan_display};
use crate::common_tests::TestSessionContext;

/// The columns both tables share.
const COLUMNS: &str = "id INT NOT NULL, a INT, b INT NOT NULL, p DECIMAL(15, 2) NOT NULL, \
                       r DECIMAL(10, 0) NOT NULL, s VARCHAR, f DOUBLE";

/// Integer extremes, zeros to divide by, a null in each nullable column, an empty
/// string, and both signed zeros.
const ROWS: &[&str] = &[
    "0, 2147483647, 0, 1.50, 0, 'xy', -0.0",
    "1, -2147483648, 0, 0.00, 0, 'abc', 0.0",
    "2, 1, 1, 1.00, 1, NULL, 1.5",
    "3, 2, 2, 2.00, 2, 'q', -0.0",
    "4, NULL, 0, 0.00, 0, '', NULL",
    "5, 4, 2, 2.00, 2, 'xyz', 0.0",
];

/// Filters whose conversion into a Vortex predicate has to keep `DataFusion`'s answer.
const FILTERS: &[&str] = &[
    // An `IN` list holding a column is not a constant list.
    "a IN (b, 4, 5, 6, 7, 8)",
    "a NOT IN (b, 4, 5, 6, 7, 8)",
    // SQL's three-valued `IN`: a null needle, and a null in the list.
    "a IN (1, 2, 4, 5)",
    "a NOT IN (1, 2, 4, 5)",
    "NOT (a IN (1, 2, 4, 5))",
    "a IN (1, 2, 4, NULL)",
    "a NOT IN (1, 2, 4, NULL)",
    "s IN ('xy', 'q', 'zz', 'abc')",
    "s NOT IN ('xy', 'q', 'zz', 'abc')",
    // A `CASE` that guards a division by zero.
    "CASE WHEN b <> 0 THEN 12 / b ELSE 0 END = 6",
    "CASE WHEN b = 0 THEN 0 ELSE 12 / b END = 6",
    // Arithmetic over operands of different decimal types.
    "p * 2 > 3",
    "p * r > 3",
    "p + r > 3",
    "p - r > 0",
    "p * p > 3",
    // Comparisons across decimal types, and integer arithmetic.
    "p > r",
    "p = r",
    "a + b > 3",
    // A `CASE` whose unselected branch overflows.
    "CASE WHEN a > 0 THEN a ELSE a * 2 END > 1",
    // Signed zeros compare equal.
    "f = -0.0",
    "f = 0.0",
];

/// Projections whose conversion into a Vortex expression has to keep `DataFusion`'s answer.
const PROJECTIONS: &[&str] = &[
    // An alias that shares a column's name.
    "b AS a, abs(a) AS x",
    "a AS b, b AS a",
    // Expressions Vortex has no conversion for.
    "a % 2 AS x",
    "-a AS x",
    "a + character_length(s) AS x",
    "CASE WHEN b > 0 THEN a END AS x",
    // Expressions it does convert, next to ones it does not.
    "abs(a) AS x, a % 3 AS y",
];

/// Create the Vortex table `v` and the in-memory table `m` holding [`ROWS`].
async fn tables() -> anyhow::Result<TestSessionContext> {
    let ctx = TestSessionContext::new(true);
    ctx.session
        .sql(&format!(
            "CREATE EXTERNAL TABLE v ({COLUMNS}) STORED AS vortex LOCATION '/differential/'"
        ))
        .await?
        .collect()
        .await?;
    ctx.session
        .sql(&format!("CREATE TABLE m ({COLUMNS})"))
        .await?
        .collect()
        .await?;
    for table in ["v", "m"] {
        insert_rows(&ctx, table, ROWS).await?;
    }
    Ok(ctx)
}

/// The rows `sql` returns, rendered, or `ERROR` if it fails.
async fn answer(ctx: &TestSessionContext, sql: &str) -> String {
    let batches = async { ctx.session.sql(sql).await?.collect().await }.await;
    match batches {
        Ok(batches) => pretty_format_batches(&batches)
            .map_or_else(|error| format!("FORMAT ERROR {error}"), |t| t.to_string()),
        Err(_) => "ERROR".to_string(),
    }
}

/// Run each query over both tables and return every query whose answers differ.
async fn mismatches(
    ctx: &TestSessionContext,
    queries: impl IntoIterator<Item = String>,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for query in queries {
        let vortex = answer(ctx, &query.replace("{t}", "v")).await;
        let oracle = answer(ctx, &query.replace("{t}", "m")).await;
        if vortex != oracle {
            mismatches.push(format!("{query}\nvortex:\n{vortex}\nin-memory:\n{oracle}"));
        }
    }
    mismatches
}

#[tokio::test]
async fn pushed_down_filters_return_what_datafusion_returns() -> anyhow::Result<()> {
    let ctx = tables().await?;
    let mismatches = mismatches(
        &ctx,
        FILTERS
            .iter()
            .map(|filter| format!("SELECT id FROM {{t}} WHERE {filter} ORDER BY id")),
    )
    .await;
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
    Ok(())
}

#[tokio::test]
async fn pushed_down_projections_return_what_datafusion_returns() -> anyhow::Result<()> {
    let ctx = tables().await?;
    let mismatches = mismatches(
        &ctx,
        PROJECTIONS
            .iter()
            .map(|projection| format!("SELECT id, {projection} FROM {{t}} ORDER BY id")),
    )
    .await;
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n\n"));
    Ok(())
}

/// The filters above have to reach the Vortex scan, or the comparison checks only
/// `DataFusion` against itself.
#[tokio::test]
async fn the_differential_filters_reach_the_vortex_scan() -> anyhow::Result<()> {
    let ctx = tables().await?;
    let mut pushed = Vec::new();
    for filter in FILTERS {
        let plan = physical_plan_display(&ctx.session, &format!("SELECT id FROM v WHERE {filter}"))
            .await?;
        if plan.contains("predicate:") {
            pushed.push(*filter);
        }
    }
    for expected in [
        "a IN (1, 2, 4, 5)",
        "a NOT IN (1, 2, 4, 5)",
        "s IN ('xy', 'q', 'zz', 'abc')",
        "p * p > 3",
        "f = -0.0",
    ] {
        assert!(
            pushed.contains(&expected),
            "{expected} has to reach the scan; pushed: {pushed:?}"
        );
    }
    Ok(())
}

/// The projection comparison above has to cover both decisions: a projection whose every
/// expression converts is evaluated in the scan, and one holding an expression that does not
/// is evaluated above it whole.
#[test]
fn projections_are_pushed_whole_or_not_at_all() -> anyhow::Result<()> {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
    use datafusion_physical_plan::expressions as df_expr;

    use crate::convert::exprs::{DefaultExpressionConvertor, ExpressionConvertor};

    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
    ]);
    let convertor = DefaultExpressionConvertor::default();
    let pushed_down = |exprs: Vec<(Arc<dyn PhysicalExpr>, &str)>| -> anyhow::Result<bool> {
        let projection = ProjectionExprs::new(
            exprs
                .into_iter()
                .map(|(expr, alias)| ProjectionExpr::new(expr, alias)),
        );
        let output = projection.project_schema(&schema)?;
        Ok(convertor
            .split_projection(projection, &schema, &output)?
            .pushed_down)
    };

    assert!(
        pushed_down(vec![
            (df_expr::col("b", &schema)?, "a"),
            (df_expr::col("a", &schema)?, "b"),
        ])?,
        "renamed columns convert, so the projection is evaluated in the scan"
    );

    let modulo: Arc<dyn PhysicalExpr> = Arc::new(df_expr::BinaryExpr::new(
        df_expr::col("a", &schema)?,
        datafusion_expr::Operator::Modulo,
        Arc::new(df_expr::Literal::new(2_i32.into())),
    ));
    assert!(
        !pushed_down(vec![(df_expr::col("b", &schema)?, "a"), (modulo, "x")])?,
        "`%` does not convert, so the whole projection stays above the scan"
    );
    Ok(())
}
