// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2024-2026 The Spice.ai OSS Authors

//! Differential tests for filter and projection pushdown into the Vortex scan.
//!
//! Every query runs twice over the same rows: once over a Vortex table, where the
//! filter and projection may be converted and evaluated by Vortex, and once over
//! an in-memory `DataFusion` table, which shares none of that code. The answers
//! have to match row for row. A query `DataFusion` fails has to fail over the
//! Vortex table too, with `DataFusion`'s own error, rather than return rows or fail
//! in the scan; [`FAILING`] names those queries, so an error over both tables never
//! stands in for an answer.

use datafusion::arrow::util::pretty::pretty_format_batches;

use super::tests::{insert_rows, physical_plan_display};
use crate::common_tests::TestSessionContext;

/// The columns both tables share.
const COLUMNS: &str = "id INT NOT NULL, a INT, b INT NOT NULL, p DECIMAL(15, 2) NOT NULL, \
                       r DECIMAL(10, 0) NOT NULL, s VARCHAR, f DOUBLE, g DOUBLE, \
                       w DECIMAL(40, 0)";

/// Integer extremes, zeros to divide by, a null in each nullable column, an empty
/// string, both signed zeros, and a `DECIMAL(40, 0)` (a `Decimal256`) past `BIGINT`'s
/// range.
const ROWS: &[&str] = &[
    "0, 2147483647, 0, 1.50, 0, 'xy', -0.0, 0.0, 1000000000000000000000000000000000000000",
    "1, -2147483648, 0, 0.00, 0, 'abc', 0.0, -0.0, 1",
    "2, 1, 1, 1.00, 1, NULL, 1.5, 1.5, 2",
    "3, 2, 2, 2.00, 2, 'q', -0.0, 0.0, NULL",
    "4, NULL, 0, 0.00, 0, '', NULL, 0.0, 3",
    "5, 4, 2, 2.00, 2, 'xyz', 0.0, 0.0, -5",
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
    // A `CASE` whose unselected branch overflows: `a + a` stays `INT`, and only the
    // extremes, which the `THEN` takes, overflow it.
    "CASE WHEN a > 1000 OR a < -1000 THEN 0 ELSE a + a END > 1",
    // The same guards under a cast: a `CASE` that guards a division by zero, arithmetic
    // over decimals of different types, and a comparison of two float columns.
    "CAST(CASE WHEN b <> 0 THEN 12 / b ELSE 0 END AS DOUBLE) = 6",
    "CAST(p * r AS DECIMAL(30, 2)) > 3",
    "CAST(f = g AS INT) = 1",
    // A cast from a `Decimal256` past the target's range is an error, not a row.
    "CAST(w AS BIGINT) > 0",
    // Signed zeros compare equal.
    "f = -0.0",
    "f = 0.0",
];

/// Projections whose conversion into a Vortex expression has to keep `DataFusion`'s answer.
const PROJECTIONS: &[&str] = &[
    // An alias that shares a column's name, in a projection the scan evaluates whole.
    "b AS a, a + 1 AS x",
    "a AS b, b AS a",
    // Computed expressions the scan evaluates.
    "a + b AS x",
    "b * 2 AS x",
    // Expressions Vortex has no conversion for.
    "a % 2 AS x",
    "-a AS x",
    "a + character_length(s) AS x",
    "CASE WHEN b > 0 THEN a END AS x",
    // An expression it converts, next to one it does not.
    "a + b AS x, a % 3 AS y",
    // Arithmetic over decimals of one type, whose result type `DataFusion` chooses.
    "p * p AS x",
];

/// The filters and projections above that `DataFusion` fails over [`ROWS`], each with why.
const FAILING: &[&str] = &[
    // `w` holds 10^39, past `BIGINT`'s range.
    "CAST(w AS BIGINT) > 0",
    // Negating `INT`'s minimum overflows.
    "-a AS x",
];

/// What a query returned: its rows, rendered, or the error it failed with.
#[derive(PartialEq)]
enum Answer {
    Rows(String),
    Error(String),
}

impl std::fmt::Display for Answer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rows(rows) => f.write_str(rows),
            Self::Error(error) => write!(f, "ERROR {error}"),
        }
    }
}

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

/// The rows `sql` returns, rendered, or the error it fails with.
async fn answer(ctx: &TestSessionContext, sql: &str) -> Answer {
    let batches = async { ctx.session.sql(sql).await?.collect().await }.await;
    match batches {
        Ok(batches) => Answer::Rows(
            pretty_format_batches(&batches)
                .map_or_else(|error| format!("FORMAT ERROR {error}"), |t| t.to_string()),
        ),
        Err(error) => Answer::Error(error.to_string()),
    }
}

/// Run each `(entry, query)` over both tables and return every query whose answers differ,
/// that fails over the in-memory table without being in [`FAILING`], or that is in
/// [`FAILING`] and answers. Two failures agree only on the same error.
async fn mismatches(
    ctx: &TestSessionContext,
    queries: impl IntoIterator<Item = (&'static str, String)>,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    for (entry, query) in queries {
        let vortex = answer(ctx, &query.replace("{t}", "v")).await;
        let oracle = answer(ctx, &query.replace("{t}", "m")).await;
        let expected_to_fail = FAILING.contains(&entry);
        let problem = match (&oracle, expected_to_fail) {
            (Answer::Error(_), false) => Some("the in-memory table fails it"),
            (Answer::Rows(_), true) => Some("it is in FAILING, and the in-memory table answers it"),
            _ if vortex != oracle => Some("the answers differ"),
            _ => None,
        };
        if let Some(problem) = problem {
            mismatches.push(format!(
                "{query}: {problem}\nvortex:\n{vortex}\nin-memory:\n{oracle}"
            ));
        }
    }
    mismatches
}

#[tokio::test]
async fn pushed_down_filters_return_what_datafusion_returns() -> anyhow::Result<()> {
    let ctx = tables().await?;
    let mismatches = mismatches(
        &ctx,
        FILTERS.iter().map(|filter| {
            (
                *filter,
                format!("SELECT id FROM {{t}} WHERE {filter} ORDER BY id"),
            )
        }),
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
        PROJECTIONS.iter().map(|projection| {
            (
                *projection,
                format!("SELECT id, {projection} FROM {{t}} ORDER BY id"),
            )
        }),
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

    let sum: Arc<dyn PhysicalExpr> = Arc::new(df_expr::BinaryExpr::new(
        df_expr::col("a", &schema)?,
        datafusion_expr::Operator::Plus,
        df_expr::col("b", &schema)?,
    ));
    assert!(
        pushed_down(vec![(Arc::clone(&sum), "x")])?,
        "`a + b` converts, so the projection is evaluated in the scan"
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

/// A list cast stays above the scan unless Vortex evaluates its element cast. Vortex has no
/// cast from a fixed-size list to a list, so pushing `array_length(CAST(f AS INT[]))` over a
/// `FixedSizeList` column failed the read with "No `CastKernel` to cast canonical array
/// `vortex.fixed_size_list`"; above the scan the filter answers as `DataFusion` does.
#[tokio::test]
async fn a_fixed_size_list_cast_to_a_list_stays_above_the_scan() -> anyhow::Result<()> {
    let ctx = TestSessionContext::with_nested_functions(true);
    ctx.session
        .sql(
            "COPY (SELECT 1 AS id, arrow_cast(make_array(1, 2), 'FixedSizeList(2, Int32)') AS f \
             UNION ALL SELECT 2 AS id, arrow_cast(NULL, 'FixedSizeList(2, Int32)') AS f) \
             TO '/fixed_size_list/' STORED AS vortex",
        )
        .await?
        .collect()
        .await?;
    ctx.session
        .sql("CREATE EXTERNAL TABLE fsl STORED AS vortex LOCATION '/fixed_size_list/'")
        .await?
        .collect()
        .await?;

    for (sql, expected) in [
        (
            "SELECT id FROM fsl WHERE array_length(CAST(f AS INT[])) = 2",
            vec![1],
        ),
        (
            "SELECT id FROM fsl WHERE CAST(f AS BIGINT[]) IS NOT NULL",
            vec![1],
        ),
        ("SELECT id FROM fsl WHERE CAST(f AS INT[]) IS NULL", vec![2]),
    ] {
        let batches = ctx.session.sql(sql).await?.collect().await?;
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                datafusion::arrow::compute::cast(batch.column(0), &arrow_schema::DataType::Int64)
                    .expect("ids cast to Int64")
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .expect("Int64 ids")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, expected, "{sql}");
    }
    Ok(())
}
