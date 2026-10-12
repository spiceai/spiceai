/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Moves a row limit below the projection where federation stops, so the
//! source applies it.
//!
//! A source refuses a sub-plan that calls a function it cannot run, and the
//! federation analyzer then federates the largest sub-plans beneath it. The
//! `LIMIT` of `SELECT json_get_str(doc, 'a') FROM t LIMIT 3` sits above the
//! projection that calls the function, so without this rule the source is
//! asked for every row and the limit is applied locally.
//!
//! A projection computes exactly one output row from each input row, so taking
//! the first rows before it or after it selects the same rows: the `Limit`
//! moves beneath the projection. Over a `Sort` (`ORDER BY id LIMIT 3`), a copy
//! of the sort that fetches the rows the limit needs is placed beneath the
//! projection when every sort key is a column the projection passes through
//! unchanged; the original sort and limit stay above it to order and count the
//! rows that come back.
//!
//! The source returns the limited rows as a derived table that the federated
//! statement reads back by column name, so beneath the limit only the columns
//! the projection reads are kept, and the limit stays where it is when two of
//! them share a name (`t.id` and `u.id` over a join).
//!
//! A plan the source accepts whole is left as written.

use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::{Column, Result, tree_node::Transformed},
    logical_expr::{
        Expr, Limit, LogicalPlan, Projection, Sort, SortExpr,
        expr::Alias,
        logical_plan::{FetchType, SkipType},
    },
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

use super::correlated_filter_push_down::federates_whole;

/// Moves a `Limit`, or a copy of the `Sort` beneath it, below a projection the
/// source refuses over an input it accepts. See the module documentation.
#[derive(Default, Debug)]
pub struct PushLimitBelowProjection;

impl OptimizerRule for PushLimitBelowProjection {
    fn name(&self) -> &'static str {
        "federation_push_limit_below_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(limit) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let (SkipType::Literal(skip), FetchType::Literal(Some(fetch))) =
            (limit.get_skip_type()?, limit.get_fetch_type()?)
        else {
            return Ok(Transformed::no(plan));
        };
        if federates_whole(&plan)? {
            return Ok(Transformed::no(plan));
        }

        let rewritten = match limit.input.as_ref() {
            LogicalPlan::Projection(projection) => limit_below_projection(limit, projection)?,
            LogicalPlan::Sort(sort) => {
                let rows = sort
                    .fetch
                    .map_or(skip + fetch, |sort_fetch| sort_fetch.min(skip + fetch));
                sort_below_projection(sort, rows)?.map(|sort| {
                    LogicalPlan::Limit(Limit {
                        input: Arc::new(LogicalPlan::Sort(sort)),
                        ..limit.clone()
                    })
                })
            }
            _ => None,
        };
        Ok(rewritten.map_or_else(|| Transformed::no(plan), Transformed::yes))
    }
}

/// `limit` moved beneath `projection`, its input. Where federation stops at
/// the projection the limit lands on the federated input, narrowed to the
/// columns the projection reads; otherwise it moves on down, toward the
/// projection or sort where federation stops.
fn limit_below_projection(limit: &Limit, projection: &Projection) -> Result<Option<LogicalPlan>> {
    let input = if federation_stops_at(projection)? {
        let Some(narrowed) = narrowed_input(projection)? else {
            return Ok(None);
        };
        Arc::new(narrowed)
    } else {
        Arc::clone(&projection.input)
    };
    let limit = LogicalPlan::Limit(Limit {
        input,
        ..limit.clone()
    });
    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(
            projection.expr.clone(),
            Arc::new(limit),
            Arc::clone(&projection.schema),
        )?,
    )))
}

/// `sort` with a copy of itself fetching `rows` rows placed beneath the
/// projection it sorts, when federation stops at that projection and every
/// sort key is a column the projection passes through unchanged.
fn sort_below_projection(sort: &Sort, rows: usize) -> Result<Option<Sort>> {
    let LogicalPlan::Projection(projection) = sort.input.as_ref() else {
        return Ok(None);
    };
    let Some(inner_expr) = sort
        .expr
        .iter()
        .map(|sort_expr| {
            passed_through(projection, &sort_expr.expr).map(|column| SortExpr {
                expr: Expr::Column(column),
                ..sort_expr.clone()
            })
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    // A copy already beneath the projection makes another one redundant.
    if let LogicalPlan::Sort(below) = projection.input.as_ref()
        && below.expr == inner_expr
        && below.fetch.is_some_and(|below_fetch| below_fetch <= rows)
    {
        return Ok(None);
    }
    if !federation_stops_at(projection)? {
        return Ok(None);
    }
    let Some(narrowed) = narrowed_input(projection)? else {
        return Ok(None);
    };

    let below = LogicalPlan::Sort(Sort {
        expr: inner_expr,
        input: Arc::new(narrowed),
        fetch: Some(rows),
    });
    let projection = Projection::try_new_with_schema(
        projection.expr.clone(),
        Arc::new(below),
        Arc::clone(&projection.schema),
    )?;
    Ok(Some(Sort {
        input: Arc::new(LogicalPlan::Projection(projection)),
        ..sort.clone()
    }))
}

/// The input column `projection` outputs unchanged as `expr`, if `expr` is a
/// column the projection computes as a bare (possibly aliased) column.
fn passed_through(projection: &Projection, expr: &Expr) -> Option<Column> {
    let Expr::Column(column) = expr else {
        return None;
    };
    let index = projection.schema.index_of_column(column).ok()?;
    match projection.expr.get(index)? {
        Expr::Column(input) => Some(input.clone()),
        Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
            Expr::Column(input) => Some(input.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// The projection's input reduced to the columns the projection reads, or
/// `None` when two of them share a name and so could not be read back from a
/// derived table.
fn narrowed_input(projection: &Projection) -> Result<Option<LogicalPlan>> {
    let input = &projection.input;
    let read: HashSet<&Column> = projection.expr.iter().flat_map(Expr::column_refs).collect();
    let all = input.schema().columns();
    let kept: Vec<Column> = if read.is_empty() {
        all.clone()
    } else {
        all.iter()
            .filter(|column| read.contains(column))
            .cloned()
            .collect()
    };

    let mut names = HashSet::new();
    if !kept.iter().all(|column| names.insert(column.name.as_str())) {
        return Ok(None);
    }
    if kept.len() == all.len() {
        return Ok(Some(input.as_ref().clone()));
    }
    Ok(Some(LogicalPlan::Projection(Projection::try_new(
        kept.into_iter().map(Expr::Column).collect(),
        Arc::clone(input),
    )?)))
}

/// Whether federation stops at `projection`: its source refuses it but
/// accepts the projection's input.
fn federation_stops_at(projection: &Projection) -> Result<bool> {
    let plan = LogicalPlan::Projection(projection.clone());
    Ok(!federates_whole(&plan)? && federates_whole(&projection.input)?)
}

#[cfg(test)]
mod tests {
    use std::{any::Any, error::Error, sync::Arc};

    use async_trait::async_trait;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::TableReference,
        execution::SessionStateBuilder,
        logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf},
        optimizer::AnalyzerRule,
        physical_plan::{ExecutionPlan, displayable},
        prelude::SessionContext,
    };
    use datafusion_federation::FederatedQueryPlanner;
    use datafusion_table_providers::{
        sql::{
            db_connection_pool::{DbConnectionPool, JoinPushDown, dbconnection::DbConnection},
            sql_provider_datafusion::SqlTable,
        },
        util::supported_functions::{FunctionRestriction, FunctionSupport},
    };

    use crate::analyzer_rule::AnalyzerRulesBuilder;

    struct MockConn;

    impl DbConnection<(), &'static dyn ToString> for MockConn {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    /// Every table from this pool shares one remote compute context, so a join
    /// between them can federate as one statement.
    struct SharedContextPool;

    #[async_trait]
    impl DbConnectionPool<(), &'static dyn ToString> for SharedContextPool {
        async fn connect(
            &self,
        ) -> Result<Box<dyn DbConnection<(), &'static dyn ToString>>, Box<dyn Error + Send + Sync>>
        {
            Ok(Box::new(MockConn))
        }

        fn join_push_down(&self) -> JoinPushDown {
            JoinPushDown::AllowedFor("shared".to_string())
        }
    }

    /// Stands in for a Spice-only function such as `json_get_str`.
    fn spice_only_udf() -> ScalarUDF {
        create_udf(
            "json_get_str",
            vec![DataType::Utf8],
            DataType::Utf8,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        )
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "test fixture over a mock pool: the deny-list is installed right below"
    )]
    fn federated_table(
        pool: &Arc<dyn DbConnectionPool<(), &'static dyn ToString> + Send + Sync>,
        name: &'static str,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let table =
            SqlTable::new_with_schema("test", pool, schema, TableReference::bare(name), None)
                .with_function_support(Some(FunctionSupport::new(
                    Some(FunctionRestriction::Deny(vec!["json_get_str".to_string()])),
                    None,
                    None,
                )));
        Arc::new(
            Arc::new(table)
                .create_federated_table_provider()
                .expect("build the federated table"),
        )
    }

    fn context(analyzer_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .with_query_planner(Arc::new(FederatedQueryPlanner::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(spice_only_udf());
        let pool: Arc<dyn DbConnectionPool<(), &'static dyn ToString> + Send + Sync> =
            Arc::new(SharedContextPool);
        for name in ["t", "u"] {
            ctx.register_table(name, federated_table(&pool, name))
                .expect("register the federated table");
        }
        ctx
    }

    /// The physical plan of `sql`, rendered.
    async fn physical_plan(ctx: &SessionContext, sql: &str) -> String {
        let plan: Arc<dyn ExecutionPlan> = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
            .create_physical_plan()
            .await
            .unwrap_or_else(|e| panic!("physical plan for {sql}: {e}"));
        displayable(plan.as_ref()).indent(true).to_string()
    }

    /// The SQL of every remote scan in a rendered plan, in plan order.
    fn remote_sql(rendered: &str) -> Vec<String> {
        rendered
            .lines()
            .filter(|line| line.contains("SqlExec") || line.contains("VirtualExecutionPlan"))
            .filter_map(|line| {
                line.split_once("sql=")
                    .map(|(_, sql)| sql.trim().to_string())
            })
            .collect()
    }

    /// Regression test for the limit half of #13770: once the source refuses
    /// the projection that calls a Spice-only function, a `LIMIT` or an
    /// `ORDER BY … LIMIT` above it must still reach the source, instead of the
    /// source being asked for every row.
    #[tokio::test]
    async fn a_limit_above_a_refused_projection_reaches_the_source() {
        let ctx = context(AnalyzerRulesBuilder::new().build());
        let mut mismatches = vec![];
        for (sql, expected_remote, expected_local) in [
            // The limit moves into the source whole: nothing above the
            // projection limits locally.
            (
                "SELECT json_get_str(val) FROM t LIMIT 3",
                "SELECT val FROM (SELECT t.val FROM t LIMIT 3)",
                "ProjectionExec",
            ),
            (
                "SELECT json_get_str(val) FROM t ORDER BY id LIMIT 3",
                "SELECT id, val FROM (SELECT t.id, t.val FROM t ORDER BY t.id ASC NULLS LAST LIMIT 3) ORDER BY id ASC NULLS LAST",
                "SortPreservingMergeExec: [id@1 ASC NULLS LAST], fetch=3",
            ),
            // The source returns the rows the offset skips as well.
            (
                "SELECT json_get_str(val) FROM t ORDER BY id DESC LIMIT 3 OFFSET 2",
                "SELECT id, val FROM (SELECT t.id, t.val FROM t ORDER BY t.id DESC NULLS FIRST LIMIT 5) ORDER BY id DESC NULLS FIRST",
                "GlobalLimitExec: skip=2, fetch=3",
            ),
            // The function above a join: the join federates, and so does the
            // limit, over only the column the function reads.
            (
                "SELECT json_get_str(t.val) FROM t JOIN u ON t.id = u.id LIMIT 3",
                "SELECT val FROM (SELECT t.val FROM t INNER JOIN u ON (t.id = u.id) LIMIT 3)",
                "ProjectionExec",
            ),
        ] {
            let rendered = physical_plan(&ctx, sql).await;
            let remote = remote_sql(&rendered);
            if remote != [expected_remote] {
                mismatches.push(format!("{sql}\n  remote SQL: {remote:?}\n{rendered}"));
            }
            if !(rendered.contains(expected_local) && rendered.contains("json_get_str")) {
                mismatches.push(format!(
                    "{sql}\n  must still limit, and evaluate json_get_str, locally\n{rendered}"
                ));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    /// The limit must stay where moving it would change the answer, or where
    /// the sort key is the function's own result.
    #[tokio::test]
    async fn a_limit_stays_above_what_it_cannot_cross() {
        let ctx = context(AnalyzerRulesBuilder::new().build());
        let mut mismatches = vec![];
        for (sql, expected_remote) in [
            // A limit taken before the filter would keep rows the filter drops.
            (
                "SELECT id FROM t WHERE json_get_str(val) = 'x' LIMIT 3",
                "SELECT t.id, t.val FROM t",
            ),
            // The source cannot order by a value only Spice computes.
            (
                "SELECT json_get_str(val) AS j FROM t ORDER BY j LIMIT 3",
                "SELECT t.val FROM t",
            ),
            // Two columns named `id` could not be read back from the derived
            // table the limited rows come back as.
            (
                "SELECT json_get_str(t.val), t.id, u.id FROM t JOIN u ON t.id = u.id LIMIT 3",
                "SELECT t.id, t.val, u.id FROM t INNER JOIN u ON (t.id = u.id)",
            ),
        ] {
            let rendered = physical_plan(&ctx, sql).await;
            let remote = remote_sql(&rendered);
            if remote != [expected_remote] {
                mismatches.push(format!("{sql}\n  remote SQL: {remote:?}\n{rendered}"));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    /// A plan the source accepts whole federates exactly as it does without
    /// the rule: the rule only moves a limit where federation stops.
    #[tokio::test]
    async fn a_plan_the_source_accepts_whole_is_unchanged() {
        let with_rule = context(AnalyzerRulesBuilder::new().build());
        let without_rule = context(vec![Arc::new(
            datafusion_federation::sql::federation_analyzer_rule(),
        )]);
        for sql in [
            "SELECT upper(val) FROM t LIMIT 3",
            "SELECT upper(val) AS v, id FROM t ORDER BY id LIMIT 3",
            "SELECT id, count(*) AS n FROM t GROUP BY id ORDER BY n DESC LIMIT 10",
            "SELECT t.val FROM t JOIN u ON t.id = u.id ORDER BY t.id LIMIT 5",
        ] {
            let expected = remote_sql(&physical_plan(&without_rule, sql).await);
            let rendered = physical_plan(&with_rule, sql).await;
            assert_eq!(expected.len(), 1, "{sql} must federate whole: {expected:?}");
            assert_eq!(
                remote_sql(&rendered),
                expected,
                "remote SQL for {sql}; plan:\n{rendered}"
            );
        }
    }
}
