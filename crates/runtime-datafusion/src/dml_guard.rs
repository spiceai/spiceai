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

//! Refuses a `DELETE`/`UPDATE` whose row condition cannot reach the table.
//!
//! A table provider receives the rows to affect as a list of filter
//! expressions, and an **empty list means every row** — that is the documented
//! contract of `TableProvider::delete_from` and `TableProvider::update`. The
//! physical planner recovers that list from the statement's input plan, reading
//! `Filter` predicates and `TableScan` filters. A restriction the optimizer has
//! moved anywhere else is not seen, and the statement silently widens:
//!
//! - `WHERE FALSE` collapses the input to an `EmptyRelation`. No filter is left
//!   to find, so a statement that matches no rows would affect all of them.
//!   [`rewrite_dml_for_the_table`] plans it as a statement that affects none.
//!   Such a statement never reaches the table, so a dataset that refuses every
//!   write of its kind (a snapshot acceleration, for one) returns 0 for it.
//! - `WHERE id IN (SELECT ...)`, `EXISTS`, and `NOT EXISTS` are decorrelated
//!   into a join, which carries the restriction the filter list then lacks.
//! - `WHERE id = 1 AND EXISTS (...)` is worse: `id = 1` survives on its own, so
//!   the statement looks well-formed while quietly affecting a superset of the
//!   rows the condition selected.
//!
//! This has already cost data once: a pushed-down `WHERE` the planner did not
//! read back deleted every row of the table (regression test
//! `test_postgres_write_through_delete_with_where`). Teaching each provider to
//! recognise the shapes cannot work — by the time a provider is called the
//! condition is gone, and "every row" is indistinguishable from "the condition
//! I could not represent". So the check belongs here, while the input plan is
//! still intact, and it is a *positive* recognizer: a plan is planned only when
//! every node in it is one whose restriction the filter list provably carries.
//! Anything else is refused rather than approximated.
//!
//! The filter list also has to name only the table's own columns. When a
//! condition repeats an expression, the optimizer computes it once in a
//! projection under the filter and the filter reads the computed column
//! (`__common_expr_1`), which the table does not have.
//! [`rewrite_dml_for_the_table`] puts a non-volatile expression back in its
//! place. A column computed by a volatile expression, such as a model call
//! placed to run once per row, keeps its name, and the table refuses it.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    DmlStatement, Expr, Filter, LogicalPlan, LogicalPlanBuilder, Projection, WriteOp, lit,
};
use datafusion::optimizer::push_down_filter::replace_cols_by_name;

/// How a statement restricts its rows in a way the extracted filters cannot
/// express, and what the user can do instead.
struct Unrepresentable {
    cause: &'static str,
    remedy: &'static str,
}

/// The statement being refused, in the two forms its message needs.
struct Verb {
    /// Fills `Failed to {action} table 'x'`.
    action: &'static str,
    /// Fills `more rows would be {affected}`.
    affected: &'static str,
}

/// Whether this node restricts the affected rows in a way the filter list
/// cannot carry.
///
/// Exhaustive on purpose: a wildcard arm would silently admit a future
/// `LogicalPlan` variant, and admitting the wrong one costs rows.
fn unrepresentable_node(node: &LogicalPlan) -> Option<Unrepresentable> {
    match node {
        // The only nodes whose effect on the row set is exactly what the
        // planner reads back: `Filter` predicates and `TableScan` filters,
        // under projections and aliases, neither of which drops a row.
        LogicalPlan::TableScan(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Filter(_) => None,

        // What `IN (SELECT ...)`, `EXISTS`, and `NOT EXISTS` decorrelate into.
        LogicalPlan::Join(_) | LogicalPlan::Subquery(_) => Some(Unrepresentable {
            cause: "a subquery",
            remedy: "Run the subquery first and give the values it returns as a literal condition",
        }),

        LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) | LogicalPlan::Window(_) => {
            Some(Unrepresentable {
                cause: "a grouping or window computed across rows",
                remedy: "Select the rows to affect first, then name them by a condition on the table's own columns",
            })
        }

        LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => Some(Unrepresentable {
            cause: "an ordering or row limit",
            remedy: "Select the rows to affect first, then name them by a condition on the table's own columns",
        }),

        _ => Some(Unrepresentable {
            cause: "a construct that cannot be reduced to a condition on a single row",
            remedy: "Rewrite the statement so the rows are selected by a condition on the table's own columns",
        }),
    }
}

/// The refusal a caller gets. A pure function so its wording is pinned by a
/// test: it has to keep naming the table, the impact, and the way out.
fn refusal(verb: &Verb, table: &str, found: &Unrepresentable) -> String {
    format!(
        "Failed to {} table '{table}': the statement is restricted by {}, which cannot be carried to the table as a row condition, \
        so more rows would be {} than the condition selected. {}. \
        See: https://spiceai.org/docs/reference/sql",
        verb.action, found.cause, verb.affected, found.remedy
    )
}

/// Refuse a `DELETE`/`UPDATE` whose row condition the planner cannot carry to
/// the table.
///
/// The statement is found anywhere in the plan, not only at its root: the
/// optimizer wraps every write in other nodes, such as the cache invalidation
/// node, before the plan reaches the physical planner.
///
/// Returns `Ok(())` for every other plan, including `INSERT` and `TRUNCATE`,
/// which carry no row condition to lose.
///
/// # Errors
///
/// [`DataFusionError::Plan`] when the statement's input plan restricts rows in
/// a way the extracted filter list cannot express.
pub fn ensure_dml_restriction_reaches_the_table(plan: &LogicalPlan) -> Result<()> {
    plan.apply(|node| match node {
        LogicalPlan::Dml(dml) => {
            ensure_statement_restriction_reaches_the_table(dml)?;
            Ok(TreeNodeRecursion::Jump)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .map(|_| ())
}

/// Rewrite each `DELETE`/`UPDATE` so that the filter list the planner extracts
/// from it names only the table's own columns:
///
/// - A statement whose condition selects no row is planned as one that affects
///   no row, since its filter list would otherwise be empty, which means every
///   row.
/// - A column that a projection computes under the condition from a
///   non-volatile expression is replaced by that expression.
///
/// Returns the plan unchanged, without copying it, when it holds no such
/// statement.
///
/// # Errors
///
/// When a rewritten node cannot be rebuilt.
pub fn rewrite_dml_for_the_table(plan: &LogicalPlan) -> Result<Cow<'_, LogicalPlan>> {
    let has_row_condition = plan.exists(|node| {
        Ok(matches!(
            node,
            LogicalPlan::Dml(DmlStatement {
                op: WriteOp::Delete | WriteOp::Update,
                ..
            })
        ))
    })?;
    if !has_row_condition {
        return Ok(Cow::Borrowed(plan));
    }

    let rewritten = plan.clone().transform_up(|node| match node {
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Delete | WriteOp::Update) => {
            if selects_no_row(&dml.input) {
                return Ok(Transformed::yes(affects_no_row(&dml)?));
            }
            let input = Arc::unwrap_or_clone(Arc::clone(&dml.input))
                .transform_up(inline_computed_columns)?;
            if !input.transformed {
                return Ok(Transformed::no(LogicalPlan::Dml(dml)));
            }
            Ok(Transformed::yes(LogicalPlan::Dml(DmlStatement {
                input: Arc::new(input.data),
                ..dml
            })))
        }
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Cow::Owned(rewritten.data))
}

/// Whether the statement's input provably yields no row: an empty relation
/// under nodes that never add one.
fn selects_no_row(input: &LogicalPlan) -> bool {
    match input {
        LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
        LogicalPlan::Projection(Projection { input, .. })
        | LogicalPlan::Filter(Filter { input, .. })
        | LogicalPlan::SubqueryAlias(datafusion::logical_expr::SubqueryAlias { input, .. }) => {
            selects_no_row(input)
        }
        _ => false,
    }
}

/// The result of a statement that affects no row, in the statement's own
/// output schema: a count of 0.
fn affects_no_row(dml: &DmlStatement) -> Result<LogicalPlan> {
    let count = dml.output_schema.field(0).name();
    LogicalPlanBuilder::empty(true)
        .project(vec![lit(ScalarValue::UInt64(Some(0))).alias(count)])?
        .build()
}

/// In a filter or projection, replace each column that the projection below
/// it computes (looking through filters) from a non-volatile expression by
/// that expression. That projection then also passes on every column of its
/// input, since the expression reads columns the projection may have left out.
fn inline_computed_columns(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match node {
        LogicalPlan::Filter(filter) => {
            let Some(computed) = computed_columns_below(&filter.input) else {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            };
            let predicate = replace_cols_by_name(filter.predicate.clone(), &computed)?;
            if predicate == filter.predicate {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
            Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
                predicate,
                pass_on_input_columns_below(&filter.input)?,
            )?)))
        }
        LogicalPlan::Projection(projection) => {
            let Some(computed) = computed_columns_below(&projection.input) else {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            };
            let mut changed = false;
            let exprs = projection
                .expr
                .iter()
                .map(|expr| {
                    let inlined = replace_cols_by_name(expr.clone(), &computed)?;
                    if inlined == *expr {
                        return Ok(inlined);
                    }
                    changed = true;
                    // Keep the output name the projection had.
                    inlined.alias_if_changed(expr.schema_name().to_string())
                })
                .collect::<Result<Vec<_>>>()?;
            if !changed {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            Ok(Transformed::yes(LogicalPlan::Projection(
                Projection::try_new(exprs, pass_on_input_columns_below(&projection.input)?)?,
            )))
        }
        other => Ok(Transformed::no(other)),
    }
}

/// The columns the nearest projection below `plan` computes from a
/// non-volatile expression, by name. Filters keep their input's columns, so
/// they are looked through. `None` when there is no such column.
fn computed_columns_below(plan: &LogicalPlan) -> Option<HashMap<String, Expr>> {
    match plan {
        LogicalPlan::Filter(filter) => computed_columns_below(&filter.input),
        LogicalPlan::Projection(projection) => {
            let computed: HashMap<String, Expr> = projection
                .schema
                .columns()
                .into_iter()
                .zip(&projection.expr)
                // A volatile expression, such as a model call placed to run
                // once per row, keeps its column.
                .filter(|(_, expr)| !matches!(expr, Expr::Column(_)) && !expr.is_volatile())
                .map(|(column, expr)| (column.flat_name(), expr.clone().unalias()))
                .collect();
            (!computed.is_empty()).then_some(computed)
        }
        _ => None,
    }
}

/// Rebuild the nearest projection below `plan` (looking through filters) so
/// that it also passes on each column of its input that it leaves out.
fn pass_on_input_columns_below(plan: &Arc<LogicalPlan>) -> Result<Arc<LogicalPlan>> {
    match plan.as_ref() {
        LogicalPlan::Filter(filter) => Ok(Arc::new(LogicalPlan::Filter(Filter::try_new(
            filter.predicate.clone(),
            pass_on_input_columns_below(&filter.input)?,
        )?))),
        LogicalPlan::Projection(projection) => {
            let left_out: Vec<Expr> = projection
                .input
                .schema()
                .columns()
                .into_iter()
                .filter(|column| !projection.schema.has_column(column))
                .map(Expr::Column)
                .collect();
            if left_out.is_empty() {
                return Ok(Arc::clone(plan));
            }
            let mut exprs = projection.expr.clone();
            exprs.extend(left_out);
            Ok(Arc::new(LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::clone(&projection.input),
            )?)))
        }
        _ => Ok(Arc::clone(plan)),
    }
}

fn ensure_statement_restriction_reaches_the_table(dml: &DmlStatement) -> Result<()> {
    let verb = match dml.op {
        WriteOp::Delete => Verb {
            action: "delete from",
            affected: "deleted",
        },
        WriteOp::Update => Verb {
            action: "update",
            affected: "updated",
        },
        // An insert names its rows outright, and a truncate means every row on
        // purpose; neither has a condition that could be dropped. A merge carries
        // its conditions in the merge operation itself rather than in a filter
        // list extracted from the input, and any other write names no row
        // condition this guard knows how to check.
        // (`Insert`, `Ctas`, `Truncate`, `MergeInto`, and any write op added later.)
        _ => return Ok(()),
    };

    let mut found = None;
    dml.input.apply(|node| {
        if let Some(unrepresentable) = unrepresentable_node(node) {
            found = Some(unrepresentable);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    match found {
        None => Ok(()),
        Some(found) => Err(DataFusionError::Plan(refusal(
            &verb,
            &dml.table_name.to_string(),
            &found,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extension::ExtensionPlanQueryPlanner;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use async_trait::async_trait;
    use datafusion::catalog::Session;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::logical_expr::Expr;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use datafusion_optimizer_rules::logical_plan::{
        CacheInvalidationExtensionPlanner, cache_invalidation::CacheInvalidationOptimizerRule,
    };
    use std::sync::{Arc, Mutex, Weak};

    /// Records the filter list each DML call receives, so a test can assert not
    /// only that a statement was refused but that nothing reached the table,
    /// and the values each UPDATE assigns.
    #[derive(Debug)]
    struct Recorder {
        schema: SchemaRef,
        seen: Arc<Mutex<Vec<Vec<Expr>>>>,
        assigned: Arc<Mutex<Vec<Vec<String>>>>,
    }

    #[async_trait]
    impl TableProvider for Recorder {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let schema = match projection {
                Some(columns) => Arc::new(self.schema.project(columns)?),
                None => Arc::clone(&self.schema),
            };
            Ok(Arc::new(EmptyExec::new(schema)))
        }

        async fn delete_from(
            &self,
            _state: &dyn Session,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().expect("record delete").push(filters);
            Ok(dml_result())
        }

        async fn update(
            &self,
            _state: &dyn Session,
            assignments: Vec<(String, Expr)>,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().expect("record update").push(filters);
            self.assigned.lock().expect("record assignments").push(
                assignments
                    .iter()
                    .map(|(column, value)| format!("{column} = {value}"))
                    .collect(),
            );
            Ok(dml_result())
        }
    }

    /// A DML plan returns the count of rows it affected, as every provider's does.
    fn dml_result() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))))
    }

    /// What the table saw when a statement was planned and run.
    struct Outcome {
        error: Option<String>,
        calls: Vec<Vec<String>>,
        /// The values each UPDATE call assigned.
        assignments: Vec<Vec<String>>,
        /// The `count` the statement returned, when it ran.
        count: Option<u64>,
    }

    impl Outcome {
        /// The filter list of each DML call the provider received.
        fn planned(self) -> Vec<Vec<String>> {
            assert!(
                self.error.is_none(),
                "expected the statement to be planned: {:?}",
                self.error
            );
            self.calls
        }

        /// Proof that a statement affected no row: it ran, returned a count of
        /// 0, and never reached the table, whose empty filter list would mean
        /// every row.
        fn affected_nothing(self) {
            assert!(
                self.error.is_none(),
                "expected the statement to run: {:?}",
                self.error
            );
            assert!(
                self.calls.is_empty(),
                "a statement that selects no row reached the table: {:?}",
                self.calls
            );
            assert_eq!(self.count, Some(0));
        }

        /// The refusal — and, in the same breath, proof that nothing reached
        /// the table. A guard that refused the statement but still let a call
        /// through would have deleted the rows it was refusing to delete.
        fn refused(self) -> String {
            assert!(
                self.calls.is_empty(),
                "a refused statement still reached the table: {:?}",
                self.calls
            );
            self.error.expect("expected the statement to be refused")
        }
    }

    async fn run(sql: &str) -> Outcome {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("price", DataType::Float64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let assigned = Arc::new(Mutex::new(Vec::new()));

        // The planner under test, wired as the runtime wires it, including the
        // cache invalidation rule that wraps every write.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(
                ExtensionPlanQueryPlanner::from_extension_planners(vec![Arc::new(
                    CacheInvalidationExtensionPlanner::new(),
                )]),
            ))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.add_optimizer_rule(Arc::new(CacheInvalidationOptimizerRule::new(Weak::new())));
        ctx.register_table(
            "t",
            Arc::new(Recorder {
                schema: Arc::clone(&schema),
                seen: Arc::clone(&seen),
                assigned: Arc::clone(&assigned),
            }),
        )
        .expect("register t");
        ctx.register_table(
            "s",
            Arc::new(Recorder {
                schema,
                seen: Arc::new(Mutex::new(Vec::new())),
                assigned: Arc::new(Mutex::new(Vec::new())),
            }),
        )
        .expect("register s");

        let (error, count) = match ctx.sql(sql).await {
            Err(e) => (Some(e.to_string()), None),
            Ok(plan) => match plan.collect().await {
                Err(e) => (Some(e.to_string()), None),
                Ok(batches) => (None, returned_count(&batches)),
            },
        };

        let calls = seen
            .lock()
            .expect("read calls")
            .iter()
            .map(|filters: &Vec<Expr>| {
                filters
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect()
            })
            .collect();

        let assignments = assigned.lock().expect("read assignments").clone();

        Outcome {
            error,
            calls,
            assignments,
            count,
        }
    }

    /// The single `count` value a DML statement returns.
    fn returned_count(batches: &[arrow::record_batch::RecordBatch]) -> Option<u64> {
        let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
        batch
            .column_by_name("count")?
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .map(|counts| counts.value(0))
    }

    /// A statement the table can carry reaches it, with its condition intact.
    #[tokio::test]
    async fn a_condition_on_the_tables_own_columns_reaches_the_table() {
        assert_eq!(
            run("DELETE FROM t WHERE id = 1").await.planned(),
            vec![vec!["id = Int32(1)".to_string()]]
        );
        assert_eq!(
            run("UPDATE t SET label = 'x' WHERE id = 1").await.planned(),
            vec![vec!["id = Int32(1)".to_string()]]
        );
    }

    /// `DELETE FROM t` really does mean every row, and must keep working.
    #[tokio::test]
    async fn a_statement_with_no_condition_still_affects_every_row() {
        let empty: Vec<String> = Vec::new();
        assert_eq!(run("DELETE FROM t").await.planned(), vec![empty.clone()]);
        // `WHERE TRUE` is the same statement once the optimizer is done.
        assert_eq!(run("DELETE FROM t WHERE TRUE").await.planned(), vec![empty]);
    }

    /// A condition that selects nothing must not be read as "no condition".
    ///
    /// `WHERE FALSE` collapses the input to an `EmptyRelation`, leaving no
    /// filter to extract, and an empty filter list means every row. The
    /// statement affects no row and never reaches the table.
    #[tokio::test]
    async fn a_condition_that_matches_no_rows_affects_none() {
        for sql in [
            "DELETE FROM t WHERE FALSE",
            "DELETE FROM t WHERE 1 = 0",
            "DELETE FROM t WHERE NULL",
            "DELETE FROM t WHERE id > 0 AND FALSE",
            "UPDATE t SET label = 'x' WHERE FALSE",
            "EXPLAIN ANALYZE DELETE FROM t WHERE FALSE",
        ] {
            let outcome = run(sql).await;
            if sql.starts_with("EXPLAIN") {
                assert!(outcome.error.is_none(), "{sql}: {:?}", outcome.error);
                assert!(outcome.calls.is_empty(), "{sql}: {:?}", outcome.calls);
            } else {
                outcome.affected_nothing();
            }
        }
    }

    /// An UPDATE that repeats an expression reaches the table with conditions
    /// and values on its own columns, also when it assigns a column that only
    /// the repeated expression reads.
    #[tokio::test]
    async fn a_repeated_expression_in_an_update_reaches_the_table_as_written() {
        let outcome = run("UPDATE t SET price = 9 WHERE price * 2 > 3 AND price * 2 < 10").await;
        assert_eq!(
            (outcome.calls.clone(), outcome.error.clone()),
            (
                vec![vec![
                    "price * Float64(2) > Float64(3)".to_string(),
                    "price * Float64(2) < Float64(10)".to_string(),
                ]],
                None
            )
        );
        assert_eq!(
            outcome.assignments,
            vec![vec!["price = Float64(9)".to_string()]]
        );

        let outcome = run("UPDATE t SET price = price * 2 + price * 2 WHERE id = 1").await;
        assert_eq!(outcome.error, None);
        assert_eq!(outcome.calls, vec![vec!["id = Int32(1)".to_string()]]);
        assert_eq!(
            outcome.assignments,
            vec![vec![
                "price = price * Float64(2) + price * Float64(2)".to_string()
            ]]
        );
    }

    /// A condition that repeats an expression reaches the table as conditions
    /// on its own columns, not on the column the optimizer computes the
    /// expression into.
    #[tokio::test]
    async fn a_repeated_expression_reaches_the_table_as_written() {
        assert_eq!(
            run("DELETE FROM t WHERE id * 2 > 1 AND id * 2 < 10")
                .await
                .planned(),
            vec![vec![
                "CAST(id AS Int64) * Int64(2) > Int64(1)".to_string(),
                "CAST(id AS Int64) * Int64(2) < Int64(10)".to_string(),
            ]]
        );
        assert_eq!(
            run("DELETE FROM t WHERE id * 2 = 4 OR id * 2 = 8")
                .await
                .planned(),
            vec![vec![
                "CAST(id AS Int64) * Int64(2) = Int64(4) OR CAST(id AS Int64) * Int64(2) = Int64(8)".to_string()
            ]]
        );
        assert_eq!(
            run("UPDATE t SET price = id * 2 WHERE id * 2 > 3 AND id * 2 < 7")
                .await
                .planned(),
            vec![vec![
                "CAST(id AS Int64) * Int64(2) > Int64(3)".to_string(),
                "CAST(id AS Int64) * Int64(2) < Int64(7)".to_string(),
            ]]
        );
    }

    /// A subquery condition is decorrelated into a join, which the filter list
    /// cannot carry, so the statement would widen to the whole table.
    #[tokio::test]
    async fn a_subquery_condition_is_refused() {
        for sql in [
            "DELETE FROM t WHERE id IN (SELECT id FROM s)",
            "DELETE FROM t WHERE EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
            "DELETE FROM t WHERE NOT EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
            "UPDATE t SET label = 'x' WHERE id IN (SELECT id FROM s)",
        ] {
            let err = run(sql).await.refused();
            assert!(
                err.contains("a subquery"),
                "{sql} was not refused for the right reason: {err}"
            );
        }
    }

    /// The dangerous middle case: part of the condition survives extraction, so
    /// the statement looks well-formed while affecting a superset of the rows.
    #[tokio::test]
    async fn a_partly_extractable_condition_is_refused() {
        for sql in [
            "DELETE FROM t WHERE id = 1 AND EXISTS (SELECT 1 FROM s WHERE s.id = t.id)",
            "DELETE FROM t WHERE label = 'a' AND id IN (SELECT id FROM s)",
        ] {
            let err = run(sql).await.refused();
            assert!(
                err.contains("a subquery"),
                "{sql} was not refused for the right reason: {err}"
            );
        }
    }

    /// `EXPLAIN ANALYZE` runs the statement, so it is refused like the statement.
    #[tokio::test]
    async fn an_explain_analyze_of_a_refused_statement_is_refused() {
        let err = run("EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s)")
            .await
            .refused();
        assert!(err.contains("a subquery"), "{err}");
    }

    /// The refusal names the table the user asked about.
    #[tokio::test]
    async fn a_refusal_names_the_table() {
        let err = run("DELETE FROM t WHERE id IN (SELECT id FROM s)")
            .await
            .refused();
        assert!(err.contains("Failed to delete from table 't'"), "{err}");
    }

    /// A column computed by a volatile expression, such as a model call placed
    /// to run once per row, is not put back into the condition.
    #[test]
    fn a_volatile_computed_column_stays_a_column() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let table = datafusion::datasource::provider_as_source(Arc::new(Recorder {
            schema,
            seen: Arc::new(Mutex::new(Vec::new())),
            assigned: Arc::new(Mutex::new(Vec::new())),
        }));
        let input = LogicalPlanBuilder::scan("t", Arc::clone(&table), None)
            .and_then(|plan| {
                plan.project(vec![
                    datafusion::functions::math::expr_fn::random().alias("r"),
                    datafusion::prelude::col("id"),
                ])
            })
            .and_then(|plan| plan.filter(datafusion::prelude::col("r").gt(lit(0.5))))
            .and_then(|plan| plan.project(vec![datafusion::prelude::col("id")]))
            .and_then(LogicalPlanBuilder::build)
            .expect("build the statement's input");
        let delete = LogicalPlan::Dml(DmlStatement::new(
            "t".into(),
            table,
            WriteOp::Delete,
            Arc::new(input),
        ));

        let rewritten = rewrite_dml_for_the_table(&delete).expect("rewrite");
        assert_eq!(rewritten.as_ref(), &delete);
    }

    /// A plan that is not DML has no row condition to lose, and is never
    /// refused — the guard sits on every query's planning path.
    #[tokio::test]
    async fn a_plan_that_is_not_dml_is_never_refused() {
        let ctx = SessionContext::new();
        let logical = ctx
            .sql("SELECT 1")
            .await
            .expect("plan select")
            .logical_plan()
            .clone();
        ensure_dml_restriction_reaches_the_table(&logical).expect("never refused");
    }

    /// The refusal has to keep naming the table, the impact, and the way out.
    #[test]
    fn the_refusal_names_the_table_the_impact_and_the_remedy() {
        let message = refusal(
            &Verb {
                action: "delete from",
                affected: "deleted",
            },
            "sales.orders",
            &Unrepresentable {
                cause: "a subquery",
                remedy: "Run the subquery first",
            },
        );

        assert!(message.contains("'sales.orders'"), "{message}");
        assert!(message.contains("more rows would be deleted"), "{message}");
        assert!(message.contains("Run the subquery first"), "{message}");
        assert!(
            message.contains("https://spiceai.org/docs/reference/sql"),
            "{message}"
        );
        assert!(!message.contains('\n'), "must stay on one line: {message}");
    }
}
