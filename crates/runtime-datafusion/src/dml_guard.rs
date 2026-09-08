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
//!   to find, so a statement that matches no rows affects all of them.
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

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{LogicalPlan, WriteOp};

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

        // `WHERE FALSE`, and anything the optimizer proves selects nothing.
        LogicalPlan::EmptyRelation(_) => Some(Unrepresentable {
            cause: "a condition that no row can satisfy",
            remedy: "Correct the condition, or remove the statement if it was meant to affect nothing",
        }),

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
/// Returns `Ok(())` for every other plan, including `INSERT` and `TRUNCATE`,
/// which carry no row condition to lose.
///
/// # Errors
///
/// [`DataFusionError::Plan`] when the statement's input plan restricts rows in
/// a way the extracted filter list cannot express.
pub fn ensure_dml_restriction_reaches_the_table(plan: &LogicalPlan) -> Result<()> {
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(());
    };

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
        // purpose; neither has a condition that could be dropped.
        WriteOp::Insert(_) | WriteOp::Ctas | WriteOp::Truncate => return Ok(()),
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
    use std::sync::{Arc, Mutex};

    /// Records the filter list each DML call receives, so a test can assert not
    /// only that a statement was refused but that nothing reached the table.
    #[derive(Debug)]
    struct Recorder {
        schema: SchemaRef,
        seen: Arc<Mutex<Vec<Vec<Expr>>>>,
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
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
        }

        async fn delete_from(
            &self,
            _state: &dyn Session,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().expect("record delete").push(filters);
            Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
        }

        async fn update(
            &self,
            _state: &dyn Session,
            _assignments: Vec<(String, Expr)>,
            filters: Vec<Expr>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.seen.lock().expect("record update").push(filters);
            Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
        }
    }

    /// What the table saw when a statement was planned and run.
    struct Outcome {
        error: Option<String>,
        calls: Vec<Vec<String>>,
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

        // The planner under test, wired exactly as the runtime wires it.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_query_planner(Arc::new(ExtensionPlanQueryPlanner::default()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table(
            "t",
            Arc::new(Recorder {
                schema: Arc::clone(&schema),
                seen: Arc::clone(&seen),
            }),
        )
        .expect("register t");
        ctx.register_table(
            "s",
            Arc::new(Recorder {
                schema,
                seen: Arc::new(Mutex::new(Vec::new())),
            }),
        )
        .expect("register s");

        let error = match ctx.sql(sql).await {
            Err(e) => Some(e.to_string()),
            Ok(plan) => plan.collect().await.err().map(|e| e.to_string()),
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

        Outcome { error, calls }
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
    /// Regression: `WHERE FALSE` collapses the input to an `EmptyRelation`,
    /// leaving no filter to extract, and an empty filter list means every row —
    /// so a statement matching zero rows deleted the whole table.
    #[tokio::test]
    async fn a_condition_that_matches_no_rows_is_refused() {
        for sql in [
            "DELETE FROM t WHERE FALSE",
            "DELETE FROM t WHERE 1 = 0",
            "DELETE FROM t WHERE id > 0 AND FALSE",
            "UPDATE t SET label = 'x' WHERE FALSE",
        ] {
            let err = run(sql).await.refused();
            assert!(
                err.contains("no row can satisfy"),
                "{sql} was not refused for the right reason: {err}"
            );
        }
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

    /// The refusal names the table the user asked about.
    #[tokio::test]
    async fn a_refusal_names_the_table() {
        let err = run("DELETE FROM t WHERE id IN (SELECT id FROM s)")
            .await
            .refused();
        assert!(err.contains("Failed to delete from table 't'"), "{err}");
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
