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

//! [`super::sql_depth_guard`] on the runtime's real planner, on the worker
//! stack size `spiced` runs queries on.

use std::sync::Arc;

use datafusion::error::Result as DFResult;

use super::sql_depth_guard::{MAX_SQL_EXPR_DEPTH, MAX_UNPROTECTED_EXPR_DEPTH};

/// `n` terms of `+1` after `head`: an expression tree `n` deep.
fn plus_chain(head: &str, n: usize) -> String {
    format!("{head}{}", "+1".repeat(n - 1))
}

/// Plans `sql` through the runtime's planner on a Tokio worker with the
/// default 2 MiB stack `spiced`'s query runtimes run on, so an over-deep
/// expression the guard lets through aborts the test process instead of
/// passing. The test harness's own threads are larger (`RUST_MIN_STACK`).
async fn plan_on_default_worker_stack(sql: String) -> DFResult<()> {
    let runtime = crate::Runtime::builder().build().await;
    let df = Arc::clone(&runtime.df);
    let workers = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("worker runtime builds");
    let result = workers
        .spawn(async move {
            let session = df.ctx.state();
            df.create_logical_plan(&session, &sql, None)
                .await
                .map(|_| ())
        })
        .await
        .expect("planning task completes");
    workers.shutdown_background();
    result
}

/// Regression test: a `SELECT`, `ORDER BY`, or `VALUES` chain under the
/// general cap overflowed the worker stack while `DataFusion` rendered its
/// column name, aborting the process. It is now a planning error.
#[tokio::test]
async fn deep_named_chains_are_rejected_on_the_worker_stack() {
    let expected = format!(
        "Error during planning: Query rejected: a selected, grouped, ordered, or VALUES \
         expression is nested more than {MAX_UNPROTECTED_EXPR_DEPTH} levels deep. Simplify the \
         expression (for example, compute part of a long chain of +, || or AND in a subquery)."
    );
    for sql in [
        format!("SELECT {}", plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH + 1)),
        format!(
            "SELECT id FROM (VALUES (1)) t(id) ORDER BY {}",
            plus_chain("id", MAX_UNPROTECTED_EXPR_DEPTH + 1)
        ),
        format!(
            "SELECT * FROM (VALUES ({})) v(x)",
            plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH + 1)
        ),
    ] {
        let err = plan_on_default_worker_stack(sql)
            .await
            .expect_err("a deep named chain is rejected");
        assert_eq!(err.strip_backtrace(), expected);
    }
}

/// Chains at the named cap, and a predicate chain at the general cap, still
/// plan on the worker stack.
#[tokio::test]
async fn chains_at_the_caps_plan_on_the_worker_stack() {
    let or_terms = vec!["id = 1"; MAX_SQL_EXPR_DEPTH - 1].join(" OR ");
    for sql in [
        format!("SELECT {}", plus_chain("1", MAX_UNPROTECTED_EXPR_DEPTH)),
        format!(
            "SELECT id FROM (VALUES (1)) t(id) ORDER BY {}",
            plus_chain("id", MAX_UNPROTECTED_EXPR_DEPTH)
        ),
        format!("SELECT id FROM (VALUES (1)) t(id) WHERE {or_terms}"),
    ] {
        plan_on_default_worker_stack(sql.clone())
            .await
            .unwrap_or_else(|e| panic!("{sql:.60}… must plan: {e}"));
    }
}
