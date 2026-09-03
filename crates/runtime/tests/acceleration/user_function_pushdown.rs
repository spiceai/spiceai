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

//! A user-defined function must never be pushed into a remote backend: no
//! source has an equivalent, so the SQL it receives names a function it cannot
//! resolve — or, where it happens to have one of that name, answers from a
//! different function. Both halves are measured here, because only the first
//! one is loud.
//!
//! The deny-list that prevents this freezes its *names* when a table-provider
//! factory is built, and every accelerator engine is constructed in
//! `RuntimeBuilder::build` before that same `build` registers the spicepod's
//! `functions:` entries. So the snapshot the `SQLite` accelerator carries names
//! no user function at all, and the whole of `functions:` was pushdown-eligible
//! against it (#13726 / #13810).

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::component::function::{
    Function, FunctionArg, FunctionKind, FunctionReturns, Signature,
    Volatility as FunctionVolatility,
};
use spicepod::component::runtime::{Functions, Runtime as SpicepodRuntime};
use spicepod::param::Params;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::load_runtime_datasets;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, to_pretty_display},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

fn write_csv_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,x\n\
         1,10\n\
         2,20\n\
         3,30\n",
    )?;
    Ok(())
}

/// `double_it(x) = x * 2`, declared immutable so nothing but the deny-list
/// decides whether the call federates — a volatile function is held back by the
/// optimizer anyway, which would make this test pass with the fix removed.
fn double_it() -> Function {
    Function {
        name: "double_it".to_string(),
        from: "sql".to_string(),
        enabled: true,
        description: Some("Double a 64-bit integer.".to_string()),
        kind: FunctionKind::Scalar,
        volatility: FunctionVolatility::Immutable,
        signature: Signature {
            tables: vec![],
            args: vec![FunctionArg {
                name: "x".to_string(),
                arrow_type: "int64".to_string(),
            }],
            returns: Some(FunctionReturns::Scalar("int64".to_string())),
        },
        body: Some("x * 2".to_string()),
        body_ref: None,
        metadata: HashMap::new(),
        params: HashMap::new(),
        depends_on: vec![],
        metrics: None,
        as_tool: false,
    }
}

/// `unlikely(x) = x * 2`, under a name `SQLite` also has: its own
/// `unlikely(X)` is an optimizer hint returning `X` unchanged, and it returns
/// an integer, so a pushed-down call answers a *different number* of the right
/// type rather than failing. This is the half of #13726 that returns wrong rows
/// instead of an error.
fn shadowing_unlikely() -> Function {
    let mut function = double_it();
    function.name = "unlikely".to_string();
    function.description =
        Some("SQLite has a function of this name that returns its argument unchanged.".to_string());
    function
}

fn csv_params() -> Option<Params> {
    Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ))
}

fn sqlite_accelerated(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = csv_params();
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("sqlite".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

fn unaccelerated(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = csv_params();
    dataset
}

/// A loaded runtime carrying `function`, an `accelerated` dataset over `csv` and
/// an unaccelerated `local` control over the same file.
///
/// Built through `Runtime::builder().build()` rather than by registering the
/// function afterwards, because the ordering *inside* that call is the defect:
/// it constructs every accelerator engine — freezing each one's deny-list —
/// before it registers the spicepod's `functions:` entries.
async fn runtime_with(
    app_name: &str,
    function: Function,
    csv: &Path,
) -> Result<Arc<Runtime>, anyhow::Error> {
    write_csv_source(csv)?;
    let from = format!("file://{}", csv.display());

    let app = AppBuilder::new(app_name)
        .with_runtime(SpicepodRuntime {
            functions: Functions::enabled(),
            ..Default::default()
        })
        .with_function(function)
        .with_dataset(sqlite_accelerated(&from, "accelerated"))
        .with_dataset(unaccelerated(&from, "local"))
        .build();

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;
    Ok(rt)
}

/// Regression test for #13726 / #13810.
///
/// Before the fix the `SQLite` accelerator was asked to evaluate `double_it`
/// and answered `no such function: double_it`; the query failed rather than
/// returning a row.
#[tokio::test]
async fn a_user_function_is_not_pushed_into_a_sqlite_accelerator() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let rt = runtime_with(
                "user_function_pushdown",
                double_it(),
                &dir.path().join("numbers.csv"),
            )
            .await?;

            // `base_sql` is the SQL the federated scan sends, so it is the only
            // part of the plan that says what SQLite is asked to evaluate — the
            // logical plan above it names `double_it` either way.
            let plan = to_pretty_display(
                &run_query(
                    &rt,
                    "EXPLAIN SELECT id, double_it(x) AS doubled FROM accelerated",
                )
                .await?,
            )?
            .to_string();
            let remote_sql: String = plan
                .split("base_sql=")
                .skip(1)
                .map(|tail| tail.split('\n').next().unwrap_or_default().to_string())
                .collect::<Vec<_>>()
                .join("\n");
            assert!(
                !remote_sql.contains("double_it"),
                "SQLite has no `double_it`; the user function must stay above the federated scan, \
                 but the pushed-down SQL was:\n{remote_sql}\nfull plan:\n{plan}"
            );

            let query = "SELECT id, double_it(x) AS doubled FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            let expected = [
                "+----+---------+",
                "| id | doubled |",
                "+----+---------+",
                "| 1  | 20      |",
                "| 2  | 40      |",
                "| 3  | 60      |",
                "+----+---------+",
            ];
            assert_batches_eq!(expected, &accelerated);

            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "an accelerated user function must answer what local evaluation answers"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// The wrong-rows half of #13726 / #13810.
///
/// `SQLite` has an `unlikely` of its own, so before the fix this query answered
/// `10, 20, 30` from the accelerated dataset and `20, 40, 60` from the
/// unaccelerated one — the same SQL over the same rows, two different answers,
/// no error anywhere.
#[tokio::test]
async fn a_user_function_the_backend_also_has_is_not_pushed_down() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let rt = runtime_with(
                "user_function_pushdown_shadowing",
                shadowing_unlikely(),
                &dir.path().join("numbers.csv"),
            )
            .await?;

            let query = "SELECT id, unlikely(x) AS v FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            // The user function's own body is `x * 2`; SQLite's `unlikely` would
            // answer `x`. Asserting the values, not just that the two agree, so
            // a future change that pushed the call down to *both* sides could
            // not satisfy this test.
            let expected = [
                "+----+----+",
                "| id | v  |",
                "+----+----+",
                "| 1  | 20 |",
                "| 2  | 40 |",
                "| 3  | 60 |",
                "+----+----+",
            ];
            assert_batches_eq!(expected, &accelerated);
            assert_batches_eq!(expected, &local);

            rt.shutdown().await;
            Ok(())
        })
        .await
}
