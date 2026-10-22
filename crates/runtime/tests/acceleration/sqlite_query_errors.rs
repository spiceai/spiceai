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

//! An error `SQLite` raises while it evaluates a query federated to a `SQLite`
//! acceleration fails the query. It is never read as the end of the rows,
//! which answered an empty or truncated result with no error (#14661).

use app::AppBuilder;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::load_runtime_datasets;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, to_pretty_display},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// Three `BIGINT`s whose sum passes `i64::MAX`, and an `x` whose last value has
/// no absolute value in an `i64`: `SQLite` raises `integer overflow` for both.
const ROWS: &str = "id,v,x\n\
                    1,9000000000000000000,1\n\
                    2,9000000000000000000,-2\n\
                    3,9000000000000000000,-9223372036854775808\n";

fn sqlite_accelerated(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("sqlite".to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

#[tokio::test]
async fn sqlite_accelerator_fails_a_query_sqlite_aborts() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("overflow.csv");
            std::fs::write(&csv, ROWS)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("sqlite_query_errors")
                .with_dataset(sqlite_accelerated(&from, "accelerated"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The rows are there, so an empty answer below is not an empty table.
            assert_eq!(
                to_pretty_display(
                    &run_query(&rt, "SELECT id, x FROM accelerated ORDER BY id").await?
                )?
                .to_string(),
                "+----+----------------------+\n\
                 | id | x                    |\n\
                 +----+----------------------+\n\
                 | 1  | 1                    |\n\
                 | 2  | -2                   |\n\
                 | 3  | -9223372036854775808 |\n\
                 +----+----------------------+"
            );

            // An ungrouped `SUM` fails on the only row it produces, and `abs`
            // on the third row of a scan, after two rows came back.
            for (sql, rendering) in [
                ("SELECT sum(v) AS s FROM accelerated", "sum("),
                ("SELECT id, abs(x) AS a FROM accelerated", "abs("),
            ] {
                // `SQLite` evaluates the call, so the error below is its own.
                let plan = to_pretty_display(&run_query(&rt, &format!("EXPLAIN {sql}")).await?)?
                    .to_string();
                assert!(
                    plan.contains("VirtualExecutionPlan") && plan.contains(rendering),
                    "`{sql}` must be federated to SQLite; plan was:\n{plan}"
                );

                match run_query(&rt, sql).await {
                    Ok(batches) => panic!(
                        "`{sql}` must fail with SQLite's integer overflow, but answered:\n{}",
                        to_pretty_display(&batches)?
                    ),
                    Err(e) => assert!(
                        e.to_string().contains("integer overflow"),
                        "`{sql}` must fail with SQLite's integer overflow, but failed with: {e}"
                    ),
                }
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}
