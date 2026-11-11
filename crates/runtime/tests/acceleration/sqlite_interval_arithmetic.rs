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

//! Interval arithmetic on a `SQLite` acceleration answers what local
//! evaluation answers (#14714). A projected shift reaches the engine through
//! `datetime()` for a timestamp, which keeps its time of day, and `date()` for
//! a date. Each case is diffed against the unaccelerated source, and three are
//! also pinned to their rows.

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::{csv_dataset, load_runtime_datasets};
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{
        federated_sql, register_test_connectors, run_query, test_request_context, to_pretty_display,
    },
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// Timestamps with a time of day that a date-only shift discards, and the
/// days they fall on.
const ROWS: &str = "id,ts,d\n\
                    1,2026-10-02T12:34:56,2026-10-02\n\
                    2,2026-10-02T00:30:00,2026-10-02\n\
                    3,2026-01-31T23:59:59,2026-01-31\n";

/// Every shape of the report, a whole-day shift of a timestamp, and the date
/// shifts a `datetime()` rendering would break: a shifted date projected, and
/// compared with a date literal. A case with rows is also pinned to them, so
/// its agreement is not two engines agreeing on a wrong answer.
const CASES: &[(&str, Option<&[&str]>)] = &[
    // A date-only shift answered midnight.
    (
        "SELECT id, ts - INTERVAL '1 hour' AS shifted FROM {table} ORDER BY id",
        Some(&[
            "+----+---------------------+",
            "| id | shifted             |",
            "+----+---------------------+",
            "| 1  | 2026-10-02T11:34:56 |",
            "| 2  | 2026-10-01T23:30:00 |",
            "| 3  | 2026-01-31T22:59:59 |",
            "+----+---------------------+",
        ]),
    ),
    (
        "SELECT id, ts + INTERVAL '1 day' AS shifted FROM {table} ORDER BY id",
        None,
    ),
    (
        "SELECT id, ts - INTERVAL '1 day' AS shifted FROM {table} ORDER BY id",
        None,
    ),
    // A date-only shift kept row 1, whose shifted timestamp is 11:34:56.
    (
        "SELECT id FROM {table} WHERE ts - INTERVAL '1 hour' < TIMESTAMP '2026-10-02 10:00:00' ORDER BY id",
        Some(&["+----+", "| id |", "+----+", "| 2  |", "| 3  |", "+----+"]),
    ),
    (
        "SELECT id FROM {table} WHERE ts + INTERVAL '1 day' > TIMESTAMP '2026-10-03 12:00:00' ORDER BY id",
        None,
    ),
    (
        "SELECT id, d + INTERVAL '1 day' AS next_day FROM {table} ORDER BY id",
        None,
    ),
    (
        "SELECT id FROM {table} WHERE d + INTERVAL '1 day' = DATE '2026-10-03' ORDER BY id",
        Some(&["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"]),
    ),
    (
        "SELECT id FROM {table} WHERE d - INTERVAL '1 day' <= DATE '2026-10-01' ORDER BY id",
        None,
    ),
];

async fn interval_arithmetic_matches_local_evaluation(engine: &str) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("rows.csv");
            std::fs::write(&csv, ROWS)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("interval_arithmetic")
                .with_dataset(csv_dataset(&from, "accelerated", Some(engine)))
                .with_dataset(csv_dataset(&from, "local", None))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            for (sql, rows) in CASES {
                let accelerated = run_query(&rt, &sql.replace("{table}", "accelerated")).await?;
                let local = run_query(&rt, &sql.replace("{table}", "local")).await?;
                assert_eq!(
                    to_pretty_display(&accelerated)?.to_string(),
                    to_pretty_display(&local)?.to_string(),
                    "{engine}: `{sql}` must agree with local evaluation"
                );
                if let Some(rows) = rows {
                    assert_batches_eq!(*rows, &accelerated);
                }
            }

            // The shifts reach the engine, each through the function its operand's
            // type calls for, so the agreement above is not local evaluation
            // agreeing with itself. A projected shift is federated on both
            // engines; a filter on one is kept above the `SQLite` scan.
            for (sql, rendering, not_rendering) in [
                (
                    "EXPLAIN SELECT id, ts - INTERVAL '1 hour' AS shifted FROM accelerated",
                    "datetime(",
                    "date(",
                ),
                (
                    "EXPLAIN SELECT id, d + INTERVAL '1 day' AS next_day FROM accelerated",
                    "date(",
                    "datetime(",
                ),
            ] {
                let plan = run_query(&rt, sql).await?;
                let remote_sql = federated_sql(&plan, "rewritten_executor_sql=")?;
                assert!(
                    remote_sql.contains(rendering) && !remote_sql.contains(not_rendering),
                    "{engine}: the shift must reach the engine as {rendering}…) and not as \
                     {not_rendering}…); the SQL sent was:\n{remote_sql}\nplan:\n{}",
                    to_pretty_display(&plan)?
                );
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_interval_arithmetic_keeps_the_time_of_day() -> Result<(), anyhow::Error> {
    interval_arithmetic_matches_local_evaluation("sqlite").await
}
