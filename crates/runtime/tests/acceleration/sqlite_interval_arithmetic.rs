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

//! Interval arithmetic federated to a `SQLite` or Turso acceleration is
//! rendered through `datetime()` for a timestamp and `date()` for a date, so
//! it keeps a timestamp's time of day and still compares a shifted date equal
//! to a date literal (#14714). Each query is diffed against the same query
//! over the unaccelerated source.

use app::AppBuilder;
use datafusion::assert_batches_eq;
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

/// Timestamps with a time of day that a date-only shift discards, and the
/// days they fall on.
const ROWS: &str = "id,ts,d\n\
                    1,2026-10-02T12:34:56,2026-10-02\n\
                    2,2026-10-02T00:30:00,2026-10-02\n\
                    3,2026-01-31T23:59:59,2026-01-31\n";

/// Every shape of the report, a whole-day shift of a timestamp, and the date
/// shifts a `datetime()` rendering would break: a shifted date projected, and
/// compared with a date literal.
const CASES: &[&str] = &[
    "SELECT id, ts - INTERVAL '1 hour' AS shifted FROM {table} ORDER BY id",
    "SELECT id, ts + INTERVAL '1 day' AS shifted FROM {table} ORDER BY id",
    "SELECT id, ts - INTERVAL '1 day' AS shifted FROM {table} ORDER BY id",
    "SELECT id FROM {table} WHERE ts - INTERVAL '1 hour' < TIMESTAMP '2026-10-02 10:00:00' ORDER BY id",
    "SELECT id FROM {table} WHERE ts + INTERVAL '1 day' > TIMESTAMP '2026-10-03 12:00:00' ORDER BY id",
    "SELECT id, d + INTERVAL '1 day' AS next_day FROM {table} ORDER BY id",
    "SELECT id FROM {table} WHERE d + INTERVAL '1 day' = DATE '2026-10-03' ORDER BY id",
    "SELECT id FROM {table} WHERE d - INTERVAL '1 day' <= DATE '2026-10-01' ORDER BY id",
];

fn dataset(from: &str, name: &str, engine: Option<&str>) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = engine.map(|engine| Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

/// The SQL each federated scan in `plan` sends to the engine after its own
/// rewrites, one per line.
fn executor_sql(plan: &str) -> String {
    plan.split("rewritten_executor_sql=")
        .skip(1)
        .map(|tail| {
            tail.split('\n')
                .next()
                .unwrap_or_default()
                .trim_end_matches(|c: char| c == '|' || c.is_whitespace())
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

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
                .with_dataset(dataset(&from, "accelerated", Some(engine)))
                .with_dataset(dataset(&from, "local", None))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            for sql in CASES {
                let accelerated = run_query(&rt, &sql.replace("{table}", "accelerated")).await?;
                let local = run_query(&rt, &sql.replace("{table}", "local")).await?;
                assert_eq!(
                    to_pretty_display(&accelerated)?.to_string(),
                    to_pretty_display(&local)?.to_string(),
                    "{engine}: `{sql}` must agree with local evaluation"
                );
            }

            // The shifts reach the engine, each through the function its operand's
            // type calls for, so the agreement above is not local evaluation
            // agreeing with itself.
            for (sql, rendering) in [
                (
                    "EXPLAIN SELECT id FROM accelerated \
                     WHERE ts - INTERVAL '1 hour' < TIMESTAMP '2026-10-02 10:00:00'",
                    "datetime(",
                ),
                (
                    "EXPLAIN SELECT id FROM accelerated WHERE d + INTERVAL '1 day' = DATE '2026-10-03'",
                    "date(",
                ),
            ] {
                let plan = to_pretty_display(&run_query(&rt, sql).await?)?.to_string();
                let remote_sql = executor_sql(&plan);
                assert!(
                    remote_sql.contains(rendering),
                    "{engine}: the shift must reach the engine as {rendering}…); the SQL sent was:\n{remote_sql}\nplan:\n{plan}"
                );
            }

            // Pinned rows, so the agreement above is not two engines agreeing on
            // a wrong answer. A date-only shift kept row 1, whose shifted
            // timestamp is 11:34:56.
            assert_batches_eq!(
                ["+----+", "| id |", "+----+", "| 2  |", "| 3  |", "+----+"],
                &run_query(
                    &rt,
                    "SELECT id FROM accelerated \
                     WHERE ts - INTERVAL '1 hour' < TIMESTAMP '2026-10-02 10:00:00' ORDER BY id",
                )
                .await?
            );
            assert_batches_eq!(
                ["+----+", "| id |", "+----+", "| 1  |", "| 2  |", "+----+"],
                &run_query(
                    &rt,
                    "SELECT id FROM accelerated WHERE d + INTERVAL '1 day' = DATE '2026-10-03' ORDER BY id",
                )
                .await?
            );

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

#[cfg(feature = "turso")]
#[tokio::test]
async fn turso_interval_arithmetic_keeps_the_time_of_day() -> Result<(), anyhow::Error> {
    interval_arithmetic_matches_local_evaluation("turso").await
}
