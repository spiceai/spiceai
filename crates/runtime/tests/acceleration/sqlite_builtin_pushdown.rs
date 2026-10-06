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

//! `DataFusion` built-ins that `SQLite` cannot evaluate faithfully must stay
//! above the federated scan and agree with local evaluation. `SQLite` has no
//! unparser-dialect seam, so the deny-list is the only lever.

use app::AppBuilder;
use datafusion::assert_batches_eq;
use runtime::Runtime;
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::acceleration::load_runtime_datasets;
use crate::{
    configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, test_request_context, to_pretty_display},
};

const LOAD_TIMEOUT: Duration = Duration::from_mins(1);

/// Rows that make each `SQLite` refusal observable: a NULL region (concat),
/// a non-ASCII name (`upper`/`lower`), ASCII padding, and a timestamp
/// (`date_part` / `date_trunc`).
fn write_orders_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,customer,region,qty,ts\n\
         1,alice,eu,10,2026-01-15T10:00:00Z\n\
         2,  bob  ,,20,2026-02-01T00:00:00Z\n\
         3,Ångström,eu,15,2026-03-01T12:00:00Z\n\
         4,alice,us,10,2026-01-20T08:00:00Z\n",
    )?;
    Ok(())
}

/// The SQL each federated scan in `plan` sends to `SQLite`, one per line.
fn pushed_down_sql(plan: &str) -> String {
    plan.split("base_sql=")
        .skip(1)
        .map(|tail| tail.split('\n').next().unwrap_or_default().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

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

fn unaccelerated(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset
}

/// Every built-in `SQLite` used to fail or answer wrongly is evaluated locally
/// and agrees with the unaccelerated engine. A control projection still
/// federates, so the negative `base_sql` assertions are not vacuous.
#[tokio::test]
async fn sqlite_accelerator_evaluates_unfaithful_builtins_locally() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("orders.csv");
            write_orders_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("sqlite_builtin_pushdown")
                .with_dataset(sqlite_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The scan still federates: a denied function is evaluated above it.
            let control = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT customer FROM accelerated").await?,
            )?
            .to_string();
            assert!(
                !pushed_down_sql(&control).is_empty(),
                "a plain column must still be federated to SQLite; plan was:\n{control}"
            );

            // `None` — must not appear in the SQL sent to SQLite.
            // Each query must succeed and match local evaluation.
            let cases: &[(&str, Option<&str>)] = &[
                (
                    "SELECT id, upper(customer) AS u, lower(customer) AS l FROM {table} ORDER BY id",
                    Some("upper("),
                ),
                (
                    "SELECT id, concat(customer, '-', region) AS c FROM {table} ORDER BY id",
                    Some("concat("),
                ),
                (
                    "SELECT id, concat(customer, CAST(NULL AS VARCHAR)) AS c \
                     FROM {table} ORDER BY id",
                    Some("concat("),
                ),
                (
                    // Keep `id` in the projection: a constant-only SELECT over
                    // the accelerator is an empty federated scan, and the
                    // SQLite row decoder panics on a zero-field schema.
                    "SELECT id, concat(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) AS c \
                     FROM {table} WHERE id = 1",
                    Some("concat("),
                ),
                (
                    "SELECT id, concat(customer, '') AS c FROM {table} ORDER BY id",
                    Some("concat("),
                ),
                (
                    "SELECT id, concat('', region) AS c FROM {table} ORDER BY id",
                    Some("concat("),
                ),
                (
                    "SELECT id, to_hex(id) AS h FROM {table} ORDER BY id",
                    Some("to_hex("),
                ),
                (
                    "SELECT id, md5(customer) AS d FROM {table} ORDER BY id",
                    Some("md5("),
                ),
                (
                    "SELECT id, sha256(customer) AS d FROM {table} ORDER BY id",
                    Some("sha256("),
                ),
                (
                    "SELECT id, date_part('month', try_cast(ts AS timestamp)) AS m \
                     FROM {table} ORDER BY id",
                    Some("date_part("),
                ),
                (
                    "SELECT id, extract(month FROM try_cast(ts AS timestamp)) AS m \
                     FROM {table} ORDER BY id",
                    Some("date_part("),
                ),
                (
                    "SELECT id, date_trunc('month', try_cast(ts AS timestamp)) AS m \
                     FROM {table} ORDER BY id",
                    Some("date_trunc("),
                ),
                (
                    "SELECT id FROM {table} WHERE customer ILIKE '%alice%' ORDER BY id",
                    Some("ILIKE"),
                ),
                (
                    "SELECT id FROM {table} WHERE customer LIKE '%ALICE%' ORDER BY id",
                    Some("LIKE"),
                ),
                (
                    "SELECT id, regexp_like(customer, 'ali') AS m FROM {table} ORDER BY id",
                    Some("regexp_like("),
                ),
                (
                    "SELECT id, regexp_replace(customer, 'a', 'X') AS r FROM {table} ORDER BY id",
                    Some("regexp_replace("),
                ),
                ("SELECT median(qty) AS m FROM {table}", Some("median(")),
                (
                    "SELECT approx_distinct(customer) AS n FROM {table}",
                    Some("approx_distinct("),
                ),
                (
                    "SELECT string_agg(DISTINCT customer, '|' ORDER BY customer) AS customers \
                     FROM {table} WHERE region = 'eu'",
                    Some("string_agg("),
                ),
            ];

            for (sql, forbidden) in cases {
                let accelerated =
                    run_query(&rt, &sql.replace("{table}", "accelerated")).await?;
                let local = run_query(&rt, &sql.replace("{table}", "local")).await?;
                assert_eq!(
                    to_pretty_display(&accelerated)?.to_string(),
                    to_pretty_display(&local)?.to_string(),
                    "`{sql}` must agree with local evaluation"
                );

                if let Some(name) = forbidden {
                    let plan = to_pretty_display(
                        &run_query(
                            &rt,
                            &format!("EXPLAIN {}", sql.replace("{table}", "accelerated")),
                        )
                        .await?,
                    )?
                    .to_string();
                    let remote_sql = pushed_down_sql(&plan);
                    assert!(
                        !remote_sql.contains(name),
                        "{name} must not reach SQLite; the SQL sent was:\n{remote_sql}\nplan:\n{plan}"
                    );
                }
            }

            // Pinned values, so the agreements above are not two engines
            // agreeing on a wrong answer. `SQLite`'s `concat` would have
            // answered `'  bob  -'` instead of NULL.
            // DataFusion folds `ö` to `Ö`. SQLite's ASCII-only `upper` would
            // have left `ö` in place (`ÅNGSTRöM`).
            assert_batches_eq!(
                [
                    "+----------+",
                    "| u        |",
                    "+----------+",
                    "| ÅNGSTRÖM |",
                    "+----------+",
                ],
                &run_query(
                    &rt,
                    "SELECT upper(customer) AS u FROM accelerated WHERE id = 3",
                )
                .await?
            );
            assert_batches_eq!(
                [
                    "+----+------+",
                    "| id | isn  |",
                    "+----+------+",
                    "| 2  | true |",
                    "+----+------+",
                ],
                &run_query(
                    &rt,
                    "SELECT id, concat(customer, '-', region) IS NULL AS isn \
                     FROM accelerated WHERE id = 2"
                )
                .await?
            );
            assert_batches_eq!(
                [
                    "+------+",
                    "| isn  |",
                    "+------+",
                    "| true |",
                    "+------+",
                ],
                &run_query(
                    &rt,
                    "SELECT concat(customer, CAST(NULL AS VARCHAR)) IS NULL AS isn \
                     FROM accelerated WHERE id = 1"
                )
                .await?
            );
            assert_batches_eq!(
                [
                    "+----+------+",
                    "| id | isn  |",
                    "+----+------+",
                    "| 1  | true |",
                    "+----+------+",
                ],
                &run_query(
                    &rt,
                    "SELECT id, concat(CAST(NULL AS VARCHAR), CAST(NULL AS VARCHAR)) IS NULL AS isn \
                     FROM accelerated WHERE id = 1"
                )
                .await?
            );
            assert_batches_eq!(
                [
                    "+-------+",
                    "| c     |",
                    "+-------+",
                    "| alice |",
                    "+-------+",
                ],
                &run_query(
                    &rt,
                    "SELECT concat(customer, '') AS c FROM accelerated WHERE id = 1"
                )
                .await?
            );
            // SQLite LIKE folds ASCII case, so a federated
            // `customer LIKE '%ALICE%'` would have kept the two `alice` rows.
            // Distinct SQL from the loop case so the results cache cannot
            // return a schema-less empty batch.
            assert_batches_eq!(
                [
                    "+----+----------+",
                    "| id | customer |",
                    "+----+----------+",
                    "+----+----------+",
                ],
                &run_query(
                    &rt,
                    "SELECT id, customer FROM accelerated WHERE customer LIKE '%ALICE%' ORDER BY id"
                )
                .await?
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}
