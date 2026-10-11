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

//! The unparser drops two clauses on their way to `PostgreSQL`: `IGNORE NULLS`
//! (on a window or an aggregate), and an aggregate's argument-list `ORDER BY`
//! unless it can be written as `WITHIN GROUP`. A federated `lag(v) IGNORE NULLS`
//! came back respecting nulls, and `array_agg(id ORDER BY id DESC)` in
//! `PostgreSQL`'s own order, with no error. Each query here runs against the
//! federated table and against an Arrow acceleration of the same table, which
//! `DataFusion` evaluates itself, and must agree with it and with pinned rows.

use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;
use datafusion::assert_batches_eq;
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{self, get_pg_params},
    utils::{
        register_test_connectors, run_query, runtime_ready_check, test_request_context,
        to_pretty_display,
    },
};

/// A query, the SQL the federated scan must contain, the SQL it must not
/// contain, and the rows it returns.
type ClauseCase<'a> = (&'a str, &'a [&'a str], &'a [&'a str], &'a [&'a str]);

fn dataset(port: usize, name: &str, accelerated: bool) -> Dataset {
    let mut ds = Dataset::new("postgres:clause_values".to_string(), name.to_string());
    ds.params = Some(Params::from_string_map(
        get_pg_params(port)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect::<HashMap<String, String>>(),
    ));
    if accelerated {
        ds.acceleration = Some(Acceleration {
            enabled: true,
            engine: Some("arrow".to_string()),
            refresh_mode: Some(RefreshMode::Full),
            ..Acceleration::default()
        });
    }
    ds
}

/// NULLs between non-NULL values, so a call that ignores nulls answers
/// differently from the same call respecting them.
async fn seed(port: usize) -> Result<(), anyhow::Error> {
    let pool = common::get_postgres_connection_pool(port, None).await?;
    let conn = pool
        .connect_direct()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    for statement in [
        "DROP TABLE IF EXISTS clause_values",
        "CREATE TABLE clause_values (id integer PRIMARY KEY, grp text NOT NULL, v integer)",
        "INSERT INTO clause_values VALUES \
         (1, 'a', 10), (2, 'a', NULL), (3, 'b', 30), (4, 'b', NULL), (5, 'b', 50)",
    ] {
        conn.conn.execute(statement, &[]).await?;
    }
    Ok(())
}

/// The SQL `plan` sends to `PostgreSQL`, one statement per line: a federated
/// subtree's `base_sql`, or the `sql` of a scan the table provider runs itself,
/// which is what remains when nothing above the scan can federate.
fn pushed_down_sql(plan: &str) -> String {
    plan.lines()
        .filter_map(|line| {
            line.split_once("base_sql=")
                .or_else(|| line.split_once("SqlExec sql="))
                // The rest of the plan's table row: drop the cell padding and border.
                .map(|(_, sql)| {
                    sql.trim_end_matches(|c: char| c == '|' || c.is_whitespace())
                        .to_string()
                })
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[tokio::test]
async fn postgres_keeps_calls_with_unrendered_clauses_local() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let running_container = common::start_postgres_docker_container().await?;
            let port = usize::from(running_container.host_port(5432)?);
            seed(port).await?;

            register_test_connectors().await;
            let app = AppBuilder::new("postgres_aggregate_clause_pushdown")
                .with_dataset(dataset(port, "federated", false))
                .with_dataset(dataset(port, "local", true))
                .build();
            configure_test_datafusion();
            let rt = Arc::new(runtime::Runtime::builder().with_app(app).build().await);
            tokio::select! {
                () = tokio::time::sleep(Duration::from_mins(2)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for the datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }
            runtime_ready_check(&rt).await;

            let cases: [ClauseCase; 11] = [
                // The unparser drops `IGNORE NULLS` from a window, so each of
                // these reached `PostgreSQL` respecting nulls.
                (
                    "SELECT id, lag(v) IGNORE NULLS OVER (ORDER BY id) AS w \
                     FROM {table} ORDER BY id",
                    &[],
                    &["lag(", "IGNORE"],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  |    |",
                        "| 2  | 10 |",
                        "| 3  | 10 |",
                        "| 4  | 30 |",
                        "| 5  | 30 |",
                        "+----+----+",
                    ],
                ),
                (
                    "SELECT id, lead(v) IGNORE NULLS OVER (ORDER BY id) AS w \
                     FROM {table} ORDER BY id",
                    &[],
                    &["lead("],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  | 30 |",
                        "| 2  | 30 |",
                        "| 3  | 50 |",
                        "| 4  | 50 |",
                        "| 5  |    |",
                        "+----+----+",
                    ],
                ),
                (
                    "SELECT id, first_value(v) IGNORE NULLS OVER (ORDER BY id \
                     ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w \
                     FROM {table} ORDER BY id",
                    &[],
                    &["first_value("],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  | 10 |",
                        "| 2  | 10 |",
                        "| 3  | 30 |",
                        "| 4  | 30 |",
                        "| 5  | 50 |",
                        "+----+----+",
                    ],
                ),
                (
                    "SELECT id, last_value(v) IGNORE NULLS OVER (ORDER BY id \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w \
                     FROM {table} ORDER BY id",
                    &[],
                    &["last_value("],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  | 10 |",
                        "| 2  | 10 |",
                        "| 3  | 30 |",
                        "| 4  | 30 |",
                        "| 5  | 50 |",
                        "+----+----+",
                    ],
                ),
                (
                    "SELECT id, nth_value(v, 2) IGNORE NULLS OVER (ORDER BY id \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w \
                     FROM {table} ORDER BY id",
                    &[],
                    &["nth_value("],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  | 30 |",
                        "| 2  | 30 |",
                        "| 3  | 30 |",
                        "| 4  | 30 |",
                        "| 5  | 30 |",
                        "+----+----+",
                    ],
                ),
                // The unparser drops an argument-list `ORDER BY`, so these came
                // back in the order `PostgreSQL` produced.
                (
                    "SELECT array_agg(id ORDER BY id DESC) AS a FROM {table}",
                    &[],
                    &["array_agg("],
                    &[
                        "+-----------------+",
                        "| a               |",
                        "+-----------------+",
                        "| [5, 4, 3, 2, 1] |",
                        "+-----------------+",
                    ],
                ),
                (
                    "SELECT string_agg(grp, ',' ORDER BY id DESC) AS s FROM {table}",
                    &[],
                    &["string_agg("],
                    &[
                        "+-----------+",
                        "| s         |",
                        "+-----------+",
                        "| b,b,b,a,a |",
                        "+-----------+",
                    ],
                ),
                (
                    "SELECT grp, array_agg(id ORDER BY id DESC) AS a \
                     FROM {table} GROUP BY grp ORDER BY grp",
                    &[],
                    &["array_agg("],
                    &[
                        "+-----+-----------+",
                        "| grp | a         |",
                        "+-----+-----------+",
                        "| a   | [2, 1]    |",
                        "| b   | [5, 4, 3] |",
                        "+-----+-----------+",
                    ],
                ),
                // Controls that still federate, so the refusals above are not
                // every aggregate and window falling back: the same window
                // respecting nulls, an `ORDER BY` that cannot change a `sum`, and
                // one the unparser writes as `WITHIN GROUP`.
                (
                    "SELECT id, lag(v) OVER (ORDER BY id) AS w FROM {table} ORDER BY id",
                    &[r#"lag("clause_values"."v")"#],
                    &[],
                    &[
                        "+----+----+",
                        "| id | w  |",
                        "+----+----+",
                        "| 1  |    |",
                        "| 2  | 10 |",
                        "| 3  |    |",
                        "| 4  | 30 |",
                        "| 5  |    |",
                        "+----+----+",
                    ],
                ),
                (
                    "SELECT grp, sum(v ORDER BY id) AS s FROM {table} GROUP BY grp ORDER BY grp",
                    &[r#"sum("clause_values"."v")"#],
                    &[],
                    &[
                        "+-----+----+",
                        "| grp | s  |",
                        "+-----+----+",
                        "| a   | 10 |",
                        "| b   | 80 |",
                        "+-----+----+",
                    ],
                ),
                (
                    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS p FROM {table}",
                    &["WITHIN GROUP"],
                    &[],
                    &["+------+", "| p    |", "+------+", "| 30.0 |", "+------+"],
                ),
            ];

            for (query, pushed, kept_local, expected) in cases {
                let plan = to_pretty_display(
                    &run_query(
                        &rt,
                        &format!("EXPLAIN {}", query.replace("{table}", "federated")),
                    )
                    .await?,
                )?
                .to_string();
                let remote_sql = pushed_down_sql(&plan);
                assert!(
                    !remote_sql.is_empty(),
                    "the scan under `{query}` must still be federated to PostgreSQL; plan was:\n{plan}"
                );
                for rendering in pushed {
                    assert!(
                        remote_sql.contains(rendering),
                        "`{query}` must reach PostgreSQL as {rendering}; the SQL sent was:\n{remote_sql}"
                    );
                }
                for clause in kept_local {
                    assert!(
                        !remote_sql.contains(clause),
                        "{clause} must not be sent to PostgreSQL; the SQL sent was:\n{remote_sql}"
                    );
                }

                let federated = run_query(&rt, &query.replace("{table}", "federated")).await?;
                let local = run_query(&rt, &query.replace("{table}", "local")).await?;
                assert_batches_eq!(expected, &federated);
                assert_eq!(
                    to_pretty_display(&federated)?.to_string(),
                    to_pretty_display(&local)?.to_string(),
                    "federated `{query}` must agree with local evaluation"
                );
            }

            rt.shutdown().await;
            running_container.remove().await?;
            Ok(())
        })
        .await
}
