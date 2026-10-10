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
        .map(|tail| {
            // The rest of the plan's table row: drop the cell padding and border.
            tail.split('\n')
                .next()
                .unwrap_or_default()
                .trim_end_matches(|c: char| c == '|' || c.is_whitespace())
                .to_string()
        })
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
                    // A constant-only SELECT federates a scan with no column;
                    // `sqlite_accelerator_answers_a_constant_only_select`
                    // covers that shape.
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
                    "SELECT id, encode(sha256(customer), 'hex') AS h FROM {table} ORDER BY id",
                    Some("encode("),
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
                (
                    "SELECT id, regexp_match(customer, '(a)') AS m FROM {table} ORDER BY id",
                    Some("regexp_match("),
                ),
                (
                    "SELECT id, regexp_instr(customer, 'a') AS i FROM {table} ORDER BY id",
                    Some("regexp_instr("),
                ),
                (
                    "SELECT id, regexp_count(customer, 'a') AS c FROM {table} ORDER BY id",
                    Some("regexp_count("),
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
                // `SQLite` has no `array_agg`, and `first_value`, `last_value`
                // and `nth_value` only as window functions: each aggregate
                // failed remotely.
                (
                    "SELECT array_agg(customer ORDER BY id) AS a FROM {table}",
                    Some("array_agg("),
                ),
                (
                    "SELECT first_value(customer ORDER BY id) AS f FROM {table}",
                    Some("first_value("),
                ),
                (
                    "SELECT last_value(customer ORDER BY id) AS l FROM {table}",
                    Some("last_value("),
                ),
                (
                    "SELECT nth_value(customer, 2 ORDER BY id) AS n FROM {table}",
                    Some("nth_value("),
                ),
                // `SQLite` has only `count`, `sum`, `avg`, `min` and `max` of
                // `DataFusion`'s aggregates, each over at most one argument. Every
                // other aggregate failed with `no such function` (`count(a, b)`
                // with `wrong number of arguments`), over a window too.
                ("SELECT stddev(qty) AS s FROM {table}", Some("stddev(")),
                ("SELECT var_pop(qty) AS v FROM {table}", Some("var_pop(")),
                ("SELECT corr(qty, id) AS c FROM {table}", Some("corr(")),
                (
                    "SELECT bool_and(qty > 10) AS b FROM {table}",
                    Some("bool_and("),
                ),
                ("SELECT bit_or(qty) AS b FROM {table}", Some("bit_or(")),
                (
                    "SELECT approx_median(qty) AS m FROM {table}",
                    Some("approx_median("),
                ),
                (
                    "SELECT count(region, customer) AS n FROM {table}",
                    Some("count("),
                ),
                (
                    "SELECT id, stddev(qty) OVER (PARTITION BY region) AS s \
                     FROM {table} ORDER BY id",
                    Some("stddev("),
                ),
                // `SQLite` refuses `DISTINCT` in any window.
                (
                    "SELECT id, count(DISTINCT qty) OVER (PARTITION BY region) AS n \
                     FROM {table} ORDER BY id",
                    Some("count("),
                ),
                // The unparser drops `IGNORE NULLS` from a window, so each of these
                // reached `SQLite` respecting nulls and landed on the NULL region.
                (
                    "SELECT id, lag(region) IGNORE NULLS OVER (ORDER BY id) AS r \
                     FROM {table} ORDER BY id",
                    Some("lag("),
                ),
                (
                    "SELECT id, lead(region) IGNORE NULLS OVER (ORDER BY id) AS r \
                     FROM {table} ORDER BY id",
                    Some("lead("),
                ),
                (
                    "SELECT id, last_value(region) IGNORE NULLS OVER (ORDER BY id \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r \
                     FROM {table} ORDER BY id",
                    Some("last_value("),
                ),
                (
                    "SELECT id, nth_value(region, 2) IGNORE NULLS OVER (ORDER BY id \
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r \
                     FROM {table} ORDER BY id",
                    Some("nth_value("),
                ),
                // `SQLite` has no `ROLLUP`.
                (
                    "SELECT region, sum(qty) AS s FROM {table} GROUP BY ROLLUP(region) \
                     ORDER BY region NULLS LAST, s",
                    Some("ROLLUP"),
                ),
                // `SQLite`'s own aggregates and window functions, which still reach
                // it (asserted below): its answers must agree too.
                (
                    "SELECT region, count(qty) AS n, sum(qty) AS s, avg(qty) AS a, \
                     min(qty) AS lo, max(qty) AS hi FROM {table} \
                     GROUP BY region ORDER BY region NULLS LAST",
                    None,
                ),
                (
                    "SELECT id, row_number() OVER (ORDER BY id) AS n, \
                     lag(region) OVER (ORDER BY id) AS r FROM {table} ORDER BY id",
                    None,
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

            // The aggregates and window functions `SQLite` has still reach it,
            // so the refusals above are not every aggregate falling back.
            for (sql, renderings) in [
                (
                    "EXPLAIN SELECT region, count(qty) AS n, sum(qty) AS s, avg(qty) AS a, \
                     min(qty) AS lo, max(qty) AS hi FROM accelerated GROUP BY region",
                    &["count(", "sum(", "avg(", "min(", "max("][..],
                ),
                (
                    "EXPLAIN SELECT id, row_number() OVER (ORDER BY id) AS n, \
                     lag(region) OVER (ORDER BY id) AS r FROM accelerated",
                    &["row_number(", "lag("][..],
                ),
            ] {
                let plan = to_pretty_display(&run_query(&rt, sql).await?)?.to_string();
                let remote_sql = pushed_down_sql(&plan);
                for rendering in renderings {
                    assert!(
                        remote_sql.contains(rendering),
                        "{rendering} is SQLite's own and must reach it; the SQL sent was:\n{remote_sql}"
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
            // 10 | 20 | 15 | 10. SQLite failed with `no such function: bit_or`.
            assert_batches_eq!(
                ["+----+", "| b  |", "+----+", "| 31 |", "+----+"],
                &run_query(&rt, "SELECT bit_or(qty) AS b FROM accelerated").await?
            );
            // Row 3 skips the NULL region of row 2. Federated, it respected that
            // NULL and answered it.
            assert_batches_eq!(
                [
                    "+----+----+",
                    "| id | r  |",
                    "+----+----+",
                    "| 1  |    |",
                    "| 2  | eu |",
                    "| 3  | eu |",
                    "| 4  | eu |",
                    "+----+----+",
                ],
                &run_query(
                    &rt,
                    "SELECT id, lag(region) IGNORE NULLS OVER (ORDER BY id) AS r \
                     FROM accelerated ORDER BY id"
                )
                .await?
            );
            // SQLite LIKE folds ASCII case, so a federated
            // `customer LIKE '%ALICE%'` would have kept the two `alice` rows.
            // Zero-row answers arrive as no batches, which pretty-print as
            // `++` rather than an empty headered table.
            let like_alice = run_query(
                &rt,
                "SELECT id, customer FROM accelerated WHERE customer LIKE '%ALICE%' ORDER BY id",
            )
            .await?;
            assert_eq!(
                like_alice
                    .iter()
                    .map(arrow::array::RecordBatch::num_rows)
                    .sum::<usize>(),
                0,
                "LIKE '%ALICE%' must match no row; SQLite would have kept the two alice rows"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// TPC-H Q2's shape: a correlated scalar subquery over the `partsupp` table,
/// held above a join whose `part` side carries a `LIKE` `SQLite` cannot run
/// faithfully, so only part of the join federates.
fn write_part_and_partsupp_sources(part: &Path, partsupp: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        part,
        "p_partkey,p_type\n\
         1,LARGE BRASS\n\
         2,SMALL COPPER\n\
         3,ECONOMY BRASS\n\
         4,large brass\n",
    )?;
    std::fs::write(
        partsupp,
        "ps_partkey,ps_suppkey,ps_supplycost\n\
         1,10,5.0\n\
         1,11,3.0\n\
         2,12,7.0\n\
         3,13,9.0\n\
         3,14,9.0\n\
         4,15,1.0\n",
    )?;
    Ok(())
}

/// Regression test for the federation analyzer federating a correlated scalar
/// subquery on its own once the query around it does not federate whole. The
/// subquery's outer reference names `part`, which the statement it was federated
/// as never scans, and `DataFusion` refused the plan with "Correlated scalar
/// subquery must be aggregated to return at most one row" — TPC-H Q2 on a
/// `SQLite` accelerator, which returns 100 rows unaccelerated. The correlation
/// now stays with the query that binds it, and the answer matches local
/// evaluation, including the case-sensitive `LIKE` that skips `large brass`.
#[tokio::test]
async fn sqlite_accelerator_answers_a_correlated_subquery_above_a_partly_federated_join()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let part = dir.path().join("part.csv");
            let partsupp = dir.path().join("partsupp.csv");
            write_part_and_partsupp_sources(&part, &partsupp)?;
            let part = format!("file://{}", part.display());
            let partsupp = format!("file://{}", partsupp.display());

            let app = AppBuilder::new("sqlite_correlated_subquery_above_partial_join")
                .with_dataset(sqlite_accelerated(&part, "part"))
                .with_dataset(sqlite_accelerated(&partsupp, "partsupp"))
                .with_dataset(unaccelerated(&part, "part_local"))
                .with_dataset(unaccelerated(&partsupp, "partsupp_local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            let query = "SELECT p.p_partkey, ps.ps_suppkey, ps.ps_supplycost \
                         FROM {part} p JOIN {partsupp} ps ON p.p_partkey = ps.ps_partkey \
                         WHERE p.p_type LIKE '%BRASS' \
                           AND ps.ps_supplycost = ( \
                             SELECT min(ps2.ps_supplycost) FROM {partsupp} ps2 \
                             WHERE ps2.ps_partkey = p.p_partkey) \
                         ORDER BY p.p_partkey, ps.ps_suppkey";
            let accelerated_query = query
                .replace("{part}", "part")
                .replace("{partsupp}", "partsupp");
            let local_query = query
                .replace("{part}", "part_local")
                .replace("{partsupp}", "partsupp_local");

            let plan =
                to_pretty_display(&run_query(&rt, &format!("EXPLAIN {accelerated_query}")).await?)?
                    .to_string();
            let remote_sql = pushed_down_sql(&plan);
            assert!(
                !remote_sql.is_empty(),
                "the scans must still be federated to SQLite; plan was:\n{plan}"
            );
            assert!(
                !remote_sql.contains("LIKE"),
                "SQLite LIKE folds ASCII case and must stay local; the SQL sent was:\n{remote_sql}"
            );

            let accelerated = run_query(&rt, &accelerated_query).await?;
            let local = run_query(&rt, &local_query).await?;
            assert_batches_eq!(
                [
                    "+-----------+------------+---------------+",
                    "| p_partkey | ps_suppkey | ps_supplycost |",
                    "+-----------+------------+---------------+",
                    "| 1         | 11         | 3.0           |",
                    "| 3         | 13         | 9.0           |",
                    "| 3         | 14         | 9.0           |",
                    "+-----------+------------+---------------+",
                ],
                &accelerated
            );
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "the SQLite-accelerated answer must agree with local evaluation"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// A projection of constants only federates a scan with no column, which the
/// unparser renders as `SELECT 1 FROM …` because `SQLite` has no empty select
/// list. Until spiceai/datafusion-federation#91 the federation executor asked
/// `SQLite` for that statement under a zero-field schema: the row decoder
/// panicked indexing past it, and the dataset answered no query after that.
#[tokio::test]
async fn sqlite_accelerator_answers_a_constant_only_select() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("orders.csv");
            write_orders_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("sqlite_constant_only_select")
                .with_dataset(sqlite_accelerated(&from, "accelerated"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // `upper` stays local (`SQLite`'s folds only ASCII), so the scan
            // under it federates with no column.
            assert_batches_eq!(
                [
                    "+---+", "| u |", "+---+", "| A |", "| A |", "| A |", "| A |", "+---+",
                ],
                &run_query(&rt, "SELECT upper('a') AS u FROM accelerated").await?
            );
            // The dataset still answers afterwards.
            assert_batches_eq!(
                [
                    "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 3  |", "| 4  |", "+----+",
                ],
                &run_query(&rt, "SELECT id FROM accelerated ORDER BY id").await?
            );
            let plan = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT upper('a') AS u FROM accelerated").await?,
            )?
            .to_string();
            assert_eq!(
                pushed_down_sql(&plan),
                "SELECT 1 FROM `accelerated`",
                "plan:\n{plan}"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}
