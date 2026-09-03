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

//! `DataFusion` built-ins pushed down to a `DuckDB` accelerator must be
//! unparsed to a function `DuckDB` actually has, and must answer the same as
//! local evaluation.

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

/// The rows the trims are measured on: padded with spaces, padded with a
/// character set, padded with both, and NULL.
fn write_csv_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,name\n\
         1,\"  padded  \"\n\
         2,xyhelloyx\n\
         3,\"x hello x\"\n\
         4,\n",
    )?;
    Ok(())
}

/// A row padded with Unicode `Zs` separators rather than ASCII spaces:
/// `U+00A0`, then `U+2003`, then `U+3000`.
///
/// `btrim(str)` strips ASCII `U+0020` and nothing else, so all three rows come
/// back unchanged. `DuckDB`'s *one-argument* `trim` strips every `Zs`, which is
/// why the dialect renders the one-argument call as `trim(str, ' ')` — without
/// that, these rows would come back shortened and the accelerated dataset would
/// silently disagree with the unaccelerated one instead of erroring.
fn write_unicode_space_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,name\n\
         1,\"\u{a0}x\u{a0}\"\n\
         2,\"\u{2003}x\u{2003}\"\n\
         3,\"\u{3000}x\u{3000}\"\n",
    )?;
    Ok(())
}

/// Integers whose hex rendering exercises the case divergence: a value with
/// digits above 9 in both nibbles, a wide one, zero (whose single digit is the
/// same in either case, which is why a spot check can miss this), the negative
/// bound, and NULL.
fn write_hex_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,h\n\
         1,255\n\
         2,3735928559\n\
         3,0\n\
         4,-1\n\
         5,\n",
    )?;
    Ok(())
}

/// The SQL each federated scan in `plan` sends to `DuckDB`, one per line.
///
/// `base_sql` is the only part of an `EXPLAIN` that says what `DuckDB` is asked
/// to evaluate — the logical plan above it names the `DataFusion` function
/// whether or not the dialect rewrote the call — so every test here that claims
/// a rewrite reached the remote engine reads it rather than the whole plan.
fn pushed_down_sql(plan: &str) -> String {
    plan.split("base_sql=")
        .skip(1)
        .map(|tail| tail.split('\n').next().unwrap_or_default().to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn duckdb_accelerated(from: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(from, name);
    dataset.params = Some(Params::from_string_map(
        vec![("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
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

/// `trim` is an alias of `btrim` in `DataFusion`, so a `trim(...)` call reaches
/// the unparser under the name `btrim` — which `DuckDB` has no function for.
/// Before the dialect rewrote it, this query failed against the accelerator
/// with `Catalog Error: Scalar Function with name btrim does not exist!`
/// (regression test for #13794).
#[tokio::test]
async fn duckdb_accelerator_answers_btrim_and_agrees_with_local() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("names.csv");
            write_csv_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("duckdb_builtin_pushdown")
                .with_dataset(duckdb_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The call must reach DuckDB for this to be testing anything: an
            // accelerated query that evaluated the trims locally would pass
            // even with the rewrite removed. `base_sql` is the SQL the
            // federated scan sends, so it is the only part of the plan that
            // says what DuckDB is asked to evaluate — the logical plan above it
            // names the DataFusion function either way.
            let plan = to_pretty_display(
                &run_query(
                    &rt,
                    "EXPLAIN SELECT trim(name), btrim(name, 'xy') FROM accelerated",
                )
                .await?,
            )?
            .to_string();
            let remote_sql = pushed_down_sql(&plan);
            assert!(
                remote_sql.contains("trim("),
                "the trims must be pushed down to DuckDB, not evaluated locally; plan was:\n{plan}"
            );
            assert!(
                !remote_sql.contains("btrim("),
                "DuckDB has no `btrim`; the pushed-down SQL must call `trim`: {remote_sql}"
            );

            let query = "SELECT id, \
                         trim(name) AS spaces, \
                         btrim(name, 'xy') AS chars, \
                         trim(btrim(name, 'xy')) AS both \
                         FROM {table} ORDER BY id";

            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            let expected = [
                "+----+-----------+------------+--------+",
                "| id | spaces    | chars      | both   |",
                "+----+-----------+------------+--------+",
                "| 1  | padded    |   padded   | padded |",
                "| 2  | xyhelloyx | hello      | hello  |",
                "| 3  | x hello x |  hello     | hello  |",
                "| 4  |           |            |        |",
                "+----+-----------+------------+--------+",
            ];
            assert_batches_eq!(expected, &accelerated);

            // The accelerator's answer is only right if it is the same answer
            // DataFusion gives; a rename that is not semantics-preserving
            // shows up as a divergence here, not as a query error.
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "DuckDB-accelerated trims must agree with local evaluation"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// `btrim(str)` strips ASCII `U+0020` only, so a `Zs`-padded string is returned
/// unchanged. `DuckDB`'s one-argument `trim` strips every `Zs` instead, which
/// would make the accelerated dataset answer differently from the
/// unaccelerated one — a silently wrong result rather than the loud
/// unknown-function error of #13794. Measured by length, so the comparison does
/// not depend on how the separators render.
#[tokio::test]
async fn duckdb_accelerated_btrim_leaves_unicode_separators_alone() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("unicode_spaces.csv");
            write_unicode_space_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("duckdb_builtin_pushdown_unicode")
                .with_dataset(duckdb_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // Every input is separator + "x" + separator, so `btrim` leaves all
            // three characters in place. A `trim` that stripped the separators
            // would report 1.
            let query = "SELECT id, character_length(trim(name)) AS len \
                         FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            let expected = [
                "+----+-----+",
                "| id | len |",
                "+----+-----+",
                "| 1  | 3   |",
                "| 2  | 3   |",
                "| 3  | 3   |",
                "+----+-----+",
            ];
            assert_batches_eq!(expected, &accelerated);
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "a Zs-padded string must trim identically accelerated and local"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// `to_hex` exists in both engines, so nothing denied the call and — before the
/// dialect rewrote it — nothing changed it either: it was pushed into the
/// accelerated store verbatim and came back with **upper-case** digits where
/// the kernel produces lower-case ones. No error and no warning; the same query
/// over the same rows just answered differently once the dataset was
/// accelerated (regression test for #13818).
///
/// The predicate is the shape that makes this data loss rather than cosmetics:
/// `WHERE to_hex(h) = 'deadbeef'` matched nothing accelerated and matched a row
/// unaccelerated.
#[tokio::test]
async fn duckdb_accelerated_to_hex_agrees_with_local() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("hex.csv");
            write_hex_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("duckdb_builtin_pushdown_to_hex")
                .with_dataset(duckdb_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The call must reach DuckDB for this to be testing anything: an
            // accelerated query that evaluated `to_hex` locally would agree
            // with the control even with the rewrite removed.
            let plan = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT to_hex(h) FROM accelerated").await?,
            )?
            .to_string();
            let remote_sql = pushed_down_sql(&plan);
            assert!(
                remote_sql.contains("lower(to_hex("),
                "the hex rendering must be pushed down to DuckDB inside a `lower(..)`; \
                 plan was:\n{plan}"
            );

            let query = "SELECT id, to_hex(h) AS hx FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            let expected = [
                "+----+------------------+",
                "| id | hx               |",
                "+----+------------------+",
                "| 1  | ff               |",
                "| 2  | deadbeef         |",
                "| 3  | 0                |",
                "| 4  | ffffffffffffffff |",
                "| 5  |                  |",
                "+----+------------------+",
            ];
            assert_batches_eq!(expected, &accelerated);

            // The accelerator's answer is only right if it is the answer
            // DataFusion gives; an upper-cased rendering shows up here as a
            // divergence, not as a query error.
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "DuckDB-accelerated to_hex must agree with local evaluation"
            );

            // A predicate over the hex string: with the digits upper-cased
            // remotely this returned no rows at all.
            let predicate = "SELECT id FROM {table} WHERE to_hex(h) = 'deadbeef'";
            let accelerated = run_query(&rt, &predicate.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &predicate.replace("{table}", "local")).await?;
            assert_batches_eq!(
                ["+----+", "| id |", "+----+", "| 2  |", "+----+",],
                &accelerated
            );
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "a predicate over to_hex must select the same rows accelerated and local"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}
