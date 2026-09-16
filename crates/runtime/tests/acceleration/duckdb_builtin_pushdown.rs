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
use arrow::array::Array;
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

fn write_regexp_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,s\n\
         1,ab\n\
         2,xyz\n\
         3,aXbY\n\
         4,\n",
    )?;
    Ok(())
}

/// Strings whose SHA-256 exercises the hex-text-vs-bytes divergence: ASCII, a
/// mixed-case string with a space, a non-ASCII one (whose UTF-8 bytes are what
/// gets hashed), and NULL.
///
/// The empty string is not a row here — the CSV reader reads a quoted empty
/// field as NULL, so it cannot be expressed in the source. The test reaches it
/// through `coalesce(name, '')` over the NULL row instead, which is still
/// evaluated by `DuckDB`.
fn write_digest_source(path: &Path) -> Result<(), anyhow::Error> {
    std::fs::write(
        path,
        "id,name\n\
         1,alpha\n\
         2,BeTa gamma\n\
         3,Ünïcödé\n\
         4,\n",
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

/// `sha256` exists in both engines, so nothing denied the call and — before
/// the dialect rewrote it — nothing changed it either: it was pushed into the
/// accelerated store verbatim. `DataFusion` returns the 32-byte digest as
/// `Binary`; `DuckDB` returns its 64-character hex rendering as `VARCHAR`,
/// which the scan then cast into the plan's `Binary` column. The accelerated
/// dataset therefore held 64 bytes of ASCII hex text where the unaccelerated
/// one held the digest — every non-NULL row different in length as well as
/// content, with no error and no warning (regression test for #13850).
///
/// The empty string and NULL are here because they are where a decode-based
/// rewrite could go wrong in the other direction. The pushed-down expression
/// is `unhex(sha256(..))`, so those two cases are `unhex(sha256(''))`, which
/// must stay the empty-input digest, and `unhex(sha256(NULL))`, which must
/// stay NULL — rather than either collapsing into the other.
#[tokio::test]
async fn duckdb_accelerated_sha256_agrees_with_local() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("digest.csv");
            write_digest_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("duckdb_builtin_pushdown_sha256")
                .with_dataset(duckdb_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The call must reach DuckDB for this to be testing anything: an
            // accelerated query that evaluated `sha256` locally would agree
            // with the control even with the rewrite removed.
            let plan = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT sha256(name) FROM accelerated").await?,
            )?
            .to_string();
            let remote_sql = pushed_down_sql(&plan);
            assert!(
                remote_sql.contains("unhex(sha256("),
                "the digest must be pushed down to DuckDB inside an `unhex(..)`; \
                 plan was:\n{plan}"
            );

            let query = "SELECT id, sha256(name) AS d FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &query.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &query.replace("{table}", "local")).await?;

            // 64 hex characters is a 32-byte digest. Before the rewrite this
            // column held 128 of them: the hex text's own ASCII bytes.
            let expected = [
                "+----+------------------------------------------------------------------+",
                "| id | d                                                                |",
                "+----+------------------------------------------------------------------+",
                "| 1  | 8ed3f6ad685b959ead7022518e1af76cd816f8e8ec7ccdda1ed4018e8f2223f8 |",
                "| 2  | 5b771e77826caa5ec36e3fbf8f5b2c59b606253913fcfe10104a43410b7a380b |",
                "| 3  | 39af95d07d82b5d68b6639fea9557192025b64fcc79d700c4cce10f94c16bfc8 |",
                "| 4  |                                                                  |",
                "+----+------------------------------------------------------------------+",
            ];
            assert_batches_eq!(expected, &accelerated);

            // Row 4's blank cell above does not pin the NULL half of the
            // contract on its own: `pretty_format_batches` renders a SQL NULL
            // and a zero-length `Binary` value identically, so a rewrite that
            // turned `unhex(sha256(NULL))` into the empty blob would print the
            // same table. Only the null bitmap separates them. The query is
            // `ORDER BY id` over four rows, so the last one is id=4.
            let nulls: Vec<bool> = accelerated
                .iter()
                .flat_map(|batch| {
                    let d = batch.column_by_name("d").expect("the digest column");
                    (0..batch.num_rows())
                        .map(|row| d.is_null(row))
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                nulls,
                vec![false, false, false, true],
                "only the NULL name may produce a NULL digest, and the empty blob prints alike"
            );

            // The accelerator's answer is only right if it is the answer
            // DataFusion gives; the hex-text rendering shows up here as a
            // divergence, not as a query error.
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "DuckDB-accelerated sha256 must agree with local evaluation"
            );

            // The empty string, reached over the NULL row. DuckDB's
            // sha256('') is the empty-input digest's hex text, so the pushed
            // unhex(sha256('')) decodes back to that digest; a rewrite that
            // confused "no bytes" with "no value" would answer NULL here while
            // the kernel answers the digest.
            let empty = "SELECT id, sha256(coalesce(name, '')) AS d FROM {table} WHERE id = 4";
            let accelerated = run_query(&rt, &empty.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &empty.replace("{table}", "local")).await?;
            assert_batches_eq!(
                [
                    "+----+------------------------------------------------------------------+",
                    "| id | d                                                                |",
                    "+----+------------------------------------------------------------------+",
                    "| 4  | e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 |",
                    "+----+------------------------------------------------------------------+",
                ],
                &accelerated
            );
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "DuckDB-accelerated sha256 of the empty string must agree with local evaluation"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}

/// `regexp_match` returns the first match's capture groups, and NULL when
/// nothing matches. `DuckDB` has no function with those semantics, and the
/// `regexp_extract(s, p, 0)` the dialect used to rewrite it into answered a
/// different question on both counts: it collapsed `['a','b']` to `['ab']` and
/// turned a non-match's NULL into `['']`. It also emitted `ARRAY[...] AS item`,
/// which `DuckDB`'s parser rejects outright wherever the expression carried an
/// alias or sat in a predicate. `regexp_match` is denied for `DuckDB` now, so
/// the call evaluates locally and agrees with the control by construction
/// (regression test for #13809).
///
/// The body covers the whole regexp deny-list decision, not `regexp_match`
/// alone: `regexp_instr` and `regexp_count` are denied here too and are checked
/// the same way, while `regexp_like` and `regexp_replace` are the controls that
/// must *still* be pushed down -- `regexp_like` in particular is what keeps the
/// negative assertions from being vacuous.
#[tokio::test]
async fn duckdb_accelerated_regexp_builtins_agree_with_local() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let dir = tempfile::tempdir()?;
            let csv = dir.path().join("regexp.csv");
            write_regexp_source(&csv)?;
            let from = format!("file://{}", csv.display());

            let app = AppBuilder::new("duckdb_builtin_pushdown_regexp_match")
                .with_dataset(duckdb_accelerated(&from, "accelerated"))
                .with_dataset(unaccelerated(&from, "local"))
                .build();

            configure_test_datafusion();
            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            load_runtime_datasets(&rt, LOAD_TIMEOUT).await?;

            // The deny-list is what this test is about, so assert the call is
            // *not* pushed down. Reading `base_sql` rather than the logical
            // plan matters here: the plan names `regexp_match` either way.
            let plan = to_pretty_display(
                &run_query(
                    &rt,
                    "EXPLAIN SELECT id, regexp_match(s, '(a)(b)') AS m FROM accelerated ORDER BY id",
                )
                .await?,
            )?
            .to_string();
            // `can_execute_plan` refuses to federate a plan containing a denied
            // function at all, so this plan may carry no `base_sql` whatsoever --
            // which is the intended outcome, not a missing measurement. The
            // `regexp_like` control at the end of this test is what keeps the
            // negative assertions here from being vacuous: it proves a federated
            // scan with a translated regexp call is still produced on this setup.
            let remote_sql = pushed_down_sql(&plan);
            assert!(
                !remote_sql.contains("regexp_extract"),
                "regexp_match must not be rewritten into DuckDB's regexp_extract; \
                 plan was:\n{plan}"
            );
            assert!(
                !remote_sql.contains("regexp_match"),
                "regexp_match must not be sent to DuckDB under its DataFusion name either; \
                 plan was:\n{plan}"
            );

            // Capture groups: the rewrite answered `[ab]` for row 1, because
            // `regexp_extract(..., 0)` is the whole match rather than the groups.
            let groups = "SELECT id, regexp_match(s, '(a)(b)') AS m FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &groups.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &groups.replace("{table}", "local")).await?;
            let expected = [
                "+----+--------+",
                "| id | m      |",
                "+----+--------+",
                "| 1  | [a, b] |",
                "| 2  |        |",
                "| 3  |        |",
                "| 4  |        |",
                "+----+--------+",
            ];
            assert_batches_eq!(expected, &accelerated);
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "DuckDB-accelerated regexp_match must agree with local evaluation"
            );

            // A non-match is NULL, not the `['']` DuckDB's regexp_extract
            // answers — a one-element list holding the empty string, which
            // pretty-prints as `[]` and so reads as an empty list. `IS NULL` is
            // where the two readings disagree about the truth of a row rather
            // than only about its value.
            let no_match = "SELECT id, regexp_match(s, 'zzz') IS NULL AS unmatched \
                            FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &no_match.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &no_match.replace("{table}", "local")).await?;
            assert_batches_eq!(
                [
                    "+----+-----------+",
                    "| id | unmatched |",
                    "+----+-----------+",
                    "| 1  | true      |",
                    "| 2  | true      |",
                    "| 3  | true      |",
                    "| 4  | true      |",
                    "+----+-----------+",
                ],
                &accelerated
            );
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "a non-matching regexp_match must be NULL accelerated and local alike"
            );

            // `regexp_instr` has no DuckDB function of that name and the dialect
            // renders none, so before the deny this failed remotely with
            // `Catalog Error: Scalar Function with name regexp_instr does not
            // exist!` rather than answering.
            let instr = "SELECT id, regexp_instr(s, 'b') AS i FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &instr.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &instr.replace("{table}", "local")).await?;
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "regexp_instr must answer, and agree with local evaluation"
            );

            // Pinned separately over the non-NULL rows, so the values assert
            // what the function returns without depending on whether the CSV
            // reader gives the empty field NULL or an empty string.
            let instr_rows = "SELECT id, regexp_instr(s, 'b') AS i FROM accelerated \
                              WHERE id <= 3 ORDER BY id";
            assert_batches_eq!(
                [
                    "+----+---+",
                    "| id | i |",
                    "+----+---+",
                    "| 1  | 2 |",
                    "| 2  | 0 |",
                    "| 3  | 3 |",
                    "+----+---+",
                ],
                &run_query(&rt, instr_rows).await?
            );

            // All three remaining regexp built-ins agree with local evaluation over
            // every row, the NULL one included — `regexp_like` and `regexp_replace`
            // because DuckDB answers them identically and they are still pushed
            // down, `regexp_count` because it is denied and so evaluated locally.
            let siblings = "SELECT id, regexp_like(s, '(a)(b)') AS l, \
                            regexp_replace(s, '(a)(b)', 'X') AS r, \
                            regexp_count(s, 'a') AS c FROM {table} ORDER BY id";
            let accelerated = run_query(&rt, &siblings.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &siblings.replace("{table}", "local")).await?;
            assert_eq!(
                to_pretty_display(&accelerated)?.to_string(),
                to_pretty_display(&local)?.to_string(),
                "regexp_like/replace/count must agree with local evaluation on every row"
            );

            // The NULL row is where `regexp_count` used to part company: the dialect
            // renders it `len(regexp_extract_all(..))`, and `regexp_extract_all(NULL,
            // p)` is NULL in DuckDB, so the federated answer was NULL where
            // DataFusion counts zero matches and answers 0. Denying it makes both
            // sides 0; #13870 tracks restoring the pushdown with a NULL-preserving
            // rewrite, at which point this assertion still has to hold.
            let null_row = "SELECT regexp_count(s, 'a') AS c FROM {table} WHERE s IS NULL";
            let accelerated = run_query(&rt, &null_row.replace("{table}", "accelerated")).await?;
            let local = run_query(&rt, &null_row.replace("{table}", "local")).await?;
            for batches in [&accelerated, &local] {
                assert_batches_eq!(["+---+", "| c |", "+---+", "| 0 |", "+---+",], batches);
            }

            // And it is no longer sent to DuckDB at all.
            let plan = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT regexp_count(s, 'a') FROM accelerated").await?,
            )?
            .to_string();
            assert!(
                !pushed_down_sql(&plan).contains("regexp_extract_all"),
                "regexp_count must not be pushed down as len(regexp_extract_all(..)); \
                 plan was:\n{plan}"
            );

            let plan = to_pretty_display(
                &run_query(&rt, "EXPLAIN SELECT regexp_like(s, 'a') FROM accelerated").await?,
            )?
            .to_string();
            assert!(
                pushed_down_sql(&plan).contains("regexp_matches"),
                "regexp_like must still be pushed down as DuckDB's regexp_matches; \
                 plan was:\n{plan}"
            );

            rt.shutdown().await;
            Ok(())
        })
        .await
}
