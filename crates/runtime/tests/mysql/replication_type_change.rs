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

//! Regression tests for #14718: a `refresh_mode: changes` `MySQL` dataset keeps
//! applying changes after a source column's type changes, through the runtime
//! and a real acceleration.
//!
//! The source table starts as `(id bigint primary key, k int)`, and the column
//! then becomes `double` with `ALTER TABLE ... MODIFY`. The binlog carries the
//! `ALTER` as a statement before the first row event that uses the new type.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::common::TableReference;
use futures::TryStreamExt;
use mysql_async::prelude::Queryable;
use runtime::Runtime;
use runtime::status::ComponentStatus;
use spicepod::acceleration::{Acceleration, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::OnSchemaChange;
use spicepod::param::Params;

use crate::mysql::common;
use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

const CHANGE_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

async fn run_query(rt: &Runtime, sql: &str) -> Result<String, anyhow::Error> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow!("query `{sql}` failed: {e}"))?;
    let batches = result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow!("collect `{sql}` failed: {e}"))?;
    Ok(pretty_format_batches(&batches)?.to_string())
}

/// Poll `sql` until it prints `expected`, or fail with the last result.
///
/// A query error is retried until the deadline: a Cayenne scan that overlaps
/// the table's live schema evolution can fail (#15133), and the change under
/// test is what the query returns once the evolution has settled.
async fn wait_for(rt: &Runtime, sql: &str, expected: &str) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
    loop {
        let actual = match run_query(rt, sql).await {
            Ok(actual) => actual,
            Err(error) => error.to_string(),
        };
        if actual == expected {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for `{sql}` to return\n{expected}\nlast saw\n{actual}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn exec(pool: &mysql_async::Pool, sql: &str) -> Result<(), anyhow::Error> {
    let mut conn = pool.get_conn().await?;
    conn.query_drop(sql)
        .await
        .map_err(|e| anyhow!("mysql error running `{sql}`: {e}"))?;
    Ok(())
}

/// Start a runtime replicating `mysqldb.<table>` into `engine` under `policy`,
/// after the source holds the three seed rows, with `k` of type `k_type`.
async fn start(
    port: u16,
    table: &str,
    k_type: &str,
    engine: &str,
    policy: OnSchemaChange,
    accel_dir: &std::path::Path,
) -> Result<(mysql_async::Pool, Arc<Runtime>), anyhow::Error> {
    let pool = common::get_mysql_conn(port)?;
    exec(
        &pool,
        &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, k {k_type})"),
    )
    .await?;
    exec(
        &pool,
        &format!("INSERT INTO {table} VALUES (1, 10), (2, 20), (3, 30)"),
    )
    .await?;

    let rt = start_runtime(port, table, engine, policy, accel_dir).await?;
    wait_for(
        &rt,
        &format!("SELECT id, CAST(k AS BIGINT) AS k FROM {table} ORDER BY id"),
        "+----+----+\n\
         | id | k  |\n\
         +----+----+\n\
         | 1  | 10 |\n\
         | 2  | 20 |\n\
         | 3  | 30 |\n\
         +----+----+",
    )
    .await?;
    Ok((pool, rt))
}

/// Start a runtime replicating the existing `mysqldb.<table>` into `engine`
/// under `policy`.
async fn start_runtime(
    port: u16,
    table: &str,
    engine: &str,
    policy: OnSchemaChange,
    accel_dir: &std::path::Path,
) -> Result<Arc<Runtime>, anyhow::Error> {
    let mut dataset = common::make_mysql_dataset(&format!("mysqldb.{table}"), table, port, false);
    dataset.on_schema_change = policy;
    let accel_params = if engine == "cayenne" {
        Some(Params::from_string_map(HashMap::from([
            (
                "cayenne_file_path".to_string(),
                accel_dir.join("cayenne").display().to_string(),
            ),
            (
                "cayenne_metadata_dir".to_string(),
                accel_dir.join("metadata.db").display().to_string(),
            ),
        ])))
    } else {
        None
    };
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: if engine == "cayenne" {
            spicepod::acceleration::Mode::File
        } else {
            spicepod::acceleration::Mode::Memory
        },
        params: accel_params,
        refresh_mode: Some(RefreshMode::Changes),
        primary_key: Some("id".to_string()),
        on_conflict: if engine == "cayenne" {
            HashMap::new()
        } else {
            HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)])
        },
        ..Acceleration::default()
    });
    let app = AppBuilder::new(format!("type_change_{table}"))
        .with_dataset(dataset)
        .build();

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_secs(90)) => {
            return Err(anyhow!("timed out waiting for {table} to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// Cayenne evolves the table live under `sync_all_columns`, so the changes
/// after the `ALTER` land with their fractional values.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_sync_all_columns_widens_live() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_cayenne_sync";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY k DOUBLE")).await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 1.5)")).await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (5, 2.5)")).await?;
            wait_for(
                &rt,
                &format!("SELECT id, k, arrow_typeof(k) AS t FROM {table} ORDER BY id"),
                "+----+------+---------+\n\
                 | id | k    | t       |\n\
                 +----+------+---------+\n\
                 | 1  | 10.0 | Float64 |\n\
                 | 2  | 20.0 | Float64 |\n\
                 | 3  | 30.0 | Float64 |\n\
                 | 4  | 1.5  | Float64 |\n\
                 | 5  | 2.5  | Float64 |\n\
                 +----+------+---------+",
            )
            .await
        })
        .await
}

/// Under `block` the registered `Int32` type stays: a value that type holds
/// exactly is applied as it, and the stream keeps applying later changes.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_block_keeps_the_registered_type() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_cayenne_block";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "cayenne",
                OnSchemaChange::Block,
                dir.path(),
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY k DOUBLE")).await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 2.0)")).await?;
            exec(&pool, &format!("UPDATE {table} SET k = 7 WHERE id = 1")).await?;
            wait_for(
                &rt,
                &format!("SELECT id, k, arrow_typeof(k) AS t FROM {table} ORDER BY id"),
                "+----+----+-------+\n\
                 | id | k  | t     |\n\
                 +----+----+-------+\n\
                 | 1  | 7  | Int32 |\n\
                 | 2  | 20 | Int32 |\n\
                 | 3  | 30 | Int32 |\n\
                 | 4  | 2  | Int32 |\n\
                 +----+----+-------+",
            )
            .await
        })
        .await
}

/// `DuckDB` cannot evolve mid-stream, so under `sync_all_columns` a change is
/// cast to the table's current `Int32` type until a restart applies the
/// evolution. A value the cast holds exactly is applied; one it would round is
/// refused, never stored rounded.
#[cfg(feature = "duckdb")]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_duckdb_sync_all_columns_never_stores_a_rounded_value() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_duckdb_sync";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "duckdb",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY k DOUBLE")).await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 2.0)")).await?;
            wait_for(
                &rt,
                &format!("SELECT id, k FROM {table} WHERE id = 4"),
                "+----+---+\n\
                 | id | k |\n\
                 +----+---+\n\
                 | 4  | 2 |\n\
                 +----+---+",
            )
            .await?;

            exec(&pool, &format!("INSERT INTO {table} VALUES (5, 2.5)")).await?;
            // The refusal leaves nothing to poll for, so watch for the bound the
            // other changes in this file arrive within.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                let rows = run_query(
                    &rt,
                    &format!("SELECT count(*) AS c FROM {table} WHERE id = 5"),
                )
                .await?;
                assert_eq!(
                    rows, "+---+\n| c |\n+---+\n| 0 |\n+---+",
                    "2.5 must not be stored as an Int32"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Ok(())
        })
        .await
}

/// Wait for `table`'s dataset to report the error its change stream stopped
/// with, and return the message. Called right after the `ALTER`, before any row
/// change follows it, so the error can only come from the `ALTER` itself.
async fn wait_for_stream_error(rt: &Runtime, table: &str) -> Result<String, anyhow::Error> {
    let name = TableReference::from(table);
    let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
    loop {
        let status = rt.status().get_dataset_status(&name);
        if let Some(ComponentStatus::Error(Some(message))) = status {
            return Ok(message);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "timed out waiting for dataset {table} to report the error its stream stopped \
                 with after the `ALTER`; last status {status:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The status a dataset named after `table` reports when the source ran
/// `statement` and the stream stopped at it for `cause`.
fn stop_message(table: &str, statement: &str, cause: &str) -> String {
    format!(
        "External error: Dataset '{table}' stopped applying changes from MySQL table \
         'mysqldb.{table}', so it keeps serving the rows it held before the source ran \
         '{statement}'. Cause: {cause}. To re-load the dataset with the new column type, remove \
         the dataset's acceleration data and restart Spice. See: \
         https://spiceai.org/docs/components/data-connectors/mysql"
    )
}

/// The cause [`stop_message`] gives when `column` changed from `BIGINT` to
/// `DOUBLE`, a type that does not hold every `BIGINT` value.
fn bigint_to_double_cause(column: &str) -> String {
    format!(
        "column '{column}' changed from `bigint` to `double`, which does not hold every `bigint` \
         value, and MySQL rewrites such values in place without sending row changes, so the \
         dataset may hold values the source no longer has"
    )
}

/// Assert `table` never gains row `id` within the bound the other changes in
/// this file arrive within: the stream stopped at the `ALTER`.
async fn assert_never_applied(rt: &Runtime, table: &str, id: i64) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let rows = run_query(
            rt,
            &format!("SELECT count(*) AS c FROM {table} WHERE id = {id}"),
        )
        .await?;
        assert_eq!(
            rows, "+---+\n| c |\n+---+\n| 0 |\n+---+",
            "row {id} must not be applied after an `ALTER` the stream cannot follow"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Ok(())
}

/// An integer column that becomes a `DECIMAL` stops the stream at the `ALTER`
/// rather than adopting a layout whose first fractional value fails to apply:
/// no accelerator widens an integer to a decimal, so even an integer-valued
/// change after it is not applied. The dataset reports why, and how to
/// re-bootstrap, before any row change follows the `ALTER`.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_integer_to_decimal_stops_the_stream() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_cayenne_decimal";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(
                &pool,
                &format!("ALTER TABLE {table} MODIFY k DECIMAL(12,2)"),
            )
            .await?;
            let message = wait_for_stream_error(&rt, table).await?;
            assert_eq!(
                message,
                stop_message(
                    table,
                    &format!("ALTER TABLE {table} MODIFY k DECIMAL(12,2)"),
                    "column 'k' changed from `int` to `decimal(12,2)`, and the dataset keeps the \
                     column's integer type, which cannot store a fractional value"
                )
            );
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 7)")).await?;
            assert_never_applied(&rt, table, 4).await
        })
        .await
}

/// A `DECIMAL` with no fractional part holds only integers, so the stream keeps
/// applying changes into the registered integer type.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_integer_to_zero_scale_decimal_keeps_applying() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_cayenne_decimal0";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(
                &pool,
                &format!("ALTER TABLE {table} MODIFY k DECIMAL(10,0)"),
            )
            .await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 7)")).await?;
            wait_for(
                &rt,
                &format!("SELECT id, k, arrow_typeof(k) AS t FROM {table} WHERE id = 4"),
                "+----+---+-------+\n\
                 | id | k | t     |\n\
                 +----+---+-------+\n\
                 | 4  | 7 | Int32 |\n\
                 +----+---+-------+",
            )
            .await
        })
        .await
}

/// Under `block` a `FLOAT` column keeps its registered `Float32` type, so a
/// `DOUBLE` it becomes stops the stream at the `ALTER` rather than storing
/// `16777217` rounded to `16777216`.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_block_float_to_double_stops_the_stream() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_cayenne_float";
            let (pool, rt) = start(
                port,
                table,
                "FLOAT",
                "cayenne",
                OnSchemaChange::Block,
                dir.path(),
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY k DOUBLE")).await?;
            let message = wait_for_stream_error(&rt, table).await?;
            assert_eq!(
                message,
                stop_message(
                    table,
                    &format!("ALTER TABLE {table} MODIFY k DOUBLE"),
                    "column 'k' changed from `float` to `double`, and the dataset keeps the \
                     column's `Float32` type, which would round the new values"
                )
            );
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 16777217)")).await?;
            assert_never_applied(&rt, table, 4).await
        })
        .await
}

/// Regression test: every `DECIMAL(18,0)` value is a `BIGINT`, so `MySQL`
/// rewrites no stored value and the stream keeps applying changes into the
/// registered decimal type, under either policy. No Arrow widening turns a
/// decimal into an integer, which once made the stream refuse this `ALTER`.
async fn zero_scale_decimal_to_bigint_keeps_applying(
    table: &str,
    policy: OnSchemaChange,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let (pool, rt) =
                start(port, table, "DECIMAL(18,0)", "cayenne", policy, dir.path()).await?;
            exec(
                &pool,
                &format!("UPDATE {table} SET k = 999999999999999999 WHERE id = 1"),
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY k BIGINT")).await?;
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 7)")).await?;
            wait_for(
                &rt,
                &format!("SELECT id, k, arrow_typeof(k) AS t FROM {table} ORDER BY id"),
                "+----+--------------------+-------------------+\n\
                 | id | k                  | t                 |\n\
                 +----+--------------------+-------------------+\n\
                 | 1  | 999999999999999999 | Decimal128(18, 0) |\n\
                 | 2  | 20                 | Decimal128(18, 0) |\n\
                 | 3  | 30                 | Decimal128(18, 0) |\n\
                 | 4  | 7                  | Decimal128(18, 0) |\n\
                 +----+--------------------+-------------------+",
            )
            .await
        })
        .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_block_zero_scale_decimal_to_bigint_keeps_applying()
-> Result<(), anyhow::Error> {
    zero_scale_decimal_to_bigint_keeps_applying("type_change_dec18_block", OnSchemaChange::Block)
        .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_sync_all_columns_zero_scale_decimal_to_bigint_keeps_applying()
-> Result<(), anyhow::Error> {
    zero_scale_decimal_to_bigint_keeps_applying(
        "type_change_dec18_sync",
        OnSchemaChange::SyncAllColumns,
    )
    .await
}

/// A key column retyped to a type that does not hold every old key stops the
/// stream at the `ALTER`: `MySQL` rewrote the key `9007199254740993` to
/// `9007199254740992` in place, so a later change matched by the rewritten key
/// would miss the row the acceleration holds.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_lossy_key_retype_stops_the_stream() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_lossy_key";
            let (pool, rt) = start(
                port,
                table,
                "INT",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(
                &pool,
                &format!("INSERT INTO {table} VALUES (9007199254740993, 40)"),
            )
            .await?;
            wait_for(
                &rt,
                &format!("SELECT id, k FROM {table} WHERE k = 40"),
                "+------------------+----+\n\
                 | id               | k  |\n\
                 +------------------+----+\n\
                 | 9007199254740993 | 40 |\n\
                 +------------------+----+",
            )
            .await?;
            exec(&pool, &format!("ALTER TABLE {table} MODIFY id DOUBLE")).await?;
            let message = wait_for_stream_error(&rt, table).await?;
            assert_eq!(
                message,
                stop_message(
                    table,
                    &format!("ALTER TABLE {table} MODIFY id DOUBLE"),
                    &bigint_to_double_cause("id")
                )
            );
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 4)")).await?;
            assert_never_applied(&rt, table, 4).await
        })
        .await
}

/// A non-key column retyped to a type that does not hold every old value stops
/// the stream at the `ALTER`. Following the fix the error names re-loads the
/// dataset with the new column type, and it then matches the source, including
/// the value `MySQL` rewrote and a change the stream refused before the re-load.
async fn lossy_retype_stops_and_the_named_fix_reloads(
    table: &str,
    policy: OnSchemaChange,
) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let (pool, rt) = start(port, table, "BIGINT", "cayenne", policy, dir.path()).await?;
            exec(
                &pool,
                &format!("UPDATE {table} SET k = 9007199254740993 WHERE id = 1"),
            )
            .await?;
            wait_for(
                &rt,
                &format!("SELECT id, k FROM {table} WHERE id = 1"),
                "+----+------------------+\n\
                 | id | k                |\n\
                 +----+------------------+\n\
                 | 1  | 9007199254740993 |\n\
                 +----+------------------+",
            )
            .await?;

            exec(&pool, &format!("ALTER TABLE {table} MODIFY k DOUBLE")).await?;
            let message = wait_for_stream_error(&rt, table).await?;
            assert_eq!(
                message,
                stop_message(
                    table,
                    &format!("ALTER TABLE {table} MODIFY k DOUBLE"),
                    &bigint_to_double_cause("k")
                )
            );
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 50.5)")).await?;
            assert_never_applied(&rt, table, 4).await?;
            rt.shutdown().await;
            drop(rt);

            // The fix the error names: remove the acceleration data, then restart.
            tokio::fs::remove_dir_all(dir.path()).await?;
            tokio::fs::create_dir(dir.path()).await?;
            let rt = start_runtime(port, table, "cayenne", policy, dir.path()).await?;
            let query = format!("SELECT id, k, arrow_typeof(k) AS t FROM {table} ORDER BY id");
            wait_for(
                &rt,
                &query,
                "+----+--------------------+---------+\n\
                 | id | k                  | t       |\n\
                 +----+--------------------+---------+\n\
                 | 1  | 9007199254740992.0 | Float64 |\n\
                 | 2  | 20.0               | Float64 |\n\
                 | 3  | 30.0               | Float64 |\n\
                 | 4  | 50.5               | Float64 |\n\
                 +----+--------------------+---------+",
            )
            .await?;

            // The re-loaded dataset applies changes again.
            exec(&pool, &format!("INSERT INTO {table} VALUES (5, 2.5)")).await?;
            wait_for(
                &rt,
                &query,
                "+----+--------------------+---------+\n\
                 | id | k                  | t       |\n\
                 +----+--------------------+---------+\n\
                 | 1  | 9007199254740992.0 | Float64 |\n\
                 | 2  | 20.0               | Float64 |\n\
                 | 3  | 30.0               | Float64 |\n\
                 | 4  | 50.5               | Float64 |\n\
                 | 5  | 2.5                | Float64 |\n\
                 +----+--------------------+---------+",
            )
            .await?;
            rt.shutdown().await;
            Ok(())
        })
        .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_block_lossy_retype_stops_and_the_named_fix_reloads()
-> Result<(), anyhow::Error> {
    lossy_retype_stops_and_the_named_fix_reloads(
        "type_change_lossy_reload_block",
        OnSchemaChange::Block,
    )
    .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_sync_all_columns_lossy_retype_stops_and_the_named_fix_reloads()
-> Result<(), anyhow::Error> {
    lossy_retype_stops_and_the_named_fix_reloads(
        "type_change_lossy_reload_sync",
        OnSchemaChange::SyncAllColumns,
    )
    .await
}

/// Regression test: an unsigned integer column whose source becomes a
/// `DECIMAL` stops the stream at the `ALTER`. The column keeps its unsigned
/// type, and the binlog sends a decimal as text, which the unsigned decode
/// reads as `BIT` bytes: `42` was once stored as `13362`.
#[cfg(not(target_os = "windows"))]
#[tokio::test(flavor = "multi_thread")]
async fn mysql_cayenne_unsigned_integer_to_decimal_stops_the_stream() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_mysql_docker_container().await?;
            let port = container.host_port(3306)?;
            let dir = tempfile::tempdir()?;
            let table = "type_change_unsigned_decimal";
            let (pool, rt) = start(
                port,
                table,
                "INT UNSIGNED",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
                dir.path(),
            )
            .await?;
            exec(
                &pool,
                &format!("ALTER TABLE {table} MODIFY k DECIMAL(10,0)"),
            )
            .await?;
            let message = wait_for_stream_error(&rt, table).await?;
            assert_eq!(
                message,
                stop_message(
                    table,
                    &format!("ALTER TABLE {table} MODIFY k DECIMAL(10,0)"),
                    "column 'k' changed from `int unsigned` to `decimal(10,0)`, and the dataset \
                     keeps the column's unsigned integer type, which the change stream cannot \
                     read a decimal value into"
                )
            );
            exec(&pool, &format!("INSERT INTO {table} VALUES (4, 42)")).await?;
            assert_never_applied(&rt, table, 4).await
        })
        .await
}
