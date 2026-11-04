/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Regression tests for #14718: a `refresh_mode: changes` Postgres dataset
//! keeps applying changes after a source column's type changes, through the
//! runtime and a real acceleration.
//!
//! The source table starts as `(id bigint primary key, k integer)`, and the
//! column then becomes `double precision`. Postgres announces the new type in
//! the `Relation` message it sends before the first change after the `ALTER`,
//! which is also the first `Relation` the stream receives.

#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use app::AppBuilder;
use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use futures::TryStreamExt;
use runtime::Runtime;
use secrecy::ExposeSecret;
use spicepod::acceleration::{Acceleration, OnConflictBehavior, RefreshMode};
use spicepod::component::dataset::{Dataset, OnSchemaChange};
use spicepod::param::Params;

use crate::postgres::common;
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
async fn wait_for(rt: &Runtime, sql: &str, expected: &str) -> Result<(), anyhow::Error> {
    let deadline = std::time::Instant::now() + CHANGE_PROPAGATION_TIMEOUT;
    loop {
        let actual = run_query(rt, sql).await?;
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

/// Start a runtime replicating `public.<table>` into `engine` under `policy`,
/// after the source holds the three seed rows.
async fn start(
    port: u16,
    table: &str,
    engine: &str,
    policy: OnSchemaChange,
) -> Result<(tokio_postgres::Client, Arc<Runtime>), anyhow::Error> {
    let source = common::connect(port).await?;
    source
        .simple_query(&format!(
            "CREATE TABLE public.{table} (id bigint PRIMARY KEY, k integer); \
             INSERT INTO public.{table} VALUES (1, 10), (2, 20), (3, 30);"
        ))
        .await?;

    let pg_params: HashMap<String, String> = common::get_pg_params(usize::from(port))
        .into_iter()
        .map(|(k, v)| (k, v.expose_secret().to_string()))
        .collect();
    let mut dataset = Dataset::new(format!("postgres:public.{table}"), table.to_string());
    dataset.params = Some(Params::from_string_map(pg_params));
    dataset.on_schema_change = policy;
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
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
    wait_for(
        &rt,
        &format!("SELECT id, k FROM {table} ORDER BY id"),
        "+----+----+\n\
         | id | k  |\n\
         +----+----+\n\
         | 1  | 10 |\n\
         | 2  | 20 |\n\
         | 3  | 30 |\n\
         +----+----+",
    )
    .await?;
    Ok((source, rt))
}

async fn alter_to_double(
    source: &tokio_postgres::Client,
    table: &str,
) -> Result<(), anyhow::Error> {
    source
        .simple_query(&format!(
            "ALTER TABLE public.{table} ALTER COLUMN k TYPE double precision"
        ))
        .await?;
    Ok(())
}

/// Cayenne evolves the table live under `sync_all_columns`, so the change
/// after the `ALTER` lands with its fractional value.
#[tokio::test(flavor = "multi_thread")]
async fn cayenne_sync_all_columns_widens_live() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container_with_logical_wal().await?;
            let port = container.host_port(5432)?;
            let (source, rt) = start(
                port,
                "type_change_cayenne_sync",
                "cayenne",
                OnSchemaChange::SyncAllColumns,
            )
            .await?;
            alter_to_double(&source, "type_change_cayenne_sync").await?;
            source
                .simple_query("INSERT INTO public.type_change_cayenne_sync VALUES (4, 1.5)")
                .await?;
            wait_for(
                &rt,
                "SELECT id, k, arrow_typeof(k) AS t FROM type_change_cayenne_sync ORDER BY id",
                "+----+------+---------+\n\
                 | id | k    | t       |\n\
                 +----+------+---------+\n\
                 | 1  | 10.0 | Float64 |\n\
                 | 2  | 20.0 | Float64 |\n\
                 | 3  | 30.0 | Float64 |\n\
                 | 4  | 1.5  | Float64 |\n\
                 +----+------+---------+",
            )
            .await
        })
        .await
}

/// Under `block` the registered `Int32` type stays: a value that type holds
/// exactly is applied as it.
#[tokio::test(flavor = "multi_thread")]
async fn cayenne_block_keeps_the_registered_type() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container_with_logical_wal().await?;
            let port = container.host_port(5432)?;
            let (source, rt) = start(
                port,
                "type_change_cayenne_block",
                "cayenne",
                OnSchemaChange::Block,
            )
            .await?;
            alter_to_double(&source, "type_change_cayenne_block").await?;
            source
                .simple_query("INSERT INTO public.type_change_cayenne_block VALUES (4, 2.0)")
                .await?;
            wait_for(
                &rt,
                "SELECT id, k, arrow_typeof(k) AS t FROM type_change_cayenne_block WHERE id = 4",
                "+----+---+-------+\n\
                 | id | k | t     |\n\
                 +----+---+-------+\n\
                 | 4  | 2 | Int32 |\n\
                 +----+---+-------+",
            )
            .await
        })
        .await
}

/// `DuckDB` cannot evolve mid-stream, so under `sync_all_columns` a change is
/// cast to the table's current `Int32` type until a restart applies the
/// evolution. A value the cast holds exactly is applied; one it would round
/// is refused, never stored rounded.
#[tokio::test(flavor = "multi_thread")]
async fn duckdb_sync_all_columns_never_stores_a_rounded_value() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container_with_logical_wal().await?;
            let port = container.host_port(5432)?;
            let (source, rt) = start(
                port,
                "type_change_duckdb_sync",
                "duckdb",
                OnSchemaChange::SyncAllColumns,
            )
            .await?;
            alter_to_double(&source, "type_change_duckdb_sync").await?;
            source
                .simple_query("INSERT INTO public.type_change_duckdb_sync VALUES (4, 2.0)")
                .await?;
            wait_for(
                &rt,
                "SELECT id, k FROM type_change_duckdb_sync WHERE id = 4",
                "+----+---+\n\
                 | id | k |\n\
                 +----+---+\n\
                 | 4  | 2 |\n\
                 +----+---+",
            )
            .await?;

            source
                .simple_query("INSERT INTO public.type_change_duckdb_sync VALUES (5, 2.5)")
                .await?;
            // The refusal leaves nothing to poll for, so watch for the bound
            // the other changes in this file arrive within; the truncated row
            // arrived in under a second before the refusal existed.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while std::time::Instant::now() < deadline {
                let rows = run_query(
                    &rt,
                    "SELECT count(*) AS c FROM type_change_duckdb_sync WHERE id = 5",
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

/// A delete carries the old row, which under `REPLICA IDENTITY FULL` holds every
/// column. A keyed delete uses only the primary key, so an old value the
/// acceleration's narrower column type cannot hold does not refuse it.
#[tokio::test(flavor = "multi_thread")]
async fn duckdb_sync_all_columns_applies_a_delete_whose_old_row_no_longer_fits()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container_with_logical_wal().await?;
            let port = container.host_port(5432)?;
            let table = "type_change_duckdb_delete";
            let (source, rt) = start(port, table, "duckdb", OnSchemaChange::SyncAllColumns).await?;
            source
                .simple_query(&format!("ALTER TABLE public.{table} REPLICA IDENTITY FULL"))
                .await?;
            source
                .simple_query(&format!(
                    "ALTER TABLE public.{table} ALTER COLUMN k TYPE double precision USING k + 0.5"
                ))
                .await?;
            source
                .simple_query(&format!("DELETE FROM public.{table} WHERE id = 1"))
                .await?;
            wait_for(
                &rt,
                &format!("SELECT id FROM {table} ORDER BY id"),
                "+----+\n\
                 | id |\n\
                 +----+\n\
                 | 2  |\n\
                 | 3  |\n\
                 +----+",
            )
            .await
        })
        .await
}

/// Under `block`, a delete's old row is decoded with the registered types. With
/// `REPLICA IDENTITY FULL` it carries every column, and after a type change its
/// non-key values may not fit them; the delete applies by key regardless.
#[tokio::test(flavor = "multi_thread")]
async fn cayenne_block_applies_a_delete_whose_old_row_no_longer_fits() -> Result<(), anyhow::Error>
{
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    test_request_context()
        .scope(async {
            let container = common::start_postgres_docker_container_with_logical_wal().await?;
            let port = container.host_port(5432)?;
            let table = "type_change_cayenne_block_delete";
            let (source, rt) = start(port, table, "cayenne", OnSchemaChange::Block).await?;
            source
                .simple_query(&format!("ALTER TABLE public.{table} REPLICA IDENTITY FULL"))
                .await?;
            source
                .simple_query(&format!(
                    "ALTER TABLE public.{table} ALTER COLUMN k TYPE double precision USING k + 0.5"
                ))
                .await?;
            source
                .simple_query(&format!("DELETE FROM public.{table} WHERE id = 1"))
                .await?;
            wait_for(
                &rt,
                &format!("SELECT id FROM {table} ORDER BY id"),
                "+----+\n\
                 | id |\n\
                 +----+\n\
                 | 2  |\n\
                 | 3  |\n\
                 +----+",
            )
            .await
        })
        .await
}
