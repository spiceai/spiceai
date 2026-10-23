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

//! Wired-path regression for PK-accelerated `INSERT` (`write_mode: acceleration`,
//! `primary_key: id`, no `on_conflict`).
//!
//! Default replace-on-key is unchanged. These cases cover the unambiguous bugs:
//! `SQLite` must reject a NULL primary key; an in-batch duplicate must name the
//! uniqueness constraint rather than `Query is not read-only`; `DuckDB` must not
//! leak `__data_<dataset>_<n>`; Cayenne last-arrival-wins must report the
//! incoming row count.

use std::collections::HashMap;
use std::sync::Arc;

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, RefreshMode, WriteMode},
    component::{access::AccessMode, dataset::Dataset},
    param::Params,
};

use crate::{
    configure_test_datafusion, init_tracing,
    utils::{runtime_ready_check, test_request_context},
};

async fn execute_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("collect failed: {e}"))
}

async fn execute_sql_err(rt: &Arc<Runtime>, sql: &str) -> String {
    match execute_sql(rt, sql).await {
        Ok(batches) => panic!("expected the statement to fail, got {batches:?}"),
        Err(error) => error.to_string(),
    }
}

fn reported_count(batches: &[RecordBatch]) -> u64 {
    batches
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|values| values.values().first())
        .copied()
        .expect("a DML result must carry a UInt64 count")
}

async fn pk_runtime(
    engine: &str,
    table: &str,
) -> Result<(tempfile::TempDir, Arc<Runtime>), anyhow::Error> {
    let temp_dir = tempfile::tempdir()?;
    let csv = temp_dir.path().join(format!("{table}.csv"));
    std::fs::write(
        &csv,
        "id,customer,amount,status\n\
         1,cust_000,10.0,new\n\
         2,cust_001,20.0,paid\n",
    )?;

    configure_test_datafusion();

    let mut params = HashMap::new();
    match engine {
        "duckdb" => {
            params.insert(
                "duckdb_file".to_string(),
                temp_dir.path().join("acc.db").display().to_string(),
            );
        }
        "sqlite" => {
            params.insert(
                "sqlite_file".to_string(),
                temp_dir.path().join("acc.db").display().to_string(),
            );
        }
        "cayenne" => {
            params.insert(
                "cayenne_file_path".to_string(),
                temp_dir.path().join("cayenne").display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                temp_dir.path().join("metadata").display().to_string(),
            );
        }
        other => panic!("unsupported engine {other}"),
    }

    let mut dataset = Dataset::new(format!("file://{}", csv.display()), table);
    dataset.access = AccessMode::ReadWrite;
    dataset.params = Some(Params::from_string_map(
        [("file_format".to_string(), "csv".to_string())]
            .into_iter()
            .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        mode: spicepod::acceleration::Mode::File,
        refresh_mode: Some(RefreshMode::Full),
        params: Some(Params::from_string_map(params)),
        primary_key: Some("id".to_string()),
        write_mode: WriteMode::Acceleration,
        ..Acceleration::default()
    });

    let app = AppBuilder::new("pk_insert_semantics")
        .with_dataset(dataset)
        .build();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err(anyhow::anyhow!("timeout waiting for components to load"));
        }
        () = Arc::clone(&rt).load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok((temp_dir, rt))
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_rejects_a_null_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let (_dir, rt) = pk_runtime("sqlite", "o_sqlite").await?;
            let message = execute_sql_err(
                &rt,
                "INSERT INTO o_sqlite (id, customer, amount, status) VALUES (NULL, 'nullpk', 1.0, 'new')",
            )
            .await;
            assert!(
                message.contains("Primary key") && message.to_lowercase().contains("null"),
                "NULL primary key must be rejected, got: {message}"
            );
            assert!(
                !message.contains("Query is not read-only"),
                "NULL primary key must not be mis-reported as read-only, got: {message}"
            );
            let nulls = execute_sql(&rt, "SELECT count(*) FROM o_sqlite WHERE id IS NULL").await?;
            let count = nulls[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("count is Int64")
                .value(0);
            assert_eq!(count, 0, "a rejected NULL primary key must not insert a row");
            Ok(())
        })
        .await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_reports_uniqueness_for_an_in_batch_duplicate_key() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let (_dir, rt) = pk_runtime("sqlite", "o_sqlite").await?;
            let message = execute_sql_err(
                &rt,
                "INSERT INTO o_sqlite (id, customer, amount, status) VALUES (5001, 'a', 1.0, 'new'), (5001, 'b', 2.0, 'new')",
            )
            .await;
            assert!(
                message.contains("uniqueness")
                    || message.contains("UNIQUE")
                    || message.contains("constraint"),
                "in-batch duplicate key must name the uniqueness constraint, got: {message}"
            );
            assert!(
                !message.contains("Query is not read-only"),
                "in-batch duplicate key must not be mis-reported as read-only, got: {message}"
            );
            Ok(())
        })
        .await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
async fn duckdb_constraint_errors_name_the_user_dataset() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let (_dir, rt) = pk_runtime("duckdb", "o_duckdb").await?;
            let message = execute_sql_err(
                &rt,
                "INSERT INTO o_duckdb (id, customer, amount, status) VALUES (NULL, 'nullpk', 1.0, 'new')",
            )
            .await;
            assert!(
                !message.contains("__data_"),
                "DuckDB constraint errors must not leak the internal table name, got: {message}"
            );
            assert!(
                message.contains("o_duckdb")
                    || message.contains("Primary key")
                    || message.to_lowercase().contains("null"),
                "the error must name the dataset or the NULL primary key, got: {message}"
            );
            Ok(())
        })
        .await
}

#[cfg(not(target_os = "windows"))]
#[tokio::test]
async fn cayenne_reports_incoming_row_count_after_last_arrival_wins() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    test_request_context()
        .scope(async {
            let (_dir, rt) = pk_runtime("cayenne", "o_cayenne").await?;
            let written = execute_sql(
                &rt,
                "INSERT INTO o_cayenne (id, customer, amount, status) VALUES (5001, 'a', 1.0, 'new'), (5001, 'b', 2.0, 'new')",
            )
            .await?;
            assert_eq!(
                reported_count(&written),
                2,
                "a 2-row VALUES must report 2 after last-arrival-wins"
            );
            let rows = execute_sql(
                &rt,
                "SELECT id, customer FROM o_cayenne WHERE id = 5001",
            )
            .await?;
            let customer = rows[0]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("customer is Utf8")
                .value(0);
            assert_eq!(customer, "b", "the last arrival remains");
            Ok(())
        })
        .await
}
