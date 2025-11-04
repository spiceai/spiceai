/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use arrow::array::{BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::TableReference;
use futures::{StreamExt, TryStreamExt};
use secrecy::ExposeSecret;
use spicepod::{
    acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
    component::dataset::{Dataset, TimeFormat},
    param::Params,
};
use std::{collections::HashMap, sync::Arc, time::Duration};

use app::AppBuilder;

use runtime::Runtime;

use crate::{
    configure_test_datafusion, init_tracing,
    postgres::common::{
        get_pg_params, get_postgres_connection_pool, get_random_port,
        start_postgres_docker_container,
    },
    utils::{runtime_ready_check, test_request_context},
};

fn make_spiceai_dataset(path: &str, name: &str, engine: &str, retention_sql: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spice.ai/{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        retention_sql: Some(retention_sql.to_string()),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        ..Default::default()
    });
    ds
}

fn make_s3_dataset(
    path: &str,
    name: &str,
    engine: &str,
    retention_sql: &str,
    time_column: Option<&str>,
    retention_period: Option<&str>,
) -> Dataset {
    let mut ds = Dataset::new(format!("s3://{path}"), name.to_string());
    ds.time_column = time_column.map(ToString::to_string);
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some(engine.to_string()),
        retention_sql: Some(retention_sql.to_string()),
        retention_check_enabled: true,
        retention_check_interval: Some("200ms".to_string()),
        retention_period: retention_period.map(ToString::to_string),
        ..Default::default()
    });
    ds
}

fn rows_from_batches(batches: &[RecordBatch]) -> Result<Vec<(String, i64, bool)>, anyhow::Error> {
    let mut rows = Vec::new();

    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Expected StringArray in column 0"))?;
        let updated_at = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray in column 1"))?;
        let deleted = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray in column 2"))?;

        for row_idx in 0..batch.num_rows() {
            rows.push((
                names.value(row_idx).to_string(),
                updated_at.value(row_idx),
                deleted.value(row_idx),
            ));
        }
    }

    Ok(rows)
}

struct TimezoneGuard {
    original: Option<String>,
}

unsafe extern "C" {
    fn tzset();
}

impl TimezoneGuard {
    fn new(tz: &str) -> Self {
        let original = std::env::var("TZ").ok();
        // Change the TZ environment variable; this is process-wide but scoped by the guard.
        unsafe {
            // Safety: Modifying TZ is process-wide but controlled via the guard.
            std::env::set_var("TZ", tz);
        }
        unsafe {
            tzset();
        }
        Self { original }
    }
}

impl Drop for TimezoneGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => unsafe {
                // Safety: Restoring original TZ value captured by the guard.
                std::env::set_var("TZ", value);
            },
            None => unsafe {
                // Safety: Clearing TZ resets process state to original value.
                std::env::remove_var("TZ");
            },
        }
        unsafe {
            tzset();
        }
    }
}

async fn execute_rt_sql(rt: Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let mut result = rt.datafusion().query_builder(sql).build().run().await?;

    let mut results: Vec<RecordBatch> = vec![];
    while let Some(batch) = result.data.next().await {
        results.push(batch?);
    }

    Ok(results)
}

async fn refresh_table(rt: Arc<Runtime>, table_name: &str) -> Result<(), anyhow::Error> {
    let notifier = rt
        .datafusion()
        .refresh_table(&TableReference::from(table_name), None)
        .await?;
    notifier
        .ok_or_else(|| anyhow::anyhow!("Failed to refresh table"))?
        .notified()
        .await;
    Ok(())
}

#[tokio::test]
async fn test_retention_sql() -> Result<(), anyhow::Error> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let _tracing = init_tracing(None);

    test_request_context()
        .scope(async {
            let app = AppBuilder::new("retention_sql")
                .with_dataset(make_spiceai_dataset(
                    "spiceai/tpch/datasets/tpch.nation",
                    "nation",
                    "arrow",
                    // keep only ALGERIA, ARGENTINA and CANADA
                    "DELETE FROM nation WHERE n_nationkey >= 5 OR n_name NOT LIKE '%A'",
                ))
                .with_dataset(make_s3_dataset(
                    "spiceai-public-datasets/taxi_small_samples/taxi_sample.parquet",
                    "taxi_trips",
                    "duckdb",
                    "DELETE FROM taxi_trips WHERE VendorID != 2 OR Airport_fee != 1.75",
                    Some("tpep_pickup_datetime"),
                    Some("1000000000w"), // Some large retention period to ensure data is not fitlered out by time
                ))
                .build();

            configure_test_datafusion();
            let rt = Runtime::builder()
                .with_app(app)
                .build()
                .await;

            let cloned_rt = Arc::new(rt.clone());

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            tokio::time::sleep(Duration::from_secs(1)).await; // Allow retention to complete

            for (sql, snapshot_name) in [
                (
                    "SELECT n_nationkey, n_name, n_regionkey FROM nation",
                    "retention_sql",
                ),
                ("SELECT VendorID, Airport_fee, tpep_pickup_datetime, passenger_count, trip_distance FROM taxi_trips", "retention_sql_and_time_column"),
            ] {
                let query = rt.datafusion().query_builder(sql).build().run().await?;

                let results: Vec<RecordBatch> =
                    query.data.try_collect::<Vec<RecordBatch>>().await?;

                let results_str =
                    arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
                insta::assert_snapshot!(snapshot_name, results_str);
            }

            Ok(())
        })
        .await
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_duckdb_append_refresh_preserves_timestamptz() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let _tz_guard = TimezoneGuard::new("Asia/Tokyo");

            let port = get_random_port()?;
            let running_container = start_postgres_docker_container(port).await?;

            let pool = get_postgres_connection_pool(port, None).await?;
            let db_conn = pool
                .connect_direct()
                .await
                .expect("connection can be established");

            db_conn
                .conn
                .batch_execute(
                    r"
CREATE TABLE IF NOT EXISTS widgets (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    quantity INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS widgets_set_updated_at ON widgets;
CREATE TRIGGER widgets_set_updated_at
BEFORE UPDATE ON widgets
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

INSERT INTO widgets (name, description, quantity, deleted)
VALUES
    ('Sample widget', 'First sample widget row', 10, FALSE),
    ('Default widget', 'Second example row', 25, FALSE),
    ('Legacy widget', 'Record starts soft-deleted', 0, TRUE)
ON CONFLICT (name) DO UPDATE
SET
    description = EXCLUDED.description,
    quantity = EXCLUDED.quantity,
    deleted = EXCLUDED.deleted;
                ",
                )
                .await?;

            db_conn
                .conn
                .execute("UPDATE widgets SET deleted = FALSE;", &[])
                .await?;

            let pg_initial_ts: i64 = db_conn
                .conn
                .query_one(
                    "SELECT (EXTRACT(EPOCH FROM updated_at) * 1000000000)::BIGINT FROM widgets WHERE name = 'Sample widget';",
                    &[],
                )
                .await?
                .get(0);

            let mut dataset = Dataset::new("postgres:widgets", "widgets");
            let params = get_pg_params(port)
                .into_iter()
                .map(|(k, v)| (k, v.expose_secret().to_string()))
                .collect::<HashMap<String, String>>();
            dataset.params = Some(Params::from_string_map(params));
            dataset.time_column = Some("updated_at".to_string());
            dataset.time_format = Some(TimeFormat::Timestamptz);
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                mode: Mode::File,
                engine: Some("duckdb".to_string()),
                refresh_mode: Some(RefreshMode::Append),
                refresh_check_interval: Some("200ms".to_string()),
                retention_sql: Some("DELETE FROM widgets WHERE deleted = true".to_string()),
                primary_key: Some("id".to_string()),
                on_conflict: HashMap::from([("id".to_string(), OnConflictBehavior::Upsert)]),
                ..Acceleration::default()
            });

            configure_test_datafusion();
            let app = AppBuilder::new("duckdb_timezone_retention")
                .with_dataset(dataset)
                .build();

            let rt = Runtime::builder().with_app(app).build().await;
            let rt = Arc::new(rt);
            let cloned_rt = Arc::clone(&rt);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(120)) => {
                    panic!("Timeout waiting for components to load");
                }
                () = cloned_rt.load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Ensure the accelerator processes an initial refresh before validation.
            refresh_table(Arc::clone(&rt), "widgets").await?;

            let initial_batches = execute_rt_sql(
                Arc::clone(&rt),
                "SELECT name, updated_at, deleted FROM widgets ORDER BY name",
            )
            .await?;

            assert!(
                !initial_batches.is_empty(),
                "expected accelerated rows to load"
            );

            let schema = initial_batches[0].schema();
            let updated_field = schema
                .field_with_name("updated_at")
                .expect("updated_at column present");
            assert_eq!(
                updated_field.data_type(),
                &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "accelerator should expose updated_at as UTC"
            );

            let initial_rows = rows_from_batches(&initial_batches)?;
            let (_, runtime_initial_ts, runtime_initial_deleted) = initial_rows
                .into_iter()
                .find(|(name, _, _)| name == "Sample widget")
                .expect("sample widget present after initial load");

            assert_eq!(
                runtime_initial_ts, pg_initial_ts,
                "initial load should keep timestamptz values aligned with source"
            );
            assert!(
                !runtime_initial_deleted,
                "sample widget should not be marked deleted initially"
            );

            db_conn
                .conn
                .execute(
                    "UPDATE widgets SET deleted = TRUE WHERE name = 'Sample widget';",
                    &[],
                )
                .await?;

            refresh_table(Arc::clone(&rt), "widgets").await?;

            let deleted_batches = execute_rt_sql(
                Arc::clone(&rt),
                "SELECT name FROM widgets WHERE deleted = TRUE",
            )
            .await?;
            let deleted_count: usize = deleted_batches.iter().map(RecordBatch::num_rows).sum();
            assert!(
                deleted_count == 0,
                "retention_sql should remove soft-deleted rows on refresh commit"
            );

            running_container.remove().await?;

            Ok(())
        })
        .await
}
