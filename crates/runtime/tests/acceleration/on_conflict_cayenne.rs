/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Integration tests for Cayenne accelerator covering:
//! - On conflict behaviors (Upsert/Drop)
//! - Core Arrow data types
//! - Primary key support

use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;
use arrow::array::RecordBatch;
use datafusion::assert_batches_eq;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::{
    acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
    component::dataset::Dataset,
    param::Params,
};

use crate::utils::{runtime_ready_check, test_request_context};

/// Test Cayenne on_conflict: upsert behavior
///
/// Verifies that when a row with the same primary key is inserted,
/// the existing row is updated with the new values.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_on_conflict_upsert() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Create test data files
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            // Initial data file
            let initial_csv = data_dir.join("events_initial.csv");
            std::fs::write(
                &initial_csv,
                "event_id,event_name,event_timestamp\n\
                 1,User Registration,2023-05-16 10:00:00\n\
                 2,Password Change,2023-05-16 14:30:00\n\
                 3,User Login,2023-05-17 08:45:00\n",
            )?;

            // Cayenne data directory
            let cayenne_dir = temp_dir.path().join("cayenne");
            let metadata_dir = temp_dir.path().join("metadata");

            crate::configure_test_datafusion();

            // Create dataset with on_conflict: upsert
            let mut on_conflict = HashMap::new();
            on_conflict.insert("event_id".to_string(), OnConflictBehavior::Upsert);

            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                cayenne_dir.display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            );

            let mut dataset = Dataset::new(format!("file://{}", initial_csv.display()), "events");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("event_id".to_string()),
                on_conflict,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_on_conflict_upsert")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify initial data
            let result = execute_sql(&rt, "SELECT * FROM events ORDER BY event_id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 3, "Should have 3 initial rows");

            // Insert data with duplicate primary key (event_id = 2) - should upsert
            rt.datafusion()
                .query_builder(
                    "INSERT INTO events (event_id, event_name, event_timestamp) \
                     VALUES (2, 'Password Reset', '2024-01-15 09:00:00')",
                )
                .build()
                .run()
                .await?;

            // Verify upsert happened - event_id 2 should have new values
            let result =
                execute_sql(&rt, "SELECT event_name FROM events WHERE event_id = 2").await?;

            let expected = [
                "+----------------+",
                "| event_name     |",
                "+----------------+",
                "| Password Reset |",
                "+----------------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 3 (upsert, not insert)
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM events").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne on_conflict: drop behavior
///
/// Verifies that when a row with the same primary key is inserted,
/// the new row is dropped and the existing row is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_on_conflict_drop() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Create test data files
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            // Initial data file
            let initial_csv = data_dir.join("events_initial.csv");
            std::fs::write(
                &initial_csv,
                "event_id,event_name,event_timestamp\n\
                 1,User Registration,2023-05-16 10:00:00\n\
                 2,Password Change,2023-05-16 14:30:00\n\
                 3,User Login,2023-05-17 08:45:00\n",
            )?;

            // Cayenne data directory
            let cayenne_dir = temp_dir.path().join("cayenne_drop");
            let metadata_dir = temp_dir.path().join("metadata_drop");

            crate::configure_test_datafusion();

            // Create dataset with on_conflict: drop
            let mut on_conflict = HashMap::new();
            on_conflict.insert("event_id".to_string(), OnConflictBehavior::Drop);

            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                cayenne_dir.display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            );

            let mut dataset =
                Dataset::new(format!("file://{}", initial_csv.display()), "events_drop");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("event_id".to_string()),
                on_conflict,
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_on_conflict_drop")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify initial data
            let result = execute_sql(&rt, "SELECT * FROM events_drop ORDER BY event_id").await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 3, "Should have 3 initial rows");

            // Insert data with duplicate primary key (event_id = 2) - should drop new row
            rt.datafusion()
                .query_builder(
                    "INSERT INTO events_drop (event_id, event_name, event_timestamp) \
                     VALUES (2, 'Password Reset', '2024-01-15 09:00:00')",
                )
                .build()
                .run()
                .await?;

            // Verify drop happened - event_id 2 should have original values
            let result =
                execute_sql(&rt, "SELECT event_name FROM events_drop WHERE event_id = 2").await?;

            let expected = [
                "+-----------------+",
                "| event_name      |",
                "+-----------------+",
                "| Password Change |",
                "+-----------------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 3 (drop, not insert)
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM events_drop").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with core Arrow data types
///
/// Verifies that Cayenne correctly handles the core Arrow data types:
/// - Int32, Int64
/// - Float32, Float64
/// - Utf8
/// - Boolean
/// - Timestamp
/// - Date32
/// - Decimal128
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_core_arrow_data_types() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            // Create test data with various data types
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            // CSV file with various types
            let types_csv = data_dir.join("types_test.csv");
            std::fs::write(
                &types_csv,
                "id,int_col,float_col,text_col,bool_col,ts_col,date_col,decimal_col\n\
                 1,100,1.5,hello,true,2023-05-16 10:00:00,2023-05-16,123.45\n\
                 2,200,2.5,world,false,2023-05-17 11:00:00,2023-05-17,678.90\n\
                 3,-50,3.14159,test,true,2023-05-18 12:00:00,2023-05-18,-99.99\n",
            )?;

            // Cayenne data directory
            let cayenne_dir = temp_dir.path().join("cayenne_types");
            let metadata_dir = temp_dir.path().join("metadata_types");

            crate::configure_test_datafusion();

            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                cayenne_dir.display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            );

            let mut dataset =
                Dataset::new(format!("file://{}", types_csv.display()), "types_test");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_data_types")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify all data was loaded correctly
            let result =
                execute_sql(&rt, "SELECT COUNT(*) as cnt FROM types_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Test integer operations
            let result = execute_sql(&rt, "SELECT SUM(int_col) as sum_int FROM types_test").await?;
            let expected = [
                "+---------+",
                "| sum_int |",
                "+---------+",
                "| 250     |",
                "+---------+",
            ];
            assert_batches_eq!(expected, &result);

            // Test float operations
            let result = execute_sql(
                &rt,
                "SELECT ROUND(AVG(float_col), 2) as avg_float FROM types_test",
            )
            .await?;
            let count = result.iter().map(RecordBatch::num_rows).sum::<usize>();
            assert_eq!(count, 1, "Should have 1 row for aggregate");

            // Test text filtering
            let result =
                execute_sql(&rt, "SELECT text_col FROM types_test WHERE id = 1").await?;
            let expected = [
                "+----------+",
                "| text_col |",
                "+----------+",
                "| hello    |",
                "+----------+",
            ];
            assert_batches_eq!(expected, &result);

            // Test boolean filtering
            let result = execute_sql(
                &rt,
                "SELECT COUNT(*) as cnt FROM types_test WHERE bool_col = true",
            )
            .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 2   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with primary key-based deletions
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_primary_key_delete() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            let csv_file = data_dir.join("pk_delete_test.csv");
            std::fs::write(
                &csv_file,
                "id,name,value\n\
                 1,alpha,100\n\
                 2,beta,200\n\
                 3,gamma,300\n\
                 4,delta,400\n\
                 5,epsilon,500\n",
            )?;

            let cayenne_dir = temp_dir.path().join("cayenne_pk");
            let metadata_dir = temp_dir.path().join("metadata_pk");

            crate::configure_test_datafusion();

            let mut params = HashMap::new();
            params.insert(
                "cayenne_file_path".to_string(),
                cayenne_dir.display().to_string(),
            );
            params.insert(
                "cayenne_metadata_dir".to_string(),
                metadata_dir.display().to_string(),
            );

            let mut dataset =
                Dataset::new(format!("file://{}", csv_file.display()), "pk_test");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("id".to_string()),
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_pk_delete")
                .with_dataset(dataset)
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::Error::msg("Timeout waiting for components to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            runtime_ready_check(&rt).await;

            // Verify initial data
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM pk_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Delete by primary key
            rt.datafusion()
                .query_builder("DELETE FROM pk_test WHERE id = 3")
                .build()
                .run()
                .await?;

            // Verify deletion
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM pk_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 4   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify specific row is deleted
            let result = execute_sql(&rt, "SELECT id FROM pk_test ORDER BY id").await?;
            let expected = [
                "+----+", "| id |", "+----+", "| 1  |", "| 2  |", "| 4  |", "| 5  |", "+----+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

async fn execute_sql(
    rt: &Arc<Runtime>,
    sql: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    rt.datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Query failed: {e}"))?
        .data
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to collect results: {e}"))
}
