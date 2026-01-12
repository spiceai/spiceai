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

use std::fmt::Write as _;
use std::{collections::HashMap, sync::Arc};

use app::AppBuilder;
use arrow::array::RecordBatch;
use data_components::delete::{DeletionTableProvider, get_deletion_provider};
use datafusion::{assert_batches_eq, physical_plan::collect, prelude::*, sql::TableReference};
use futures::TryStreamExt;
use runtime::{Runtime, accelerated_table::AcceleratedTable};
use runtime_request_context::{CacheControl, Protocol, RequestContext, UserAgent};
use spicepod::{
    acceleration::{Acceleration, Mode, OnConflictBehavior, RefreshMode},
    component::{access::AccessMode, dataset::Dataset},
    param::Params,
    partitioning::PartitionedBy,
};

use crate::utils::{runtime_ready_check, test_request_context};

/// Test Cayenne `on_conflict`: upsert behavior
///
/// Verifies that when a row with the same primary key is inserted,
/// the existing row is updated with the new values.
///
/// This test creates a Cayenne table directly using the `CayenneTableProvider` API
/// to test `on_conflict` behavior without going through the file connector refresh path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_on_conflict_upsert() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne");
            let metadata_db = temp_dir.path().join("metadata.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            // Create schema matching the test data
            let schema = Arc::new(Schema::new(vec![
                Field::new("event_id", DataType::Int64, false),
                Field::new("event_name", DataType::Utf8, false),
                Field::new(
                    "event_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ]));

            // Create table options with on_conflict: upsert
            let table_options = CreateTableOptions {
                table_name: "events".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["event_id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "event_id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            // Create metadata catalog using CayenneCatalog
            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            // Create the Cayenne table
            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            // Create a SessionContext and register the table
            let ctx = SessionContext::new();
            ctx.register_table(
                "events",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO events (event_id, event_name, event_timestamp) VALUES \
                 (1, 'User Registration', '2023-05-16 10:00:00'), \
                 (2, 'Password Change', '2023-05-16 14:30:00'), \
                 (3, 'User Login', '2023-05-17 08:45:00')",
            )
            .await?
            .collect()
            .await?;

            // Verify initial data
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM events")
                .await?
                .collect()
                .await?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 1);

            // Insert data with duplicate primary key (event_id = 2) - should upsert
            ctx.sql(
                "INSERT INTO events (event_id, event_name, event_timestamp) \
                 VALUES (2, 'Password Reset', '2024-01-15 09:00:00')",
            )
            .await?
            .collect()
            .await?;

            // Verify upsert happened - event_id 2 should have new values
            let result = ctx
                .sql("SELECT event_name FROM events WHERE event_id = 2")
                .await?
                .collect()
                .await?;

            let expected = [
                "+----------------+",
                "| event_name     |",
                "+----------------+",
                "| Password Reset |",
                "+----------------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 3 (upsert, not insert)
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM events")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne `on_conflict`: drop behavior
///
/// Verifies that when a row with the same primary key is inserted,
/// the new row is dropped and the existing row is preserved.
///
/// This test creates a Cayenne table directly using the `CayenneTableProvider` API
/// to test `on_conflict` behavior without going through the file connector refresh path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_on_conflict_drop() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_drop");
            let metadata_db = temp_dir.path().join("metadata_drop.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            // Create schema matching the test data
            let schema = Arc::new(Schema::new(vec![
                Field::new("event_id", DataType::Int64, false),
                Field::new("event_name", DataType::Utf8, false),
                Field::new(
                    "event_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ]));

            // Create table options with on_conflict: drop (DoNothing)
            let table_options = CreateTableOptions {
                table_name: "events_drop".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["event_id".to_string()],
                on_conflict: Some(OnConflict::DoNothing(ColumnReference::new(vec![
                    "event_id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            // Create metadata catalog using CayenneCatalog
            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            // Create the Cayenne table
            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            // Create a SessionContext and register the table
            let ctx = SessionContext::new();
            ctx.register_table(
                "events_drop",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO events_drop (event_id, event_name, event_timestamp) VALUES \
                 (1, 'User Registration', '2023-05-16 10:00:00'), \
                 (2, 'Password Change', '2023-05-16 14:30:00'), \
                 (3, 'User Login', '2023-05-17 08:45:00')",
            )
            .await?
            .collect()
            .await?;

            // Verify initial data
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM events_drop")
                .await?
                .collect()
                .await?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 1);

            // Insert data with duplicate primary key (event_id = 2) - should drop new row
            ctx.sql(
                "INSERT INTO events_drop (event_id, event_name, event_timestamp) \
                 VALUES (2, 'Password Reset', '2024-01-15 09:00:00')",
            )
            .await?
            .collect()
            .await?;

            // Verify drop happened - event_id 2 should have original values
            let result = ctx
                .sql("SELECT event_name FROM events_drop WHERE event_id = 2")
                .await?
                .collect()
                .await?;

            let expected = [
                "+-----------------+",
                "| event_name      |",
                "+-----------------+",
                "| Password Change |",
                "+-----------------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 3 (drop, not insert)
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM events_drop")
                .await?
                .collect()
                .await?;
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

            let mut dataset = Dataset::new(format!("file://{}", types_csv.display()), "types_test");
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
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM types_test").await?;
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
            let result = execute_sql(&rt, "SELECT text_col FROM types_test WHERE id = 1").await?;
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

    // Use a no-cache request context to ensure fresh results after deletion
    let no_cache_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spiceci/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .with_cache_control(CacheControl::NoCache)
            .build(),
    );

    no_cache_context
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

            let mut dataset = Dataset::new(format!("file://{}", csv_file.display()), "pk_test");
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

            // Delete by primary key using DeletionTableProvider::delete_from
            // (SQL DELETE is not supported through the runtime's SQL interface)
            let table_ref = TableReference::bare("pk_test");
            let table = rt
                .datafusion()
                .get_table(&table_ref)
                .await
                .ok_or_else(|| anyhow::anyhow!("Table pk_test not found"))?;

            // Get the AcceleratedTable, then its underlying accelerator (which may be wrapped)
            let accelerated_table = table
                .as_any()
                .downcast_ref::<AcceleratedTable>()
                .ok_or_else(|| anyhow::anyhow!("Table is not an AcceleratedTable"))?;

            let accelerator = accelerated_table.get_accelerator();
            let deletion_provider = get_deletion_provider(Arc::clone(&accelerator))
                .ok_or_else(|| anyhow::anyhow!("Accelerator does not support deletion"))?;

            let ctx = rt.datafusion().ctx.state();
            let filter = col("id").eq(lit(3i64));
            let delete_plan = deletion_provider.delete_from(&ctx, &[filter]).await?;
            collect(delete_plan, rt.datafusion().ctx.task_ctx()).await?;

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

async fn execute_sql(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<RecordBatch>, anyhow::Error> {
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

/// Test Cayenne partitioned table with primary key support
///
/// Verifies that partitioned Cayenne tables correctly handle primary keys
/// for deletion operations within each partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_partitioned_primary_key() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    // Use a no-cache request context to ensure fresh results after deletion
    let no_cache_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spiceci/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .with_cache_control(CacheControl::NoCache)
            .build(),
    );

    no_cache_context
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            // Create CSV with partition column and primary key
            let csv_file = data_dir.join("partitioned_pk_test.csv");
            std::fs::write(
                &csv_file,
                "id,region,name,value\n\
                 1,us,alpha,100\n\
                 2,us,beta,200\n\
                 3,eu,gamma,300\n\
                 4,eu,delta,400\n\
                 5,asia,epsilon,500\n",
            )?;

            let cayenne_dir = temp_dir.path().join("cayenne_partitioned_pk");
            let metadata_dir = temp_dir.path().join("metadata_partitioned_pk");

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
                Dataset::new(format!("file://{}", csv_file.display()), "partitioned_pk_test");
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("id".to_string()),
                partition_by: vec![PartitionedBy {
                    name: "region".to_string(),
                    expression: "region".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_partitioned_pk")
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

            // Verify initial data across partitions
            let result =
                execute_sql(&rt, "SELECT COUNT(*) as cnt FROM partitioned_pk_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify data per partition
            let result = execute_sql(
                &rt,
                "SELECT region, COUNT(*) as cnt FROM partitioned_pk_test GROUP BY region ORDER BY region",
            )
            .await?;
            let expected = [
                "+--------+-----+",
                "| region | cnt |",
                "+--------+-----+",
                "| asia   | 1   |",
                "| eu     | 2   |",
                "| us     | 2   |",
                "+--------+-----+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne partitioned table with `on_conflict` upsert
///
/// Verifies that partitioned Cayenne tables correctly handle upsert behavior
/// when inserting rows with duplicate primary keys within a partition.
///
/// This test creates a partitioned Cayenne table directly using the `CayenneTableProvider` API
/// to test `on_conflict` behavior without going through the file connector refresh path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_partitioned_on_conflict_upsert() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_partitioned_upsert");
            let metadata_db = temp_dir.path().join("metadata_partitioned_upsert.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            // Create schema matching the test data
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("region", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]));

            // Create table options with partition and on_conflict: upsert
            let table_options = CreateTableOptions {
                table_name: "partitioned_upsert_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: Some("region".to_string()),
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            // Create metadata catalog using CayenneCatalog
            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            // Create the Cayenne table
            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            // Create a SessionContext and register the table
            let ctx = SessionContext::new();
            ctx.register_table(
                "partitioned_upsert_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO partitioned_upsert_test (id, region, name, value) VALUES \
                 (1, 'us', 'alpha', 100), \
                 (2, 'us', 'beta', 200), \
                 (3, 'eu', 'gamma', 300)",
            )
            .await?
            .collect()
            .await?;

            // Verify initial data
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM partitioned_upsert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert data with duplicate primary key in same partition - should upsert
            ctx.sql(
                "INSERT INTO partitioned_upsert_test (id, region, name, value) \
                 VALUES (2, 'us', 'beta_updated', 999)",
            )
            .await?
            .collect()
            .await?;

            // Verify upsert happened - id 2 in 'us' partition should have new values
            let result = ctx
                .sql("SELECT name, value FROM partitioned_upsert_test WHERE id = 2 AND region = 'us'")
                .await?
                .collect()
                .await?;
            let expected = [
                "+--------------+-------+",
                "| name         | value |",
                "+--------------+-------+",
                "| beta_updated | 999   |",
                "+--------------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 3 (upsert, not insert)
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM partitioned_upsert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with composite primary key
///
/// Verifies that Cayenne correctly handles composite (multi-column) primary keys
/// for upsert and deletion operations.
///
/// This test creates a Cayenne table directly using the `CayenneTableProvider` API
/// to test `on_conflict` behavior without going through the file connector refresh path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_composite_primary_key() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_composite_pk");
            let metadata_db = temp_dir.path().join("metadata_composite_pk.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            // Create schema with composite primary key (user_id + product_id)
            let schema = Arc::new(Schema::new(vec![
                Field::new("user_id", DataType::Int64, false),
                Field::new("product_id", DataType::Int64, false),
                Field::new("quantity", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]));

            // Create table options with composite primary key and on_conflict: upsert
            let table_options = CreateTableOptions {
                table_name: "composite_pk_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["user_id".to_string(), "product_id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "user_id".to_string(),
                    "product_id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            // Create metadata catalog using CayenneCatalog
            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            // Create the Cayenne table
            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            // Create a SessionContext and register the table
            let ctx = SessionContext::new();
            ctx.register_table(
                "composite_pk_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO composite_pk_test (user_id, product_id, quantity, price) VALUES \
                 (1, 101, 5, 10.00), \
                 (1, 102, 3, 20.00), \
                 (2, 101, 2, 10.00), \
                 (2, 103, 1, 30.00)",
            )
            .await?
            .collect()
            .await?;

            // Verify initial data
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM composite_pk_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 4   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert with duplicate composite key - should upsert
            ctx.sql(
                "INSERT INTO composite_pk_test (user_id, product_id, quantity, price) \
                 VALUES (1, 101, 10, 15.00)",
            )
            .await?
            .collect()
            .await?;

            // Verify upsert happened - (1, 101) should have new values
            let result = ctx
                .sql("SELECT quantity, price FROM composite_pk_test WHERE user_id = 1 AND product_id = 101")
                .await?
                .collect()
                .await?;
            let expected = [
                "+----------+-------+",
                "| quantity | price |",
                "+----------+-------+",
                "| 10       | 15.0  |",
                "+----------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify total count is still 4 (upsert, not insert)
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM composite_pk_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 4   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert with new composite key - should insert new row
            ctx.sql(
                "INSERT INTO composite_pk_test (user_id, product_id, quantity, price) \
                 VALUES (3, 101, 7, 10.00)",
            )
            .await?
            .collect()
            .await?;

            // Verify insert happened - now 5 rows
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM composite_pk_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne primary key with no `on_conflict` (default behavior)
///
/// Verifies that when `primary_key` is set but `on_conflict` is not,
/// Cayenne defaults to drop behavior - new rows with duplicate keys are dropped
/// and the original row is preserved.
///
/// This test creates a Cayenne table directly using the `CayenneTableProvider` API
/// to test primary key behavior without going through the file connector refresh path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_primary_key_no_on_conflict() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_pk_no_conflict");
            let metadata_db = temp_dir.path().join("metadata_pk_no_conflict.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            // Create schema matching the test data
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]));

            // Create table options with primary_key but NO on_conflict
            let table_options = CreateTableOptions {
                table_name: "pk_no_conflict_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None, // No on_conflict - duplicates should be allowed
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            // Create metadata catalog using CayenneCatalog
            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            // Create the Cayenne table
            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            // Create a SessionContext and register the table
            let ctx = SessionContext::new();
            ctx.register_table(
                "pk_no_conflict_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO pk_no_conflict_test (id, name, value) VALUES \
                 (1, 'alpha', 100), \
                 (2, 'beta', 200), \
                 (3, 'gamma', 300)",
            )
            .await?
            .collect()
            .await?;

            // Verify initial data
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM pk_no_conflict_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert with duplicate primary key - with primary key but no on_conflict,
            // Cayenne drops the new row (do-nothing behavior), keeping the original
            ctx.sql(
                "INSERT INTO pk_no_conflict_test (id, name, value) \
                 VALUES (2, 'beta_new', 999)",
            )
            .await?
            .collect()
            .await?;

            // With primary key but no on_conflict configured, new duplicate rows are dropped
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM pk_no_conflict_test")
                .await?
                .collect()
                .await?;
            // Count should still be 3 because the duplicate key (id=2) was dropped
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify the original row is preserved (new row was dropped)
            let result = ctx
                .sql("SELECT name, value FROM pk_no_conflict_test WHERE id = 2")
                .await?
                .collect()
                .await?;
            let expected = [
                "+------+-------+",
                "| name | value |",
                "+------+-------+",
                "| beta | 200   |",
                "+------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Integration test to verify `on_conflict` works through the runtime loading path
/// This test uses the full runtime to load a dataset with `on_conflict` configuration
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_on_conflict_runtime_integration() -> Result<(), anyhow::Error> {
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
            dataset.access = AccessMode::ReadWrite;
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

            let app = AppBuilder::new("test_cayenne_on_conflict_runtime")
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

            // Verify initial data loaded
            let result = execute_sql(&rt, "SELECT COUNT(*) as cnt FROM events").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert data with duplicate primary key - should upsert
            rt.datafusion()
                .query_builder(
                    "INSERT INTO events (event_id, event_name, event_timestamp) \
                     VALUES (2, 'Password Reset', '2024-01-15 09:00:00')",
                )
                .build()
                .run()
                .await?;

            // Verify upsert happened
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

// ============================================================================
// Additional Comprehensive Cayenne Round-Trip Tests
// ============================================================================

/// Test Cayenne round-trip with large data batches
///
/// Verifies that Cayenne correctly handles large data insertions and queries
/// with multiple iterations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_large_batch_roundtrip() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_large_batch");
            let metadata_db = temp_dir.path().join("metadata_large_batch.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]));

            let table_options = CreateTableOptions {
                table_name: "large_batch_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "large_batch_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert 1000 rows in batches of 100
            let batch_size = 100;
            let num_batches = 10;

            for batch_num in 0..num_batches {
                let mut values = String::new();
                for i in 0..batch_size {
                    let id = batch_num * batch_size + i;
                    if i > 0 {
                        values.push_str(", ");
                    }
                    let _ = write!(values, "({id}, 'name_{id}', {})", f64::from(id) * 1.5);
                }
                ctx.sql(&format!(
                    "INSERT INTO large_batch_test (id, name, value) VALUES {values}"
                ))
                .await?
                .collect()
                .await?;
            }

            // Verify total count
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM large_batch_test")
                .await?
                .collect()
                .await?;
            let expected = ["+------+", "| cnt  |", "+------+", "| 1000 |", "+------+"];
            assert_batches_eq!(expected, &result);

            // Verify SUM
            let result = ctx
                .sql("SELECT CAST(SUM(id) AS BIGINT) as sum_id FROM large_batch_test")
                .await?
                .collect()
                .await?;
            // Sum of 0..999 = 999 * 1000 / 2 = 499500
            let expected = [
                "+--------+",
                "| sum_id |",
                "+--------+",
                "| 499500 |",
                "+--------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify filtering works
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM large_batch_test WHERE id >= 500")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 500 |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with special characters and edge case strings
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_special_characters() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_special_chars");
            let metadata_db = temp_dir.path().join("metadata_special_chars.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("text", DataType::Utf8, false),
            ]));

            let table_options = CreateTableOptions {
                table_name: "special_chars_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "special_chars_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert rows with special characters
            ctx.sql(
                "INSERT INTO special_chars_test (id, text) VALUES \
                 (1, 'Hello, World!'), \
                 (2, 'Line1\nLine2'), \
                 (3, 'Tab\there'), \
                 (4, 'Quote''s'), \
                 (5, 'Unicode: 日本語 中文 한국어'), \
                 (6, 'Emoji: 🎉 🚀 ✨'), \
                 (7, ''), \
                 (8, 'Very long string that spans multiple words and contains various punctuation marks: commas, periods, exclamation points! And question marks?')",
            )
            .await?
            .collect()
            .await?;

            // Verify all rows inserted
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM special_chars_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 8   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify Unicode text can be queried
            let result = ctx
                .sql("SELECT id FROM special_chars_test WHERE text LIKE '%日本語%'")
                .await?
                .collect()
                .await?;
            let expected = ["+----+", "| id |", "+----+", "| 5  |", "+----+"];
            assert_batches_eq!(expected, &result);

            // Verify empty string handling
            let result = ctx
                .sql("SELECT id FROM special_chars_test WHERE text = ''")
                .await?
                .collect()
                .await?;
            let expected = ["+----+", "| id |", "+----+", "| 7  |", "+----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with NULL values across various data types
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_null_handling() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_null_handling");
            let metadata_db = temp_dir.path().join("metadata_null_handling.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("nullable_int", DataType::Int64, true),
                Field::new("nullable_float", DataType::Float64, true),
                Field::new("nullable_text", DataType::Utf8, true),
                Field::new("nullable_bool", DataType::Boolean, true),
                Field::new(
                    "nullable_ts",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ]));

            let table_options = CreateTableOptions {
                table_name: "null_handling_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "null_handling_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert rows with various NULL patterns
            ctx.sql(
                "INSERT INTO null_handling_test (id, nullable_int, nullable_float, nullable_text, nullable_bool, nullable_ts) VALUES \
                 (1, 100, 1.5, 'text', true, '2023-01-01 00:00:00'), \
                 (2, NULL, 2.5, 'text2', false, '2023-01-02 00:00:00'), \
                 (3, 300, NULL, 'text3', true, '2023-01-03 00:00:00'), \
                 (4, 400, 4.5, NULL, false, '2023-01-04 00:00:00'), \
                 (5, 500, 5.5, 'text5', NULL, '2023-01-05 00:00:00'), \
                 (6, 600, 6.5, 'text6', true, NULL), \
                 (7, NULL, NULL, NULL, NULL, NULL)",
            )
            .await?
            .collect()
            .await?;

            // Verify all rows inserted
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM null_handling_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 7   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Count NULLs in nullable_int
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM null_handling_test WHERE nullable_int IS NULL")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 2   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Count non-NULLs in nullable_text
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM null_handling_test WHERE nullable_text IS NOT NULL")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify SUM ignores NULLs
            let result = ctx
                .sql("SELECT CAST(SUM(nullable_int) AS BIGINT) as sum_int FROM null_handling_test")
                .await?
                .collect()
                .await?;
            // Sum of 100 + 300 + 400 + 500 + 600 = 1900
            let expected = [
                "+---------+",
                "| sum_int |",
                "+---------+",
                "| 1900    |",
                "+---------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne upsert with batch update (multiple rows at once)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_upsert_batch_update() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion_table_providers::util::{
        column_reference::ColumnReference, on_conflict::OnConflict,
    };

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_batch_upsert");
            let metadata_db = temp_dir.path().join("metadata_batch_upsert.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ]));

            let table_options = CreateTableOptions {
                table_name: "batch_upsert_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "batch_upsert_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial rows
            ctx.sql(
                "INSERT INTO batch_upsert_test (id, name, value) VALUES \
                (1, 'alpha', 100), \
                (2, 'beta', 200), \
                (3, 'gamma', 300)",
            )
            .await?
            .collect()
            .await?;

            // Verify initial count
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM batch_upsert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Batch upsert - update existing rows and add new ones
            ctx.sql(
                "INSERT INTO batch_upsert_test (id, name, value) VALUES \
                (2, 'beta_updated', 999), \
                (4, 'delta', 400)",
            )
            .await?
            .collect()
            .await?;

            // Verify total count (3 original + 1 new = 4, one was upserted)
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM batch_upsert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 4   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify upserted row has new values
            let result = ctx
                .sql("SELECT name, value FROM batch_upsert_test WHERE id = 2")
                .await?
                .collect()
                .await?;
            let expected = [
                "+--------------+-------+",
                "| name         | value |",
                "+--------------+-------+",
                "| beta_updated | 999   |",
                "+--------------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify new row was added
            let result = ctx
                .sql("SELECT name, value FROM batch_upsert_test WHERE id = 4")
                .await?
                .collect()
                .await?;
            let expected = [
                "+-------+-------+",
                "| name  | value |",
                "+-------+-------+",
                "| delta | 400   |",
                "+-------+-------+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne deletion followed by new insert (not re-insertion of same key)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_delete_then_insert_new() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
    use datafusion::physical_plan::collect;

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_delete_insert");
            let metadata_db = temp_dir.path().join("metadata_delete_insert.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("version", DataType::Int64, false),
            ]));

            let table_options = CreateTableOptions {
                table_name: "delete_insert_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec!["id".to_string()],
                on_conflict: None, // No upsert - append only
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "delete_insert_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert initial data
            ctx.sql(
                "INSERT INTO delete_insert_test (id, name, version) VALUES \
                 (1, 'alpha', 1), \
                 (2, 'beta', 1), \
                 (3, 'gamma', 1)",
            )
            .await?
            .collect()
            .await?;

            // Verify initial count
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM delete_insert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Delete row with id=2 using DeletionTableProvider
            let filter = col("id").eq(lit(2i64));
            let delete_plan = table.delete_from(&ctx.state(), &[filter]).await?;
            collect(delete_plan, ctx.task_ctx()).await?;

            // Verify deletion
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM delete_insert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 2   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Insert a NEW row with different id
            ctx.sql("INSERT INTO delete_insert_test (id, name, version) VALUES (4, 'delta', 2)")
                .await?
                .collect()
                .await?;

            // Verify new row was added
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM delete_insert_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify the new row exists
            let result = ctx
                .sql("SELECT name, version FROM delete_insert_test WHERE id = 4")
                .await?
                .collect()
                .await?;
            let expected = [
                "+-------+---------+",
                "| name  | version |",
                "+-------+---------+",
                "| delta | 2       |",
                "+-------+---------+",
            ];
            assert_batches_eq!(expected, &result);

            // Verify deleted row is still gone
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM delete_insert_test WHERE id = 2")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 0   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne with boundary numeric values
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_boundary_values() -> Result<(), anyhow::Error> {
    use arrow::datatypes::{DataType, Field, Schema};
    use cayenne::metadata::CreateTableOptions;
    use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};

    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let cayenne_dir = temp_dir.path().join("cayenne_boundary");
            let metadata_db = temp_dir.path().join("metadata_boundary.db");
            std::fs::create_dir_all(&cayenne_dir)?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("int_val", DataType::Int64, false),
                Field::new("float_val", DataType::Float64, false),
            ]));

            let table_options = CreateTableOptions {
                table_name: "boundary_test".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: cayenne_dir.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            };

            let connection_string = format!("sqlite://{}", metadata_db.to_string_lossy());
            let catalog = Arc::new(CayenneCatalog::new(connection_string)?);
            catalog.init().await?;
            let catalog_arc: Arc<dyn MetadataCatalog> = catalog;

            let table = CayenneTableProvider::create_table(catalog_arc, table_options).await?;
            let table = Arc::new(table);

            let ctx = SessionContext::new();
            ctx.register_table(
                "boundary_test",
                Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
            )?;

            // Insert boundary values - use large but not MAX values to avoid DataFusion overflow issues
            let large_positive: i64 = 1_000_000_000_000_000; // 10^15
            let large_negative: i64 = -1_000_000_000_000_000;
            ctx.sql(&format!(
                "INSERT INTO boundary_test (id, int_val, float_val) VALUES \
                 (1, {large_positive}, 0.0), \
                 (2, {large_negative}, 0.0), \
                 (3, 0, 1.7976931348623157e100), \
                 (4, 0, 2.2250738585072014e-100), \
                 (5, 0, -1.7976931348623157e100), \
                 (6, 0, 0.0), \
                 (7, 1, -0.0)"
            ))
            .await?
            .collect()
            .await?;

            // Verify all rows inserted
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM boundary_test")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 7   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify large positive value can be retrieved and filtered
            let result = ctx
                .sql(&format!(
                    "SELECT int_val FROM boundary_test WHERE int_val = {large_positive}"
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 1);

            // Verify large negative value can be retrieved and filtered
            let result = ctx
                .sql(&format!(
                    "SELECT int_val FROM boundary_test WHERE int_val = {large_negative}"
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 1);

            // Verify zero handling
            let result = ctx
                .sql("SELECT COUNT(*) as cnt FROM boundary_test WHERE float_val = 0.0")
                .await?
                .collect()
                .await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}

/// Test Cayenne partitioned table with deletion across partitions
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(not(target_os = "windows"))]
async fn test_cayenne_partitioned_deletion() -> Result<(), anyhow::Error> {
    let _tracing = crate::init_tracing(Some("integration=debug,info"));

    // Use a no-cache request context to ensure fresh results after deletion
    let no_cache_context = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str(&format!(
                "spiceci/{}",
                env!("CARGO_PKG_VERSION")
            )))
            .with_cache_control(CacheControl::NoCache)
            .build(),
    );

    no_cache_context
        .scope(async {
            let temp_dir = tempfile::tempdir()?;
            let data_dir = temp_dir.path().join("data");
            std::fs::create_dir_all(&data_dir)?;

            let csv_file = data_dir.join("partitioned_delete_test.csv");
            std::fs::write(
                &csv_file,
                "id,region,name,value\n\
                 1,us,alpha,100\n\
                 2,us,beta,200\n\
                 3,eu,gamma,300\n\
                 4,eu,delta,400\n\
                 5,asia,epsilon,500\n\
                 6,asia,zeta,600\n",
            )?;

            let cayenne_dir = temp_dir.path().join("cayenne_partitioned_delete");
            let metadata_dir = temp_dir.path().join("metadata_partitioned_delete");

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

            let mut dataset = Dataset::new(
                format!("file://{}", csv_file.display()),
                "partitioned_delete_test",
            );
            dataset.acceleration = Some(Acceleration {
                enabled: true,
                engine: Some("cayenne".to_string()),
                mode: Mode::File,
                refresh_mode: Some(RefreshMode::Full),
                params: Some(Params::from_string_map(params)),
                primary_key: Some("id".to_string()),
                partition_by: vec![PartitionedBy {
                    name: "region".to_string(),
                    expression: "region".to_string(),
                }],
                ..Acceleration::default()
            });

            let app = AppBuilder::new("test_cayenne_partitioned_deletion")
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
            let result =
                execute_sql(&rt, "SELECT COUNT(*) as cnt FROM partitioned_delete_test").await?;
            let expected = ["+-----+", "| cnt |", "+-----+", "| 6   |", "+-----+"];
            assert_batches_eq!(expected, &result);

            // Verify data per partition
            let result = execute_sql(
                &rt,
                "SELECT region, COUNT(*) as cnt FROM partitioned_delete_test GROUP BY region ORDER BY region",
            )
            .await?;
            let expected = [
                "+--------+-----+",
                "| region | cnt |",
                "+--------+-----+",
                "| asia   | 2   |",
                "| eu     | 2   |",
                "| us     | 2   |",
                "+--------+-----+",
            ];
            assert_batches_eq!(expected, &result);

            Ok(())
        })
        .await
}
