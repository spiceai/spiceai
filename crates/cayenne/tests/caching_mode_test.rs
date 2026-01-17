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

//! Tests for Cayenne data accelerator caching mode functionality.
//!
//! Caching mode in Cayenne allows data to be cached based on filter values,
//! supporting multi-filter caching where each unique filter combination
//! produces a separate cache entry.
//!
//! Key behaviors tested:
//! - Cache miss triggers data fetch and storage
//! - Cache hit returns cached data without refetching
//! - Multiple filter combinations cached independently (upsert-based)
//! - Cache updates via upsert when same filter is queried again
//! - Cache persistence across sessions

#![allow(clippy::expect_used)]

mod common;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::*;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::sync::Arc;
use tempfile::TempDir;

/// Test basic caching mode functionality with filter-based caching.
/// Simulates caching HTTP response data with filter columns.
#[tokio::test]
async fn test_cayenne_caching_mode_basic() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode basic functionality...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("caching_test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    // Schema simulating HTTP caching with filter columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("request_path", DataType::Utf8, false),
        Field::new("request_query", DataType::Utf8, false),
        Field::new("response_data", DataType::Utf8, true),
        Field::new("cached_at", DataType::Int64, false),
    ]));

    // Use primary key on filter columns to enable upsert-based caching
    let table_options = CreateTableOptions {
        table_name: "http_cache".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["request_path".to_string(), "request_query".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "request_path".to_string(),
            "request_query".to_string(),
        ]))),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Cache table created with upsert behavior");

    let ctx = SessionContext::new();
    ctx.register_table("http_cache", Arc::new(table))?;

    // Simulate cache miss - first request
    println!("\n--- Simulating cache miss ---");
    ctx.sql(
        "INSERT INTO http_cache VALUES \
         ('/api/users', 'page=1', '{\"users\": [\"alice\"]}', 1704067200000)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ First request cached");

    // Verify cache entry
    let df = ctx
        .sql("SELECT * FROM http_cache WHERE request_path = '/api/users' AND request_query = 'page=1'")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1, "Cache should contain 1 entry");
    println!("✓ Cache hit verified");

    // Simulate another cache miss with different filter
    println!("\n--- Simulating second cache miss ---");
    ctx.sql(
        "INSERT INTO http_cache VALUES \
         ('/api/users', 'page=2', '{\"users\": [\"bob\"]}', 1704067300000)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Second request cached");

    // Verify both entries are cached (multi-filter caching)
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM http_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        2,
        "Both filter combinations should be cached"
    );
    println!("✓ Multi-filter caching verified (2 entries)");

    // Simulate cache update (same filters, new data)
    println!("\n--- Simulating cache update ---");
    ctx.sql(
        "INSERT INTO http_cache VALUES \
         ('/api/users', 'page=1', '{\"users\": [\"alice\", \"charlie\"]}', 1704067400000)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Cache update executed");

    // Verify count is still 2 (upsert, not insert)
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM http_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        2,
        "Count should still be 2 after upsert"
    );

    // Verify updated data
    let df = ctx
        .sql("SELECT response_data, cached_at FROM http_cache WHERE request_path = '/api/users' AND request_query = 'page=1'")
        .await?;
    let results = df.collect().await?;
    let response_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");
    let timestamp_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");

    assert!(
        response_array.value(0).contains("charlie"),
        "Response should have updated data"
    );
    assert_eq!(
        timestamp_array.value(0),
        1704067400000,
        "Timestamp should be updated"
    );
    println!("✓ Cache update verified");

    println!("\n✅ Cayenne caching mode basic test passed!");
    Ok(())
}

/// Test caching mode with multiple simultaneous filter combinations.
/// Verifies that different filter values are cached independently.
#[tokio::test]
async fn test_cayenne_caching_mode_multi_filter() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode with multiple filters...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("multi_filter_cache.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("search_term", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("result_count", DataType::Int64, false),
        Field::new("result_json", DataType::Utf8, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "search_cache".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["search_term".to_string(), "category".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "search_term".to_string(),
            "category".to_string(),
        ]))),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Search cache table created");

    let ctx = SessionContext::new();
    ctx.register_table("search_cache", Arc::new(table))?;

    // Cache multiple search combinations
    ctx.sql(
        "INSERT INTO search_cache VALUES \
         ('laptop', 'electronics', 150, '[...]'), \
         ('laptop', 'accessories', 75, '[...]'), \
         ('phone', 'electronics', 200, '[...]'), \
         ('phone', 'accessories', 120, '[...]')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Multiple filter combinations cached");

    // Verify all combinations are cached independently
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM search_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        4,
        "All 4 filter combinations should be cached"
    );
    println!("✓ All filter combinations cached independently");

    // Query specific filter combination
    let df = ctx
        .sql("SELECT result_count FROM search_cache WHERE search_term = 'laptop' AND category = 'electronics'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 150);
    println!("✓ Specific filter query returned correct cached data");

    // Update one combination
    ctx.sql("INSERT INTO search_cache VALUES ('laptop', 'electronics', 160, '[updated]')")
        .await?
        .collect()
        .await?;

    // Verify only that entry was updated
    let df = ctx
        .sql("SELECT result_count, result_json FROM search_cache WHERE search_term = 'laptop' AND category = 'electronics'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    let json_array = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");
    assert_eq!(count_array.value(0), 160);
    assert_eq!(json_array.value(0), "[updated]");
    println!("✓ Single entry updated correctly");

    // Verify other entries unchanged
    let df = ctx
        .sql("SELECT result_count FROM search_cache WHERE search_term = 'laptop' AND category = 'accessories'")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        75,
        "Other entries should be unchanged"
    );
    println!("✓ Other cache entries unchanged");

    // Verify total count is still 4
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM search_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 4);
    println!("✓ Total cache entries unchanged");

    println!("\n✅ Cayenne caching mode multi-filter test passed!");
    Ok(())
}

/// Test caching mode persistence across sessions.
/// Verifies that cached data survives reconnection.
#[tokio::test]
async fn test_cayenne_caching_mode_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode persistence...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("cache_persist.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let connection_string = format!("sqlite://{}", db_path.to_string_lossy());

    // Session 1: Create cache and populate
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);
        catalog.init().await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("cache_key", DataType::Utf8, false),
            Field::new("cache_value", DataType::Utf8, true),
            Field::new("expires_at", DataType::Int64, false),
        ]));

        let table_options = CreateTableOptions {
            table_name: "persist_cache".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["cache_key".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "cache_key".to_string()
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_cache", Arc::new(table))?;

        ctx.sql(
            "INSERT INTO persist_cache VALUES \
             ('user:1', '{\"name\": \"Alice\"}', 1704153600000), \
             ('user:2', '{\"name\": \"Bob\"}', 1704153600000)",
        )
        .await?
        .collect()
        .await?;
        println!("✓ Session 1: Cache entries created");
    }

    // Session 2: Reconnect and verify cache persisted
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);

        let table = CayenneTableProvider::new("persist_cache", catalog).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_cache", Arc::new(table))?;

        let df = ctx.sql("SELECT COUNT(*) as cnt FROM persist_cache").await?;
        let results = df.collect().await?;
        let count_array = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        assert_eq!(
            count_array.value(0),
            2,
            "Cache should persist across sessions"
        );
        println!("✓ Session 2: Cache persisted (2 entries)");

        // Add a new entry (append)
        ctx.sql(
            "INSERT INTO persist_cache VALUES ('user:3', '{\"name\": \"Charlie\"}', 1704240000000)",
        )
        .await?
        .collect()
        .await?;
        println!("✓ Session 2: New cache entry added");
    }

    // Session 3: Verify new entry persisted
    {
        let catalog: Arc<dyn MetadataCatalog> =
            Arc::new(CayenneCatalog::new(connection_string.clone())?);

        let table = CayenneTableProvider::new("persist_cache", catalog).await?;

        let ctx = SessionContext::new();
        ctx.register_table("persist_cache", Arc::new(table))?;

        // Verify count is now 3
        let df = ctx.sql("SELECT COUNT(*) as cnt FROM persist_cache").await?;
        let results = df.collect().await?;
        let count_array = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        assert_eq!(count_array.value(0), 3, "All entries should persist");
        println!(
            "✓ Session 3: All entries persisted ({} entries)",
            count_array.value(0)
        );

        // Verify data content
        let df = ctx
            .sql("SELECT cache_key FROM persist_cache ORDER BY cache_key")
            .await?;
        let results = df.collect().await?;
        let mut keys = Vec::new();
        for batch in &results {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Expected StringArray");
            for i in 0..batch.num_rows() {
                keys.push(key_array.value(i).to_string());
            }
        }
        assert_eq!(keys, vec!["user:1", "user:2", "user:3"]);
        println!("✓ Data content verified");
    }

    println!("\n✅ Cayenne caching mode persistence test passed!");
    Ok(())
}

/// Test caching mode with cache invalidation using INSERT OVERWRITE.
/// Verifies that cached entries can be replaced via full table overwrite.
/// Note: Cayenne currently doesn't support DELETE, so invalidation is done via INSERT OVERWRITE.
#[tokio::test]
async fn test_cayenne_caching_mode_invalidation_via_overwrite(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode with invalidation via overwrite...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("cache_invalidate.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("cache_key", DataType::Utf8, false),
        Field::new("data", DataType::Utf8, true),
        Field::new("ttl_seconds", DataType::Int64, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "ttl_cache".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["cache_key".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "cache_key".to_string()
        ]))),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ TTL cache table created");

    let ctx = SessionContext::new();
    ctx.register_table("ttl_cache", Arc::new(table))?;

    // Populate cache
    ctx.sql(
        "INSERT INTO ttl_cache VALUES \
         ('session:abc', 'user_data_1', 3600), \
         ('session:def', 'user_data_2', 3600), \
         ('session:ghi', 'user_data_3', 7200)",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Cache populated (3 entries)");

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM ttl_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 3);
    println!("✓ Initial cache count verified");

    // Invalidate by overwriting with subset (simulating selective invalidation)
    // Keep only entries with TTL >= 7000
    ctx.sql("INSERT OVERWRITE ttl_cache VALUES ('session:ghi', 'user_data_3', 7200)")
        .await?
        .collect()
        .await?;
    println!("✓ Cache overwritten with filtered entries");

    // Wait for cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let df = ctx.sql("SELECT COUNT(*) as cnt FROM ttl_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        1,
        "Only one entry should remain after overwrite"
    );
    println!("✓ Cache count after invalidation via overwrite: 1");

    // Verify the correct entry remains
    let df = ctx
        .sql("SELECT cache_key FROM ttl_cache ORDER BY cache_key")
        .await?;
    let results = df.collect().await?;
    let mut keys = Vec::new();
    for batch in &results {
        let key_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Expected StringArray");
        for i in 0..batch.num_rows() {
            keys.push(key_array.value(i).to_string());
        }
    }
    assert_eq!(keys, vec!["session:ghi"]);
    println!("✓ Correct entry remains in cache");

    println!("\n✅ Cayenne caching mode invalidation via overwrite test passed!");
    Ok(())
}

/// Test caching mode with overwrite behavior (full cache refresh).
/// Simulates scenarios where the entire cache needs to be replaced.
#[tokio::test]
async fn test_cayenne_caching_mode_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode with overwrite...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("cache_overwrite.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, false),
    ]));

    let table_options = CreateTableOptions {
        table_name: "status_cache".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ Status cache table created");

    let ctx = SessionContext::new();
    ctx.register_table("status_cache", Arc::new(table))?;

    // Initial cache population
    ctx.sql(
        "INSERT INTO status_cache VALUES \
         (1, 'active'), (2, 'active'), (3, 'inactive')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Initial cache populated (3 entries)");

    let df = ctx.sql("SELECT * FROM status_cache ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
    println!("✓ Initial cache verified");

    // Full cache overwrite using INSERT OVERWRITE
    ctx.sql(
        "INSERT OVERWRITE status_cache VALUES \
         (10, 'pending'), (20, 'complete')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Cache overwritten with new data");

    // Wait for cleanup to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let df = ctx.sql("SELECT * FROM status_cache ORDER BY id").await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, 2,
        "Cache should only contain new data after overwrite"
    );
    println!("✓ Cache count after overwrite: 2");

    // Verify old data is gone
    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM status_cache WHERE id < 10")
        .await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 0, "Old data should be gone");
    println!("✓ Old cache entries removed");

    // Verify new data is present
    let mut ids = Vec::new();
    let df = ctx.sql("SELECT id FROM status_cache ORDER BY id").await?;
    let results = df.collect().await?;
    for batch in &results {
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array");
        for i in 0..batch.num_rows() {
            ids.push(id_array.value(i));
        }
    }
    assert_eq!(ids, vec![10, 20]);
    println!("✓ New cache entries verified");

    println!("\n✅ Cayenne caching mode overwrite test passed!");
    Ok(())
}

/// Test caching mode with concurrent filter combinations.
/// Simulates multiple different requests being cached simultaneously.
#[tokio::test]
async fn test_cayenne_caching_mode_concurrent_filters() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🧪 Testing Cayenne caching mode with concurrent filter combinations...");

    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("concurrent_cache.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path)?;

    let catalog: Arc<dyn MetadataCatalog> = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}",
        db_path.to_string_lossy()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("endpoint", DataType::Utf8, false),
        Field::new("params", DataType::Utf8, false),
        Field::new("response", DataType::Utf8, true),
    ]));

    let table_options = CreateTableOptions {
        table_name: "api_cache".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["endpoint".to_string(), "params".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "endpoint".to_string(),
            "params".to_string(),
        ]))),
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(Arc::clone(&catalog), table_options).await?;
    println!("✓ API cache table created");

    let ctx = SessionContext::new();
    ctx.register_table("api_cache", Arc::new(table))?;

    // Simulate concurrent caching of multiple API responses
    let endpoints = vec![
        ("/users", "limit=10", "users_page_1"),
        ("/users", "limit=20", "users_page_1_larger"),
        ("/products", "category=books", "books_list"),
        ("/products", "category=electronics", "electronics_list"),
        ("/orders", "status=pending", "pending_orders"),
        ("/orders", "status=completed", "completed_orders"),
    ];

    for (endpoint, params, response) in &endpoints {
        ctx.sql(&format!(
            "INSERT INTO api_cache VALUES ('{}', '{}', '{}')",
            endpoint, params, response
        ))
        .await?
        .collect()
        .await?;
    }
    println!("✓ Multiple API responses cached (6 entries)");

    // Verify all entries are cached
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM api_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(count_array.value(0), 6);
    println!("✓ All filter combinations cached independently");

    // Query by endpoint
    let df = ctx
        .sql("SELECT params FROM api_cache WHERE endpoint = '/products' ORDER BY params")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 2, "Should have 2 product endpoint variants");
    println!("✓ Endpoint-specific query works");

    // Update one combination without affecting others
    ctx.sql("INSERT INTO api_cache VALUES ('/users', 'limit=10', 'users_page_1_updated')")
        .await?
        .collect()
        .await?;

    let df = ctx
        .sql("SELECT response FROM api_cache WHERE endpoint = '/users' AND params = 'limit=10'")
        .await?;
    let results = df.collect().await?;
    let response_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");
    assert_eq!(response_array.value(0), "users_page_1_updated");
    println!("✓ Specific entry updated");

    // Verify total count unchanged
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM api_cache").await?;
    let results = df.collect().await?;
    let count_array = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array");
    assert_eq!(
        count_array.value(0),
        6,
        "Total count should be unchanged after upsert"
    );
    println!("✓ Total cache entries unchanged");

    println!("\n✅ Cayenne caching mode concurrent filters test passed!");
    Ok(())
}
