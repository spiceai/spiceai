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

#![allow(clippy::expect_used)]

//! Regression test for issue #8770: Unsupported `ScalarFunctionExpr` in ORDER BY
//!
//! This test ensures that queries with scalar functions (like `to_timestamp`) in ORDER BY
//! clauses work correctly with Cayenne, even though these functions cannot be pushed down
//! to Vortex.

mod common;

use arrow::array::TimestampSecondArray;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::*;
use std::sync::Arc;

// Generate test variants for each backend
test_with_backends!(test_scalar_function_in_order_by_impl);

/// Regression test for issue #8770: "Unsupported `ScalarFunctionExpr`: `to_timestamp`"
///
/// This test validates that queries with scalar functions in ORDER BY clauses
/// work correctly. Previously, unsupported scalar functions like `to_timestamp`
/// would cause the query to fail with an error during filter pushdown.
///
/// The fix ensures that unsupported expressions are gracefully skipped during
/// pushdown, allowing `DataFusion` to handle them at a higher level.
async fn test_scalar_function_in_order_by_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("✓ Catalog initialized with {backend_name} backend");

    // Create table schema with a timestamp column (simulating clickbench hits table)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("search_phrase", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
    ]));

    let table_options = CreateTableOptions {
        table_name: "hits".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec![],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(catalog, table_options).await?;
    println!("✓ Table created");

    // Register with DataFusion context
    let ctx = SessionContext::new();
    ctx.register_table("hits", Arc::new(table))?;
    println!("✓ Table registered with DataFusion");

    // Insert test data with timestamps
    ctx.sql(
        "INSERT INTO hits VALUES \
         (1, 'rust programming', TIMESTAMP '2024-01-15 10:30:00'), \
         (2, 'spice ai', TIMESTAMP '2024-01-14 09:00:00'), \
         (3, '', TIMESTAMP '2024-01-16 14:45:00'), \
         (4, 'datafusion', TIMESTAMP '2024-01-13 08:15:00'), \
         (5, '', TIMESTAMP '2024-01-17 16:00:00')",
    )
    .await?
    .collect()
    .await?;
    println!("✓ Test data inserted");

    // Test 1: Query with to_timestamp in ORDER BY (the original failing case from issue #8770)
    // This is similar to clickbench q25:
    // SELECT "SearchPhrase" FROM hits WHERE "SearchPhrase" <> '' ORDER BY to_timestamp("EventTime") LIMIT 10;
    println!("\n--- Test 1: ORDER BY with to_timestamp scalar function ---");
    let df = ctx
        .sql(
            "SELECT search_phrase FROM hits \
             WHERE search_phrase <> '' \
             ORDER BY to_timestamp(event_time) \
             LIMIT 10",
        )
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3, "Expected 3 non-empty search phrases");
    println!("✓ to_timestamp in ORDER BY works correctly ({total_rows} rows)");

    // Test 2: Query with other scalar functions in ORDER BY
    println!("\n--- Test 2: ORDER BY with UPPER scalar function ---");
    let df = ctx
        .sql(
            "SELECT search_phrase FROM hits \
             WHERE search_phrase <> '' \
             ORDER BY UPPER(search_phrase) \
             LIMIT 10",
        )
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 3);
    println!("✓ UPPER in ORDER BY works correctly ({total_rows} rows)");

    // Test 3: Query with scalar function in both WHERE and ORDER BY
    println!("\n--- Test 3: Scalar functions in WHERE and ORDER BY ---");
    let df = ctx
        .sql(
            "SELECT search_phrase, event_time FROM hits \
             WHERE LENGTH(search_phrase) > 5 \
             ORDER BY DATE_TRUNC('day', event_time) DESC \
             LIMIT 10",
        )
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert!(total_rows <= 5, "Expected at most 5 rows with length > 5");
    println!("✓ Scalar functions in WHERE and ORDER BY work correctly ({total_rows} rows)");

    // Test 4: Direct timestamp ordering (should work via native Vortex support)
    println!("\n--- Test 4: Direct timestamp column in ORDER BY ---");
    let df = ctx
        .sql(
            "SELECT id, event_time FROM hits \
             ORDER BY event_time DESC \
             LIMIT 3",
        )
        .await?;
    let results = df.collect().await?;

    // Verify ordering is correct (descending by event_time)
    assert_eq!(results.len(), 1);
    let batch = &results[0];
    assert_eq!(batch.num_rows(), 3);

    let event_times = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .expect("event_time should be TimestampSecondArray");

    // Verify descending order
    for i in 0..event_times.len() - 1 {
        assert!(
            event_times.value(i) >= event_times.value(i + 1),
            "Timestamps should be in descending order"
        );
    }
    println!("✓ Direct timestamp ordering works correctly");

    println!("\n=== All scalar function pushdown tests passed ===");
    Ok(())
}
