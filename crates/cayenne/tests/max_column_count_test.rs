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

#![allow(clippy::expect_used)]

//! Max column count test for Cayenne accelerator.
//!
//! This test verifies that Cayenne can handle tables with a large number of columns,
//! meeting the Beta release criteria requirement:
//! "The accelerator supports reading datasets with the same max column count as the
//! accelerator source."
//!
//! The test creates a table with 1000 columns, inserts data, and verifies that
//! queries against the accelerated table return correct results.

mod common;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::CayenneTableProvider;
use common::insert_batch;
use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use std::sync::Arc;

/// Number of columns to test - matches the example in beta criteria documentation
const MAX_COLUMN_COUNT: usize = 1000;

/// Number of rows to insert for testing
const TEST_ROW_COUNT: usize = 100;

// Generate test variants for each backend
test_with_backends!(test_max_column_count_impl);

async fn test_max_column_count_impl(
    fixture: common::TestFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = &fixture.catalog;
    let data_path = &fixture.data_path;
    let backend_name = fixture.backend_type.name();

    println!("Testing max column count ({MAX_COLUMN_COUNT} columns) with {backend_name} backend");

    // 1. Create schema with MAX_COLUMN_COUNT columns
    // First column is the primary key (id), remaining columns are data columns
    let mut fields = Vec::with_capacity(MAX_COLUMN_COUNT);
    fields.push(Field::new("id", DataType::Int64, false));

    for i in 1..MAX_COLUMN_COUNT {
        // Alternate between Int64 and Utf8 to test different data types
        let field = if i % 2 == 0 {
            Field::new(format!("col_{i}"), DataType::Int64, true)
        } else {
            Field::new(format!("col_{i}"), DataType::Utf8, true)
        };
        fields.push(field);
    }

    let schema = Arc::new(Schema::new(fields));
    assert_eq!(
        schema.fields().len(),
        MAX_COLUMN_COUNT,
        "Schema should have {MAX_COLUMN_COUNT} columns"
    );
    println!("✓ Created schema with {MAX_COLUMN_COUNT} columns");

    // 2. Create Cayenne table
    let table_options = CreateTableOptions {
        table_name: "wide_table".to_string(),
        schema: Arc::clone(&schema),
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: cayenne::metadata::VortexConfig::default(),
    };

    let table = CayenneTableProvider::create_table(
        Arc::<cayenne::CayenneCatalog>::clone(catalog),
        table_options,
    )
    .await?;
    println!("✓ Created Cayenne table with {MAX_COLUMN_COUNT} columns");

    // Verify table schema
    assert_eq!(
        table.schema().fields().len(),
        MAX_COLUMN_COUNT,
        "Table schema should have {MAX_COLUMN_COUNT} columns"
    );

    // 3. Create test data with MAX_COLUMN_COUNT columns and TEST_ROW_COUNT rows
    let batch = create_wide_record_batch(&schema, TEST_ROW_COUNT)?;
    assert_eq!(batch.num_columns(), MAX_COLUMN_COUNT);
    assert_eq!(batch.num_rows(), TEST_ROW_COUNT);
    println!("✓ Created test batch with {TEST_ROW_COUNT} rows x {MAX_COLUMN_COUNT} columns");

    // 4. Insert data
    let rows_inserted = insert_batch(&table, batch).await?;
    assert_eq!(
        rows_inserted,
        u64::try_from(TEST_ROW_COUNT).expect("TEST_ROW_COUNT fits in u64"),
        "Should insert {TEST_ROW_COUNT} rows"
    );
    println!("✓ Inserted {rows_inserted} rows");

    // 5. Register with DataFusion and run queries
    let ctx = SessionContext::new();
    ctx.register_table("wide_table", Arc::new(table))?;

    // 5a. Count query
    let df = ctx.sql("SELECT COUNT(*) as cnt FROM wide_table").await?;
    let results = df.collect().await?;
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array")
        .value(0);
    assert_eq!(
        count,
        i64::try_from(TEST_ROW_COUNT).expect("TEST_ROW_COUNT fits in i64"),
        "COUNT(*) should return {TEST_ROW_COUNT}"
    );
    println!("✓ COUNT(*) query returned {count} rows");

    // 5b. Select all columns query
    let df = ctx
        .sql("SELECT * FROM wide_table ORDER BY id LIMIT 1")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Should have 1 result batch");
    assert_eq!(
        results[0].num_columns(),
        MAX_COLUMN_COUNT,
        "Result should have {MAX_COLUMN_COUNT} columns"
    );
    assert_eq!(results[0].num_rows(), 1, "Should have 1 row");
    println!("✓ SELECT * query returned all {MAX_COLUMN_COUNT} columns");

    // 5c. Select specific columns from different positions (first, middle, last)
    let df = ctx
        .sql("SELECT id, col_1, col_500, col_999 FROM wide_table WHERE id = 0")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Should have 1 result batch");
    assert_eq!(results[0].num_rows(), 1, "Should have 1 row");

    // Verify values
    let id_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array for id");
    assert_eq!(id_col.value(0), 0, "id should be 0");

    let col_1 = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray for col_1");
    assert_eq!(col_1.value(0), "row_0_col_1", "col_1 value mismatch");

    let col_500 = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array for col_500");
    assert_eq!(col_500.value(0), 500, "col_500 value mismatch");

    let col_999 = results[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray for col_999");
    assert_eq!(col_999.value(0), "row_0_col_999", "col_999 value mismatch");
    println!("✓ Selective column query verified (first, middle, last columns)");

    // 5d. Aggregation query across multiple columns
    let df = ctx
        .sql("SELECT SUM(col_2) as sum_col2, SUM(col_500) as sum_col500, SUM(col_998) as sum_col998 FROM wide_table")
        .await?;
    let results = df.collect().await?;
    assert_eq!(results.len(), 1, "Should have 1 result batch");

    // Sum of column index * row_count for each column (since value = column_index for Int64 columns)
    // For col_2: 2 * 100 = 200
    // For col_500: 500 * 100 = 50000
    // For col_998: 998 * 100 = 99800
    let sum_col2 = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array")
        .value(0);
    let sum_col500 = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array")
        .value(0);
    let sum_col998 = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Expected Int64Array")
        .value(0);

    let expected_row_count = i64::try_from(TEST_ROW_COUNT).expect("TEST_ROW_COUNT fits in i64");
    assert_eq!(
        sum_col2,
        2 * expected_row_count,
        "SUM(col_2) should be {}",
        2 * TEST_ROW_COUNT
    );
    assert_eq!(
        sum_col500,
        500 * expected_row_count,
        "SUM(col_500) should be {}",
        500 * TEST_ROW_COUNT
    );
    assert_eq!(
        sum_col998,
        998 * expected_row_count,
        "SUM(col_998) should be {}",
        998 * TEST_ROW_COUNT
    );
    println!("✓ Aggregation query verified across multiple columns");

    // 5e. Filter query on non-primary key column
    let df = ctx
        .sql("SELECT id FROM wide_table WHERE col_500 = 500 ORDER BY id")
        .await?;
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_rows, TEST_ROW_COUNT,
        "Filter on col_500 = 500 should return all rows"
    );
    println!("✓ Filter query on non-primary key column verified");

    println!(
        "\n✅ Max column count test passed: Successfully handled {MAX_COLUMN_COUNT} columns with {TEST_ROW_COUNT} rows"
    );

    Ok(())
}

/// Create a `RecordBatch` with the given schema and row count.
///
/// For Int64 columns (even indices except 0): value = `column_index`
/// For Utf8 columns (odd indices): value = `"row_{row_index}_col_{column_index}"`
/// For id column (index 0): value = `row_index`
fn create_wide_record_batch(
    schema: &Arc<Schema>,
    row_count: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Int64 => {
                if col_idx == 0 {
                    // id column: sequential row numbers
                    Arc::new(Int64Array::from_iter_values(
                        (0..row_count).map(|i| i64::try_from(i).expect("row index fits in i64")),
                    ))
                } else {
                    // Other Int64 columns: constant value = column index
                    let col_value = i64::try_from(col_idx).expect("column index fits in i64");
                    Arc::new(Int64Array::from_iter_values(std::iter::repeat_n(
                        col_value, row_count,
                    )))
                }
            }
            DataType::Utf8 => {
                // String columns: "row_{row_idx}_col_{col_idx}"
                let values: Vec<String> = (0..row_count)
                    .map(|row_idx| format!("row_{row_idx}_col_{col_idx}"))
                    .collect();
                Arc::new(StringArray::from(values))
            }
            _ => {
                return Err(format!("Unsupported data type: {:?}", field.data_type()).into());
            }
        };
        columns.push(array);
    }

    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

#[cfg(test)]
mod additional_tests {
    use super::*;

    const COLUMN_COUNT: usize = 100;
    const ROW_COUNT: usize = 10;

    /// Test with a moderate column count to ensure the pattern works
    #[tokio::test]
    async fn test_moderate_column_count() -> Result<(), Box<dyn std::error::Error>> {
        let fixture = common::TestFixture::new(common::BackendType::Sqlite).await?;
        let catalog = &fixture.catalog;
        let data_path = &fixture.data_path;

        // Create schema
        let mut fields = Vec::with_capacity(COLUMN_COUNT);
        fields.push(Field::new("id", DataType::Int64, false));
        for i in 1..COLUMN_COUNT {
            fields.push(Field::new(format!("col_{i}"), DataType::Int64, true));
        }
        let schema = Arc::new(Schema::new(fields));

        // Create table
        let table_options = CreateTableOptions {
            table_name: "moderate_wide_table".to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: cayenne::metadata::VortexConfig::default(),
        };

        let table = CayenneTableProvider::create_table(
            Arc::<cayenne::CayenneCatalog>::clone(catalog),
            table_options,
        )
        .await?;

        // Create and insert data
        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(COLUMN_COUNT);
        for col_idx in 0..COLUMN_COUNT {
            let col_value = i64::try_from(col_idx).expect("column index fits in i64");
            let values: Vec<i64> = if col_idx == 0 {
                (0..ROW_COUNT)
                    .map(|i| i64::try_from(i).expect("row index fits in i64"))
                    .collect()
            } else {
                std::iter::repeat_n(col_value, ROW_COUNT).collect()
            };
            columns.push(Arc::new(Int64Array::from(values)));
        }
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        insert_batch(&table, batch).await?;

        // Verify
        let ctx = SessionContext::new();
        ctx.register_table("moderate_wide_table", Arc::new(table))?;

        let df = ctx
            .sql("SELECT COUNT(*) as cnt FROM moderate_wide_table")
            .await?;
        let results = df.collect().await?;
        let count = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Expected Int64Array")
            .value(0);

        let expected_count = i64::try_from(ROW_COUNT).expect("ROW_COUNT fits in i64");
        assert_eq!(count, expected_count);
        println!("✓ Moderate column count test ({COLUMN_COUNT} columns) passed");

        Ok(())
    }
}
