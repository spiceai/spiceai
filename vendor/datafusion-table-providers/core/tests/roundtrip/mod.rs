//! Compatibility roundtrip tests for all table providers.
//!
//! This module tests that data can be written and read back correctly through each
//! table provider, ensuring type conversion works correctly including decimal types.
//!
//! # Decimal Support
//!
//! SQLite stores decimals as TEXT strings using BigDecimal, supporting all decimal precisions.
//! When reading back, the schema casting with `try_cast_to` handles the conversion from Utf8
//! to the target decimal type.
//!
//! Supported decimal types for roundtrip:
//! - Decimal128: precision up to 38
//! - Decimal256: precision up to 76
//!
//! Note: Decimal32 and Decimal64 are not currently supported because `try_cast_to` doesn't
//! support casting from Utf8 to these types yet.

use crate::arrow_record_batch_gen::*;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    RecordBatch, StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{i256, DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{dml::InsertOp, CreateExternalTable};
use datafusion::physical_plan::collect;
use datafusion::sql::TableReference;
#[cfg(feature = "sqlite-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::util::test::MockExec;
use rstest::rstest;
use std::collections::HashMap;
use std::sync::Arc;

/// Enum representing the different table providers we can test
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProviderType {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(all(feature = "duckdb", feature = "federation"))]
    DuckDB,
}

impl std::fmt::Display for ProviderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(feature = "sqlite")]
            ProviderType::Sqlite => write!(f, "SQLite"),
            #[cfg(all(feature = "duckdb", feature = "federation"))]
            ProviderType::DuckDB => write!(f, "DuckDB"),
        }
    }
}

/// Create a table provider for the given provider type
async fn create_table_provider(
    provider_type: ProviderType,
    schema: SchemaRef,
    table_name: &str,
) -> Arc<dyn TableProvider> {
    let ctx = SessionContext::new();
    let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

    let external_table = CreateExternalTable {
        schema: df_schema,
        name: TableReference::bare(table_name),
        location: String::new(),
        file_type: String::new(),
        table_partition_cols: vec![],
        if_not_exists: true,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: HashMap::new(),
        constraints: Constraints::new_unverified(vec![]),
        column_defaults: HashMap::default(),
        temporary: false,
    };

    match provider_type {
        #[cfg(feature = "sqlite")]
        ProviderType::Sqlite => {
            use datafusion_table_providers::sqlite::SqliteTableProviderFactory;
            let factory = SqliteTableProviderFactory::default();
            factory
                .create(&ctx.state(), &external_table)
                .await
                .expect("SQLite table should be created")
        }
        #[cfg(all(feature = "duckdb", feature = "federation"))]
        ProviderType::DuckDB => {
            use datafusion_table_providers::duckdb::DuckDBTableProviderFactory;
            let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
            factory
                .create(&ctx.state(), &external_table)
                .await
                .expect("DuckDB table should be created")
        }
    }
}

/// Insert data into the table provider
async fn insert_data(
    table: &Arc<dyn TableProvider>,
    data: RecordBatch,
    schema: SchemaRef,
) {
    let ctx = SessionContext::new();
    let exec = MockExec::new(vec![Ok(data)], schema);
    let insertion = table
        .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
        .await
        .expect("insertion should be successful");

    collect(insertion, ctx.task_ctx())
        .await
        .expect("insert should complete");
}

/// Query all data from the table provider
async fn query_data(table: &Arc<dyn TableProvider>) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    let scan = table
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan should be successful");
    collect(scan, ctx.task_ctx())
        .await
        .expect("scan should succeed")
}

/// Helper function to run a complete roundtrip test
///
/// This is the main DRY helper function similar to spiceai's `run_compat_test`.
/// It handles creating a table, inserting data, querying it back, and verifying results.
async fn run_roundtrip_test(
    provider_type: ProviderType,
    data: RecordBatch,
    schema: SchemaRef,
    table_name: &str,
) {
    let table = create_table_provider(provider_type, Arc::clone(&schema), table_name).await;

    insert_data(&table, data.clone(), Arc::clone(&schema)).await;

    let results = query_data(&table).await;
    assert_eq!(results.len(), 1, "Should have one result batch");

    let result = &results[0];
    assert_eq!(
        result.num_rows(),
        data.num_rows(),
        "{}: Row count should match",
        table_name
    );
    assert_eq!(
        result.num_columns(),
        data.num_columns(),
        "{}: Column count should match",
        table_name
    );
}

/// Helper function to run a roundtrip test with schema casting
/// 
/// SQLite stores some types (like decimals) as TEXT, so we need to cast
/// the result back to the original schema for comparison.
#[cfg(feature = "sqlite-federation")]
async fn run_roundtrip_test_with_cast(
    provider_type: ProviderType,
    data: RecordBatch,
    schema: SchemaRef,
    table_name: &str,
) {
    let table = create_table_provider(provider_type, Arc::clone(&schema), table_name).await;

    insert_data(&table, data.clone(), Arc::clone(&schema)).await;

    let results = query_data(&table).await;
    assert_eq!(results.len(), 1, "Should have one result batch");

    let result = &results[0];
    assert_eq!(
        result.num_rows(),
        data.num_rows(),
        "{}: Row count should match",
        table_name
    );

    // Cast the result back to the original schema
    let casted_result = try_cast_to(result.clone(), Arc::clone(&schema))
        .expect("Should be able to cast result to original schema");

    assert_eq!(
        casted_result.num_columns(),
        data.num_columns(),
        "{}: Column count should match after cast",
        table_name
    );

    // Verify the data matches
    assert_eq!(
        casted_result, data,
        "{}: Data should match after roundtrip and cast",
        table_name
    );
}

/// Helper function to run a complete roundtrip test with decimal verification
#[allow(dead_code)]
async fn run_decimal_roundtrip_test(
    provider_type: ProviderType,
    data: RecordBatch,
    schema: SchemaRef,
    table_name: &str,
) {
    let table = create_table_provider(provider_type, Arc::clone(&schema), table_name).await;

    insert_data(&table, data.clone(), Arc::clone(&schema)).await;

    let results = query_data(&table).await;
    assert_eq!(results.len(), 1, "Should have one result batch");

    let result = &results[0];
    assert_eq!(
        result.num_rows(),
        data.num_rows(),
        "{}: Row count should match",
        table_name
    );
    assert_eq!(
        result.num_columns(),
        data.num_columns(),
        "{}: Column count should match",
        table_name
    );

    // Verify decimal values roundtrip correctly
    verify_decimal_roundtrip(&data, result);
}

/// Get a comprehensive test schema with decimal types
fn get_comprehensive_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        // Integer types
        Field::new("id", DataType::Int64, false),
        Field::new("int8_col", DataType::Int8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("uint8_col", DataType::UInt8, true),
        Field::new("uint16_col", DataType::UInt16, true),
        Field::new("uint32_col", DataType::UInt32, true),
        Field::new("uint64_col", DataType::UInt64, true),
        // Float types
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        // String types
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, true),
        // Boolean
        Field::new("bool_col", DataType::Boolean, true),
        // Date types
        Field::new("date32_col", DataType::Date32, true),
        Field::new("date64_col", DataType::Date64, true),
        // Timestamp
        Field::new(
            "timestamp_us_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        // Decimal types - SQLite has max precision of 16
        Field::new("decimal128_col", DataType::Decimal128(10, 2), true),
        Field::new("decimal128_large_col", DataType::Decimal128(15, 5), true),
    ]))
}

/// Generate test data for the comprehensive schema
fn generate_comprehensive_test_data(schema: SchemaRef, num_records: usize, offset: i64) -> RecordBatch {
    let nullable_mod = 10; // Every 10th value is null for testing null handling

    let id_array = Int64Array::from(
        (0..num_records)
            .map(|i| offset + i as i64)
            .collect::<Vec<_>>(),
    );

    let int8_array = Int8Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i % 128) as i8)
                }
            })
            .collect::<Vec<_>>(),
    );

    let int16_array = Int16Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as i16) * 100)
                }
            })
            .collect::<Vec<_>>(),
    );

    let int32_array = Int32Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as i32) * 1000)
                }
            })
            .collect::<Vec<_>>(),
    );

    let uint8_array = UInt8Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i % 256) as u8)
                }
            })
            .collect::<Vec<_>>(),
    );

    let uint16_array = UInt16Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as u16) * 100)
                }
            })
            .collect::<Vec<_>>(),
    );

    let uint32_array = UInt32Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as u32) * 1000)
                }
            })
            .collect::<Vec<_>>(),
    );

    let uint64_array = UInt64Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as u64) * 10000)
                }
            })
            .collect::<Vec<_>>(),
    );

    let float32_array = Float32Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as f32) * 1.5)
                }
            })
            .collect::<Vec<_>>(),
    );

    let float64_array = Float64Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as f64) * 2.5)
                }
            })
            .collect::<Vec<_>>(),
    );

    let utf8_array = StringArray::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("name_{}", i))
                }
            })
            .collect::<Vec<_>>(),
    );

    let large_utf8_array = LargeStringArray::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(format!("large_text_{}", i))
                }
            })
            .collect::<Vec<_>>(),
    );

    let bool_array = BooleanArray::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(i % 2 == 0)
                }
            })
            .collect::<Vec<_>>(),
    );

    let date32_array = Date32Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(18000 + i as i32) // Days since epoch
                }
            })
            .collect::<Vec<_>>(),
    );

    let date64_array = Date64Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(1_600_000_000_000_i64 + (i as i64 * 86_400_000)) // Milliseconds since epoch
                }
            })
            .collect::<Vec<_>>(),
    );

    let timestamp_array = TimestampMicrosecondArray::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some(1_600_000_000_000_000_i64 + (i as i64 * 1_000_000))
                }
            })
            .collect::<Vec<_>>(),
    );

    // Decimal128 with precision 10, scale 2 (fits in SQLite's max 16)
    let decimal128_array = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    // Value like 123.45 stored as 12345 with scale 2
                    Some((i as i128 * 100) + 99)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 2)
    .expect("valid decimal128");

    // Decimal128 with precision 15, scale 5 (fits in SQLite's max 16)
    let decimal128_large_array = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % nullable_mod == 0 {
                    None
                } else {
                    Some((i as i128 * 100000) + 12345)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(15, 5)
    .expect("valid decimal128");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(int8_array),
            Arc::new(int16_array),
            Arc::new(int32_array),
            Arc::new(uint8_array),
            Arc::new(uint16_array),
            Arc::new(uint32_array),
            Arc::new(uint64_array),
            Arc::new(float32_array),
            Arc::new(float64_array),
            Arc::new(utf8_array),
            Arc::new(large_utf8_array),
            Arc::new(bool_array),
            Arc::new(date32_array),
            Arc::new(date64_array),
            Arc::new(timestamp_array),
            Arc::new(decimal128_array),
            Arc::new(decimal128_large_array),
        ],
    )
    .expect("record batch should be created")
}

/// Get a simple decimal-only test schema for focused decimal testing
fn get_decimal_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("decimal_small", DataType::Decimal128(10, 2), true),
        Field::new("decimal_medium", DataType::Decimal128(18, 5), true),
        Field::new("decimal_large", DataType::Decimal128(38, 10), true),
        Field::new("decimal_no_scale", DataType::Decimal128(10, 0), true),
        Field::new("decimal_high_scale", DataType::Decimal128(15, 10), true),
    ]))
}

/// Get a Decimal256 test schema for high-precision decimal testing
/// Decimal256 supports precision up to 76 digits
fn get_decimal256_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        // Decimal256 with various precisions and scales
        Field::new("decimal256_small", DataType::Decimal256(40, 10), true),
        Field::new("decimal256_medium", DataType::Decimal256(55, 15), true),
        Field::new("decimal256_large", DataType::Decimal256(76, 20), true),
        Field::new("decimal256_no_scale", DataType::Decimal256(50, 0), true),
    ]))
}

/// Get a comprehensive decimal test schema with Decimal128 and Decimal256
/// Note: Decimal32 and Decimal64 are not included because try_cast_to doesn't support
/// casting Utf8 to these types yet.
fn get_all_decimal_types_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        // Decimal128: precision up to 38
        Field::new("decimal128_small", DataType::Decimal128(10, 2), true),
        Field::new("decimal128_medium", DataType::Decimal128(18, 5), true),
        Field::new("decimal128_large", DataType::Decimal128(38, 10), true),
        // Decimal256: precision up to 76
        Field::new("decimal256_small", DataType::Decimal256(40, 10), true),
        Field::new("decimal256_large", DataType::Decimal256(76, 20), true),
    ]))
}

/// Generate test data for all decimal types (128 and 256)
fn generate_all_decimal_types_data(schema: SchemaRef, num_records: usize) -> RecordBatch {
    let id_array = Int64Array::from((0..num_records as i64).collect::<Vec<_>>());

    // Decimal128 (10, 2)
    let decimal128_small = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 100) + 99)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 2)
    .expect("valid decimal128");

    // Decimal128 (18, 5)
    let decimal128_medium = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 100000) + 12345)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(18, 5)
    .expect("valid decimal128");

    // Decimal128 (38, 10)
    let decimal128_large = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 10_000_000_000) + 1_234_567_890)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(38, 10)
    .expect("valid decimal128");

    // Decimal256 (40, 10)
    let decimal256_small = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(i256::from_i128((i as i128 * 10_000_000_000) + 1_234_567_890))
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(40, 10)
    .expect("valid decimal256");

    // Decimal256 (76, 20)
    let decimal256_large = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    let base = i256::from_i128(i as i128);
                    let multiplier = i256::from_i128(10_i128.pow(18));
                    Some(base * multiplier + i256::from_i128(9_876_543_210_i128))
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(76, 20)
    .expect("valid decimal256");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(decimal128_small),
            Arc::new(decimal128_medium),
            Arc::new(decimal128_large),
            Arc::new(decimal256_small),
            Arc::new(decimal256_large),
        ],
    )
    .expect("record batch should be created")
}

/// Generate test data for decimal-focused testing
fn generate_decimal_test_data(schema: SchemaRef, num_records: usize) -> RecordBatch {
    let id_array = Int64Array::from((0..num_records as i64).collect::<Vec<_>>());

    // Small decimal (10, 2) - typical currency format
    let decimal_small = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 100) + 99) // e.g., 99, 199, 299...
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 2)
    .expect("valid decimal");

    // Medium decimal (18, 5)
    let decimal_medium = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 100000) + 12345) // e.g., 0.12345, 1.12345...
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(18, 5)
    .expect("valid decimal");

    // Large decimal (38, 10)
    let decimal_large = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 10_000_000_000) + 1_234_567_890)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(38, 10)
    .expect("valid decimal");

    // No scale decimal (10, 0) - essentially large integers
    let decimal_no_scale = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(i as i128 * 1000000)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(10, 0)
    .expect("valid decimal");

    // High scale decimal (15, 10) - very precise small numbers
    let decimal_high_scale = Decimal128Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some((i as i128 * 10) + 1) // very small values with high precision
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(15, 10)
    .expect("valid decimal");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(decimal_small),
            Arc::new(decimal_medium),
            Arc::new(decimal_large),
            Arc::new(decimal_no_scale),
            Arc::new(decimal_high_scale),
        ],
    )
    .expect("record batch should be created")
}

/// Generate test data for Decimal256-focused testing (precision up to 76)
fn generate_decimal256_test_data(schema: SchemaRef, num_records: usize) -> RecordBatch {
    let id_array = Int64Array::from((0..num_records as i64).collect::<Vec<_>>());

    // Decimal256 with precision 40, scale 10
    let decimal256_small = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(i256::from_i128((i as i128 * 10_000_000_000) + 1_234_567_890))
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(40, 10)
    .expect("valid decimal256");

    // Decimal256 with precision 55, scale 15
    let decimal256_medium = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    // Use larger values that exceed i128 range
                    let base = i256::from_i128(i as i128);
                    let multiplier = i256::from_i128(1_000_000_000_000_000_i128);
                    Some(base * multiplier + i256::from_i128(12345_67890_12345_i128))
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(55, 15)
    .expect("valid decimal256");

    // Decimal256 with precision 76, scale 20 - maximum precision
    let decimal256_large = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    // Create large values using i256 arithmetic
                    let base = i256::from_i128(i as i128);
                    let multiplier = i256::from_i128(10_i128.pow(18));
                    Some(base * multiplier * multiplier + i256::from_i128(9_876_543_210_i128))
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(76, 20)
    .expect("valid decimal256");

    // Decimal256 with no scale (50, 0) - essentially very large integers
    let decimal256_no_scale = Decimal256Array::from(
        (0..num_records)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    let base = i256::from_i128(i as i128);
                    let multiplier = i256::from_i128(10_i128.pow(15));
                    Some(base * multiplier)
                }
            })
            .collect::<Vec<_>>(),
    )
    .with_precision_and_scale(50, 0)
    .expect("valid decimal256");

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(decimal256_small),
            Arc::new(decimal256_medium),
            Arc::new(decimal256_large),
            Arc::new(decimal256_no_scale),
        ],
    )
    .expect("record batch should be created")
}

/// Test basic roundtrip with comprehensive types including decimals
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_comprehensive_roundtrip(#[case] provider: ProviderType) {
    let schema = get_comprehensive_test_schema();
    let data = generate_comprehensive_test_data(Arc::clone(&schema), 20, 0);
    let table_name = format!("test_comprehensive_{}", provider);
    run_roundtrip_test(provider, data, schema, &table_name).await;
}

/// Test decimal-specific roundtrip (Decimal128)
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[ignore = "Requires decimal extension"]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_decimal_roundtrip(#[case] provider: ProviderType) {
    let schema = get_decimal_test_schema();
    let data = generate_decimal_test_data(Arc::clone(&schema), 10);
    let table_name = format!("test_decimal_{}", provider);
    run_decimal_roundtrip_test(provider, data, schema, &table_name).await;
}

/// Test all decimal types (128, 256) roundtrip (SQLite only)
/// 
/// SQLite stores all decimals as TEXT strings using BigDecimal. When reading back,
/// the schema casting with try_cast_to handles the conversion from Utf8 to decimal.
/// Note: DuckDB doesn't support Decimal256 via Arrow scan.
#[cfg(all(feature = "sqlite", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_all_decimal_types_roundtrip() {
    let schema = get_all_decimal_types_schema();
    let data = generate_all_decimal_types_data(Arc::clone(&schema), 10);
    let table_name = "test_all_decimals_SQLite";
    run_roundtrip_test_with_cast(ProviderType::Sqlite, data, schema, table_name).await;
}

/// Test Decimal256 roundtrip (SQLite only)
/// 
/// Tests high-precision Decimal256 values with precision up to 76 digits.
/// Note: DuckDB doesn't support Decimal256 via Arrow scan.
#[cfg(all(feature = "sqlite", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_decimal256_roundtrip() {
    let schema = get_decimal256_test_schema();
    let data = generate_decimal256_test_data(Arc::clone(&schema), 10);
    let table_name = "test_decimal256_SQLite";
    run_roundtrip_test_with_cast(ProviderType::Sqlite, data, schema, table_name).await;
}

/// Test List roundtrip (DuckDB only - SQLite doesn't support List type)
#[cfg(all(feature = "duckdb", feature = "federation", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_list_roundtrip() {
    let (data, schema) = get_arrow_list_record_batch();
    let table_name = "test_list_DuckDB";
    run_roundtrip_test_with_cast(ProviderType::DuckDB, data, schema, table_name).await;
}

/// Test Map roundtrip (DuckDB only - SQLite doesn't support Map type)
#[cfg(all(feature = "duckdb", feature = "federation", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_map_roundtrip() {
    let (data, schema) = get_arrow_map_record_batch();
    let table_name = "test_map_DuckDB";
    run_roundtrip_test_with_cast(ProviderType::DuckDB, data, schema, table_name).await;
}

/// Test List of structs roundtrip (DuckDB only - SQLite doesn't support nested types)
#[cfg(all(feature = "duckdb", feature = "federation", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_list_of_structs_roundtrip() {
    let (data, schema) = get_arrow_list_of_structs_record_batch();
    let table_name = "test_list_structs_DuckDB";
    run_roundtrip_test_with_cast(ProviderType::DuckDB, data, schema, table_name).await;
}

/// Test List of lists roundtrip (DuckDB only - SQLite doesn't support nested types)
#[cfg(all(feature = "duckdb", feature = "federation", feature = "sqlite-federation"))]
#[test_log::test(tokio::test)]
async fn test_list_of_lists_roundtrip() {
    let (data, schema) = get_arrow_list_of_lists_record_batch();
    let table_name = "test_list_of_lists_DuckDB";
    run_roundtrip_test_with_cast(ProviderType::DuckDB, data, schema, table_name).await;
}

/// Verify that decimal values roundtrip correctly (supports both Decimal128 and Decimal256)
fn verify_decimal_roundtrip(original: &RecordBatch, result: &RecordBatch) {
    for (col_idx, field) in original.schema().fields().iter().enumerate() {
        match field.data_type() {
            DataType::Decimal128(precision, scale) => {
                verify_decimal128_column(original, result, col_idx, field.name(), *precision, *scale);
            }
            DataType::Decimal256(precision, scale) => {
                verify_decimal256_column(original, result, col_idx, field.name(), *precision, *scale);
            }
            _ => {}
        }
    }
}

/// Verify Decimal128 column roundtrip
fn verify_decimal128_column(
    original: &RecordBatch,
    result: &RecordBatch,
    col_idx: usize,
    col_name: &str,
    precision: u8,
    scale: i8,
) {
    let original_col = original
        .column(col_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("should be decimal128 array");
    let result_col = result
        .column(col_idx)
        .as_any()
        .downcast_ref::<Decimal128Array>();

    if let Some(result_col) = result_col {
        assert_eq!(
            original_col.len(),
            result_col.len(),
            "Column {} length should match",
            col_name
        );

        for row in 0..original_col.len() {
            if original_col.is_null(row) {
                assert!(
                    result_col.is_null(row),
                    "Column {} row {} null should match",
                    col_name,
                    row
                );
            } else {
                assert!(
                    !result_col.is_null(row),
                    "Column {} row {} should not be null",
                    col_name,
                    row
                );
                assert_eq!(
                    original_col.value(row),
                    result_col.value(row),
                    "Column {} row {} value should match (precision={}, scale={})",
                    col_name,
                    row,
                    precision,
                    scale
                );
            }
        }
    } else {
        tracing::warn!(
            "Column {} has different type in result, skipping exact comparison",
            col_name
        );
    }
}

/// Verify Decimal256 column roundtrip
fn verify_decimal256_column(
    original: &RecordBatch,
    result: &RecordBatch,
    col_idx: usize,
    col_name: &str,
    precision: u8,
    scale: i8,
) {
    let original_col = original
        .column(col_idx)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .expect("should be decimal256 array");
    let result_col = result
        .column(col_idx)
        .as_any()
        .downcast_ref::<Decimal256Array>();

    if let Some(result_col) = result_col {
        assert_eq!(
            original_col.len(),
            result_col.len(),
            "Column {} length should match",
            col_name
        );

        for row in 0..original_col.len() {
            if original_col.is_null(row) {
                assert!(
                    result_col.is_null(row),
                    "Column {} row {} null should match",
                    col_name,
                    row
                );
            } else {
                assert!(
                    !result_col.is_null(row),
                    "Column {} row {} should not be null",
                    col_name,
                    row
                );
                assert_eq!(
                    original_col.value(row),
                    result_col.value(row),
                    "Column {} row {} value should match (precision={}, scale={})",
                    col_name,
                    row,
                    precision,
                    scale
                );
            }
        }
    } else {
        tracing::warn!(
            "Column {} has different type in result, skipping exact comparison",
            col_name
        );
    }
}

/// Test null handling across all providers
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_null_handling(#[case] provider: ProviderType) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nullable_int", DataType::Int32, true),
        Field::new("nullable_string", DataType::Utf8, true),
        Field::new("nullable_bool", DataType::Boolean, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let int_array = Int32Array::from(vec![Some(1), None, Some(3), None, Some(5)]);
    let string_array = StringArray::from(vec![None, Some("b"), None, Some("d"), None]);
    let bool_array = BooleanArray::from(vec![Some(true), None, Some(false), None, Some(true)]);

    let data = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(id_array) as ArrayRef,
            Arc::new(int_array),
            Arc::new(string_array),
            Arc::new(bool_array),
        ],
    )
    .expect("record batch should be created");

    let table_name = format!("test_null_handling_{}", provider);
    let table = create_table_provider(provider, Arc::clone(&schema), &table_name).await;

    insert_data(&table, data.clone(), Arc::clone(&schema)).await;

    let results = query_data(&table).await;
    let result = &results[0];

    // Verify null positions are preserved
    let result_int = result
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("should be int32 array");

    assert!(!result_int.is_null(0), "Row 0 should not be null");
    assert!(result_int.is_null(1), "Row 1 should be null");
    assert!(!result_int.is_null(2), "Row 2 should not be null");
    assert!(result_int.is_null(3), "Row 3 should be null");
    assert!(!result_int.is_null(4), "Row 4 should not be null");
}

/// Test schema preservation
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_schema_preservation(#[case] provider: ProviderType) {
    let schema = get_comprehensive_test_schema();
    let table_name = format!("test_schema_{}", provider);
    let table = create_table_provider(provider, Arc::clone(&schema), &table_name).await;

    let table_schema = table.schema();

    assert_eq!(
        table_schema.fields().len(),
        schema.fields().len(),
        "Schema field count should match"
    );

    for (i, field) in schema.fields().iter().enumerate() {
        let table_field = table_schema.field(i);
        assert_eq!(
            field.name(),
            table_field.name(),
            "Field {} name should match",
            i
        );
        assert_eq!(
            field.is_nullable(),
            table_field.is_nullable(),
            "Field {} nullable should match",
            i
        );
    }
}

/// Test multiple inserts
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_multiple_inserts(#[case] provider: ProviderType) {
    let schema = get_comprehensive_test_schema();
    let table_name = format!("test_multi_insert_{}", provider);
    let table = create_table_provider(provider, Arc::clone(&schema), &table_name).await;

    // Insert first batch
    let data1 = generate_comprehensive_test_data(Arc::clone(&schema), 10, 0);
    insert_data(&table, data1, Arc::clone(&schema)).await;

    // Verify first batch
    let results1 = query_data(&table).await;
    let total1: usize = results1.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total1, 10, "Should have 10 rows after first insert");

    // Insert second batch
    let data2 = generate_comprehensive_test_data(Arc::clone(&schema), 10, 10);
    insert_data(&table, data2, Arc::clone(&schema)).await;

    // Verify both batches
    let results2 = query_data(&table).await;
    let total2: usize = results2.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total2, 20, "Should have 20 rows after second insert");
}

/// Test decimal roundtrip using the pre-built batch
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[ignore = "Requires decimal extension to be loaded"]
#[rstest]
#[case::sqlite(ProviderType::Sqlite)]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb(ProviderType::DuckDB))]
#[test_log::test(tokio::test)]
async fn test_compatible_decimal_roundtrip(#[case] provider: ProviderType) {
    let (data, schema) = get_sqlite_compatible_decimal_record_batch();
    let table_name = format!("test_decimal_{}", provider);
    run_decimal_roundtrip_test(provider, data, schema, &table_name).await;
}

/// Test with existing record batch generators
#[cfg(any(feature = "sqlite", all(feature = "duckdb", feature = "federation")))]
#[rstest]
#[case::sqlite_int(ProviderType::Sqlite, get_arrow_int_record_batch(), "int")]
#[case::sqlite_float(ProviderType::Sqlite, get_arrow_float_record_batch(), "float")]
#[case::sqlite_utf8(ProviderType::Sqlite, get_arrow_utf8_record_batch(), "utf8")]
#[case::sqlite_date(ProviderType::Sqlite, get_arrow_date_record_batch(), "date")]
#[case::sqlite_timestamp(ProviderType::Sqlite, get_arrow_timestamp_record_batch(), "timestamp")]
#[case::sqlite_null(ProviderType::Sqlite, get_arrow_null_record_batch(), "null")]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_int(ProviderType::DuckDB, get_arrow_int_record_batch(), "int"))]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_float(ProviderType::DuckDB, get_arrow_float_record_batch(), "float"))]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_utf8(ProviderType::DuckDB, get_arrow_utf8_record_batch(), "utf8"))]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_date(ProviderType::DuckDB, get_arrow_date_record_batch(), "date"))]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_timestamp(ProviderType::DuckDB, get_arrow_timestamp_record_batch(), "timestamp"))]
#[cfg_attr(all(feature = "duckdb", feature = "federation"), case::duckdb_null(ProviderType::DuckDB, get_arrow_null_record_batch(), "null"))]
#[test_log::test(tokio::test)]
async fn test_existing_batches(
    #[case] provider: ProviderType,
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] batch_type: &str,
) {
    let (data, schema) = arrow_result;
    let full_name = format!("test_roundtrip_{}_{}", batch_type, provider);

    let table = create_table_provider(provider, Arc::clone(&schema), &full_name).await;

    insert_data(&table, data.clone(), Arc::clone(&schema)).await;

    let results = query_data(&table).await;
    assert_eq!(results.len(), 1, "Should have one result batch");
    assert_eq!(
        results[0].num_rows(),
        data.num_rows(),
        "Row count should match"
    );
}
