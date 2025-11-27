use crate::arrow_record_batch_gen::*;
use arrow::array::{
    BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{dml::InsertOp, CreateExternalTable};
use datafusion::physical_plan::collect;
use datafusion::sql::TableReference;
#[cfg(feature = "sqlite-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, InsertBuilder,
};
use datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPoolFactory;
use datafusion_table_providers::sql::db_connection_pool::{DbConnectionPool, Mode};
use datafusion_table_providers::sql::sql_provider_datafusion::SqlTable;
use datafusion_table_providers::sqlite::{DynSqliteConnectionPool, SqliteTableProviderFactory};
use datafusion_table_providers::util::test::MockExec;
use rstest::rstest;
use std::collections::HashMap;
use std::sync::Arc;

async fn arrow_sqlite_round_trip(
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    tracing::debug!("Running tests on {table_name}");
    let ctx = SessionContext::new();

    let pool = SqliteConnectionPoolFactory::new(
        ":memory:",
        Mode::Memory,
        std::time::Duration::from_millis(5000),
    )
    .build()
    .await
    .expect("Sqlite connection pool to be created");

    let conn = pool
        .connect()
        .await
        .expect("Sqlite connection should be established");
    let conn = conn.as_async().unwrap();

    // Create sqlite table from arrow records and insert arrow records
    let schema = Arc::clone(&arrow_record.schema());
    let create_table_stmts = CreateTableBuilder::new(schema, table_name).build_sqlite();
    let insert_table_stmt = InsertBuilder::new(
        &TableReference::from(table_name),
        vec![arrow_record.clone()],
    )
    .build_sqlite(None)
    .expect("SQLite insert statement should be constructed");

    // Test arrow -> Sqlite row coverage
    let _ = conn
        .execute(&create_table_stmts, &[])
        .await
        .expect("Sqlite table should be created");
    let _ = conn
        .execute(&insert_table_stmt, &[])
        .await
        .expect("Sqlite data should be inserted");

    // Register datafusion table, test row -> arrow conversion
    let sqltable_pool: Arc<DynSqliteConnectionPool> = Arc::new(pool);

    // Perform the test twice: first, simulate a request without a known schema;
    // then, test result conversion with a known projected schema (matching the test RecordBatch).
    for projected_schema in [None, Some(arrow_record.schema())] {
        if ctx
            .table_exist(table_name)
            .expect("should be able to check if table exists")
        {
            ctx.deregister_table(table_name)
                .expect("Table should be deregistered");
        }

        let table = match projected_schema {
            None => SqlTable::new("sqlite", &sqltable_pool, table_name, None)
                .await
                .expect("Table should be created"),
            Some(schema) => {
                SqlTable::new_with_schema("sqlite", &sqltable_pool, schema, table_name, None)
            }
        };

        ctx.register_table(table_name, Arc::new(table))
            .expect("Table should be registered");

        let sql = format!("SELECT * FROM {table_name}");
        let df = ctx
            .sql(&sql)
            .await
            .expect("DataFrame should be created from query");

        let record_batch = df.collect().await.expect("RecordBatch should be collected");

        #[cfg(feature = "sqlite-federation")]
        let casted_record =
            try_cast_to(record_batch[0].clone(), Arc::clone(&source_schema)).unwrap();

        tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
        tracing::debug!(
            "Sqlite returned Record Batch: {:?}",
            record_batch[0].columns()
        );

        // Check results
        assert_eq!(record_batch.len(), 1);
        assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
        assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());
        #[cfg(feature = "sqlite-federation")]
        assert_eq!(casted_record, arrow_record);
    }
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
#[ignore] // Requires a custom sqlite extension for decimal types
#[case::decimal(get_arrow_decimal_record_batch(), "decimal")]
#[ignore] // TODO: interval types are broken in SQLite - Interval is not available in Sqlite.
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[test_log::test(tokio::test)]
async fn test_arrow_sqlite_roundtrip(
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    arrow_sqlite_round_trip(
        arrow_result.0,
        arrow_result.1,
        &format!("{table_name}_types"),
    )
    .await;
}

/// Helper function to create test data for comprehensive type testing
fn create_comprehensive_test_data() -> (RecordBatch, SchemaRef) {
    // Create a comprehensive schema with various data types supported by SQLite
    // Note: Date and Timestamp types have separate tests as they have special handling
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_int8", DataType::Int8, true),
        Field::new("col_int16", DataType::Int16, true),
        Field::new("col_int32", DataType::Int32, true),
        Field::new("col_int64", DataType::Int64, true),
        Field::new("col_uint8", DataType::UInt8, true),
        Field::new("col_uint16", DataType::UInt16, true),
        Field::new("col_uint32", DataType::UInt32, true),
        Field::new("col_uint64", DataType::UInt64, true),
        Field::new("col_float32", DataType::Float32, true),
        Field::new("col_float64", DataType::Float64, true),
        Field::new("col_utf8", DataType::Utf8, true),
        Field::new("col_large_utf8", DataType::LargeUtf8, true),
        Field::new("col_bool", DataType::Boolean, true),
        Field::new("col_binary", DataType::Binary, true),
        Field::new("col_large_binary", DataType::LargeBinary, true),
        Field::new(
            "col_time32_second",
            DataType::Time32(TimeUnit::Second),
            true,
        ),
        Field::new(
            "col_time32_milli",
            DataType::Time32(TimeUnit::Millisecond),
            true,
        ),
        Field::new(
            "col_time64_micro",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        ),
        Field::new(
            "col_time64_nano",
            DataType::Time64(TimeUnit::Nanosecond),
            true,
        ),
    ]));

    // Create test data with multiple rows including nulls
    let col_int8 = Int8Array::from(vec![Some(1), None, Some(-127), Some(42)]);
    let col_int16 = Int16Array::from(vec![Some(100), Some(-200), None, Some(300)]);
    let col_int32 = Int32Array::from(vec![Some(1000), None, Some(-2000), Some(3000)]);
    let col_int64 = Int64Array::from(vec![None, Some(100000), Some(-200000), Some(300000)]);
    let col_uint8 = UInt8Array::from(vec![Some(255), Some(128), None, Some(64)]);
    let col_uint16 = UInt16Array::from(vec![Some(65535), None, Some(32768), Some(1000)]);
    let col_uint32 = UInt32Array::from(vec![
        None,
        Some(4294967295),
        Some(2147483648),
        Some(1000000),
    ]);
    // SQLite uses signed 64-bit integers, so we need to stay within i64::MAX
    let col_uint64 = UInt64Array::from(vec![
        Some(9223372036854775807u64), // i64::MAX
        Some(1000000u64),
        None,
        Some(500000u64),
    ]);
    let col_float32 = Float32Array::from(vec![
        Some(1.5),
        None,
        Some(-std::f32::consts::PI),
        Some(2.71),
    ]);
    let col_float64 = Float64Array::from(vec![
        None,
        Some(std::f64::consts::E),
        Some(-1.414),
        Some(std::f64::consts::PI),
    ]);
    let col_utf8 = StringArray::from(vec![Some("hello"), Some("world"), None, Some("test")]);
    let col_large_utf8 = LargeStringArray::from(vec![
        None,
        Some("large text"),
        Some("data"),
        Some("more data"),
    ]);
    let col_bool = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
    let col_binary = BinaryArray::from_vec(vec![b"bin1", b"bin2", b"", b"bin3"]);
    let col_large_binary = LargeBinaryArray::from_vec(vec![b"large1", b"", b"large3", b"large4"]);
    let col_time32_second =
        Time32SecondArray::from(vec![Some(3600), Some(7200), None, Some(10800)]);
    let col_time32_milli =
        Time32MillisecondArray::from(vec![None, Some(3600000), Some(7200000), Some(10800000)]);
    let col_time64_micro = Time64MicrosecondArray::from(vec![
        Some(3600000000),
        None,
        Some(7200000000),
        Some(10800000000),
    ]);
    let col_time64_nano = Time64NanosecondArray::from(vec![
        Some(3600000000000),
        Some(7200000000000),
        None,
        Some(10800000000000),
    ]);

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(col_int8),
            Arc::new(col_int16),
            Arc::new(col_int32),
            Arc::new(col_int64),
            Arc::new(col_uint8),
            Arc::new(col_uint16),
            Arc::new(col_uint32),
            Arc::new(col_uint64),
            Arc::new(col_float32),
            Arc::new(col_float64),
            Arc::new(col_utf8),
            Arc::new(col_large_utf8),
            Arc::new(col_bool),
            Arc::new(col_binary),
            Arc::new(col_large_binary),
            Arc::new(col_time32_second),
            Arc::new(col_time32_milli),
            Arc::new(col_time64_micro),
            Arc::new(col_time64_nano),
        ],
    )
    .expect("Failed to create record batch");

    (record_batch, schema)
}

/// Test comprehensive round-trip of Arrow data through SqliteTableProviderFactory
/// This test creates a table via TableProviderFactory, inserts data using insert_into, and reads it back
/// Tests both prepared statements and inline SQL paths to ensure they produce identical results
#[rstest]
#[case::prepared_statements(true, "comprehensive_types_test_prepared")]
#[case::inline_sql(false, "comprehensive_types_test_inline")]
#[test_log::test(tokio::test)]
async fn test_sqlite_table_provider_roundtrip(
    #[case] use_prepared_statements: bool,
    #[case] table_name: &str,
) {
    let ctx = SessionContext::new();
    let (record_batch, schema) = create_comprehensive_test_data();

    let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

    // Create external table using SqliteTableProviderFactory with specified config
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

    let factory = SqliteTableProviderFactory::default()
        .with_batch_insert_use_prepared_statements(use_prepared_statements);
    let table = factory
        .create(&ctx.state(), &external_table)
        .await
        .expect("table should be created");

    // Insert data using TableProvider's insert_into method
    let exec = MockExec::new(vec![Ok(record_batch.clone())], Arc::clone(&schema));
    let insertion = table
        .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
        .await
        .expect("insertion should be successful");

    collect(insertion, ctx.task_ctx())
        .await
        .expect("insert should complete");

    // Register the table to query it back
    ctx.register_table(table_name, Arc::clone(&table))
        .expect("table should be registered");

    // Query the data back
    let query_sql = format!("SELECT * FROM {table_name}");
    let df = ctx.sql(&query_sql).await.expect("query should succeed");

    let result_batches = df.collect().await.expect("should collect results");

    // Verify results
    assert_eq!(result_batches.len(), 1, "Should have one result batch");
    let result_batch = &result_batches[0];

    assert_eq!(
        result_batch.num_rows(),
        record_batch.num_rows(),
        "Should have same number of rows"
    );
    assert_eq!(
        result_batch.num_columns(),
        record_batch.num_columns(),
        "Should have same number of columns"
    );

    // Cast the result back to the original schema for comparison
    let casted_result = try_cast_to(result_batch.clone(), Arc::clone(&schema))
        .expect("should cast result to original schema");

    // Verify the data matches
    assert_eq!(
        casted_result, record_batch,
        "Round-tripped data should match original"
    );
}
