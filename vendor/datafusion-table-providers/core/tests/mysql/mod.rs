use datafusion::{datasource::memory::MemorySourceConfig, execution::context::SessionContext};
use datafusion_table_providers::sql::{
    db_connection_pool::DbConnectionPool, sql_provider_datafusion::SqlTable,
};
use mysql_async::prelude::ToValue;
use rstest::{fixture, rstest};
use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{i256, DataType, Field, Schema, TimeUnit, UInt16Type},
};

use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;

use crate::arrow_record_batch_gen::*;
use crate::docker::RunningContainer;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProviderFactory;
use datafusion::common::{Constraints, ToDFSchema};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::physical_plan::collect;
#[cfg(feature = "mysql-federation")]
use datafusion_federation::schema_cast::record_convert::try_cast_to;
use datafusion_table_providers::mysql::MySQLTableProviderFactory;
use secrecy::ExposeSecret;
use tokio::sync::Mutex;

mod common;

async fn test_mysql_timestamp_types(port: usize) {
    let create_table_stmt = "
        CREATE TABLE timestamp_table (
    timestamp_no_fraction TIMESTAMP(0) DEFAULT CURRENT_TIMESTAMP, 
    timestamp_one_fraction TIMESTAMP(1),  
    timestamp_two_fraction TIMESTAMP(2), 
    timestamp_three_fraction TIMESTAMP(3),                   
    timestamp_four_fraction TIMESTAMP(4),
    timestamp_five_fraction TIMESTAMP(5),
    timestamp_six_fraction TIMESTAMP(6) 
);
        ";
    let insert_table_stmt = "
INSERT INTO timestamp_table (
    timestamp_no_fraction, 
    timestamp_one_fraction, 
    timestamp_two_fraction, 
    timestamp_three_fraction, 
    timestamp_four_fraction, 
    timestamp_five_fraction, 
    timestamp_six_fraction
) 
VALUES 
(
    '2024-09-12 10:00:00',             
    '2024-09-12 10:00:00.1',
    '2024-09-12 10:00:00.12',
    '2024-09-12 10:00:00.123',
    '2024-09-12 10:00:00.1234',        
    '2024-09-12 10:00:00.12345',       
    '2024-09-12 10:00:00.123456'
);
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_no_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_one_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_two_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_three_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_four_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_five_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_six_fraction",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_100_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_120_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_400])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_450])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "timestamp_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

/// Tests the MySQL TIMESTAMP with time zone override.
/// The test verifies that the TIMESTAMP type correctly adjusts to the specified time zone when retrieved from the database.
/// `TIMESTAMP` columns should be automatically converted to the specified time zone,
/// while `DATETIME` columns should remain unchanged.
async fn test_mysql_timestamp_tz_override(port: usize) {
    let create_table_stmt = "
        CREATE TABLE timestamp_tz_table (
            ts TIMESTAMP,
            dt DATETIME
        );
    ";
    // values will be inserted in UTC
    let insert_table_stmt = "
        INSERT INTO timestamp_tz_table (ts, dt)
        VALUES
            ('2024-09-12 10:00:00', '2024-09-12 10:00:00');
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("dt", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]));

    // Both columns should remain unchanged as target time zone is UTC (same as insert time zone)
    let expected_utc = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "timestamp_tz_table",
        create_table_stmt,
        insert_table_stmt,
        expected_utc,
        Some("UTC"),
    )
    .await;

    // "+02:00"
    let expected_custom_tz = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            // ts: TIMESTAMP column, should be shifted +2 hours (timezone override)
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_726_135_200_000_000 + 2 * 60 * 60 * 1_000_000,
            ])),
            // dt: DATETIME column, should remain unchanged
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "timestamp_tz_table",
        create_table_stmt,
        insert_table_stmt,
        expected_custom_tz,
        Some("+02:00"), // Override time zone to +02:00
    )
    .await;
}

async fn test_mysql_datetime_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE datetime_table (
    dt0 DATETIME(0),  
    dt1 DATETIME(1), 
    dt2 DATETIME(2), 
    dt3 DATETIME(3), 
    dt4 DATETIME(4), 
    dt5 DATETIME(5), 
    dt6 DATETIME(6)  
);

        ";
    let insert_table_stmt = "
INSERT INTO datetime_table (dt0, dt1, dt2, dt3, dt4, dt5, dt6)
VALUES (
    '2024-09-12 10:00:00',
    '2024-09-12 10:00:00.1',
    '2024-09-12 10:00:00.12',
    '2024-09-12 10:00:00.123',
    '2024-09-12 10:00:00.1234',
    '2024-09-12 10:00:00.12345',
    '2024-09-12 10:00:00.123456'
);
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "dt0",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt1",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt2",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt3",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt4",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt5",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "dt6",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_000_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_100_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_120_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_000])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_400])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_450])),
            Arc::new(TimestampMicrosecondArray::from(vec![1_726_135_200_123_456])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "datetime_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_time_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE time_table (
    t0 TIME(0),  
    t1 TIME(1), 
    t2 TIME(2), 
    t3 TIME(3), 
    t4 TIME(4), 
    t5 TIME(5), 
    t6 TIME(6)
);
        ";
    let insert_table_stmt = "
INSERT INTO time_table (t0, t1, t2, t3, t4, t5, t6)
VALUES 
    ('12:30:00', 
     '12:30:00.1', 
     '12:30:00.12', 
     '12:30:00.123', 
     '12:30:00.1234', 
     '12:30:00.12345', 
     '12:30:00.123456');
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("t0", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t1", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t2", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t3", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t4", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t5", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new("t6", DataType::Time64(TimeUnit::Nanosecond), true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 100_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 120_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_000_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_400_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_450_000,
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                (12 * 3600 + 30 * 60) * 1_000_000_000 + 123_456_000,
            ])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "time_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_enum_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE enum_table (
    status ENUM('active', 'inactive', 'pending', 'suspended')
);
        ";
    let insert_table_stmt = "
INSERT INTO enum_table (status)
VALUES
(NULL),
('active'),
('inactive'),
('pending'),
('suspended'),
('inactive');
        ";

    let mut builder = StringDictionaryBuilder::<UInt16Type>::new();
    builder.append_null();
    builder.append_value("active");
    builder.append_value("inactive");
    builder.append_value("pending");
    builder.append_value("suspended");
    builder.append_value("inactive");

    let array: DictionaryArray<UInt16Type> = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
        true,
    )]));

    let expected_record = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .expect("Failed to created arrow dictionary array record batch");

    arrow_mysql_one_way(
        port,
        "enum_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_blob_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE blobs_table (
    tinyblob_col    TINYBLOB,
    tinytext_col    TINYTEXT,
    mediumblob_col  MEDIUMBLOB,
    mediumtext_col  MEDIUMTEXT,
    blob_col        BLOB,
    text_col        TEXT,
    longblob_col    LONGBLOB,
    longtext_col    LONGTEXT
);
        ";
    let insert_table_stmt = "
INSERT INTO blobs_table (
    tinyblob_col, tinytext_col, mediumblob_col, mediumtext_col, blob_col, text_col, longblob_col, longtext_col
)
VALUES
    (
        'small_blob', 'small_text',
        'medium_blob', 'medium_text',
        'larger_blob', 'larger_text',
        'very_large_blob', 'very_large_text'
    );
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("tinyblob_col", DataType::Binary, true),
        Field::new("tinytext_col", DataType::Utf8, true),
        Field::new("mediumblob_col", DataType::Binary, true),
        Field::new("mediumtext_col", DataType::Utf8, true),
        Field::new("blob_col", DataType::Binary, true),
        Field::new("text_col", DataType::Utf8, true),
        Field::new("longblob_col", DataType::LargeBinary, true),
        Field::new("longtext_col", DataType::LargeUtf8, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(BinaryArray::from_vec(vec![b"small_blob"])),
            Arc::new(StringArray::from(vec!["small_text"])),
            Arc::new(BinaryArray::from_vec(vec![b"medium_blob"])),
            Arc::new(StringArray::from(vec!["medium_text"])),
            Arc::new(BinaryArray::from_vec(vec![b"larger_blob"])),
            Arc::new(StringArray::from(vec!["larger_text"])),
            Arc::new(LargeBinaryArray::from_vec(vec![b"very_large_blob"])),
            Arc::new(LargeStringArray::from(vec!["very_large_text"])),
        ],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "blobs_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_string_types(port: usize) {
    let create_table_stmt = "
CREATE TABLE string_table (
    name VARCHAR(255),
    data VARBINARY(255),
    fixed_name CHAR(10),
    fixed_data BINARY(10)
);
        ";
    let insert_table_stmt = "
INSERT INTO string_table (name, data, fixed_name, fixed_data)
VALUES 
('Alice', 'Alice', 'ALICE', 'abc'),
('Bob', 'Bob', 'BOB', 'bob1234567'),
('Charlie', 'Charlie', 'CHARLIE', '0123456789'),
('Dave', 'Dave', 'DAVE', 'dave000000');
        ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("data", DataType::Binary, true),
        Field::new("fixed_name", DataType::Utf8, true),
        Field::new("fixed_data", DataType::Binary, true),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])),
            Arc::new(BinaryArray::from_vec(vec![
                b"Alice", b"Bob", b"Charlie", b"Dave",
            ])),
            Arc::new(StringArray::from(vec!["ALICE", "BOB", "CHARLIE", "DAVE"])),
            Arc::new(BinaryArray::from_vec(vec![
                b"abc\0\0\0\0\0\0\0",
                b"bob1234567",
                b"0123456789",
                b"dave000000",
            ])),
        ],
    )
    .expect("Failed to created arrow record batch");
    arrow_mysql_one_way(
        port,
        "string_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_decimal_types_to_decimal256(port: usize) {
    let create_table_stmt = "
CREATE TABLE high_precision_decimal (
    decimal_values DECIMAL(50, 10)
);
        ";
    let insert_table_stmt = "
INSERT INTO high_precision_decimal (decimal_values) VALUES
(NULL),
(1234567890123456789012345678901234567890.1234567890),
(-9876543210987654321098765432109876543210.9876543210),
(0.0000000001),
(-0.000000001),
(0);
        ";

    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_values",
        DataType::Decimal256(50, 10),
        true,
    )]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal256Array::from(vec![
                None,
                Some(
                    i256::from_string("12345678901234567890123456789012345678901234567890")
                        .unwrap(),
                ),
                Some(
                    i256::from_string("-98765432109876543210987654321098765432109876543210")
                        .unwrap(),
                ),
                Some(i256::from_string("1").unwrap()),
                Some(i256::from_string("-10").unwrap()),
                Some(i256::from_string("0").unwrap()),
            ])
            .with_precision_and_scale(50, 10)
            .expect("Failed to create decimal256 array"),
        )],
    )
    .expect("Failed to created arrow record batch");

    arrow_mysql_one_way(
        port,
        "high_precision_decimal",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_zero_date_type(port: usize) {
    let create_table_stmt = "
        CREATE TABLE zero_datetime_test_table (
            col_date DATE,
            col_time TIME,
            col_datetime DATETIME,
            col_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ";

    let insert_table_stmt = "
        INSERT INTO zero_datetime_test_table (
            col_date, col_time, col_datetime, col_timestamp
        ) 
        VALUES 
        -- Real Values
        ('2023-05-15', '10:00:00', '2024-09-12 10:00:00', '2024-09-12 10:00:00'),
        -- Null Values
        (NULL, NULL, NULL, NULL),
        -- Zero Values
        ('0000-00-00', '00:00:00', '0000-00-00 00:00:00', '0000-00-00 00:00:00');
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("col_date", DataType::Date32, true),
        Field::new("col_time", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new(
            "col_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "col_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![
                Some(19492), // '2023-05-15'
                None,        // NULL
                None,        // '0000-00-00'
            ])),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(36000000000000), // '10:00:00'
                None,                 // NULL
                Some(0),              // '00:00:00'
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_726_135_200_000_000), // '2024-09-12 10:00:00'
                None,                        // NULL
                None,                        // '0000-00-00 00:00:00'
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_726_135_200_000_000), // '2024-09-12 10:00:00'
                None,                        // NULL
                None,                        // '0000-00-00 00:00:00'
            ])),
        ],
    )
    .expect("Failed to create expected arrow record batch");

    arrow_mysql_one_way(
        port,
        "zero_datetime_test_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_decimal_types_to_decimal128(port: usize) {
    let create_table_stmt = "
        CREATE TABLE IF NOT EXISTS decimal_table (decimal_col DECIMAL(10, 2));
        ";
    let insert_table_stmt = "
        INSERT INTO decimal_table (decimal_col) VALUES (NULL), (12);
        ";

    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_col",
        DataType::Decimal128(10, 2),
        true,
    )]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(
            Decimal128Array::from(vec![None, Some(i128::from(1200))])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        )],
    )
    .expect("Failed to created arrow record batch");

    let _ = arrow_mysql_one_way(
        port,
        "decimal_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn test_mysql_nullability_constraints(port: usize) {
    let create_table_stmt = "
        CREATE TABLE nullability_table (
            id INT NOT NULL,
            name VARCHAR(50) NOT NULL,
            age INT,
            email VARCHAR(100),
            score DECIMAL(5, 2) NOT NULL
        );
    ";
    let insert_table_stmt = "
        INSERT INTO nullability_table (id, name, age, email, score)
        VALUES
        (1, 'Alice', 30, 'alice@example.com', 95.50),
        (2, 'Bob', NULL, NULL, 87.25),
        (3, 'Charlie', 25, 'charlie@example.com', 92.00);
    ";

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("score", DataType::Decimal128(5, 2), false),
    ]));

    let expected_record = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int32Array::from(vec![Some(30), None, Some(25)])),
            Arc::new(StringArray::from(vec![
                Some("alice@example.com"),
                None,
                Some("charlie@example.com"),
            ])),
            Arc::new(
                Decimal128Array::from(vec![i128::from(9550), i128::from(8725), i128::from(9200)])
                    .with_precision_and_scale(5, 2)
                    .unwrap(),
            ),
        ],
    )
    .expect("Failed to create expected arrow record batch");

    arrow_mysql_one_way(
        port,
        "nullability_table",
        create_table_stmt,
        insert_table_stmt,
        expected_record,
        None,
    )
    .await;
}

async fn arrow_mysql_one_way(
    port: usize,
    table_name: &str,
    create_table_stmt: &str,
    insert_table_stmt: &str,
    expected_record: RecordBatch,
    test_query_tz: Option<&str>,
) -> Vec<RecordBatch> {
    tracing::debug!("Running tests on {table_name}");

    let ctx = SessionContext::new();
    // For dataset initialization we always use UTC (default) timezone
    let pool = common::get_mysql_connection_pool(port, None)
        .await
        .expect("MySQL connection pool for test table creation should be created");

    let db_conn = pool
        .connect_direct()
        .await
        .expect("Connection should be established");

    // Disable NO_ZERO_DATE and NO_ZERO_IN_DATE (requied for `test_mysql_zero_date_type`)
    let _ = db_conn
        .execute(
            "SET SESSION sql_mode = REPLACE(REPLACE(REPLACE(@@SESSION.sql_mode, 'NO_ZERO_IN_DATE,', ''), 'NO_ZERO_DATE,', ''), 'NO_ZERO_DATE', '')",
            &[]
        )
        .await
        .expect("SQL mode should be adjusted");

    // Drop table if already exists
    let _ = db_conn
        .execute(format!("DROP TABLE IF EXISTS {table_name}").as_str(), &[])
        .await
        .expect("MySQL table should be dropped if exists");

    // Create table and insert data into mysql test_table
    let _ = db_conn
        .execute(create_table_stmt, &[])
        .await
        .expect("MySQL table should be created");

    let _ = db_conn
        .execute(insert_table_stmt, &[])
        .await
        .expect("MySQL table data should be inserted");

    // For the test query, use a new connection pool with optional time zone override
    let pool = common::get_mysql_connection_pool(port, test_query_tz)
        .await
        .expect("MySQL connection pool for test query should be created");

    // Register datafusion table, test mysql row -> arrow conversion
    let sqltable_pool: Arc<
        dyn DbConnectionPool<mysql_async::Conn, &'static (dyn ToValue + Sync)>
            + Send
            + Sync
            + 'static,
    > = Arc::new(pool);
    let table = SqlTable::new("mysql", &sqltable_pool, table_name, None)
        .await
        .expect("Table should be created");

    ctx.register_table(table_name, Arc::new(table))
        .expect("Table should be registered");

    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    tracing::debug!(
        "MySQL returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0], expected_record);

    record_batch
}

#[allow(unused_variables)]
async fn arrow_mysql_round_trip(
    port: usize,
    arrow_record: RecordBatch,
    source_schema: SchemaRef,
    table_name: &str,
) {
    let factory = MySQLTableProviderFactory::new();
    let ctx = SessionContext::new();
    let cmd = CreateExternalTable {
        schema: Arc::new(arrow_record.schema().to_dfschema().expect("to df schema")),
        name: table_name.into(),
        location: "".to_string(),
        file_type: "".to_string(),
        table_partition_cols: vec![],
        if_not_exists: false,
        temporary: false,
        definition: None,
        order_exprs: vec![],
        unbounded: false,
        options: common::get_mysql_params(port, None)
            .into_iter()
            .map(|(k, v)| (k, v.expose_secret().to_string()))
            .collect(),
        constraints: Constraints::default(),
        column_defaults: Default::default(),
    };
    let table_provider = factory
        .create(&ctx.state(), &cmd)
        .await
        .expect("table provider created");

    let ctx = SessionContext::new();
    let mem_exec = MemorySourceConfig::try_new_exec(
        &[vec![arrow_record.clone()]],
        arrow_record.schema(),
        None,
    )
    .expect("memory exec created");
    let insert_plan = table_provider
        .insert_into(&ctx.state(), mem_exec, InsertOp::Overwrite)
        .await
        .expect("insert plan created");

    let _ = collect(insert_plan, ctx.task_ctx())
        .await
        .expect("insert done");
    ctx.register_table(table_name, table_provider)
        .expect("Table should be registered");
    let sql = format!("SELECT * FROM {table_name}");
    let df = ctx
        .sql(&sql)
        .await
        .expect("DataFrame should be created from query");

    let record_batch = df.collect().await.expect("RecordBatch should be collected");
    tracing::debug!("Original Arrow Record Batch: {:?}", arrow_record.columns());
    tracing::debug!(
        "MySQL returned Record Batch: {:?}",
        record_batch[0].columns()
    );

    #[cfg(feature = "mysql-federation")]
    let casted_result =
        try_cast_to(record_batch[0].clone(), source_schema).expect("Failed to cast record batch");

    // Check results
    assert_eq!(record_batch.len(), 1);
    assert_eq!(record_batch[0].num_rows(), arrow_record.num_rows());
    assert_eq!(record_batch[0].num_columns(), arrow_record.num_columns());

    #[cfg(feature = "mysql-federation")]
    assert_eq!(arrow_record, casted_result);
}

#[derive(Debug)]
struct ContainerManager {
    port: usize,
    claimed: bool,
}

#[fixture]
#[once]
fn container_manager() -> Mutex<ContainerManager> {
    Mutex::new(ContainerManager {
        port: crate::get_random_port(),
        claimed: false,
    })
}

async fn start_mysql_container(port: usize) -> RunningContainer {
    let running_container = common::start_mysql_docker_container(port)
        .await
        .expect("MySQL container to start");

    tracing::debug!("Container started");

    running_container
}

#[rstest]
#[case::binary(get_arrow_binary_record_batch(), "binary")]
#[case::int(get_arrow_int_record_batch(), "int")]
#[case::float(get_arrow_float_record_batch(), "float")]
#[case::utf8(get_arrow_utf8_record_batch(), "utf8")]
#[case::time(get_arrow_time_record_batch(), "time")]
#[case::timestamp(get_arrow_timestamp_record_batch_without_timezone(), "timestamp")]
#[case::date(get_arrow_date_record_batch(), "date")]
#[case::struct_type(get_arrow_struct_record_batch(), "struct")]
// MySQL only supports up to 65 precision for decimal through REAL type.
#[case::decimal(get_mysql_arrow_decimal_record(), "decimal")]
#[ignore] // TODO: interval types are broken in MySQL - Interval is not available in MySQL.
#[case::interval(get_arrow_interval_record_batch(), "interval")]
#[case::duration(get_arrow_duration_record_batch(), "duration")]
#[ignore] // TODO: array types are broken in MySQL - array is not available in MySQL.
#[case::list(get_arrow_list_record_batch(), "list")]
#[case::null(get_arrow_null_record_batch(), "null")]
#[ignore]
#[case::bytea_array(get_arrow_bytea_array_record_batch(), "bytea_array")]
#[test_log::test(tokio::test)]
async fn test_arrow_mysql_roundtrip(
    container_manager: &Mutex<ContainerManager>,
    #[case] arrow_result: (RecordBatch, SchemaRef),
    #[case] table_name: &str,
) {
    let mut container_manager = container_manager.lock().await;
    if !container_manager.claimed {
        container_manager.claimed = true;
        start_mysql_container(container_manager.port).await;
    }

    arrow_mysql_round_trip(
        container_manager.port,
        arrow_result.0,
        arrow_result.1,
        table_name,
    )
    .await;
}

#[rstest]
#[test_log::test(tokio::test)]
async fn test_mysql_arrow_oneway() {
    let port = crate::get_random_port();
    let mysql_container = start_mysql_container(port).await;

    test_mysql_timestamp_types(port).await;
    test_mysql_timestamp_tz_override(port).await;
    test_mysql_datetime_types(port).await;
    test_mysql_time_types(port).await;
    test_mysql_enum_types(port).await;
    test_mysql_blob_types(port).await;
    test_mysql_string_types(port).await;
    test_mysql_decimal_types_to_decimal128(port).await;
    test_mysql_decimal_types_to_decimal256(port).await;
    test_mysql_zero_date_type(port).await;
    test_mysql_nullability_constraints(port).await;

    mysql_container.remove().await.expect("container to stop");
}
