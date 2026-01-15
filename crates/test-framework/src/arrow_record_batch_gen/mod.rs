use chrono::NaiveDate;
use datafusion::arrow::array::RecordBatch;
#[expect(clippy::wildcard_imports)]
use datafusion::arrow::{
    array::*,
    datatypes::{
        DataType, Date32Type, Date64Type, Field, Int8Type, IntervalDayTime, IntervalMonthDayNano,
        IntervalUnit, Schema, SchemaRef, TimeUnit, i256,
    },
};
use std::sync::Arc;

// Helper functions to create arrow record batches of different types

// Binary - comprehensive edge cases
#[must_use]
pub fn get_arrow_binary_record_batch() -> (RecordBatch, SchemaRef) {
    // Binary Array with comprehensive edge cases
    let binary_array = BinaryArray::from_opt_vec(vec![
        Some(b"one".as_slice()),          // normal ASCII
        Some(b"".as_slice()),             // empty bytes
        Some(b"\x00\x00\x00".as_slice()), // null bytes only
        Some(b"\xff\xfe\xfd".as_slice()), // high byte values
        Some(b"\x00mid\x00".as_slice()),  // null bytes in middle
        Some(b"a".as_slice()),            // single byte
        // 1KB of data to test larger payloads
        Some(&[0xAB; 1024]),
        None, // null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "binary",
        DataType::Binary,
        true,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(binary_array)])
        .expect("failed to create arrow binary record batch");

    (record_batch, schema)
}

// LargeBinary - comprehensive edge cases
#[must_use]
pub fn get_arrow_large_binary_record_batch() -> (RecordBatch, SchemaRef) {
    // LargeBinary Array with comprehensive edge cases
    let large_binary_array = LargeBinaryArray::from_opt_vec(vec![
        Some(b"one".as_slice()),          // normal ASCII
        Some(b"".as_slice()),             // empty bytes
        Some(b"\x00\x00\x00".as_slice()), // null bytes only
        Some(b"\xff\xfe\xfd".as_slice()), // high byte values
        Some(b"\x00mid\x00".as_slice()),  // null bytes in middle
        Some(b"a".as_slice()),            // single byte
        // 1KB of data to test larger payloads
        Some(&[0xCD; 1024]),
        None, // null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "large_binary",
        DataType::LargeBinary,
        true,
    )]));

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(large_binary_array)])
            .expect("failed to create arrow binary record batch");

    (record_batch, schema)
}

// FixedSizeBinary - comprehensive edge cases (16 bytes)
#[must_use]
pub fn get_arrow_fixed_sized_binary_record_batch() -> (RecordBatch, SchemaRef) {
    // FixedSizeBinary Array with edge cases (16 bytes = common UUID size)
    // Note: FixedSizeBinaryArray::from requires Option<&[u8]>, not Option<[u8; N]>
    let val_sequential: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
    let val_zeros: [u8; 16] = [0; 16];
    let val_ones: [u8; 16] = [0xFF; 16];
    let val_alternating: [u8; 16] = [
        0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00,
        0xFF,
    ];
    let val_uuid_like: [u8; 16] = [
        0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        0xf0,
    ];
    let input_arg: Vec<Option<&[u8]>> = vec![
        Some(&val_sequential),  // sequential
        Some(&val_zeros),       // all zeros
        Some(&val_ones),        // all 0xFF
        Some(&val_alternating), // alternating
        Some(&val_uuid_like),   // UUID-like pattern
        None,                   // null value
    ];
    let fixed_size_binary_array = FixedSizeBinaryArray::from(input_arg);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "fixed_size_binary",
        DataType::FixedSizeBinary(16),
        true,
    )]));

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(fixed_size_binary_array)])
            .expect("failed to create arrow binary record batch");

    (record_batch, schema)
}

// All Int types - comprehensive edge cases with min/max/zero/null
#[must_use]
pub fn get_arrow_int_record_batch() -> (RecordBatch, SchemaRef) {
    // Arrow Integer Types with comprehensive edge cases
    // Each array: [min, max, zero, positive, negative (if signed), null]
    let int8_arr = Int8Array::from(vec![
        Some(i8::MIN),
        Some(i8::MAX),
        Some(0),
        Some(42),
        Some(-42),
        None,
    ]);
    let int16_arr = Int16Array::from(vec![
        Some(i16::MIN),
        Some(i16::MAX),
        Some(0),
        Some(1000),
        Some(-1000),
        None,
    ]);
    let int32_arr = Int32Array::from(vec![
        Some(i32::MIN),
        Some(i32::MAX),
        Some(0),
        Some(100_000),
        Some(-100_000),
        None,
    ]);
    let int64_arr = Int64Array::from(vec![
        Some(i64::MIN),
        Some(i64::MAX),
        Some(0),
        Some(10_000_000_000),
        Some(-10_000_000_000),
        None,
    ]);
    let uint8_arr = UInt8Array::from(vec![
        Some(u8::MIN),
        Some(u8::MAX),
        Some(0),
        Some(128),
        Some(1),
        None,
    ]);
    let uint16_arr = UInt16Array::from(vec![
        Some(u16::MIN),
        Some(u16::MAX),
        Some(0),
        Some(32768),
        Some(1),
        None,
    ]);
    let uint32_arr = UInt32Array::from(vec![
        Some(u32::MIN),
        Some(u32::MAX),
        Some(0),
        Some(2_147_483_648),
        Some(1),
        None,
    ]);
    let uint64_arr = UInt64Array::from(vec![
        Some(u64::MIN),
        Some(u64::MAX),
        Some(0),
        Some(9_223_372_036_854_775_808),
        Some(1),
        None,
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("int8", DataType::Int8, true),
        Field::new("int16", DataType::Int16, true),
        Field::new("int32", DataType::Int32, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("uint8", DataType::UInt8, true),
        Field::new("uint16", DataType::UInt16, true),
        Field::new("uint32", DataType::UInt32, true),
        Field::new("uint64", DataType::UInt64, true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(int8_arr),
            Arc::new(int16_arr),
            Arc::new(int32_arr),
            Arc::new(int64_arr),
            Arc::new(uint8_arr),
            Arc::new(uint16_arr),
            Arc::new(uint32_arr),
            Arc::new(uint64_arr),
        ],
    )
    .expect("failed to create arrow binary record batch");

    (record_batch, schema)
}

// All Float Types - comprehensive edge cases including infinity, subnormals
#[must_use]
pub fn get_arrow_float_record_batch() -> (RecordBatch, SchemaRef) {
    // Arrow Float Types with comprehensive edge cases
    // Note: NaN is excluded because NaN != NaN in equality comparisons
    let float32_arr = Float32Array::from(vec![
        Some(1.0_f32),           // normal positive
        Some(-1.0_f32),          // normal negative
        Some(0.0_f32),           // positive zero
        Some(-0.0_f32),          // negative zero
        Some(f32::MIN),          // minimum finite
        Some(f32::MAX),          // maximum finite
        Some(f32::MIN_POSITIVE), // smallest positive normal
        Some(f32::INFINITY),     // positive infinity
        Some(f32::NEG_INFINITY), // negative infinity
        Some(f32::EPSILON),      // machine epsilon
        Some(1.0e-38_f32),       // subnormal range
        None,                    // null value
    ]);
    let float64_arr = Float64Array::from(vec![
        Some(1.0_f64),           // normal positive
        Some(-1.0_f64),          // normal negative
        Some(0.0_f64),           // positive zero
        Some(-0.0_f64),          // negative zero
        Some(f64::MIN),          // minimum finite
        Some(f64::MAX),          // maximum finite
        Some(f64::MIN_POSITIVE), // smallest positive normal
        Some(f64::INFINITY),     // positive infinity
        Some(f64::NEG_INFINITY), // negative infinity
        Some(f64::EPSILON),      // machine epsilon
        Some(1.0e-308_f64),      // subnormal range
        None,                    // null value
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("float32", DataType::Float32, true),
        Field::new("float64", DataType::Float64, true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(float32_arr), Arc::new(float64_arr)],
    )
    .expect("failed to create arrow float record batch");

    (record_batch, schema)
}

// Float16 - half precision floating point
#[must_use]
pub fn get_arrow_float16_record_batch() -> (RecordBatch, SchemaRef) {
    use datafusion::arrow::datatypes::Float16Type;
    // Float16 with edge cases: normal values, zero, negative, max, min, infinity
    // Note: Using Float16Type::Native which is half::f16
    type F16 = <Float16Type as ArrowPrimitiveType>::Native;
    let float16_arr = Float16Array::from(vec![
        Some(F16::from_f32(1.0)),
        Some(F16::from_f32(-1.0)),
        Some(F16::from_f32(0.0)),
        Some(F16::NEG_ZERO),
        Some(F16::MIN_POSITIVE), // smallest positive subnormal
        Some(F16::MAX),
        Some(F16::MIN),
        Some(F16::INFINITY),
        Some(F16::NEG_INFINITY),
        None, // null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "float16",
        DataType::Float16,
        true,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(float16_arr)])
        .expect("failed to create arrow float16 record batch");

    (record_batch, schema)
}

// Utf8/LargeUtf8/Boolean - comprehensive edge cases
#[must_use]
pub fn get_arrow_utf8_record_batch() -> (RecordBatch, SchemaRef) {
    // Utf8, LargeUtf8, Boolean with comprehensive edge cases
    let string_arr = StringArray::from(vec![
        Some(""),                            // empty string
        Some("hello"),                       // simple ASCII
        Some("日本語テスト"),                // Japanese (CJK)
        Some("مرحبا بالعالم"),               // Arabic (RTL)
        Some("🦀🔥💯🎉"),                    // emoji
        Some("hello\nworld\ttab\rcarriage"), // escape sequences
        Some("   spaces   "),                // leading/trailing whitespace
        Some("Ḽơᶉëᶆ ȋṕṡűṁ"),                 // diacritics
        Some("\u{0000}null\u{0000}bytes"),   // embedded nulls (valid UTF-8)
        Some(&"x".repeat(10_000)),           // 10KB string
        None,                                // null value
    ]);
    let large_string_arr = LargeStringArray::from(vec![
        Some(""),                            // empty string
        Some("hello"),                       // simple ASCII
        Some("日本語テスト"),                // Japanese (CJK)
        Some("مرحبا بالعالم"),               // Arabic (RTL)
        Some("🦀🔥💯🎉"),                    // emoji
        Some("hello\nworld\ttab\rcarriage"), // escape sequences
        Some("   spaces   "),                // leading/trailing whitespace
        Some("Ḽơᶉëᶆ ȋṕṡűṁ"),                 // diacritics
        Some("\u{0000}null\u{0000}bytes"),   // embedded nulls (valid UTF-8)
        Some(&"y".repeat(10_000)),           // 10KB string
        None,                                // null value
    ]);
    let bool_arr = BooleanArray::from(vec![
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        Some(true),
        Some(false),
        None,
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("utf8", DataType::Utf8, true),
        Field::new("largeutf8", DataType::LargeUtf8, true),
        Field::new("boolean", DataType::Boolean, true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(string_arr),
            Arc::new(large_string_arr),
            Arc::new(bool_arr),
        ],
    )
    .expect("failed to create arrow utf8 record batch");

    (record_batch, schema)
}

// Utf8View - native string view type
#[must_use]
pub fn get_arrow_utf8_view_record_batch() -> (RecordBatch, SchemaRef) {
    // Utf8View with edge cases: empty string, unicode, long strings (>12 bytes triggers out-of-line storage)
    let utf8_view_arr = StringViewArray::from(vec![
        Some(""),                                                  // empty string
        Some("hello"),                                             // short string (inline)
        Some("this is a longer string that exceeds twelve bytes"), // long string (out-of-line)
        Some("日本語"),                                            // unicode (Japanese)
        Some("emoji: 🦀🔥"),                                       // emoji
        Some("\n\t\r"),                                            // escape characters
        None,                                                      // null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "utf8_view",
        DataType::Utf8View,
        true,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(utf8_view_arr)])
        .expect("failed to create arrow utf8 view record batch");

    (record_batch, schema)
}

// BinaryView - native binary view type
#[must_use]
pub fn get_arrow_binary_view_record_batch() -> (RecordBatch, SchemaRef) {
    // BinaryView with edge cases: empty, short (inline), long (out-of-line), null bytes
    let binary_view_arr = BinaryViewArray::from(vec![
        Some(b"".as_slice()),      // empty
        Some(b"short".as_slice()), // short (inline)
        Some(b"this binary data is longer than twelve bytes and goes out of line".as_slice()), // long
        Some(b"\x00\x01\x02\xff\xfe".as_slice()), // binary with null bytes
        None,                                     // null value
    ]);

    let schema = Arc::new(Schema::new(vec![Field::new(
        "binary_view",
        DataType::BinaryView,
        true,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(binary_view_arr)])
        .expect("failed to create arrow binary view record batch");

    (record_batch, schema)
}

// Time32, Time64 - comprehensive edge cases
#[expect(clippy::identity_op)]
#[expect(clippy::erasing_op)]
#[must_use]
pub fn get_arrow_time_record_batch() -> (RecordBatch, SchemaRef) {
    // Time32, Time64 Types with comprehensive edge cases
    // Edge cases: midnight (00:00:00), end of day (23:59:59.999...), noon, random times, null
    let time32_milli_array = Time32MillisecondArray::from(vec![
        Some(0),                                        // midnight
        Some((23 * 3600 + 59 * 60 + 59) * 1_000 + 999), // 23:59:59.999 (end of day)
        Some((12 * 3600 + 0 * 60 + 0) * 1_000),         // noon
        Some((10 * 3600 + 30 * 60 + 15) * 1_000 + 500), // 10:30:15.500
        Some(1),                                        // 1 millisecond after midnight
        None,                                           // null value
    ]);
    let time32_sec_array = Time32SecondArray::from(vec![
        Some(0),                        // midnight
        Some(23 * 3600 + 59 * 60 + 59), // 23:59:59 (end of day)
        Some(12 * 3600 + 0 * 60 + 0),   // noon
        Some(10 * 3600 + 30 * 60 + 15), // 10:30:15
        Some(1),                        // 1 second after midnight
        None,                           // null value
    ]);
    let time64_micro_array = Time64MicrosecondArray::from(vec![
        Some(0),                                                // midnight
        Some((23 * 3600 + 59 * 60 + 59) * 1_000_000 + 999_999), // 23:59:59.999999
        Some((12 * 3600 + 0 * 60 + 0) * 1_000_000),             // noon
        Some((10 * 3600 + 30 * 60 + 15) * 1_000_000 + 123_456), // 10:30:15.123456
        Some(1),                                                // 1 microsecond after midnight
        None,                                                   // null value
    ]);
    let time64_nano_array = Time64NanosecondArray::from(vec![
        Some(0),                                                        // midnight
        Some((23 * 3600 + 59 * 60 + 59) * 1_000_000_000 + 999_999_999), // 23:59:59.999999999
        Some((12 * 3600 + 0 * 60 + 0) * 1_000_000_000),                 // noon
        Some((10 * 3600 + 30 * 60 + 15) * 1_000_000_000 + 123_456_789), // 10:30:15.123456789
        Some(1), // 1 nanosecond after midnight
        None,    // null value
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time32_milli",
            DataType::Time32(TimeUnit::Millisecond),
            true,
        ),
        Field::new("time32_sec", DataType::Time32(TimeUnit::Second), true),
        Field::new(
            "time64_micro",
            DataType::Time64(TimeUnit::Microsecond),
            true,
        ),
        Field::new("time64_nano", DataType::Time64(TimeUnit::Nanosecond), true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(time32_milli_array),
            Arc::new(time32_sec_array),
            Arc::new(time64_micro_array),
            Arc::new(time64_nano_array),
        ],
    )
    .expect("failed to create arrow time record batch");

    (record_batch, schema)
}

// Timestamp (with/without TZ) - comprehensive edge cases
#[must_use]
pub fn get_arrow_timestamp_record_batch() -> (RecordBatch, SchemaRef) {
    // Timestamp Types with comprehensive edge cases
    // Edge cases: Unix epoch (0), near epoch, year 2000, recent dates, far future, negative (before epoch), null
    let timestamp_second_array = TimestampSecondArray::from(vec![
        Some(0),             // Unix epoch: 1970-01-01 00:00:00
        Some(1),             // 1 second after epoch
        Some(-1),            // 1 second before epoch (1969-12-31 23:59:59)
        Some(946_684_800),   // Y2K: 2000-01-01 00:00:00
        Some(1_680_000_000), // 2023-03-28
        Some(4_102_444_800), // Far future: 2100-01-01
        None,                // null value
    ]);
    let timestamp_milli_array = TimestampMillisecondArray::from(vec![
        Some(0_i64),                 // Unix epoch
        Some(1_i64),                 // 1 millisecond after epoch
        Some(-1_i64),                // 1 millisecond before epoch
        Some(946_684_800_000_i64),   // Y2K
        Some(1_680_000_000_123_i64), // 2023-03-28 with milliseconds
        Some(4_102_444_800_999_i64), // Far future with milliseconds
        None,                        // null value
    ])
    .with_timezone("+10:00".to_string());
    let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
        Some(0_i64),                     // Unix epoch
        Some(1_i64),                     // 1 microsecond after epoch
        Some(-1_i64),                    // 1 microsecond before epoch
        Some(946_684_800_000_000_i64),   // Y2K
        Some(1_680_000_000_123_456_i64), // 2023-03-28 with microseconds
        Some(4_102_444_800_999_999_i64), // Far future with microseconds
        None,                            // null value
    ])
    .with_timezone("-05:00".to_string()); // US Eastern timezone
    let timestamp_nano_array = TimestampNanosecondArray::from(vec![
        Some(0_i64),                         // Unix epoch
        Some(1_i64),                         // 1 nanosecond after epoch
        Some(-1_i64),                        // 1 nanosecond before epoch
        Some(946_684_800_000_000_000_i64),   // Y2K
        Some(1_680_000_000_123_456_789_i64), // 2023-03-28 with nanoseconds
        Some(4_102_444_800_999_999_999_i64), // Far future with nanoseconds
        None,                                // null value
    ])
    .with_timezone("UTC".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_milli",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+10:00".to_string()))),
            true,
        ),
        Field::new(
            "timestamp_micro",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("-05:00".to_string()))),
            true,
        ),
        Field::new(
            "timestamp_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC".to_string()))),
            true,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(timestamp_second_array),
            Arc::new(timestamp_milli_array),
            Arc::new(timestamp_micro_array),
            Arc::new(timestamp_nano_array),
        ],
    )
    .expect("failed to create arrow timestamp record batch");

    (record_batch, schema)
}

// Timestamp without timezone - comprehensive edge cases
#[must_use]
pub fn get_arrow_timestamp_record_batch_without_timezone() -> (RecordBatch, SchemaRef) {
    // Timestamp Types without timezone - edge cases: epoch, negative (pre-1970), far future, null
    let timestamp_second_array = TimestampSecondArray::from(vec![
        Some(0),             // Unix epoch
        Some(-86_400),       // 1 day before epoch
        Some(946_684_800),   // Y2K
        Some(4_102_444_800), // 2100-01-01
        Some(1_680_000_000), // recent timestamp
        None,                // null
    ]);
    let timestamp_milli_array = TimestampMillisecondArray::from(vec![
        Some(0_i64),
        Some(-86_400_000_i64),
        Some(946_684_800_000_i64),
        Some(4_102_444_800_000_i64),
        Some(1_680_000_000_999_i64),
        None,
    ]);
    let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
        Some(0_i64),
        Some(-86_400_000_000_i64),
        Some(946_684_800_000_000_i64),
        Some(4_102_444_800_000_000_i64),
        Some(1_680_000_000_999_999_i64),
        None,
    ]);
    let timestamp_nano_array = TimestampNanosecondArray::from(vec![
        Some(0_i64),
        Some(-86_400_000_000_000_i64),
        Some(946_684_800_000_000_000_i64),
        Some(4_102_444_800_000_000_000_i64),
        Some(1_680_000_000_999_999_999_i64),
        None,
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new(
            "timestamp_milli",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "timestamp_micro",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "timestamp_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(timestamp_second_array),
            Arc::new(timestamp_milli_array),
            Arc::new(timestamp_micro_array),
            Arc::new(timestamp_nano_array),
        ],
    )
    .expect("failed to create arrow timestamp record batch");

    (record_batch, schema)
}

// Date32, Date64 - comprehensive edge cases
#[must_use]
pub fn get_arrow_date_record_batch() -> (RecordBatch, SchemaRef) {
    // Edge cases: epoch, leap years, Y2K, far past, far future, null
    let date32_array = Date32Array::from(vec![
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default(),
        )), // Unix epoch
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 2, 29).unwrap_or_default(),
        )), // Y2K leap day
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap_or_default(),
        )), // Recent leap day
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(1582, 10, 15).unwrap_or_default(),
        )), // Gregorian calendar start
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(9999, 12, 31).unwrap_or_default(),
        )), // Max year
        Some(Date32Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 12, 31).unwrap_or_default(),
        )), // End of year
        None, // null value
    ]);
    let date64_array = Date64Array::from(vec![
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(1970, 1, 1).unwrap_or_default(),
        )), // Unix epoch
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2000, 2, 29).unwrap_or_default(),
        )), // Y2K leap day
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap_or_default(),
        )), // Recent leap day
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(1582, 10, 15).unwrap_or_default(),
        )), // Gregorian calendar start
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(9999, 12, 31).unwrap_or_default(),
        )), // Max year
        Some(Date64Type::from_naive_date(
            NaiveDate::from_ymd_opt(2023, 12, 31).unwrap_or_default(),
        )), // End of year
        None, // null value
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("date32", DataType::Date32, true),
        Field::new("date64", DataType::Date64, true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(date32_array), Arc::new(date64_array)],
    )
    .expect("failed to create arrow date record batch");

    (record_batch, schema)
}

// struct - comprehensive edge cases
#[must_use]
pub fn get_arrow_struct_record_batch() -> (RecordBatch, SchemaRef) {
    // Edge cases: normal values, null fields, boundary int values, all fields null
    let schema = Arc::new(Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(
            vec![
                Field::new("b", DataType::Boolean, true),
                Field::new("c", DataType::Int32, true),
            ]
            .into(),
        ),
        true,
    )]));

    let mut struct_builder = StructBuilder::new(
        vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("c", DataType::Int32, true),
        ],
        vec![
            Box::new(BooleanBuilder::new()),
            Box::new(Int32Builder::new()),
        ],
    );

    // Row 1: normal values
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_value(false);
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_value(30);
    struct_builder.append(true);

    // Row 2: null boolean, valid int
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_null();
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_value(100);
    struct_builder.append(true);

    // Row 3: valid boolean, null int
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_value(true);
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_null();
    struct_builder.append(true);

    // Row 4: boundary int values
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_value(true);
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_value(i32::MAX);
    struct_builder.append(true);

    // Row 5: negative boundary
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_value(false);
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_value(i32::MIN);
    struct_builder.append(true);

    // Row 6: all fields null
    struct_builder
        .field_builder::<BooleanBuilder>(0)
        .expect("should return field builder")
        .append_null();
    struct_builder
        .field_builder::<Int32Builder>(1)
        .expect("should return field builder")
        .append_null();
    struct_builder.append(true);

    // Row 7: null struct itself (disabled as not properly supported by duckdb/postgres)
    // Keeping this in the test may break compatibility
    // struct_builder.append(false);

    let struct_array = struct_builder.finish();

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(struct_array)])
        .expect("failed to create arrow struct record batch");

    (record_batch, schema)
}

// Decimal128/Decimal256 - comprehensive edge cases
#[must_use]
pub fn get_arrow_decimal_record_batch() -> (RecordBatch, SchemaRef) {
    // Edge cases: max precision values, zero, negative, min/max, near-overflow, null
    let decimal128_array = Decimal128Array::from(vec![
        Some(i128::from(0)),                  // zero
        Some(i128::from(1)),                  // smallest positive unit
        Some(i128::from(-1)),                 // smallest negative unit
        Some(i128::from(9_999_999_999_i64)),  // large positive
        Some(i128::from(-9_999_999_999_i64)), // large negative
        Some(i128::MAX / 1_000_000),          // near max (scaled down for precision)
        Some(i128::MIN / 1_000_000),          // near min (scaled down for precision)
        None,                                 // null value
    ]);
    let decimal256_array = Decimal256Array::from(vec![
        Some(i256::from(0)),                          // zero
        Some(i256::from(1)),                          // smallest positive unit
        Some(i256::from(-1)),                         // smallest negative unit
        Some(i256::from(9_999_999_999_999_999_i64)),  // large positive
        Some(i256::from(-9_999_999_999_999_999_i64)), // large negative
        Some(i256::from(i64::MAX)),                   // i64 max as i256
        Some(i256::from(i64::MIN)),                   // i64 min as i256
        None,                                         // null value
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("decimal128", DataType::Decimal128(38, 10), true),
        Field::new("decimal256", DataType::Decimal256(76, 10), true),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(decimal128_array), Arc::new(decimal256_array)],
    )
    .expect("failed to create arrow decimal record batch");

    (record_batch, schema)
}

// Duration
#[must_use]
pub fn get_arrow_duration_record_batch() -> (RecordBatch, SchemaRef) {
    let duration_nano_array = DurationNanosecondArray::from(vec![1, 2, 3]);
    let duration_micro_array = DurationMicrosecondArray::from(vec![1, 2, 3]);
    let duration_milli_array = DurationMillisecondArray::from(vec![1, 2, 3]);
    let duration_sec_array = DurationSecondArray::from(vec![1, 2, 3]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "duration_nano",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        ),
        Field::new(
            "duration_micro",
            DataType::Duration(TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "duration_milli",
            DataType::Duration(TimeUnit::Millisecond),
            false,
        ),
        Field::new("duration_sec", DataType::Duration(TimeUnit::Second), false),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(duration_nano_array),
            Arc::new(duration_micro_array),
            Arc::new(duration_milli_array),
            Arc::new(duration_sec_array),
        ],
    )
    .expect("failed to create arrow duration record batch");

    (record_batch, schema)
}

// Interval
#[must_use]
pub fn get_arrow_interval_record_batch() -> (RecordBatch, SchemaRef) {
    let interval_daytime_array = IntervalDayTimeArray::from(vec![
        IntervalDayTime::new(1, 1000),
        IntervalDayTime::new(33, 0),
        IntervalDayTime::new(0, 12 * 60 * 60 * 1000),
    ]);
    let interval_monthday_nano_array = IntervalMonthDayNanoArray::from(vec![
        IntervalMonthDayNano::new(1, 2, 1000),
        IntervalMonthDayNano::new(12, 1, 0),
        IntervalMonthDayNano::new(0, 0, 12 * 1000 * 1000),
    ]);
    let interval_yearmonth_array = IntervalYearMonthArray::from(vec![2, 25, -1]);

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "interval_daytime",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
        Field::new(
            "interval_monthday_nano",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new(
            "interval_yearmonth",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(interval_daytime_array),
            Arc::new(interval_monthday_nano_array),
            Arc::new(interval_yearmonth_array),
        ],
    )
    .expect("failed to create arrow interval record batch");

    (record_batch, schema)
}

//  List/FixedSizeList/LargeList - comprehensive edge cases
#[must_use]
pub fn get_arrow_list_record_batch() -> (RecordBatch, SchemaRef) {
    // Edge cases: empty list, null elements, varying sizes, single element, many elements, null list
    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.append_value(Vec::<Option<i32>>::new()); // empty list
    list_builder.append_value([Some(1), None, Some(3)]); // list with null element
    list_builder.append_value([Some(42)]); // single element
    list_builder.append_value([Some(i32::MIN), Some(0), Some(i32::MAX)]); // boundary values
    list_builder.append_value([Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]); // multiple elements
    list_builder.append_null(); // null list
    let list_array = list_builder.finish();

    let mut large_list_builder = LargeListBuilder::new(Int32Builder::new());
    large_list_builder.append_value(Vec::<Option<i32>>::new()); // empty list
    large_list_builder.append_value([Some(1), None, Some(3)]); // list with null element
    large_list_builder.append_value([Some(42)]); // single element
    large_list_builder.append_value([Some(i32::MIN), Some(0), Some(i32::MAX)]); // boundary values
    large_list_builder.append_value([Some(-1), Some(-2), Some(-3), Some(-4), Some(-5)]); // multiple elements
    large_list_builder.append_null(); // null list
    let large_list_array = large_list_builder.finish();

    // FixedSizeList with 3 elements - edge cases: normal, nulls, boundaries, null list
    let mut fixed_size_list_builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);
    // Normal values
    fixed_size_list_builder.values().append_value(1);
    fixed_size_list_builder.values().append_value(2);
    fixed_size_list_builder.values().append_value(3);
    fixed_size_list_builder.append(true);
    // All nulls in list
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.append(true);
    // Boundary values
    fixed_size_list_builder.values().append_value(i32::MIN);
    fixed_size_list_builder.values().append_value(0);
    fixed_size_list_builder.values().append_value(i32::MAX);
    fixed_size_list_builder.append(true);
    // Mixed null and values
    fixed_size_list_builder.values().append_value(100);
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.values().append_value(200);
    fixed_size_list_builder.append(true);
    // Normal again
    fixed_size_list_builder.values().append_value(7);
    fixed_size_list_builder.values().append_value(8);
    fixed_size_list_builder.values().append_value(9);
    fixed_size_list_builder.append(true);
    // Null list
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.values().append_null();
    fixed_size_list_builder.append(false);
    let fixed_size_list_array = fixed_size_list_builder.finish();

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "list",
            DataType::List(Field::new("item", DataType::Int32, true).into()),
            true,
        ),
        Field::new(
            "large_list",
            DataType::LargeList(Field::new("item", DataType::Int32, true).into()),
            true,
        ),
        Field::new(
            "fixed_size_list",
            DataType::FixedSizeList(Field::new("item", DataType::Int32, true).into(), 3),
            true,
        ),
    ]));

    let record_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(list_array),
            Arc::new(large_list_array),
            Arc::new(fixed_size_list_array),
        ],
    )
    .expect("failed to create arrow list record batch");

    (record_batch, schema)
}

#[must_use]
pub fn get_arrow_list_of_structs_record_batch() -> (RecordBatch, SchemaRef) {
    let input_batch_json_data = r#"
            {"labels": [{"id": 1}, {"id": 2}]}
            {"labels": null}
            {"labels": null}
            {"labels": null}
            {"labels": [{"id": 3}, {"id": null}]}
            {"labels": [{"id": 4,"name":"test"}, {"id": null,"name":null}]}
            {"labels": null}
            "#;

    let record_batch = parse_json_to_batch(
        input_batch_json_data,
        Arc::new(Schema::new(vec![Field::new(
            "labels",
            DataType::List(Arc::new(Field::new(
                "struct",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, true),
                        Field::new("name", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )])),
    );

    let schema = record_batch.schema();

    (record_batch, schema)
}

#[must_use]
pub fn get_arrow_list_of_lists_record_batch() -> (RecordBatch, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        DataType::List(
            Field::new(
                "item",
                DataType::List(Field::new("item", DataType::Int32, true).into()),
                true,
            )
            .into(),
        ),
        true,
    )]));

    let mut list_builder = ListBuilder::new(ListBuilder::new(Int32Builder::new()));
    // Append first list of items
    {
        let list_item_builder = list_builder.values();
        list_item_builder.append_value([Some(1), Some(2)]);
        // Append NULL list item
        list_item_builder.append_null();
        list_item_builder.append_value([Some(3), None, Some(5)]);
        list_builder.append(true);
    }
    // Append NULL list
    list_builder.append_null();

    let list_array = list_builder.finish();

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_array) as ArrayRef])
            .expect("Failed to create RecordBatch");

    (record_batch, schema)
}

#[must_use]
pub fn get_arrow_list_of_fixed_size_lists_record_batch() -> (RecordBatch, Arc<Schema>) {
    // Define FixedSizeList field schema
    let fixed_size_list_field = Field::new(
        "item", // Match the internal field name of FixedSizeListBuilder
        DataType::FixedSizeList(Field::new("item", DataType::Int32, true).into(), 3),
        true,
    );

    // Define List<FixedSizeList> schema
    let schema = Arc::new(Schema::new(vec![Field::new(
        "list",
        DataType::List(fixed_size_list_field.into()),
        true,
    )]));

    let mut list_builder = ListBuilder::new(FixedSizeListBuilder::new(Int32Builder::new(), 3));

    // Append first list of FixedSizeList items
    {
        let fixed_size_list_builder = list_builder.values();
        fixed_size_list_builder.values().append_value(1);
        fixed_size_list_builder.values().append_value(2);
        fixed_size_list_builder.values().append_value(3);
        fixed_size_list_builder.append(true);

        // Append NULL fixed-size list item
        fixed_size_list_builder.values().append_null();
        fixed_size_list_builder.values().append_null();
        fixed_size_list_builder.values().append_null();
        fixed_size_list_builder.append(false);

        fixed_size_list_builder.values().append_value(4);
        fixed_size_list_builder.values().append_value(5);
        fixed_size_list_builder.values().append_value(6);
        fixed_size_list_builder.append(true);

        list_builder.append(true);
    }

    // Append NULL list
    list_builder.append_null();

    // Append third list of FixedSizeList items
    {
        let fixed_size_list_builder = list_builder.values();
        fixed_size_list_builder.values().append_value(10);
        fixed_size_list_builder.values().append_value(11);
        fixed_size_list_builder.values().append_value(12);
        fixed_size_list_builder.append(true);

        list_builder.append(true);
    }

    let list_array = list_builder.finish();

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(list_array) as ArrayRef])
            .expect("Failed to create RecordBatch");

    (record_batch, schema)
}

// Null
#[must_use]
pub fn get_arrow_null_record_batch() -> (RecordBatch, SchemaRef) {
    let null_arr = Int8Array::from(vec![Some(1), None, Some(3)]);
    let schema = Arc::new(Schema::new(vec![Field::new("int8", DataType::Int8, true)]));
    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(null_arr)])
        .expect("failed to create arrow null record batch");
    (record_batch, schema)
}

// BYTEA_ARRAY
#[must_use]
pub fn get_arrow_bytea_array_record_batch() -> (RecordBatch, SchemaRef) {
    let mut bytea_array_builder = ListBuilder::new(BinaryBuilder::new());
    bytea_array_builder.append_value([Some(b"1"), Some(b"2"), Some(b"3")]);
    bytea_array_builder.append_value([Some(b"4")]);
    bytea_array_builder.append_value([Some(b"6")]);
    let bytea_array_builder = bytea_array_builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "bytea_array",
        DataType::List(Field::new("item", DataType::Binary, true).into()),
        false,
    )]));

    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(bytea_array_builder)])
            .expect("failed to create arrow bytea array record batch");

    (record_batch, schema)
}

// DICTIONARY_ARRAY - comprehensive edge cases
#[must_use]
pub fn get_arrow_dictionary_array_record_batch() -> (RecordBatch, SchemaRef) {
    // Edge cases: duplicates (same dictionary key), null, empty string, unicode, long string
    let mut builder = StringDictionaryBuilder::<Int8Type>::new();
    builder.append_value("happy"); // normal value
    builder.append_value("sad"); // normal value
    builder.append_value(""); // empty string
    builder.append_value("happy"); // duplicate value (reuses dictionary key)
    builder.append_null(); // null value
    builder.append_value("日本語"); // unicode (Japanese)
    builder.append_value("happy"); // another duplicate
    builder.append_value("🎉🚀"); // emoji
    builder.append_value("a".repeat(100).as_str()); // longer string
    let array: DictionaryArray<Int8Type> = builder.finish();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "mood_status",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    )]));

    let record_batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)])
        .expect("failed to create arrow dictionary array record batch");

    (record_batch, schema)
}

#[must_use]
pub fn get_arrow_map_record_batch() -> (RecordBatch, SchemaRef) {
    let keys = vec!["a", "b", "c", "d", "e", "f", "g", "h"];
    let values_data = UInt32Array::from(vec![
        Some(0u32),
        None,
        Some(20),
        Some(30),
        None,
        Some(50),
        Some(60),
        Some(70),
    ]);
    // Construct a buffer for value offsets, for the nested array:
    //  [[a, b, c], [d, e, f], [g, h]]
    let entry_offsets = [0, 3, 6, 8];
    let map_array =
        MapArray::new_from_strings(keys.clone().into_iter(), &values_data, &entry_offsets)
            .expect("Failed to create MapArray");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "map_array",
        map_array.data_type().clone(),
        true,
    )]));
    let rb = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(map_array)])
        .expect("failed to create arrow Map array record batch");
    (rb, schema)
}

#[must_use]
pub fn parse_json_to_batch(json_data: &str, schema: SchemaRef) -> RecordBatch {
    let reader = arrow_json::ReaderBuilder::new(schema)
        .build(std::io::Cursor::new(json_data))
        .expect("Failed to create JSON reader");

    reader
        .into_iter()
        .next()
        .expect("Expected a record batch")
        .expect("Failed to read record batch")
}
