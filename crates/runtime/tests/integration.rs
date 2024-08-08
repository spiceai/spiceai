/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{sync::Arc, time::Duration};

use arrow::array::RecordBatch;
use arrow::{
    array::*,
    datatypes::{i256, DataType, Date32Type, Date64Type, Field, Fields, Schema, TimeUnit},
};
use chrono::NaiveDate;
use datafusion::{
    assert_batches_eq, execution::context::SessionContext,
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
};
use futures::Future;
use runtime::{datafusion::DataFusion, Runtime};
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

mod catalog;
mod docker;
mod federation;
mod graphql;
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod postgres;
mod refresh_retry;
mod refresh_sql;
mod results_cache;
mod tls;

#[cfg(feature = "odbc")]
mod odbc;

/// Gets a test `DataFusion` to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 3, by default its the number of CPU cores available.
fn get_test_datafusion() -> Arc<DataFusion> {
    let mut df = DataFusion::new();

    // Set the target partitions to 3 to make RepartitionExec show consistent partitioning across machines with different CPU counts.
    let mut new_state = df.ctx.state();
    new_state
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 3;
    let new_ctx = SessionContext::new_with_state(new_state);

    // Replace the old context with the modified one
    df.ctx = new_ctx.into();
    Arc::new(df)
}

fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new(
            "runtime=TRACE,datafusion-federation=TRACE,datafusion-federation-sql=TRACE",
        ),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

async fn get_tpch_lineitem() -> Result<Vec<RecordBatch>, anyhow::Error> {
    let lineitem_parquet_bytes =
        reqwest::get("https://public-data.spiceai.org/tpch_lineitem.parquet")
            .await?
            .bytes()
            .await?;

    let parquet_reader =
        ParquetRecordBatchReaderBuilder::try_new(lineitem_parquet_bytes)?.build()?;

    Ok(parquet_reader.collect::<Result<Vec<_>, arrow::error::ArrowError>>()?)
}

type ValidateFn = dyn FnOnce(Vec<RecordBatch>);

async fn run_query_and_check_results<F>(
    rt: &mut Runtime,
    query: &str,
    expected_plan: &[&str],
    validate_result: Option<F>,
) -> Result<(), String>
where
    F: FnOnce(Vec<RecordBatch>),
{
    // Check the plan
    let plan_results = rt
        .datafusion()
        .ctx
        .sql(&format!("EXPLAIN {query}"))
        .await
        .map_err(|e| format!("query `{query}` to plan: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("query `{query}` to results: {e}"))?;

    assert_batches_eq!(expected_plan, &plan_results);

    // Check the result
    if let Some(validate_result) = validate_result {
        let result_batches = rt
            .datafusion()
            .ctx
            .sql(query)
            .await
            .map_err(|e| format!("query `{query}` to plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query `{query}` to results: {e}"))?;

        validate_result(result_batches);
    }

    Ok(())
}

async fn wait_until_true<F, Fut>(max_wait: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait {
        if f().await {
            return true;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    false
}

fn container_registry() -> String {
    std::env::var("CONTAINER_REGISTRY")
        .unwrap_or_else(|_| "public.ecr.aws/docker/library/".to_string())
}

// Helper functions to create arrow record batches of different types
fn get_arrow_binary_record_batch() -> RecordBatch {
    // Binary/LargeBinary/FixedSizeBinary Array
    let values: Vec<&[u8]> = vec![b"one", b"two", b""];
    let binary_array = BinaryArray::from_vec(values.clone());
    let large_binary_array = LargeBinaryArray::from_vec(values);
    let input_arg = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let fixed_size_binary_array =
        FixedSizeBinaryArray::try_from_iter(input_arg.into_iter()).unwrap();

    let schema = Schema::new(vec![
        Field::new("binary", DataType::Binary, false),
        Field::new("large_binary", DataType::LargeBinary, false),
        Field::new("fixed_size_binary", DataType::FixedSizeBinary(2), false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(binary_array),
            Arc::new(large_binary_array),
            Arc::new(fixed_size_binary_array),
        ],
    )
    .expect("Failed to created arrow binary record batch")
}

fn get_arrow_int_recordbatch() -> RecordBatch {
    // Arrow Integer Types
    let int8_arr = Int8Array::from(vec![1, 2, 3]);
    let int16_arr = Int16Array::from(vec![1, 2, 3]);
    let int32_arr = Int32Array::from(vec![1, 2, 3]);
    let int64_arr = Int64Array::from(vec![1, 2, 3]);
    let uint8_arr = UInt8Array::from(vec![1, 2, 3]);
    let uint16_arr = UInt16Array::from(vec![1, 2, 3]);
    let uint32_arr = UInt32Array::from(vec![1, 2, 3]);
    let uint64_arr = UInt64Array::from(vec![1, 2, 3]);

    let schema = Schema::new(vec![
        Field::new("int8", DataType::Int8, false),
        Field::new("int16", DataType::Int16, false),
        Field::new("int32", DataType::Int32, false),
        Field::new("int64", DataType::Int64, false),
        Field::new("uint8", DataType::UInt8, false),
        Field::new("uint16", DataType::UInt16, false),
        Field::new("uint32", DataType::UInt32, false),
        Field::new("uint64", DataType::UInt64, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
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
    .expect("Failed to created arrow int record batch")
}

fn get_arrow_float_record_batch() -> RecordBatch {
    // Arrow Float Types
    let float32_arr = Float32Array::from(vec![1.0, 2.0, 3.0]);
    let float64_arr = Float64Array::from(vec![1.0, 2.0, 3.0]);

    let schema = Schema::new(vec![
        Field::new("float32", DataType::Float32, false),
        Field::new("float64", DataType::Float64, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(float32_arr), Arc::new(float64_arr)],
    )
    .expect("Failed to created arrow float record batch")
}

fn get_arrow_utf8_record_batch() -> RecordBatch {
    // Utf8, LargeUtf8 Types
    let string_arr = StringArray::from(vec!["foo", "bar", "baz"]);
    let large_string_arr = LargeStringArray::from(vec!["foo", "bar", "baz"]);
    let bool_arr: BooleanArray = vec![true, true, false].into();

    let schema = Schema::new(vec![
        Field::new("utf8", DataType::Utf8, false),
        Field::new("largeutf8", DataType::LargeUtf8, false),
        Field::new("boolean", DataType::Boolean, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(string_arr),
            Arc::new(large_string_arr),
            Arc::new(bool_arr),
        ],
    )
    .expect("Failed to created arrow utf8 record batch")
}

fn get_arrow_time_record_batch() -> RecordBatch {
    // Time32, Time64 Types
    let time32_milli_array: Time32MillisecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000,
        (11 * 3600 + 0 * 60 + 15) * 1_000,
    ]
    .into();
    let time32_sec_array: Time32SecondArray = vec![
        (10 * 3600 + 30 * 60),
        (10 * 3600 + 45 * 60 + 15),
        (11 * 3600 + 00 * 60 + 15),
    ]
    .into();
    let time64_micro_array: Time64MicrosecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000_000,
        (11 * 3600 + 0 * 60 + 15) * 1_000_000,
    ]
    .into();
    let time64_nano_array: Time64NanosecondArray = vec![
        (10 * 3600 + 30 * 60) * 1_000_000_000,
        (10 * 3600 + 45 * 60 + 15) * 1_000_000_000,
        (11 * 3600 + 00 * 60 + 15) * 1_000_000_000,
    ]
    .into();

    let schema = Schema::new(vec![
        Field::new(
            "time32_milli",
            DataType::Time32(TimeUnit::Millisecond),
            false,
        ),
        Field::new("time32_sec", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "time64_micro",
            DataType::Time64(TimeUnit::Microsecond),
            false,
        ),
        Field::new("time64_nano", DataType::Time64(TimeUnit::Nanosecond), false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(time32_milli_array),
            Arc::new(time32_sec_array),
            Arc::new(time64_micro_array),
            Arc::new(time64_nano_array),
        ],
    )
    .expect("Failed to created arrow time record batch")
}

fn get_arrow_timestamp_record_batch() -> RecordBatch {
    // Timestamp Types
    let timestamp_second_array =
        TimestampSecondArray::from(vec![1_680_000_000, 1_680_040_000, 1_680_080_000])
            .with_timezone("+10:00".to_string());
    let timestamp_milli_array = TimestampMillisecondArray::from(vec![
        1_680_000_000_000,
        1_680_040_000_000,
        1_680_080_000_000,
    ])
    .with_timezone("+10:00".to_string());
    let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
        1_680_000_000_000_000,
        1_680_040_000_000_000,
        1_680_080_000_000_000,
    ])
    .with_timezone("+10:00".to_string());
    let timestamp_nano_array = TimestampNanosecondArray::from(vec![
        1_680_000_000_000_000_000,
        1_680_040_000_000_000_000,
        1_680_080_000_000_000_000,
    ])
    .with_timezone("+10:00".to_string());

    let schema = Schema::new(vec![
        Field::new(
            "timestamp_second",
            DataType::Timestamp(TimeUnit::Second, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
        Field::new(
            "timestamp_milli",
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
        Field::new(
            "timestamp_micro",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
        Field::new(
            "timestamp_nano",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+10:00".to_string()))),
            false,
        ),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(timestamp_second_array),
            Arc::new(timestamp_milli_array),
            Arc::new(timestamp_micro_array),
            Arc::new(timestamp_nano_array),
        ],
    )
    .expect("Failed to created arrow timestamp record batch")
}

// Date32, Date64
fn get_arrow_date_record_batch() -> RecordBatch {
    let date32_array = Date32Array::from(vec![
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2015, 3, 14).unwrap_or_default()),
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2016, 1, 12).unwrap_or_default()),
        Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2017, 9, 17).unwrap_or_default()),
    ]);
    let date64_array = Date64Array::from(vec![
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2015, 3, 14).unwrap_or_default()),
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2016, 1, 12).unwrap_or_default()),
        Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2017, 9, 17).unwrap_or_default()),
    ]);

    println!("{:?}", date32_array.value(0));

    let schema = Schema::new(vec![
        Field::new("date32", DataType::Date32, false),
        Field::new("date64", DataType::Date64, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(date32_array), Arc::new(date64_array)],
    )
    .expect("Failed to created arrow date record batch")
}

fn get_arrow_struct_record_batch() -> RecordBatch {
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]);

    let schema = Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(Fields::from(vec![
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ])),
        false,
    )]);

    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])
        .expect("Failed to created arrow struct record batch")
}

fn get_arrow_decimal_record_batch() -> RecordBatch {
    let decimal128_array =
        Decimal128Array::from(vec![i128::from(123), i128::from(222), i128::from(321)]);
    let decimal256_array =
        Decimal256Array::from(vec![i256::from(123), i256::from(222), i256::from(321)]);

    let schema = Schema::new(vec![
        Field::new("decimal128", DataType::Decimal128(38, 10), false),
        Field::new("decimal256", DataType::Decimal256(76, 10), false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(decimal128_array), Arc::new(decimal256_array)],
    )
    .expect("Failed to created arrow decimal record batch")
}

// TODO
// Duration, Interval
// List/FixedSizeList/LargeList
