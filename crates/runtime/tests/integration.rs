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
    datatypes::{DataType, Field, Schema, TimeUnit},
};
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

// WIP: Add all arrow types covered in acceleration beta hardening criteria
// fn get_arrow_int_recordbatch() {
//     // Arrow Integer Types
//     let int8_arr = Int8Array::from(vec![1, 2, 3]);
//     let int16_arr = Int16Array::from(vec![1, 2, 3]);
//     let int32_arr = Int32Array::from(vec![1, 2, 3]);
//     let int64_arr = Int64Array::from(vec![1, 2, 3]);
//     let uint8_arr = UInt8Array::from(vec![1, 2, 3]);
//     let uint16_arr = UInt16Array::from(vec![1, 2, 3]);
//     let uint32_arr = UInt32Array::from(vec![1, 2, 3]);
//     let uint64_arr = UInt64Array::from(vec![1, 2, 3]);
// }

// fn get_arrow_float_record_batch() {
//     // Arrow Float Types
//     let float32_arr = Float32Array::from(vec![1.0, 2.0, 3.0]);
//     let float64_arr = Float64Array::from(vec![1.0, 2.0, 3.0]);
// }

// fn get_arrow_utf8_record_batch() {
//     // Utf8, LargeUtf8 Types
//     let string_arr = StringArray::from(vec!["foo", "bar", "baz"]);
//     let large_string_arr = LargeStringArray::from(vec!["foo", "bar", "baz"]);
//     let bool_arr: BooleanArray = vec![true, true, false].into();
// }

// fn get_arrow_time_record_batch() {
//     // Time32, Time64 Types
//     let time32_milli_array: Time32MillisecondArray = vec![
//         (10 * 3600 + 30 * 60) * 1_000,
//         (10 * 3600 + 45 * 60 + 15) * 1_000,
//         (11 * 3600 + 0 * 60 + 15) * 1_000,
//     ]
//     .into();
//     let time32_sec_array: Time32SecondArray = vec![
//         (10 * 3600 + 30 * 60),
//         (10 * 3600 + 45 * 60 + 15),
//         (11 * 3600 + 00 * 60 + 15),
//     ]
//     .into();
//     let time64_micro_array: Time64MicrosecondArray = vec![
//         (10 * 3600 + 30 * 60) * 1_000_000,
//         (10 * 3600 + 45 * 60 + 15) * 1_000_000,
//         (11 * 3600 + 0 * 60 + 15) * 1_000_000,
//     ]
//     .into();
//     let time64_nano_array: Time64NanosecondArray = vec![
//         (10 * 3600 + 30 * 60) * 1_000_000_000,
//         (10 * 3600 + 45 * 60 + 15) * 1_000_000_000,
//         (11 * 3600 + 00 * 60 + 15) * 1_000_000_000,
//     ]
//     .into();
// }

// fn get_arrow_timestamp_record_batch() {
//     // Timestamp Types
//     let timestamp_second_array =
//         TimestampSecondArray::from(vec![1_680_000_000, 1_680_040_000, 1_680_080_000])
//             .with_timezone("+10:00".to_string());
//     let timestamp_milli_array = TimestampMillisecondArray::from(vec![
//         1_680_000_000_000,
//         1_680_040_000_000,
//         1_680_080_000_000,
//     ])
//     .with_timezone("+10:00".to_string());
//     let timestamp_micro_array = TimestampMicrosecondArray::from(vec![
//         1_680_000_000_000_000,
//         1_680_040_000_000_000,
//         1_680_080_000_000_000,
//     ])
//     .with_timezone("+10:00".to_string());
//     let timestamp_nano_array = TimestampMicrosecondArray::from(vec![
//         1_680_000_000_000_000_000,
//         1_680_040_000_000_000_000,
//         1_680_080_000_000_000_000,
//     ])
//     .with_timezone("+10:00".to_string());
// }
