/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::component::dataset::TimeFormat;
use crate::dataupdate::StreamingDataUpdate;
use arrow::array::{
    Array, Date32Array, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::TimeUnit;
use arrow_schema::{DataType, Field, SchemaRef};
use async_stream::stream;
use chrono::DateTime;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;

const MS_IN_A_DAY: i64 = 24 * 60 * 60 * 1000;

macro_rules! max_ts_macro {
    ($ty:ty, $max_ts:expr, $array:expr, $value:expr) => {{
        match $array.as_any().downcast_ref::<$ty>() {
            Some(arr) => {
                let batch_max = (0..arr.len())
                    .filter(|&i| !arr.is_null(i))
                    .filter_map(|i| $value(arr, i))
                    .max();
                match batch_max {
                    Some(bm) => Some($max_ts.map_or(bm, |cur| cur.max(bm))),
                    None => $max_ts,
                }
            }
            None => $max_ts,
        }
    }};
}

/// Extracts the maximum timestamp from a stream of record batches.
///
/// The extraction uses `time_column` and `time_format` to interpret timestamps
/// from various data types (timestamps, strings, integers, floats, dates).
///
/// # Parameters
/// - `time_column`: Name of the column containing timestamp data
/// - `time_format`: Format specification for interpreting the timestamp values
/// - `source_name`: Used for logging warnings
///
/// # Returns
/// - An updated data stream
/// - An `Option<Arc<Mutex<Option<i64>>>>` with the following semantics:
///     - `None`: Did not attempt to find a max timestamp (e.g., no `time_column`
///       provided or column does not exist in schema)
///     - `Some(Arc<Mutex<Option<i64>>>)`: Will be populated after the stream is consumed:
///         - `Some(i64)`: The maximum timestamp found in the stream (in milliseconds)
///         - `None`: Attempted extraction but no valid timestamps found
///             - If batches were not empty, a warning is logged
pub async fn with_find_max_timestamp_in_stream(
    data_update: StreamingDataUpdate,
    schema: SchemaRef,
    time_column: Option<String>,
    time_format: Option<TimeFormat>,
    source_name: String,
) -> (StreamingDataUpdate, Option<Arc<Mutex<Option<i64>>>>) {
    let Some(time_column) = time_column else {
        return (data_update, None);
    };

    let time_format = time_format.unwrap_or_default();
    let Some(field) = schema
        .column_with_name(&time_column)
        .map(|(_, f)| f)
        .cloned()
    else {
        tracing::warn!(
            "Failed to extract max_timestamp after refresh for {}: column {} not found in schema.",
            source_name,
            time_column,
        );
        return (data_update, None);
    };

    let max_ts = Arc::new(Mutex::new(None::<i64>));
    let max_ts_clone = Arc::clone(&max_ts);

    let out_stream = find_max_timestamp_in_stream_inner(
        data_update.data,
        time_column,
        time_format,
        field,
        max_ts_clone,
        Arc::clone(&schema),
        source_name,
    );

    let new_data_update = StreamingDataUpdate {
        data: out_stream,
        update_type: data_update.update_type,
    };

    (new_data_update, Some(max_ts))
}

fn find_max_timestamp_in_stream_inner(
    mut input_stream: SendableRecordBatchStream,
    time_column: String,
    time_format: TimeFormat,
    field: Field,
    max_ts_clone: Arc<Mutex<Option<i64>>>,
    schema: SchemaRef,
    source_name: String,
) -> SendableRecordBatchStream {
    let output_stream = stream! {
        let mut max_ts: Option<i64> = None;

        while let Some(batch_result) = input_stream.next().await {
            if let Ok(ref batch) = batch_result
                && let Ok(idx) = batch.schema().index_of(field.name()) {
                    let array = batch.column(idx);

                    let mut matched_supported_type = true;

                    match field.data_type() {
                        DataType::Timestamp(time_unit, _) => {
                            match time_unit {
                                TimeUnit::Nanosecond =>
                                    max_ts = max_ts_macro!(TimestampNanosecondArray, max_ts, array, |arr: &TimestampNanosecondArray, i| ts_to_ms(arr.value(i), time_format, TimeUnit::Nanosecond)),
                                TimeUnit::Microsecond =>
                                    max_ts = max_ts_macro!(TimestampMicrosecondArray, max_ts, array, |arr: &TimestampMicrosecondArray, i| ts_to_ms(arr.value(i), time_format, TimeUnit::Microsecond)),
                                TimeUnit::Millisecond =>
                                    max_ts = max_ts_macro!(TimestampMillisecondArray, max_ts, array, |arr: &TimestampMillisecondArray, i| ts_to_ms(arr.value(i), time_format, TimeUnit::Millisecond)),
                                TimeUnit::Second =>
                                    max_ts = max_ts_macro!(TimestampSecondArray, max_ts, array, |arr: &TimestampSecondArray, i| ts_to_ms(arr.value(i), time_format, TimeUnit::Second)),
                            }
                        }

                        // Utf8
                        DataType::Utf8 =>
                            max_ts = max_ts_macro!(StringArray, max_ts, array, |arr: &StringArray, i| string_to_ms(arr.value(i), time_format)),
                        DataType::LargeUtf8 =>
                            max_ts = max_ts_macro!(LargeStringArray, max_ts, array, |arr: &LargeStringArray, i| string_to_ms(arr.value(i), time_format)),

                        // Int
                        DataType::Int8 =>
                            max_ts = max_ts_macro!(Int8Array, max_ts, array, |arr: &Int8Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::Int16 =>
                            max_ts = max_ts_macro!(Int16Array, max_ts, array, |arr: &Int16Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::Int32 =>
                            max_ts = max_ts_macro!(Int32Array, max_ts, array, |arr: &Int32Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::Int64 =>
                            max_ts = max_ts_macro!(Int64Array, max_ts, array, |arr: &Int64Array, i| int_to_ms(arr.value(i), time_format)),

                        // UInt
                        DataType::UInt8 =>
                            max_ts = max_ts_macro!(UInt8Array, max_ts, array, |arr: &UInt8Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::UInt16 =>
                            max_ts = max_ts_macro!(UInt16Array, max_ts, array, |arr: &UInt16Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::UInt32 =>
                            max_ts = max_ts_macro!(UInt32Array, max_ts, array, |arr: &UInt32Array, i| int_to_ms(i64::from(arr.value(i)), time_format)),
                        DataType::UInt64 =>
                            max_ts = max_ts_macro!(UInt64Array, max_ts, array, |arr: &UInt64Array, i| uint64_to_ms(arr.value(i), time_format)),

                        // Float
                        DataType::Float16 =>
                            max_ts = max_ts_macro!(Float16Array, max_ts, array, |arr: &Float16Array, i| float_to_ms(arr.value(i).to_f64(), time_format)),
                        DataType::Float32 =>
                            max_ts = max_ts_macro!(Float32Array, max_ts, array, |arr: &Float32Array, i| float_to_ms(f64::from(arr.value(i)), time_format)),
                        DataType::Float64 =>
                            max_ts = max_ts_macro!(Float64Array, max_ts, array, |arr: &Float64Array, i| float_to_ms(arr.value(i), time_format)),

                        // Date32
                        DataType::Date32 =>
                            max_ts = max_ts_macro!(Date32Array, max_ts, array, |arr: &Date32Array, i| Some(i64::from(arr.value(i)) * MS_IN_A_DAY)),

                        _ => {
                            matched_supported_type = false;
                        }
                    }

                    if batch.num_rows() > 0 && max_ts.is_none() {
                        if matched_supported_type {
                            tracing::warn!(
                                "Failed to extract max_timestamp after refresh for {}: batch_size={}, time_column={}, field_type={}",
                                source_name,
                                batch.num_rows(),
                                time_column,
                                field.data_type(),
                            );
                        } else {
                            tracing::warn!(
                                "Failed to extract max_timestamp after refresh for {}: unsupported time column: time_column={}, field_type={}",
                                source_name,
                                time_column,
                                field.data_type(),
                            );
                        }
                    }
                }

            yield batch_result;
        }

        // Make this update as fast as possible
        *max_ts_clone.lock().await = max_ts;
    };

    Box::pin(RecordBatchStreamAdapter::new(schema, output_stream))
}

fn ts_to_ms(ts: i64, time_format: TimeFormat, time_unit: TimeUnit) -> Option<i64> {
    match time_format {
        TimeFormat::Timestamp | TimeFormat::Timestamptz => match time_unit {
            TimeUnit::Nanosecond => Some(ts / 1_000_000),
            TimeUnit::Microsecond => Some(ts / 1_000),
            TimeUnit::Millisecond => Some(ts),
            TimeUnit::Second => Some(ts * 1000),
        },
        _ => None,
    }
}

fn int_to_ms(ts: i64, time_format: TimeFormat) -> Option<i64> {
    match time_format {
        TimeFormat::UnixSeconds => Some(ts * 1000),
        TimeFormat::UnixMillis => Some(ts),
        _ => None,
    }
}

fn uint64_to_ms(ts: u64, time_format: TimeFormat) -> Option<i64> {
    i64::try_from(ts)
        .map(|v| int_to_ms(v, time_format))
        .ok()
        .flatten()
}

#[allow(clippy::cast_possible_truncation)]
fn float_to_ms(ts: f64, time_format: TimeFormat) -> Option<i64> {
    match time_format {
        TimeFormat::UnixSeconds => Some((ts * 1000.0) as i64),
        TimeFormat::UnixMillis => Some(ts as i64),
        _ => None,
    }
}

fn string_to_ms(s: &str, time_format: TimeFormat) -> Option<i64> {
    if time_format != TimeFormat::ISO8601 {
        return None;
    }

    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.timestamp_millis())
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataupdate::UpdateType;
    use arrow::array::*;
    use arrow::record_batch::RecordBatch;
    use arrow_schema::{Field, Schema};

    async fn prepare_and_run(
        batch: RecordBatch,
        time_format: Option<TimeFormat>,
    ) -> (StreamingDataUpdate, Option<Arc<Mutex<Option<i64>>>>) {
        // Prepare
        let schema = batch.schema();
        let stream = futures::stream::iter(vec![Ok(batch)]);

        let data_update = StreamingDataUpdate {
            data: Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), stream)),
            update_type: UpdateType::Append,
        };

        // Run
        let (new_data_update, max_ts_arc_opt) = with_find_max_timestamp_in_stream(
            data_update,
            schema,
            Some("ts".to_string()),
            time_format,
            "test_source".to_string(),
        )
        .await;
        (new_data_update, max_ts_arc_opt)
    }

    async fn perform_test(batch: RecordBatch, time_format: Option<TimeFormat>) -> i64 {
        let (new_data_update, max_ts_arc_opt) = prepare_and_run(batch, time_format).await;

        // Consume stream and extract max_ts
        let max_ts_arc = max_ts_arc_opt.expect("max_ts Arc should be returned");
        let mut stream = new_data_update.data;

        while let Some(_batch_result) = stream.next().await {}

        {
            let guard = max_ts_arc.lock().await;
            guard.expect("max_ts should be set after consuming stream")
        }
    }

    #[tokio::test]
    async fn test_utf8_iso8601() {
        let batch = record_batch!((
            "ts",
            Utf8,
            [
                Some("2010-01-01T00:00:00Z"),
                None,
                Some("2020-01-01T00:00:00Z")
            ]
        ))
        .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::ISO8601)).await;

        assert_eq!(max_ts, 1_577_836_800_000);
    }

    #[tokio::test]
    async fn test_uint16_milli_seconds() {
        let batch =
            record_batch!(("ts", UInt16, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_uint32_milli_seconds() {
        let batch =
            record_batch!(("ts", UInt32, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_uint64_milli_seconds() {
        let batch =
            record_batch!(("ts", UInt64, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_uint16_seconds() {
        let batch =
            record_batch!(("ts", UInt16, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixSeconds)).await;

        assert_eq!(max_ts, 2000 * 1000);
    }

    #[tokio::test]
    async fn test_uint32_seconds() {
        let batch =
            record_batch!(("ts", UInt32, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixSeconds)).await;

        assert_eq!(max_ts, 2000 * 1000);
    }

    #[tokio::test]
    async fn test_uint64_seconds() {
        let batch =
            record_batch!(("ts", UInt64, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixSeconds)).await;

        assert_eq!(max_ts, 2000 * 1000);
    }

    #[tokio::test]
    async fn test_int16_milli_seconds() {
        let batch =
            record_batch!(("ts", Int16, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_int32_milli_seconds() {
        let batch =
            record_batch!(("ts", Int32, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_int64_milli_seconds() {
        let batch =
            record_batch!(("ts", Int64, [Some(1000), Some(2000), None])).expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_float32_unix_milli_seconds() {
        let batch = record_batch!(("ts", Float32, [Some(1000.0), Some(2000.0), None]))
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_float64_unix_milli_seconds() {
        let batch = record_batch!(("ts", Float64, [Some(1000.0), Some(2000.0), None]))
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixMillis)).await;

        assert_eq!(max_ts, 2000);
    }

    #[tokio::test]
    async fn test_float32_unix_seconds() {
        let batch = record_batch!(("ts", Float32, [Some(1000.0), Some(2000.0), None]))
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixSeconds)).await;

        assert_eq!(max_ts, 2000 * 1000);
    }

    #[tokio::test]
    async fn test_float64_unix_seconds() {
        let batch = record_batch!(("ts", Float64, [Some(1000.0), Some(2000.0), None]))
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::UnixSeconds)).await;
        assert_eq!(max_ts, 2000 * 1000);
    }

    #[tokio::test]
    async fn test_date32() {
        let array = Date32Array::from(vec![Some(0), Some(18262), None]);

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Date32, true)]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Date)).await;
        assert_eq!(max_ts, 18262 * 86_400_000);
    }

    #[tokio::test]
    async fn test_timestamp_nanosecond() {
        let array = TimestampNanosecondArray::from(vec![
            Some(1_500_000_000_000_000_000),
            None,
            Some(1_600_000_000_000_000_000),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000_000_000 / 1_000_000);
    }

    #[tokio::test]
    async fn test_timestamp_microsecond() {
        let array = TimestampMicrosecondArray::from(vec![
            Some(1_500_000_000_000_000),
            None,
            Some(1_600_000_000_000_000),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000_000 / 1_000);
    }

    #[tokio::test]
    async fn test_timestamp_millisecond() {
        let array = TimestampMillisecondArray::from(vec![
            Some(1_500_000_000_000),
            None,
            Some(1_600_000_000_000),
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000);
    }

    #[tokio::test]
    async fn test_timestamp_second() {
        let array =
            TimestampSecondArray::from(vec![Some(1_500_000_000), None, Some(1_600_000_000)]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000 * 1000);
    }

    #[tokio::test]
    async fn test_timestamp_nanosecond_timezone() {
        let tz = Arc::<str>::from("UTC");
        let mut builder =
            TimestampNanosecondBuilder::with_capacity(3).with_timezone(Arc::clone(&tz));
        builder.append_value(1_500_000_000_000_000_000);
        builder.append_null();
        builder.append_value(1_600_000_000_000_000_000);
        let array = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000_000_000 / 1_000_000);
    }

    #[tokio::test]
    async fn test_timestamp_microsecond_timezone() {
        let tz = Arc::<str>::from("America/New_York");
        let mut builder =
            TimestampMicrosecondBuilder::with_capacity(3).with_timezone(Arc::clone(&tz));
        builder.append_value(1_500_000_000_000_000_000);
        builder.append_null();
        builder.append_value(1_600_000_000_000_000_000);
        let array = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(tz)),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000_000_000 / 1_000);
    }

    #[tokio::test]
    async fn test_timestamp_millisecond_timezone() {
        let tz = Arc::<str>::from("Asia/Tokyo");
        let mut builder =
            TimestampMillisecondBuilder::with_capacity(3).with_timezone(Arc::clone(&tz));
        builder.append_value(1_500_000_000_000);
        builder.append_null();
        builder.append_value(1_600_000_000_000);
        let array = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some(tz)),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000_000);
    }

    #[tokio::test]
    async fn test_timestamp_second_timezone() {
        let tz = Arc::<str>::from("Europe/London");
        let mut builder = TimestampSecondBuilder::with_capacity(3).with_timezone(Arc::clone(&tz));
        builder.append_value(1_500_000_000);
        builder.append_null();
        builder.append_value(1_600_000_000);
        let array = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, Some(tz)),
            true,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let max_ts = perform_test(batch, Some(TimeFormat::Timestamp)).await;
        assert_eq!(max_ts, 1_600_000_000 * 1000);
    }

    #[tokio::test]
    async fn test_missing_time_column() {
        let batch = record_batch!(("other_column", UInt16, [Some(1000), Some(2000), None]))
            .expect("created batch");

        let (_, max_ts) = prepare_and_run(batch, Some(TimeFormat::UnixSeconds)).await;
        assert!(max_ts.is_none());
    }

    #[tokio::test]
    async fn test_unsupported_time_column() {
        let array = Date64Array::from(vec![Some(0), Some(18262), None]);

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Date64, true)]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let (new_data_update, max_ts_arc_opt) =
            prepare_and_run(batch, Some(TimeFormat::UnixSeconds)).await;

        let max_ts_arc = max_ts_arc_opt.expect("max_ts Arc should be returned");
        let mut stream = new_data_update.data;

        while let Some(_batch_result) = stream.next().await {}

        let max_ts = {
            let guard = max_ts_arc.lock().await;
            *guard
        };

        assert!(max_ts.is_none());
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let array = UInt16Array::from(vec![] as Vec<u16>);

        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::UInt16, true)]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("created batch");

        let (new_data_update, max_ts_arc_opt) =
            prepare_and_run(batch, Some(TimeFormat::UnixSeconds)).await;

        let max_ts_arc = max_ts_arc_opt.expect("max_ts Arc should be returned");
        let mut stream = new_data_update.data;

        while let Some(_batch_result) = stream.next().await {}

        let max_ts = {
            let guard = max_ts_arc.lock().await;
            *guard
        };

        assert!(max_ts.is_none());
    }
}
