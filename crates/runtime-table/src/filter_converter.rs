/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Runtime-specific helpers for creating [`TimestampFilterConvert`] instances
//! from spicepod configuration types.
//!
//! The core conversion logic lives in [`util::timestamp_filter`].

use runtime_component::dataset::TimeFormat;
pub use util::timestamp_filter::TimestampFilterConvert;
use util::timestamp_filter::data_type_to_timestamp_format;

/// Create a [`TimestampFilterConvert`] from runtime configuration types.
///
/// This is the runtime-specific entry point that maps [`TimeFormat`] config
/// values to the unix timestamp scale, then delegates to the shared util logic.
#[expect(clippy::needless_pass_by_value)]
pub(crate) fn create_timestamp_filter_convert(
    field: Option<arrow::datatypes::Field>,
    time_column: Option<String>,
    time_format: Option<TimeFormat>,
    partition_field: Option<arrow::datatypes::Field>,
    time_partition_column: Option<String>,
    time_partition_format: Option<TimeFormat>,
) -> Option<TimestampFilterConvert> {
    let field = field?;
    let time_column = time_column?;

    let unix_scale = time_format_to_unix_scale(time_format);
    let format = data_type_to_timestamp_format(field.data_type(), unix_scale)?;

    let partition_fmt = partition_field.as_ref().and_then(|f| {
        let partition_scale = time_format_to_unix_scale(time_partition_format);
        data_type_to_timestamp_format(f.data_type(), partition_scale)
    });

    // Day-granularity is deliberately NOT resolved here. It governs the append
    // high-water comparison, whose two sides are filtered through two different
    // converters (source schema and accelerator schema); deciding it from one schema
    // at a time lets those two disagree and re-append the same rows every refresh.
    // The caller resolves it across both schemas and applies
    // `with_day_granular_columns` — see `RefreshTask::day_granular_high_water_columns`.
    Some(TimestampFilterConvert::new(
        time_column,
        format,
        time_partition_column,
        partition_fmt,
    ))
}

/// Map the runtime `TimeFormat` config to the unix timestamp scale used by
/// [`data_type_to_timestamp_format`] for integer columns.
///
/// Returns `None` for formats that don't represent unix epoch integers.
fn time_format_to_unix_scale(time_format: Option<TimeFormat>) -> Option<u128> {
    match time_format {
        Some(TimeFormat::UnixSeconds) => Some(1_000_000_000),
        Some(TimeFormat::UnixMillis) => Some(1_000_000),
        Some(TimeFormat::UnixNanos) => Some(1),
        Some(
            TimeFormat::Timestamp
            | TimeFormat::Timestamptz
            | TimeFormat::ISO8601
            | TimeFormat::Date,
        )
        | None => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use datafusion::logical_expr::Operator;

    #[test]
    fn test_timestamp_filter_convert() {
        test(
            Field::new("timestamp", DataType::Int64, false),
            TimeFormat::UnixMillis,
            1_620_000_000_000_000_000,
            "timestamp > UInt64(1620000000000)",
        );
        test(
            Field::new("timestamp", DataType::Int64, false),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            "timestamp > UInt64(1620000000)",
        );
        test(
            Field::new("timestamp", DataType::Int64, false),
            TimeFormat::UnixNanos,
            1_620_000_000_000_000_000,
            "timestamp > UInt64(1620000000000000000)",
        );
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
        );
        test(
            Field::new("timestamp", DataType::Utf8, false),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            r#"timestamp > Utf8("2021-05-03T00:00:00.000000000")"#,
        );
    }

    #[test]
    fn test_timestamp_filter_convert_with_partition() {
        // Test case with both time and partition columns as Int64
        let time_field = Field::new("timestamp", DataType::Int64, false);
        let partition_field = Field::new("partition_ts", DataType::Int64, false);

        let converter = create_timestamp_filter_convert(
            Some(time_field),
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixMillis),
            Some(partition_field),
            Some("partition_ts".to_string()),
            Some(TimeFormat::UnixMillis),
        );

        let result = match converter {
            Some(c) => c.convert(1_620_000_000_000_000_000, Operator::Gt),
            None => panic!("Failed to create converter"),
        };

        assert_eq!(
            result.to_string(),
            "timestamp > UInt64(1620000000000) AND partition_ts > UInt64(1620000000000)"
        );

        // Test case with timestamp and partition columns having different formats
        let time_field = Field::new("timestamp", DataType::Int64, false);
        let partition_field = Field::new(
            "partition_ts",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        );

        let converter = create_timestamp_filter_convert(
            Some(time_field),
            Some("timestamp".to_string()),
            Some(TimeFormat::UnixMillis),
            Some(partition_field),
            Some("partition_ts".to_string()),
            Some(TimeFormat::UnixSeconds),
        );

        let result = match converter {
            Some(c) => c.convert(1_620_000_000_000_000_000, Operator::Gt),
            None => panic!("Failed to create converter"),
        };

        assert_eq!(
            result.to_string(),
            "timestamp > UInt64(1620000000000) AND CAST(partition_ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)"
        );
    }

    #[test]
    fn test_timestamp_filter_convert_with_timezone() {
        let time_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        );

        let converter = create_timestamp_filter_convert(
            Some(time_field),
            Some("timestamp".to_string()),
            None,
            None,
            None,
            None,
        );

        let result = match converter {
            Some(c) => c.convert(1_620_000_000_000_000_000, Operator::Gt),
            None => panic!("Failed to create converter"),
        };

        assert_eq!(
            result.to_string(),
            r#"CAST(timestamp AS Timestamp(ns, "UTC")) > TimestampNanosecond(1620000000000000000, Some("UTC"))"#
        );
    }

    /// This factory deliberately does not resolve day-granularity: the append
    /// high-water comparison is filtered through two converters (source schema and
    /// accelerator schema) that must agree, so the caller resolves it across both and
    /// applies `with_day_granular_columns`. A converter straight out of here must
    /// therefore compare strictly, even for a `Date32` column — otherwise a caller that
    /// forgot to resolve it would silently get half of the #12492 fix, which is the
    /// duplication case rather than the stall.
    #[test]
    fn test_factory_leaves_the_high_water_comparison_strict() {
        let converter = create_timestamp_filter_convert(
            Some(Field::new("ts", DataType::Date32, false)),
            Some("ts".to_string()),
            Some(TimeFormat::Date),
            None,
            None,
            None,
        )
        .expect("converter should be created");

        assert_eq!(
            converter
                .convert_high_water_mark(1_620_000_000_000_000_000)
                .to_string(),
            "CAST(ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
        );
    }

    fn test(field: Field, time_format: TimeFormat, timestamp: u128, expected: &str) {
        let time_column = "timestamp".to_string();
        let timestamp_filter_convert = create_timestamp_filter_convert(
            Some(field),
            Some(time_column),
            Some(time_format),
            None,
            None,
            None,
        );
        assert!(timestamp_filter_convert.is_some());
        let timestamp_filter_convert =
            timestamp_filter_convert.expect("the convert can be created");
        let expr = timestamp_filter_convert.convert(timestamp, Operator::Gt);
        assert_eq!(expr.to_string(), expected);
    }
}
