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

//! Shared timestamp filter conversion logic for building `DataFusion` filter
//! expressions from timestamp values.

use arrow::datatypes::DataType;
use datafusion::{
    logical_expr::{Expr, Operator, binary_expr, cast, col, lit},
    prelude::and,
    scalar::ScalarValue,
};
use std::sync::Arc;

/// Timestamp format for a column — determines how target filter expressions
/// are constructed.
#[derive(Debug, Clone)]
pub enum TimestampFormat {
    /// ISO 8601 string column (`Utf8`, `LargeUtf8`, `Utf8View`).
    Iso8601,
    /// Integer column representing a unix epoch value, divided by `scale`
    /// to convert from nanoseconds.
    ///
    /// `scale = 1_000_000_000` → seconds, `scale = 1_000_000` → milliseconds.
    UnixTimestamp { scale: u128 },
    /// `Date64`, `Time32`, `Time64`
    Timestamp,
    /// `Timestamp(unit, tz)` — with optional timezone.
    Timestamptz(Option<Arc<str>>),
    /// `Date32`.
    Date,
}

/// Builds a filter expression for a single column and timestamp value.
///
/// Constructs `CAST(col AS Timestamp(ns, tz)) op literal` for timestamp columns,
/// or `col op literal_value` for unix integer columns.
#[expect(clippy::cast_possible_truncation)]
fn convert_timestamp_expr(
    timestamp_in_nanos: u128,
    time_column: &str,
    time_format: &TimestampFormat,
    op: Operator,
) -> Expr {
    let time_column: &str = &format!(r#""{}""#, &time_column);
    match time_format {
        TimestampFormat::UnixTimestamp { scale } => binary_expr(
            col(time_column),
            op,
            lit((timestamp_in_nanos / scale) as u64),
        ),
        TimestampFormat::Date | TimestampFormat::Timestamp | TimestampFormat::Iso8601 => {
            binary_expr(
                cast(
                    col(time_column),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                ),
                op,
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), None),
                    None,
                ),
            )
        }
        TimestampFormat::Timestamptz(tz) => binary_expr(
            cast(
                col(time_column),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, tz.clone()),
            ),
            op,
            Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), tz.to_owned()),
                None,
            ),
        ),
    }
}

/// Derive a [`TimestampFormat`] from an Arrow `DataType`.
///
/// `unix_timestamp_scale` is used only for integer/float columns:
/// - `Some(1_000_000_000)` — values are in seconds (divide nanos by 1B)
/// - `Some(1_000_000)` — values are in milliseconds (divide nanos by 1M)
/// - `None` — integer columns are not supported (returns `None` for them)
#[must_use]
pub fn data_type_to_timestamp_format(
    data_type: &DataType,
    unix_timestamp_scale: Option<u128>,
) -> Option<TimestampFormat> {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => {
            let scale = unix_timestamp_scale?;
            Some(TimestampFormat::UnixTimestamp { scale })
        }
        DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
            Some(TimestampFormat::Timestamp)
        }
        DataType::Timestamp(_, tz) => Some(TimestampFormat::Timestamptz(tz.to_owned())),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Some(TimestampFormat::Iso8601),
        DataType::Date32 => Some(TimestampFormat::Date),
        _ => {
            tracing::warn!("Data type is not supported yet: {data_type}");
            None
        }
    }
}

/// Converter that builds filter expressions from a timestamp value,
/// supporting an optional partition column.
///
/// Stores pre-resolved [`TimestampFormat`]s for both the main time column
/// and an optional partition column.
#[expect(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub struct TimestampFilterConvert {
    time_column: String,
    time_format: TimestampFormat,

    /// An optional column that represents the same time as `time_column`
    /// but is used for partitioning.
    time_partition_column: Option<String>,
    time_partition_format: Option<TimestampFormat>,
}

impl TimestampFilterConvert {
    /// Create a new converter with pre-resolved formats.
    ///
    /// Use [`data_type_to_timestamp_format`] to resolve formats from Arrow
    /// `DataType`s before calling this.
    #[must_use]
    pub fn new(
        time_column: String,
        time_format: TimestampFormat,
        time_partition_column: Option<String>,
        time_partition_format: Option<TimestampFormat>,
    ) -> Self {
        Self {
            time_column,
            time_format,
            time_partition_column,
            time_partition_format,
        }
    }

    /// Build a filter expression for the given timestamp (in nanoseconds).
    ///
    /// If a partition column is configured, the result is
    /// `time_expr AND partition_expr`.
    #[must_use]
    pub fn convert(&self, timestamp_in_nanos: u128, op: Operator) -> Expr {
        let time_expr =
            convert_timestamp_expr(timestamp_in_nanos, &self.time_column, &self.time_format, op);
        match (&self.time_partition_column, &self.time_partition_format) {
            (Some(time_partition_column), Some(time_partition_format)) => {
                let time_partition_expr = convert_timestamp_expr(
                    timestamp_in_nanos,
                    time_partition_column,
                    time_partition_format,
                    op,
                );
                and(time_expr, time_partition_expr)
            }
            _ => time_expr,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    /// Helper: build a converter from a data type + optional scale, assert expr output.
    fn test_convert(
        data_type: &DataType,
        unix_scale: Option<u128>,
        timestamp: u128,
        expected: &str,
    ) {
        let format =
            data_type_to_timestamp_format(data_type, unix_scale).expect("format should resolve");
        let converter = TimestampFilterConvert::new("timestamp".to_string(), format, None, None);
        let expr = converter.convert(timestamp, Operator::Gt);
        assert_eq!(expr.to_string(), expected);
    }

    #[test]
    fn test_unix_millis() {
        test_convert(
            &DataType::Int64,
            Some(1_000_000),
            1_620_000_000_000_000_000,
            "timestamp > UInt64(1620000000000)",
        );
    }

    #[test]
    fn test_unix_seconds() {
        test_convert(
            &DataType::Int64,
            Some(1_000_000_000),
            1_620_000_000_000_000_000,
            "timestamp > UInt64(1620000000)",
        );
    }

    #[test]
    fn test_timestamp_no_tz() {
        test_convert(
            &DataType::Timestamp(TimeUnit::Second, None),
            None,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_utf8_iso8601() {
        test_convert(
            &DataType::Utf8,
            None,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_timestamp_with_timezone() {
        let format = data_type_to_timestamp_format(
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            None,
        )
        .expect("should resolve");
        let converter = TimestampFilterConvert::new("timestamp".to_string(), format, None, None);
        let result = converter.convert(1_620_000_000_000_000_000, Operator::Gt);
        assert_eq!(
            result.to_string(),
            r#"CAST(timestamp AS Timestamp(ns, "UTC")) > TimestampNanosecond(1620000000000000000, Some("UTC"))"#,
        );
    }

    #[test]
    fn test_with_partition_column() {
        let time_format = data_type_to_timestamp_format(&DataType::Int64, Some(1_000_000))
            .expect("should resolve");
        let partition_format = data_type_to_timestamp_format(&DataType::Int64, Some(1_000_000))
            .expect("should resolve");

        let converter = TimestampFilterConvert::new(
            "timestamp".to_string(),
            time_format,
            Some("partition_ts".to_string()),
            Some(partition_format),
        );

        let result = converter.convert(1_620_000_000_000_000_000, Operator::Gt);
        assert_eq!(
            result.to_string(),
            "timestamp > UInt64(1620000000000) AND partition_ts > UInt64(1620000000000)",
        );
    }

    #[test]
    fn test_mixed_partition_formats() {
        let time_format = data_type_to_timestamp_format(&DataType::Int64, Some(1_000_000))
            .expect("should resolve");
        let partition_format =
            data_type_to_timestamp_format(&DataType::Timestamp(TimeUnit::Second, None), None)
                .expect("should resolve");

        let converter = TimestampFilterConvert::new(
            "timestamp".to_string(),
            time_format,
            Some("partition_ts".to_string()),
            Some(partition_format),
        );

        let result = converter.convert(1_620_000_000_000_000_000, Operator::Gt);
        assert_eq!(
            result.to_string(),
            "timestamp > UInt64(1620000000000) AND CAST(partition_ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_int_column_without_scale_returns_none() {
        let result = data_type_to_timestamp_format(&DataType::Int64, None);
        assert!(result.is_none(), "Int64 without scale should return None");
    }

    #[test]
    fn test_unsupported_type_returns_none() {
        let result = data_type_to_timestamp_format(&DataType::Boolean, None);
        assert!(result.is_none(), "Boolean should return None");
    }
}
