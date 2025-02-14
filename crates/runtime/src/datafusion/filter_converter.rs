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
use arrow::datatypes::DataType;
use datafusion::{
    logical_expr::{binary_expr, cast, col, lit, Expr, Operator},
    prelude::and,
    scalar::ScalarValue,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum ExprTimeFormat {
    ISO8601,
    UnixTimestamp(ExprUnixTimestamp),
    Timestamp,
    Timestamptz(Option<Arc<str>>),
    Date,
}

#[derive(Debug, Clone, Copy)]
struct ExprUnixTimestamp {
    scale: u128,
}

#[allow(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub(crate) struct TimestampFilterConvert {
    time_column: String,
    time_format: ExprTimeFormat,

    // An optional column that represents the same time as `time_column` but is used for partitioning
    time_partition_column: Option<String>,
    time_partition_format: Option<ExprTimeFormat>,
}

#[allow(clippy::needless_pass_by_value)]
impl TimestampFilterConvert {
    pub(crate) fn create(
        field: Option<arrow::datatypes::Field>,
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        partition_field: Option<arrow::datatypes::Field>,
        time_partition_column: Option<String>,
        time_partition_format: Option<TimeFormat>,
    ) -> Option<Self> {
        let field = field?;
        let time_column = time_column?;

        let time_format = data_type_to_time_format(field.data_type(), time_format)?;
        let time_partition_format = partition_field
            .as_ref()
            .and_then(|f| data_type_to_time_format(f.data_type(), time_partition_format));

        Some(Self {
            time_column,
            time_format,
            time_partition_column,
            time_partition_format,
        })
    }

    #[allow(clippy::cast_possible_wrap)]
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn convert(&self, timestamp_in_nanos: u128, op: Operator) -> Expr {
        let time_expr =
            convert_to_expr(timestamp_in_nanos, &self.time_column, &self.time_format, op);
        match (&self.time_partition_column, &self.time_partition_format) {
            (Some(time_partition_column), Some(time_partition_format)) => {
                let time_partition_expr = convert_to_expr(
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

#[allow(clippy::cast_possible_truncation)]
fn convert_to_expr(
    timestamp_in_nanos: u128,
    time_column: &str,
    time_format: &ExprTimeFormat,
    op: Operator,
) -> Expr {
    let time_column: &str = &format!(r#""{}""#, &time_column);
    match time_format {
        ExprTimeFormat::UnixTimestamp(format) => binary_expr(
            col(time_column),
            op,
            lit((timestamp_in_nanos / format.scale) as u64),
        ),
        ExprTimeFormat::Date | ExprTimeFormat::Timestamp | ExprTimeFormat::ISO8601 => {
            binary_expr(
                // The time unit of timestamp is unknown before filtering
                // Convert the left and right expr to same unit for safe comparison
                cast(
                    col(time_column),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                ),
                op,
                Expr::Literal(ScalarValue::TimestampNanosecond(
                    Some(timestamp_in_nanos as i64),
                    None,
                )),
            )
        }
        ExprTimeFormat::Timestamptz(tz) => binary_expr(
            cast(
                col(time_column),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            ),
            op,
            Expr::Literal(ScalarValue::TimestampNanosecond(
                Some(timestamp_in_nanos as i64),
                tz.to_owned(),
            )),
        ),
    }
}

fn data_type_to_time_format(
    data_type: &DataType,
    time_format: Option<TimeFormat>,
) -> Option<ExprTimeFormat> {
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
            let mut scale = 1_000_000_000;
            if let Some(time_format) = time_format {
                if time_format == TimeFormat::UnixMillis {
                    scale = 1_000_000;
                }
            }
            Some(ExprTimeFormat::UnixTimestamp(ExprUnixTimestamp { scale }))
        }
        DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
            Some(ExprTimeFormat::Timestamp)
        }
        DataType::Timestamp(_, tz) => Some(ExprTimeFormat::Timestamptz(tz.to_owned())),
        DataType::Utf8 | DataType::LargeUtf8 => Some(ExprTimeFormat::ISO8601),
        DataType::Date32 => Some(ExprTimeFormat::Date),
        _ => {
            tracing::warn!("Date type is not handled yet: {}", data_type);
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};

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
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(Nanosecond, None)) > TimestampNanosecond(1620000000000000000, None)",
        );
        test(
            Field::new(
                "timestamp",
                DataType::Utf8,
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(Nanosecond, None)) > TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_timestamp_filter_convert_with_partition() {
        // Test case with both time and partition columns as Int64
        let time_field = Field::new("timestamp", DataType::Int64, false);
        let partition_field = Field::new("partition_ts", DataType::Int64, false);

        let converter = TimestampFilterConvert::create(
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

        let converter = TimestampFilterConvert::create(
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
            "timestamp > UInt64(1620000000000) AND CAST(partition_ts AS Timestamp(Nanosecond, None)) > TimestampNanosecond(1620000000000000000, None)"
        );
    }

    #[test]
    fn test_timestamp_filter_convert_with_timezone() {
        let time_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        );

        let converter = TimestampFilterConvert::create(
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
            r#"CAST(timestamp AS Timestamp(Nanosecond, None)) > TimestampNanosecond(1620000000000000000, Some("UTC"))"#
        );
    }

    fn test(field: Field, time_format: TimeFormat, timestamp: u128, expected: &str) {
        let time_column = "timestamp".to_string();
        let timestamp_filter_convert = TimestampFilterConvert::create(
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
