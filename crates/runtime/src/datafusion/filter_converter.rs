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

use crate::component::dataset::TimeFormat;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    logical_expr::{Expr, Operator, binary_expr, cast, col, lit},
    prelude::and,
    scalar::ScalarValue,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum ExprTimeFormat {
    ISO8601,
    UnixTimestamp(ExprUnixTimestamp),
    /// Timestamp without timezone. If `unit` is `None` (unknown), a cast to nanosecond is required
    /// (e.g., for Date64, Time32, Time64 types that need conversion).
    Timestamp {
        unit: Option<TimeUnit>,
    },
    Timestamptz {
        unit: TimeUnit,
        tz: Arc<str>,
    },
    Date,
}

#[derive(Debug, Clone, Copy)]
struct ExprUnixTimestamp {
    scale: u128,
}

#[expect(clippy::struct_field_names)]
#[derive(Clone, Debug)]
pub(crate) struct TimestampFilterConvert {
    time_column: String,
    time_format: ExprTimeFormat,

    // An optional column that represents the same time as `time_column` but is used for partitioning
    time_partition_column: Option<String>,
    time_partition_format: Option<ExprTimeFormat>,
}

#[expect(clippy::needless_pass_by_value)]
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

#[expect(clippy::cast_possible_truncation)]
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
        ExprTimeFormat::Date
        | ExprTimeFormat::ISO8601
        | ExprTimeFormat::Timestamp { unit: None } => {
            // Cast to nanosecond for types where we don't have precise unit info
            // (ISO8601 strings, Date32, Date64, Time32, Time64)
            binary_expr(
                cast(
                    col(time_column),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ),
                op,
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), None),
                    None,
                ),
            )
        }
        ExprTimeFormat::Timestamp { unit: Some(unit) } => {
            timestamp_filter_expr(timestamp_in_nanos, time_column, *unit, None, op)
        }
        ExprTimeFormat::Timestamptz { unit, tz } => {
            timestamp_filter_expr(timestamp_in_nanos, time_column, *unit, Some(tz), op)
        }
    }
}

/// Creates a timestamp filter expression using the column's native time unit when possible.
///
/// Skips casting if `timestamp_in_nanos` is aligned to the column's unit (no precision loss).
/// Falls back to casting the column to nanoseconds if the value has higher precision (for safety).
/// The timestamp is converted without additional cast for:
/// 1. Refresh: `timestamp_in_nanos` is based on data values which can't have higher precision than the column's unit
/// 2. Retention period: `timestamp_in_nanos` has millisecond precision (truncated in retention.rs) and will be converted without cast for millisecond, microsecond, and nanosecond columns (second precision is rare and will still cast)
#[expect(clippy::cast_possible_truncation)]
fn timestamp_filter_expr(
    timestamp_in_nanos: u128,
    time_column: &str,
    unit: TimeUnit,
    tz: Option<&Arc<str>>,
    op: Operator,
) -> Expr {
    let tz_owned = tz.map(Arc::clone);
    if is_aligned_to_unit(timestamp_in_nanos, unit) {
        let literal = timestamp_scalar(timestamp_in_nanos, unit, tz_owned);
        binary_expr(col(time_column), op, Expr::Literal(literal, None))
    } else {
        binary_expr(
            cast(
                col(time_column),
                DataType::Timestamp(TimeUnit::Nanosecond, tz_owned.clone()),
            ),
            op,
            Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), tz_owned),
                None,
            ),
        )
    }
}

/// Checks if the nanosecond timestamp is aligned to the given time unit (no precision loss).
fn is_aligned_to_unit(timestamp_in_nanos: u128, unit: TimeUnit) -> bool {
    let divisor = match unit {
        TimeUnit::Nanosecond => 1,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Second => 1_000_000_000,
    };
    timestamp_in_nanos.is_multiple_of(divisor)
}

/// Creates a `ScalarValue` timestamp literal with the specified time unit and optional timezone.
/// Converts from nanoseconds to the target unit.
#[expect(clippy::cast_possible_truncation)]
fn timestamp_scalar(timestamp_in_nanos: u128, unit: TimeUnit, tz: Option<Arc<str>>) -> ScalarValue {
    match unit {
        TimeUnit::Nanosecond => {
            ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), tz)
        }
        TimeUnit::Microsecond => {
            ScalarValue::TimestampMicrosecond(Some((timestamp_in_nanos / 1_000) as i64), tz)
        }
        TimeUnit::Millisecond => {
            ScalarValue::TimestampMillisecond(Some((timestamp_in_nanos / 1_000_000) as i64), tz)
        }
        TimeUnit::Second => {
            ScalarValue::TimestampSecond(Some((timestamp_in_nanos / 1_000_000_000) as i64), tz)
        }
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
            if let Some(time_format) = time_format
                && time_format == TimeFormat::UnixMillis
            {
                scale = 1_000_000;
            }
            Some(ExprTimeFormat::UnixTimestamp(ExprUnixTimestamp { scale }))
        }
        DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) => {
            Some(ExprTimeFormat::Timestamp { unit: None })
        }
        DataType::Timestamp(unit, None) => Some(ExprTimeFormat::Timestamp { unit: Some(*unit) }),
        DataType::Timestamp(unit, Some(tz)) => Some(ExprTimeFormat::Timestamptz {
            unit: *unit,
            tz: Arc::clone(tz),
        }),
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
            "timestamp > TimestampSecond(1620000000, None)",
        );
        test(
            Field::new("timestamp", DataType::Utf8, false),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000,
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
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
            "timestamp > UInt64(1620000000000) AND partition_ts > TimestampSecond(1620000000, None)"
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
            r#"timestamp > TimestampNanosecond(1620000000000000000, Some("UTC"))"#
        );
    }

    #[test]
    fn test_aligned_timestamp_skips_cast() {
        // Aligned to microseconds (divisible by 1000) - should skip cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000, // Aligned to microseconds
            "timestamp > TimestampMicrosecond(1620000000000000, None)",
        );

        // Aligned to milliseconds (divisible by 1_000_000) - should skip cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000, // Aligned to milliseconds
            "timestamp > TimestampMillisecond(1620000000000, None)",
        );

        // Aligned to seconds (divisible by 1_000_000_000) - should skip cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_000, // Aligned to seconds
            "timestamp > TimestampSecond(1620000000, None)",
        );
    }

    #[test]
    fn test_unaligned_timestamp_uses_cast() {
        // Not aligned to microseconds - should cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_001, // Has sub-microsecond precision
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000001, None)",
        );

        // Not aligned to milliseconds - should cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_000_000_500, // Has sub-millisecond precision
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000000000500, None)",
        );

        // Not aligned to seconds - should cast
        test(
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000_500_000_000, // Has sub-second precision
            "CAST(timestamp AS Timestamp(ns)) > TimestampNanosecond(1620000000500000000, None)",
        );
    }

    #[test]
    fn test_aligned_timestamptz_skips_cast() {
        // Aligned to microseconds with timezone - should skip cast
        let time_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
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
            r#"timestamp > TimestampMicrosecond(1620000000000000, Some("UTC"))"#
        );
    }

    #[test]
    fn test_unaligned_timestamptz_uses_cast() {
        // Not aligned to microseconds with timezone - should cast
        let time_field = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
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
            Some(c) => c.convert(1_620_000_000_000_000_001, Operator::Gt), // Sub-microsecond
            None => panic!("Failed to create converter"),
        };

        assert_eq!(
            result.to_string(),
            r#"CAST(timestamp AS Timestamp(ns, "UTC")) > TimestampNanosecond(1620000000000000001, Some("UTC"))"#
        );
    }

    #[test]
    fn test_is_aligned_to_unit() {
        // Nanosecond - always aligned
        assert!(is_aligned_to_unit(1, TimeUnit::Nanosecond));
        assert!(is_aligned_to_unit(999, TimeUnit::Nanosecond));

        // Microsecond - aligned if divisible by 1000
        assert!(is_aligned_to_unit(1_000, TimeUnit::Microsecond));
        assert!(is_aligned_to_unit(1_000_000, TimeUnit::Microsecond));
        assert!(!is_aligned_to_unit(1_001, TimeUnit::Microsecond));
        assert!(!is_aligned_to_unit(999, TimeUnit::Microsecond));

        // Millisecond - aligned if divisible by 1_000_000
        assert!(is_aligned_to_unit(1_000_000, TimeUnit::Millisecond));
        assert!(is_aligned_to_unit(1_000_000_000, TimeUnit::Millisecond));
        assert!(!is_aligned_to_unit(1_000_001, TimeUnit::Millisecond));
        assert!(!is_aligned_to_unit(999_999, TimeUnit::Millisecond));

        // Second - aligned if divisible by 1_000_000_000
        assert!(is_aligned_to_unit(1_000_000_000, TimeUnit::Second));
        assert!(is_aligned_to_unit(2_000_000_000, TimeUnit::Second));
        assert!(!is_aligned_to_unit(1_000_000_001, TimeUnit::Second));
        assert!(!is_aligned_to_unit(999_999_999, TimeUnit::Second));
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
