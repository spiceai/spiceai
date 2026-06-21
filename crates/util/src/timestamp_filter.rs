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

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::{
    logical_expr::{Expr, Operator, binary_expr, cast, lit},
    prelude::{and, ident},
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
    /// `Timestamp(unit, tz)` — carries the column's stored time unit (and
    /// optional timezone) so the filter literal matches the column exactly. A
    /// fixed nanosecond literal + `CAST(col AS Timestamp(ns))` would otherwise
    /// force a lossy cross-unit cast that can invert interval bounds during
    /// statistics analysis (tripping the `lower <= upper` assertion in
    /// `Interval::try_new`).
    Timestamptz(TimeUnit, Option<Arc<str>>),
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
    match time_format {
        TimestampFormat::UnixTimestamp { scale } => binary_expr(
            ident(time_column),
            op,
            lit((timestamp_in_nanos / scale) as u64),
        ),
        TimestampFormat::Iso8601 => {
            // ISO8601 strings are lexicographically orderable — string comparison
            // produces correct results without a CAST, which avoids issues with engines
            // (e.g. Vortex/Cayenne) that lack a utf8→timestamp cast kernel.
            let iso_string = nanos_to_iso8601_string(timestamp_in_nanos);
            binary_expr(ident(time_column), op, lit(iso_string))
        }
        TimestampFormat::Date | TimestampFormat::Timestamp => binary_expr(
            cast(
                ident(time_column),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            ),
            op,
            Expr::Literal(
                ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), None),
                None,
            ),
        ),
        TimestampFormat::Timestamptz(unit, tz) => binary_expr(
            // No CAST: a literal in the column's own unit compares directly,
            // which keeps the predicate exact, lets the federated source prune
            // on it, and avoids the cross-unit cast that inverts interval
            // bounds in statistics analysis.
            ident(time_column),
            op,
            Expr::Literal(
                timestamp_scalar_for_unit(timestamp_in_nanos, *unit, tz.as_ref()),
                None,
            ),
        ),
    }
}

/// Build a `Timestamp` [`ScalarValue`] in `unit` from a nanosecond value, so the
/// filter literal matches the column's stored unit exactly (no cross-unit cast).
///
/// Sub-unit precision is truncated toward the epoch. The refresh watermark is
/// derived from the column itself, so it already carries the column's precision
/// and this is exact in practice; even if it weren't, truncating a `>` watermark
/// (or `<` retention cutoff) toward zero is conservative — it never drops a new
/// row or over-deletes — and the exact dedup step downstream removes any
/// boundary duplicate.
#[expect(clippy::cast_possible_truncation)]
fn timestamp_scalar_for_unit(
    timestamp_in_nanos: u128,
    unit: TimeUnit,
    tz: Option<&Arc<str>>,
) -> ScalarValue {
    let tz = tz.map(Arc::clone);
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
        DataType::Timestamp(unit, tz) => Some(TimestampFormat::Timestamptz(*unit, tz.to_owned())),
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

/// Convert nanoseconds since Unix epoch to an ISO 8601 string.
///
/// Returns a string like `2024-01-25T20:43:41.000000000`.
#[must_use]
pub fn nanos_to_iso8601_string(nanos: u128) -> String {
    let format_max_timestamp = || {
        tracing::warn!(
            "Timestamp value {nanos}ns exceeds chrono range; saturating ISO 8601 filter literal to the maximum representable UTC timestamp"
        );
        chrono::DateTime::<chrono::Utc>::MAX_UTC
            .format("%Y-%m-%dT%H:%M:%S%.9f")
            .to_string()
    };

    let Ok(secs) = i64::try_from(nanos / 1_000_000_000) else {
        return format_max_timestamp();
    };
    let Ok(subsec_nanos) = u32::try_from(nanos % 1_000_000_000) else {
        unreachable!("nanosecond remainder always fits in u32")
    };

    let Some(datetime) = chrono::DateTime::from_timestamp(secs, subsec_nanos) else {
        return format_max_timestamp();
    };

    datetime.format("%Y-%m-%dT%H:%M:%S%.9f").to_string()
}

/// Parse an ISO 8601 string back to nanoseconds since Unix epoch.
///
/// Tries RFC 3339 first, then `%Y-%m-%dT%H:%M:%S%.f` (no timezone).
#[expect(clippy::cast_sign_loss)]
#[must_use]
pub fn parse_iso8601_to_nanos(s: &str) -> Option<u128> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_nanos_opt()? as u128);
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc().timestamp_nanos_opt()? as u128);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::datasource::{DefaultTableSource, TableProvider, empty::EmptyTable};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::sql::unparser::Unparser;

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
    fn test_preserves_mixed_case_column_when_unparsed() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_Timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]));
        let table_provider: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&schema)));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let converter = TimestampFilterConvert::new(
            "col_Timestamp".to_string(),
            TimestampFormat::Timestamptz(TimeUnit::Microsecond, None),
            None,
            None,
        );
        let filter = converter.convert(1_620_000_000_000_000_000, Operator::Gt);

        let plan = LogicalPlanBuilder::scan("test_table", table_source, None)
            .expect("logical scan should be created")
            .filter(filter)
            .expect("filter should be applied")
            .build()
            .expect("logical plan should be built");
        let sql = Unparser::default()
            .plan_to_sql(&plan)
            .expect("logical plan should unparse")
            .to_string();

        assert!(
            sql.contains(r#""col_Timestamp""#),
            "mixed-case timestamp column should stay quoted in SQL, got: {sql}"
        );
        assert!(
            !sql.contains("col_timestamp"),
            "mixed-case timestamp column must not be normalized to lowercase, got: {sql}"
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
        // Literal is built in the column's own unit (seconds) — no CAST.
        test_convert(
            &DataType::Timestamp(TimeUnit::Second, None),
            None,
            1_620_000_000_000_000_000,
            "timestamp > TimestampSecond(1620000000, None)",
        );
    }

    #[test]
    fn test_timestamp_microsecond_matches_column_unit() {
        // Regression for the interval-assertion crash: a µs column must yield a
        // µs literal (not `CAST(col AS ns) > ns_literal`), so statistics
        // analysis never performs a lossy cross-unit cast that inverts bounds.
        test_convert(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            None,
            1_620_000_000_000_000_000,
            "timestamp > TimestampMicrosecond(1620000000000000, None)",
        );
    }

    #[test]
    fn test_timestamp_millisecond_matches_column_unit() {
        test_convert(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            None,
            1_620_000_000_000_000_000,
            "timestamp > TimestampMillisecond(1620000000000, None)",
        );
    }

    #[test]
    fn test_utf8_iso8601() {
        test_convert(
            &DataType::Utf8,
            None,
            1_620_000_000_000_000_000,
            r#"timestamp > Utf8("2021-05-03T00:00:00.000000000")"#,
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
            r#"timestamp > TimestampNanosecond(1620000000000000000, Some("UTC"))"#,
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
            "timestamp > UInt64(1620000000000) AND partition_ts > TimestampSecond(1620000000, None)",
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

    #[test]
    fn test_nanos_to_iso8601_string() {
        assert_eq!(
            nanos_to_iso8601_string(1_620_000_000_000_000_000),
            "2021-05-03T00:00:00.000000000"
        );
        assert_eq!(
            nanos_to_iso8601_string(1_706_215_421_123_456_789),
            "2024-01-25T20:43:41.123456789"
        );
    }

    #[test]
    fn test_nanos_to_iso8601_string_out_of_range_does_not_truncate() {
        assert_eq!(
            nanos_to_iso8601_string(u128::MAX),
            chrono::DateTime::<chrono::Utc>::MAX_UTC
                .format("%Y-%m-%dT%H:%M:%S%.9f")
                .to_string()
        );
    }

    #[test]
    fn test_parse_iso8601_to_nanos() {
        // RFC 3339 with timezone
        assert_eq!(
            parse_iso8601_to_nanos("2021-05-03T00:00:00+00:00"),
            Some(1_620_000_000_000_000_000)
        );
        // Naive format without timezone
        assert_eq!(
            parse_iso8601_to_nanos("2021-05-03T00:00:00.000000000"),
            Some(1_620_000_000_000_000_000)
        );
        // With subsecond precision
        assert_eq!(
            parse_iso8601_to_nanos("2024-01-25T20:43:41.123456789"),
            Some(1_706_215_421_123_456_789)
        );
        // Invalid string
        assert_eq!(parse_iso8601_to_nanos("not-a-date"), None);
    }

    #[test]
    fn test_nanos_roundtrip() {
        let original: u128 = 1_706_215_421_123_456_789;
        let iso = nanos_to_iso8601_string(original);
        let parsed = parse_iso8601_to_nanos(&iso).expect("roundtrip should succeed");
        assert_eq!(original, parsed);
    }
}
