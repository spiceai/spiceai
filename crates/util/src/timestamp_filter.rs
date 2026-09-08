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
    logical_expr::{Expr, Operator, binary_expr, cast, lit},
    prelude::{and, ident},
    scalar::ScalarValue,
};
use std::sync::Arc;

use crate::timezone;

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
        TimestampFormat::Timestamptz(tz) => {
            // The cast carries the accelerator schema's timezone spelling, and the
            // DuckDB unparser renders `CAST(col AS Timestamp(ns, tz))` as
            // `col AT TIME ZONE '<tz>'`, which DuckDB resolves through ICU — named
            // zones only. Iceberg spells every `timestamptz` as the fixed offset
            // `+00:00`, so carrying that spelling through unchanged gets the whole
            // filter rejected with `Unknown TimeZone '+00:00'`, leaving the dataset
            // stuck retrying a refresh that can never bind (#12528).
            //
            // Naming the same zone in a spelling every engine knows leaves the
            // comparison unchanged. A non-UTC offset is left alone: it denotes a
            // different zone, so rewriting it would move the comparison.
            let tz = tz.as_ref().map(|tz| {
                if timezone::is_utc(tz) {
                    Arc::from(timezone::CANONICAL_UTC)
                } else {
                    Arc::clone(tz)
                }
            });

            binary_expr(
                cast(
                    ident(time_column),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, tz.clone()),
                ),
                op,
                Expr::Literal(
                    ScalarValue::TimestampNanosecond(Some(timestamp_in_nanos as i64), tz),
                    None,
                ),
            )
        }
    }
}

/// Whether an Arrow `DataType` can only represent whole days.
///
/// `Date32` counts days since the epoch, and Arrow requires every `Date64` value to
/// be an exact multiple of `86_400_000` ms, so both cast to midnight. A value read out
/// of such a column therefore carries no information about the time of day.
///
/// This is deliberately keyed on the `DataType` rather than on [`TimestampFormat`]:
/// `Date64` shares the [`TimestampFormat::Timestamp`] arm with `Time32`/`Time64`,
/// which are legitimately sub-day.
#[must_use]
pub fn is_day_granular(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
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

    /// Whether `time_column`'s Arrow type is day-granular ([`is_day_granular`]).
    /// Only consulted by [`TimestampFilterConvert::convert_high_water_mark`].
    time_is_day_granular: bool,
    /// Whether `time_partition_column`'s Arrow type is day-granular.
    time_partition_is_day_granular: bool,
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
            time_is_day_granular: false,
            time_partition_is_day_granular: false,
        }
    }

    /// Record whether each column's Arrow type is day-granular, as reported by
    /// [`is_day_granular`].
    ///
    /// Both default to `false`, which keeps
    /// [`convert_high_water_mark`](TimestampFilterConvert::convert_high_water_mark)
    /// on strict `>` for every column; set them whenever the source Arrow
    /// `DataType`s are available.
    ///
    /// When two converters are used against the two sides of one high-water
    /// comparison — a source filter and a dedupe filter — they must be given the
    /// *same* flags. Deriving them separately from each side's own schema lets the
    /// two disagree, which widens one side only: the already-loaded rows then fall
    /// outside the dedupe's comparison set and the re-fetched rows are appended
    /// again on every refresh.
    #[must_use]
    pub fn with_day_granular_columns(
        mut self,
        time_is_day_granular: bool,
        time_partition_is_day_granular: bool,
    ) -> Self {
        self.time_is_day_granular = time_is_day_granular;
        self.time_partition_is_day_granular = time_partition_is_day_granular;
        self
    }

    /// Build a filter expression for the given timestamp (in nanoseconds).
    ///
    /// If a partition column is configured, the result is
    /// `time_expr AND partition_expr`.
    #[must_use]
    pub fn convert(&self, timestamp_in_nanos: u128, op: Operator) -> Expr {
        let bound = Bound {
            timestamp_in_nanos,
            op,
        };
        self.convert_with_bounds(bound, bound)
    }

    /// Build a filter expression selecting rows *after* an append high-water mark
    /// that was itself read out of one of these columns.
    ///
    /// A day-granular column (`Date32`/`Date64`) is compared inclusively against the
    /// **start of the mark's day**. Every one of its values casts to midnight, so
    /// once any row of day *D* is loaded the mark is *D*-midnight and a strict `>`
    /// excludes every other row of day *D* — they are skipped on each later refresh
    /// and lost for good once the source advances past *D*. Callers must pair this
    /// with an exact-row dedupe against the already-loaded rows, widened the same
    /// way, so the rows that come back are dropped instead of appended twice.
    ///
    /// Flooring to the day is what makes a day-granular *partition* column work when
    /// the time column itself is sub-day: the mark then carries a time of day, and
    /// `CAST(day AS Timestamp) >= <midday>` is false for the very partition the new
    /// rows live in. Flooring is a no-op when the mark came from a day-granular
    /// column, since it is already midnight.
    ///
    /// Sub-day columns (`Time32`, `Time64`, `Timestamp`) keep the strict `>` against
    /// the exact mark: they carry the precision that makes it correct.
    #[must_use]
    pub fn convert_high_water_mark(&self, timestamp_in_nanos: u128) -> Expr {
        self.convert_with_bounds(
            high_water_mark_bound(timestamp_in_nanos, self.time_is_day_granular),
            high_water_mark_bound(timestamp_in_nanos, self.time_partition_is_day_granular),
        )
    }

    /// Build the filter expression, applying a separate comparison to the time column
    /// and to the partition column.
    fn convert_with_bounds(&self, time: Bound, time_partition: Bound) -> Expr {
        let time_expr = convert_timestamp_expr(
            time.timestamp_in_nanos,
            &self.time_column,
            &self.time_format,
            time.op,
        );
        match (&self.time_partition_column, &self.time_partition_format) {
            (Some(time_partition_column), Some(time_partition_format)) => {
                let time_partition_expr = convert_timestamp_expr(
                    time_partition.timestamp_in_nanos,
                    time_partition_column,
                    time_partition_format,
                    time_partition.op,
                );
                and(time_expr, time_partition_expr)
            }
            _ => time_expr,
        }
    }
}

/// One column's half of a filter: the literal to compare against, and the operator.
#[derive(Clone, Copy)]
struct Bound {
    timestamp_in_nanos: u128,
    op: Operator,
}

/// Nanoseconds in a UTC day. Arrow dates carry no leap seconds, so this is exact.
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

/// Round a mark down to midnight UTC of its own day.
///
/// Done in **signed** space. A mark taken from an Arrow timestamp arrives here as
/// `i64 as u128`, so a pre-1970 value is a wrapped two's-complement number — it is only
/// meaningful again after the `as i64` that builds the literal. Flooring the wrapped
/// value with an unsigned modulo would produce a bound unrelated to the mark's day
/// (`1969-12-31T23:00Z` floors to some instant late on the 30th), and the day's own
/// partition would then fail its predicate. `rem_euclid` is never negative, so
/// subtracting it always rounds toward negative infinity.
/// `saturating_sub` because the plain subtraction underflows near `i64::MIN`: `rem_euclid`
/// is non-negative, so subtracting it from a mark within one day of the floor of the range
/// would panic in debug and wrap in release. Saturating clamps the bound to `i64::MIN`,
/// which for an inclusive `>=` comparison simply admits everything — safe, and the mark
/// would have to sit before 1678 to get there at all.
#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn floor_to_day(timestamp_in_nanos: u128) -> u128 {
    let signed = timestamp_in_nanos as i64;
    signed.saturating_sub(signed.rem_euclid(NANOS_PER_DAY)) as u128
}

/// The comparison a high-water mark implies for one column.
///
/// Day-granular columns compare `>=` against midnight of the mark's day; everything
/// else compares `>` against the mark itself. See
/// [`convert_high_water_mark`](TimestampFilterConvert::convert_high_water_mark).
fn high_water_mark_bound(timestamp_in_nanos: u128, is_day_granular: bool) -> Bound {
    if is_day_granular {
        Bound {
            timestamp_in_nanos: floor_to_day(timestamp_in_nanos),
            op: Operator::GtEq,
        }
    } else {
        Bound {
            timestamp_in_nanos,
            op: Operator::Gt,
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
            TimestampFormat::Timestamptz(None),
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
            r#"CAST(timestamp AS Timestamp(ns, "UTC")) > TimestampNanosecond(1620000000000000000, Some("UTC"))"#,
        );
    }

    /// Builds the filter for a `Timestamp(ns, tz)` column and returns it rendered.
    fn convert_with_timezone(tz: &str) -> String {
        let format = data_type_to_timestamp_format(
            &DataType::Timestamp(TimeUnit::Nanosecond, Some(tz.into())),
            None,
        )
        .expect("should resolve");
        let converter = TimestampFilterConvert::new("timestamp".to_string(), format, None, None);
        converter
            .convert(1_620_000_000_000_000_000, Operator::Gt)
            .to_string()
    }

    /// Regression test for #12528. Iceberg maps every `timestamptz` to the fixed
    /// offset `+00:00`, which the `DuckDB` unparser renders as
    /// `AT TIME ZONE '+00:00'` — a spelling `DuckDB`'s ICU resolver rejects, so the
    /// filter has to name the zone in a form every engine accepts.
    #[test]
    fn a_fixed_offset_utc_column_is_named_utc() {
        let expected = r#"CAST(timestamp AS Timestamp(ns, "UTC")) > TimestampNanosecond(1620000000000000000, Some("UTC"))"#;

        for tz in ["+00:00", "-00:00", "+0000", "+00"] {
            assert_eq!(convert_with_timezone(tz), expected, "{tz} denotes UTC");
        }
        for tz in ["Z", "GMT", "Etc/UTC", "utc"] {
            assert_eq!(convert_with_timezone(tz), expected, "{tz} denotes UTC");
        }
    }

    /// A non-UTC zone must survive untouched — rewriting it would move the
    /// comparison to a different instant.
    #[test]
    fn a_non_utc_timezone_is_left_alone() {
        assert_eq!(
            convert_with_timezone("America/New_York"),
            r#"CAST(timestamp AS Timestamp(ns, "America/New_York")) > TimestampNanosecond(1620000000000000000, Some("America/New_York"))"#,
        );
        assert_eq!(
            convert_with_timezone("-05:00"),
            r#"CAST(timestamp AS Timestamp(ns, "-05:00")) > TimestampNanosecond(1620000000000000000, Some("-05:00"))"#,
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

    /// 2021-05-03T00:00:00Z, the instant the other tests in this module use.
    const TS_NANOS: u128 = 1_620_000_000_000_000_000;
    /// 2021-05-03T12:00:00Z — the same day, but with a time of day.
    const TS_MIDDAY_NANOS: u128 = 1_620_043_200_000_000_000;
    /// Expected high-water-mark filter for a day-granular column named `ts`.
    const TS_INCLUSIVE: &str =
        "CAST(ts AS Timestamp(ns)) >= TimestampNanosecond(1620000000000000000, None)";
    /// Expected high-water-mark filter for a sub-day column named `ts`.
    const TS_STRICT: &str =
        "CAST(ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)";

    /// Build the high-water-mark filter for a single column of `data_type`.
    fn high_water_mark_expr(data_type: &DataType) -> String {
        let format = data_type_to_timestamp_format(data_type, None).expect("format resolves");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None)
            .with_day_granular_columns(is_day_granular(data_type), false);
        converter.convert_high_water_mark(TS_NANOS).to_string()
    }

    #[test]
    fn test_is_day_granular() {
        assert!(is_day_granular(&DataType::Date32));
        // Arrow requires Date64 values to be exact multiples of 86_400_000ms, so it is
        // day-granular too — even though it shares a TimestampFormat with Time32/Time64.
        assert!(is_day_granular(&DataType::Date64));

        assert!(!is_day_granular(&DataType::Time32(TimeUnit::Second)));
        assert!(!is_day_granular(&DataType::Time64(TimeUnit::Nanosecond)));
        assert!(!is_day_granular(&DataType::Utf8));
        assert!(!is_day_granular(&DataType::Int64));

        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert!(!is_day_granular(&timestamp));
    }

    #[test]
    fn test_high_water_mark_is_inclusive_for_day_granular_types() {
        // Every value casts to midnight, so a strict `>` against a high-water mark drawn
        // from day D skips every other row of day D, permanently (#12492).
        assert_eq!(high_water_mark_expr(&DataType::Date32), TS_INCLUSIVE);
        assert_eq!(high_water_mark_expr(&DataType::Date64), TS_INCLUSIVE);
    }

    #[test]
    fn test_high_water_mark_stays_strict_for_sub_day_types() {
        // Time32/Time64 share TimestampFormat::Timestamp with Date64 but carry sub-day
        // precision, so widening them would re-read rows that are already loaded.
        let time32 = DataType::Time32(TimeUnit::Second);
        let time64 = DataType::Time64(TimeUnit::Nanosecond);
        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, None);

        assert_eq!(high_water_mark_expr(&time32), TS_STRICT);
        assert_eq!(high_water_mark_expr(&time64), TS_STRICT);
        assert_eq!(high_water_mark_expr(&timestamp), TS_STRICT);
        assert_eq!(
            high_water_mark_expr(&DataType::Utf8),
            r#"ts > Utf8("2021-05-03T00:00:00.000000000")"#,
        );
    }

    #[test]
    fn test_high_water_mark_defaults_to_strict() {
        // Without day-granularity information the converter keeps the old behaviour.
        let format = data_type_to_timestamp_format(&DataType::Date32, None).expect("resolves");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None);
        assert_eq!(
            converter.convert_high_water_mark(TS_NANOS).to_string(),
            TS_STRICT,
        );
    }

    /// Build a converter for a sub-day `ts` column partitioned by a day-granular `day`.
    fn sub_day_time_with_day_partition() -> TimestampFilterConvert {
        let time_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let time_format =
            data_type_to_timestamp_format(&time_type, None).expect("time format resolves");
        let partition_format = data_type_to_timestamp_format(&DataType::Date32, None)
            .expect("partition format resolves");

        TimestampFilterConvert::new(
            "ts".to_string(),
            time_format,
            Some("day".to_string()),
            Some(partition_format),
        )
        .with_day_granular_columns(false, true)
    }

    #[test]
    fn test_high_water_mark_widens_a_day_partition_independently() {
        // A sub-day time column partitioned by day: the partition predicate is ANDed on,
        // so leaving it strict would exclude the whole current-day partition and stall
        // the append just as surely as a day-granular time column would.
        assert_eq!(
            sub_day_time_with_day_partition()
                .convert_high_water_mark(TS_NANOS)
                .to_string(),
            "CAST(ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None) AND CAST(day AS Timestamp(ns)) >= TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_high_water_mark_floors_a_day_partition_to_midnight() {
        // The regression `>=` alone does not fix: with a sub-day time column the mark
        // carries a time of day, and every `day` value casts to midnight — so
        // `CAST(day) >= <midday>` is false for the partition the new rows are in and the
        // whole current day stays excluded. The partition literal has to be floored to
        // the start of the mark's day. Not `>=` at midday: 2021-05-03T12:00Z.
        assert_eq!(
            sub_day_time_with_day_partition()
                .convert_high_water_mark(TS_MIDDAY_NANOS)
                .to_string(),
            "CAST(ts AS Timestamp(ns)) > TimestampNanosecond(1620043200000000000, None) AND CAST(day AS Timestamp(ns)) >= TimestampNanosecond(1620000000000000000, None)",
        );
    }

    #[test]
    fn test_high_water_mark_floors_a_pre_1970_mark_in_signed_space() {
        // 1969-12-31T23:00:00Z as the runtime delivers it: the mark is an `i64` epoch
        // value widened with `as u128`, so a negative one arrives two's-complement
        // wrapped and only means anything again after the `as i64` that builds the
        // literal. Flooring the wrapped value with an unsigned modulo would land on an
        // unrelated instant, and the mark's own day would fail its partition predicate.
        let mark = u128::MAX - 3_600_000_000_000 + 1;

        let format =
            data_type_to_timestamp_format(&DataType::Date32, None).expect("format resolves");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None)
            .with_day_granular_columns(true, false);

        // Midnight on 1969-12-31 — not some instant on the 30th.
        assert_eq!(
            converter.convert_high_water_mark(mark).to_string(),
            "CAST(ts AS Timestamp(ns)) >= TimestampNanosecond(-86400000000000, None)",
        );
    }

    #[test]
    fn test_high_water_mark_floors_an_extreme_negative_mark_without_underflowing() {
        // The two's-complement wrap of i64::MIN, written without a signed cast: the floor of
        // a mark this low is less than i64::MIN, so subtracting the remainder outright would
        // panic in debug and wrap in release. It saturates instead, and an inclusive bound at
        // the bottom of the range simply admits every row.
        let mark = u128::MAX - (1u128 << 63) + 1;

        let format =
            data_type_to_timestamp_format(&DataType::Date32, None).expect("format resolves");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None)
            .with_day_granular_columns(true, false);

        assert_eq!(
            converter.convert_high_water_mark(mark).to_string(),
            "CAST(ts AS Timestamp(ns)) >= TimestampNanosecond(-9223372036854775808, None)",
        );
    }

    #[test]
    fn test_high_water_mark_floors_a_day_granular_time_column_too() {
        // A day-granular time column's own mark is already midnight, so flooring is
        // normally a no-op — except with `append_overlap`, which subtracts a duration and
        // can leave the mark mid-day. Flooring keeps the comparison on a day boundary
        // there rather than excluding the day the mark now falls inside.
        let format =
            data_type_to_timestamp_format(&DataType::Date32, None).expect("format resolves");
        let converter = TimestampFilterConvert::new("ts".to_string(), format, None, None)
            .with_day_granular_columns(true, false);

        assert_eq!(
            converter
                .convert_high_water_mark(TS_MIDDAY_NANOS)
                .to_string(),
            TS_INCLUSIVE,
        );
    }

    #[test]
    fn test_convert_still_applies_one_operator_to_every_column() {
        // `convert` also builds the wall-clock refresh_data_window filter, which must
        // keep taking its operator verbatim.
        let time_format =
            data_type_to_timestamp_format(&DataType::Date32, None).expect("time format resolves");
        let partition_format = data_type_to_timestamp_format(&DataType::Date32, None)
            .expect("partition format resolves");
        let converter = TimestampFilterConvert::new(
            "ts".to_string(),
            time_format,
            Some("day".to_string()),
            Some(partition_format),
        )
        .with_day_granular_columns(true, true);

        assert_eq!(
            converter.convert(TS_NANOS, Operator::Gt).to_string(),
            "CAST(ts AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None) AND CAST(day AS Timestamp(ns)) > TimestampNanosecond(1620000000000000000, None)",
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
