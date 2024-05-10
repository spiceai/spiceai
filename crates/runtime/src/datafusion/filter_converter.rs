use arrow::datatypes::DataType;
use datafusion::{
    logical_expr::{binary_expr, cast, col, lit, Expr, Operator},
    scalar::ScalarValue,
};
use spicepod::component::dataset::TimeFormat;

#[derive(Debug, Clone)]
enum ExprTimeFormat {
    ISO8601,
    UnixTimestamp(ExprUnixTimestamp),
    Timestamp,
}

#[derive(Debug, Clone)]
struct ExprUnixTimestamp {
    scale: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct TimestampFilterConvert {
    time_column: String,
    time_format: ExprTimeFormat,
}

#[allow(clippy::needless_pass_by_value)]
impl TimestampFilterConvert {
    pub(crate) fn create(
        field: Option<&arrow::datatypes::Field>,
        time_column: Option<String>,
        mut time_format: Option<TimeFormat>,
    ) -> Option<Self> {
        let field = field?;
        let time_column = time_column?;

        let time_format = match field.data_type() {
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
                let mut scale = 1;
                if let Some(time_format) = time_format.take() {
                    if time_format == TimeFormat::UnixMillis {
                        scale = 1000;
                    }
                }
                ExprTimeFormat::UnixTimestamp(ExprUnixTimestamp { scale })
            }
            DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_) => ExprTimeFormat::Timestamp,
            DataType::Utf8 | DataType::LargeUtf8 => ExprTimeFormat::ISO8601,
            _ => {
                tracing::warn!("Date type is not handled yet: {}", field.data_type());
                return None;
            }
        };

        Some(Self {
            time_column,
            time_format,
        })
    }

    #[allow(clippy::cast_possible_wrap)]
    pub(crate) fn convert(&self, timestamp: u64, op: Operator) -> Expr {
        let time_column: &str = self.time_column.as_ref();
        match &self.time_format {
            ExprTimeFormat::ISO8601 => binary_expr(
                cast(
                    col(time_column),
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                ),
                op,
                Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some((timestamp * 1000) as i64),
                    None,
                )),
            ),
            ExprTimeFormat::UnixTimestamp(format) => {
                binary_expr(col(time_column), op, lit(timestamp * format.scale))
            }
            ExprTimeFormat::Timestamp => binary_expr(
                col(time_column),
                op,
                Expr::Literal(ScalarValue::TimestampMillisecond(
                    Some((timestamp * 1000) as i64),
                    None,
                )),
            ),
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
            &Field::new("timestamp", DataType::Int64, false),
            TimeFormat::UnixMillis,
            1_620_000_000,
            "timestamp > UInt64(1620000000000)",
        );
        test(
            &Field::new("timestamp", DataType::Int64, false),
            TimeFormat::UnixSeconds,
            1_620_000_000,
            "timestamp > UInt64(1620000000)",
        );
        test(
            &Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000,
            "timestamp > TimestampMillisecond(1620000000000, None)",
        );
        test(
            &Field::new(
                "timestamp",
                DataType::Utf8,
                false,
            ),
            TimeFormat::UnixSeconds,
            1_620_000_000,
            "CAST(timestamp AS Timestamp(Millisecond, None)) > TimestampMillisecond(1620000000000, None)",
        );
    }

    fn test(field: &Field, time_format: TimeFormat, timestamp: u64, expected: &str) {
        let time_column = "timestamp".to_string();
        let timestamp_filter_convert =
            TimestampFilterConvert::create(Some(field), Some(time_column), Some(time_format));
        assert!(timestamp_filter_convert.is_some());
        let timestamp_filter_convert =
            timestamp_filter_convert.expect("the convert can be created");
        let expr = timestamp_filter_convert.convert(timestamp, Operator::Gt);
        assert_eq!(expr.to_string(), expected);
    }
}
