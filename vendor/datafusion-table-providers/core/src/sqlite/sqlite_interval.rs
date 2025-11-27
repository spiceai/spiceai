use datafusion::error::DataFusionError;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArgumentList, Ident,
    ObjectNamePart, VisitorMut,
};
use std::fmt::Display;
use std::ops::ControlFlow;
use std::str::FromStr;

#[derive(Default)]
pub struct SQLiteIntervalVisitor {}

#[derive(Default, Debug)]
struct IntervalParts {
    years: i64,
    months: i64,
    days: i64,
    hours: i64,
    minutes: i64,
    seconds: i64,
    nanos: u32,
}

enum SQLiteIntervalType {
    Date,
    Datetime,
}

impl Display for SQLiteIntervalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SQLiteIntervalType::Date => write!(f, "date"),
            SQLiteIntervalType::Datetime => write!(f, "datetime"),
        }
    }
}

type IntervalSetter = fn(IntervalParts, i64) -> IntervalParts;

impl IntervalParts {
    fn new() -> Self {
        Self::default()
    }

    fn intraday(&self) -> bool {
        self.hours > 0 || self.minutes > 0 || self.seconds > 0 || self.nanos > 0
    }

    fn negate(mut self) -> Self {
        self.years = -self.years;
        self.months = -self.months;
        self.days = -self.days;
        self.hours = -self.hours;
        self.minutes = -self.minutes;
        self.seconds = -self.seconds;
        self
    }

    fn with_years(mut self, years: i64) -> Self {
        self.years = years;
        self
    }

    fn with_months(mut self, months: i64) -> Self {
        self.months = months;
        self
    }

    fn with_days(mut self, days: i64) -> Self {
        self.days = days;
        self
    }

    fn with_hours(mut self, hours: i64) -> Self {
        self.hours = hours;
        self
    }

    fn with_minutes(mut self, minutes: i64) -> Self {
        self.minutes = minutes;
        self
    }

    fn with_seconds(mut self, seconds: i64) -> Self {
        self.seconds = seconds;
        self
    }

    fn with_nanos(mut self, nanos: u32) -> Self {
        self.nanos = nanos;
        self
    }
}

impl VisitorMut for SQLiteIntervalVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // for each INTERVAL, find the previous (or next, if the INTERVAL is first) expression or column name that is associated with it
        // e.g. `column_name + INTERVAL '1' DAY``, we should find the `column_name`
        // then replace the `INTERVAL` with e.g. `datetime(column_name, '+1 day')`
        // this should also apply to expressions though, like `CAST(column_name AS TEXT) + INTERVAL '1' DAY`
        // in this example, it would be replaced with `datetime(CAST(column_name AS TEXT), '+1 day')`

        // TODO: figure out nested BinaryOp, e.g. `column_name + INTERVAL '1' DAY + INTERVAL '1' DAY`
        if let Expr::BinaryOp { op, left, right } = expr {
            if *op != BinaryOperator::Plus && *op != BinaryOperator::Minus {
                return ControlFlow::Continue(());
            }

            let (target, interval) = SQLiteIntervalVisitor::normalize_interval_expr(left, right);

            if let Expr::Interval(_) = interval.as_ref() {
                // parse the INTERVAL and get the bits out of it
                // e.g. INTERVAL 0 YEARS 0 MONS 1 DAYS 0 HOURS 0 MINUTES 0.000000000 SECS -> IntervalParts { days: 1 }
                if let Ok(interval_parts) = SQLiteIntervalVisitor::parse_interval(interval) {
                    // negate the interval parts if the operator is minus
                    let interval_parts = if *op == BinaryOperator::Minus {
                        interval_parts.negate()
                    } else {
                        interval_parts
                    };

                    *expr =
                        SQLiteIntervalVisitor::create_datetime_function(target, &interval_parts);
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl SQLiteIntervalVisitor {
    // normalize the sides of the operation to make sure the INTERVAL is always on the right
    fn normalize_interval_expr<'a>(
        left: &'a mut Box<Expr>,
        right: &'a mut Box<Expr>,
    ) -> (&'a mut Box<Expr>, &'a mut Box<Expr>) {
        if let Expr::Interval { .. } = left.as_ref() {
            (right, left)
        } else {
            (left, right)
        }
    }

    fn parse_interval(interval: &Expr) -> Result<IntervalParts, DataFusionError> {
        if let Expr::Interval(interval_expr) = interval {
            if let Expr::Value(ast::ValueWithSpan {
                value: ast::Value::SingleQuotedString(value),
                span: _,
            }) = interval_expr.value.as_ref()
            {
                return SQLiteIntervalVisitor::parse_interval_string(value);
            }
        }
        Err(DataFusionError::Plan(
            "Invalid interval expression".to_string(),
        ))
    }

    fn parse_interval_string(value: &str) -> Result<IntervalParts, DataFusionError> {
        let mut parts = IntervalParts::new();
        let mut remaining = value;

        let components: [(_, IntervalSetter); 5] = [
            ("YEARS", IntervalParts::with_years),
            ("MONS", IntervalParts::with_months),
            ("DAYS", IntervalParts::with_days),
            ("HOURS", IntervalParts::with_hours),
            ("MINS", IntervalParts::with_minutes),
        ];

        for (unit, setter) in &components {
            if let Some((value, rest)) = remaining.split_once(unit) {
                let parsed_value: i64 = SQLiteIntervalVisitor::parse_value(value.trim())?;
                parts = setter(parts, parsed_value);
                remaining = rest;
            }
        }

        // Parse seconds and nanoseconds separately
        if let Some((secs, _)) = remaining.split_once("SECS") {
            let (seconds, nanos) = SQLiteIntervalVisitor::parse_seconds_and_nanos(secs.trim())?;
            parts = parts.with_seconds(seconds).with_nanos(nanos);
        }

        Ok(parts)
    }

    fn parse_seconds_and_nanos(value: &str) -> Result<(i64, u32), DataFusionError> {
        let parts: Vec<&str> = value.split('.').collect();
        let seconds = SQLiteIntervalVisitor::parse_value(parts[0])?;
        let nanos = if parts.len() > 1 {
            let nanos_str = format!("{:0<9}", parts[1]);
            nanos_str[..9].parse().map_err(|_| {
                DataFusionError::Plan(format!("Failed to parse nanoseconds: {}", parts[1]))
            })?
        } else {
            0
        };
        Ok((seconds, nanos))
    }

    fn parse_value<T: FromStr>(value: &str) -> Result<T, DataFusionError> {
        value
            .parse()
            .map_err(|_| DataFusionError::Plan(format!("Failed to parse interval value: {value}")))
    }

    fn create_datetime_function(target: &Expr, interval: &IntervalParts) -> Expr {
        let interval_date_type = if interval.intraday() {
            SQLiteIntervalType::Datetime
        } else {
            SQLiteIntervalType::Date
        };

        let function_args = vec![
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(target.clone()))),
            SQLiteIntervalVisitor::create_interval_arg("years", interval.years),
            SQLiteIntervalVisitor::create_interval_arg("months", interval.months),
            SQLiteIntervalVisitor::create_interval_arg("days", interval.days),
            SQLiteIntervalVisitor::create_interval_arg("hours", interval.hours),
            SQLiteIntervalVisitor::create_interval_arg("minutes", interval.minutes),
            SQLiteIntervalVisitor::create_interval_arg_with_fraction(
                "seconds",
                interval.seconds,
                interval.nanos,
            ),
        ]
        .into_iter()
        .flatten() // flatten the list of arguments to exclude 0 values
        .collect();

        let datetime_function = Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                interval_date_type.to_string(),
            ))]),
            args: ast::FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: function_args,
                clauses: Vec::new(),
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: Vec::new(),
            parameters: ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        });

        Expr::Cast {
            expr: Box::new(datetime_function),
            data_type: ast::DataType::Text,
            format: None,
            kind: ast::CastKind::Cast,
        }
    }

    fn create_interval_arg(unit: &str, value: i64) -> Option<FunctionArg> {
        if value == 0 {
            None
        } else {
            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                ast::Value::SingleQuotedString(format!("{value:+} {unit}")),
            ))))
        }
    }

    fn create_interval_arg_with_fraction(
        unit: &str,
        value: i64,
        fraction: u32,
    ) -> Option<FunctionArg> {
        if value == 0 && fraction == 0 {
            None
        } else {
            let fraction_str = if fraction > 0 {
                format!(".{fraction:09}")
            } else {
                String::new()
            };

            Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                ast::Value::SingleQuotedString(format!("{value:+}{fraction_str} {unit}")),
            ))))
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_interval_parts_parse() {
        let parts = SQLiteIntervalVisitor::parse_interval_string(
            "0 YEARS 0 MONS 1 DAYS 0 HOURS 0 MINS 0.000000000 SECS",
        )
        .expect("interval parts should be parsed");

        assert_eq!(parts.years, 0);
        assert_eq!(parts.months, 0);
        assert_eq!(parts.days, 1);
        assert_eq!(parts.hours, 0);
        assert_eq!(parts.minutes, 0);
        assert_eq!(parts.seconds, 0);
        assert_eq!(parts.nanos, 0);
    }

    #[test]
    fn test_interval_parts_parse_with_nanos() {
        let parts = SQLiteIntervalVisitor::parse_interval_string(
            "0 YEARS 0 MONS 0 DAYS 0 HOURS 0 MINS 0.000000001 SECS",
        )
        .expect("interval parts should be parsed");

        assert_eq!(parts.years, 0);
        assert_eq!(parts.months, 0);
        assert_eq!(parts.days, 0);
        assert_eq!(parts.hours, 0);
        assert_eq!(parts.minutes, 0);
        assert_eq!(parts.seconds, 0);
        assert_eq!(parts.nanos, 1);
    }

    #[test]
    fn test_interval_parts_parse_negative() {
        let parts = SQLiteIntervalVisitor::parse_interval_string(
            "0 YEARS 0 MONS -1 DAYS 0 HOURS 0 MINS 0.000000000 SECS",
        )
        .expect("interval parts should be parsed");

        assert_eq!(parts.years, 0);
        assert_eq!(parts.months, 0);
        assert_eq!(parts.days, -1);
        assert_eq!(parts.hours, 0);
        assert_eq!(parts.minutes, 0);
        assert_eq!(parts.seconds, 0);
        assert_eq!(parts.nanos, 0);
    }

    #[test]
    fn test_interval_parts_parse_intraday() {
        let parts = SQLiteIntervalVisitor::parse_interval_string(
            "0 YEARS 0 MONS 0 DAYS 1 HOURS 1 MINS 1.000000001 SECS",
        )
        .expect("interval parts should be parsed");

        assert_eq!(parts.years, 0);
        assert_eq!(parts.months, 0);
        assert_eq!(parts.days, 0);
        assert_eq!(parts.hours, 1);
        assert_eq!(parts.minutes, 1);
        assert_eq!(parts.seconds, 1);
        assert_eq!(parts.nanos, 1);

        assert!(parts.intraday());
    }

    #[test]
    fn test_interval_parts_parse_interday() {
        let parts = SQLiteIntervalVisitor::parse_interval_string(
            "0 YEARS 0 MONS 1 DAYS 0 HOURS 0 MINS 0.000000000 SECS",
        )
        .expect("interval parts should be parsed");

        assert_eq!(parts.years, 0);
        assert_eq!(parts.months, 0);
        assert_eq!(parts.days, 1);
        assert_eq!(parts.hours, 0);
        assert_eq!(parts.minutes, 0);
        assert_eq!(parts.seconds, 0);
        assert_eq!(parts.nanos, 0);

        assert!(!parts.intraday());
    }

    #[test]
    fn test_create_date_function() {
        let target = Expr::value(ast::Value::SingleQuotedString("1995-01-01".to_string()));
        let interval = IntervalParts::new()
            .with_years(1)
            .with_months(2)
            .with_days(3)
            .with_hours(0)
            .with_minutes(0)
            .with_seconds(0)
            .with_nanos(0);

        let datetime_function = SQLiteIntervalVisitor::create_datetime_function(&target, &interval);

        let expected = Expr::Cast {
            expr: Box::new(Expr::Function(ast::Function {
                name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("date"))]),
                args: ast::FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("1995-01-01".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+1 years".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+2 months".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+3 days".to_string()),
                        ))),
                    ],
                    clauses: Vec::new(),
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: Vec::new(),
                parameters: ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })),
            data_type: ast::DataType::Text,
            format: None,
            kind: ast::CastKind::Cast,
        };

        assert_eq!(datetime_function, expected);
    }

    #[test]
    fn test_create_datetime_function() {
        let target = Expr::value(ast::Value::SingleQuotedString("1995-01-01".to_string()));
        let interval = IntervalParts::new()
            .with_years(0)
            .with_months(0)
            .with_days(0)
            .with_hours(1)
            .with_minutes(2)
            .with_seconds(3)
            .with_nanos(0);

        let datetime_function = SQLiteIntervalVisitor::create_datetime_function(&target, &interval);

        let expected = Expr::Cast {
            expr: Box::new(Expr::Function(ast::Function {
                name: ast::ObjectName(vec![ObjectNamePart::Identifier(Ident::new("datetime"))]),
                args: ast::FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("1995-01-01".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+1 hours".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+2 minutes".to_string()),
                        ))),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::value(
                            ast::Value::SingleQuotedString("+3 seconds".to_string()),
                        ))),
                    ],
                    clauses: Vec::new(),
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: Vec::new(),
                parameters: ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })),
            data_type: ast::DataType::Text,
            format: None,
            kind: ast::CastKind::Cast,
        };

        assert_eq!(datetime_function, expected);
    }
}
