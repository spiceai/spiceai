use std::sync::Arc;

use bigdecimal::{num_bigint::BigInt, BigDecimal};
use datafusion::{
    logical_expr::{Cast, Expr},
    scalar::ScalarValue,
    sql::unparser::dialect::{
        DefaultDialect, Dialect, DuckDBDialect, MySqlDialect, PostgreSqlDialect, SqliteDialect,
    },
};

pub const SECONDS_IN_DAY: i32 = 86_400;

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Expression not supported {expr}"))]
    UnsupportedFilterExpr { expr: String },

    #[snafu(display("Engine {engine} not supported for expression {expr}"))]
    EngineNotSupportedForExpression { engine: String, expr: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug)]
pub enum Engine {
    Spark,
    SQLite,
    DuckDB,
    ODBC,
    Postgres,
    MySQL,
}

impl Engine {
    /// Get the corresponding `Dialect` to use for unparsing
    pub fn dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        match self {
            Engine::SQLite => Arc::new(SqliteDialect {}),
            Engine::Postgres => Arc::new(PostgreSqlDialect {}),
            Engine::MySQL => Arc::new(MySqlDialect {}),
            Engine::DuckDB => Arc::new(DuckDBDialect::new()),
            Engine::Spark | Engine::ODBC => Arc::new(DefaultDialect {}),
        }
    }
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::cast_precision_loss)]
pub fn to_sql_with_engine(expr: &Expr, engine: Option<Engine>) -> Result<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql_with_engine(&binary_expr.left, engine)?;
            let right = to_sql_with_engine(&binary_expr.right, engine)?;

            if let Some(Engine::DuckDB) = engine {
                // TODO: DuckDB doesn't support comparison between timestamp_s /timestamp_ms with timestampz as of v1
                // Revisit in future DuckDB versions
                if right.starts_with("TO_TIMESTAMP") && !left.starts_with("TO_TIMESTAMP") {
                    return Ok(format!(
                        "TO_TIMESTAMP(EPOCH_MS({}) / 1000) {} {}",
                        left, binary_expr.op, right
                    ));
                }
            }

            Ok(format!("{} {} {}", left, binary_expr.op, right))
        }
        Expr::Column(name) => match engine {
            Some(Engine::Spark | Engine::ODBC) => Ok(format!("{name}")),
            _ => Ok(format!("\"{name}\"")),
        },
        Expr::Cast(cast) => handle_cast(cast, engine, expr),
        Expr::Literal(value, _) => match value {
            ScalarValue::Date32(Some(value)) => match engine {
                Some(Engine::SQLite) => {
                    Ok(format!("date({}, 'unixepoch')", value * SECONDS_IN_DAY))
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value * SECONDS_IN_DAY)),
            },
            ScalarValue::Date64(Some(value)) => match engine {
                Some(Engine::SQLite) => Ok(format!(
                    "date({}, 'unixepoch')",
                    value * i64::from(SECONDS_IN_DAY)
                )),
                _ => Ok(format!(
                    "TO_TIMESTAMP({})",
                    value * i64::from(SECONDS_IN_DAY)
                )),
            },
            ScalarValue::Null => Ok(value.to_string()),
            ScalarValue::Int16(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int32(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int64(Some(value)) => Ok(value.to_string()),
            ScalarValue::Boolean(Some(value)) => Ok(value.to_string()),
            ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
                Ok(format!("'{value}'"))
            }
            ScalarValue::Float32(Some(value)) => Ok(value.to_string()),
            ScalarValue::Float64(Some(value)) => Ok(value.to_string()),
            ScalarValue::Int8(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt8(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt16(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt32(Some(value)) => Ok(value.to_string()),
            ScalarValue::UInt64(Some(value)) => Ok(value.to_string()),
            ScalarValue::TimestampNanosecond(Some(value), timezone) => match engine {
                Some(Engine::SQLite) => {
                    Ok(format!("datetime({}, 'unixepoch')", value / 1_000_000_000))
                }
                Some(Engine::Postgres) => {
                    if timezone.is_none() {
                        Ok(format!(
                            "TO_TIMESTAMP({}) AT TIME ZONE 'UTC'",
                            *value as f64 / 1_000_000_000.0
                        ))
                    } else {
                        Ok(format!("TO_TIMESTAMP({})", *value as f64 / 1_000_000_000.0))
                    }
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1_000_000_000)),
            },
            ScalarValue::TimestampMicrosecond(Some(value), timezone) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({}, 'unixepoch')", value / 1_000_000)),
                Some(Engine::Postgres) => {
                    if timezone.is_none() {
                        Ok(format!(
                            "TO_TIMESTAMP({}) AT TIME ZONE 'UTC'",
                            *value as f64 / 1_000_000.0
                        ))
                    } else {
                        Ok(format!("TO_TIMESTAMP({})", *value as f64 / 1_000_000.0))
                    }
                }
                Some(Engine::MySQL) => {
                    Ok(format!("FROM_UNIXTIME({})", *value as f64 / 1_000_000.0))
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1_000_000)),
            },
            ScalarValue::TimestampMillisecond(Some(value), timezone) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({}, 'unixepoch')", value / 1000)),
                Some(Engine::Postgres) => {
                    if timezone.is_none() {
                        Ok(format!(
                            "TO_TIMESTAMP({}) AT TIME ZONE 'UTC'",
                            *value as f64 / 1000.0
                        ))
                    } else {
                        Ok(format!("TO_TIMESTAMP({})", *value as f64 / 1000.0))
                    }
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1000)),
            },
            ScalarValue::TimestampSecond(Some(value), timezone) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({value}, 'unixepoch')")),
                Some(Engine::Postgres) => {
                    if timezone.is_none() {
                        Ok(format!("TO_TIMESTAMP({value}) AT TIME ZONE 'UTC'"))
                    } else {
                        Ok(format!("TO_TIMESTAMP({value})"))
                    }
                }
                _ => Ok(format!("TO_TIMESTAMP({value})")),
            },
            ScalarValue::Decimal128(Some(v), _, s) => {
                let decimal = BigDecimal::new(BigInt::from(*v), *s as i64);
                Ok(decimal.to_string())
            }
            _ => Err(Error::UnsupportedFilterExpr {
                expr: format!("{expr}"),
            }),
        },
        Expr::Like(like_expr) => {
            if like_expr.escape_char.is_some() {
                // Escape char is not supported
                return Err(Error::UnsupportedFilterExpr {
                    expr: format!("{expr}"),
                });
            }
            let expr = to_sql_with_engine(&like_expr.expr, engine)?;
            let pattern = to_sql_with_engine(&like_expr.pattern, engine)?;

            let mut op_and_pattern = match (engine, like_expr.case_insensitive) {
                (Some(Engine::Postgres | Engine::DuckDB), true) => format!("ILIKE {pattern}"),
                (Some(Engine::Postgres | Engine::DuckDB), false) => format!("LIKE {pattern}"),
                (Some(Engine::SQLite), true) => format!("LIKE {pattern}"),
                (Some(Engine::SQLite), false) => format!("LIKE {pattern} COLLATE BINARY"),
                _ => {
                    return Err(Error::UnsupportedFilterExpr {
                        expr: expr.to_string(),
                    });
                }
            };

            if like_expr.negated {
                op_and_pattern = format!("NOT {}", op_and_pattern)
            };

            Ok(format!("{expr} {op_and_pattern}"))
        }
        Expr::InList(in_list) => {
            let expr = to_sql_with_engine(&in_list.expr, engine)?;
            let list = in_list
                .list
                .iter()
                .map(|expr| to_sql_with_engine(expr, engine))
                .collect::<Result<Vec<String>>>()?;

            let op = if in_list.negated { "NOT IN" } else { "IN" };

            Ok(format!("{expr} {op} ({list})", list = list.join(", ")))
        }
        Expr::ScalarFunction(scalar_function) => {
            let args = scalar_function
                .args
                .iter()
                .map(|expr| to_sql_with_engine(expr, engine))
                .collect::<Result<Vec<String>>>()?;

            Ok(format!("{}({})", scalar_function.name(), args.join(", ")))
        }
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}

pub fn to_sql(expr: &Expr) -> Result<String> {
    to_sql_with_engine(expr, None)
}

fn handle_cast(cast: &Cast, engine: Option<Engine>, expr: &Expr) -> Result<String> {
    match cast.data_type {
        arrow::datatypes::DataType::Timestamp(_, Some(_) | None) => match engine {
            Some(Engine::ODBC) => Ok(format!(
                "CAST({} AS TIMESTAMP)",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            // This needs to match the timestamp conversion below
            Some(Engine::DuckDB) => Ok(format!(
                "TO_TIMESTAMP(EPOCH(CAST({} AS TIMESTAMP)))",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            Some(Engine::SQLite) => Ok(format!(
                "datetime({}, 'subsec', 'utc')",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
            Some(Engine::Spark) => EngineNotSupportedForExpressionSnafu {
                engine: "Spark".to_string(),
                expr: format!("{expr}"),
            }
            .fail()?,
            _ => Ok(format!(
                "CAST({} AS TIMESTAMPTZ)",
                to_sql_with_engine(&cast.expr, engine)?,
            )),
        },
        arrow::datatypes::DataType::Int64 => Ok(format!(
            "CAST({} AS BIGINT)",
            to_sql_with_engine(&cast.expr, engine)?,
        )),
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}

// Helper function to check if expression contains subquery
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

#[allow(dead_code)]
pub(super) fn expr_contains_subquery(expr: &Expr) -> datafusion::error::Result<bool> {
    let mut contains_subquery = false;
    expr.apply(|expr| match expr {
        Expr::ScalarSubquery(_) | Expr::InSubquery(_) | Expr::Exists(_) => {
            contains_subquery = true;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(contains_subquery)
}

#[cfg(test)]
mod tests {
    use std::{any::Any, sync::Arc};

    use super::*;
    use arrow::datatypes::DataType;
    use datafusion::{
        logical_expr::{
            expr::{InList, ScalarFunction},
            ColumnarValue, Expr, Like, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
            Volatility,
        },
        prelude::col,
        scalar::ScalarValue,
    };

    #[test]
    fn test_like_expr_to_sql() -> Result<()> {
        for (engine, case_insensitive, negated, expected) in [
            (Engine::Postgres, false, false, "\"name\" LIKE '%John%'"),
            (Engine::Postgres, true, true, "\"name\" NOT ILIKE '%John%'"),
            (Engine::DuckDB, false, false, "\"name\" LIKE '%John%'"),
            (Engine::DuckDB, true, true, "\"name\" NOT ILIKE '%John%'"),
            (
                Engine::SQLite,
                false,
                false,
                "\"name\" LIKE '%John%' COLLATE BINARY",
            ),
            (Engine::SQLite, true, true, "\"name\" NOT LIKE '%John%'"),
        ] {
            let expr = Expr::Like(Like {
                expr: Box::new(col("name")),
                pattern: Box::new(Expr::Literal(
                    ScalarValue::Utf8(Some("%John%".to_string())),
                    None,
                )),
                case_insensitive,
                negated,
                escape_char: None,
            });

            let sql = to_sql_with_engine(&expr, Some(engine))?;
            assert_eq!(sql, expected);
        }
        Ok(())
    }

    #[test]
    fn test_decimal128_literal_to_sql() -> Result<()> {
        let expr = Expr::Literal(ScalarValue::Decimal128(Some(1234567890), 38, 2), None);
        assert_eq!(to_sql_with_engine(&expr, None)?, "12345678.90");

        let expr_negative = Expr::Literal(ScalarValue::Decimal128(Some(-1234567890), 38, 4), None);
        assert_eq!(to_sql_with_engine(&expr_negative, None)?, "-123456.7890");

        let expr_int = Expr::Literal(ScalarValue::Decimal128(Some(1234567890), 38, 0), None);
        assert_eq!(to_sql_with_engine(&expr_int, None)?, "1234567890");

        Ok(())
    }

    #[test]
    fn test_expr_inlist_to_sql() -> Result<()> {
        let expr = Expr::InList(InList {
            expr: Box::new(col("a")),
            list: vec![
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
                Expr::Literal(ScalarValue::Int32(Some(2)), None),
                Expr::Literal(ScalarValue::Int32(Some(3)), None),
            ],
            negated: false,
        });
        assert_eq!(to_sql_with_engine(&expr, None)?, "\"a\" IN (1, 2, 3)");

        let expr_negated = Expr::InList(InList {
            expr: Box::new(col("a")),
            list: vec![
                Expr::Literal(ScalarValue::Int32(Some(4)), None),
                Expr::Literal(ScalarValue::Int32(Some(5)), None),
                Expr::Literal(ScalarValue::Int32(Some(6)), None),
            ],
            negated: true,
        });

        assert_eq!(
            to_sql_with_engine(&expr_negated, None)?,
            "\"a\" NOT IN (4, 5, 6)"
        );

        Ok(())
    }

    #[test]
    fn test_expr_scalar_function_to_sql() -> Result<()> {
        #[derive(Debug, Hash, Eq, PartialEq)]
        struct TestScalarUDF {
            signature: Signature,
        }
        impl ScalarUDFImpl for TestScalarUDF {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "substring"
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
                Ok(DataType::Utf8)
            }

            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> datafusion::error::Result<ColumnarValue> {
                Ok(ColumnarValue::Scalar(ScalarValue::from("a")))
            }
        }
        let substring_udf = Arc::new(ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(
                3,
                vec![DataType::Utf8, DataType::Int32, DataType::Int32],
                Volatility::Stable,
            ),
        }));

        let expr = Expr::ScalarFunction(ScalarFunction {
            func: substring_udf,
            args: vec![
                Expr::Literal(ScalarValue::Utf8(Some("hello world".to_string())), None),
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
                Expr::Literal(ScalarValue::Int32(Some(5)), None),
            ],
        });
        assert_eq!(
            to_sql_with_engine(&expr, None)?,
            "substring('hello world', 1, 5)"
        );

        Ok(())
    }

    #[test]
    fn test_expr_timestamp_scalar_value_to_sql() -> Result<()> {
        let expr = Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1_693_219_803_001_000_000), None),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001) AT TIME ZONE 'UTC'"
        );
        let expr = Expr::Literal(
            ScalarValue::TimestampNanosecond(
                Some(1_693_219_803_001_000_000),
                Some(Arc::from("+10:00")),
            ),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001)"
        );

        let expr = Expr::Literal(
            ScalarValue::TimestampMicrosecond(Some(1_693_219_803_001_000), None),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001) AT TIME ZONE 'UTC'"
        );
        let expr = Expr::Literal(
            ScalarValue::TimestampMicrosecond(
                Some(1_693_219_803_001_000),
                Some(Arc::from("+10:00")),
            ),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001)"
        );

        let expr = Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_693_219_803_001), None),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001) AT TIME ZONE 'UTC'"
        );
        let expr = Expr::Literal(
            ScalarValue::TimestampMillisecond(Some(1_693_219_803_001), Some(Arc::from("+10:00"))),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803.001)"
        );

        let expr = Expr::Literal(
            ScalarValue::TimestampSecond(Some(1_693_219_803), None),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803) AT TIME ZONE 'UTC'"
        );
        let expr = Expr::Literal(
            ScalarValue::TimestampSecond(Some(1_693_219_803), Some(Arc::from("+10:00"))),
            None,
        );
        assert_eq!(
            to_sql_with_engine(&expr, Some(Engine::Postgres))?,
            "TO_TIMESTAMP(1693219803)"
        );

        Ok(())
    }
}
