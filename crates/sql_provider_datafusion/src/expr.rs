/*
Copyright 2024 The Spice.ai OSS Authors

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

use datafusion::{logical_expr::Expr, scalar::ScalarValue};

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
}

#[allow(clippy::needless_pass_by_value)]
pub fn to_sql_with_engine(expr: &Expr, engine: Option<Engine>) -> Result<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql_with_engine(&binary_expr.left, engine)?;
            let right = to_sql_with_engine(&binary_expr.right, engine)?;

            if let Some(Engine::DuckDB) = engine {
                // DuckDB doesn't support compare timestamp with timestamptz in 0.10.1 yet. Revisit
                // in 0.10.2
                if right.starts_with("TO_TIMESTAMP") && !left.starts_with("TO_TIMESTAMP") {
                    return Ok(format!(
                        "TO_TIMESTAMP(EPOCH({})) {} {}",
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
        Expr::Cast(cast) => {
            match cast.data_type {
                arrow::datatypes::DataType::Timestamp(_, Some(_) | None) => match engine {
                    None => Ok(format!(
                        "CAST({} AS TIMESTAMPTZ)",
                        to_sql_with_engine(&cast.expr, engine)?,
                    )),
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
                },
                _ => Err(Error::UnsupportedFilterExpr {
                    expr: format!("{expr}"),
                }),
            }
        }
        Expr::Literal(value) => match value {
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
            ScalarValue::TimestampNanosecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => {
                    Ok(format!("datetime({}, 'unixepoch')", value / 1_000_000_000))
                }
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1_000_000_000)),
            },
            ScalarValue::TimestampMillisecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({}, 'unixepoch')", value / 1000)),
                _ => Ok(format!("TO_TIMESTAMP({})", value / 1000)),
            },
            ScalarValue::TimestampSecond(Some(value), None | Some(_)) => match engine {
                Some(Engine::SQLite) => Ok(format!("datetime({value}, 'unixepoch')")),
                _ => Ok(format!("TO_TIMESTAMP({value})")),
            },
            _ => Err(Error::UnsupportedFilterExpr {
                expr: format!("{expr}"),
            }),
        },
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}

pub fn to_sql(expr: &Expr) -> Result<String> {
    to_sql_with_engine(expr, None)
}
