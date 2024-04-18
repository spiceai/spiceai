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
    UnsupportedFilterExpr { expr: String },
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn to_sql(expr: &Expr) -> Result<String> {
    match expr {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql(&binary_expr.left)?;
            let right = to_sql(&binary_expr.right)?;
            Ok(format!("{} {} {}", left, binary_expr.op, right))
        }
        Expr::Column(name) => Ok(format!("\"{name}\"")),
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
            ScalarValue::TimestampMillisecond(Some(value), None | Some(_)) => Ok(format!(
                "'{}'",
                chrono::DateTime::from_timestamp_millis(*value)
                    .unwrap_or_default()
                    .to_rfc3339()
            )),
            _ => Err(Error::UnsupportedFilterExpr {
                expr: format!("{expr}"),
            }),
        },
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}
