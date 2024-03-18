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
            _ => Err(Error::UnsupportedFilterExpr {
                expr: format!("{expr}"),
            }),
        },
        _ => Err(Error::UnsupportedFilterExpr {
            expr: format!("{expr}"),
        }),
    }
}
