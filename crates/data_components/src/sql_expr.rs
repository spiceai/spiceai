/*
Copyright 2026 The Spice.ai OSS Authors

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

use datafusion::logical_expr::Expr;
use datafusion_table_providers::sql::sql_provider_datafusion::expr;

pub fn to_sql_preserving_precedence(expr_value: &Expr) -> expr::Result<String> {
    match expr_value {
        Expr::BinaryExpr(binary_expr) => {
            let left = to_sql_preserving_precedence(binary_expr.left.as_ref())?;
            let right = to_sql_preserving_precedence(binary_expr.right.as_ref())?;
            Ok(format!("({left} {} {right})", binary_expr.op))
        }
        Expr::Not(inner) => Ok(format!(
            "(NOT {})",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNull(inner) => Ok(format!(
            "({} IS NULL)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotNull(inner) => Ok(format!(
            "({} IS NOT NULL)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsTrue(inner) => Ok(format!(
            "({} IS TRUE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsFalse(inner) => Ok(format!(
            "({} IS FALSE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsUnknown(inner) => Ok(format!(
            "({} IS UNKNOWN)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotTrue(inner) => Ok(format!(
            "({} IS NOT TRUE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotFalse(inner) => Ok(format!(
            "({} IS NOT FALSE)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::IsNotUnknown(inner) => Ok(format!(
            "({} IS NOT UNKNOWN)",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        Expr::Negative(inner) => Ok(format!(
            "(-{})",
            to_sql_preserving_precedence(inner.as_ref())?
        )),
        _ => expr::to_sql(expr_value),
    }
}
