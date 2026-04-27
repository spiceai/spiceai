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

//! MSSQL (T-SQL) dialect for `DataFusion` unparser.

use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::ast::{self, ExactNumberInfo, Ident, ObjectName};
use datafusion::sql::sqlparser::tokenizer::Span;
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::Dialect;

/// Microsoft SQL Server dialect for T-SQL compatibility.
///
/// - Uses `FLOAT` for Float64 instead of the default `DOUBLE`.
/// - Remaps `substr` to T-SQL's `SUBSTRING`.
#[derive(Debug, Default)]
pub struct MsSqlDialect {}

impl MsSqlDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Dialect for MsSqlDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        None
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        ast::DataType::Float(ExactNumberInfo::None)
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        match func_name {
            "substr" => substr_to_substring(unparser, args),
            _ => Ok(None),
        }
    }
}

/// Converts `DataFusion`'s `substr(str, start[, len])` to T-SQL's `SUBSTRING(str, start, len)`.
///
/// T-SQL `SUBSTRING` requires 3 arguments on SQL Server 2022 and earlier; the
/// `length` parameter became optional only in SQL Server 2025+. For broad
/// compatibility, the 2-argument form is translated to
/// `SUBSTRING(str, start, 2147483647)` using SQL Server's `INT` maximum.
/// `SUBSTRING` clamps internally so the result is the rest of the string.
///
/// **Why not `LEN(str)`?** `LEN()` strips trailing spaces, which would silently
/// truncate results for strings ending in blanks — violating data correctness.
fn substr_to_substring(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    let mut sql_args: Vec<ast::FunctionArg> = args
        .iter()
        .map(|arg| {
            Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                unparser.expr_to_sql(arg)?,
            )))
        })
        .collect::<Result<Vec<_>>>()?;

    match sql_args.len() {
        2 => {
            // Append SQL Server INT max as the length so SUBSTRING returns the
            // rest of the string. This is safe across all SQL Server versions.
            sql_args.push(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                ast::Expr::Value(ast::Value::Number("2147483647".to_string(), false).into()),
            )));
        }
        3 => {} // already has all three arguments
        n => {
            return plan_err!("substr expects 2 or 3 arguments, got {n}");
        }
    }

    Ok(Some(ast::Expr::Function(ast::Function {
        name: ObjectName::from(vec![Ident {
            value: "SUBSTRING".to_string(),
            quote_style: None,
            span: Span::empty(),
        }]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: sql_args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{col, lit};
    use datafusion::sql::unparser::Unparser;

    fn create_dialect() -> MsSqlDialect {
        MsSqlDialect::new()
    }

    #[test]
    fn test_float64_cast() -> Result<()> {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let expr = datafusion::logical_expr::cast(
            col("a"),
            datafusion::arrow::datatypes::DataType::Float64,
        );
        let sql = unparser.expr_to_sql(&expr)?.to_string();
        assert_eq!(sql, "CAST(a AS FLOAT)");
        Ok(())
    }

    #[test]
    fn test_substr_to_substring() -> Result<()> {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let args = vec![col("name"), lit(1i64), lit(3i64)];
        let result = dialect
            .scalar_function_to_sql_overrides(&unparser, "substr", &args)?
            .expect("should return Some for substr");

        let sql = result.to_string();
        assert_eq!(sql, "SUBSTRING(name, 1, 3)");
        Ok(())
    }

    #[test]
    fn test_substr_two_args_appends_max_length() -> Result<()> {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let args = vec![col("name"), lit(2i64)];
        let result = dialect
            .scalar_function_to_sql_overrides(&unparser, "substr", &args)?
            .expect("should return Some for substr");

        let sql = result.to_string();
        assert_eq!(sql, "SUBSTRING(name, 2, 2147483647)");
        Ok(())
    }

    #[test]
    fn test_unknown_function_returns_none() -> Result<()> {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let args = vec![col("a")];
        let result = dialect.scalar_function_to_sql_overrides(&unparser, "some_other_fn", &args)?;

        assert!(result.is_none());
        Ok(())
    }
}
