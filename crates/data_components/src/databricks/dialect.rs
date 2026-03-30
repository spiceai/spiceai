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

//! Databricks SQL dialect for `DataFusion` unparser.
//!
//! This dialect configures the `DataFusion` unparser to generate Databricks/Spark SQL-compatible
//! syntax, including function translations like `array_has` -> `array_contains`.

use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    self, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle};

/// Databricks SQL dialect for Spark SQL compatibility.
///
/// This dialect generates Databricks/Spark SQL-compatible SQL, including:
/// - Backtick identifier quoting
/// - MySQL-style intervals
/// - Function translations (e.g., `array_has` -> `array_contains`)
#[derive(Debug, Default)]
pub struct DatabricksDialect {}

impl DatabricksDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl Dialect for DatabricksDialect {
    /// Databricks uses backticks for identifier quoting.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    /// Databricks uses MySQL-style intervals.
    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::MySQL
    }

    /// Override scalar functions to translate `DataFusion` functions to Spark SQL equivalents.
    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser<'_>,
        func_name: &str,
        args: &[Expr],
    ) -> datafusion::error::Result<Option<ast::Expr>> {
        match func_name {
            "array_has" | "list_has" => array_has_to_array_contains(unparser, args),
            _ => Ok(None),
        }
    }
}

/// Converts `DataFusion`'s `array_has(array, element)` to Spark SQL's `array_contains(array, element)`.
///
/// Both functions have the same argument order, so this is a simple name translation.
fn array_has_to_array_contains(
    unparser: &Unparser<'_>,
    args: &[Expr],
) -> datafusion::error::Result<Option<ast::Expr>> {
    if args.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "array_has requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    if let Expr::Literal(literal, _) = &args[0] {
        match literal {
            ScalarValue::List(..) | ScalarValue::LargeList(..) | ScalarValue::FixedSizeList(..) => {
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "array_has first argument must be an array literal or column".to_string(),
                ));
            }
        }
    }

    let ast_args: Vec<FunctionArg> = args
        .iter()
        .map(|arg| {
            Ok(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                unparser.expr_to_sql(arg)?,
            )))
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;

    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
            "array_contains",
        ))]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: ast_args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
        uses_odbc_syntax: false,
    });

    Ok(Some(ast_fn))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::{Column, Spans};
    use datafusion::functions_nested::array_has::array_has_udf;
    use datafusion::functions_nested::make_array::make_array_udf;
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::lit;
    use datafusion::scalar::ScalarValue;
    use datafusion::sql::TableReference;
    use datafusion::sql::unparser::Unparser;

    fn create_dialect() -> DatabricksDialect {
        DatabricksDialect::new()
    }

    #[test]
    fn test_identifier_quote_style() {
        let dialect = create_dialect();
        assert_eq!(dialect.identifier_quote_style("test"), Some('`'));
        assert_eq!(dialect.identifier_quote_style("column_name"), Some('`'));
    }

    #[test]
    fn test_interval_style() {
        let dialect = create_dialect();
        assert!(matches!(dialect.interval_style(), IntervalStyle::MySQL));
    }

    #[test]
    fn test_array_has_to_array_contains() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        // Create array_has(make_array(1, 2, 3), 2)
        let array_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            make_array_udf(),
            vec![lit(1), lit(2), lit(3)],
        ));
        let element_expr = lit(2);

        let args = vec![array_expr, element_expr];

        let result = array_has_to_array_contains(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let result_str = result.to_string();
        assert!(
            result_str.contains("array_contains"),
            "Expected 'array_contains' in result, got: {result_str}"
        );
    }

    #[test]
    fn test_array_has_with_column() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        // Create array_has(column, value)
        let column_expr = Expr::Column(Column {
            relation: Some(TableReference::from("my_table")),
            name: "my_array".to_string(),
            spans: Spans::new(),
        });
        let element_expr = lit("search_value");

        let args = vec![column_expr, element_expr];

        let result = array_has_to_array_contains(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let result_str = result.to_string();
        assert!(
            result_str.contains("array_contains"),
            "Expected 'array_contains' in result, got: {result_str}"
        );
        assert!(
            result_str.contains("my_array"),
            "Expected 'my_array' in result, got: {result_str}"
        );
    }

    #[test]
    fn test_list_has_alias() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let array_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            make_array_udf(),
            vec![lit(1), lit(2), lit(3)],
        ));
        let element_expr = lit(2);
        let args = vec![array_expr, element_expr];

        // Verify list_has also maps to array_contains via scalar_function_to_sql_overrides
        let result = dialect
            .scalar_function_to_sql_overrides(&unparser, "list_has", &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let result_str = result.to_string();
        assert!(
            result_str.contains("array_contains"),
            "Expected 'array_contains' in result for list_has, got: {result_str}"
        );
    }

    #[test]
    fn test_unknown_function_returns_none() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let args = vec![lit(1)];

        let result = dialect
            .scalar_function_to_sql_overrides(&unparser, "unknown_function", &args)
            .expect("should execute successfully");

        assert!(
            result.is_none(),
            "Expected None for unknown function, got: {result:?}"
        );
    }

    #[test]
    fn test_array_has_wrong_arg_count() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        // Only one argument - should fail
        let args = vec![lit(1)];

        let result = array_has_to_array_contains(&unparser, &args);
        assert!(result.is_err(), "Expected error for wrong argument count");
    }

    #[test]
    fn test_array_has_literal_non_array_errors() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let args = vec![lit(1), lit(2)];

        let result = array_has_to_array_contains(&unparser, &args);
        assert!(
            result
                .expect_err("expected error for non-array literal")
                .to_string()
                .contains("first argument must be an array literal or column")
        );
    }

    #[test]
    fn test_array_has_with_null_element() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let column_expr = Expr::Column(Column {
            relation: Some(TableReference::from("my_table")),
            name: "my_array".to_string(),
            spans: Spans::new(),
        });
        let null_expr = Expr::Literal(ScalarValue::Null, None);

        let args = vec![column_expr, null_expr];

        let result = array_has_to_array_contains(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let result_str = result.to_string();
        assert!(result_str.contains("array_contains"));
        assert!(result_str.contains("NULL"));
    }

    #[test]
    fn test_array_has_identifier_quoting() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        let column_expr = Expr::Column(Column {
            relation: Some(TableReference::from("my-db")),
            name: "array col".to_string(),
            spans: Spans::new(),
        });
        let element_expr = lit(2);
        let args = vec![column_expr, element_expr];

        let result = array_has_to_array_contains(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let result_str = result.to_string();
        assert!(result_str.contains("array_contains"));
        assert!(
            result_str.contains("`array col`"),
            "Expected backtick quoting, got: {result_str}"
        );
    }

    #[test]
    fn test_scalar_function_override_integration() {
        let dialect = create_dialect();
        let unparser = Unparser::new(&dialect);

        // Create an array_has scalar function expression
        let array_has_expr = Expr::ScalarFunction(ScalarFunction::new_udf(
            array_has_udf(),
            vec![
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    make_array_udf(),
                    vec![lit(1), lit(2), lit(3)],
                )),
                lit(2),
            ],
        ));

        // Convert to SQL using the unparser with our dialect
        let sql_str = unparser
            .expr_to_sql(&array_has_expr)
            .expect("to convert expr to sql")
            .to_string();
        assert_eq!(
            sql_str, "array_contains([1, 2, 3], 2)",
            "Expected full SQL string match"
        );
    }
}
