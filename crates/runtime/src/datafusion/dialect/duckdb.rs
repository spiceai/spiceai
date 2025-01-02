/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::ast::{self, Function, FunctionArgExpr, Ident, ObjectName};
use itertools::Itertools;

/// Converts the `cosine_distance` UDF into `DuckDB` `array_cosine_distance` function:
/// `https://duckdb.org/docs/sql/functions/array.html#array_cosine_distancearray1-array2`
///
///  - replaces `make_array` function with the array constructor (`make_array` is not supported in `DuckDB`)
///  - casts to `DuckDB` Array (`FixedSizeList`)
pub(crate) fn cosine_distance_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    let ast_args: Vec<ast::Expr> = args
        .iter()
        .map(|arg| match arg {
            // embeddings array is wrapped in a make_array function, unwrap it
            Expr::ScalarFunction(scalar_func)
                if scalar_func.name().to_lowercase() == "make_array" =>
            {
                let num_elements = scalar_func.args.len() as u64;

                let array = ast::Expr::Array(ast::Array {
                    elem: scalar_func
                        .args
                        .iter()
                        .map(|x| unparser.expr_to_sql(x))
                        .try_collect()?,
                    named: false,
                });

                // Apply required ::FLOAT[] casting. Only FLOAT emneddings are curently supported
                Ok(ast::Expr::Cast {
                    expr: Box::new(array),
                    data_type: ast::DataType::Array(ast::ArrayElemTypeDef::SquareBracket(
                        Box::new(ast::DataType::Float(None)),
                        Some(num_elements),
                    )),
                    kind: ast::CastKind::DoubleColon,
                    format: None,
                })
            }
            // For all other expressions, directly convert them to SQL
            _ => unparser.expr_to_sql(arg),
        })
        .try_collect()?;

    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![Ident {
            value: "array_cosine_distance".to_string(),
            quote_style: None,
        }]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: ast_args
                .into_iter()
                .map(|x| ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(x)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        parameters: ast::FunctionArguments::None,
    });

    Ok(Some(ast_fn))
}
#[cfg(test)]
mod tests {
    use datafusion::{
        common::Column,
        functions_array::make_array::make_array_udf,
        logical_expr::expr::ScalarFunction,
        prelude::lit,
        scalar::ScalarValue,
        sql::{unparser::Unparser, TableReference},
    };

    use crate::datafusion::dialect::new_duckdb_dialect;

    use super::*;

    #[test]
    fn test_cosine_distance_to_sql_scalars() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            // raw values
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![lit(1.0), lit(2.0), lit(3.0)],
            )),
            // values wrapped as literals
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![
                    Expr::Literal(ScalarValue::Float32(Some(4.0))),
                    Expr::Literal(ScalarValue::Float32(Some(5.0))),
                    Expr::Literal(ScalarValue::Float32(Some(6.0))),
                ],
            )),
        ];
        let result = cosine_distance_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let expected =
            "array_cosine_distance([1.0, 2.0, 3.0]::FLOAT[3], [4.0, 5.0, 6.0]::FLOAT[3])";

        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn test_cosine_distance_to_sql_column_and_scalar() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            Expr::Column(Column {
                relation: Some(TableReference::from("table_name")),
                name: "column_name".to_string(),
            }),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![
                    Expr::Literal(ScalarValue::Float32(Some(4.0))),
                    Expr::Literal(ScalarValue::Float32(Some(5.0))),
                    Expr::Literal(ScalarValue::Float32(Some(6.0))),
                ],
            )),
        ];

        let result = cosine_distance_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");
        let expected =
            r#"array_cosine_distance("table_name"."column_name", [4.0, 5.0, 6.0]::FLOAT[3])"#;

        assert_eq!(result.to_string(), expected);
    }
}
