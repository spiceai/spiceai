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
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{
    self, Array, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, ValueWithSpan,
};
use itertools::Itertools;

pub(crate) const REGEXP_LIKE_NAME: &str = "regexp_matches";
pub(crate) const REGEXP_MATCH_NAME: &str = "regexp_extract";
pub(crate) const REGEXP_REPLACE_NAME: &str = "regexp_replace";
pub(crate) const REGEXP_COUNT_NAME: &str = "regexp_extract_all";

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

                // Apply required ::FLOAT[] casting. Only FLOAT embeddings are currently supported
                Ok(ast::Expr::Cast {
                    expr: Box::new(array),
                    data_type: ast::DataType::Array(ast::ArrayElemTypeDef::SquareBracket(
                        Box::new(ast::DataType::Float(ast::ExactNumberInfo::None)),
                        Some(num_elements),
                    )),
                    kind: ast::CastKind::DoubleColon,
                    array: false,
                    format: None,
                })
            }
            Expr::Literal(ScalarValue::FixedSizeList(array), None) => {
                let num_elements = u64::try_from(array.value_length()).map_err(|e| {
                    DataFusionError::Execution(format!("Cannot cast array length to u64 {e}"))
                })?;
                let array = unparser.expr_to_sql(arg)?;

                // Apply required ::FLOAT[] casting. Only FLOAT embeddings are curently supported
                Ok(ast::Expr::Cast {
                    expr: Box::new(array),
                    data_type: ast::DataType::Array(ast::ArrayElemTypeDef::SquareBracket(
                        Box::new(ast::DataType::Float(ast::ExactNumberInfo::None)),
                        Some(num_elements),
                    )),
                    kind: ast::CastKind::DoubleColon,
                    array: false,
                    format: None,
                })
            }
            // For all other expressions, directly convert them to SQL
            _ => unparser.expr_to_sql(arg),
        })
        .try_collect()?;

    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
            "array_cosine_distance",
        ))]),
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
        uses_odbc_syntax: false,
    });

    Ok(Some(ast_fn))
}

/// Converts `array_distance(query, embed_col)` to `DuckDB` `array_distance` with explicit
/// `::FLOAT[N]` casts on both arguments.
///
/// `DuckDB`'s `array_distance` requires both arguments to have the same element type. When the
/// stored embedding column has type `DECIMAL(12,11)[]` and the query literal is `FLOAT[N]`,
/// `DuckDB` rejects the call. Casting both sides to `FLOAT[N]` resolves the mismatch.
pub(crate) fn array_distance_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    let num_elements: Option<u64> = args.iter().find_map(|arg| match arg {
        Expr::Literal(ScalarValue::FixedSizeList(array), _) => {
            u64::try_from(array.value_length()).ok()
        }
        _ => None,
    });

    let cast_to_float_array = |expr: ast::Expr| -> ast::Expr {
        let Some(n) = num_elements else {
            return expr;
        };
        ast::Expr::Cast {
            expr: Box::new(expr),
            data_type: ast::DataType::Array(ast::ArrayElemTypeDef::SquareBracket(
                Box::new(ast::DataType::Float(ast::ExactNumberInfo::None)),
                Some(n),
            )),
            kind: ast::CastKind::DoubleColon,
            array: false,
            format: None,
        }
    };

    let ast_args: Vec<ast::Expr> = args
        .iter()
        .map(|arg| unparser.expr_to_sql(arg).map(cast_to_float_array))
        .try_collect()?;

    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
            "array_distance",
        ))]),
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
        uses_odbc_syntax: false,
    });

    Ok(Some(ast_fn))
}

#[expect(clippy::unnecessary_wraps)] // Required to match the signature of the `ScalarFnToSqlHandler` trait
pub(crate) fn rand_to_random(
    _unparser: &datafusion::sql::unparser::Unparser,
    _args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    let ast_fn = ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new("random"))]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![],
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

pub(super) enum DuckDBRegexpFunction {
    Match,
    Like,
    Replace,
    Count,
}

impl DuckDBRegexpFunction {
    fn process_args(&self, ast_args: &mut Vec<FunctionArg>) -> Result<(), DataFusionError> {
        match self {
            DuckDBRegexpFunction::Match => {
                if ast_args.len() == 3 {
                    // regexp_extract has 4 positional args, position 3 = group not flags
                    // bump flags to 4, insert default 0 group
                    ast_args.insert(
                        2,
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                            ValueWithSpan {
                                value: sqlparser::ast::Value::Number("0".to_string(), false),
                                span: sqlparser::tokenizer::Span::empty(),
                            },
                        ))),
                    );
                }
            }
            DuckDBRegexpFunction::Count => {
                if ast_args.len() == 3 {
                    // arg #3 is start position
                    // DuckDB has no equivalent for column or function name, but we can use list slicing if an integer start is specified
                    let Some(start_arg) = ast_args.get(2) else {
                        unreachable!("start_arg should be present")
                    };

                    match start_arg {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                            ValueWithSpan {
                                value: sqlparser::ast::Value::Number(num_str, _),
                                ..
                            },
                        ))) => {
                            let start: u64 = num_str.parse().map_err(|e| {
                            DataFusionError::Plan(format!(
                                "Could not parse start position {num_str} as integer for function {}: {e}", self.federated_function_name()
                            ))
                        })?;
                            // DuckDB uses 0-based indexing, DataFusion uses 1-based indexing
                            if start < 1 {
                                return Err(DataFusionError::Plan(format!(
                                    "Start position must be a positive integer for regular expression function {}, received {start}",
                                    self.federated_function_name()
                                )));
                            }
                            let duckdb_start = start - 1;
                            ast_args.remove(2);

                            // wrap the input column/value with a substring. ``substring(string, start[, length])``
                            // length can be omitted as only the start value is specified
                            let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))) =
                                ast_args.first()
                            else {
                                unreachable!("input_arg should be present")
                            };

                            ast_args[0] =
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Substring {
                                    expr: Box::new(expr.clone()),
                                    substring_from: Some(Box::new(ast::Expr::Value(
                                        ValueWithSpan {
                                            value: sqlparser::ast::Value::Number(
                                                duckdb_start.to_string(),
                                                false,
                                            ),
                                            span: sqlparser::tokenizer::Span::empty(),
                                        },
                                    ))),
                                    substring_for: None,
                                    special: true,
                                    shorthand: false,
                                }));
                        }
                        _ => {
                            return Err(DataFusionError::Plan(format!(
                                "Only integer start positions are supported for regular expression function {} with DuckDB",
                                self.federated_function_name()
                            )));
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn wrap_function(ast_fn: ast::Expr, function_name: &str) -> ast::Expr {
        ast::Expr::Function(Function {
            name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                function_name,
            ))]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(ast_fn))],
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: ast::FunctionArguments::None,
            uses_odbc_syntax: false,
        })
    }

    fn postprocess_function(&self, mut ast_fn: ast::Expr) -> ast::Expr {
        match self {
            DuckDBRegexpFunction::Match => {
                // DuckDB ``regexp_extract`` returns a plain string
                // DataFusion ``regexp_match`` returns an array with a single string value
                ast_fn = ast::Expr::Named {
                    expr: Box::new(ast::Expr::Array(Array {
                        elem: vec![ast_fn],
                        named: true,
                    })),
                    name: Ident::new("item"),
                }
            }
            DuckDBRegexpFunction::Count => {
                // Wrap the extract array in a ``len()``
                ast_fn = Self::wrap_function(ast_fn, "len");
            }
            _ => {}
        }

        ast_fn
    }

    fn federated_function_name(&self) -> &str {
        match self {
            DuckDBRegexpFunction::Match => REGEXP_MATCH_NAME,
            DuckDBRegexpFunction::Like => REGEXP_LIKE_NAME,
            DuckDBRegexpFunction::Replace => REGEXP_REPLACE_NAME,
            DuckDBRegexpFunction::Count => REGEXP_COUNT_NAME,
        }
    }

    /// Maps an input function to an underlying function, whose underlying function accepts the same arguments as the input function
    /// For example, ``DataFusion``'s ``regexp_like`` -> ``DuckDB``'s ``regexp_matches``
    pub(super) fn to_datafusion_function(
        &self,
        flags_position: usize,
    ) -> impl Fn(
        &datafusion::sql::unparser::Unparser,
        &[Expr],
    ) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
        move |unparser, args| {
            let mut ast_args: Vec<FunctionArg> = args
                .iter()
                .map(|arg| {
                    Ok::<FunctionArg, DataFusionError>(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                        unparser.expr_to_sql(arg)?,
                    )))
                })
                .try_collect()?;

            if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(
                ValueWithSpan {
                    value:
                        sqlparser::ast::Value::SingleQuotedString(string)
                        | sqlparser::ast::Value::DoubleQuotedString(string),
                    ..
                },
            )))) = ast_args.get(flags_position)
            {
                // Check if `U` or `R` flags are set, which are not supported by DuckDB
                if string.contains('U') || string.contains('R') {
                    return Err(DataFusionError::Plan(format!(
                        "Regular expression flags `U` or `R` are not supported by DuckDB for function {}.",
                        self.federated_function_name()
                    )));
                }
            }

            self.process_args(&mut ast_args)?;

            let ast_fn = ast::Expr::Function(Function {
                name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
                    self.federated_function_name(),
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

            Ok(Some(self.postprocess_function(ast_fn)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{FixedSizeListArray, Float32Array};
    use arrow_schema::{DataType, Field};
    use datafusion::{
        common::{Column, Spans},
        functions_nested::make_array::make_array_udf,
        logical_expr::expr::ScalarFunction,
        prelude::{Expr, lit},
        scalar::ScalarValue,
        sql::{TableReference, unparser::Unparser},
    };

    use crate::dialect::new_duckdb_dialect;

    fn fixed_size_list_literal(values: Vec<f32>) -> Expr {
        let n = i32::try_from(values.len()).expect("values length fits in i32");
        let inner = Arc::new(Float32Array::from(values));
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let array =
            FixedSizeListArray::try_new(field, n, inner, None).expect("valid FixedSizeListArray");
        Expr::Literal(ScalarValue::FixedSizeList(Arc::new(array)), None)
    }

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
                    Expr::Literal(ScalarValue::Float32(Some(4.0)), None),
                    Expr::Literal(ScalarValue::Float32(Some(5.0)), None),
                    Expr::Literal(ScalarValue::Float32(Some(6.0)), None),
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
                spans: Spans::new(),
            }),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![
                    Expr::Literal(ScalarValue::Float32(Some(4.0)), None),
                    Expr::Literal(ScalarValue::Float32(Some(5.0)), None),
                    Expr::Literal(ScalarValue::Float32(Some(6.0)), None),
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

    #[test]
    fn test_array_distance_to_sql_literal_and_column() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            fixed_size_list_literal(vec![1.0, 2.0, 3.0]),
            Expr::Column(Column {
                relation: Some(TableReference::from("bluesky_posts")),
                name: "text_embedding".to_string(),
                spans: Spans::new(),
            }),
        ];

        let result = array_distance_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let expected = r#"array_distance([1.0, 2.0, 3.0]::FLOAT[3], "bluesky_posts"."text_embedding"::FLOAT[3])"#;
        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn test_array_distance_to_sql_two_literals() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            fixed_size_list_literal(vec![0.1, 0.2]),
            fixed_size_list_literal(vec![0.3, 0.4]),
        ];

        let result = array_distance_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");

        let expected = "array_distance([0.1, 0.2]::FLOAT[2], [0.3, 0.4]::FLOAT[2])";
        assert_eq!(result.to_string(), expected);
    }
}
