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

/// Shared conversion for Spice vector UDFs that have a native `DuckDB` ARRAY
/// equivalent taking two equal-length `FLOAT[N]` operands (e.g.
/// `array_inner_product`, `array_distance`).
///
///  - replaces the `make_array` constructor with a `DuckDB` array literal
///    (`make_array` is not supported in `DuckDB`)
///  - applies the required `::FLOAT[N]` cast to array operands (only FLOAT
///    embeddings are currently supported)
///  - emits a call to `duckdb_fn`
fn spice_array_fn_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
    duckdb_fn: &str,
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
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(duckdb_fn))]),
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

/// Wraps `expr` so a non-finite result becomes `NULL`, the way the Spice vector
/// kernels do.
///
/// The kernels return `NULL` when a distance is not defined rather than a
/// fabricated number, because `_score` is `1 - distance` and any number at all
/// competes with real matches (see `runtime_datafusion_udfs::vector_simd`). A
/// pushed-down call has to carry that contract too, or the same row scores
/// differently depending only on whether the table federated.
///
/// `nullif` rather than `CASE WHEN isfinite(..)`: `DuckDB` compares `NaN` equal
/// to `NaN` and each infinity equal to itself, so the chain below reaches every
/// non-finite value while naming `expr` once. `CASE` would have to repeat the
/// whole call — operands included — in both arms.
///
/// Measured against `DuckDB` v1.5.5: the chain returns `NULL` for `NaN`, `inf`
/// and `-inf`, passes finite values through unchanged, and leaves the result's
/// `FLOAT` type alone (a `DOUBLE` sentinel would widen it).
///
/// This screens the *result*, so it only reaches an engine function that lets a
/// non-finite input reach its output. `array_inner_product` accumulates its
/// operands directly and does propagate — the same reason
/// `Kernel::hides_non_finite_input` is `false` for `Dot`. A function that
/// normalizes, and so answers a finite number for a non-finite input, cannot be
/// repaired this way and does not belong in the pushdown list at all.
fn null_if_not_finite(expr: ast::Expr) -> ast::Expr {
    ["nan", "inf", "-inf"]
        .into_iter()
        .fold(expr, |acc, sentinel| {
            ast::Expr::Function(Function {
                name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new("nullif"))]),
                args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: vec![
                        ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(acc)),
                        ast::FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Cast {
                            expr: Box::new(ast::Expr::Value(ValueWithSpan {
                                value: ast::Value::SingleQuotedString(sentinel.to_string()),
                                span: sqlparser::tokenizer::Span::empty(),
                            })),
                            data_type: ast::DataType::Float(ast::ExactNumberInfo::None),
                            kind: ast::CastKind::DoubleColon,
                            array: false,
                            format: None,
                        })),
                    ],
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
                parameters: ast::FunctionArguments::None,
                uses_odbc_syntax: false,
            })
        })
}

/// Converts the `inner_product` UDF into `DuckDB`'s `array_inner_product` (dot
/// product, `sum(a[i] * b[i])`), screened so a non-finite result is `NULL`.
///
/// The two compute the same value for finite operands, which is what makes the
/// pushdown sound. They disagreed on a non-finite one — the UDF answers `NULL`,
/// `DuckDB` propagated the `NaN` or infinity — so [`null_if_not_finite`] closes
/// that. Numeric precision is a separate axis and unchanged: `DuckDB`
/// accumulates in `FLOAT` where `simsimd` accumulates wider, so a dot product
/// that overflows `FLOAT` alone now federates to `NULL` rather than to `inf`,
/// which is the same direction the local output check takes.
/// `https://duckdb.org/docs/sql/functions/array.html#array_inner_productarray1-array2`
pub(crate) fn inner_product_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    Ok(spice_array_fn_to_sql(unparser, args, "array_inner_product")?.map(null_if_not_finite))
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
            DuckDBRegexpFunction::Match if ast_args.len() == 3 => {
                // regexp_extract has 4 positional args, position 3 = group not flags
                // bump flags to 4, insert default 0 group
                ast_args.insert(
                    2,
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(ast::Expr::Value(ValueWithSpan {
                        value: sqlparser::ast::Value::Number("0".to_string(), false),
                        span: sqlparser::tokenizer::Span::empty(),
                    }))),
                );
            }
            DuckDBRegexpFunction::Count if ast_args.len() == 3 => {
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
                                substring_from: Some(Box::new(ast::Expr::Value(ValueWithSpan {
                                    value: sqlparser::ast::Value::Number(
                                        duckdb_start.to_string(),
                                        false,
                                    ),
                                    span: sqlparser::tokenizer::Span::empty(),
                                }))),
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
    fn cosine_distance_is_not_pushed_down_to_duckdb() {
        // Regression test for #13088. `array_cosine_distance` is NOT the DuckDB
        // equivalent of this UDF: measured against DuckDB v1.5.5 it answers
        // `1 - cosine_similarity` over [0, 2] where the UDF answers
        // `(1 - cosine_similarity) / 2` over [0, 1] — twice the distance for
        // every non-identical pair — plus 2.0 where the UDF answers 0.5 (a
        // zero-magnitude vector) and NULL (a non-finite element). While it was
        // carved out of the deny-list, the same query returned one number
        // locally and a different one federated.
        assert!(
            !crate::dialect::duckdb_native_function_names()
                .contains(&runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME),
            "cosine_distance must stay denied so DataFusion evaluates it locally; \
             array_cosine_distance returns a differently-scaled distance"
        );
    }

    #[test]
    fn test_inner_product_to_sql_column_and_scalar() {
        // inner_product(column, [4,5,6]) must unparse to DuckDB's native
        // array_inner_product with the ::FLOAT[N] casts the array functions need.
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            Expr::Column(Column {
                relation: Some(TableReference::from("table_name")),
                name: "embedding".to_string(),
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

        let result = inner_product_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression");
        // The nullif chain is the NULL-for-non-finite contract the local kernel
        // holds, carried into the pushed-down SQL (#13088). The operands appear
        // once: `CASE WHEN isfinite(..)` would have to repeat the whole call.
        let expected = r#"nullif(nullif(nullif(array_inner_product("table_name"."embedding", [4.0, 5.0, 6.0]::FLOAT[3]), 'nan'::FLOAT), 'inf'::FLOAT), '-inf'::FLOAT)"#;
        assert_eq!(result.to_string(), expected);
    }

    #[test]
    fn inner_product_screen_names_its_operands_once() {
        // The screen must not duplicate the operands: a repeated column is
        // harmless, but a repeated *literal* embedding doubles the size of every
        // pushed-down vector filter, and a repeated volatile sub-expression would
        // be evaluated twice.
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            Expr::Column(Column {
                relation: Some(TableReference::from("t")),
                name: "embedding".to_string(),
                spans: Spans::new(),
            }),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![lit(4.0), lit(5.0), lit(6.0)],
            )),
        ];
        let rendered = inner_product_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression")
            .to_string();
        assert_eq!(
            rendered.matches("array_inner_product").count(),
            1,
            "the screened rendering must call array_inner_product once, got {rendered}"
        );
    }

    #[test]
    fn inner_product_screen_covers_every_non_finite_value() {
        // Each of the three non-finite values needs its own sentinel: DuckDB
        // compares NaN equal to NaN and each infinity equal to itself, but an
        // infinity is not equal to a NaN, so one nullif cannot reach all three.
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = vec![
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![lit(1.0), lit(2.0)],
            )),
            Expr::ScalarFunction(ScalarFunction::new_udf(
                make_array_udf(),
                vec![lit(3.0), lit(4.0)],
            )),
        ];
        let rendered = inner_product_to_sql(&unparser, &args)
            .expect("should execute successfully")
            .expect("should return expression")
            .to_string();
        for sentinel in ["'nan'::FLOAT", "'inf'::FLOAT", "'-inf'::FLOAT"] {
            assert!(
                rendered.contains(sentinel),
                "screened rendering must nullif against {sentinel}, got {rendered}"
            );
        }
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

    #[test]
    fn test_rand_to_random() {
        // `rand` is a deny-listed Spice function that the federation deny-list
        // nonetheless lets push down to DuckDB *because* this dialect rewrites it
        // into DuckDB's native `random()`. This test backs that pushdown claim:
        // if the rewrite ever broke, pushing `rand` to DuckDB would emit invalid
        // SQL.
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let result = rand_to_random(&unparser, &[])
            .expect("should execute successfully")
            .expect("should return expression");
        assert_eq!(result.to_string(), "random()");
    }

    #[test]
    fn duckdb_native_function_names_advertises_denylisted_pushables() {
        // The federation deny-list relies on these names to let `inner_product`
        // and `rand` push down to DuckDB, so the dialect must advertise them.
        // `cosine_distance` used to be asserted here too; it is now asserted
        // *absent* by `cosine_distance_is_not_pushed_down_to_duckdb`, because
        // DuckDB's `array_cosine_distance` answers a differently-scaled distance
        // (#13088).
        let names = crate::dialect::duckdb_native_function_names();
        assert!(
            names.contains(&runtime_datafusion_udfs::inner_product::INNER_PRODUCT_UDF_NAME),
            "duckdb_native_function_names() missing inner_product; got {names:?}"
        );
        assert!(
            names.contains(&"rand"),
            "duckdb_native_function_names() missing rand; got {names:?}"
        );
        // Derived from the same override list, so they cannot drift.
        assert_eq!(
            names.len(),
            crate::dialect::duckdb_scalar_overrides().len(),
            "name list and scalar-override list must have the same length"
        );
    }
}
