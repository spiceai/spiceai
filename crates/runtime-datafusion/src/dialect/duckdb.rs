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

/// `DuckDB`'s name for the both-ends trim `DataFusion` calls `btrim`.
pub(crate) const TRIM_NAME: &str = "trim";

/// The trim set `btrim` uses when called with one argument — see
/// [`btrim_to_trim`] for why it has to be passed to `DuckDB` explicitly.
const ASCII_SPACE: &str = " ";

/// The name both engines give the integer-to-hex function. They disagree only
/// on the case of the digits — see [`to_hex_to_lowercase_hex`].
const TO_HEX_NAME: &str = "to_hex";

/// `DuckDB`'s lower-casing function, applied over [`TO_HEX_NAME`] to match
/// `DataFusion`'s lower-case hex digits.
const LOWER_NAME: &str = "lower";

/// Renders `args` as a call to `duckdb_fn`, in the order given.
///
/// The caller is responsible for having already put `args` into the shape
/// `duckdb_fn` expects — see [`btrim_to_trim`], which has to supply a trim set
/// the `DataFusion` call left implicit. A function needing per-argument
/// inspection or rejection wants
/// [`DuckDBRegexpFunction::to_datafusion_function`]'s shape instead.
fn renamed_fn_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
    duckdb_fn: &str,
) -> Result<Option<ast::Expr>, DataFusionError> {
    let ast_args: Vec<FunctionArg> = args
        .iter()
        .map(|arg| {
            Ok::<FunctionArg, DataFusionError>(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                unparser.expr_to_sql(arg)?,
            )))
        })
        .try_collect()?;

    Ok(Some(ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(duckdb_fn))]),
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
    })))
}

/// Converts `DataFusion`'s `btrim` into `DuckDB`'s [`TRIM_NAME`].
///
/// `trim` is only an alias in `DataFusion`: the function's own name is `btrim`,
/// so that is what the unparser emits, and `DuckDB` has no function of that
/// name. `SELECT trim(name) FROM <a DuckDB-accelerated dataset>` therefore
/// fails with `Catalog Error: Scalar Function with name btrim does not exist!`
/// (issue #13794).
///
/// **The one-argument form is not a bare rename.** `btrim(str)` strips ASCII
/// `U+0020` and nothing else — `general_trim` calls `trim_ascii_char(s, b' ')` —
/// whereas `DuckDB`'s one-argument `trim(str)` strips every Unicode `Zs`
/// separator. On `U+00A0 'x' U+00A0` `DataFusion` returns the string unchanged
/// and `DuckDB` returns `x`, so renaming it would turn a remote error into a
/// silently different answer, which is worse. Passing the trim set explicitly
/// as `trim(str, ' ')` puts `DuckDB` on the character-set path, where it strips
/// exactly the characters given.
///
/// The two-argument form *is* a rename: both engines strip any character of the
/// set from both ends.
pub(crate) fn btrim_to_trim(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>, DataFusionError> {
    match args {
        [input] => renamed_fn_to_sql(
            unparser,
            &[input.clone(), datafusion::prelude::lit(ASCII_SPACE)],
            TRIM_NAME,
        ),
        [_, _] => renamed_fn_to_sql(unparser, args, TRIM_NAME),
        // `btrim`'s signature admits one or two arguments, so the planner
        // cannot build this. Fail rather than fall through to `Ok(None)`,
        // which would put `btrim` back into the DuckDB SQL.
        _ => Err(DataFusionError::Plan(format!(
            "btrim takes one or two arguments, got {}; cannot render it as DuckDB SQL.",
            args.len()
        ))),
    }
}

/// Renders `inner` as the sole argument of a call to `function_name`.
fn wrap_in_call(inner: ast::Expr, function_name: &str) -> ast::Expr {
    ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(Ident::new(
            function_name,
        ))]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
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

/// Lower-cases `DuckDB`'s `to_hex`, which upper-cases the digits `DataFusion`
/// renders in lower case.
///
/// Both engines have a `to_hex`, so nothing denied the call and nothing
/// rewrote it: it was pushed into the accelerated store verbatim and came back
/// with different characters. `to_hex(255)` is `ff` from the kernel and `FF`
/// from `DuckDB`, so the same query over the same rows answered differently
/// depending on whether the dataset happened to be accelerated, with no error
/// and no warning (issue #13818). A predicate such as
/// `WHERE to_hex(h) = 'deadbeef'` simply matched nothing.
///
/// Case is the *only* divergence: measured across `Int16`, `Int32` and `Int64`
/// inputs, negative values, zero and NULL, both engines widen to 64 bits and
/// produce the same digits in the same order, so wrapping the call in
/// [`LOWER_NAME`] makes the two answers identical rather than merely closer.
pub(crate) fn to_hex_to_lowercase_hex(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>, DataFusionError> {
    match args {
        [_] => {
            let Some(hex) = renamed_fn_to_sql(unparser, args, TO_HEX_NAME)? else {
                return Ok(None);
            };
            Ok(Some(wrap_in_call(hex, LOWER_NAME)))
        }
        // `to_hex` is a single-argument function, so the planner cannot build
        // this. Fail rather than fall through to `Ok(None)`, which would put
        // the un-lowered `to_hex` back into the DuckDB SQL.
        _ => Err(DataFusionError::Plan(format!(
            "to_hex takes one argument, got {}; cannot render it as DuckDB SQL.",
            args.len()
        ))),
    }
}

/// Shared conversion for Spice vector UDFs that have a native `DuckDB` ARRAY
/// equivalent taking two equal-length `FLOAT[N]` operands (e.g.
/// `array_cosine_distance`, `array_inner_product`).
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

/// Converts the `cosine_distance` UDF into `DuckDB`'s `array_cosine_distance`:
/// `https://duckdb.org/docs/sql/functions/array.html#array_cosine_distancearray1-array2`
pub(crate) fn cosine_distance_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    spice_array_fn_to_sql(unparser, args, "array_cosine_distance")
}

/// Converts the `inner_product` UDF into `DuckDB`'s `array_inner_product` (dot
/// product, `sum(a[i] * b[i])`): both compute the same value, so federating the
/// call to `DuckDB` (>= 1.5.3) is exact.
/// `https://duckdb.org/docs/sql/functions/array.html#array_inner_productarray1-array2`
pub(crate) fn inner_product_to_sql(
    unparser: &datafusion::sql::unparser::Unparser,
    args: &[Expr],
) -> Result<Option<datafusion::sql::sqlparser::ast::Expr>, DataFusionError> {
    spice_array_fn_to_sql(unparser, args, "array_inner_product")
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
                ast_fn = wrap_in_call(ast_fn, "len");
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
        let expected =
            r#"array_inner_product("table_name"."embedding", [4.0, 5.0, 6.0]::FLOAT[3])"#;
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

    /// `trim` is an alias of `btrim` in `DataFusion`, so the planner resolves
    /// `trim(x)` to a `btrim` call and the unparser emits the function's own
    /// name. `DuckDB` has no `btrim`, which is what issue #13794 hits.
    ///
    /// The one-argument call must carry the trim set: a bare `trim(x)` puts
    /// `DuckDB` on its Unicode-`Zs` path, which `btrim` is not.
    #[test]
    fn btrim_unparses_to_duckdb_trim() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let column = Expr::Column(Column {
            relation: Some(TableReference::bare("t")),
            name: "name".to_string(),
            spans: Spans::new(),
        });

        for (args, expected) in [
            (vec![column.clone()], r#"trim("t"."name", ' ')"#),
            (vec![column, lit("xy")], r#"trim("t"."name", 'xy')"#),
        ] {
            let rendered = btrim_to_trim(&unparser, &args)
                .expect("should execute successfully")
                .expect("should return expression");
            assert_eq!(rendered.to_string(), expected);
        }
    }

    /// `btrim`'s own signature admits only one or two arguments, so this is a
    /// defensive arm — but it must be an error, not `Ok(None)`, which would
    /// hand `btrim` straight back to `DuckDB`.
    #[test]
    fn btrim_with_an_impossible_arity_is_an_error_not_a_passthrough() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());

        let error = btrim_to_trim(&unparser, &[lit("a"), lit("b"), lit("c")])
            .expect_err("three arguments cannot be rendered");
        assert!(
            error
                .to_string()
                .contains("btrim takes one or two arguments"),
            "unexpected error: {error}"
        );
    }

    /// The whole `btrim` call, planned from SQL and unparsed through the
    /// dialect, so a handler that is written but never installed fails here.
    #[test]
    fn duckdb_dialect_installs_the_btrim_override() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions::string::btrim(),
            vec![lit("  hi  ")],
        ));

        let rendered = unparser
            .expr_to_sql(&call)
            .expect("btrim unparses for DuckDB");
        assert_eq!(rendered.to_string(), "trim('  hi  ', ' ')");
    }

    /// Both engines have a `to_hex`, so the call federated verbatim and came
    /// back with upper-case digits where the kernel produces lower-case ones —
    /// a silently different answer, not an error (regression test for #13818).
    #[test]
    fn to_hex_unparses_to_a_lowercased_duckdb_to_hex() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let column = Expr::Column(Column {
            relation: Some(TableReference::bare("t")),
            name: "h".to_string(),
            spans: Spans::new(),
        });

        let rendered = to_hex_to_lowercase_hex(&unparser, &[column])
            .expect("should execute successfully")
            .expect("should return expression");
        assert_eq!(rendered.to_string(), r#"lower(to_hex("t"."h"))"#);
    }

    /// `to_hex` takes exactly one argument, so this is a defensive arm — but it
    /// must be an error, not `Ok(None)`, which would hand the un-lowered
    /// `to_hex` straight back to `DuckDB`.
    #[test]
    fn to_hex_with_an_impossible_arity_is_an_error_not_a_passthrough() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());

        let error = to_hex_to_lowercase_hex(&unparser, &[lit(1_i64), lit(2_i64)])
            .expect_err("two arguments cannot be rendered");
        assert!(
            error.to_string().contains("to_hex takes one argument"),
            "unexpected error: {error}"
        );
    }

    /// The whole `to_hex` call, planned from the `DataFusion` UDF and unparsed
    /// through the dialect, so a handler that is written but never installed
    /// fails here rather than in a federated query.
    #[test]
    fn duckdb_dialect_installs_the_to_hex_override() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions::string::to_hex(),
            vec![lit(255_i64)],
        ));

        let rendered = unparser
            .expr_to_sql(&call)
            .expect("to_hex unparses for DuckDB");
        assert_eq!(rendered.to_string(), "lower(to_hex(255))");
    }

    #[test]
    fn duckdb_native_function_names_advertises_denylisted_pushables() {
        // The federation deny-list relies on these names to let `cosine_distance`
        // and `rand` push down to DuckDB, so the dialect must advertise them.
        let names = crate::dialect::duckdb_native_function_names();
        assert!(
            names.contains(&runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME),
            "duckdb_native_function_names() missing cosine_distance; got {names:?}"
        );
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
