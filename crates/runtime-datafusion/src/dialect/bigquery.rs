/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! `BigQuery` translations for the JSON extraction functions and the regexp
//! predicate, and the `BigQuery` dialect that installs them.
//!
//! `datafusion-functions-json` takes a **variadic path**, not a `JSONPath`
//! string: `json_get_int(col, 'a', 'b', 0)` reads key `a`, then key `b`, then
//! array element 0. A string argument is always an object key — `('a.b')` is
//! one key literally named `a.b`, not a two-step path — and an integer argument
//! is always an array index. [`json_path`] is the one place that mapping is
//! written down, and both the translation and the pushdown policy read it, so
//! what the dialect can render and what the deny-list lets through cannot
//! diverge.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::timezone::Tz;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, SortExpr};
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, CaseWhen, Function, FunctionArg, FunctionArgExpr, ObjectName,
    WindowFrameBound,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{
    BigQueryDialect, CharacterLengthStyle, DateFieldExtractStyle, Dialect, IntervalStyle,
    ScalarFnToSqlHandler,
};

pub(crate) const JSON_GET_INT_NAME: &str = "json_get_int";
pub(crate) const JSON_GET_STR_NAME: &str = "json_get_str";
pub(crate) const JSON_GET_BOOL_NAME: &str = "json_get_bool";
pub(crate) const JSON_GET_FLOAT_NAME: &str = "json_get_float";
pub(crate) const JSON_LENGTH_NAME: &str = "json_length";
/// `json_length`'s alias. `ScalarUDF::name` returns the canonical name, so a
/// plan never carries this one — but the federation deny-list is built from
/// the registry, which does, and a name it denies that this dialect cannot
/// render would be inconsistent. Both names carry the same handler.
pub(crate) const JSON_LEN_NAME: &str = "json_len";
pub(crate) const JSON_OBJECT_KEYS_NAME: &str = "json_object_keys";
/// `json_object_keys`'s alias, carried for the reason [`JSON_LEN_NAME`] gives.
pub(crate) const JSON_KEYS_NAME: &str = "json_keys";

/// The first byte of a JSON string node's normalized JSON token.
///
/// `json_get_str` answers only for a JSON **string** — `jiter`'s `Peek::String`
/// — and NULL for every other node. `JSON_VALUE` cannot express that on its
/// own: it renders a number as its digits and a bool as `true`/`false`, where
/// `json_get_str` returns NULL. `JSON_QUERY` returns either a native `JSON`
/// value or a JSON-formatted `STRING`, matching the document's input type;
/// `FORMAT('%t', …)` normalizes both forms to printable JSON text. A leading
/// double quote then distinguishes a string node from every other type. See
/// [`json_get_str_to_sql`].
const JSON_STRING_TOKEN_PREFIX: &str = "\"";

/// The grammar Rust's `i64::FromStr` accepts, which is what
/// `json_get_int` applies to a JSON **string** node.
///
/// `SAFE_CAST(… AS INT64)` on its own is wider than that — `BigQuery` reads a
/// hexadecimal literal, and trims surrounding whitespace — so extracting
/// through this pattern first is what makes the string case exact. Everything
/// it rejects, `json_get_int` also rejects, and returns NULL for.
///
/// It also agrees at the boundaries: an integer too large for `i64` is NULL on
/// both sides, because Rust's `i64::FromStr` fails rather than saturating.
/// See [`FLOAT64_FROM_STR`] for the float grammar, which agrees for the
/// opposite reason — both sides saturate.
///
/// Any group must be non-capturing. `REGEXP_EXTRACT` accepts **at most one**
/// capturing group and errors on more, which would fail every federated call
/// remotely; with none it returns the whole match, which is what this wants.
/// [`tests::no_pattern_has_a_capturing_group`] holds the patterns to that.
const INT64_FROM_STR: &str = r"^[+-]?[0-9]+$";

/// The grammar Rust's `f64::FromStr` accepts, which is what `json_get_float`
/// applies to a JSON **string** node.
///
/// Measured against `BigQuery`: `SAFE_CAST(… AS FLOAT64)` saturates an
/// out-of-range magnitude to `±Infinity` and underflows to zero, exactly as
/// Rust does, and reads `inf`, `infinity` and `nan` case-insensitively, exactly
/// as Rust does. So the boundaries need no special rendering — they already
/// agree.
///
/// What does not agree is the same pair the integer form has: `SAFE_CAST`
/// reads `0x2A` as 42 and trims `  1.5  `, where `f64::FromStr` fails and
/// `json_get_float` is NULL. Extracting through this pattern first is what
/// closes that, and nothing else.
///
/// Every group is non-capturing, for the reason [`INT64_FROM_STR`] gives.
const FLOAT64_FROM_STR: &str = r"^[+-]?(?:(?i:inf|infinity|nan)|(?:[0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+)(?:[eE][+-]?[0-9]+)?)$";

/// What `BigQuery` should be asked for, given a `json_get_*` call's path
/// arguments.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JsonPath {
    /// A `BigQuery` JSON path — `$."a"."b"[0]` — reaching the same node the
    /// variadic path names.
    Path(String),
    /// The path can never resolve, so the call is NULL for every row.
    /// `datafusion-functions-json` maps a negative index and a NULL path
    /// element to its own `JsonPath::None`, which no lookup ever matches.
    NeverResolves,
}

/// Builds the `BigQuery` JSON path for a `json_get_*` call, or `None` when the
/// call has a shape `BigQuery` cannot be asked for.
///
/// The one shape with no translation is a **non-literal** path element:
/// `json_get_int(doc, key_col)` is legal — the functions are
/// `Signature::variadic_any` — and `BigQuery`'s JSON path argument must be a
/// constant, so there is nothing to emit. Such a call must not federate at all;
/// `crate::function_support` is what stops it, using this same function.
pub(crate) fn json_path(args: &[Expr]) -> Option<JsonPath> {
    // The first argument is the document; the rest are the path.
    let (_document, path) = args.split_first()?;
    if path.is_empty() {
        // `json_get_int(col)` reads the document itself. `$` is the JSONPath
        // for that, but what BigQuery returns for a bare `$` over a scalar
        // document is not pinned by anything here, so it is left to evaluate
        // locally rather than guessed at.
        return None;
    }

    let mut rendered = String::from("$");
    for element in path {
        let Expr::Literal(value, _) = element else {
            return None;
        };
        match value {
            ScalarValue::Utf8(Some(key))
            | ScalarValue::LargeUtf8(Some(key))
            | ScalarValue::Utf8View(Some(key)) => {
                if !key_is_renderable(key) {
                    return None;
                }
                // BigQuery quotes a key with double quotes, which is what lets
                // a key containing `.` stay one key.
                let _ = write!(rendered, r#"."{key}""#);
            }
            ScalarValue::Int64(Some(index)) if *index >= 0 => {
                let _ = write!(rendered, "[{index}]");
            }
            ScalarValue::UInt64(Some(index)) => {
                let _ = write!(rendered, "[{index}]");
            }
            // A negative index, or a NULL path element of a type the function
            // accepts, resolves to nothing for every row.
            ScalarValue::Int64(Some(_) | None)
            | ScalarValue::UInt64(None)
            | ScalarValue::Utf8(None)
            | ScalarValue::LargeUtf8(None)
            | ScalarValue::Utf8View(None) => return Some(JsonPath::NeverResolves),
            // Any other literal type is rejected by the function's own
            // `return_type`, so it cannot reach a plan.
            _ => return None,
        }
    }
    Some(JsonPath::Path(rendered))
}

/// Whether a key can be written into the path at all.
///
/// The path is emitted as a `BigQuery` **raw** string literal, so what is
/// written is what the JSON path parser reads: nothing processes backslashes in
/// between. That holds only while the key contains nothing either layer would
/// read as structure — `\'` would end the literal, `"` would end the quoted
/// field name, `\\` is an escape to the JSON path parser, and a control
/// character has no agreed spelling in either. Escaping across two layers with
/// different rules is how a key silently becomes a *different* path, so such a
/// key is left for the local engine instead.
fn key_is_renderable(key: &str) -> bool {
    !key.contains(['\'', '"', '\\']) && !key.chars().any(char::is_control)
}

/// Whether the path arguments of a `json_get_*` call can be rendered. Reads
/// [`json_path`], so it answers exactly the question the handlers can answer.
fn json_path_is_renderable(args: &[Expr]) -> bool {
    json_path(args).is_some()
}

/// Whether the `BigQuery` dialect can translate this call, for the pushdown
/// policy to consult.
///
/// A function with no entry in [`SCALAR_OVERRIDES`] or
/// [`BUILTIN_SCALAR_OVERRIDES`] is not this check's business: a Spice function
/// the deny-list has not carved out is already denied, and a `DataFusion`
/// built-in with no handler here is either denied by name (`regexp_match`) or
/// unparses through the inner dialect.
#[must_use]
pub fn can_translate(call: &ScalarFunction) -> bool {
    let name = call.func.name();
    SCALAR_OVERRIDES
        .iter()
        .chain(BUILTIN_SCALAR_OVERRIDES)
        .find(|entry| entry.name == name)
        .is_none_or(|entry| (entry.can_translate)(&call.args))
}

/// A function the `BigQuery` dialect rewrites into native SQL.
///
/// The handler and the per-call check are one entry because they are one fact:
/// a handler that can only render some call shapes is safe only while the
/// deny-list refuses the rest. Splitting them into a list and a separate match
/// is what lets a later partial handler be carved out of the deny-list with
/// nothing to refuse its untranslatable shapes, which puts the function name
/// verbatim into the remote SQL.
pub(crate) struct ScalarOverride {
    pub(crate) name: &'static str,
    /// Renders the call, or fails if it cannot — see [`json_get_number_to_sql`].
    pub(crate) handler: fn(&Unparser, &[Expr]) -> Result<Option<ast::Expr>>,
    /// Whether `handler` can render a call with these arguments.
    pub(crate) can_translate: fn(&[Expr]) -> bool,
}

/// Every function the `BigQuery` dialect rewrites, with what each consumer
/// needs. [`crate::dialect`] derives the dialect's handlers, the deny-list
/// carve-out, and the per-call check from this one table.
pub(crate) const SCALAR_OVERRIDES: &[ScalarOverride] = &[
    ScalarOverride {
        name: JSON_GET_INT_NAME,
        handler: json_get_int_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_GET_STR_NAME,
        handler: json_get_str_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_GET_BOOL_NAME,
        handler: json_get_bool_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_GET_FLOAT_NAME,
        handler: json_get_float_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_LENGTH_NAME,
        handler: json_length_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_LEN_NAME,
        handler: json_length_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_OBJECT_KEYS_NAME,
        handler: json_object_keys_to_sql,
        can_translate: json_path_is_renderable,
    },
    ScalarOverride {
        name: JSON_KEYS_NAME,
        handler: json_object_keys_to_sql,
        can_translate: json_path_is_renderable,
    },
];

/// The `DataFusion` built-ins the `BigQuery` dialect rewrites into native SQL.
///
/// A separate table from [`SCALAR_OVERRIDES`] because the deny-list treats the
/// two differently: a Spice function must be carved out of the deny-list by
/// name to federate at all, while a built-in federates unless denied, so
/// putting one in the carve-out would do nothing. What a built-in needs is the
/// other two pieces — a handler, because the unparser otherwise emits the call
/// verbatim into SQL `BigQuery` rejects, and a per-call check, because the
/// handler can only render some call shapes and the rest must stay local.
pub(crate) const BUILTIN_SCALAR_OVERRIDES: &[ScalarOverride] = &[
    ScalarOverride {
        name: super::REGEXP_LIKE_NAME,
        handler: regexp_like_to_sql,
        can_translate: regexp_like_is_renderable,
    },
    ScalarOverride {
        name: "array_element",
        handler: array_element_to_sql,
        can_translate: array_index_is_renderable,
    },
];

/// `array_element` is 1-based and counts from the end for a negative index.
/// `BigQuery` has no end-relative subscript, so the fork's dialect renders only
/// a non-negative index and refuses the rest; this is the check that keeps the
/// refusal off the pushdown path, leaving such a call to evaluate locally rather
/// than failing the query.
fn array_index_is_renderable(args: &[Expr]) -> bool {
    let [_, Expr::Literal(index, _)] = args else {
        return false;
    };
    index.data_type().is_integer()
        && matches!(
            index.cast_to(&DataType::Int64),
            Ok(ScalarValue::Int64(Some(index))) if index >= 0
        )
}

/// Defers to the fork's `BigQuery` rendering, which spells the subscript
/// `SAFE_ORDINAL` so it agrees with `array_element`'s 1-based indexing.
///
/// Registered here only so [`can_translate`] can refuse the indexes that
/// rendering will not take. Returning `Ok(None)` instead would fall through to
/// the generic 0-based subscript, which reads the neighbouring element.
fn array_element_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    BigQueryDialect::new().scalar_function_to_sql_overrides(unparser, "array_element", args)
}

/// Renders one `json_get_*` call: pulls out the document and the JSON path,
/// and hands both to `render` as `BigQuery` SQL.
///
/// Every handler goes through here so the failure below is written once. It is
/// unreachable with the deny-list installed, which refuses exactly the calls
/// [`json_path`] cannot render. Reachable only if this dialect is used without
/// `deny_spice_functions_for_bigquery_table_providers`, and there the
/// alternative — returning `Ok(None)` — makes the unparser emit `json_get_*`
/// verbatim into `BigQuery` SQL, which is the wrong answer dressed as a remote
/// error. Fail where it can be read instead.
///
/// `null_type` is what a path that can never resolve renders as. It carries the
/// function's own return type: an untyped NULL would leave the federated schema
/// disagreeing with the plan's.
fn json_call_to_sql(
    unparser: &Unparser,
    args: &[Expr],
    function: &str,
    null_type: ast::DataType,
    render: impl FnOnce(ast::Expr, ast::Expr) -> ast::Expr,
) -> Result<Option<ast::Expr>> {
    let (Some(document), Some(path)) = (args.first(), json_path(args)) else {
        return Err(DataFusionError::Plan(format!(
            "Failed to run this query against BigQuery: '{function}' was called in a form BigQuery \
             cannot express, so the query cannot be completed. BigQuery needs a constant JSON path \
             built only from plain keys: every path argument must be a literal, there must be at \
             least one, and a key cannot contain a quote, a backslash or a control character. \
             Rewrite the call to a constant path of plain keys, or set 'query_federation: disabled' \
             on the dataset to evaluate it locally instead. \
             See: https://spiceai.org/docs/components/data-connectors/adbc"
        )));
    };

    let path = match path {
        JsonPath::NeverResolves => return Ok(Some(cast_null_to(null_type))),
        JsonPath::Path(path) => path,
    };

    Ok(Some(render(
        unparser.expr_to_sql(document)?,
        ast::Expr::Value(raw_string(&path).into()),
    )))
}

/// `json_get_int(doc, path…)` →
/// `SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(doc, '<path>'), r'^[+-]?[0-9]+$') AS INT64)`.
///
/// `JSON_VALUE` renders the node at the path as a string and returns NULL for
/// an object, an array, a JSON `null` and a missing path — which is what
/// `json_get_int` returns for all four. `true` and `false` render as `"true"`
/// and `"false"`, which the pattern rejects, matching `json_get_int` again. A
/// number renders as its own token, so a float or an exponent form is rejected
/// exactly as `json_get_int` rejects it, and an integer outside `INT64` is a
/// NULL from `SAFE_CAST` exactly as it is a NULL from the `i64` conversion. The
/// pattern is what makes a JSON **string** node exact — see [`INT64_FROM_STR`].
pub(crate) fn json_get_int_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    json_get_number_to_sql(
        unparser,
        args,
        JSON_GET_INT_NAME,
        INT64_FROM_STR,
        ast::DataType::Int64,
    )
}

/// `json_get_str(doc, path…)` →
/// `CASE WHEN STARTS_WITH(FORMAT('%t', JSON_QUERY(doc, '<path>')), '"') THEN JSON_VALUE(doc, '<path>') END`.
///
/// `json_get_str` answers only for a JSON **string** node and NULL for every
/// other kind. `JSON_VALUE` alone is wider than that: it renders a number as
/// its digits and a bool as `true`/`false`, so it would answer a string where
/// `json_get_str` answers NULL — on rows a `WHERE … IS NOT NULL` then keeps
/// remotely and drops locally.
///
/// `JSON_QUERY` preserves the document representation: it returns `JSON` for a
/// native `JSON` document and `STRING` for a JSON-formatted string document.
/// `FORMAT('%t', …)` turns both results into the node's printable JSON token,
/// where only a string node opens with a double quote — a number, a bool, a
/// JSON `null`, an object and an array all render bare. Testing that first byte
/// is what narrows `JSON_VALUE` to exactly the nodes the local function answers
/// for, so the guard is the whole reason this is translatable at all.
///
/// Where the two already agree, no guard is needed: `JSON_QUERY` returns SQL
/// NULL for a missing path, and `STARTS_WITH` over NULL is NULL, so the `CASE`
/// falls through to its implicit NULL — which is what `json_get_str` returns.
/// The escape handling agrees too: `JSON_VALUE` unescapes, and so does
/// `jiter`'s `known_str`, so the guard reads the raw token while the value
/// comes back decoded.
pub(crate) fn json_get_str_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    json_call_to_sql(
        unparser,
        args,
        JSON_GET_STR_NAME,
        ast::DataType::String(None),
        |document, path| {
            let normalized_json_token = call_function(
                "FORMAT",
                vec![
                    ast::Expr::Value(ast::Value::SingleQuotedString("%t".to_string()).into()),
                    call_function("JSON_QUERY", vec![document.clone(), path.clone()]),
                ],
            );
            let is_string_node = call_function(
                "STARTS_WITH",
                vec![
                    normalized_json_token,
                    ast::Expr::Value(
                        ast::Value::SingleQuotedString(JSON_STRING_TOKEN_PREFIX.to_string()).into(),
                    ),
                ],
            );
            ast::Expr::Case {
                case_token: AttachedToken::empty(),
                end_token: AttachedToken::empty(),
                operand: None,
                conditions: vec![CaseWhen {
                    condition: is_string_node,
                    result: call_function("JSON_VALUE", vec![document, path]),
                }],
                // No ELSE: a CASE with no matching WHEN is NULL, which is what
                // `json_get_str` returns for every non-string node.
                else_result: None,
            }
        },
    )
}

/// `json_get_bool(doc, path…)` →
/// `CASE JSON_VALUE(doc, '<path>') WHEN 'true' THEN TRUE WHEN 'false' THEN FALSE END`.
///
/// `json_get_bool` answers for a JSON `true`/`false`, and for a JSON **string**
/// that Rust's `bool::from_str` accepts — which is `"true"` and `"false"`
/// exactly, case-sensitively. Everything else is NULL.
///
/// Comparing `JSON_VALUE`'s rendering is what matches that, and the reason no
/// cast appears here. Measured against `BigQuery`: `SAFE_CAST('TRUE' AS BOOL)`
/// and `SAFE_CAST('True' AS BOOL)` both give `true`, where `bool::from_str`
/// rejects both and the local function returns NULL. A string comparison is
/// case-sensitive, so the two agree on exactly the same values.
/// [`tests::no_bool_rendering_casts_to_bool`] is what keeps the cast out.
///
/// The single `JSON_VALUE` covers both accepted shapes at once: it renders a
/// bool node as `true`/`false` and a string node as its decoded contents, so
/// `"true"` and `true` both arrive as `'true'` — which is what `json_get_bool`
/// does too. It decodes escapes on the way, so a string written `"tr\u0075e"`
/// is `true` on both sides. A number renders as its digits and never matches,
/// and an object, an array, a JSON `null` and a missing path are NULL from
/// `JSON_VALUE` — NULL from `json_get_bool` as well.
pub(crate) fn json_get_bool_to_sql(
    unparser: &Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>> {
    json_call_to_sql(
        unparser,
        args,
        JSON_GET_BOOL_NAME,
        ast::DataType::Bool,
        |document, path| {
            let arm = |literal: &str, value: bool| CaseWhen {
                condition: ast::Expr::Value(
                    ast::Value::SingleQuotedString(literal.to_string()).into(),
                ),
                result: ast::Expr::Value(ast::Value::Boolean(value).into()),
            };
            ast::Expr::Case {
                case_token: AttachedToken::empty(),
                end_token: AttachedToken::empty(),
                operand: Some(Box::new(call_function("JSON_VALUE", vec![document, path]))),
                conditions: vec![arm("true", true), arm("false", false)],
                // No ELSE: everything `bool::from_str` rejects is NULL on both sides.
                else_result: None,
            }
        },
    )
}

/// `json_get_float(doc, path…)` →
/// `SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(doc, '<path>'), r'<float grammar>') AS FLOAT64)`.
///
/// The same shape as [`json_get_int_to_sql`], because the same two things are
/// true: `JSON_VALUE` renders the node as its own token, and `SAFE_CAST` is
/// wider than Rust's `FromStr` in ways a pattern can close.
///
/// The boundaries need nothing extra. `SAFE_CAST(… AS FLOAT64)` saturates an
/// out-of-range magnitude to `±Infinity` and underflows to zero exactly as
/// `f64::FromStr` does, and accepts `inf`/`infinity`/`nan` with the same
/// case-insensitivity — all measured against `BigQuery`.
/// `json_get_float_saturates_an_out_of_range_magnitude_to_infinity` in
/// `runtime-udfs-api` holds the local half of that agreement.
pub(crate) fn json_get_float_to_sql(
    unparser: &Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>> {
    json_get_number_to_sql(
        unparser,
        args,
        JSON_GET_FLOAT_NAME,
        FLOAT64_FROM_STR,
        ast::DataType::Float64,
    )
}

/// `json_length(doc, path…)` → a `CASE` counting an array's elements or an
/// object's keys, and NULL for every other node.
///
/// `json_length` answers only for an array and an object. `BigQuery` counts the
/// two with different functions, so the node's own JSON token picks the branch:
/// only an array's opens with `[` and only an object's with `{`.
///
/// * array — `ARRAY_LENGTH(JSON_QUERY_ARRAY(doc, path))`.
/// * object — `ARRAY_LENGTH(JSON_KEYS(SAFE.PARSE_JSON(JSON_QUERY(doc, path)), 1))`.
///   The depth argument is load-bearing: without it `JSON_KEYS` descends,
///   returning `["b", "c", "c.d"]` for `{"b":1,"c":{"d":2}}` where
///   `json_length` counts two. `SAFE.PARSE_JSON` cannot fail the query — it is
///   NULL for anything it will not parse, and `JSON_KEYS` and `ARRAY_LENGTH`
///   carry that NULL out — so neither `CASE` branch can raise where the local
///   function returns a number.
///
/// An empty array and an empty object are both 0 on both sides, and every
/// scalar node, a JSON `null`, a missing path and a NULL document are NULL on
/// both — all measured against `BigQuery`.
///
/// The local function returns `UInt64` and `BigQuery` has no unsigned type, so
/// the count arrives as `INT64` and the scan's schema cast reconciles it. A
/// count is never negative, so the conversion cannot lose one.
pub(crate) fn json_length_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    json_call_to_sql(
        unparser,
        args,
        JSON_LENGTH_NAME,
        ast::DataType::Int64,
        |document, path| {
            let token = || call_function("JSON_QUERY", vec![document.clone(), path.clone()]);
            let opens_with = |brace: &str| {
                call_function(
                    "STARTS_WITH",
                    vec![
                        token(),
                        ast::Expr::Value(ast::Value::SingleQuotedString(brace.to_string()).into()),
                    ],
                )
            };
            ast::Expr::Case {
                case_token: AttachedToken::empty(),
                end_token: AttachedToken::empty(),
                operand: None,
                conditions: vec![
                    CaseWhen {
                        condition: opens_with("["),
                        result: call_function(
                            "ARRAY_LENGTH",
                            vec![call_function(
                                "JSON_QUERY_ARRAY",
                                vec![document.clone(), path.clone()],
                            )],
                        ),
                    },
                    CaseWhen {
                        condition: opens_with("{"),
                        result: call_function(
                            "ARRAY_LENGTH",
                            vec![call_function(
                                "JSON_KEYS",
                                vec![parse_json(token()), depth(1)],
                            )],
                        ),
                    },
                ],
                // No ELSE: json_length is NULL for every node that is neither.
                else_result: None,
            }
        },
    )
}

/// `json_object_keys(doc, path…)` →
/// `CASE WHEN STARTS_WITH(JSON_QUERY(doc, '<path>'), '{')
///  THEN JSON_KEYS(SAFE.PARSE_JSON(JSON_QUERY(doc, '<path>')), 1) END`.
///
/// `json_object_keys` answers only for an object, and only with that object's
/// own keys. The depth argument is what holds `JSON_KEYS` to the same level —
/// without it `BigQuery` descends and returns `["b", "c", "c.d"]` where the local
/// function returns `["b", "c"]`. The `{` test is what makes every other node
/// NULL, matching the local function, since what `JSON_KEYS` does with an array
/// or a scalar is not something this rests on.
///
/// This is the one function here whose element type has to survive the trip:
/// it returns `List(Field { name: "item", … })`, and `BigQuery` sends back an
/// `ARRAY<STRING>` whose element field the driver names. A disagreement there
/// would fail the query rather than answer it differently, so unlike the rest
/// of this module the risk is loud. Measured against a real `BigQuery` through
/// the ADBC driver: the array arrives as a `List(Utf8)` the plan accepts.
pub(crate) fn json_object_keys_to_sql(
    unparser: &Unparser,
    args: &[Expr],
) -> Result<Option<ast::Expr>> {
    json_call_to_sql(
        unparser,
        args,
        JSON_OBJECT_KEYS_NAME,
        ast::DataType::Array(ast::ArrayElemTypeDef::AngleBracket(Box::new(
            ast::DataType::String(None),
        ))),
        |document, path| {
            let token = call_function("JSON_QUERY", vec![document, path]);
            ast::Expr::Case {
                case_token: AttachedToken::empty(),
                end_token: AttachedToken::empty(),
                operand: None,
                conditions: vec![CaseWhen {
                    condition: call_function(
                        "STARTS_WITH",
                        vec![
                            token.clone(),
                            ast::Expr::Value(
                                ast::Value::SingleQuotedString("{".to_string()).into(),
                            ),
                        ],
                    ),
                    result: call_function("JSON_KEYS", vec![parse_json(token), depth(1)]),
                }],
                else_result: None,
            }
        },
    )
}

fn json_get_number_to_sql(
    unparser: &Unparser,
    args: &[Expr],
    function: &str,
    pattern: &str,
    cast_to: ast::DataType,
) -> Result<Option<ast::Expr>> {
    json_call_to_sql(
        unparser,
        args,
        function,
        cast_to.clone(),
        |document, path| ast::Expr::Cast {
            kind: ast::CastKind::SafeCast,
            expr: Box::new(call_function(
                "REGEXP_EXTRACT",
                vec![
                    call_function("JSON_VALUE", vec![document, path]),
                    ast::Expr::Value(raw_string(pattern).into()),
                ],
            )),
            data_type: cast_to,
            array: false,
            format: None,
        },
    )
}

/// `regexp_like(str, pattern[, flags])` →
/// `REGEXP_CONTAINS(str, r'<pattern>')`, with literal flags folded into the
/// pattern as an inline `(?ims)` group.
///
/// The two agree call-for-call: both answer whether the pattern matches
/// anywhere in the string, both are a plain `BOOL`, and both are NULL when the
/// string or the pattern is NULL. That exactness holds only for a pattern both
/// regex engines read identically, which is what [`regexp_contains`] holds the
/// translation to; every other shape is refused by the per-call check and
/// evaluated locally.
///
/// The failure below is written for the same reason [`json_call_to_sql`]'s is:
/// unreachable with the deny-list's per-call check installed, and the
/// alternative — `Ok(None)` — makes the unparser emit `regexp_like` verbatim
/// into `BigQuery` SQL, which fails remotely as `Function not found`.
pub(crate) fn regexp_like_to_sql(unparser: &Unparser, args: &[Expr]) -> Result<Option<ast::Expr>> {
    let Some(call) = regexp_contains(args) else {
        return Err(DataFusionError::Plan(format!(
            "Failed to run this query against BigQuery: '{name}' was called in a form BigQuery \
             cannot express, so the query cannot be completed. BigQuery needs a constant pattern \
             both regular-expression engines read identically: the pattern and any flags must be \
             literals, the only supported flags are 'i', 'm' and 's' (as the flags argument or \
             an inline (?...) group), and the pattern cannot contain a quote, a control or \
             non-ASCII character, or the classes \\d, \\D, \\w, \\W, \\s, \\S, \\b, \\B, \\p or \
             \\P. Use a plain constant pattern, or set 'query_federation: disabled' on the \
             dataset to evaluate it locally instead. \
             See: https://spiceai.org/docs/components/data-connectors/adbc",
            name = super::REGEXP_LIKE_NAME,
        )));
    };
    Ok(Some(call_function(
        "REGEXP_CONTAINS",
        vec![
            unparser.expr_to_sql(call.input)?,
            ast::Expr::Value(raw_string(&call.pattern).into()),
        ],
    )))
}

/// Whether the arguments of a `regexp_like` call can be rendered. Reads
/// [`regexp_contains`], so it answers exactly the question the handler can
/// answer.
fn regexp_like_is_renderable(args: &[Expr]) -> bool {
    regexp_contains(args).is_some()
}

/// The `REGEXP_CONTAINS` call a `regexp_like` invocation translates into.
struct RegexpContains<'a> {
    input: &'a Expr,
    /// The pattern with any flags already folded in as an inline group.
    pattern: String,
}

/// Builds the `REGEXP_CONTAINS` arguments for a `regexp_like` call, or `None`
/// when the call has a shape whose remote behavior is not pinned to the local
/// one: a non-literal pattern or flags argument, a flag with no `BigQuery`
/// equivalent, or a pattern the two engines read differently — see
/// [`pattern_is_engine_agnostic`].
fn regexp_contains(args: &[Expr]) -> Option<RegexpContains<'_>> {
    let (input, pattern, flags) = match args {
        [input, pattern] => (input, pattern, ""),
        [input, pattern, flags] => (input, pattern, literal_utf8(flags)?),
        _ => return None,
    };
    let pattern = literal_utf8(pattern)?;
    if !pattern_is_engine_agnostic(pattern) {
        return None;
    }
    let flags = folded_flags(flags)?;
    let pattern = if flags.is_empty() {
        pattern.to_string()
    } else {
        format!("(?{flags}){pattern}")
    };
    Some(RegexpContains { input, pattern })
}

/// The text of a Utf8-family literal, or `None` for anything else — including
/// a typed NULL, which has no text to scan.
fn literal_utf8(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => Some(text),
        _ => None,
    }
}

/// Whether the local engine — Rust's `regex` crate — and `BigQuery`'s RE2 read
/// this pattern to mean the same thing.
///
/// The two are near relatives, but the differences are silent: a diverging
/// pattern changes *which rows match*, not whether the query runs. The known
/// divergences are Unicode, inline modes, character-class algebra, and repeat
/// bounds. The Perl classes and word
/// boundaries (`\d`, `\w`, `\s`, `\b` and their negations) are Unicode-aware
/// in Rust's `regex` and ASCII-only in RE2, so `\d` matches an Arabic-Indic
/// digit locally and not remotely, and `\p{…}`/`\P{…}` lean on each engine's
/// Unicode tables. Inline `(?…)` modifier groups carry the same hazard in
/// mode form: Rust reads modes RE2 does not have — `(?x)a b` (extended mode)
/// matches `ab` locally and is a syntax error remotely — so a group head is
/// accepted only when [`group_options_are_engine_agnostic`] can read it as
/// something both engines agree on. Rather than enumerate agreements, this
/// accepts only patterns built from constructs with one reading: printable
/// ASCII, with no escape of those class letters and no group options beyond
/// `i`, `m` and `s`. Rust's set operators inside character classes (`&&`,
/// `--`, `~~`) are rejected because RE2 reads them as literal punctuation,
/// and counted repetition bounds above RE2's 1000 limit stay local.
/// [`tests::rusts_perl_classes_are_unicode_aware`],
/// [`tests::rust_only_inline_modes_exist`], and
/// [`tests::rust_supports_regex_features_re2_does_not`] hold the local halves
/// of the divergences this guards against.
///
/// A single quote and control characters are rejected for a different reason:
/// the pattern is emitted as a `BigQuery` **raw** string literal (see
/// [`raw_string`]), which a `'` would terminate and a control character has no
/// spelling in.
fn pattern_is_engine_agnostic(pattern: &str) -> bool {
    let mut chars = pattern.chars().peekable();
    let mut in_character_class = false;
    let mut previous_class_character = None;
    while let Some(c) = chars.next() {
        if !c.is_ascii() || c.is_ascii_control() || c == '\'' {
            return false;
        }
        match c {
            '\\' => match chars.next() {
                // A trailing backslash is an invalid pattern; refuse rather
                // than reason about which engine rejects it first.
                None => return false,
                Some(escaped) => {
                    if !escaped.is_ascii() || escaped.is_ascii_control() || escaped == '\'' {
                        return false;
                    }
                    if matches!(
                        escaped,
                        'd' | 'D' | 'w' | 'W' | 's' | 'S' | 'b' | 'B' | 'p' | 'P'
                    ) {
                        return false;
                    }
                    previous_class_character = None;
                }
            },
            '[' if in_character_class && chars.peek() == Some(&':') => {
                if !consume_posix_class(&mut chars) {
                    return false;
                }
                previous_class_character = None;
            }
            '[' if in_character_class => {
                // Nested character classes participate in Rust's set algebra
                // but are not a portable RE2 construct.
                return false;
            }
            '[' => {
                in_character_class = true;
                previous_class_character = None;
            }
            ']' if in_character_class => {
                in_character_class = false;
                previous_class_character = None;
            }
            '&' | '-' | '~' if in_character_class => {
                if previous_class_character == Some(c) {
                    return false;
                }
                previous_class_character = Some(c);
            }
            _ if in_character_class => previous_class_character = Some(c),
            '(' if chars.peek() == Some(&'?') => {
                chars.next();
                if !group_options_are_engine_agnostic(&mut chars) {
                    return false;
                }
            }
            '{' if counted_repetition_exceeds_re2_limit(&chars) => return false,
            _ => {}
        }
    }
    true
}

/// Whether the characters after an unescaped `{` begin a counted repetition
/// whose lower or upper bound exceeds RE2's hard limit of 1000. Invalid or
/// non-repetition brace text is left for the local regex compiler to diagnose.
fn counted_repetition_exceeds_re2_limit(chars: &std::iter::Peekable<std::str::Chars>) -> bool {
    let mut chars = chars.clone();
    let Some(lower) = repetition_bound(&mut chars) else {
        return false;
    };
    if lower > 1000 {
        return true;
    }
    matches!(chars.next(), Some(','))
        && repetition_bound(&mut chars).is_some_and(|upper| upper > 1000)
}

fn repetition_bound(chars: &mut std::iter::Peekable<std::str::Chars>) -> Option<u32> {
    let mut value: Option<u32> = None;
    while let Some(digit) = chars.peek().and_then(|c| c.to_digit(10)) {
        chars.next();
        value = Some(value.unwrap_or(0).saturating_mul(10).saturating_add(digit));
    }
    value
}

/// Consume the remainder of a POSIX class such as `[:digit:]` after its
/// opening `[` has already been read. Both engines support these ASCII
/// classes, and treating the inner `[` as Rust class nesting would otherwise
/// unnecessarily keep them local.
fn consume_posix_class(chars: &mut std::iter::Peekable<std::str::Chars>) -> bool {
    if chars.next() != Some(':') {
        return false;
    }
    let mut saw_name_character = false;
    while let Some(c) = chars.next() {
        if c == ':' && chars.peek() == Some(&']') {
            chars.next();
            return saw_name_character;
        }
        if !c.is_ascii_alphabetic() {
            return false;
        }
        saw_name_character = true;
    }
    false
}

/// Reads the `…` of a `(?…` group head, accepting only what both engines
/// agree on: `:` (a plain non-capturing group), or inline flags from
/// `i`/`m`/`s` — optionally negated after one `-` — closed by `)` (a flag
/// directive) or `:` (a scoped group). Everything else is refused: Rust-only
/// modes (`x` extended, `R` CRLF, `u` Unicode toggles), `U` (deliberately
/// refused as a flags argument, so its inline spelling must not slip
/// through), named groups, and lookarounds. An empty directive like `(?)` is
/// refused too — both engines reject it, and the local error is the readable
/// one.
fn group_options_are_engine_agnostic(chars: &mut std::iter::Peekable<std::str::Chars>) -> bool {
    if chars.peek() == Some(&':') {
        chars.next();
        return true;
    }
    let mut saw_flag = false;
    let mut saw_dash = false;
    loop {
        match chars.next() {
            Some('i' | 'm' | 's') => saw_flag = true,
            Some('-') if !saw_dash => saw_dash = true,
            Some(')' | ':') => return saw_flag,
            _ => return false,
        }
    }
}

/// The flags to fold into the pattern as an inline `(?…)` group, deduplicated
/// into a canonical order, or `None` when a flag has no `BigQuery` equivalent.
///
/// RE2 accepts inline `i`, `m` and `s` with the meanings the local engine
/// gives them. `R` (CRLF mode) has no RE2 counterpart, and it changes where
/// `^`/`$` match, so it cannot be dropped. RE2 documents `U` (swap greediness)
/// too, but whether `BigQuery`'s build honors it has not been measured against
/// a real `BigQuery`, so it is refused rather than assumed.
fn folded_flags(flags: &str) -> Option<String> {
    if flags.chars().any(|flag| !matches!(flag, 'i' | 'm' | 's')) {
        return None;
    }
    Some(
        ['i', 'm', 's']
            .into_iter()
            .filter(|flag| flags.contains(*flag))
            .collect(),
    )
}

/// `SAFE.PARSE_JSON(value)` — SAFE so a document it will not parse is a NULL
/// carried out through the call rather than a failed query.
fn parse_json(value: ast::Expr) -> ast::Expr {
    call_qualified_function("SAFE", "PARSE_JSON", vec![value])
}

/// A `JSON_KEYS` depth argument. Load-bearing: without it `BigQuery` descends and
/// returns nested paths, where the local functions see only the top level.
fn depth(levels: u32) -> ast::Expr {
    ast::Expr::Value(ast::Value::Number(levels.to_string(), false).into())
}

fn cast_null_to(data_type: ast::DataType) -> ast::Expr {
    ast::Expr::Cast {
        kind: ast::CastKind::Cast,
        expr: Box::new(ast::Expr::Value(ast::Value::Null.into())),
        data_type,
        array: false,
        format: None,
    }
}

/// A call to a function reached through a prefix, such as `SAFE.PARSE_JSON`.
fn call_qualified_function(prefix: &str, name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    let mut call = call_function(name, args);
    if let ast::Expr::Function(function) = &mut call {
        function.name = ObjectName(vec![
            ast::ObjectNamePart::Identifier(ast::Ident::new(prefix)),
            ast::ObjectNamePart::Identifier(ast::Ident::new(name)),
        ]);
    }
    call
}

fn call_function(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(Function {
        name: ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(name))]),
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|arg| FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)))
                .collect(),
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

/// A `BigQuery` raw string literal, `r'…'`. Raw so a backslash in the value is
/// the value's own, not something the SQL string layer consumes first — which
/// matters for a regex escape and for a JSON path alike.
fn raw_string(value: &str) -> ast::Value {
    ast::Value::SingleQuotedRawStringLiteral(value.to_string())
}

/// [`BigQueryDialect`] plus Spice's own scalar-function handlers.
///
/// `BigQueryDialect` does not implement [`Dialect::with_custom_scalar_overrides`]
/// — it panics — so the handlers cannot be attached to it directly. This wraps
/// it instead, holding the handler map itself and forwarding every other
/// [`Dialect`] method to the inner dialect. **Every** method is forwarded
/// explicitly: inheriting a trait default here would silently unparse
/// `BigQuery` SQL as if it were the generic dialect, changing quoting, casts
/// and interval rendering with no error anywhere.
pub struct SpiceBigQueryDialect {
    inner: BigQueryDialect,
    custom_scalar_fn_overrides: HashMap<String, ScalarFnToSqlHandler>,
}

impl Default for SpiceBigQueryDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SpiceBigQueryDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: BigQueryDialect::new(),
            custom_scalar_fn_overrides: HashMap::new(),
        }
    }
}

/// Every `Dialect` method is forwarded explicitly, and the lint keeps it that way.
///
/// An unlisted method falls back to the *trait default*, which is the generic
/// rendering rather than `BigQuery`'s — so a method added upstream would silently
/// revert `BigQuery` to generic SQL, with no error anywhere. Denying
/// `missing_trait_methods` turns that into a compile failure instead.
#[deny(clippy::missing_trait_methods)]
impl Dialect for SpiceBigQueryDialect {
    fn with_custom_scalar_overrides(mut self, handlers: Vec<(&str, ScalarFnToSqlHandler)>) -> Self {
        for (name, handler) in handlers {
            self.custom_scalar_fn_overrides
                .insert(name.to_string(), handler);
        }
        self
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if let Some(handler) = self.custom_scalar_fn_overrides.get(func_name) {
            return handler(unparser, args);
        }
        self.inner
            .scalar_function_to_sql_overrides(unparser, func_name, args)
    }

    fn aggregate_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
        distinct: bool,
        filter: Option<&Expr>,
        order_by: &[SortExpr],
    ) -> Result<Option<ast::Expr>> {
        self.inner.aggregate_function_to_sql_overrides(
            unparser, func_name, args, distinct, filter, order_by,
        )
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        self.inner.identifier_quote_style(identifier)
    }

    fn use_array_keyword_for_array_literals(&self) -> bool {
        self.inner.use_array_keyword_for_array_literals()
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.inner.supports_nulls_first_in_sort()
    }

    fn use_timestamp_for_date64(&self) -> bool {
        self.inner.use_timestamp_for_date64()
    }

    fn interval_style(&self) -> IntervalStyle {
        self.inner.interval_style()
    }

    fn float64_ast_dtype(&self) -> ast::DataType {
        self.inner.float64_ast_dtype()
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.utf8_cast_dtype()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.inner.large_utf8_cast_dtype()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.inner.date_field_extract_style()
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.inner.character_length_style()
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.inner.int64_cast_dtype()
    }

    fn int8_cast_dtype(&self) -> ast::DataType {
        self.inner.int8_cast_dtype()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.inner.int32_cast_dtype()
    }

    fn timestamp_cast_dtype(&self, time_unit: &TimeUnit, tz: &Option<Arc<str>>) -> ast::DataType {
        self.inner.timestamp_cast_dtype(time_unit, tz)
    }

    fn timestamp_literal_cast_dtype(
        &self,
        time_unit: &TimeUnit,
        tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        self.inner.timestamp_literal_cast_dtype(time_unit, tz)
    }

    fn date_difference_to_sql(&self, lhs: ast::Expr, rhs: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_difference_to_sql(lhs, rhs)
    }

    fn date_to_integer_to_sql(&self, date: ast::Expr) -> Option<ast::Expr> {
        self.inner.date_to_integer_to_sql(date)
    }

    fn requires_explicit_comparison_coercion(&self) -> bool {
        self.inner.requires_explicit_comparison_coercion()
    }

    fn timestamp_literal_max_subsecond_digits(&self) -> Option<usize> {
        self.inner.timestamp_literal_max_subsecond_digits()
    }

    fn timestamp_at_time_zone_to_sql(&self, input: ast::Expr, tz: &str) -> Option<ast::Expr> {
        self.inner.timestamp_at_time_zone_to_sql(input, tz)
    }

    fn date32_cast_dtype(&self) -> ast::DataType {
        self.inner.date32_cast_dtype()
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        self.inner.supports_column_alias_in_table_alias()
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.inner.requires_derived_table_alias()
    }

    fn division_operator(&self) -> BinaryOperator {
        self.inner.division_operator()
    }

    fn higher_order_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        self.inner
            .higher_order_function_to_sql_overrides(unparser, func_name, args)
    }

    fn window_func_support_window_frame(
        &self,
        func_name: &str,
        start_bound: &WindowFrameBound,
        end_bound: &WindowFrameBound,
    ) -> bool {
        self.inner
            .window_func_support_window_frame(func_name, start_bound, end_bound)
    }

    fn union_distinct_set_quantifier(&self) -> ast::SetQuantifier {
        self.inner.union_distinct_set_quantifier()
    }

    fn full_qualified_col(&self) -> bool {
        self.inner.full_qualified_col()
    }

    fn unnest_as_table_factor(&self) -> bool {
        self.inner.unnest_as_table_factor()
    }

    fn unnest_as_lateral_flatten(&self) -> bool {
        self.inner.unnest_as_lateral_flatten()
    }

    fn col_alias_overrides(&self, alias: &str) -> Result<Option<String>> {
        self.inner.col_alias_overrides(alias)
    }

    fn supports_qualify(&self) -> bool {
        self.inner.supports_qualify()
    }

    fn timestamp_with_tz_to_string(&self, dt: DateTime<Tz>, unit: TimeUnit) -> String {
        self.inner.timestamp_with_tz_to_string(dt, unit)
    }

    fn supports_empty_select_list(&self) -> bool {
        self.inner.supports_empty_select_list()
    }

    fn string_literal_to_sql(&self, s: &str) -> Option<ast::Expr> {
        self.inner.string_literal_to_sql(s)
    }

    fn group_by_matches_select_subexpressions(&self) -> bool {
        self.inner.group_by_matches_select_subexpressions()
    }

    fn range_window_default_nulls_first(&self, asc: bool) -> Option<bool> {
        self.inner.range_window_default_nulls_first(asc)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::DataType;
    use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionParams};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarUDF, Volatility, WindowFrame, WindowFunctionDefinition, create_udf,
    };
    use datafusion::prelude::{col, lit};
    use datafusion::sql::unparser::Unparser;

    use super::{
        FLOAT64_FROM_STR, INT64_FROM_STR, JSON_GET_BOOL_NAME, JSON_GET_FLOAT_NAME,
        JSON_GET_INT_NAME, JSON_GET_STR_NAME, JSON_KEYS_NAME, JSON_LEN_NAME, JSON_LENGTH_NAME,
        JSON_OBJECT_KEYS_NAME, JsonPath, SpiceBigQueryDialect, can_translate, json_path,
    };
    use crate::dialect::{REGEXP_LIKE_NAME, new_bigquery_dialect};
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::Expr;
    use datafusion::logical_expr::expr::ScalarFunction;

    fn json_udf(name: &str, arity: usize) -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            name,
            vec![DataType::Utf8; arity],
            DataType::Int64,
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        ))
    }

    fn call(name: &str, args: Vec<Expr>) -> ScalarFunction {
        ScalarFunction::new_udf(json_udf(name, args.len()), args)
    }

    /// The SQL the `BigQuery` dialect renders for one `json_get_*` call.
    fn render(name: &str, args: Vec<Expr>) -> String {
        let dialect = new_bigquery_dialect();
        Unparser::new(dialect.as_ref())
            .expr_to_sql(&Expr::ScalarFunction(call(name, args)))
            .expect("the BigQuery dialect renders this call")
            .to_string()
    }

    #[test]
    fn a_literal_key_becomes_a_bigquery_json_path() {
        assert_eq!(
            json_path(&[col("doc"), lit("a")]),
            Some(JsonPath::Path(r#"$."a""#.to_string()))
        );
    }

    #[test]
    fn the_variadic_path_becomes_one_json_path() {
        assert_eq!(
            json_path(&[col("doc"), lit("a"), lit("b"), lit(0_i64)]),
            Some(JsonPath::Path(r#"$."a"."b"[0]"#.to_string()))
        );
    }

    #[test]
    fn a_dotted_key_stays_one_key() {
        // `json_get_int(doc, 'a.b')` reads a key literally named `a.b`. Quoting
        // it is what stops BigQuery reading it as two steps, which would return
        // a different value rather than an error.
        assert_eq!(
            json_path(&[col("doc"), lit("a.b")]),
            Some(JsonPath::Path(r#"$."a.b""#.to_string()))
        );
    }

    #[test]
    fn a_key_the_two_quoting_layers_disagree_about_is_not_translated() {
        // A quote, a backslash or a control character means one of the SQL
        // literal layer and the JSONPath layer would read the key as structure.
        // Escaping across both is how a key silently becomes a different path,
        // so these are left for the local engine.
        for key in [r#"a"b"#, r"a\b", "a'b", "a\nb", "a\tb"] {
            assert_eq!(
                json_path(&[col("doc"), lit(key)]),
                None,
                "{key:?} cannot be written into a BigQuery JSON path unambiguously"
            );
        }
    }

    #[test]
    fn no_pattern_has_a_capturing_group() {
        // `REGEXP_EXTRACT` accepts at most one capturing group and errors on
        // more; with none it returns the whole match. A capturing group added
        // here would fail every federated call at BigQuery, which no local test
        // can see.
        for pattern in [INT64_FROM_STR, FLOAT64_FROM_STR] {
            let mut chars = pattern.chars().peekable();
            while let Some(c) = chars.next() {
                match c {
                    '\\' => {
                        chars.next();
                    }
                    '(' => assert_eq!(
                        chars.peek(),
                        Some(&'?'),
                        "capturing group in {pattern}: every group must be `(?:…)`"
                    ),
                    _ => {}
                }
            }
        }
    }

    #[test]
    fn a_path_that_can_never_resolve_is_recognised_as_such() {
        // A negative index and a NULL path element both map to
        // `datafusion-functions-json`'s own `JsonPath::None`, which matches
        // nothing, so the call is NULL for every row.
        for element in [
            lit(-1_i64),
            Expr::Literal(ScalarValue::Utf8(None), None),
            Expr::Literal(ScalarValue::Int64(None), None),
        ] {
            assert_eq!(
                json_path(&[col("doc"), element.clone()]),
                Some(JsonPath::NeverResolves),
                "{element} names nothing"
            );
        }
    }

    #[test]
    fn a_non_literal_path_element_has_no_translation() {
        // BigQuery's JSON path argument must be a constant, so there is nothing
        // to render for a per-row path.
        assert_eq!(json_path(&[col("doc"), col("key")]), None);
        assert_eq!(json_path(&[col("doc"), lit("a"), col("key")]), None);
    }

    #[test]
    fn a_call_with_no_path_has_no_translation() {
        assert_eq!(json_path(&[col("doc")]), None);
    }

    /// `array_element` renders only a non-negative index, so the gate has to
    /// refuse the rest — otherwise the call is pushed down and the rendering
    /// then fails the whole query instead of evaluating locally.
    #[test]
    fn array_element_federates_only_for_a_non_negative_integer_index() {
        assert!(can_translate(&call(
            "array_element",
            vec![col("arr"), lit(1i64)]
        )));
        assert!(can_translate(&call(
            "array_element",
            vec![col("arr"), lit(0i64)]
        )));
        // Counts from the end, which BigQuery cannot express.
        assert!(!can_translate(&call(
            "array_element",
            vec![col("arr"), lit(-1i64)]
        )));
        // Sign unknown until it runs.
        assert!(!can_translate(&call(
            "array_element",
            vec![col("arr"), col("i")]
        )));
        // Not an ordinal BigQuery would take.
        assert!(!can_translate(&call(
            "array_element",
            vec![col("arr"), lit("1")]
        )));

        // And what does federate keeps the 1-based spelling.
        let sql = render("array_element", vec![col("arr"), lit(1i64)]);
        assert!(sql.contains("SAFE_ORDINAL(1)"), "not 1-based: {sql}");
    }

    #[test]
    fn can_translate_answers_for_the_json_functions_and_defers_on_the_rest() {
        assert!(can_translate(&call(
            JSON_GET_INT_NAME,
            vec![col("doc"), lit("a")]
        )));
        assert!(!can_translate(&call(
            JSON_GET_INT_NAME,
            vec![col("doc"), col("key")]
        )));
        assert!(can_translate(&call(
            JSON_GET_STR_NAME,
            vec![col("doc"), lit("a")]
        )));
        assert!(!can_translate(&call(
            JSON_GET_STR_NAME,
            vec![col("doc"), col("key")]
        )));
        assert!(can_translate(&call(
            JSON_GET_BOOL_NAME,
            vec![col("doc"), lit("a")]
        )));
        assert!(!can_translate(&call(
            JSON_GET_BOOL_NAME,
            vec![col("doc"), col("key")]
        )));
        for name in ["json_contains", "upper"] {
            assert!(
                can_translate(&call(name, vec![col("doc"), col("key")])),
                "{name} has no handler in this dialect, so it is not this check's business — \
                 the deny-list has not carved it out and it cannot federate at all"
            );
        }
    }

    #[test]
    fn json_get_int_renders_as_a_guarded_safe_cast() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit("a")]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, R'$."a"'), R'^[+-]?[0-9]+$') AS INT64)"#
        );
    }

    #[test]
    fn an_index_in_the_path_renders_after_the_key() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit("a"), lit(2_i64)]),
            r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, R'$."a"[2]'), R'^[+-]?[0-9]+$') AS INT64)"#
        );
    }

    #[test]
    fn json_get_str_renders_as_a_guarded_json_value() {
        // The STARTS_WITH/FORMAT/JSON_QUERY guard is the whole reason this is
        // translatable: JSON_VALUE alone renders a JSON number as its digits,
        // where `json_get_str` returns NULL. FORMAT normalizes JSON_QUERY's
        // native JSON and JSON-formatted string result types before the guard.
        assert_eq!(
            render(JSON_GET_STR_NAME, vec![col("doc"), lit("a")]),
            r#"CASE WHEN STARTS_WITH(FORMAT('%t', JSON_QUERY(`doc`, R'$."a"')), '"') THEN JSON_VALUE(`doc`, R'$."a"') END"#
        );
    }

    #[test]
    fn json_get_str_never_resolving_renders_as_a_typed_null() {
        // Utf8, not Int64: a federated schema that disagrees with the local one
        // is a cast error at best and a wrong column type at worst.
        assert_eq!(
            render(JSON_GET_STR_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS STRING)"
        );
    }

    #[test]
    fn json_get_bool_renders_as_a_case_over_the_rendered_text() {
        // A string comparison, not SAFE_CAST(… AS BOOL): the cast is
        // case-insensitive where Rust's `bool::from_str` is not, so a cast
        // would answer true for "TRUE" where the local function is NULL.
        assert_eq!(
            render(JSON_GET_BOOL_NAME, vec![col("doc"), lit("a")]),
            r#"CASE JSON_VALUE(`doc`, R'$."a"') WHEN 'true' THEN true WHEN 'false' THEN false END"#
        );
    }

    #[test]
    fn no_bool_rendering_casts_to_bool() {
        // The whole reason `json_get_bool` is translatable is that it never
        // reaches BigQuery's BOOL cast. If one appears, the case-sensitivity
        // agreement is gone and "TRUE" starts answering true remotely only.
        let sql = render(JSON_GET_BOOL_NAME, vec![col("doc"), lit("a")]);
        assert!(
            !sql.contains("AS BOOL"),
            "a BOOL cast is case-insensitive and must not appear: {sql}"
        );
    }

    #[test]
    fn json_get_float_renders_as_a_guarded_safe_cast() {
        assert_eq!(
            render(JSON_GET_FLOAT_NAME, vec![col("doc"), lit("a")]),
            concat!(
                r#"SAFE_CAST(REGEXP_EXTRACT(JSON_VALUE(`doc`, R'$."a"'), "#,
                r#"R'^[+-]?(?:(?i:inf|infinity|nan)|(?:[0-9]+\.[0-9]*|[0-9]*\.[0-9]+|[0-9]+)"#,
                r#"(?:[eE][+-]?[0-9]+)?)$') AS FLOAT64)"#
            )
        );
    }

    #[test]
    fn the_float_grammar_accepts_what_rust_accepts_and_nothing_more() {
        // Every case is asserted against Rust's own parser as well as the
        // pattern, so the two cannot drift apart while the test still passes.
        // `regex` and BigQuery's engine are both RE2 lineage, so a pattern this
        // one accepts is one BigQuery accepts.
        let pattern = regex::Regex::new(FLOAT64_FROM_STR).expect("the float grammar compiles");
        for accepted in [
            "1", "-1", "1.5", "-1.5", "1.", ".5", "1e3", "-1E-3", "0", "inf", "-inf", "Infinity",
            "INFINITY", "nan", "NaN", "+1.5",
        ] {
            assert!(
                pattern.is_match(accepted),
                "`{accepted}` parses as f64 in Rust, so the pattern must accept it"
            );
            assert!(
                accepted.parse::<f64>().is_ok(),
                "`{accepted}` must actually parse in Rust, or this test is asserting the wrong thing"
            );
        }
        for rejected in [
            "0x2A", "  1.5  ", "1.5f", "", "e3", "1e", "1,5", "true", "--1",
        ] {
            assert!(
                !pattern.is_match(rejected),
                "`{rejected}` is not an f64 in Rust, so the pattern must reject it"
            );
            assert!(
                rejected.parse::<f64>().is_err(),
                "`{rejected}` must actually fail in Rust, or this test is asserting the wrong thing"
            );
        }
    }

    #[test]
    fn regexp_like_renders_as_regexp_contains() {
        assert_eq!(
            render(REGEXP_LIKE_NAME, vec![col("code"), lit("^R[0-9]{2}")]),
            r"REGEXP_CONTAINS(`code`, R'^R[0-9]{2}')"
        );
    }

    #[test]
    fn regexp_like_folds_literal_flags_into_the_pattern() {
        // BigQuery's REGEXP_CONTAINS has no flags argument; RE2 reads the same
        // flags inline. Deduplicated into a canonical order so the rendering
        // is deterministic.
        assert_eq!(
            render(
                REGEXP_LIKE_NAME,
                vec![col("code"), lit("^r[0-9]{2}"), lit("si")]
            ),
            r"REGEXP_CONTAINS(`code`, R'(?is)^r[0-9]{2}')"
        );
        assert_eq!(
            render(REGEXP_LIKE_NAME, vec![col("code"), lit("^r"), lit("")]),
            render(REGEXP_LIKE_NAME, vec![col("code"), lit("^r")]),
            "empty flags are the two-argument call"
        );
    }

    #[test]
    fn regexp_like_translates_only_the_shapes_both_engines_read_identically() {
        for (accepted, args) in [
            (
                "a plain literal pattern",
                vec![col("code"), lit("^R[0-9]{2}$")],
            ),
            ("an escaped metacharacter", vec![col("code"), lit(r"^R\.")]),
            (
                "a POSIX class, which is ASCII in both engines",
                vec![col("code"), lit("[[:digit:]]+")],
            ),
            (
                "literal ims flags",
                vec![col("code"), lit("^r"), lit("ims")],
            ),
            (
                "an inline flag directive both engines read",
                vec![col("code"), lit("(?i)^r[0-9]{2}")],
            ),
            (
                "a scoped inline flag group",
                vec![col("code"), lit("(?im-s:foo)bar")],
            ),
            (
                "a plain non-capturing group",
                vec![col("code"), lit("(?:ab)+")],
            ),
            ("a negated inline flag", vec![col("code"), lit("(?-i)r")]),
            (
                "a capture group followed by a quantifier",
                vec![col("code"), lit("(ab)?c")],
            ),
            (
                "counted repetition at RE2's inclusive limit",
                vec![col("code"), lit("a{1,1000}")],
            ),
            (
                "an open-ended repetition at RE2's inclusive limit",
                vec![col("code"), lit("a{1000,}")],
            ),
            (
                "single class punctuation without a Rust set operator",
                vec![col("code"), lit("[a&~-]")],
            ),
            (
                "an escaped parenthesis before a question mark",
                vec![col("code"), lit(r"\(?a")],
            ),
        ] {
            assert!(
                can_translate(&call(REGEXP_LIKE_NAME, args)),
                "{accepted} must translate"
            );
        }
        for (rejected, args) in [
            (
                "a non-literal pattern, which cannot be scanned",
                vec![col("code"), col("pattern")],
            ),
            (
                "a NULL pattern, which has no text to scan",
                vec![col("code"), Expr::Literal(ScalarValue::Utf8(None), None)],
            ),
            (
                r"\d, Unicode-aware locally and ASCII in RE2",
                vec![col("code"), lit(r"^\d+$")],
            ),
            (
                r"\b, whose word boundary is Unicode-dependent",
                vec![col("code"), lit(r"\bR01\b")],
            ),
            ("a non-ASCII pattern", vec![col("code"), lit("^caf\u{e9}")]),
            (
                "a quote, which would end the raw string literal",
                vec![col("code"), lit("^'R")],
            ),
            ("a trailing backslash", vec![col("code"), lit(r"^R\")]),
            (
                "the U flag, unmeasured against BigQuery",
                vec![col("code"), lit("^r"), lit("U")],
            ),
            (
                "the R flag, which has no RE2 counterpart",
                vec![col("code"), lit("^r"), lit("R")],
            ),
            (
                "non-literal flags",
                vec![col("code"), lit("^r"), col("flags")],
            ),
            (
                "the inline x mode, which RE2 does not have",
                vec![col("code"), lit("(?x)a b")],
            ),
            (
                "the inline U flag, refused for the same reason as the U flags argument",
                vec![col("code"), lit("(?U)a+")],
            ),
            (
                "the inline R CRLF mode, which has no RE2 counterpart",
                vec![col("code"), lit("(?R)^a")],
            ),
            ("an inline Unicode toggle", vec![col("code"), lit("(?u)a")]),
            (
                "a named capture group, which the two engines spell differently",
                vec![col("code"), lit("(?P<n>a)")],
            ),
            (
                "a lookahead, which neither engine supports",
                vec![col("code"), lit("(?=a)")],
            ),
            ("an empty flag directive", vec![col("code"), lit("(?)a")]),
            (
                "an exact repetition above RE2's limit",
                vec![col("code"), lit("a{1001}")],
            ),
            (
                "an open-ended repetition above RE2's limit",
                vec![col("code"), lit("a{1001,}")],
            ),
            (
                "an upper repetition bound above RE2's limit",
                vec![col("code"), lit("a{1,1001}")],
            ),
            (
                "Rust character-class intersection",
                vec![col("code"), lit("[a&&b]")],
            ),
            (
                "Rust character-class difference",
                vec![col("code"), lit("[a--b]")],
            ),
            (
                "Rust character-class symmetric difference",
                vec![col("code"), lit("[a~~b]")],
            ),
            (
                "a nested class used by Rust set algebra",
                vec![col("code"), lit("[a&&[b]]")],
            ),
        ] {
            assert!(
                !can_translate(&call(REGEXP_LIKE_NAME, args)),
                "{rejected} must stay local"
            );
        }
    }

    #[test]
    fn rusts_perl_classes_are_unicode_aware() {
        // The local half of the divergence the pattern gate guards against:
        // Rust's `\d` matches a Unicode digit where RE2's is `[0-9]`, so a
        // pattern carrying it federated to BigQuery would silently keep
        // different rows. If this stops matching, the gate guards nothing and
        // can be relaxed.
        let digits = regex::Regex::new(r"^\d+$").expect("the digit pattern compiles");
        assert!(
            digits.is_match("\u{663}\u{664}\u{665}"),
            "Rust's \\d must match Arabic-Indic digits, or the gate is pointless"
        );
    }

    #[test]
    fn rust_only_inline_modes_exist() {
        // The local half of the inline-modifier divergence the gate guards
        // against: Rust's regex reads `(?x)` (extended mode, whitespace
        // ignored), which RE2 does not have — so `(?x)a b` matches `ab`
        // locally and is a syntax error remotely. If Rust ever drops the
        // mode, the group-options gate can be relaxed.
        let extended = regex::Regex::new("(?x)a b").expect("Rust reads extended mode");
        assert!(
            extended.is_match("ab"),
            "extended mode must ignore the space, or the gate guards nothing"
        );
        assert!(
            !extended.is_match("a b"),
            "in extended mode the literal space is not part of the pattern"
        );
    }

    #[test]
    fn rust_supports_regex_features_re2_does_not() {
        assert!(
            regex::Regex::new("a{1001}").is_ok(),
            "Rust must accept a repetition above RE2's 1000 limit, or the bound gate is unnecessary"
        );

        let intersection =
            regex::Regex::new("^[a&&b]$").expect("Rust reads character-class intersection");
        assert!(
            !intersection.is_match("a") && !intersection.is_match("b"),
            "Rust must read `&&` as set intersection rather than literal ampersands"
        );
    }

    #[test]
    fn regexp_like_never_renders_verbatim() {
        for args in [
            vec![col("code"), lit("^R[0-9]{2}")],
            vec![col("code"), lit("^r"), lit("i")],
        ] {
            let sql = render(REGEXP_LIKE_NAME, args);
            assert!(
                !sql.contains(REGEXP_LIKE_NAME),
                "{REGEXP_LIKE_NAME} must not reach BigQuery SQL: {sql}"
            );
        }
    }

    #[test]
    fn an_untranslatable_regexp_like_fails_rather_than_unparsing_verbatim() {
        // Unreachable with the deny-list's per-call check installed; see
        // `an_untranslatable_call_fails_rather_than_unparsing_verbatim` for why
        // the alternative is worse.
        let dialect = new_bigquery_dialect();
        let error = Unparser::new(dialect.as_ref())
            .expr_to_sql(&Expr::ScalarFunction(call(
                REGEXP_LIKE_NAME,
                vec![col("code"), col("pattern")],
            )))
            .expect_err("a non-literal pattern has no BigQuery translation");
        let message = error.to_string();
        for expected in [
            "must be literals",
            "query_federation",
            "https://spiceai.org/docs/components/data-connectors/adbc",
        ] {
            assert!(
                message.contains(expected),
                "the error must carry {expected:?}: {message}"
            );
        }
    }

    #[test]
    fn the_dialect_renders_every_builtin_it_overrides() {
        // The mirror of `the_dialect_renders_every_name_the_deny_list_carves_out`
        // for the built-in table: an entry whose handler cannot render the
        // shape its own `can_translate` accepts would fail at execution.
        let dialect = new_bigquery_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        for entry in super::BUILTIN_SCALAR_OVERRIDES {
            let args = [col("code"), lit("^R[0-9]{2}")];
            assert!(
                (entry.can_translate)(&args),
                "`{name}`'s representative call must be translatable",
                name = entry.name
            );
            let rendered = dialect
                .scalar_function_to_sql_overrides(&unparser, entry.name, &args)
                .unwrap_or_else(|error| {
                    panic!("the dialect must render `{}`: {error}", entry.name)
                });
            assert!(
                rendered.is_some(),
                "`{name}` has an override entry but no handler answered for it",
                name = entry.name
            );
        }
    }

    #[test]
    fn a_rewritten_null_check_unparses_into_regexp_contains() {
        // The production path end to end, minus the network: the optimizer rule
        // rewrites the NULL-check idiom before federation, and this dialect
        // renders what the rewrite produces.
        use datafusion::optimizer::{OptimizerContext, OptimizerRule};

        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("code", DataType::Utf8, true),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let matches = Expr::ScalarFunction(ScalarFunction::new_udf(
            datafusion::functions::regex::regexp_match(),
            vec![col("code"), lit("^R[0-9]{2}")],
        ));
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None)
            .expect("scan t")
            .filter(Expr::IsNotNull(Box::new(matches)))
            .expect("filter")
            .project(vec![col("code")])
            .expect("project")
            .build()
            .expect("build");

        let rewritten = crate::optimizer_rule::RegexpMatchNullCheckRewrite::new()
            .rewrite(plan, &OptimizerContext::new())
            .expect("the optimizer rule rewrites the plan")
            .data;
        let sql = unparse_plan(new_bigquery_dialect().as_ref(), &rewritten);

        assert!(
            sql.contains("REGEXP_CONTAINS(") && sql.contains("IS TRUE"),
            "the NULL-check must reach BigQuery as a REGEXP_CONTAINS predicate: {sql}"
        );
        assert!(
            !sql.contains("regexp_"),
            "no DataFusion regexp function may reach BigQuery SQL: {sql}"
        );
    }

    #[test]
    fn json_length_counts_an_array_and_an_object_differently() {
        // The depth argument on JSON_KEYS is what keeps an object's count to
        // its own keys: without it BigQuery descends and returns nested paths,
        // where json_length counts only the top level.
        assert_eq!(
            render(JSON_LENGTH_NAME, vec![col("doc"), lit("a")]),
            concat!(
                r#"CASE WHEN STARTS_WITH(JSON_QUERY(`doc`, R'$."a"'), '[') "#,
                r#"THEN ARRAY_LENGTH(JSON_QUERY_ARRAY(`doc`, R'$."a"')) "#,
                r#"WHEN STARTS_WITH(JSON_QUERY(`doc`, R'$."a"'), '{') "#,
                r#"THEN ARRAY_LENGTH(JSON_KEYS(SAFE.PARSE_JSON(JSON_QUERY(`doc`, R'$."a"')), 1)) END"#
            )
        );
    }

    #[test]
    fn json_length_object_keys_are_capped_to_the_top_level() {
        let sql = render(JSON_LENGTH_NAME, vec![col("doc"), lit("a")]);
        assert!(
            sql.contains("JSON_KEYS(SAFE.PARSE_JSON(JSON_QUERY(`doc`, R'$.\"a\"')), 1)"),
            "JSON_KEYS must carry the depth argument, or it counts nested paths: {sql}"
        );
    }

    #[test]
    fn json_object_keys_renders_as_a_depth_capped_json_keys() {
        assert_eq!(
            render(JSON_OBJECT_KEYS_NAME, vec![col("doc"), lit("a")]),
            concat!(
                r#"CASE WHEN STARTS_WITH(JSON_QUERY(`doc`, R'$."a"'), '{') "#,
                r#"THEN JSON_KEYS(SAFE.PARSE_JSON(JSON_QUERY(`doc`, R'$."a"')), 1) END"#
            )
        );
    }

    #[test]
    fn json_object_keys_never_resolving_keeps_its_element_type() {
        // An untyped NULL would make the federated schema disagree with the
        // plan's List(Utf8), which is a failed query rather than a wrong row.
        assert_eq!(
            render(JSON_OBJECT_KEYS_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS ARRAY<STRING>)"
        );
    }

    #[test]
    fn the_alias_renders_exactly_as_the_canonical_name_does() {
        assert_eq!(
            render(JSON_LEN_NAME, vec![col("doc"), lit("a")]),
            render(JSON_LENGTH_NAME, vec![col("doc"), lit("a")]),
            "`json_len` is `json_length`; the two must not drift"
        );
        assert_eq!(
            render(JSON_KEYS_NAME, vec![col("doc"), lit("a")]),
            render(JSON_OBJECT_KEYS_NAME, vec![col("doc"), lit("a")]),
            "`json_keys` is `json_object_keys`; the two must not drift"
        );
    }

    #[test]
    fn a_path_that_can_never_resolve_renders_as_a_typed_null() {
        assert_eq!(
            render(JSON_GET_INT_NAME, vec![col("doc"), lit(-1_i64)]),
            "CAST(NULL AS INT64)"
        );
    }

    #[test]
    fn no_rendering_ever_contains_the_function_verbatim() {
        for name in [
            JSON_GET_INT_NAME,
            JSON_GET_STR_NAME,
            JSON_GET_BOOL_NAME,
            JSON_GET_FLOAT_NAME,
            JSON_LENGTH_NAME,
            JSON_OBJECT_KEYS_NAME,
        ] {
            for args in [
                vec![col("doc"), lit("a")],
                vec![col("doc"), lit("a"), lit(0_i64)],
                vec![col("doc"), lit(-1_i64)],
            ] {
                let sql = render(name, args);
                assert!(
                    !sql.contains(name),
                    "{name} must not reach BigQuery SQL: {sql}"
                );
            }
        }
    }

    #[test]
    fn an_untranslatable_call_fails_rather_than_unparsing_verbatim() {
        // Unreachable with the deny-list installed. If the dialect is used
        // without it, the alternative is emitting `json_get_int(...)` into
        // BigQuery SQL, so this fails where a reader can see it instead.
        let dialect = new_bigquery_dialect();
        let error = Unparser::new(dialect.as_ref())
            .expr_to_sql(&Expr::ScalarFunction(call(
                JSON_GET_INT_NAME,
                vec![col("doc"), col("key")],
            )))
            .expect_err("a dynamic path has no BigQuery translation");
        let message = error.to_string();
        for expected in [
            // The call shape that failed, not the policy that should have
            // caught it: an internal cause is no help to whoever ran the query.
            "must be a literal",
            // The way out, and where to read about it.
            "query_federation",
            "https://spiceai.org/docs/components/data-connectors/adbc",
        ] {
            assert!(
                message.contains(expected),
                "the error must carry {expected:?}: {message}"
            );
        }
        assert!(
            !message.contains("policy"),
            "the error must not name an internal mechanism: {message}"
        );
    }

    #[test]
    fn the_dialect_renders_every_name_the_deny_list_carves_out() {
        // The carve-out is what lets these names federate. A name in it that
        // the dialect has no handler for would be unparsed verbatim, so the
        // list is only safe while the dialect answers for all of it.
        let dialect = new_bigquery_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        for name in crate::dialect::bigquery_native_function_names() {
            let rendered = dialect
                .scalar_function_to_sql_overrides(&unparser, name, &[col("doc"), lit("a")])
                .unwrap_or_else(|error| panic!("the dialect must render `{name}`: {error}"));
            assert!(
                rendered.is_some(),
                "`{name}` is carved out of the deny-list but the dialect has no handler for it, \
                 so it would be unparsed verbatim into BigQuery SQL"
            );
        }
    }

    #[test]
    fn a_predicate_over_a_json_function_unparses_into_the_statement() {
        // The whole statement, not just the expression: this is the SQL a
        // federated scan sends, so it is where a verbatim `json_get_*` would
        // show up.
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("doc", DataType::Utf8, true),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None)
            .expect("scan t")
            .filter(
                Expr::ScalarFunction(call(JSON_GET_INT_NAME, vec![col("doc"), lit("a")]))
                    .eq(lit(1_i64)),
            )
            .expect("filter")
            .build()
            .expect("build");

        let dialect = new_bigquery_dialect();
        let sql = Unparser::new(dialect.as_ref())
            .plan_to_sql(&plan)
            .expect("the BigQuery dialect unparses the plan")
            .to_string();

        assert!(sql.contains("JSON_VALUE("), "no JSON_VALUE in: {sql}");
        assert!(sql.contains("SAFE_CAST("), "no SAFE_CAST in: {sql}");
        assert!(
            !sql.contains("json_get_"),
            "a Spice-only function reached BigQuery SQL: {sql}"
        );
    }

    #[test]
    fn the_wrapper_unparses_exactly_as_the_bigquery_dialect_does() {
        // Every `Dialect` method is forwarded to the inner dialect. Inheriting
        // a trait default for one instead would change quoting, a cast's type
        // name or an alias silently — valid SQL that BigQuery rejects, or worse,
        // accepts differently. Unparsing the same plan through both is what
        // notices, whichever method was missed.
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("n", DataType::Int64, false),
            datafusion::arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ),
                true,
            ),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let plan = datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None)
            .expect("scan t")
            .project(vec![
                datafusion::prelude::cast(col("n"), DataType::Float64).alias("a.b"),
                datafusion::prelude::cast(col("n"), DataType::Utf8),
                datafusion::prelude::cast(col("ts"), DataType::Date32),
                col("ts").alias("raw"),
            ])
            .expect("project")
            .build()
            .expect("build");

        let unparse = |dialect: &dyn datafusion::sql::unparser::dialect::Dialect| {
            Unparser::new(dialect)
                .plan_to_sql(&plan)
                .expect("unparse the plan")
                .to_string()
        };

        assert_eq!(
            unparse(&SpiceBigQueryDialect::new()),
            unparse(&datafusion::sql::unparser::dialect::BigQueryDialect::new()),
            "a `Dialect` method is not forwarded to the inner BigQuery dialect"
        );
    }

    #[test]
    fn the_wrapper_emits_valid_bigquery_set_and_window_syntax() {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("id", DataType::Int64, false),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            Arc::clone(&schema),
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        let scan = |name: &'static str| {
            datafusion::logical_expr::LogicalPlanBuilder::scan(name, Arc::clone(&source), None)
                .expect("build table scan")
                .project(vec![col(format!("{name}.id"))])
                .expect("project id")
                .build()
                .expect("build scan plan")
        };

        let left = scan("left_table");
        let right = scan("right_table");
        let distinct = datafusion::logical_expr::LogicalPlanBuilder::from(left.clone())
            .union_distinct(right.clone())
            .expect("build distinct union")
            .build()
            .expect("build distinct union plan");
        let all = datafusion::logical_expr::LogicalPlanBuilder::from(left)
            .union(right)
            .expect("build all union")
            .build()
            .expect("build all union plan");

        let distinct_sql = unparse_plan(new_bigquery_dialect().as_ref(), &distinct);
        let all_sql = unparse_plan(new_bigquery_dialect().as_ref(), &all);
        assert!(
            distinct_sql.contains(" UNION DISTINCT "),
            "BigQuery requires an explicit DISTINCT quantifier: {distinct_sql}"
        );
        assert!(
            all_sql.contains(" UNION ALL "),
            "UNION ALL must retain duplicate rows: {all_sql}"
        );

        let input = datafusion::logical_expr::LogicalPlanBuilder::scan(
            "window_values",
            Arc::clone(&source),
            None,
        )
        .expect("build window table scan")
        .build()
        .expect("build window input");
        let order_by = vec![col("window_values.id").sort(true, true)];
        let row_number = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(
                datafusion::functions_window::row_number::row_number_udwf(),
            ),
            params: WindowFunctionParams {
                args: vec![],
                partition_by: vec![],
                order_by: order_by.clone(),
                window_frame: WindowFrame::new(Some(false)),
                null_treatment: None,
                distinct: false,
                filter: None,
            },
        }))
        .alias("row_num");
        let running_sum = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(
                datafusion::functions_aggregate::sum::sum_udaf(),
            ),
            params: WindowFunctionParams {
                args: vec![col("window_values.id")],
                partition_by: vec![],
                order_by,
                window_frame: WindowFrame::new(Some(true)),
                null_treatment: None,
                distinct: false,
                filter: None,
            },
        }))
        .alias("running_sum");
        let window = datafusion::logical_expr::LogicalPlanBuilder::from(input)
            .window(vec![row_number, running_sum])
            .expect("build window expressions")
            .build()
            .expect("build window plan");
        let window_sql = unparse_plan(new_bigquery_dialect().as_ref(), &window);
        assert!(
            window_sql.contains(
                "row_number() OVER (ORDER BY `window_values`.`id` ASC NULLS FIRST) AS `row_num`"
            ),
            "BigQuery rejects a window frame on ROW_NUMBER: {window_sql}"
        );
        assert!(
            window_sql.contains(
                "sum(`window_values`.`id`) OVER (ORDER BY `window_values`.`id` ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `running_sum`"
            ),
            "aggregate window frames change which rows contribute and must be retained: {window_sql}"
        );
    }

    /// [`timestamp_scan`] plus a value column, for a window function to aggregate.
    fn windowed_scan() -> datafusion::logical_expr::LogicalPlanBuilder {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ),
                true,
            ),
            datafusion::arrow::datatypes::Field::new("v", DataType::Int64, true),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None).expect("scan t")
    }

    /// A scan of `t(ts)` carrying a UTC nanosecond timestamp, which most arms of
    /// [`the_wrapper_forwards_every_bigquery_specific_rendering`] filter or project over.
    fn timestamp_scan() -> datafusion::logical_expr::LogicalPlanBuilder {
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond,
                    Some("UTC".into()),
                ),
                true,
            ),
        ]));
        let source = Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
            schema,
        )) as Arc<dyn datafusion::logical_expr::TableSource>;
        datafusion::logical_expr::LogicalPlanBuilder::scan("t", source, None).expect("scan t")
    }

    /// The SQL `dialect` renders for `plan`.
    fn unparse_plan(
        dialect: &dyn datafusion::sql::unparser::dialect::Dialect,
        plan: &datafusion::logical_expr::LogicalPlan,
    ) -> String {
        Unparser::new(dialect)
            .plan_to_sql(plan)
            .expect("unparse the plan")
            .to_string()
    }

    /// Every `BigQuery`-specific rendering the fork's dialect fixes produce has to
    /// survive the wrapper.
    ///
    /// [`the_wrapper_unparses_exactly_as_the_bigquery_dialect_does`] covers the casts,
    /// quoting and aliasing an ordinary projection reaches. A [`Dialect`] method that
    /// only some SQL shapes touch is invisible to it: drop the `interval_style` forward
    /// and that plan still renders identically through both dialects, while a federated
    /// predicate carrying an interval starts reaching `BigQuery` as `INTERVAL '3 MONS'`,
    /// which it rejects.
    ///
    /// This is the path production takes — [`new_bigquery_dialect`] returns the wrapper,
    /// so every federated `BigQuery` query unparses through it, while the fork's fixes
    /// live on the inner dialect.
    ///
    /// Each arm asserts the rendering `BigQuery` receives, and that the wrapper and the
    /// inner dialect agree on it — so the two being wrong together is not a pass.
    ///
    /// Removing a forward from [`SpiceBigQueryDialect`] fails the matching arm for
    /// `interval_style`, `supports_column_alias_in_table_alias`, and the
    /// `scalar_function_to_sql_overrides` delegation that the extract and `date_trunc`
    /// arms reach. Two forwards cannot be caught this way, and those arms stand as
    /// guards on the rendering rather than on the forward: nothing consults a
    /// *wrapper's* `date_field_extract_style`, because the only caller is a dialect's
    /// own `scalar_function_to_sql_overrides` reading its own, and
    /// `timestamp_with_tz_to_string` is indistinguishable from the trait default for as
    /// long as the inner dialect carries no override of it.
    #[test]
    fn the_wrapper_forwards_every_bigquery_specific_rendering() {
        let timestamp_literal = timestamp_scan()
            .filter(col("t.ts").gt(lit(ScalarValue::TimestampNanosecond(
                Some(1_470_513_900_000_000_000),
                Some("UTC".into()),
            ))))
            .expect("filter")
            .project(vec![col("t.ts")])
            .expect("project")
            .build()
            .expect("build");

        let extract = timestamp_scan()
            .project(vec![datafusion::functions::expr_fn::date_part(
                lit("YEAR"),
                col("t.ts"),
            )])
            .expect("date_part projection")
            .build()
            .expect("build");

        let interval = timestamp_scan()
            .project(vec![
                col("t.ts")
                    + lit(ScalarValue::IntervalMonthDayNano(Some(
                        datafusion::arrow::datatypes::IntervalMonthDayNano::new(3, 0, 0),
                    ))),
            ])
            .expect("interval projection")
            .build()
            .expect("build");

        let orders = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            datafusion::arrow::datatypes::Field::new("o_orderkey", DataType::Int64, false),
        ]));
        let table_alias = datafusion::logical_expr::LogicalPlanBuilder::scan(
            "orders",
            Arc::new(datafusion::logical_expr::builder::LogicalTableSource::new(
                orders,
            )) as Arc<dyn datafusion::logical_expr::TableSource>,
            None,
        )
        .expect("scan orders")
        .project(vec![col("orders.o_orderkey")])
        .expect("inner projection")
        .project(vec![col("orders.o_orderkey").alias("key")])
        .expect("renaming projection")
        .alias("c")
        .expect("subquery alias")
        .project(vec![col("c.key")])
        .expect("outer projection")
        .build()
        .expect("build");

        let truncated = timestamp_scan()
            .project(vec![datafusion::functions::expr_fn::date_trunc(
                lit("month"),
                col("t.ts"),
            )])
            .expect("date_trunc projection")
            .build()
            .expect("build");

        // A grouped timestamp projected through a wrapper. Nothing else in this
        // test reaches `group_by_matches_select_subexpressions`, and a wrapper
        // that inherits its permissive default renders one flat SELECT that
        // BigQuery refuses with "neither grouped nor aggregated".
        let wrapped_grouping = {
            let grouped = timestamp_scan()
                .aggregate(
                    vec![datafusion::functions::expr_fn::date_trunc(
                        lit("week"),
                        col("t.ts"),
                    )],
                    vec![datafusion::functions_aggregate::expr_fn::count(lit(1_i64))],
                )
                .expect("aggregate")
                .build()
                .expect("build aggregate");
            let mut outputs = grouped.schema().columns().into_iter();
            let group_output = outputs.next().expect("the grouping expression's output");
            let count_output = outputs.next().expect("the aggregate's output");
            datafusion::logical_expr::LogicalPlanBuilder::from(grouped)
                .project(vec![
                    datafusion::logical_expr::cast(
                        datafusion::logical_expr::Expr::Column(group_output),
                        DataType::Date32,
                    )
                    .alias("week_start"),
                    datafusion::logical_expr::Expr::Column(count_output).alias("n"),
                ])
                .expect("projection over the aggregate")
                .build()
                .expect("build")
        };

        // `SUM(v) OVER (ORDER BY ts)`, whose frame a plan normalizes to RANGE and
        // whose placement it normalizes to ASC NULLS LAST — the combination
        // BigQuery refuses. A wrapper inheriting the permissive default renders
        // the NULLS clause and the statement fails.
        let range_window = {
            let windowed = datafusion::logical_expr::Expr::from(
                datafusion::logical_expr::expr::WindowFunction {
                    fun: datafusion::logical_expr::WindowFunctionDefinition::AggregateUDF(
                        datafusion::functions_aggregate::sum::sum_udaf(),
                    ),
                    params: datafusion::logical_expr::expr::WindowFunctionParams {
                        args: vec![col("t.v")],
                        partition_by: vec![],
                        order_by: vec![datafusion::logical_expr::expr::Sort::new(
                            col("t.ts"),
                            true,
                            false,
                        )],
                        window_frame: datafusion::logical_expr::WindowFrame::new(Some(false)),
                        null_treatment: None,
                        filter: None,
                        distinct: false,
                    },
                },
            );
            windowed_scan()
                .window(vec![windowed])
                .expect("window")
                .build()
                .expect("build")
        };

        for (property, plan, must_contain, must_not_contain) in [
            (
                "timestamp literal offset (fork PR #144)",
                &timestamp_literal,
                "20:05:00+00:00",
                "20:05:00 +00:00",
            ),
            (
                "date field extract style (fork PR #146)",
                &extract,
                "EXTRACT(YEAR FROM",
                "date_part",
            ),
            (
                "interval style (fork PR #146)",
                &interval,
                "INTERVAL '3' MONTH",
                "MONS",
            ),
            (
                "column alias in table alias (fork PR #148)",
                &table_alias,
                "AS `key`",
                "(key)",
            ),
            (
                "date_trunc rewrite (fork PR #169)",
                &truncated,
                "TIMESTAMP_TRUNC(`t`.`ts`, MONTH)",
                "date_trunc",
            ),
            (
                // BigQuery matches a GROUP BY entry against a whole select item
                // and a column reference and nothing in between, so the aggregate
                // has to reach it in a scope of its own. `FROM (SELECT` is that
                // scope; a flat rendering puts the grouping expression in the
                // outer select list, where the statement is refused.
                "grouping expression a select item wraps",
                &wrapped_grouping,
                "FROM (SELECT",
                "CAST(TIMESTAMP_TRUNC",
            ),
            (
                // BigQuery accepts no NULL placement but its own inside a RANGE
                // clause, and an ORDER BY with no explicit frame implies RANGE for
                // an aggregate. The placement has to be spelled as a leading key;
                // a surviving NULLS clause is the rendering BigQuery refuses.
                "RANGE window NULL placement",
                &range_window,
                "IS NULL ASC",
                "NULLS LAST",
            ),
        ] {
            let wrapper = unparse_plan(new_bigquery_dialect().as_ref(), plan);
            let inner = unparse_plan(
                &datafusion::sql::unparser::dialect::BigQueryDialect::new(),
                plan,
            );

            assert_eq!(
                wrapper, inner,
                "{property}: the wrapper and the inner BigQuery dialect no longer \
                 render this shape the same way, so federated BigQuery SQL diverges \
                 from the dialect's own rendering"
            );
            assert!(
                wrapper.contains(must_contain),
                "{property}: BigQuery needs `{must_contain}` here, so this statement is \
                 rejected: {wrapper}"
            );
            assert!(
                !wrapper.contains(must_not_contain),
                "{property}: `{must_not_contain}` is the rendering BigQuery rejects: \
                 {wrapper}"
            );
        }
    }
}
