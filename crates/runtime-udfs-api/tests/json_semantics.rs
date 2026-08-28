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

#![expect(
    clippy::expect_used,
    reason = "a failed set-up in a test should name itself and stop"
)]

//! What the JSON extraction functions return, value by value.
//!
//! These are the repo-side guards for the `datafusion-functions-json` pin
//! recorded in `docs/dev/fork_patches.md`. The pin sits ahead of the last
//! published release for three upstream correctness fixes, and every one of
//! them is a wrong answer rather than an error: a published
//! `datafusion-functions-json` returns NULL for every negative JSON number,
//! panics on an integer outside jiter's `i64` fast path, and reads the wrong
//! value out of a nested JSON string. Drop back to a published version and
//! these tests fail; nothing else in the workspace would.
//!
//! `runtime-udfs-api` hosts them because it is the crate that owns Spice's
//! relationship with `datafusion-functions-json` — it derives the federation
//! deny-list from that crate's registry — and it is small enough that the guard
//! is cheap to run.

use std::sync::Arc;

use datafusion::arrow::array::{Array, RecordBatch, StringArray, UnionArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF};
use datafusion::prelude::{SessionContext, col, lit};
use datafusion_functions_json::functions::json_as_text;
use datafusion_functions_json::udfs::{json_get_float_udf, json_get_int_udf, json_get_udf};
use datafusion_functions_json::{JSON_UNION_DATA_TYPE, JsonUnionEncoder, JsonUnionValue};

/// One element of a `json_get_*` path. The functions take a variadic path, not
/// a `JSONPath` string: a string argument is an object key, an integer argument
/// an array index.
#[derive(Clone, Copy)]
enum Path {
    Key(&'static str),
    Index(i64),
}

/// The single-element path `$.a` every scalar case below reads through.
const A: &[Path] = &[Path::Key("a")];

fn call(udf: &ScalarUDF, json: &str, path: &[Path], return_type: DataType) -> ScalarValue {
    let mut args = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        json.to_string(),
    )))];
    args.extend(path.iter().map(|element| {
        ColumnarValue::Scalar(match element {
            Path::Key(key) => ScalarValue::Utf8(Some((*key).to_string())),
            Path::Index(index) => ScalarValue::Int64(Some(*index)),
        })
    }));

    let result = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("value", return_type, true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
        .expect("a well-typed json_get_* call never fails; every miss is a NULL");

    match result {
        ColumnarValue::Scalar(scalar) => scalar,
        ColumnarValue::Array(_) => panic!("a call with scalar arguments must return a scalar"),
    }
}

fn get_int(json: &str, path: &[Path]) -> Option<i64> {
    match call(&json_get_int_udf(), json, path, DataType::Int64) {
        ScalarValue::Int64(value) => value,
        other => panic!("json_get_int returned {other:?}, not an Int64"),
    }
}

fn get_float(json: &str, path: &[Path]) -> Option<f64> {
    match call(&json_get_float_udf(), json, path, DataType::Float64) {
        ScalarValue::Float64(value) => value,
        other => panic!("json_get_float returned {other:?}, not a Float64"),
    }
}

/// Wraps a JSON value as the sole member of an object under key `a`.
fn doc(value: &str) -> String {
    format!(r#"{{"a": {value}}}"#)
}

#[test]
fn json_get_int_reads_negative_numbers() {
    for (value, expected) in [
        ("-1", Some(-1)),
        ("-42", Some(-42)),
        ("-0", Some(0)),
        (r#""-42""#, Some(-42)),
        ("1", Some(1)),
        ("0", Some(0)),
    ] {
        assert_eq!(
            get_int(&doc(value), A),
            expected,
            "json_get_int over {value}"
        );
    }
}

#[test]
fn json_get_float_reads_negative_numbers() {
    for (value, expected) in [
        ("-1.5", Some(-1.5)),
        ("-42", Some(-42.0)),
        ("-1e3", Some(-1000.0)),
        (r#""-4.25""#, Some(-4.25)),
        ("1.5", Some(1.5)),
        ("0", Some(0.0)),
    ] {
        assert_eq!(
            get_float(&doc(value), A),
            expected,
            "json_get_float over {value}"
        );
    }
}

#[test]
fn json_get_int_rejects_every_non_integer() {
    for value in [
        "1.5",  // a float is not an integer
        "-1.5", // and neither is a negative one
        "-1e3", // nor an exponent form, whatever it evaluates to
        "true", "false", "null", "{}", "[]",
        r#""abc""#, // a string that does not parse as an integer
        r#""1.5""#, // including one that parses only as a float
    ] {
        assert_eq!(get_int(&doc(value), A), None, "json_get_int over {value}");
    }
}

#[test]
fn json_get_float_rejects_every_non_number() {
    for value in ["true", "false", "null", "{}", "[]", r#""abc""#] {
        assert_eq!(
            get_float(&doc(value), A),
            None,
            "json_get_float over {value}"
        );
    }
}

#[test]
fn json_get_int_spans_the_whole_i64_range() {
    // jiter hands back a `BigInt` for any integer its fast path could not
    // decode, including values well inside `i64`. Reading it as "too large"
    // loses them; a released version panics on the same input.
    for (value, expected) in [
        ("9223372036854775807", Some(i64::MAX)),
        ("-9223372036854775808", Some(i64::MIN)),
        ("1753200000000000000", Some(1_753_200_000_000_000_000)),
        ("-1753200000000000000", Some(-1_753_200_000_000_000_000)),
        // genuinely outside i64: no representation, so NULL rather than a
        // silently truncated or float-rounded answer
        ("18446744073709551615", None),
        ("-9223372036854775809", None),
    ] {
        assert_eq!(
            get_int(&doc(value), A),
            expected,
            "json_get_int over {value}"
        );
    }
}

#[test]
fn a_missing_value_is_null_not_an_error() {
    assert_eq!(get_int(&doc("-1"), &[Path::Key("b")]), None, "wrong key");
    assert_eq!(
        get_int(r#"{"a": {"b": -1}}"#, A),
        None,
        "value is an object"
    );
    assert_eq!(get_int("[-1]", A), None, "key on a non-object");
    assert_eq!(
        get_int(&doc("-1"), &[Path::Index(0)]),
        None,
        "index on a non-array"
    );
    assert_eq!(get_int("[-1]", &[Path::Index(-1)]), None, "negative index");
    assert_eq!(
        get_float(&doc("-1.5"), &[Path::Key("b")]),
        None,
        "wrong key"
    );
}

#[test]
fn a_malformed_document_is_null_never_an_error() {
    // Malformed at or before the value the path names.
    for json in ["not json at all", r#"{"a": -}"#, r#"{"a" -1}"#, "{"] {
        assert_eq!(get_int(json, A), None, "json_get_int over {json}");
        assert_eq!(get_float(json, A), None, "json_get_float over {json}");
    }

    // The parse is incremental and stops at the value the path names, so a
    // document truncated *after* that value still reads. Pinned because it is
    // not obvious, and because any translation of these functions into a
    // backend's own JSON parser has to match it.
    assert_eq!(get_int(r#"{"a": -1"#, A), Some(-1));
    assert_eq!(get_float(r#"{"a": -1.5"#, A), Some(-1.5));
}

#[test]
fn a_nested_path_reaches_a_negative_number() {
    let json = r#"{"a": [1, {"b": -7, "c": -0.5}]}"#;
    assert_eq!(
        get_int(json, &[Path::Key("a"), Path::Index(1), Path::Key("b")]),
        Some(-7)
    );
    assert_eq!(
        get_float(json, &[Path::Key("a"), Path::Index(1), Path::Key("c")]),
        Some(-0.5)
    );
}

#[test]
fn a_key_containing_a_dot_is_one_key_not_a_path() {
    // The path is variadic, so `a.b` is a literal key. Worth pinning: it is the
    // assumption any translation of these functions into a backend's JSONPath
    // has to preserve.
    let json = r#"{"a.b": -1, "a": {"b": -2}}"#;
    assert_eq!(get_int(json, &[Path::Key("a.b")]), Some(-1));
    assert_eq!(get_int(json, &[Path::Key("a"), Path::Key("b")]), Some(-2));
}

#[test]
fn json_get_reads_an_integer_outside_the_fast_path_without_panicking() {
    // `json_get` returns a sparse union, and a released version reaches an
    // unimplemented arm for any integer jiter's fast path could not decode —
    // including values well inside `i64`. That is a panic on the query path,
    // so this test guards liveness as much as the value.
    for (value, expected) in [
        (
            "1753200000000000000",
            JsonUnionValue::Int(1_753_200_000_000_000_000),
        ),
        (
            "-1753200000000000000",
            JsonUnionValue::Int(-1_753_200_000_000_000_000),
        ),
        ("9223372036854775807", JsonUnionValue::Int(i64::MAX)),
        ("-9223372036854775808", JsonUnionValue::Int(i64::MIN)),
        // outside i64 the union has no representation for it, so it is a JSON
        // null rather than a silently rounded float
        ("18446744073709551615", JsonUnionValue::JsonNull),
        ("-9223372036854775809", JsonUnionValue::JsonNull),
    ] {
        assert_eq!(get_union(&doc(value)), expected, "json_get over {value}");
    }
}

/// `json_get(col, 'a')` over a one-row array, decoded from its sparse union.
/// The array form is what a real scan takes, and it is the shape the union
/// encoder reads.
fn get_union(json: &str) -> JsonUnionValue<'static> {
    let result = json_get_udf()
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(StringArray::from(vec![json]))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("value", JSON_UNION_DATA_TYPE.clone(), true)),
            config_options: Arc::new(ConfigOptions::new()),
        })
        .expect("a well-typed json_get call never fails; every miss is a JSON null");

    let ColumnarValue::Array(array) = result else {
        panic!("a call with an array argument must return an array");
    };
    let union = array
        .as_any()
        .downcast_ref::<UnionArray>()
        .expect("json_get returns a sparse union")
        .clone();
    let encoder =
        JsonUnionEncoder::from_union(union).expect("the union is the JSON union encoder's own");

    match encoder.get_value(0) {
        JsonUnionValue::Int(value) => JsonUnionValue::Int(value),
        JsonUnionValue::Float(value) => JsonUnionValue::Float(value),
        JsonUnionValue::Bool(value) => JsonUnionValue::Bool(value),
        JsonUnionValue::JsonNull => JsonUnionValue::JsonNull,
        other => panic!("json_get returned {other:?}, which this test does not cover"),
    }
}

#[test]
fn a_json_string_holding_json_is_read_one_level_at_a_time() {
    // `json_as_text` returns SQL text, so two nested calls are not the same as
    // one call with a two-element path: the inner call's result is a document
    // in its own right. Folding them reads the wrong value — NULL here, since
    // `outer` names a string rather than an object.
    let doc = r#"{"outer": "{\"inner\": \"value\"}"}"#;
    let nested = json_as_text(json_as_text(col("doc"), lit("outer")), lit("inner"));

    assert_eq!(
        evaluate_over(doc, nested),
        Some("value".to_string()),
        "nested json_as_text calls must not be folded into one path"
    );
}

/// Plans `expr` against a one-column, one-row batch and evaluates it.
///
/// Planning is the point: `create_physical_expr` is what applies the crate's
/// own function rewrites, which is where the folding this guards against
/// happens. Evaluating the expression by hand would pass either way.
fn evaluate_over(doc: &str, expr: Expr) -> Option<String> {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx).expect("register the JSON functions");

    let schema = Arc::new(Schema::new(vec![Field::new("doc", DataType::Utf8, true)]));
    let df_schema = DFSchema::try_from(Arc::clone(&schema)).expect("build the schema");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![doc]))])
        .expect("build the batch");

    let physical = ctx
        .state()
        .create_physical_expr(expr, &df_schema)
        .expect("plan the expression");

    match physical.evaluate(&batch).expect("evaluate the expression") {
        ColumnarValue::Array(array) => {
            let strings = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("json_as_text returns a string");
            strings.is_valid(0).then(|| strings.value(0).to_string())
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(value)) => value,
        ColumnarValue::Scalar(other) => {
            panic!("json_as_text returned {other:?}, not a string")
        }
    }
}

/// Rust's `f64::FromStr` saturates an out-of-range magnitude to infinity
/// rather than failing, so `json_get_float` returns an infinity where a
/// NULL-on-overflow implementation would return NULL.
///
/// This is why `json_get_float` is not carved out for `BigQuery` pushdown:
/// `SAFE_CAST(… AS FLOAT64)` yields NULL for a value it cannot represent, so a
/// federated plan would answer NULL where this answers infinity. `json_get_int`
/// has no such divergence — Rust and `BigQuery` both give up on an
/// out-of-range integer — which is why only the integer form is pushed down.
#[test]
fn json_get_float_saturates_an_out_of_range_magnitude_to_infinity() {
    for (value, expected) in [
        (r#""1e400""#, f64::INFINITY),
        (r#""-1e400""#, f64::NEG_INFINITY),
        ("1e400", f64::INFINITY),
        ("-1e400", f64::NEG_INFINITY),
    ] {
        assert_eq!(
            get_float(&doc(value), &[Path::Key("a")]),
            Some(expected),
            "{value} must saturate, not become NULL"
        );
    }

    // The other end collapses to zero rather than failing, for the same reason.
    for value in [r#""1e-400""#, "1e-400"] {
        assert_eq!(
            get_float(&doc(value), &[Path::Key("a")]),
            Some(0.0),
            "{value} must underflow to zero"
        );
    }
}
