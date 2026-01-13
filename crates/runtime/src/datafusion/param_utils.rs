use std::collections::HashMap;

use datafusion::{common::ParamValues, scalar::ScalarValue};
use serde_json::Value;
use snafu::prelude::*;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Nested arrays or objects are not supported as parameter values"))]
    NestedValues,
    #[snafu(display("Unsupported JSON number format"))]
    UnsupportedJsonNumberFormat,
    #[snafu(display("Parameters must be a JSON array or an object"))]
    JsonArrayOrObjectRequired,
}

/// Converts a `serde_json::Value` into a `datafusion::common::ParamValues`.
///
/// # Arguments
/// * `json` - The JSON value to convert.
///
/// # Returns
/// * `Result<ParamValues, String>` - The converted `ParamValues` or an error message.
///
/// # Supported JSON Formats
/// - Array: Converted to positional parameters (`ParamValues::Positional`).
/// - Object: Converted to named parameters (`ParamValues::Named`).
/// - Primitive types (null, bool, number, string) are supported within arrays or objects.
///
/// # Errors
/// - Returns an error if the top-level JSON is not an array or object.
/// - Returns an error if any value cannot be converted to a `ScalarValue`.
pub fn convert_json_to_param_values(json: Value) -> Result<ParamValues, Error> {
    match json {
        Value::Array(arr) => {
            let mut vec = Vec::with_capacity(arr.len());
            for val in arr {
                let scalar = json_to_scalar(&val)?;
                vec.push(scalar);
            }
            Ok(ParamValues::from(vec))
        }
        Value::Object(obj) => {
            let mut map = HashMap::new();
            for (key, val) in obj {
                let scalar = json_to_scalar(&val)?;
                map.insert(key, scalar);
            }
            Ok(ParamValues::from(map))
        }
        _ => Err(Error::JsonArrayOrObjectRequired),
    }
}

/// Helper function to convert a single `serde_json::Value` to a `datafusion_common::scalar::ScalarValue`.
///
/// # Arguments
/// * `json` - The JSON value to convert.
///
/// # Returns
/// * `Result<ScalarValue, String>` - The converted `ScalarValue` or an error message.
///
/// # Supported JSON Types
/// - Null -> `ScalarValue::Utf8(None)`
/// - Bool -> `ScalarValue::Boolean(Some(bool))`
/// - Number -> `ScalarValue::Int64(Some(i64))` if integer, else `ScalarValue::Float64(Some(f64))`
/// - String -> `ScalarValue::Utf8(Some(String))`
/// - Arrays and objects return an error (handled at a higher level).
fn json_to_scalar(json: &Value) -> Result<ScalarValue, Error> {
    match json {
        Value::Null => Ok(ScalarValue::Utf8(None)),
        Value::Bool(b) => Ok(ScalarValue::Boolean(Some(*b))),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(ScalarValue::Int64(Some(i)))
            } else if let Some(f) = n.as_f64() {
                Ok(ScalarValue::Float64(Some(f)))
            } else {
                Err(Error::UnsupportedJsonNumberFormat)
            }
        }
        Value::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
        Value::Array(_) | Value::Object(_) => Err(Error::NestedValues),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;
    use serde_json::json;
    use std::{
        collections::HashMap,
        f64::consts::{E, PI},
    };

    fn assert_eq_param_values(a: &ParamValues, b: &ParamValues) {
        match (a, b) {
            (ParamValues::Map(map_a), ParamValues::Map(map_b)) => {
                // ScalarAndMetadata doesn't impl PartialEq, compare the value fields
                assert_eq!(map_a.len(), map_b.len());
                for (key, val_a) in map_a {
                    let val_b = map_b.get(key).expect("key in both maps");
                    assert_eq!(val_a.value(), val_b.value(), "mismatch for key {key}");
                }
            }
            (ParamValues::List(vec_a), ParamValues::List(vec_b)) => {
                // ScalarAndMetadata doesn't impl PartialEq, compare the value fields
                assert_eq!(vec_a.len(), vec_b.len());
                for (val_a, val_b) in vec_a.iter().zip(vec_b.iter()) {
                    assert_eq!(val_a.value(), val_b.value(), "list element mismatch");
                }
            }
            _ => {
                panic!("ParamValues are different types: {a:?} and {b:?}");
            }
        }
    }

    #[test]
    fn test_json_array() {
        let json = json!([1, "hello", true, null]);
        let got = convert_json_to_param_values(json).expect("convert to param values");
        let want = ParamValues::from(vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("hello".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Utf8(None),
        ]);

        assert_eq_param_values(&got, &want);
    }

    #[test]
    fn test_json_object() {
        let json = json!({"x": 42, "y": "world", "z": false});
        let got = convert_json_to_param_values(json).expect("convert to param values");
        let mut want = HashMap::new();
        want.insert("x".to_string(), ScalarValue::Int64(Some(42)));
        want.insert(
            "y".to_string(),
            ScalarValue::Utf8(Some("world".to_string())),
        );
        want.insert("z".to_string(), ScalarValue::Boolean(Some(false)));
        assert_eq_param_values(&got, &ParamValues::from(want));
    }

    #[test]
    fn test_invalid_top_level() {
        let json = json!(42);
        let result = convert_json_to_param_values(json);
        result.expect_err("should error on invalid top-level JSON");
    }

    #[test]
    fn test_array_with_nested_array() {
        let json = json!([1, [2, 3]]);
        let result = convert_json_to_param_values(json);
        result.expect_err("should error on nested array");
    }

    #[test]
    fn test_object_with_nested_object() {
        let json = json!({"a": 1, "b": {"c": 2}});
        let result = convert_json_to_param_values(json);
        result.expect_err("should error on nested object");
    }

    #[test]
    fn test_empty_array() {
        let json = json!([]);
        let result = convert_json_to_param_values(json).expect("convert to param values");
        assert_eq_param_values(&result, &ParamValues::from(Vec::<ScalarValue>::new()));
    }

    #[test]
    fn test_empty_object() {
        let json = json!({});
        let result = convert_json_to_param_values(json).expect("convert to param values");
        assert_eq_param_values(
            &result,
            &ParamValues::from(HashMap::<String, ScalarValue>::new()),
        );
    }

    #[test]
    fn test_array_with_nulls() {
        let json = json!([null, null]);
        let result = convert_json_to_param_values(json).expect("convert to param values");
        assert_eq_param_values(
            &result,
            &ParamValues::from(vec![ScalarValue::Utf8(None), ScalarValue::Utf8(None)]),
        );
    }

    #[test]
    fn test_object_with_nulls() {
        let json = json!({"a": null, "b": null});
        let result = convert_json_to_param_values(json).expect("convert to param values");
        let mut expected_map = HashMap::new();
        expected_map.insert("a".to_string(), ScalarValue::Utf8(None));
        expected_map.insert("b".to_string(), ScalarValue::Utf8(None));
        assert_eq_param_values(&result, &ParamValues::from(expected_map));
    }

    #[test]
    fn test_array_with_floats() {
        let json = json!([1.5, 2.0]);
        let result = convert_json_to_param_values(json).expect("convert to param values");
        assert_eq_param_values(
            &result,
            &ParamValues::from(vec![
                ScalarValue::Float64(Some(1.5)),
                ScalarValue::Float64(Some(2.0)),
            ]),
        );
    }

    #[test]
    fn test_object_with_floats() {
        let json = json!({"pi": PI, "e": E});
        let result = convert_json_to_param_values(json).expect("convert to param values");
        let mut expected_map = HashMap::new();
        expected_map.insert("pi".to_string(), ScalarValue::Float64(Some(PI)));
        expected_map.insert("e".to_string(), ScalarValue::Float64(Some(E)));
        assert_eq_param_values(&result, &ParamValues::from(expected_map));
    }

    #[test]
    fn test_array_with_strings_and_bools() {
        let json = json!(["test", true, false]);
        let result = convert_json_to_param_values(json).expect("convert to param values");
        assert_eq_param_values(
            &result,
            &ParamValues::from(vec![
                ScalarValue::Utf8(Some("test".to_string())),
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
            ]),
        );
    }

    #[test]
    fn test_object_with_strings_and_bools() {
        let json = json!({"name": "Alice", "is_active": true});
        let result = convert_json_to_param_values(json).expect("convert to param values");
        let mut expected_map = HashMap::new();
        expected_map.insert(
            "name".to_string(),
            ScalarValue::Utf8(Some("Alice".to_string())),
        );
        expected_map.insert("is_active".to_string(), ScalarValue::Boolean(Some(true)));
        assert_eq_param_values(&result, &ParamValues::from(expected_map));
    }

    #[test]
    fn test_error_with_specific_index() {
        let json = json!([1, "two", [3]]);
        let result = convert_json_to_param_values(json);
        result.expect_err("should error on nested array");
    }

    #[test]
    fn test_error_with_specific_key() {
        let json = json!({"a": 1, "b": "two", "c": {"d": 3}});
        let result = convert_json_to_param_values(json);
        result.expect_err("should error on nested object");
    }
}
