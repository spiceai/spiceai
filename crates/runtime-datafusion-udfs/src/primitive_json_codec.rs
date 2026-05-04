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

//! Shared primitive-scalar ↔ JSON codec used by the tool-backed SQL UDF bridge.
//!
//! The runtime crate needs a few small building blocks: parse a primitive
//! Arrow-type shorthand, read a row cell as a JSON value, and build a
//! primitive-typed Arrow output column from a stream of JSON values.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Array, Float64Builder, Int64Array,
    Int64Builder, StringArray, StringBuilder,
};
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use serde_json::Value;

/// Parse a spicepod arrow-type string into one of the primitive Arrow
/// types the JSON codec supports.
///
/// The set is deliberately narrow: `int64 / float64 / utf8 / boolean`,
/// plus shorthand aliases (`int`, `double`, `string`, `bool`) and
/// narrower widths that silently widen to the canonical form. Case is
/// ignored. Unsupported types return [`None`] — callers format a
/// tier-specific error (remote vs. tool) with that return.
#[must_use]
pub fn parse_primitive_arrow_type(s: &str) -> Option<DataType> {
    match s.trim().to_ascii_lowercase().as_str() {
        // All narrower integer widths widen to Int64 over the JSON wire.
        "int8" | "int16" | "int32" | "int64" | "int" => Some(DataType::Int64),
        // Float32 widens to Float64 for the same reason.
        "float32" | "float64" | "double" | "float" => Some(DataType::Float64),
        "utf8" | "string" => Some(DataType::Utf8),
        "boolean" | "bool" => Some(DataType::Boolean),
        _ => None,
    }
}

/// The JSON Schema type name for the JSON-encoded form of a primitive
/// Arrow type. Returns [`None`] for non-primitives.
#[must_use]
pub fn arrow_to_json_schema_type(arrow: &str) -> Option<&'static str> {
    parse_primitive_arrow_type(arrow).map(|dt| match dt {
        DataType::Int64 => "integer",
        DataType::Float64 => "number",
        DataType::Utf8 => "string",
        DataType::Boolean => "boolean",
        _ => unreachable!("parse_primitive_arrow_type only returns the four primitive DataTypes"),
    })
}

/// Serialise one row of a primitive-typed Arrow column as a JSON value.
///
/// `ty` must be the `DataType` that `parse_primitive_arrow_type` would
/// return for the declared column type; violating this contract is a
/// programmer error and surfaces as [`DataFusionError::Execution`].
///
/// # Errors
///
/// Returns [`DataFusionError::Execution`] when the column cannot be
/// downcast to the expected primitive Arrow array, or when a `Float64`
/// value is NaN/Inf (JSON has no representation for those).
pub fn array_cell_to_json(
    array: &ArrayRef,
    row: usize,
    ty: &DataType,
) -> Result<Value, DataFusionError> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    match ty {
        DataType::Int64 => {
            let a = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Execution("expected Int64Array".into()))?;
            Ok(Value::Number(a.value(row).into()))
        }
        DataType::Float64 => {
            let a = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Execution("expected Float64Array".into()))?;
            serde_json::Number::from_f64(a.value(row))
                .map(Value::Number)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "float64 value is NaN/Inf, cannot encode as JSON".into(),
                    )
                })
        }
        DataType::Utf8 => {
            let a = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("expected StringArray".into()))?;
            Ok(Value::String(a.value(row).to_string()))
        }
        DataType::Boolean => {
            let a = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| DataFusionError::Execution("expected BooleanArray".into()))?;
            Ok(Value::Bool(a.value(row)))
        }
        other => Err(DataFusionError::Execution(format!(
            "cannot encode arg of type {other:?} as JSON"
        ))),
    }
}

/// Primitive-typed Arrow output builder fed by [`serde_json::Value`]s.
/// Used on the response side of JSON-wire UDFs to translate values back
/// into a columnar result.
pub enum PrimitiveOutputBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Boolean(BooleanBuilder),
}

impl PrimitiveOutputBuilder {
    /// Construct a builder for the declared return [`DataType`]. Fails
    /// for any non-primitive type.
    ///
    /// # Errors
    ///
    /// Returns [`DataFusionError::Execution`] when `ty` is not one of
    /// the supported primitives.
    pub fn new(ty: &DataType, capacity: usize) -> Result<Self, DataFusionError> {
        Ok(match ty {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            other => {
                return Err(DataFusionError::Execution(format!(
                    "primitive JSON-codec return type {other:?} not supported"
                )));
            }
        })
    }

    /// Append every value in `values` in order.
    ///
    /// # Errors
    ///
    /// Surfaces the first [`append_value`](Self::append_value) error.
    pub fn append_values(&mut self, values: &[Value]) -> Result<(), DataFusionError> {
        for v in values {
            self.append_value(v)?;
        }
        Ok(())
    }

    /// Append a single JSON value, converting it to match the builder's
    /// declared type.
    ///
    /// # Errors
    ///
    /// Returns [`DataFusionError::Execution`] when the value's JSON
    /// type does not match the declared return type, or a number is
    /// not representable in the target Arrow type.
    pub fn append_value(&mut self, v: &Value) -> Result<(), DataFusionError> {
        match (self, v) {
            (Self::Int64(b), Value::Null) => b.append_null(),
            (Self::Int64(b), Value::Number(n)) => {
                let i = n.as_i64().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "JSON-wire UDF returned non-i64-representable number: {n}"
                    ))
                })?;
                b.append_value(i);
            }
            (Self::Float64(b), Value::Null) => b.append_null(),
            (Self::Float64(b), Value::Number(n)) => {
                let f = n.as_f64().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "JSON-wire UDF returned non-f64-representable number: {n}"
                    ))
                })?;
                b.append_value(f);
            }
            (Self::Utf8(b), Value::Null) => b.append_null(),
            (Self::Utf8(b), Value::String(s)) => b.append_value(s),
            (Self::Boolean(b), Value::Null) => b.append_null(),
            (Self::Boolean(b), Value::Bool(x)) => b.append_value(*x),
            (_, other) => {
                return Err(DataFusionError::Execution(format!(
                    "JSON-wire UDF returned a value whose type does not match the declared return: {other}"
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn finish(mut self) -> ArrayRef {
        match &mut self {
            Self::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn parse_primitive_covers_aliases() {
        assert_eq!(parse_primitive_arrow_type("int64"), Some(DataType::Int64));
        assert_eq!(parse_primitive_arrow_type("int"), Some(DataType::Int64));
        assert_eq!(parse_primitive_arrow_type("INT32"), Some(DataType::Int64));
        assert_eq!(
            parse_primitive_arrow_type("float32"),
            Some(DataType::Float64)
        );
        assert_eq!(parse_primitive_arrow_type("STRING"), Some(DataType::Utf8));
        assert_eq!(parse_primitive_arrow_type("bool"), Some(DataType::Boolean));
        assert_eq!(parse_primitive_arrow_type("decimal(10,2)"), None);
    }

    #[test]
    fn arrow_to_json_schema_type_matches() {
        assert_eq!(arrow_to_json_schema_type("int64"), Some("integer"));
        assert_eq!(arrow_to_json_schema_type("float64"), Some("number"));
        assert_eq!(arrow_to_json_schema_type("utf8"), Some("string"));
        assert_eq!(arrow_to_json_schema_type("bool"), Some("boolean"));
        assert_eq!(arrow_to_json_schema_type("list"), None);
    }

    #[test]
    fn round_trip_int64() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let v0 = array_cell_to_json(&arr, 0, &DataType::Int64).expect("test");
        let v1 = array_cell_to_json(&arr, 1, &DataType::Int64).expect("test");
        let v2 = array_cell_to_json(&arr, 2, &DataType::Int64).expect("test");
        assert_eq!(v0, Value::Number(1.into()));
        assert_eq!(v1, Value::Null);
        assert_eq!(v2, Value::Number(3.into()));

        let mut b = PrimitiveOutputBuilder::new(&DataType::Int64, 3).expect("test");
        b.append_values(&[v0, v1, v2]).expect("test");
        let out = b.finish();
        let out_i64 = out.as_any().downcast_ref::<Int64Array>().expect("test");
        assert_eq!(out_i64.value(0), 1);
        assert!(out_i64.is_null(1));
        assert_eq!(out_i64.value(2), 3);
    }
}
