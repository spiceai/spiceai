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

//! [`ScalarUDFImpl`] definitions for cosine distance function.
//! Keep implementation inline with `<https://github.com/apache/datafusion/blob/main/datafusion/functions-nested/src/distance.rs#L47>`

use arrow::array::{Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, Float64, LargeList, List};
use core::any::type_name;
use datafusion::common::cast::{
    as_float32_array, as_float64_array, as_generic_list_array, as_int32_array, as_int64_array,
};
use datafusion::common::utils::coerced_fixed_size_list_to_list;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::{DataFusionError, Result as DataFusionResult, exec_err},
    logical_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility},
};
use std::any::Any;
use std::sync::Arc;

pub static COSINE_DISTANCE_UDF_NAME: &str = "cosine_distance";

macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::External(
                format!("could not cast to {}", type_name::<$ARRAY_TYPE>()).into(),
            )
        })?
    }};
}

/// array function wrapper that differentiates between scalar (length 1) and array.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> DataFusionResult<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> DataFusionResult<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        // first, identify if any of the arguments is an Array. If yes, store its `len`,
        // as any scalar will need to be converted to an array of len `len`.
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        // If all inputs are scalar, keeps output as scalar
        if len.is_none() {
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CosineDistance {
    signature: Signature,
}
impl Default for CosineDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl CosineDistance {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CosineDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        COSINE_DISTANCE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Ok(Float64),
            _ => exec_err!(
                "The {COSINE_DISTANCE_UDF_NAME} function can only accept List/LargeList/FixedSizeList."
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("{COSINE_DISTANCE_UDF_NAME} expects exactly two arguments");
        }
        let mut result = Vec::new();
        for arg_type in arg_types {
            match arg_type {
                List(_) | LargeList(_) | FixedSizeList(_, _) => {
                    result.push(coerced_fixed_size_list_to_list(arg_type));
                }
                _ => {
                    return exec_err!(
                        "The {COSINE_DISTANCE_UDF_NAME} function can only accept List/LargeList/FixedSizeList."
                    );
                }
            }
        }

        Ok(result)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(cosine_distance_inner)(&args.args)
    }

    fn schema_name(&self, args: &[Expr]) -> DataFusionResult<String> {
        let formatted_args: Vec<String> = args
            .iter()
            .map(|arg| match arg {
                Expr::Literal(scalar_value, _) => truncate_scalar_value(scalar_value),
                other => other.schema_name().to_string(),
            })
            .collect();
        Ok(format!("{}({})", self.name(), formatted_args.join(",")))
    }
}

/// Truncates large `ScalarValue` representations for readable EXPLAIN output.
/// List types show a compact type summary instead of dumping all values.
fn truncate_scalar_value(value: &ScalarValue) -> String {
    match value {
        ScalarValue::FixedSizeList(arr) => {
            if let DataType::FixedSizeList(field, size) = arr.data_type() {
                format!("<literal {}[{size}]>", field.data_type())
            } else {
                format!("<{}>", arr.data_type())
            }
        }
        ScalarValue::List(arr) => format!("<{}>", arr.data_type()),
        ScalarValue::LargeList(arr) => format!("<{}>", arr.data_type()),
        other => format!("{other}"),
    }
}

fn cosine_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("{COSINE_DISTANCE_UDF_NAME} expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        (List(_), List(_)) => general_cosine_distance::<i32>(args),
        (LargeList(_), LargeList(_)) => general_cosine_distance::<i64>(args),
        (array_type1, array_type2) => {
            exec_err!(
                "{COSINE_DISTANCE_UDF_NAME} does not support types '{array_type1:?}' and '{array_type2:?}'"
            )
        }
    }
}

fn general_cosine_distance<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_cosine_distance(arr1, arr2))
        .collect::<DataFusionResult<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

fn compute_cosine_distance(
    arr1: Option<ArrayRef>,
    arr2: Option<ArrayRef>,
) -> DataFusionResult<Option<f64>> {
    let Some(mut value1) = arr1 else {
        return Ok(None);
    };

    let Some(mut value2) = arr2 else {
        return Ok(None);
    };

    loop {
        match value1.data_type() {
            List(_) => {
                if downcast_arg!(value1, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value1, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value1 = downcast_arg!(value1, LargeListArray).value(0);
            }
            _ => break,
        }

        match value2.data_type() {
            List(_) => {
                if downcast_arg!(value2, ListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, ListArray).value(0);
            }
            LargeList(_) => {
                if downcast_arg!(value2, LargeListArray).null_count() > 0 {
                    return Ok(None);
                }
                value2 = downcast_arg!(value2, LargeListArray).value(0);
            }
            _ => break,
        }
    }

    // Check for NULL values inside the arrays
    if value1.null_count() != 0 || value2.null_count() != 0 {
        return Ok(None);
    }

    let float_vals1 = convert_to_f64_array(&value1)?;
    let float_vals2 = convert_to_f64_array(&value2)?;

    if float_vals1.len() != float_vals2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    Ok(Some(cosine_distance(&float_vals1, &float_vals2)))
}

fn cosine_distance(x: &Float64Array, y: &Float64Array) -> f64 {
    let mut x_length: f64 = 0.0;
    let mut y_length: f64 = 0.0;

    let sum_squares: f64 = x
        .iter()
        .zip(y.iter())
        .map(|(v1, v2)| {
            let a = v1.unwrap_or(0.0);
            let b = v2.unwrap_or(0.0);

            x_length += a * a;
            y_length += b * b;

            a * b
        })
        .sum();

    let similarity = sum_squares / (x_length.sqrt() * y_length.sqrt());

    // Convert cosine similarity [-1.0, 1.0] to cosine distance [0.0, 1.0]
    (1.0 - similarity) / 2.0
}

/// Converts an array of any numeric type to a `Float64Array`.
#[expect(clippy::cast_lossless, clippy::cast_precision_loss)]
fn convert_to_f64_array(array: &ArrayRef) -> DataFusionResult<Float64Array> {
    match array.data_type() {
        DataType::Float64 => Ok(as_float64_array(array)?.clone()),
        DataType::Float32 => {
            let array = as_float32_array(array)?;
            let converted: Float64Array = array.iter().map(|v| v.map(f64::from)).collect();
            Ok(converted)
        }
        DataType::Int64 => {
            let array = as_int64_array(array)?;
            let converted: Float64Array = array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int32 => {
            let array = as_int32_array(array)?;
            let converted: Float64Array = array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        _ => exec_err!("Unsupported array type for conversion to Float64Array"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Float64Array};
    use arrow::datatypes::{DataType, Field};
    use datafusion::logical_expr::{Expr, ScalarUDFImpl, col};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    use super::{CosineDistance, cosine_distance};

    #[expect(clippy::float_cmp)]
    #[test]
    fn test_cosine_distance() {
        assert_eq!(
            0.0,
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![1.0, 2.0, 3.0])
            )
        );

        assert_eq!(
            1.0,
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![-1.0, -2.0, -3.0])
            )
        );
        let dist = cosine_distance(
            &Float64Array::from(vec![1000.0, 2000.0, 30.0]),
            &Float64Array::from(vec![-42.0, 123.0, -3.0]),
        );
        assert!((0.0..=1.0).contains(&dist));
    }

    // -- schema_name tests covering all supported embedding types --

    #[test]
    fn test_schema_name_fixed_size_list_float32() {
        let udf = CosineDistance::new();

        let values = Float32Array::from(vec![0.1_f32; 1536]);
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let list_arr = FixedSizeListArray::try_new(field, 1536, Arc::new(values) as ArrayRef, None)
            .expect("valid fixed size list");
        let scalar = ScalarValue::FixedSizeList(Arc::new(list_arr));

        let args = vec![Expr::Literal(scalar, None), col("embedding_col")];
        let name = udf.schema_name(&args).expect("schema_name should succeed");

        assert_eq!(
            name,
            "cosine_distance(<literal Float32[1536]>,embedding_col)"
        );
    }

    #[test]
    fn test_schema_name_fixed_size_list_float64() {
        let udf = CosineDistance::new();

        let values = Float64Array::from(vec![0.1_f64; 768]);
        let field = Arc::new(Field::new("item", DataType::Float64, true));
        let list_arr = FixedSizeListArray::try_new(field, 768, Arc::new(values) as ArrayRef, None)
            .expect("valid fixed size list");
        let scalar = ScalarValue::FixedSizeList(Arc::new(list_arr));

        let args = vec![Expr::Literal(scalar, None), col("embedding_col")];
        let name = udf.schema_name(&args).expect("schema_name should succeed");

        assert_eq!(
            name,
            "cosine_distance(<literal Float64[768]>,embedding_col)"
        );
    }

    #[test]
    fn test_schema_name_preserves_small_literal() {
        let udf = CosineDistance::new();

        let small_literal = ScalarValue::Utf8(Some("hello".to_string()));
        let args = vec![Expr::Literal(small_literal, None), col("embedding_col")];
        let name = udf.schema_name(&args).expect("schema_name should succeed");

        assert_eq!(name, "cosine_distance(hello,embedding_col)");
    }

    #[test]
    fn test_schema_name_non_literal_args() {
        let udf = CosineDistance::new();

        let args = vec![col("vec_a"), col("vec_b")];
        let name = udf.schema_name(&args).expect("schema_name should succeed");

        assert_eq!(name, "cosine_distance(vec_a,vec_b)");
    }
}
