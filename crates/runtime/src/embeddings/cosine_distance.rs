/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::{
    Array, ArrayRef, Float64Array, LargeListArray, ListArray, OffsetSizeTrait,
};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, Float64, LargeList, List};
use datafusion::common::cast::{
    as_float32_array, as_float64_array, as_generic_list_array, as_int32_array,
    as_int64_array,
};
use datafusion::common::utils::coerced_fixed_size_list_to_list;
use datafusion::scalar::ScalarValue;
use datafusion::{
     common::{exec_err, DataFusionError, Result as DataFusionResult},
     logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
};
use core::any::type_name;
    
use std::any::Any;
use std::sync::Arc;


macro_rules! make_udf_expr_and_func {
    ($UDF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr , $SCALAR_UDF_FN:ident) => {
        paste::paste! {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN($($arg: datafusion::logical_expr::Expr),*) -> datafusion::logical_expr::Expr {
                datafusion::logical_expr::Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    vec![$($arg),*],
                ))
            }
            create_func!($UDF, $SCALAR_UDF_FN);
        }
    };
    ($UDF:ty, $EXPR_FN:ident, $DOC:expr , $SCALAR_UDF_FN:ident) => {
        paste::paste! {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN(arg: Vec<datafusion::logical_expr::Expr>) -> datafusion::logical_expr::Expr {
                datafusion::logical_expr::Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    $SCALAR_UDF_FN(),
                    arg,
                ))
            }
            create_func!($UDF, $SCALAR_UDF_FN);
        }
    };
}

/// Creates a singleton `ScalarUDF` of the `$UDF` function named `STATIC_$(UDF)` and a
/// function named `$SCALAR_UDF_FUNC` which returns that function named `STATIC_$(UDF)`.
///
/// This is used to ensure creating the list of `ScalarUDF` only happens once.
///
/// # Arguments
/// * `UDF`: name of the [`ScalarUDFImpl`]
/// * `SCALAR_UDF_FUNC`: name of the function to create (just) the `ScalarUDF`
///
/// [`ScalarUDFImpl`]: datafusion::logical_expr::ScalarUDFImpl
macro_rules! create_func {
    ($UDF:ty, $SCALAR_UDF_FN:ident) => {
        paste::paste! {
            /// Singleton instance of [`$UDF`], ensures the UDF is only created once
            /// named STATIC_$(UDF). For example `STATIC_ArrayToString`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDF >]: std::sync::OnceLock<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> =
                std::sync::OnceLock::new();

            #[doc = concat!("ScalarFunction that returns a [`ScalarUDF`](datafusion::logical_expr::ScalarUDF) for ")]
            #[doc = stringify!($UDF)]
            pub fn $SCALAR_UDF_FN() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
                [< STATIC_ $UDF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                            <$UDF>::new(),
                        ))
                    })
                    .clone()
            }
        }
    };
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

        let is_scalar = len.is_none();

        let args = ColumnarValue::values_to_arrays(args)?;

        let result = (inner)(&args);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

make_udf_expr_and_func!(
    ArrayDistance,
    array_distance,
    array,
    "returns the Euclidean distance between two numeric arrays.",
    array_distance_udf
);


macro_rules! downcast_arg {
    ($ARG:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast to {}",
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

#[derive(Debug)]
pub(super) struct ArrayDistance {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayDistance {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["list_distance".to_string()],
        }
    }
}

impl ScalarUDFImpl for ArrayDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "array_distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        match arg_types[0] {
            List(_) | LargeList(_) | FixedSizeList(_, _) => Ok(Float64),
            _ => exec_err!("The array_distance function can only accept List/LargeList/FixedSizeList."),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("array_distance expects exactly two arguments");
        }
        let mut result = Vec::new();
        for arg_type in arg_types {
            match arg_type {
                List(_) | LargeList(_) | FixedSizeList(_, _) => result.push(coerced_fixed_size_list_to_list(arg_type)),
                _ => return exec_err!("The array_distance function can only accept List/LargeList/FixedSizeList."),
            }
        }

        Ok(result)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(array_distance_inner)(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn array_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("array_distance expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        (List(_), List(_)) => general_array_distance::<i32>(args),
        (LargeList(_), LargeList(_)) => general_array_distance::<i64>(args),
        (array_type1, array_type2) => {
            exec_err!("array_distance does not support types '{array_type1:?}' and '{array_type2:?}'")
        }
    }
}

fn general_array_distance<O: OffsetSizeTrait>(arrays: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    let list_array1 = as_generic_list_array::<O>(&arrays[0])?;
    let list_array2 = as_generic_list_array::<O>(&arrays[1])?;

    let result = list_array1
        .iter()
        .zip(list_array2.iter())
        .map(|(arr1, arr2)| compute_array_distance(arr1, arr2))
        .collect::<DataFusionResult<Float64Array>>()?;

    Ok(Arc::new(result) as ArrayRef)
}

/// Computes the Euclidean distance between two arrays
fn compute_array_distance(
    arr1: Option<ArrayRef>,
    arr2: Option<ArrayRef>,
) -> DataFusionResult<Option<f64>> {
    let value1 = match arr1 {
        Some(arr) => arr,
        None => return Ok(None),
    };
    let value2 = match arr2 {
        Some(arr) => arr,
        None => return Ok(None),
    };

    let mut value1 = value1;
    let mut value2 = value2;

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

    let values1 = convert_to_f64_array(&value1)?;
    let values2 = convert_to_f64_array(&value2)?;

    if values1.len() != values2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    let mut a1_length: f64 = 0.0;
    let mut a2_length: f64 = 0.0;

    let sum_squares: f64 = values1
        .iter()
        .zip(values2.iter())
        .map(|(v1, v2)| {
            let a = v1.unwrap_or(0.0);
            let b = v2.unwrap_or(0.0);

            a1_length += a * a;
            a2_length += b * b;

            a * b 
            
        })
        .sum();

    let a1_norm = (a1_length * a1_length).sqrt();
    let a2_norm = (a2_length * a2_length).sqrt();

    Ok(Some(sum_squares/(a1_norm * a2_norm)))
}

/// Converts an array of any numeric type to a Float64Array.
fn convert_to_f64_array(array: &ArrayRef) -> DataFusionResult<Float64Array> {
    match array.data_type() {
        DataType::Float64 => Ok(as_float64_array(array)?.clone()),
        DataType::Float32 => {
            let array = as_float32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int64 => {
            let array = as_int64_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        DataType::Int32 => {
            let array = as_int32_array(array)?;
            let converted: Float64Array =
                array.iter().map(|v| v.map(|v| v as f64)).collect();
            Ok(converted)
        }
        _ => exec_err!("Unsupported array type for conversion to Float64Array"),
    }
}
