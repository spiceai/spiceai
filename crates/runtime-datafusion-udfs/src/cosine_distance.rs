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

use arrow::array::{
    Array, ArrayRef, Float64Array, Float64Builder, GenericListArray, LargeListArray, ListArray,
    OffsetSizeTrait,
};
use arrow_schema::DataType;
use arrow_schema::DataType::{FixedSizeList, Float64, LargeList, List};
use core::any::type_name;
use datafusion::common::cast::{
    as_float32_array, as_float64_array, as_generic_list_array, as_int32_array, as_int64_array,
};
use datafusion::common::utils::coerced_fixed_size_list_to_list;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::{
    common::{DataFusionError, Result as DataFusionResult, exec_err},
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
};
use std::sync::Arc;

use crate::vector_simd::{
    Kernel, compute_fsl_f32, make_scalar_function, matching_fixed_size_list_f32,
};

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

        // Keep FixedSizeList<Float32, N> as-is so invoke can take the SIMD path.
        if matching_fixed_size_list_f32(&arg_types[0], &arg_types[1]).is_some() {
            return Ok(vec![arg_types[0].clone(), arg_types[1].clone()]);
        }

        let mut result = Vec::with_capacity(2);
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
}

fn cosine_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("{COSINE_DISTANCE_UDF_NAME} expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        (FixedSizeList(_, _), FixedSizeList(_, _))
            if matching_fixed_size_list_f32(args[0].data_type(), args[1].data_type()).is_some() =>
        {
            compute_fsl_f32(args, Kernel::CosineDistance, |v| v)
        }
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

    // Fast path: flat primitive values — walk offsets into the child buffers
    // without allocating a per-row `ArrayRef` via `list.iter()` / `value(i)`.
    match (list_array1.value_type(), list_array2.value_type()) {
        (DataType::Float64, DataType::Float64) => {
            return general_cosine_distance_f64(list_array1, list_array2);
        }
        (DataType::Float32, DataType::Float32) => {
            return general_cosine_distance_f32(list_array1, list_array2);
        }
        _ => {}
    }

    // Fallback: nested lists or non-float element types.
    let n = list_array1.len();
    let mut builder = Float64Builder::with_capacity(n);
    for i in 0..n {
        if list_array1.is_null(i) || list_array2.is_null(i) {
            builder.append_null();
            continue;
        }
        // `value(i)` still builds an ArrayRef, but this path is only for nested
        // / uncommon element types; the Float32/Float64 hot path above avoids it.
        match compute_cosine_distance(Some(list_array1.value(i)), Some(list_array2.value(i)))? {
            Some(d) => builder.append_value(d),
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn general_cosine_distance_f64<O: OffsetSizeTrait>(
    list1: &GenericListArray<O>,
    list2: &GenericListArray<O>,
) -> DataFusionResult<ArrayRef> {
    let values1 = as_float64_array(list1.values())?;
    let values2 = as_float64_array(list2.values())?;
    let raw1 = values1.values().as_ref();
    let raw2 = values2.values().as_ref();
    let offsets1 = list1.value_offsets();
    let offsets2 = list2.value_offsets();
    let nulls1 = values1.nulls();
    let nulls2 = values2.nulls();
    let check_inner1 = values1.null_count() > 0;
    let check_inner2 = values2.null_count() > 0;

    let n = list1.len();
    let mut builder = Float64Builder::with_capacity(n);
    for i in 0..n {
        if list1.is_null(i) || list2.is_null(i) {
            builder.append_null();
            continue;
        }
        let start1 = offsets1[i].as_usize();
        let end1 = offsets1[i + 1].as_usize();
        let start2 = offsets2[i].as_usize();
        let end2 = offsets2[i + 1].as_usize();

        if check_inner1 && nulls1.is_some_and(|nb| (start1..end1).any(|j| nb.is_null(j))) {
            builder.append_null();
            continue;
        }
        if check_inner2 && nulls2.is_some_and(|nb| (start2..end2).any(|j| nb.is_null(j))) {
            builder.append_null();
            continue;
        }
        if end1 - start1 != end2 - start2 {
            return exec_err!("Both arrays must have the same length");
        }

        match cosine_distance_f64(&raw1[start1..end1], &raw2[start2..end2]) {
            Some(d) => builder.append_value(d),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

fn general_cosine_distance_f32<O: OffsetSizeTrait>(
    list1: &GenericListArray<O>,
    list2: &GenericListArray<O>,
) -> DataFusionResult<ArrayRef> {
    let values1 = as_float32_array(list1.values())?;
    let values2 = as_float32_array(list2.values())?;
    let raw1 = values1.values().as_ref();
    let raw2 = values2.values().as_ref();
    let offsets1 = list1.value_offsets();
    let offsets2 = list2.value_offsets();
    let nulls1 = values1.nulls();
    let nulls2 = values2.nulls();
    let check_inner1 = values1.null_count() > 0;
    let check_inner2 = values2.null_count() > 0;

    let n = list1.len();
    let mut builder = Float64Builder::with_capacity(n);
    for i in 0..n {
        if list1.is_null(i) || list2.is_null(i) {
            builder.append_null();
            continue;
        }
        let start1 = offsets1[i].as_usize();
        let end1 = offsets1[i + 1].as_usize();
        let start2 = offsets2[i].as_usize();
        let end2 = offsets2[i + 1].as_usize();

        if check_inner1 && nulls1.is_some_and(|nb| (start1..end1).any(|j| nb.is_null(j))) {
            builder.append_null();
            continue;
        }
        if check_inner2 && nulls2.is_some_and(|nb| (start2..end2).any(|j| nb.is_null(j))) {
            builder.append_null();
            continue;
        }
        if end1 - start1 != end2 - start2 {
            return exec_err!("Both arrays must have the same length");
        }

        match cosine_distance_f32(&raw1[start1..end1], &raw2[start2..end2]) {
            Some(d) => builder.append_value(d),
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
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

    // Float64: operate on the value buffer without cloning the array.
    if matches!(value1.data_type(), DataType::Float64)
        && matches!(value2.data_type(), DataType::Float64)
    {
        let f1 = as_float64_array(&value1)?;
        let f2 = as_float64_array(&value2)?;
        if f1.len() != f2.len() {
            return exec_err!("Both arrays must have the same length");
        }
        return Ok(cosine_distance_f64(f1.values(), f2.values()));
    }

    // Float32: same, promote while accumulating.
    if matches!(value1.data_type(), DataType::Float32)
        && matches!(value2.data_type(), DataType::Float32)
    {
        let f1 = as_float32_array(&value1)?;
        let f2 = as_float32_array(&value2)?;
        if f1.len() != f2.len() {
            return exec_err!("Both arrays must have the same length");
        }
        return Ok(cosine_distance_f32(f1.values(), f2.values()));
    }

    let float_vals1 = convert_to_f64_array(&value1)?;
    let float_vals2 = convert_to_f64_array(&value2)?;

    if float_vals1.len() != float_vals2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    Ok(cosine_distance_f64(
        float_vals1.values(),
        float_vals2.values(),
    ))
}

/// Computes the cosine distance between two equal-length f64 vectors.
///
/// Returns `None` when either vector has zero magnitude (e.g. an all-zero or
/// failed embedding): cosine similarity is undefined there (`0.0 / 0.0` is
/// `NaN`), and a `NaN` score sorts ahead of every real score in
/// `ORDER BY _score DESC`, surfacing failed embeddings as top matches.
fn cosine_distance_f64(x: &[f64], y: &[f64]) -> Option<f64> {
    let mut x_length: f64 = 0.0;
    let mut y_length: f64 = 0.0;
    let mut sum_squares: f64 = 0.0;

    for (&a, &b) in x.iter().zip(y.iter()) {
        x_length += a * a;
        y_length += b * b;
        sum_squares += a * b;
    }

    let similarity = sum_squares / (x_length.sqrt() * y_length.sqrt());

    // A zero-magnitude vector makes `similarity` NaN (and a non-finite value can
    // otherwise only arise from overflow). Guard it so callers get a NULL score
    // rather than a NaN that would sort to the top of `ORDER BY _score DESC`.
    if !similarity.is_finite() {
        return None;
    }

    // Convert cosine similarity [-1.0, 1.0] to cosine distance [0.0, 1.0]
    Some((1.0 - similarity) / 2.0)
}

/// Float32 variant of [`cosine_distance_f64`]; accumulates in f64.
fn cosine_distance_f32(x: &[f32], y: &[f32]) -> Option<f64> {
    let mut x_length: f64 = 0.0;
    let mut y_length: f64 = 0.0;
    let mut sum_squares: f64 = 0.0;

    for (&a, &b) in x.iter().zip(y.iter()) {
        let a = f64::from(a);
        let b = f64::from(b);
        x_length += a * a;
        y_length += b * b;
        sum_squares += a * b;
    }

    let similarity = sum_squares / (x_length.sqrt() * y_length.sqrt());
    if !similarity.is_finite() {
        return None;
    }
    Some((1.0 - similarity) / 2.0)
}

/// Thin wrapper kept for unit tests that construct `Float64Array`s directly.
#[cfg(test)]
fn cosine_distance(x: &Float64Array, y: &Float64Array) -> Option<f64> {
    cosine_distance_f64(x.values(), y.values())
}

/// Converts an array of any numeric type to a `Float64Array`.
///
/// Float64 inputs are not handled here — callers should use the zero-copy
/// [`as_float64_array`] path instead of cloning.
#[expect(clippy::cast_lossless, clippy::cast_precision_loss)]
fn convert_to_f64_array(array: &ArrayRef) -> DataFusionResult<Float64Array> {
    match array.data_type() {
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
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array};

    use super::{compute_cosine_distance, cosine_distance, cosine_distance_inner};
    use crate::vector_simd::testing::fsl_f32;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Float64Type;

    #[test]
    fn test_cosine_distance() {
        // Identical vectors -> similarity 1 -> distance 0.
        assert!(matches!(
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
            ),
            Some(d) if d.abs() < f64::EPSILON
        ));

        // Opposite vectors -> similarity -1 -> distance 1.
        assert!(matches!(
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![-1.0, -2.0, -3.0]),
            ),
            Some(d) if (d - 1.0).abs() < f64::EPSILON
        ));

        // Arbitrary vectors stay within the normalized [0, 1] range.
        assert!(matches!(
            cosine_distance(
                &Float64Array::from(vec![1000.0, 2000.0, 30.0]),
                &Float64Array::from(vec![-42.0, 123.0, -3.0]),
            ),
            Some(d) if (0.0..=1.0).contains(&d)
        ));
    }

    #[test]
    fn test_cosine_distance_zero_vector_is_null() {
        // A zero-magnitude vector has no defined direction; the distance must be
        // NULL (None) rather than NaN so failed/empty embeddings do not sort to
        // the top of `ORDER BY _score DESC`.
        assert_eq!(
            None,
            cosine_distance(
                &Float64Array::from(vec![0.0, 0.0, 0.0]),
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
            )
        );
        assert_eq!(
            None,
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![0.0, 0.0, 0.0]),
            )
        );
        assert_eq!(
            None,
            cosine_distance(
                &Float64Array::from(vec![0.0, 0.0]),
                &Float64Array::from(vec![0.0, 0.0]),
            )
        );
    }

    #[test]
    fn test_compute_cosine_distance_zero_vector_propagates_null() {
        // Exercise the production wrapper: a zero-magnitude vector must surface
        // as `Ok(None)` (SQL NULL), not `Ok(Some(NaN))`.
        let zero: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0]));
        let nonzero: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));

        let result = compute_cosine_distance(Some(zero), Some(nonzero));
        assert!(matches!(result, Ok(None)));

        // A normal pair still yields a finite distance.
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let result = compute_cosine_distance(Some(a), Some(b));
        assert!(matches!(result, Ok(Some(d)) if d.is_finite()));
    }

    #[test]
    fn fsl_f32_identical_is_zero() {
        let a = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(out.value(0).abs() < 1e-5);
    }

    #[test]
    fn fsl_f32_opposite_is_one() {
        let a = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = fsl_f32(&[&[-1.0, -2.0, -3.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 1.0).abs() < 1e-5);
    }

    #[test]
    fn fsl_f32_zero_vector_is_null() {
        let a = fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let b = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(out.is_null(0));
    }
}
