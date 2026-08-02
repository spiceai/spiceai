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

//! [`ScalarUDFImpl`] for cosine distance of two vectors.
//!
//! Two dispatch paths based on input type:
//!
//! - **SIMD path**: both inputs are `FixedSizeList<Float32, N>` (or one is
//!   `List<Float32>`/`LargeList<Float32>` promoted to FSL via `coerce_types`) →
//!   dispatches to [`simsimd`] for AVX-512 / AVX2 / NEON / scalar acceleration.
//!
//! - **Scalar fallback**: `List` or `LargeList` inputs of any numeric element
//!   type → plain Rust loop, backwards-compatible with the original implementation.
//!
//! Both paths return `(1 - cosine_similarity) / 2` ∈ `[0, 1]` (0 = identical,
//! 1 = opposite). Zero-magnitude vectors have undefined cosine direction; both
//! paths treat them as orthogonal and return 0.5.

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
runtime_udfs_api::register_spice_function!(
    COSINE_DISTANCE_SPICE_FUNCTION,
    COSINE_DISTANCE_UDF_NAME
);

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

/// Returns `true` if `dt` is `List<Float32>` or `LargeList<Float32>`.
fn is_list_f32(dt: &DataType) -> bool {
    match dt {
        List(field) | LargeList(field) => field.data_type() == &DataType::Float32,
        _ => false,
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
        use crate::vector_simd::is_fixed_size_list_f32;

        if arg_types.len() != 2 {
            return exec_err!("{COSINE_DISTANCE_UDF_NAME} expects exactly two arguments");
        }
        let lhs = &arg_types[0];
        let rhs = &arg_types[1];

        // Case 1: both are FixedSizeList<Float32, N> with matching N → SIMD path.
        if matching_fixed_size_list_f32(lhs, rhs).is_some() {
            return Ok(vec![lhs.clone(), rhs.clone()]);
        }

        // Case 2: one is List<Float32>/LargeList<Float32>, the other is
        // FixedSizeList<Float32, N> → promote the List/LargeList to FSL so the
        // SIMD path handles it. Reuse the existing FSL type verbatim so its
        // field name/nullability/metadata are preserved and no spurious cast is
        // forced on the already-correct FSL argument.
        if is_list_f32(lhs) && is_fixed_size_list_f32(rhs) {
            return Ok(vec![rhs.clone(), rhs.clone()]);
        }
        if is_fixed_size_list_f32(lhs) && is_list_f32(rhs) {
            return Ok(vec![lhs.clone(), lhs.clone()]);
        }

        // Case 3: both are List/LargeList/FixedSizeList (any element type) →
        // scalar fallback; coerce to a consistent List type.
        // If either arg is LargeList, coerce both to LargeList to avoid
        // mismatched List/LargeList at execution.
        let use_large = matches!(lhs, LargeList(_)) || matches!(rhs, LargeList(_));
        let coerce_one = |dt: &DataType| -> DataFusionResult<DataType> {
            match dt {
                List(_) | LargeList(_) | FixedSizeList(_, _) => {
                    let list_type = coerced_fixed_size_list_to_list(dt);
                    if use_large {
                        // Wrap element type in LargeList if it isn't already.
                        match list_type {
                            List(field) => Ok(LargeList(field)),
                            other => Ok(other),
                        }
                    } else {
                        Ok(list_type)
                    }
                }
                _ => exec_err!(
                    "The {COSINE_DISTANCE_UDF_NAME} function can only accept List/LargeList/FixedSizeList."
                ),
            }
        };
        Ok(vec![coerce_one(lhs)?, coerce_one(rhs)?])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(cosine_distance_inner)(&args.args)
    }
}

pub(crate) fn cosine_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("{COSINE_DISTANCE_UDF_NAME} expects exactly two arguments");
    }

    match (&args[0].data_type(), &args[1].data_type()) {
        // SIMD path: both inputs are FixedSizeList<Float32, N>.
        // `simsimd`'s `cos` kernel returns `1 - cosine_similarity` ∈ [0, 2];
        // divide by 2 to map into the standard [0, 1] distance range.
        // Zero-magnitude vectors have an undefined direction; simsimd treats the
        // dot product as zero (orthogonal), yielding distance 0.5.
        (FixedSizeList(_, _), FixedSizeList(_, _)) => compute_fsl_f32_cosine(args),
        (List(_), List(_)) => general_cosine_distance::<i32>(args),
        (LargeList(_), LargeList(_)) => general_cosine_distance::<i64>(args),
        (array_type1, array_type2) => {
            exec_err!(
                "{COSINE_DISTANCE_UDF_NAME} does not support types '{array_type1:?}' and '{array_type2:?}'"
            )
        }
    }
}

/// Compute cosine distance for `FixedSizeList<Float32>` with a non-finite safety guard.
fn compute_fsl_f32_cosine(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    let result = compute_fsl_f32(args, Kernel::Cosine, |v| v / 2.0)?;
    let float64 = result
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("compute_fsl_f32 should return Float64Array".to_string())
        })?;

    // Safety net: convert any non-finite output to NULL to prevent NaN from
    // sorting ahead of real scores in `ORDER BY _score DESC`.
    let mut builder = arrow::array::Float64Builder::with_capacity(float64.len());
    for i in 0..float64.len() {
        if float64.is_null(i) {
            builder.append_null();
        } else {
            let v = float64.value(i);
            if v.is_finite() {
                builder.append_value(v);
            } else {
                builder.append_null();
            }
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
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

        builder.append_value(cosine_distance_f64(
            &raw1[start1..end1],
            &raw2[start2..end2],
        ));
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

        builder.append_value(cosine_distance_f32(
            &raw1[start1..end1],
            &raw2[start2..end2],
        ));
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
        return Ok(Some(cosine_distance_f64(f1.values(), f2.values())));
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
        return Ok(Some(cosine_distance_f32(f1.values(), f2.values())));
    }

    let float_vals1 = convert_to_f64_array(&value1)?;
    let float_vals2 = convert_to_f64_array(&value2)?;

    if float_vals1.len() != float_vals2.len() {
        return exec_err!("Both arrays must have the same length");
    }

    Ok(Some(cosine_distance_f64(
        float_vals1.values(),
        float_vals2.values(),
    )))
}

/// Computes the cosine distance between two equal-length f64 vectors.
///
/// Zero-magnitude vectors have no defined direction; they are treated as
/// orthogonal (cosine similarity = 0), returning distance `0.5`, consistent
/// with the SIMD path.
fn cosine_distance_f64(x: &[f64], y: &[f64]) -> f64 {
    let mut x_length: f64 = 0.0;
    let mut y_length: f64 = 0.0;
    let mut sum_squares: f64 = 0.0;

    for (&a, &b) in x.iter().zip(y.iter()) {
        x_length += a * a;
        y_length += b * b;
        sum_squares += a * b;
    }

    let similarity = sum_squares / (x_length.sqrt() * y_length.sqrt());

    // Zero-magnitude vectors produce NaN (0/0); treat as zero similarity
    // (orthogonal) so behavior matches the SIMD path → distance 0.5.
    let similarity = if similarity.is_finite() {
        similarity
    } else {
        0.0
    };

    // Convert cosine similarity [-1.0, 1.0] to cosine distance [0.0, 1.0]
    (1.0 - similarity) / 2.0
}

/// Float32 variant of [`cosine_distance_f64`]; accumulates in f64.
fn cosine_distance_f32(x: &[f32], y: &[f32]) -> f64 {
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
    let similarity = if similarity.is_finite() {
        similarity
    } else {
        0.0
    };
    (1.0 - similarity) / 2.0
}

/// Thin wrapper kept for unit tests that construct `Float64Array`s directly.
#[cfg(test)]
fn cosine_distance(x: &Float64Array, y: &Float64Array) -> f64 {
    cosine_distance_f64(x.values(), y.values())
}

/// Converts an array of any numeric type to a `Float64Array`.
///
/// Same-type Float64/Float32 pairs use the zero-copy paths in
/// [`compute_cosine_distance`]; this helper still accepts `Float64` so mixed
/// numeric pairs (e.g. `Float64` + `Int32`) keep working.
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
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Float64Array, Int32Array};
    use arrow_schema::Field;

    use super::{CosineDistance, compute_cosine_distance, cosine_distance, cosine_distance_inner};
    use crate::vector_simd::testing::fsl_f32;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;
    use arrow_schema::DataType;
    use datafusion::logical_expr::ScalarUDFImpl;

    #[test]
    fn test_cosine_distance() {
        // Identical vectors -> similarity 1 -> distance 0.
        assert!(
            cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
            )
            .abs()
                < f64::EPSILON
        );

        // Opposite vectors -> similarity -1 -> distance 1.
        assert!(
            (cosine_distance(
                &Float64Array::from(vec![1.0, 2.0, 3.0]),
                &Float64Array::from(vec![-1.0, -2.0, -3.0]),
            ) - 1.0)
                .abs()
                < f64::EPSILON
        );

        // Arbitrary vectors stay within the normalized [0, 1] range.
        assert!((0.0..=1.0).contains(&cosine_distance(
            &Float64Array::from(vec![1000.0, 2000.0, 30.0]),
            &Float64Array::from(vec![-42.0, 123.0, -3.0]),
        )));
    }

    #[test]
    fn test_cosine_distance_zero_vector_yields_orthogonal_distance() {
        // Zero-magnitude vectors are treated as orthogonal → distance 0.5,
        // consistent with the SIMD path.
        let half = |d: f64| (d - 0.5).abs() < 1e-10;
        assert!(half(cosine_distance(
            &Float64Array::from(vec![0.0, 0.0, 0.0]),
            &Float64Array::from(vec![1.0, 2.0, 3.0]),
        )));
        assert!(half(cosine_distance(
            &Float64Array::from(vec![1.0, 2.0, 3.0]),
            &Float64Array::from(vec![0.0, 0.0, 0.0]),
        )));
        assert!(half(cosine_distance(
            &Float64Array::from(vec![0.0, 0.0]),
            &Float64Array::from(vec![0.0, 0.0]),
        )));
    }

    #[test]
    fn test_compute_cosine_distance_zero_vector_yields_orthogonal_distance() {
        // Exercise the production wrapper: zero-magnitude vector → 0.5.
        let zero: ArrayRef = Arc::new(Float64Array::from(vec![0.0, 0.0, 0.0]));
        let nonzero: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));

        let result = compute_cosine_distance(Some(zero), Some(nonzero));
        assert!(matches!(result, Ok(Some(v)) if (v - 0.5).abs() < 1e-10));

        // A normal pair still yields a finite distance.
        let a: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let b: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
        let result = compute_cosine_distance(Some(a), Some(b));
        assert!(matches!(result, Ok(Some(d)) if d.is_finite()));
    }

    #[test]
    fn test_compute_cosine_distance_mixed_float64_int32() {
        // Mixed numeric element types fall through to `convert_to_f64_array`,
        // which must still accept Float64 (same behavior as trunk).
        let f64_side: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 0.0]));
        let i32_side: ArrayRef = Arc::new(Int32Array::from(vec![0, 1]));

        let result = compute_cosine_distance(Some(f64_side), Some(i32_side))
            .expect("mixed Float64/Int32 must convert");
        let distance = result.expect("non-null distance");
        assert!(
            (distance - 0.5).abs() < 1e-10,
            "orthogonal mixed-type vectors should yield 0.5, got {distance}"
        );
    }

    // --- SIMD (FixedSizeList<Float32>) path tests ---

    #[test]
    fn simd_identical_vectors_zero_distance() {
        let a = fsl_f32(&[&[1.0_f32, 2.0, 3.0]]) as ArrayRef;
        let b = fsl_f32(&[&[1.0_f32, 2.0, 3.0]]) as ArrayRef;
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(!out.is_null(0), "expected a value, got null");
        assert!(
            out.value(0).abs() < 1e-5,
            "expected ~0.0, got {}",
            out.value(0)
        );
    }

    #[test]
    fn simd_orthogonal_vectors_half_distance() {
        let a = fsl_f32(&[&[1.0_f32, 0.0]]) as ArrayRef;
        let b = fsl_f32(&[&[0.0_f32, 1.0]]) as ArrayRef;
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(!out.is_null(0), "expected a value, got null");
        assert!(
            (out.value(0) - 0.5).abs() < 1e-5,
            "expected ~0.5, got {}",
            out.value(0)
        );
    }

    #[test]
    fn simd_opposite_vectors_max_distance() {
        let a = fsl_f32(&[&[1.0_f32, 2.0, 3.0]]) as ArrayRef;
        let b = fsl_f32(&[&[-1.0_f32, -2.0, -3.0]]) as ArrayRef;
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(!out.is_null(0), "expected a value, got null");
        assert!(
            (out.value(0) - 1.0).abs() < 1e-5,
            "expected ~1.0, got {}",
            out.value(0)
        );
    }

    #[test]
    fn simd_zero_magnitude_vector_yields_orthogonal_distance() {
        // A zero-magnitude vector has no defined direction; simsimd treats the
        // dot product as zero (orthogonal), so the SIMD path returns 0.5.
        let a = fsl_f32(&[&[0.0_f32, 0.0, 0.0]]) as ArrayRef;
        let b = fsl_f32(&[&[1.0_f32, 2.0, 3.0]]) as ArrayRef;
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(
            !out.is_null(0) && (out.value(0) - 0.5).abs() < 1e-5,
            "expected 0.5 for zero-magnitude vector, got {}",
            out.value(0)
        );
    }

    // --- coerce_types tests ---

    fn fsl_f32_type(n: i32) -> DataType {
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), n)
    }

    fn list_f32_type() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
    }

    fn large_list_f32_type() -> DataType {
        DataType::LargeList(Arc::new(Field::new("item", DataType::Float32, true)))
    }

    #[test]
    fn coerce_both_fsl_returns_fsl() {
        let udf = CosineDistance::new();
        let result = udf
            .coerce_types(&[fsl_f32_type(3), fsl_f32_type(3)])
            .expect("ok");
        assert!(matches!(result[0], DataType::FixedSizeList(_, 3)));
        assert!(matches!(result[1], DataType::FixedSizeList(_, 3)));
    }

    #[test]
    fn coerce_list_and_fsl_promotes_to_fsl() {
        let udf = CosineDistance::new();
        // List<Float32> + FSL<Float32, 4> → both FSL<Float32, 4>
        let result = udf
            .coerce_types(&[list_f32_type(), fsl_f32_type(4)])
            .expect("ok");
        assert!(
            matches!(result[0], DataType::FixedSizeList(_, 4)),
            "got {:?}",
            result[0]
        );
        assert!(
            matches!(result[1], DataType::FixedSizeList(_, 4)),
            "got {:?}",
            result[1]
        );
    }

    #[test]
    fn coerce_fsl_and_list_promotes_to_fsl() {
        let udf = CosineDistance::new();
        // FSL<Float32, 5> + List<Float32> → both FSL<Float32, 5>
        let result = udf
            .coerce_types(&[fsl_f32_type(5), list_f32_type()])
            .expect("ok");
        assert!(
            matches!(result[0], DataType::FixedSizeList(_, 5)),
            "got {:?}",
            result[0]
        );
        assert!(
            matches!(result[1], DataType::FixedSizeList(_, 5)),
            "got {:?}",
            result[1]
        );
    }

    #[test]
    fn coerce_large_list_and_fsl_promotes_to_fsl() {
        let udf = CosineDistance::new();
        let result = udf
            .coerce_types(&[large_list_f32_type(), fsl_f32_type(6)])
            .expect("ok");
        assert!(
            matches!(result[0], DataType::FixedSizeList(_, 6)),
            "got {:?}",
            result[0]
        );
        assert!(
            matches!(result[1], DataType::FixedSizeList(_, 6)),
            "got {:?}",
            result[1]
        );
    }

    #[test]
    fn coerce_mixed_list_types_uses_large_list() {
        // One LargeList arg → both coerced to LargeList to stay consistent.
        let udf = CosineDistance::new();
        let list_f64 = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        let large_list_f64 =
            DataType::LargeList(Arc::new(Field::new("item", DataType::Float64, true)));
        let result = udf.coerce_types(&[list_f64, large_list_f64]).expect("ok");
        assert!(
            matches!(result[0], DataType::LargeList(_)),
            "got {:?}",
            result[0]
        );
        assert!(
            matches!(result[1], DataType::LargeList(_)),
            "got {:?}",
            result[1]
        );
    }
}
