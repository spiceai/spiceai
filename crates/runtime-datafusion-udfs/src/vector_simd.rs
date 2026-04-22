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

//! SIMD-backed vector similarity kernels used by the distance UDFs.
//!
//! Kernels dispatch to [`simsimd`], which selects the best available CPU
//! feature set at load time (AVX-512 / AVX2 / NEON / scalar). All helpers
//! operate on zero-copy `&[f32]` slices backed by Arrow buffers.

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float32Builder, Float64Builder,
};
use arrow_schema::DataType;
use datafusion::common::{DataFusionError, Result as DataFusionResult, exec_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use simsimd::SpatialSimilarity;
use std::sync::Arc;

/// Wraps a per-array kernel into a scalar/array dispatcher following `DataFusion`'s
/// `invoke_with_args` convention: scalar args are broadcast to the length of any
/// array arg, and the result is scalar when all inputs are scalar.
pub(crate) fn make_scalar_function<F>(
    inner: F,
) -> impl Fn(&[ColumnarValue]) -> DataFusionResult<ColumnarValue>
where
    F: Fn(&[ArrayRef]) -> DataFusionResult<ArrayRef>,
{
    move |args: &[ColumnarValue]| {
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let args = ColumnarValue::values_to_arrays(args)?;
        let result = (inner)(&args);

        if len.is_none() {
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}

/// Scalar similarity kernel for a single pair of equal-length f32 slices.
#[derive(Clone, Copy)]
pub(crate) enum Kernel {
    /// Dot product (inner product).
    Dot,
    /// Squared L2 distance. Sqrt is left to the caller when true L2 is needed.
    L2Squared,
}

impl Kernel {
    fn apply(self, a: &[f32], b: &[f32]) -> Option<f64> {
        match self {
            Self::Dot => f32::dot(a, b),
            Self::L2Squared => f32::l2sq(a, b),
        }
    }
}

/// Returns `true` if `dt` is `FixedSizeList<Float32, _>`.
pub(crate) fn is_fixed_size_list_f32(dt: &DataType) -> bool {
    matches!(dt, DataType::FixedSizeList(field, _) if field.data_type() == &DataType::Float32)
}

/// Checks that both arg types are `FixedSizeList<Float32, N>` with the same
/// positive `N`. Rejects `N <= 0` so planner errors surface before hitting the
/// SIMD kernel.
pub(crate) fn matching_fixed_size_list_f32(lhs: &DataType, rhs: &DataType) -> Option<i32> {
    match (lhs, rhs) {
        (DataType::FixedSizeList(lf, ln), DataType::FixedSizeList(rf, rn))
            if ln == rn
                && *ln > 0
                && lf.data_type() == &DataType::Float32
                && rf.data_type() == &DataType::Float32 =>
        {
            Some(*ln)
        }
        _ => None,
    }
}

/// Validate that a two-arg UDF call receives matching `FixedSizeList<Float32, N>`
/// inputs, returning the coerced arg types unchanged. Shared across
/// `cosine_distance`, `inner_product`, `l2_distance`, `l2_squared_distance`.
pub(crate) fn coerce_fsl_f32_binary_args(
    udf_name: &str,
    arg_types: &[DataType],
) -> DataFusionResult<Vec<DataType>> {
    if arg_types.len() != 2 {
        return exec_err!("{udf_name} expects exactly two arguments");
    }
    if matching_fixed_size_list_f32(&arg_types[0], &arg_types[1]).is_none() {
        return exec_err!(
            "{udf_name} requires both arguments to be FixedSizeList<Float32, N> with matching N, got {:?} and {:?}",
            arg_types[0],
            arg_types[1]
        );
    }
    Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
}

/// Validate arg shape for a two-arg UDF and return `Float64` as the return type.
pub(crate) fn fsl_f32_binary_return_type(
    udf_name: &str,
    arg_types: &[DataType],
) -> DataFusionResult<DataType> {
    if arg_types.len() != 2 {
        return exec_err!("{udf_name} expects exactly two arguments");
    }
    if !is_fixed_size_list_f32(&arg_types[0]) || !is_fixed_size_list_f32(&arg_types[1]) {
        return exec_err!("{udf_name} requires both arguments to be FixedSizeList<Float32, N>");
    }
    Ok(DataType::Float64)
}

fn as_fsl(arr: &ArrayRef) -> DataFusionResult<&FixedSizeListArray> {
    arr.as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_simd: expected FixedSizeListArray, got {}",
                arr.data_type()
            ))
        })
}

fn flat_f32(fsl: &FixedSizeListArray) -> DataFusionResult<&[f32]> {
    let values = fsl.values();
    let f32 = values
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_simd: FixedSizeList inner type must be Float32, got {}",
                values.data_type()
            ))
        })?;
    // Child nulls are ignored; rows with a null child slot are treated as nullable rows
    // at the outer FSL level (see `row_null_propagates`).
    Ok(f32.values())
}

/// Compute a per-row similarity over two equal-length `FixedSizeList<Float32>` arrays.
///
/// Produces a `Float64Array` with `post_process` applied to each raw kernel value.
/// Rows with null outer FSLs — or null inner slots — produce a null output.
pub(crate) fn compute_fsl_f32<F>(
    arrays: &[ArrayRef],
    kernel: Kernel,
    mut post_process: F,
) -> DataFusionResult<ArrayRef>
where
    F: FnMut(f64) -> f64,
{
    if arrays.len() != 2 {
        return exec_err!("vector_simd: expected exactly 2 arrays");
    }
    let a = as_fsl(&arrays[0])?;
    let b = as_fsl(&arrays[1])?;
    if a.len() != b.len() {
        return exec_err!(
            "vector_simd: lhs and rhs row counts differ ({} vs {})",
            a.len(),
            b.len()
        );
    }
    let dim_a = a.value_length();
    let dim_b = b.value_length();
    if dim_a != dim_b {
        return exec_err!("vector_simd: lhs and rhs dimensions differ ({dim_a} vs {dim_b})");
    }
    let dim = usize::try_from(dim_a)
        .map_err(|_| DataFusionError::Internal(format!("vector_simd: negative FSL dim {dim_a}")))?;
    if dim == 0 {
        return exec_err!("vector_simd: invalid FSL dim 0 (chunks_exact would panic)");
    }

    let flat_a = flat_f32(a)?;
    let flat_b = flat_f32(b)?;

    // Hoist null buffers out of the per-row loop. For fully-dense arrays (the
    // common case for precomputed embeddings) the `check_inner_nulls` flag is
    // false and the per-row inner-slot scan is skipped entirely.
    let a_outer = a.nulls();
    let b_outer = b.nulls();
    let a_inner = a.values().logical_nulls();
    let b_inner = b.values().logical_nulls();
    let check_inner_nulls = a_inner.is_some() || b_inner.is_some();

    let n = a.len();
    let mut builder = Float64Builder::with_capacity(n);
    let iter = flat_a.chunks_exact(dim).zip(flat_b.chunks_exact(dim));
    for (row, (slice_a, slice_b)) in iter.enumerate() {
        if a_outer.is_some_and(|nb| nb.is_null(row)) || b_outer.is_some_and(|nb| nb.is_null(row)) {
            builder.append_null();
            continue;
        }
        if check_inner_nulls {
            let start = row * dim;
            let end = start + dim;
            let any_inner_null = a_inner
                .as_ref()
                .is_some_and(|n| (start..end).any(|i| n.is_null(i)))
                || b_inner
                    .as_ref()
                    .is_some_and(|n| (start..end).any(|i| n.is_null(i)));
            if any_inner_null {
                builder.append_null();
                continue;
            }
        }
        let raw = kernel.apply(slice_a, slice_b).ok_or_else(|| {
            DataFusionError::Execution(
                "vector_simd: simsimd returned None (length mismatch)".to_string(),
            )
        })?;
        builder.append_value(post_process(raw));
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Compute L2 norm (sqrt of sum of squares) for each row of a `FixedSizeList<Float32>`.
///
/// Used by the `l2_norm` UDF. Output is `Float32Array` to preserve input precision.
/// Rows with a null outer FSL — or null inner slots — produce a null output.
pub(crate) fn compute_fsl_f32_l2_norm(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    let fsl = as_fsl(array)?;
    let dim = usize::try_from(fsl.value_length()).map_err(|_| {
        DataFusionError::Internal(format!(
            "vector_simd: negative FSL dim {}",
            fsl.value_length()
        ))
    })?;
    if dim == 0 {
        return exec_err!("vector_simd: invalid FSL dim 0 (chunks_exact would panic)");
    }
    let flat = flat_f32(fsl)?;
    let outer = fsl.nulls();
    let inner = fsl.values().logical_nulls();
    let check_inner_nulls = inner.is_some();

    let n = fsl.len();
    let mut builder = Float32Builder::with_capacity(n);
    for (row, slice) in flat.chunks_exact(dim).enumerate() {
        if outer.is_some_and(|nb| nb.is_null(row)) {
            builder.append_null();
            continue;
        }
        if check_inner_nulls {
            let start = row * dim;
            let end = start + dim;
            if inner
                .as_ref()
                .is_some_and(|n| (start..end).any(|i| n.is_null(i)))
            {
                builder.append_null();
                continue;
            }
        }
        let sq = f32::dot(slice, slice).ok_or_else(|| {
            DataFusionError::Execution("vector_simd: simsimd dot returned None".to_string())
        })?;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "sq fits in f32 by construction (bounded by input f32 magnitudes)"
        )]
        builder.append_value((sq as f32).sqrt());
    }
    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use arrow_schema::Field;

    pub(crate) fn fsl_f32(rows: &[&[f32]]) -> Arc<FixedSizeListArray> {
        let dim = i32::try_from(rows[0].len()).expect("dim fits in i32");
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let mut values = Float32Builder::with_capacity(rows.len() * rows[0].len());
        for row in rows {
            for &v in *row {
                values.append_value(v);
            }
        }
        let values = Arc::new(values.finish());
        Arc::new(
            FixedSizeListArray::try_new(field, dim, values, None)
                .expect("valid FixedSizeListArray"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    #[test]
    fn dot_basic() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = testing::fsl_f32(&[&[4.0, 5.0, 6.0]]);
        let out = compute_fsl_f32(&[a as ArrayRef, b as ArrayRef], Kernel::Dot, |v| v).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 32.0).abs() < 1e-5);
    }

    #[test]
    fn l2sq_basic() {
        let a = testing::fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let b = testing::fsl_f32(&[&[1.0, 2.0, 2.0]]);
        let out =
            compute_fsl_f32(&[a as ArrayRef, b as ArrayRef], Kernel::L2Squared, |v| v).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        // 1^2 + 2^2 + 2^2 = 9
        assert!((out.value(0) - 9.0).abs() < 1e-5);
    }

    #[test]
    fn null_outer_row_propagates() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]]);
        // Construct b with second row null by slicing a single-row fsl into a larger builder.
        let field = Arc::new(arrow_schema::Field::new("item", DataType::Float32, true));
        let inner = arrow::array::Float32Array::from(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            Some(0.0),
            Some(0.0),
            Some(0.0),
        ]);
        // Create null bitmap: row 0 valid, row 1 null.
        let nulls = arrow::buffer::NullBuffer::from(vec![true, false]);
        let b = Arc::new(
            FixedSizeListArray::try_new(field, 3, Arc::new(inner), Some(nulls)).expect("valid"),
        ) as ArrayRef;
        let out = compute_fsl_f32(&[a as ArrayRef, b], Kernel::Dot, |v| v).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(!out.is_null(0));
        assert!(out.is_null(1));
    }

    #[test]
    fn mismatched_dims_errors() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = testing::fsl_f32(&[&[1.0, 2.0]]);
        let err = compute_fsl_f32(&[a as ArrayRef, b as ArrayRef], Kernel::Dot, |v| v)
            .expect_err("should error");
        let msg = err.to_string();
        assert!(msg.contains("dimensions differ"), "got: {msg}");
    }
}
