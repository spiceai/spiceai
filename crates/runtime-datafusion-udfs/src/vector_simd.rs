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
use simsimd::SpatialSimilarity;
use std::sync::Arc;

/// Scalar similarity kernel for a single pair of equal-length f32 slices.
#[derive(Clone, Copy)]
pub(crate) enum Kernel {
    /// Cosine distance in `[0, 2]` — matches the simsimd definition `1 - cos_sim`.
    CosineRaw,
    /// Dot product (inner product).
    Dot,
    /// Squared L2 distance. Sqrt is left to the caller when true L2 is needed.
    L2Squared,
}

impl Kernel {
    fn apply(self, a: &[f32], b: &[f32]) -> Option<f64> {
        match self {
            Self::CosineRaw => f32::cosine(a, b),
            Self::Dot => f32::dot(a, b),
            Self::L2Squared => f32::l2sq(a, b),
        }
    }
}

/// Returns `true` if `dt` is `FixedSizeList<Float32, _>`.
pub(crate) fn is_fixed_size_list_f32(dt: &DataType) -> bool {
    matches!(dt, DataType::FixedSizeList(field, _) if field.data_type() == &DataType::Float32)
}

/// Checks that both arg types are `FixedSizeList<Float32, N>` with the same `N`.
pub(crate) fn matching_fixed_size_list_f32(
    lhs: &DataType,
    rhs: &DataType,
) -> Option<i32> {
    match (lhs, rhs) {
        (DataType::FixedSizeList(lf, ln), DataType::FixedSizeList(rf, rn))
            if ln == rn
                && lf.data_type() == &DataType::Float32
                && rf.data_type() == &DataType::Float32 =>
        {
            Some(*ln)
        }
        _ => None,
    }
}

fn as_fsl<'a>(arr: &'a ArrayRef) -> DataFusionResult<&'a FixedSizeListArray> {
    arr.as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_simd: expected FixedSizeListArray, got {}",
                arr.data_type()
            ))
        })
}

fn flat_f32<'a>(fsl: &'a FixedSizeListArray) -> DataFusionResult<&'a [f32]> {
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

/// Returns `true` if either outer FSL row is null or any of its f32 slots is null.
fn row_null_propagates(
    fsl_a: &FixedSizeListArray,
    fsl_b: &FixedSizeListArray,
    row: usize,
    dim: usize,
) -> bool {
    if fsl_a.is_null(row) || fsl_b.is_null(row) {
        return true;
    }
    let values_a = fsl_a.values();
    let values_b = fsl_b.values();
    let start = row * dim;
    let end = start + dim;
    // Nulls inside the child Float32Array propagate to the row result.
    let a_nulls = values_a.logical_nulls();
    let b_nulls = values_b.logical_nulls();
    if let Some(n) = a_nulls.as_ref()
        && (start..end).any(|i| n.is_null(i))
    {
        return true;
    }
    if let Some(n) = b_nulls.as_ref()
        && (start..end).any(|i| n.is_null(i))
    {
        return true;
    }
    false
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
        return exec_err!(
            "vector_simd: lhs and rhs dimensions differ ({dim_a} vs {dim_b})"
        );
    }
    let dim = usize::try_from(dim_a).map_err(|_| {
        DataFusionError::Internal(format!("vector_simd: negative FSL dim {dim_a}"))
    })?;

    let flat_a = flat_f32(a)?;
    let flat_b = flat_f32(b)?;

    let n = a.len();
    let mut builder = Float64Builder::with_capacity(n);
    for row in 0..n {
        if row_null_propagates(a, b, row, dim) {
            builder.append_null();
            continue;
        }
        let start = row * dim;
        let end = start + dim;
        let raw = kernel
            .apply(&flat_a[start..end], &flat_b[start..end])
            .ok_or_else(|| {
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
pub(crate) fn compute_fsl_f32_l2_norm(array: &ArrayRef) -> DataFusionResult<ArrayRef> {
    let fsl = as_fsl(array)?;
    let dim = usize::try_from(fsl.value_length()).map_err(|_| {
        DataFusionError::Internal(format!(
            "vector_simd: negative FSL dim {}",
            fsl.value_length()
        ))
    })?;
    let flat = flat_f32(fsl)?;

    let n = fsl.len();
    let mut builder = Float32Builder::with_capacity(n);
    for row in 0..n {
        if fsl.is_null(row) {
            builder.append_null();
            continue;
        }
        let start = row * dim;
        let end = start + dim;
        // Use simsimd dot against self for SIMD sum-of-squares.
        let sq = f32::dot(&flat[start..end], &flat[start..end]).ok_or_else(|| {
            DataFusionError::Execution("vector_simd: simsimd dot returned None".to_string())
        })?;
        #[allow(clippy::cast_possible_truncation)]
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
    fn cosine_raw_identical_is_zero() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let out = compute_fsl_f32(
            &[a as ArrayRef, b as ArrayRef],
            Kernel::CosineRaw,
            |v| v,
        )
        .expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_raw_opposite_is_two() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = testing::fsl_f32(&[&[-1.0, -2.0, -3.0]]);
        let out = compute_fsl_f32(
            &[a as ArrayRef, b as ArrayRef],
            Kernel::CosineRaw,
            |v| v,
        )
        .expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 2.0).abs() < 1e-5);
    }

    #[test]
    fn dot_basic() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = testing::fsl_f32(&[&[4.0, 5.0, 6.0]]);
        let out = compute_fsl_f32(&[a as ArrayRef, b as ArrayRef], Kernel::Dot, |v| v)
            .expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 32.0).abs() < 1e-5);
    }

    #[test]
    fn l2sq_basic() {
        let a = testing::fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let b = testing::fsl_f32(&[&[1.0, 2.0, 2.0]]);
        let out = compute_fsl_f32(
            &[a as ArrayRef, b as ArrayRef],
            Kernel::L2Squared,
            |v| v,
        )
        .expect("ok");
        let out = out.as_primitive::<Float64Type>();
        // 1^2 + 2^2 + 2^2 = 9
        assert!((out.value(0) - 9.0).abs() < 1e-5);
    }

    #[test]
    fn null_outer_row_propagates() {
        let a = testing::fsl_f32(&[&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]]);
        // Construct b with second row null by slicing a single-row fsl into a larger builder.
        let field = Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Float32,
            true,
        ));
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
            FixedSizeListArray::try_new(field, 3, Arc::new(inner), Some(nulls))
                .expect("valid"),
        ) as ArrayRef;
        let out = compute_fsl_f32(
            &[a as ArrayRef, b],
            Kernel::Dot,
            |v| v,
        )
        .expect("ok");
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
