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
//! `cosine_distance(a, b)` returns `(1 - cosine_similarity(a, b)) / 2`, mapping
//! the result into `[0, 1]` (0 = identical, 1 = opposite). Both inputs must be
//! `FixedSizeList<Float32, N>` with the same `N`. The kernel dispatches to SIMD
//! via [`simsimd`]; there is no fallback path for other element types — the UDF
//! rejects them at coercion.

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};

use crate::vector_simd::{
    Kernel, coerce_fsl_f32_binary_args, compute_fsl_f32, fsl_f32_binary_return_type,
    make_scalar_function,
};

pub static COSINE_DISTANCE_UDF_NAME: &str = "cosine_distance";

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
        fsl_f32_binary_return_type(COSINE_DISTANCE_UDF_NAME, arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        coerce_fsl_f32_binary_args(COSINE_DISTANCE_UDF_NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(cosine_distance_inner)(&args.args)
    }
}

/// `simsimd` returns `1 - similarity` in `[0, 2]`; divide by 2 to get the
/// Spice-standard `[0, 1]` range (`0` = identical, `1` = opposite).
fn cosine_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    compute_fsl_f32(args, Kernel::Cosine, |v| v / 2.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_simd::testing::fsl_f32;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    #[test]
    fn identical_vectors_zero_distance() {
        let a = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(out.value(0).abs() < 1e-6, "identical vectors must give 0, got {}", out.value(0));
    }

    #[test]
    fn opposite_vectors_max_distance() {
        let a = fsl_f32(&[&[1.0, 0.0]]);
        let b = fsl_f32(&[&[-1.0, 0.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(
            (out.value(0) - 1.0).abs() < 1e-6,
            "opposite vectors must give 1.0, got {}",
            out.value(0)
        );
    }

    #[test]
    fn orthogonal_vectors_half_distance() {
        let a = fsl_f32(&[&[1.0, 0.0]]);
        let b = fsl_f32(&[&[0.0, 1.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(
            (out.value(0) - 0.5).abs() < 1e-6,
            "orthogonal vectors must give 0.5, got {}",
            out.value(0)
        );
    }

    #[test]
    fn result_in_zero_to_one_range() {
        let a = fsl_f32(&[&[1000.0, 2000.0, 30.0]]);
        let b = fsl_f32(&[&[-42.0, 123.0, -3.0]]);
        let out = cosine_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        let v = out.value(0);
        assert!(
            (0.0..=1.0).contains(&v),
            "cosine_distance must be in [0, 1], got {v}"
        );
    }

    #[test]
    fn rejects_non_fsl_f32() {
        let udf = CosineDistance::new();
        let err = udf
            .coerce_types(&[DataType::Float32, DataType::Float32])
            .expect_err("should reject");
        assert!(err.to_string().contains("FixedSizeList"));
    }

    #[test]
    fn rejects_mismatched_dims() {
        let udf = CosineDistance::new();
        let lhs = DataType::FixedSizeList(
            std::sync::Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
            3,
        );
        let rhs = DataType::FixedSizeList(
            std::sync::Arc::new(arrow_schema::Field::new("item", DataType::Float32, true)),
            4,
        );
        let err = udf.coerce_types(&[lhs, rhs]).expect_err("should reject");
        assert!(err.to_string().contains("matching N"));
    }
}
