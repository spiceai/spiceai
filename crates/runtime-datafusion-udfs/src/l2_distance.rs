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

//! [`ScalarUDFImpl`] for Euclidean (L2) distance.
//!
//! Two UDFs are exposed:
//! - [`L2Distance`] / `l2_distance`: returns `sqrt(sum((a[i]-b[i])^2))`.
//! - [`L2SquaredDistance`] / `l2_squared_distance`: returns the same without the
//!   final sqrt. Useful for ranking since the square root is monotonic — skipping
//!   it saves a scalar op per row.
//!
//! Both require `FixedSizeList<Float32, N>` inputs and dispatch to SIMD via
//! [`simsimd`].

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::Result as DataFusionResult;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

use crate::vector_simd::{
    Kernel, coerce_fsl_f32_binary_args, compute_fsl_f32, fsl_f32_binary_return_type,
    make_scalar_function,
};

pub static L2_DISTANCE_UDF_NAME: &str = "l2_distance";
pub static L2_SQUARED_DISTANCE_UDF_NAME: &str = "l2_squared_distance";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct L2Distance {
    signature: Signature,
}

impl Default for L2Distance {
    fn default() -> Self {
        Self::new()
    }
}

impl L2Distance {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for L2Distance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        L2_DISTANCE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        fsl_f32_binary_return_type(L2_DISTANCE_UDF_NAME, arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        coerce_fsl_f32_binary_args(L2_DISTANCE_UDF_NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(l2_distance_inner)(&args.args)
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct L2SquaredDistance {
    signature: Signature,
}

impl Default for L2SquaredDistance {
    fn default() -> Self {
        Self::new()
    }
}

impl L2SquaredDistance {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for L2SquaredDistance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        L2_SQUARED_DISTANCE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        fsl_f32_binary_return_type(L2_SQUARED_DISTANCE_UDF_NAME, arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        coerce_fsl_f32_binary_args(L2_SQUARED_DISTANCE_UDF_NAME, arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(l2_squared_distance_inner)(&args.args)
    }
}

fn l2_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    compute_fsl_f32(args, Kernel::L2Squared, f64::sqrt)
}

fn l2_squared_distance_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    compute_fsl_f32(args, Kernel::L2Squared, |v| v)
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
        let out = l2_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!(out.value(0).abs() < 1e-6);
    }

    #[test]
    fn basic_l2() {
        // |[0,0,0] - [1,2,2]| = sqrt(9) = 3
        let a = fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let b = fsl_f32(&[&[1.0, 2.0, 2.0]]);
        let out = l2_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 3.0).abs() < 1e-5);
    }

    #[test]
    fn squared_matches_without_sqrt() {
        let a = fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let b = fsl_f32(&[&[1.0, 2.0, 2.0]]);
        let out = l2_squared_distance_inner(&[a, b]).expect("ok");
        let out = out.as_primitive::<Float64Type>();
        assert!((out.value(0) - 9.0).abs() < 1e-5);
    }
}
