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

//! [`ScalarUDFImpl`] for the L2 norm (Euclidean length) of a vector.
//!
//! `l2_norm(v)` returns `sqrt(sum(v[i]^2))`. Input must be
//! `FixedSizeList<Float32, N>`; the kernel is SIMD-accelerated via [`simsimd`].

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;

use crate::vector_simd::{compute_fsl_f32_l2_norm, is_fixed_size_list_f32, make_scalar_function};

pub static L2_NORM_UDF_NAME: &str = "l2_norm";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct L2Norm {
    signature: Signature,
}

impl Default for L2Norm {
    fn default() -> Self {
        Self::new()
    }
}

impl L2Norm {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for L2Norm {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        L2_NORM_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() != 1 {
            return exec_err!("{L2_NORM_UDF_NAME} expects exactly one argument");
        }
        if !is_fixed_size_list_f32(&arg_types[0]) {
            return exec_err!("{L2_NORM_UDF_NAME} requires a FixedSizeList<Float32, N> argument");
        }
        Ok(DataType::Float32)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!("{L2_NORM_UDF_NAME} expects exactly one argument");
        }
        if !is_fixed_size_list_f32(&arg_types[0]) {
            return exec_err!(
                "{L2_NORM_UDF_NAME} requires a FixedSizeList<Float32, N> argument, got {:?}",
                arg_types[0]
            );
        }
        Ok(vec![arg_types[0].clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(l2_norm_inner)(&args.args)
    }
}

fn l2_norm_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    if args.len() != 1 {
        return exec_err!("{L2_NORM_UDF_NAME} expects exactly one argument");
    }
    compute_fsl_f32_l2_norm(&args[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_simd::testing::fsl_f32;
    use arrow::array::AsArray;
    use arrow::datatypes::Float32Type;

    #[test]
    fn basic_norm() {
        // |[3, 4]| = 5
        let v = fsl_f32(&[&[3.0, 4.0]]);
        let out = l2_norm_inner(&[v]).expect("ok");
        let out = out.as_primitive::<Float32Type>();
        assert!((out.value(0) - 5.0).abs() < 1e-5);
    }

    #[test]
    fn zero_vector_zero_norm() {
        let v = fsl_f32(&[&[0.0, 0.0, 0.0]]);
        let out = l2_norm_inner(&[v]).expect("ok");
        let out = out.as_primitive::<Float32Type>();
        assert!(out.value(0).abs() < 1e-6);
    }
}
