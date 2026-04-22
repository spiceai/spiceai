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

//! [`ScalarUDFImpl`] for inner (dot) product of two vectors.
//!
//! `inner_product(a, b)` returns `sum(a[i] * b[i])` over the two input vectors.
//! Both inputs must be `FixedSizeList<Float32, N>` with the same `N`. The kernel
//! dispatches to SIMD via [`simsimd`]; there is no fallback path for other
//! element types — the UDF rejects them at coercion.

use arrow::array::ArrayRef;
use arrow_schema::DataType;
use datafusion::common::{Result as DataFusionResult, exec_err};
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::{
    logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility},
};
use std::any::Any;

use crate::cosine_distance::make_scalar_function;
use crate::vector_simd::{Kernel, compute_fsl_f32, is_fixed_size_list_f32, matching_fixed_size_list_f32};

pub static INNER_PRODUCT_UDF_NAME: &str = "inner_product";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct InnerProduct {
    signature: Signature,
}

impl Default for InnerProduct {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerProduct {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for InnerProduct {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        INNER_PRODUCT_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        if arg_types.len() != 2 {
            return exec_err!("{INNER_PRODUCT_UDF_NAME} expects exactly two arguments");
        }
        if !is_fixed_size_list_f32(&arg_types[0]) || !is_fixed_size_list_f32(&arg_types[1]) {
            return exec_err!(
                "{INNER_PRODUCT_UDF_NAME} requires both arguments to be FixedSizeList<Float32, N>"
            );
        }
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        if arg_types.len() != 2 {
            return exec_err!("{INNER_PRODUCT_UDF_NAME} expects exactly two arguments");
        }
        if matching_fixed_size_list_f32(&arg_types[0], &arg_types[1]).is_none() {
            return exec_err!(
                "{INNER_PRODUCT_UDF_NAME} requires both arguments to be FixedSizeList<Float32, N> with matching N, got {:?} and {:?}",
                arg_types[0],
                arg_types[1]
            );
        }
        Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        make_scalar_function(inner_product_inner)(&args.args)
    }
}

fn inner_product_inner(args: &[ArrayRef]) -> DataFusionResult<ArrayRef> {
    compute_fsl_f32(args, Kernel::Dot, |v| v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector_simd::testing::fsl_f32;
    use arrow::array::AsArray;
    use arrow::datatypes::Float64Type;

    #[test]
    fn basic_dot() {
        let a = fsl_f32(&[&[1.0, 2.0, 3.0]]);
        let b = fsl_f32(&[&[4.0, 5.0, 6.0]]);
        let result = inner_product_inner(&[a, b]).expect("ok");
        let result = result.as_primitive::<Float64Type>();
        assert!((result.value(0) - 32.0).abs() < 1e-5);
    }

    #[test]
    fn rejects_non_fsl_f32() {
        let udf = InnerProduct::new();
        let err = udf
            .coerce_types(&[DataType::Float32, DataType::Float32])
            .expect_err("should reject");
        assert!(err.to_string().contains("FixedSizeList"));
    }

    #[test]
    fn rejects_mismatched_dims() {
        let udf = InnerProduct::new();
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
