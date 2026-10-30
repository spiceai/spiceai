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

//! Scalar kernels that evaluate integer `+` / `-` / `*` and error on overflow.

use std::sync::{Arc, LazyLock};

use arrow::array::ArrayRef;
use arrow::compute::kernels::numeric::{add, mul, sub};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::error::ArrowError;
use datafusion::common::{Result, exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};

use super::is_integer_arith_type;

/// Rewrite targets for integer `+` / `-` / `*`. Not a documented SQL API:
/// queries keep writing the operators. The `__spice_` prefix matches other
/// analyzer-only helpers (`__spice_json_get_is_null`).
pub const CHECKED_ADD_UDF_NAME: &str = "__spice_checked_add";
pub const CHECKED_SUB_UDF_NAME: &str = "__spice_checked_sub";
pub const CHECKED_MUL_UDF_NAME: &str = "__spice_checked_mul";

runtime_udfs_api::register_spice_function!(CHECKED_ADD_SPICE_FUNCTION, CHECKED_ADD_UDF_NAME);
runtime_udfs_api::register_spice_function!(CHECKED_SUB_SPICE_FUNCTION, CHECKED_SUB_UDF_NAME);
runtime_udfs_api::register_spice_function!(CHECKED_MUL_SPICE_FUNCTION, CHECKED_MUL_UDF_NAME);

#[must_use]
pub(crate) fn checked_add_udf() -> Arc<ScalarUDF> {
    static UDF: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(CheckedIntegerOp::add())));
    Arc::clone(&UDF)
}

#[must_use]
pub(crate) fn checked_sub_udf() -> Arc<ScalarUDF> {
    static UDF: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(CheckedIntegerOp::sub())));
    Arc::clone(&UDF)
}

#[must_use]
pub(crate) fn checked_mul_udf() -> Arc<ScalarUDF> {
    static UDF: LazyLock<Arc<ScalarUDF>> =
        LazyLock::new(|| Arc::new(ScalarUDF::from(CheckedIntegerOp::mul())));
    Arc::clone(&UDF)
}

#[must_use]
pub(crate) fn checked_op_udf(op: Operator) -> Option<Arc<ScalarUDF>> {
    match op {
        Operator::Plus => Some(checked_add_udf()),
        Operator::Minus => Some(checked_sub_udf()),
        Operator::Multiply => Some(checked_mul_udf()),
        _ => None,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CheckedIntegerOp {
    op: Operator,
    signature: Signature,
}

impl CheckedIntegerOp {
    fn add() -> Self {
        Self::new(Operator::Plus)
    }

    fn sub() -> Self {
        Self::new(Operator::Minus)
    }

    fn mul() -> Self {
        Self::new(Operator::Multiply)
    }

    fn new(op: Operator) -> Self {
        Self {
            op,
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }

    fn kernel(
        &self,
    ) -> fn(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> std::result::Result<ArrayRef, ArrowError> {
        if self.op == Operator::Minus {
            sub
        } else if self.op == Operator::Multiply {
            mul
        } else {
            add
        }
    }

    fn display_op(&self) -> &'static str {
        if self.op == Operator::Minus {
            "-"
        } else if self.op == Operator::Multiply {
            "*"
        } else {
            "+"
        }
    }
}

impl ScalarUDFImpl for CheckedIntegerOp {
    fn name(&self) -> &'static str {
        if self.op == Operator::Minus {
            CHECKED_SUB_UDF_NAME
        } else if self.op == Operator::Multiply {
            CHECKED_MUL_UDF_NAME
        } else {
            CHECKED_ADD_UDF_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let [data_type, _] = arg_types {
            Ok(data_type.clone())
        } else {
            plan_err!("{} expects two arguments", self.name())
        }
    }

    /// Nullable only when an operand is, as the `+` / `-` / `*` it replaces:
    /// overflow is an error, never a NULL. The default reports every result
    /// nullable, so a plan's schema changed with the rewrite and a client
    /// that compared it with the operator's schema saw a mismatch.
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [left, right] = args.arg_fields else {
            return plan_err!("{} expects two arguments", self.name());
        };
        Ok(Arc::new(Field::new(
            self.name(),
            left.data_type().clone(),
            left.is_nullable() || right.is_nullable(),
        )))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types {
            [left, right] if left == right && is_integer_arith_type(left) => {
                Ok(vec![left.clone(), right.clone()])
            }
            _ => plan_err!(
                "{} requires two arguments of the same integer type, got {arg_types:?}",
                self.name()
            ),
        }
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if let [left, right] = args {
            Ok(format!("{left} {} {right}", self.display_op()))
        } else {
            Ok(self.name().to_string())
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let [left, right] = arrays.as_slice() else {
            return exec_err!("{} expects two arguments", self.name());
        };
        let result = (self.kernel())(left, right).map_err(|error| {
            datafusion::error::DataFusionError::Execution(format!(
                "Overflow in integer {}: {error}",
                self.display_op()
            ))
        })?;
        Ok(ColumnarValue::Array(result))
    }
}
