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

//! `assert(<bool>)` scalar UDF — the gate primitive for conditional-commit transactions.
//!
//! Returns its boolean argument unchanged when it is `TRUE`; returns an error when it is
//! `FALSE` or `NULL`. The error is a [`DataFusionError::Execution`] whose message begins with
//! the marker [`ASSERT_FAILED_MARKER`] so a transaction executor can distinguish a gate abort
//! (terminal — return to the client) from a serialization conflict (retryable).
//!
//! The UDF is declared [`Volatility::Volatile`] so the optimizer never const-folds or elides it:
//! `assert(1 > 2)` must fail at *execution* time (as an execution error), not be pre-evaluated at
//! planning time.

use arrow::array::{Array, BooleanArray};
use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

static ASSERT_SCALAR_UDF_NAME: &str = "assert";

/// Prefix on the error message of every `assert()` failure, so callers can recognize a gate
/// abort without relying on the (shared) `ErrorCode`.
static ASSERT_FAILED_MARKER: &str = "assertion failed:";

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Assert {
    signature: Signature,
}

impl Default for Assert {
    fn default() -> Self {
        Self::new()
    }
}

impl Assert {
    #[must_use]
    pub fn new() -> Self {
        Self {
            // Volatile => never const-folded/elided; evaluated at execution time.
            signature: Signature::exact(vec![DataType::Boolean], Volatility::Volatile),
        }
    }

    fn failed() -> DataFusionError {
        DataFusionError::Execution(format!(
            "{ASSERT_FAILED_MARKER} gate expression was false or NULL"
        ))
    }
}

impl ScalarUDFImpl for Assert {
    fn name(&self) -> &'static str {
        ASSERT_SCALAR_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        let args: [ColumnarValue; 1] = args.args.try_into().map_err(|args: Vec<_>| {
            DataFusionError::Execution(format!(
                "{ASSERT_FAILED_MARKER} assert() requires exactly one boolean argument, got {}",
                args.len()
            ))
        })?;
        let [arg] = args;

        match arg {
            // Scalar TRUE -> pass through.
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            }
            // Scalar FALSE or NULL -> abort.
            ColumnarValue::Scalar(ScalarValue::Boolean(_)) => Err(Self::failed()),
            // Array: abort on any FALSE or NULL, otherwise pass through.
            ColumnarValue::Array(array) => {
                let bools = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{ASSERT_FAILED_MARKER} assert() argument must be Boolean"
                        ))
                    })?;
                for i in 0..bools.len() {
                    if bools.is_null(i) || !bools.value(i) {
                        return Err(Self::failed());
                    }
                }
                Ok(ColumnarValue::Array(array))
            }
            other @ ColumnarValue::Scalar(_) => Err(DataFusionError::Execution(format!(
                "{ASSERT_FAILED_MARKER} assert() argument must be Boolean, got {}",
                other.data_type()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::ScalarFunctionArgs;

    fn call_args(args: Vec<ColumnarValue>) -> Result<ColumnarValue, DataFusionError> {
        Assert::new().invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: 1,
            return_field: std::sync::Arc::new(arrow::datatypes::Field::new(
                "r",
                DataType::Boolean,
                true,
            )),
            config_options: std::sync::Arc::new(ConfigOptions::new()),
        })
    }

    fn call(arg: ColumnarValue) -> Result<ColumnarValue, DataFusionError> {
        call_args(vec![arg])
    }

    #[test]
    fn assert_true_passes() {
        let _ = call(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))))
            .expect("TRUE should pass the assertion");
    }

    #[test]
    fn assert_false_aborts() {
        let e = call(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))))
            .expect_err("FALSE should fail the assertion");
        assert!(e.to_string().contains(ASSERT_FAILED_MARKER));
    }

    #[test]
    fn assert_null_aborts() {
        let e = call(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
            .expect_err("NULL should fail the assertion");
        assert!(e.to_string().contains(ASSERT_FAILED_MARKER));
    }

    #[test]
    fn assert_array_any_false_aborts() {
        let arr = std::sync::Arc::new(BooleanArray::from(vec![Some(true), Some(false)]));
        let e = call(ColumnarValue::Array(arr))
            .expect_err("an array containing FALSE should fail the assertion");
        assert!(e.to_string().contains(ASSERT_FAILED_MARKER));
    }

    #[test]
    fn assert_rejects_wrong_argument_count() {
        for args in [
            vec![],
            vec![
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            ],
        ] {
            let e = call_args(args).expect_err("wrong argument count should fail");
            assert!(e.to_string().contains("requires exactly one"));
        }
    }
}
