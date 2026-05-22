/*
Copyright 2026 The Spice.ai OSS Authors

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

use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use runtime_auth::AuthRequestContext as _;
use runtime_request_context::RequestContext;

pub static CURRENT_USER_HAS_ROLE_UDF_NAME: &str = "current_user_has_role";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CurrentUserHasRoleUdf {
    signature: Signature,
}

impl Default for CurrentUserHasRoleUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl CurrentUserHasRoleUdf {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8]),
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for CurrentUserHasRoleUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        CURRENT_USER_HAS_ROLE_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Plan(
                "current_user_has_role requires exactly one argument".to_string(),
            ));
        }

        let role = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(role))) => role,
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))));
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "current_user_has_role argument must be a string literal".to_string(),
                ));
            }
        };

        // SAFETY: Same justification as UserUdf — always runs inside a RequestContext scope.
        let ctx = unsafe { RequestContext::current_sync() };
        let has_role = ctx.auth_principal().is_some_and(|principal| {
            principal.identity_context().map_or_else(
                || principal.groups().iter().any(|group| *group == role),
                |identity| identity.roles.iter().any(|candidate| candidate == role),
            )
        });

        Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(has_role))))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_current_user_has_role_returns_false_without_context() {
        let udf = CurrentUserHasRoleUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "admin".to_string(),
            )))],
            number_rows: 1,
            arg_fields: vec![Arc::new(Field::new("role", DataType::Utf8, false))],
            return_field: Arc::new(Field::new("has_role", DataType::Boolean, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        match result {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {}
            other => panic!("Expected false Boolean scalar, got {other:?}"),
        }
    }
}
