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

use arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use runtime_auth::AuthRequestContext as _;
use runtime_request_context::RequestContext;

pub static USER_UDF_NAME: &str = "current_user_id";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct UserUdf {
    signature: Signature,
}

impl Default for UserUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl UserUdf {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for UserUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        USER_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        // SAFETY: This UDF is called within DataFusion query execution, which always
        // runs inside a RequestContext scope (set by track_metrics for HTTP, or
        // RequestContextMiddleware for Flight). The fallback returns the internal
        // context with no auth principal, resulting in "anonymous".
        let ctx = unsafe { RequestContext::current_sync() };
        let user_id = ctx.auth_principal().map_or_else(
            || "anonymous".to_string(),
            |p| {
                // Prefer IdentityContext.user_id when available (rich identity from OIDC).
                // Fall back to username() for backward compatibility (API key auth).
                p.identity_context()
                    .map_or_else(|| p.username().to_string(), |ic| ic.user_id.clone())
            },
        );

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(user_id))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_user_udf_returns_anonymous_without_context() {
        let udf = UserUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("user", DataType::Utf8, false)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(username))) => {
                assert_eq!(username, "anonymous");
            }
            other => panic!("Expected Utf8 scalar, got {other:?}"),
        }
    }
}
