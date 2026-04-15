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
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use runtime_auth::AuthRequestContext as _;
use runtime_auth::identity::claim_value_to_string;
use runtime_request_context::RequestContext;

pub static SESSION_PROPERTY_UDF_NAME: &str = "session_property";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SessionPropertyUdf {
    signature: Signature,
}

impl Default for SessionPropertyUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionPropertyUdf {
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

impl ScalarUDFImpl for SessionPropertyUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        SESSION_PROPERTY_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue, DataFusionError> {
        if args.args.len() != 1 {
            return Err(DataFusionError::Plan(
                "session_property requires exactly one argument".to_string(),
            ));
        }

        let key = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
            }
            _ => {
                return Err(DataFusionError::Plan(
                    "session_property argument must be a string literal".to_string(),
                ));
            }
        };

        // SAFETY: Same justification as UserUdf — always runs inside a RequestContext scope.
        let ctx = unsafe { RequestContext::current_sync() };
        let value = ctx
            .auth_principal()
            .and_then(|p| p.identity_context())
            .and_then(|ic| ic.claims.get(&key))
            .map(claim_value_to_string);

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_session_property_udf_returns_null_without_context() {
        let udf = SessionPropertyUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                "email".to_string(),
            )))],
            number_rows: 1,
            arg_fields: vec![Arc::new(Field::new("key", DataType::Utf8, false))],
            return_field: Arc::new(Field::new("session_property", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Expected Null Utf8 scalar, got {other:?}"),
        }
    }

    #[test]
    fn test_session_property_udf_null_key_returns_null() {
        let udf = SessionPropertyUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))],
            number_rows: 1,
            arg_fields: vec![Arc::new(Field::new("key", DataType::Utf8, true))],
            return_field: Arc::new(Field::new("session_property", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Expected Null Utf8 scalar, got {other:?}"),
        }
    }
}
