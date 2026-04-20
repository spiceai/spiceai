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

pub static ORG_UDF_NAME: &str = "current_org_id";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct OrgUdf {
    signature: Signature,
}

impl Default for OrgUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl OrgUdf {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for OrgUdf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        ORG_UDF_NAME
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
        // SAFETY: Same justification as UserUdf — always runs inside a RequestContext scope.
        let ctx = unsafe { RequestContext::current_sync() };
        let org_id = ctx
            .auth_principal()
            .and_then(|p| p.identity_context())
            .and_then(|ic| ic.org_id.clone());

        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(org_id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
    use std::sync::Arc;

    #[test]
    fn test_org_udf_returns_null_without_context() {
        let udf = OrgUdf::new();
        let args = ScalarFunctionArgs {
            args: vec![],
            number_rows: 1,
            arg_fields: vec![],
            return_field: Arc::new(Field::new("org", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let result = udf.invoke_with_args(args).expect("invoke UDF");
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("Expected Null Utf8 scalar, got {other:?}"),
        }
    }
}
