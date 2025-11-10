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

//! [`ScalarUDFImpl`] definitions for `CLIENT_ID` function for Row-Level Security.

use std::sync::Arc;

use app::spicepod::component::runtime::ApiKey;
use arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use runtime_auth::AuthRequestContext;
use runtime_request_context::context::RequestContext;

pub static CLIENT_ID_UDF_NAME: &str = "client_id";

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct ClientId {
    signature: Signature,
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientId {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![], Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for ClientId {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        CLIENT_ID_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        // SAFETY: This is called during query execution, which is always in an async context
        // where RequestContext is available. Using unsafe here is acceptable because:
        // 1. DataFusion query execution is always within an async runtime
        // 2. RequestContext is set up at the query entry point
        // 3. If no context exists, we return None which is the correct behavior
        let ctx = unsafe { RequestContext::current_sync() };

        let client_id = ctx.auth_principal().and_then(|principal| {
            principal
                .as_any()
                .downcast_ref::<ApiKey>()
                .map(|api_key| match api_key {
                    ApiKey::ReadOnly { key } | ApiKey::ReadWrite { key } => key.clone(),
                })
        });

        let array: ArrayRef = match client_id {
            Some(id) => Arc::new(StringArray::from(vec![id])),
            None => Arc::new(StringArray::from(vec![Option::<String>::None])),
        };

        Ok(ColumnarValue::Array(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::spicepod::component::runtime::ApiKey;
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::config::ConfigOptions;
    use datafusion::logical_expr::ScalarFunctionArgs;
    use runtime_request_context::context::RequestContext;
    use runtime_request_context::protocol::Protocol;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_client_id_with_api_key() {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());

        let api_key = Arc::new(ApiKey::ReadOnly {
            key: "test-key-123".to_string(),
        });

        ctx.set_auth_principal(api_key).ok();

        let result = ctx
            .scope(async {
                let client_id_udf = ClientId::new();
                let args = ScalarFunctionArgs {
                    args: vec![],
                    arg_fields: vec![],
                    return_field: Arc::new(Field::new("client_id", DataType::Utf8, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                    number_rows: 1,
                };

                client_id_udf.invoke_with_args(args)
            })
            .await;

        let result = result.expect("Failed to invoke CLIENT_ID");

        match result {
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");

                assert_eq!(string_array.len(), 1);
                assert_eq!(string_array.value(0), "test-key-123");
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[tokio::test]
    async fn test_client_id_without_auth() {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());

        let result = ctx
            .scope(async {
                let client_id_udf = ClientId::new();
                let args = ScalarFunctionArgs {
                    args: vec![],
                    arg_fields: vec![],
                    return_field: Arc::new(Field::new("client_id", DataType::Utf8, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                    number_rows: 1,
                };

                client_id_udf.invoke_with_args(args)
            })
            .await;

        let result = result.expect("Failed to invoke CLIENT_ID");

        match result {
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");

                assert_eq!(string_array.len(), 1);
                assert!(string_array.is_null(0));
            }
            _ => panic!("Expected Array result"),
        }
    }

    #[tokio::test]
    async fn test_client_id_with_readwrite_key() {
        let ctx = Arc::new(RequestContext::builder(Protocol::Http).build());

        let api_key = Arc::new(ApiKey::ReadWrite {
            key: "rw-key-456".to_string(),
        });

        ctx.set_auth_principal(api_key).ok();

        let result = ctx
            .scope(async {
                let client_id_udf = ClientId::new();
                let args = ScalarFunctionArgs {
                    args: vec![],
                    arg_fields: vec![],
                    return_field: Arc::new(Field::new("client_id", DataType::Utf8, true)),
                    config_options: Arc::new(ConfigOptions::default()),
                    number_rows: 1,
                };

                client_id_udf.invoke_with_args(args)
            })
            .await;

        let result = result.expect("Failed to invoke CLIENT_ID");

        match result {
            ColumnarValue::Array(array) => {
                let string_array = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Expected StringArray");

                assert_eq!(string_array.len(), 1);
                assert_eq!(string_array.value(0), "rw-key-456");
            }
            _ => panic!("Expected Array result"),
        }
    }
}
