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

use tonic::async_trait;

use super::{ConnectorParams, Validator};

pub use runtime_parameters::gcs::{Error, validate_auth};

/// Validates GCS authentication configuration.
/// Ensures only one authentication method is used.
pub struct GcsAuthValidator;

#[async_trait]
impl Validator for GcsAuthValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        validate_auth(&params.parameters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConnectorComponent;
    use datafusion::common::TableReference;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use runtime_component::dataset::DatasetSpec;
    use runtime_parameters::{ParameterSpec, Parameters};
    use std::collections::HashMap;
    use tokio::runtime::Handle;

    const TEST_PARAMETERS: &[ParameterSpec] = &[
        ParameterSpec::component("service_account_path").secret(),
        ParameterSpec::component("service_account_key").secret(),
        ParameterSpec::component("skip_signature").is_boolean(),
        ParameterSpec::component("application_default_credentials").is_boolean(),
    ];

    fn mock_connector_component() -> ConnectorComponent {
        ConnectorComponent::from(&DatasetSpec::new(
            "gs://bucket/path",
            TableReference::bare("test_dataset"),
        ))
    }

    fn create_test_params(params: HashMap<String, String>) -> ConnectorParams {
        ConnectorParams {
            parameters: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "gcs",
                TEST_PARAMETERS,
            ),
            unsupported_type_action: None,
            component: mock_connector_component(),
            io_runtime: Handle::current(),
        }
    }

    #[tokio::test]
    async fn test_multiple_auth_methods() {
        let mut params = create_test_params(
            [
                (
                    "service_account_path".to_string(),
                    "/path/to/key.json".to_string(),
                ),
                ("skip_signature".to_string(), "true".to_string()),
            ]
            .into(),
        );
        let validator = GcsAuthValidator;
        assert!(matches!(
            validator.validate(&mut params).await,
            Err(Error::MultipleAuthMethods)
        ));
    }

    #[tokio::test]
    async fn test_single_auth_method_ok() {
        let mut params = create_test_params(
            [(
                "service_account_path".to_string(),
                "/path/to/key.json".to_string(),
            )]
            .into(),
        );
        let validator = GcsAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("single auth method should be valid");
    }

    #[tokio::test]
    async fn test_no_auth_method_ok() {
        let mut params = create_test_params(HashMap::new());
        let validator = GcsAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("no auth method should be valid");
    }
}
