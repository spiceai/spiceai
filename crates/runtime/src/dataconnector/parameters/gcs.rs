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

use snafu::prelude::*;
use tonic::async_trait;

use super::{ConnectorParams, Validator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Multiple authentication methods were provided. Specify only one of the following: gcs_service_account_path, gcs_service_account_key, gcs_application_default_credentials, or gcs_skip_signature. For details, visit: https://spiceai.org/docs/components/data-connectors/gcs#auth"
    ))]
    MultipleAuthMethods,
}

/// Validates GCS authentication configuration.
/// Ensures only one authentication method is used.
pub(crate) struct GcsAuthValidator;

#[async_trait]
impl Validator for GcsAuthValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        // Check for each authentication method
        let has_service_account_path = params
            .parameters
            .get("service_account_path")
            .expose()
            .ok()
            .is_some();
        let has_service_account_key = params
            .parameters
            .get("service_account_key")
            .expose()
            .ok()
            .is_some();

        // skip_signature must be explicitly "true" to count as an auth method
        let has_skip_signature = params
            .parameters
            .get("skip_signature")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // application_default_credentials must be explicitly "true" to count as an auth method
        let has_application_default_credentials = params
            .parameters
            .get("application_default_credentials")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Count active authentication methods
        let auth_method_count = [
            has_service_account_path,
            has_service_account_key,
            has_skip_signature,
            has_application_default_credentials,
        ]
        .iter()
        .filter(|&&b| b)
        .count();

        if auth_method_count > 1 {
            return Err(Error::MultipleAuthMethods);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::RuntimeBuilder;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::ConnectorComponent;
    use crate::parameters::{ParameterSpec, Parameters};
    use app::AppBuilder;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::runtime::Handle;

    const TEST_PARAMETERS: &[ParameterSpec] = &[
        ParameterSpec::component("service_account_path").secret(),
        ParameterSpec::component("service_account_key").secret(),
        ParameterSpec::component("skip_signature").is_boolean(),
        ParameterSpec::component("application_default_credentials").is_boolean(),
    ];

    async fn create_mock_connector_component() -> ConnectorComponent {
        let app = AppBuilder::new("test").build();
        let spice_runtime = RuntimeBuilder::new().build().await;

        let dataset = DatasetBuilder::try_new("gs://bucket/path".to_string(), "test_dataset")
            .expect("to create dataset builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(spice_runtime))
            .build()
            .expect("to create dataset");
        ConnectorComponent::from(&dataset)
    }

    async fn create_test_params(params: HashMap<String, String>) -> ConnectorParams {
        ConnectorParams {
            parameters: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "gcs",
                TEST_PARAMETERS,
            ),
            unsupported_type_action: None,
            component: create_mock_connector_component().await,
            app: None,
            runtime: None,
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
        )
        .await;
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
        )
        .await;
        let validator = GcsAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("single auth method should be valid");
    }

    #[tokio::test]
    async fn test_no_auth_method_ok() {
        let mut params = create_test_params(HashMap::new()).await;
        let validator = GcsAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("no auth method should be valid");
    }
}
