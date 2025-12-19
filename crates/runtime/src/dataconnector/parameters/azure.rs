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

use snafu::prelude::*;
use tonic::async_trait;

use crate::parameters::ParamLookup;

use super::{ConnectorParams, Validator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The 'abfs_endpoint' parameter must be a HTTP/S URL, but '{endpoint}' was provided. Specify a valid HTTP/S URL. For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"
    ))]
    InvalidEndpoint { endpoint: String },

    #[snafu(display(
        "The '{endpoint}' is a HTTP URL, but 'allow_http' is not enabled. Set the parameter 'allow_http: true' and retry. For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"
    ))]
    InsecureEndpointWithoutAllowHTTP { endpoint: String },

    #[snafu(display(
        "The 'abfs_account' parameter is required for Azure Blob Storage. Specify the storage account name with the 'abfs_account' parameter. For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#params"
    ))]
    MissingAccount,

    #[snafu(display(
        "Multiple authentication methods were provided. Specify only one of the following: access_key, bearer_token, sas_string, client credentials (client_id + client_secret + tenant_id), use_cli, msi_endpoint, or skip_signature. For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#auth"
    ))]
    MultipleAuthMethods,

    #[snafu(display(
        "Incomplete client credentials. When using client credentials authentication, all three parameters are required: 'client_id', 'client_secret', and 'tenant_id'. For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#auth"
    ))]
    IncompleteClientCredentials,
}

/// Validates and normalizes Azure endpoint configuration.
pub(crate) struct AzureEndpointValidator;

#[async_trait]
impl Validator for AzureEndpointValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        if let Some(endpoint) = params.parameters.get("endpoint").expose().ok() {
            let endpoint = endpoint.to_string();

            // Trim trailing slash for consistency
            if endpoint.ends_with('/') {
                tracing::warn!("Trimming trailing '/' from Azure endpoint {endpoint}");
                params.parameters.insert(
                    "endpoint".to_string(),
                    endpoint.trim_end_matches('/').to_string().into(),
                );
            }

            // Validate endpoint is a valid HTTP/S URL
            if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                return Err(Error::InvalidEndpoint { endpoint });
            }

            // Check HTTP requires allow_http
            if endpoint.starts_with("http://")
                && params.parameters.get("allow_http").expose().ok() != Some("true")
            {
                return Err(Error::InsecureEndpointWithoutAllowHTTP { endpoint });
            }
        }
        Ok(())
    }
}

/// Validates required Azure account parameter.
pub(crate) struct AzureAccountValidator;

#[async_trait]
impl Validator for AzureAccountValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        // Account is required unless using emulator
        let use_emulator = params
            .parameters
            .get("use_emulator")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        if !use_emulator && matches!(params.parameters.get("account"), ParamLookup::Absent(_)) {
            return Err(Error::MissingAccount);
        }
        Ok(())
    }
}

/// Validates Azure authentication configuration.
/// Ensures only one authentication method is used and validates completeness.
pub(crate) struct AzureAuthValidator;

#[async_trait]
impl Validator for AzureAuthValidator {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        // Skip validation for emulator mode
        let use_emulator = params
            .parameters
            .get("use_emulator")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        if use_emulator {
            return Ok(());
        }

        // Check for each authentication method
        let has_access_key = params.parameters.get("access_key").expose().ok().is_some();
        let has_bearer_token = params
            .parameters
            .get("bearer_token")
            .expose()
            .ok()
            .is_some();
        let has_sas_string = params.parameters.get("sas_string").expose().ok().is_some();

        // skip_signature must be explicitly "true" to count as an auth method
        let has_skip_signature = params
            .parameters
            .get("skip_signature")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        // Check client credentials (all three must be present together)
        let has_client_id = params.parameters.get("client_id").expose().ok().is_some();
        let has_client_secret = params
            .parameters
            .get("client_secret")
            .expose()
            .ok()
            .is_some();
        let has_tenant_id = params.parameters.get("tenant_id").expose().ok().is_some();
        let has_any_client_cred = has_client_id || has_client_secret || has_tenant_id;
        let has_all_client_creds = has_client_id && has_client_secret && has_tenant_id;

        // Validate client credentials completeness
        if has_any_client_cred && !has_all_client_creds {
            return Err(Error::IncompleteClientCredentials);
        }

        // use_cli must be explicitly "true" to count as an auth method
        let has_use_cli = params
            .parameters
            .get("use_cli")
            .expose()
            .ok()
            .is_some_and(|v| v.eq_ignore_ascii_case("true"));

        let has_msi_endpoint = params
            .parameters
            .get("msi_endpoint")
            .expose()
            .ok()
            .is_some();
        let has_federated_token = params
            .parameters
            .get("federated_token_file")
            .expose()
            .ok()
            .is_some();

        // Count active authentication methods
        let auth_method_count = [
            has_access_key,
            has_bearer_token,
            has_sas_string,
            has_skip_signature,
            has_all_client_creds,
            has_use_cli,
            has_msi_endpoint,
            has_federated_token,
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

/// Normalizes SAS token by stripping leading '?' if present.
pub(crate) struct AzureSasTokenNormalizer;

#[async_trait]
impl Validator for AzureSasTokenNormalizer {
    type Error = Error;

    async fn validate(&self, params: &mut ConnectorParams) -> Result<(), Error> {
        if let Some(sas_token) = params.parameters.get("sas_string").expose().ok()
            && let Some(sas_token) = sas_token.strip_prefix('?')
        {
            params
                .parameters
                .insert("sas_string".to_string(), sas_token.to_string().into());
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
        ParameterSpec::component("account").secret(),
        ParameterSpec::component("container_name").secret(),
        ParameterSpec::component("access_key").secret(),
        ParameterSpec::component("bearer_token").secret(),
        ParameterSpec::component("client_id").secret(),
        ParameterSpec::component("client_secret").secret(),
        ParameterSpec::component("tenant_id").secret(),
        ParameterSpec::component("sas_string").secret(),
        ParameterSpec::component("endpoint").secret(),
        ParameterSpec::component("use_emulator").is_boolean(),
        ParameterSpec::component("skip_signature").is_boolean(),
        ParameterSpec::component("use_cli").is_boolean(),
        ParameterSpec::component("msi_endpoint").secret(),
        ParameterSpec::component("federated_token_file"),
        ParameterSpec::runtime("allow_http").is_boolean(),
    ];

    async fn create_mock_connector_component() -> ConnectorComponent {
        let app = AppBuilder::new("test").build();
        let spice_runtime = RuntimeBuilder::new().build().await;

        let dataset = DatasetBuilder::try_new("abfs://container/path".to_string(), "test_dataset")
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
                "abfs",
                TEST_PARAMETERS,
            ),
            unsupported_type_action: None,
            component: create_mock_connector_component().await,
            app: None,
            runtime: None,
            io_runtime: Handle::current(),
        }
    }

    // AzureEndpointValidator tests
    #[tokio::test]
    async fn test_endpoint_validator_valid_https() {
        let mut params = create_test_params(
            [(
                "endpoint".to_string(),
                "https://example.blob.core.windows.net".to_string(),
            )]
            .into(),
        )
        .await;
        let validator = AzureEndpointValidator;
        validator
            .validate(&mut params)
            .await
            .expect("valid HTTPS endpoint should pass validation");
    }

    #[tokio::test]
    async fn test_endpoint_validator_http_without_allow_http() {
        let mut params = create_test_params(
            [("endpoint".to_string(), "http://localhost:10000".to_string())].into(),
        )
        .await;
        let validator = AzureEndpointValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(
            result,
            Err(Error::InsecureEndpointWithoutAllowHTTP { .. })
        ));
    }

    #[tokio::test]
    async fn test_endpoint_validator_http_with_allow_http() {
        let mut params = create_test_params(
            [
                ("endpoint".to_string(), "http://localhost:10000".to_string()),
                ("allow_http".to_string(), "true".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureEndpointValidator;
        validator
            .validate(&mut params)
            .await
            .expect("HTTP endpoint with allow_http=true should pass validation");
    }

    #[tokio::test]
    async fn test_endpoint_validator_invalid_protocol() {
        let mut params =
            create_test_params([("endpoint".to_string(), "ftp://example.com".to_string())].into())
                .await;
        let validator = AzureEndpointValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(result, Err(Error::InvalidEndpoint { .. })));
    }

    #[tokio::test]
    async fn test_endpoint_validator_trims_trailing_slash() {
        let mut params = create_test_params(
            [(
                "endpoint".to_string(),
                "https://example.blob.core.windows.net/".to_string(),
            )]
            .into(),
        )
        .await;
        let validator = AzureEndpointValidator;
        validator
            .validate(&mut params)
            .await
            .expect("endpoint with trailing slash should pass validation");
        assert_eq!(
            params.parameters.get("endpoint").expose().ok(),
            Some("https://example.blob.core.windows.net")
        );
    }

    // AzureAccountValidator tests
    #[tokio::test]
    async fn test_account_validator_missing_account() {
        let mut params = create_test_params(HashMap::new()).await;
        let validator = AzureAccountValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(result, Err(Error::MissingAccount)));
    }

    #[tokio::test]
    async fn test_account_validator_with_account() {
        let mut params =
            create_test_params([("account".to_string(), "mystorageaccount".to_string())].into())
                .await;
        let validator = AzureAccountValidator;
        validator
            .validate(&mut params)
            .await
            .expect("account parameter present should pass validation");
    }

    #[tokio::test]
    async fn test_account_validator_emulator_mode_no_account() {
        let mut params =
            create_test_params([("use_emulator".to_string(), "true".to_string())].into()).await;
        let validator = AzureAccountValidator;
        validator
            .validate(&mut params)
            .await
            .expect("emulator mode should not require account");
    }

    // AzureAuthValidator tests
    #[tokio::test]
    async fn test_auth_validator_single_access_key() {
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("access_key".to_string(), "myaccesskey".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("single access_key auth should pass validation");
    }

    #[tokio::test]
    async fn test_auth_validator_multiple_auth_methods() {
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("access_key".to_string(), "myaccesskey".to_string()),
                ("sas_string".to_string(), "sv=2020-08-04".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(result, Err(Error::MultipleAuthMethods)));
    }

    #[tokio::test]
    async fn test_auth_validator_incomplete_client_credentials() {
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("client_id".to_string(), "myclientid".to_string()),
                ("client_secret".to_string(), "myclientsecret".to_string()),
                // Missing tenant_id
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(result, Err(Error::IncompleteClientCredentials)));
    }

    #[tokio::test]
    async fn test_auth_validator_complete_client_credentials() {
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("client_id".to_string(), "myclientid".to_string()),
                ("client_secret".to_string(), "myclientsecret".to_string()),
                ("tenant_id".to_string(), "mytenantid".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("complete client credentials should pass validation");
    }

    #[tokio::test]
    async fn test_auth_validator_skip_signature_false_not_conflict() {
        // skip_signature: false should NOT count as an auth method
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("access_key".to_string(), "myaccesskey".to_string()),
                ("skip_signature".to_string(), "false".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("skip_signature=false should not conflict with access_key");
    }

    #[tokio::test]
    async fn test_auth_validator_skip_signature_true_conflicts() {
        // skip_signature: true SHOULD conflict with access_key
        let mut params = create_test_params(
            [
                ("account".to_string(), "myaccount".to_string()),
                ("access_key".to_string(), "myaccesskey".to_string()),
                ("skip_signature".to_string(), "true".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        let result = validator.validate(&mut params).await;
        assert!(matches!(result, Err(Error::MultipleAuthMethods)));
    }

    #[tokio::test]
    async fn test_auth_validator_emulator_skips_validation() {
        // In emulator mode, auth validation is skipped
        let mut params = create_test_params(
            [
                ("use_emulator".to_string(), "true".to_string()),
                ("access_key".to_string(), "key1".to_string()),
                ("sas_string".to_string(), "sas1".to_string()),
            ]
            .into(),
        )
        .await;
        let validator = AzureAuthValidator;
        validator
            .validate(&mut params)
            .await
            .expect("emulator mode should skip auth validation");
    }

    // AzureSasTokenNormalizer tests
    #[tokio::test]
    async fn test_sas_normalizer_strips_question_mark() {
        let mut params = create_test_params(
            [(
                "sas_string".to_string(),
                "?sv=2020-08-04&sig=abc".to_string(),
            )]
            .into(),
        )
        .await;
        let validator = AzureSasTokenNormalizer;
        validator
            .validate(&mut params)
            .await
            .expect("SAS token with leading ? should be normalized");
        assert_eq!(
            params.parameters.get("sas_string").expose().ok(),
            Some("sv=2020-08-04&sig=abc")
        );
    }

    #[tokio::test]
    async fn test_sas_normalizer_no_question_mark() {
        let mut params = create_test_params(
            [(
                "sas_string".to_string(),
                "sv=2020-08-04&sig=abc".to_string(),
            )]
            .into(),
        )
        .await;
        let validator = AzureSasTokenNormalizer;
        validator
            .validate(&mut params)
            .await
            .expect("SAS token without leading ? should pass unchanged");
        assert_eq!(
            params.parameters.get("sas_string").expose().ok(),
            Some("sv=2020-08-04&sig=abc")
        );
    }
}
