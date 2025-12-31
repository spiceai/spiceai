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

use super::listing::{ListingTableConnector, build_fragments};
use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, ParameterSpec, Parameters,
    parameters::{
        Validator,
        azure::{
            AzureAccountValidator, AzureAuthValidator, AzureEndpointValidator,
            AzureSasTokenNormalizer,
        },
    },
};

use crate::{
    Runtime, component::dataset::Dataset, dataconnector::listing::LISTING_TABLE_PARAMETERS,
    register_data_connector,
};
use datafusion::parquet::arrow::async_reader::ObjectVersionType;
use snafu::prelude::*;
use std::any::Any;
use std::clone::Clone;
use std::future::Future;
use std::pin::Pin;
use std::string::String;
use std::sync::{Arc, LazyLock};
use tokio::runtime::Handle;
use url::Url;

static PREFIX: &str = "abfs";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = super::parameters::azure::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| {
    vec![
        Box::new(AzureSasTokenNormalizer),
        Box::new(AzureEndpointValidator),
        Box::new(AzureAccountValidator),
        Box::new(AzureAuthValidator),
    ]
});

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "The specified URL is not valid: {url}. Ensure the URL is valid and try again. {source}"
    ))]
    UnableToParseURL {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Azure managed identity authentication failed. Are you sure you're running in an environment with a managed identity? {source} For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#auth"
    ))]
    ManagedIdentityAuthenticationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Azure client credentials authentication failed. Verify your client_id, client_secret, and tenant_id are correct. {source} For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#auth"
    ))]
    ClientCredentialsAuthenticationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Azure SAS token authentication failed. Verify your sas_string is valid and not expired. {source} For details, visit: https://spiceai.org/docs/components/data-connectors/abfs#auth"
    ))]
    SasAuthenticationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct AzureBlobFS {
    params: Parameters,
    runtime: Option<Runtime>,
    tokio_io_runtime: Handle,
}

impl std::fmt::Debug for AzureBlobFS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AzureBlobFS(params: {:?})", self.params)
    }
}

#[derive(Default, Clone)]
pub struct AzureBlobFSFactory {}

impl AzureBlobFSFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

static PARAMETERS: LazyLock<Vec<ParameterSpec>> = LazyLock::new(|| {
    let mut all_parameters = Vec::new();
    all_parameters.extend_from_slice(&[
        ParameterSpec::component("account")
            .description("Azure Storage account name.")
            .secret(),
        ParameterSpec::component("container_name")
            .description("Azure Storage container name.")
            .secret(),
        ParameterSpec::component("access_key")
            .description("Azure Storage account access key.")
            .secret(),
        ParameterSpec::component("bearer_token")
            .description("Bearer token to use in Azure requests.")
            .secret(),
        ParameterSpec::component("client_id")
            .description("Azure client ID.")
            .secret(),
        ParameterSpec::component("client_secret")
            .description("Azure client secret.")
            .secret(),
        ParameterSpec::component("tenant_id")
            .description("Azure tenant ID.")
            .secret(),
        ParameterSpec::component("sas_string")
            .description("Azure SAS string.")
            .secret(),
        ParameterSpec::component("endpoint")
            .description("Azure Storage endpoint.")
            .secret(),
        ParameterSpec::component("use_emulator")
            .description("Use the Azure Storage emulator.")
            .is_boolean()
            .default("false"),
        ParameterSpec::component("use_fabric_endpoint")
            .description("Use the Azure Storage fabric endpoint.")
            .is_boolean()
            .default("false"),
        ParameterSpec::runtime("allow_http")
            .description("Allow insecure HTTP connections.")
            .is_boolean()
            .default("false"),
        ParameterSpec::component("authority_host")
            .description("Sets an alternative authority host."),
        ParameterSpec::component("max_retries")
            .description("The maximum number of retries.")
            .default("3"),
        ParameterSpec::component("retry_timeout")
            .description("Retry timeout."),
        ParameterSpec::component("backoff_initial_duration")
            .description("Initial backoff duration."),
        ParameterSpec::component("backoff_max_duration")
            .description("Maximum backoff duration."),
        ParameterSpec::component("backoff_base")
            .description("The base of the exponential to use"),
        ParameterSpec::component("proxy_url")
            .description("Proxy URL to use when connecting"),
        ParameterSpec::component("proxy_ca_certificate")
            .description("CA certificate for the proxy.")
            .secret(),
        ParameterSpec::component("proxy_excludes")
            .description("Set list of hosts to exclude from proxy connections"),
        ParameterSpec::component("msi_endpoint")
            .description("Sets the endpoint for acquiring managed identity tokens.")
            .secret(),
        ParameterSpec::component("federated_token_file")
            .description("Sets a file path for acquiring Azure federated identity token in Kubernetes"),
        ParameterSpec::component("use_cli")
            .is_boolean()
            .description("Set if the Azure CLI should be used for acquiring access tokens."),
        ParameterSpec::component("skip_signature")
            .description("Skip fetching credentials and skip signing requests. Used for interacting with public containers.")
            .is_boolean(),
        ParameterSpec::component("disable_tagging")
            .description("Ignore any tags provided to put_opts")
            .is_boolean(),
        ParameterSpec::runtime("client_timeout")
            .description("The timeout setting for Azure client."),
        ParameterSpec::component("versioning")
            .description("Enables Azure blob versioning support when set to 'enabled'. Defaults to 'disabled'.")
            .default("disabled"),
    ]);
    all_parameters.extend_from_slice(LISTING_TABLE_PARAMETERS);
    all_parameters
});

impl DataConnectorFactory for AzureBlobFSFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        mut params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        // Validate versioning parameter early
        if let Some(versioning) = params.parameters.get("versioning").expose().ok()
            && !matches!(versioning, "enabled" | "disabled")
        {
            tracing::warn!(
                "Invalid Azure versioning setting '{versioning}'. Defaulting to 'disabled'."
            );
            params
                .parameters
                .insert("versioning".to_string(), "disabled".to_string().into());
        }

        Box::pin(async move {
            // Run all validators
            for validator in VALIDATORS.iter() {
                validator.validate(&mut params).await?;
            }

            let azure = AzureBlobFS {
                params: params.parameters,
                runtime: params.runtime.map(Arc::unwrap_or_clone),
                tokio_io_runtime: params.io_runtime,
            };
            Ok(Arc::new(azure) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        PREFIX
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &PARAMETERS
    }
}

impl std::fmt::Display for AzureBlobFS {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{PREFIX}")
    }
}

impl ListingTableConnector for AzureBlobFS {
    fn object_versioning_type(&self) -> Option<ObjectVersionType> {
        if self.params.get("versioning").expose().ok() != Some("enabled") {
            return None;
        }

        Some(ObjectVersionType::Version)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> Handle {
        self.tokio_io_runtime.clone()
    }

    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        url: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let url = url.unwrap_or(dataset.from.as_str());

        let mut azure_url =
            Url::parse(url)
                .boxed()
                .context(super::InvalidConfigurationSnafu {
                    dataconnector: format!("{self}"),
                    message: format!("The specified URL is not valid: {url}. Ensure the URL is valid and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/{PREFIX}#from"),
                    connector_component: ConnectorComponent::from(dataset)
                })?;

        let params = build_fragments(
            &self.params,
            vec![
                "account",
                "container_name",
                "access_key",
                "bearer_token",
                "client_id",
                "client_secret",
                "tenant_id",
                "sas_string",
                "endpoint",
                "use_emulator",
                "use_fabric_endpoint",
                "allow_http",
                "authority_host",
                "max_retries",
                "retry_timeout",
                "backoff_initial_duration",
                "backoff_max_duration",
                "backoff_base",
                "proxy_url",
                "proxy_ca_certificate",
                "proxy_excludes",
                "msi_endpoint",
                "federated_token_file",
                "use_cli",
                "skip_signature",
                "disable_tagging",
                "client_timeout",
            ],
        );
        azure_url.set_fragment(Some(&params));
        Ok(azure_url)
    }

    fn get_runtime(&self) -> Option<Runtime> {
        self.runtime.clone()
    }

    fn handle_object_store_error(
        &self,
        dataset: &Dataset,
        error: object_store::Error,
    ) -> DataConnectorError {
        match error {
            object_store::Error::Generic { source, .. } => {
                // Try to provide more specific error messages based on auth method
                let has_msi = self.params.get("msi_endpoint").expose().ok().is_some();
                let has_use_cli = self
                    .params
                    .get("use_cli")
                    .expose()
                    .ok()
                    .is_some_and(|v| v.eq_ignore_ascii_case("true"));
                let has_client_creds = self.params.get("client_id").expose().ok().is_some();
                let has_sas = self.params.get("sas_string").expose().ok().is_some();

                if has_msi || has_use_cli {
                    let err = Error::ManagedIdentityAuthenticationFailed { source };
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else if has_client_creds {
                    let err = Error::ClientCredentialsAuthenticationFailed { source };
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else if has_sas {
                    let err = Error::SasAuthenticationFailed { source };
                    DataConnectorError::InvalidConfiguration {
                        dataconnector: format!("{self}"),
                        message: format!("{err}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source: err.into(),
                    }
                } else {
                    DataConnectorError::UnableToConnectInternal {
                        dataconnector: format!("{self}"),
                        connector_component: ConnectorComponent::from(dataset),
                        source,
                    }
                }
            }
            error => DataConnectorError::UnableToConnectInternal {
                dataconnector: format!("{self}"),
                connector_component: ConnectorComponent::from(dataset),
                source: error.into(),
            },
        }
    }
}

register_data_connector!(
    register_abfs_connector,
    REGISTER_ABFS_CONNECTOR,
    "abfs",
    AzureBlobFSFactory
);

register_data_connector!(
    register_abfss_connector,
    REGISTER_ABFSS_CONNECTOR,
    "abfss",
    AzureBlobFSFactory
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataconnector::listing::ListingTableConnector;
    use crate::parameters::ParameterSpec;
    use datafusion::parquet::arrow::async_reader::ObjectVersionType;
    use datafusion_table_providers::util::secrets::to_secret_map;
    use std::collections::HashMap;

    const TEST_PARAMETERS: &[ParameterSpec] = &[
        ParameterSpec::component("account").secret(),
        ParameterSpec::component("versioning").default("disabled"),
        ParameterSpec::component("use_cli").is_boolean(),
        ParameterSpec::component("msi_endpoint").secret(),
        ParameterSpec::component("client_id").secret(),
        ParameterSpec::component("sas_string").secret(),
    ];

    fn create_test_connector(
        params: HashMap<String, String>,
        handle: tokio::runtime::Handle,
    ) -> AzureBlobFS {
        AzureBlobFS {
            params: Parameters::new(
                to_secret_map(params).into_iter().collect(),
                "abfs",
                TEST_PARAMETERS,
            ),
            runtime: None,
            tokio_io_runtime: handle,
        }
    }

    #[tokio::test]
    async fn test_object_versioning_type_disabled_by_default() {
        let connector = create_test_connector(HashMap::new(), tokio::runtime::Handle::current());
        assert!(connector.object_versioning_type().is_none());
    }

    #[tokio::test]
    async fn test_object_versioning_type_explicitly_disabled() {
        let connector = create_test_connector(
            [("versioning".to_string(), "disabled".to_string())].into(),
            tokio::runtime::Handle::current(),
        );
        assert!(connector.object_versioning_type().is_none());
    }

    #[tokio::test]
    async fn test_object_versioning_type_enabled() {
        let connector = create_test_connector(
            [("versioning".to_string(), "enabled".to_string())].into(),
            tokio::runtime::Handle::current(),
        );
        assert_eq!(
            connector.object_versioning_type(),
            Some(ObjectVersionType::Version)
        );
    }

    #[tokio::test]
    async fn test_display_format() {
        let connector = create_test_connector(HashMap::new(), tokio::runtime::Handle::current());
        assert_eq!(format!("{connector}"), "abfs");
    }

    #[tokio::test]
    async fn test_get_runtime_returns_none_when_not_set() {
        let connector = create_test_connector(HashMap::new(), tokio::runtime::Handle::current());
        assert!(connector.get_runtime().is_none());
    }

    #[test]
    fn test_factory_prefix() {
        let factory = AzureBlobFSFactory::new();
        assert_eq!(factory.prefix(), "abfs");
    }

    #[test]
    fn test_factory_parameters_includes_versioning() {
        let factory = AzureBlobFSFactory::new();
        let params = factory.parameters();
        assert!(params.iter().any(|p| p.name == "versioning"));
    }

    #[test]
    fn test_factory_parameters_includes_client_timeout() {
        let factory = AzureBlobFSFactory::new();
        let params = factory.parameters();
        assert!(params.iter().any(|p| p.name == "client_timeout"));
    }
}
