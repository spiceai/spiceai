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

use crate::component::ComponentInitialization;
use crate::component::dataset::Dataset;
use crate::token_providers::databricks::{
    AuthCredentials, DatabricksM2MTokenProvider, DatabricksU2MTokenProvider,
};
use async_trait::async_trait;
use data_components::Read;
use data_components::databricks::{
    DatabricksDelta, DatabricksSparkConnect, DatabricksSqlWarehouse, sql_warehouse,
};
use data_components::unity_catalog::Endpoint;
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use secrecy::{ExposeSecret, SecretString};
use snafu::prelude::*;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use token_provider::registry::TokenProviderRegistry;
use token_provider::{StaticTokenProvider, TokenProvider};

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
    Parameters,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Missing required parameter: {parameter}. Specify a value. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    MissingParameter { parameter: String },

    #[snafu(display(
        "Invalid `databricks_use_ssl` value: '{value}'. Use 'true' or 'false'. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    InvalidUsessl { value: String },

    #[snafu(display(
        "Failed to connect to Databricks Spark. {source} Verify the connector configuration, and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    UnableToConstructDatabricksSpark {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to connect to Databricks SQL Warehouse. {source} Verify the connector configuration, and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    UnableToConstructDatabricksSqlWarehouse { source: sql_warehouse::Error },

    #[snafu(display(
        "Invalid `mode` value: '{value}'. Use 'delta_lake' or 'spark_connect'. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    InvalidMode { value: String },

    #[snafu(display(
        "Invalid configuration: {message}. For details, visit: https://spiceai.org/docs/components/data-connectors/databricks#parameters"
    ))]
    InvalidConfiguration { message: String },

    #[snafu(display(
        "Failed to build Databricks connector: required component '{missing_component}' is missing. An unexpected error occurred. Report a bug to request support: https://github.com/spiceai/spiceai/issues"
    ))]
    UnableToBuild { missing_component: String },

    #[snafu(display(
        "Failed to obtain Databricks service principal token for machine-to-machine authentication. {source}"
    ))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub struct Databricks {
    read_provider: Arc<dyn Read>,
    initialization: ComponentInitialization,
}

impl std::fmt::Debug for Databricks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Databricks").finish_non_exhaustive()
    }
}

impl Databricks {
    pub async fn new(
        params: Parameters,
        token_provider_registry: Arc<TokenProviderRegistry>,
    ) -> Result<Self> {
        let mode = params.get("mode").expose().ok().unwrap_or_default();
        let endpoint = params
            .get("endpoint")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let auth_credentials = Self::build_auth_credentials(&params)?;
        let initialization = match auth_credentials {
            AuthCredentials::U2M(_) => ComponentInitialization::OnTrigger,
            _ => ComponentInitialization::OnStartup,
        };

        match mode {
            "sql_warehouse" => {
                let sql_warehouse_id = params
                    .get("sql_warehouse_id")
                    .expose()
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

                let token_provider =
                    Self::get_token_provider(endpoint, auth_credentials, token_provider_registry)
                        .await?;

                let read_provider =
                    DatabricksSqlWarehouse::new(endpoint, sql_warehouse_id, token_provider)
                        .context(UnableToConstructDatabricksSqlWarehouseSnafu)?;

                Ok(Self {
                    read_provider: Arc::new(read_provider),
                    initialization,
                })
            }
            "delta_lake" => {
                let storage_options = params.to_secret_map();
                let token_provider: Arc<dyn TokenProvider> = match auth_credentials {
                    AuthCredentials::Token(token) => {
                        Arc::new(StaticTokenProvider::new(token.clone())) as Arc<dyn TokenProvider>
                    }
                    AuthCredentials::ServicePrincipal(client_id, client_secret) => {
                        Self::get_m2m_token_provider(
                            endpoint,
                            client_id,
                            client_secret,
                            &token_provider_registry,
                        )
                        .await?
                    }
                    AuthCredentials::U2M(client_id) => {
                        Self::get_u2m_token_provider(endpoint, client_id, &token_provider_registry)
                            .await?
                    }
                };

                let read_provider = DatabricksDelta::new(
                    Endpoint(endpoint.to_string()),
                    storage_options,
                    token_provider,
                );

                Ok(Self {
                    read_provider: Arc::new(read_provider),
                    initialization,
                })
            }
            "spark_connect" => {
                let cluster_id = params
                    .get("cluster_id")
                    .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

                let databricks_use_ssl = match params.get("use_ssl").expose().ok() {
                    Some(value) => match value {
                        "true" => true,
                        "false" => false,
                        _ => return InvalidUsesslSnafu { value }.fail(),
                    },
                    None => true, // Default value
                };

                Self::build_spark_connect_connector(
                    endpoint,
                    auth_credentials,
                    token_provider_registry,
                    cluster_id,
                    databricks_use_ssl,
                )
                .await
            }
            _ => Err(Error::InvalidMode {
                value: mode.to_string(),
            }),
        }
    }

    pub async fn get_token_provider(
        endpoint: &str,
        auth_credentials: AuthCredentials<'_>,
        token_provider_registry: Arc<TokenProviderRegistry>,
    ) -> Result<Arc<dyn TokenProvider>> {
        Ok(match auth_credentials {
            AuthCredentials::Token(token) => Arc::new(StaticTokenProvider::new(token.clone())),
            AuthCredentials::ServicePrincipal(client_id, client_secret) => {
                Self::get_m2m_token_provider(
                    endpoint,
                    client_id,
                    client_secret,
                    &token_provider_registry,
                )
                .await?
            }
            AuthCredentials::U2M(client_id) => {
                Self::get_u2m_token_provider(endpoint, client_id, &token_provider_registry).await?
            }
        })
    }

    pub fn build_auth_credentials(params: &Parameters) -> Result<AuthCredentials<'_>> {
        let token = params.get("token").ok();
        let client_id = params.get("client_id").expose().ok();
        let client_secret = params.get("client_secret").ok();

        match (token, client_id, client_secret) {
            (Some(token), None, None) => Ok(AuthCredentials::Token(token)),
            (None, Some(client_id), None) => Ok(AuthCredentials::U2M(client_id)),
            (None, Some(client_id), Some(client_secret)) => {
                Ok(AuthCredentials::ServicePrincipal(client_id, client_secret))
            }
            (None, None, None) => {
                InvalidConfigurationSnafu {
                    message: "Missing `databricks_token` or `databricks_client_id` and `databricks_client_secret` parameters".to_string(),
                }
                .fail()
            }
            (None, None, Some(_)) => {
                MissingParameterSnafu {
                    parameter: "databricks_client_id".to_string(),
                }
                .fail()
            }
            (Some(_), Some(_), Some(_) | None) => {
                InvalidConfigurationSnafu {
                    message: "Choose either `databricks_token` or `databricks_client_id` and `databricks_client_secret`".to_string(),
                }
                .fail()
            }
            _ => {
                InvalidConfigurationSnafu {
                    message: "Invalid authentication configuration. Choose either `databricks_token` or `databricks_client_id` and `databricks_client_secret`".to_string(),
                }
                .fail()
            }
        }
    }

    async fn build_spark_connect_connector(
        endpoint: &str,
        auth_credentials: AuthCredentials<'_>,
        token_provider_registry: Arc<TokenProviderRegistry>,
        cluster_id: &SecretString,
        databricks_use_ssl: bool,
    ) -> Result<Self> {
        let read_provider = match auth_credentials {
            AuthCredentials::Token(token) => Arc::new(
                DatabricksSparkConnect::new(
                    endpoint.to_string(),
                    cluster_id.expose_secret().to_string(),
                    token.expose_secret().to_string(),
                    databricks_use_ssl,
                )
                .await
                .context(UnableToConstructDatabricksSparkSnafu)?,
            ),

            AuthCredentials::ServicePrincipal(client_id, client_secret) => {
                let token_provider = Self::get_m2m_token_provider(
                    endpoint,
                    client_id,
                    client_secret,
                    &token_provider_registry,
                )
                .await?;

                Arc::new(
                    DatabricksSparkConnect::from_token_provider(
                        endpoint.to_string(),
                        cluster_id.expose_secret().to_string(),
                        databricks_use_ssl,
                        token_provider,
                    )
                    .await
                    .context(UnableToConstructDatabricksSparkSnafu)?,
                )
            }

            AuthCredentials::U2M(client_id) => {
                let token_provider =
                    Self::get_u2m_token_provider(endpoint, client_id, &token_provider_registry)
                        .await?;

                Arc::new(
                    DatabricksSparkConnect::from_token_provider(
                        endpoint.to_string(),
                        cluster_id.expose_secret().to_string(),
                        databricks_use_ssl,
                        token_provider,
                    )
                    .await
                    .context(UnableToConstructDatabricksSparkSnafu)?,
                )
            }
        };

        Ok(Self {
            read_provider,

            // Databricks spark connect doesn't support U2M, so no deferred loading
            initialization: ComponentInitialization::OnStartup,
        })
    }

    pub async fn get_m2m_token_provider(
        endpoint: &str,
        client_id: &str,
        client_secret: &SecretString,
        token_provider_registry: &Arc<TokenProviderRegistry>,
    ) -> Result<Arc<dyn TokenProvider>> {
        token_provider_registry
            .get_or_create_provider(format!("databricks_m2m_{client_id}"), || async {
                DatabricksM2MTokenProvider::try_new(
                    endpoint.to_string(),
                    client_id.to_string(),
                    client_secret.clone(),
                )
                .await
            })
            .await
            .map_err(|e| Error::UnableToGetToken {
                source: Box::new(e),
            })
    }

    pub async fn get_u2m_token_provider(
        endpoint: &str,
        client_id: &str,
        token_provider_registry: &Arc<TokenProviderRegistry>,
    ) -> Result<Arc<dyn TokenProvider>> {
        token_provider_registry
            .get_or_create_provider::<DatabricksU2MTokenProvider, std::convert::Infallible, _, _>(
                format!("databricks_u2m_{client_id}"),
                || async {
                    Ok(DatabricksU2MTokenProvider::new(
                        endpoint.to_string(),
                        client_id.to_string(),
                    ))
                },
            )
            .await
            .map_err(|err| Error::UnableToGetToken {
                source: Box::new(err),
            })
    }

    pub(crate) fn read_provider(&self) -> Arc<dyn Read> {
        Arc::clone(&self.read_provider)
    }
}

#[derive(Default, Clone, Copy)]
pub struct DatabricksFactory {}

impl DatabricksFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .required()
        .secret()
        .description("The endpoint of the Databricks instance."),
    ParameterSpec::component("sql_warehouse_id")
        .secret()
        .description("The SQL Warehouse ID to use when 'mode' is set to 'sql_warehouse'"),
    ParameterSpec::component("token")
        .secret()
        .description("The personal access token used to authenticate against the DataBricks API."),
    ParameterSpec::runtime("mode")
        .description("The execution mode for running queries: 'spark_connect', 'delta_lake', or 'sql_warehouse'.")
        .default("spark_connect"),
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for object store client."),
    ParameterSpec::component("cluster_id").description("The ID of the compute cluster in Databricks to use for the query. Only valid when mode is spark_connect."),
    ParameterSpec::component("use_ssl").description("Use a TLS connection to connect to the Databricks Spark Connect endpoint.").default("true"),

    // Databricks M2M Service Principal credentials
    ParameterSpec::component("client_id").description("The client ID of the Databricks service principal."),
    ParameterSpec::component("client_secret").secret().description("The client secret of the Databricks service principal."),

    // S3 storage options
    ParameterSpec::component("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),
    ParameterSpec::component("aws_allow_http")
        .description("The AWS endpoint allow http scheme")
        .secret(),

    // Azure storage options
    ParameterSpec::component("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::component("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::component("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::component("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),

    // GCS storage options
    ParameterSpec::component("google_service_account")
        .description("Filesystem path to the Google service account JSON key file.")
        .secret(),
];

impl DataConnectorFactory for DatabricksFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        if let Some(runtime) = params.runtime {
            Box::pin(async move {
                // Initialize the AWS SDK and make it available.
                let _ = aws_sdk_credential_bridge::initialize_sdk_config().await;
                let databricks =
                    Databricks::new(params.parameters, runtime.token_provider_registry()).await?;
                Ok(Arc::new(databricks) as Arc<dyn DataConnector>)
            })
        } else {
            Box::pin(async move {
                Err(Box::new(Error::UnableToBuild {
                    missing_component: "runtime".to_string(),
                })
                    as Box<dyn std::error::Error + Send + Sync>)
            })
        }
    }

    fn prefix(&self) -> &'static str {
        "databricks"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let table_reference = TableReference::from(dataset.path());
        Ok(self
            .read_provider
            .table_provider(table_reference, dataset.schema())
            .await
            .context(super::UnableToGetReadProviderSnafu {
                dataconnector: "databricks",
                connector_component: ConnectorComponent::from(dataset),
            })?)
    }

    fn initialization(&self) -> ComponentInitialization {
        self.initialization
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;

    #[test]
    fn test_build_auth_credentials_token_only() {
        let token = "test_token";
        let params_vec = vec![("token".to_string(), SecretString::from(token))];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_ok(),
            "Databricks::build_auth_credentials should return an Ok result"
        );
        if let Ok(AuthCredentials::Token(t)) = result {
            assert_eq!(t.expose_secret(), token);
        } else {
            panic!("Expected Token variant");
        }
    }

    #[test]
    fn test_build_auth_credentials_service_principal() {
        let client_id = "test_client_id";
        let client_secret = "test_client_secret";
        let params_vec = vec![
            ("client_id".to_string(), SecretString::from(client_id)),
            (
                "client_secret".to_string(),
                SecretString::from(client_secret),
            ),
        ];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_ok(),
            "Databricks::build_auth_credentials should return an Ok result"
        );
        if let Ok(AuthCredentials::ServicePrincipal(id, secret)) = result {
            assert_eq!(id, client_id);
            assert_eq!(secret.expose_secret(), client_secret);
        } else {
            panic!("Expected ServicePrincipal variant");
        }
    }

    #[test]
    fn test_build_auth_credentials_missing_all() {
        let params_vec = vec![];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_err(),
            "Databricks::build_auth_credentials should return an error"
        );
        if let Err(error) = result {
            assert!(error.to_string().contains("Missing `databricks_token` or `databricks_client_id` and `databricks_client_secret` parameters"));
        }
    }

    #[test]
    fn test_build_auth_credentials_missing_client_secret() {
        let client_id = "test_client_id";
        let params_vec = vec![("client_id".to_string(), SecretString::from(client_id))];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_ok(),
            "Databricks::build_auth_credentials should return an Ok result"
        );
        if let Ok(AuthCredentials::U2M(id)) = result {
            assert_eq!(id, client_id);
        } else {
            panic!("Expected U2M variant");
        }
    }

    #[test]
    fn test_build_auth_credentials_u2m() {
        let client_secret = "test_client_secret";
        let params_vec = vec![(
            "client_secret".to_string(),
            SecretString::from(client_secret),
        )];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_err(),
            "Databricks::build_auth_credentials should return an error"
        );
        if let Err(error) = result {
            assert!(error.to_string().contains("databricks_client_id"));
        }
    }

    #[test]
    fn test_build_auth_credentials_all_provided() {
        let token = "test_token";
        let client_id = "test_client_id";
        let client_secret = "test_client_secret";
        let params_vec = vec![
            ("token".to_string(), SecretString::from(token)),
            ("client_id".to_string(), SecretString::from(client_id)),
            (
                "client_secret".to_string(),
                SecretString::from(client_secret),
            ),
        ];
        let parameters = Parameters::new(params_vec, "databricks", PARAMETERS);

        let result = Databricks::build_auth_credentials(&parameters);

        assert!(
            result.is_err(),
            "Databricks::build_auth_credentials should return an error"
        );
        if let Err(error) = result {
            assert!(error.to_string().contains("Choose either `databricks_token` or `databricks_client_id` and `databricks_client_secret`"));
        }
    }
}
