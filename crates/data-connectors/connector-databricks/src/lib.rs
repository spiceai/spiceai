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

//! Databricks data and catalog connector for Spice.ai runtime.
//!
//! This crate provides Databricks connector implementations, allowing
//! Spice.ai to connect to Databricks (Delta Lake, Spark Connect, SQL Warehouse)
//! as data sources and Unity Catalog as a catalog source.
//!
//! This connector is extracted from the runtime crate to enable faster
//! incremental builds.

use async_trait::async_trait;
#[cfg(feature = "spark")]
use data_components::databricks::DatabricksSparkConnect;
use data_components::databricks::sql_warehouse::DatabricksMetrics;
use data_components::databricks::{DatabricksDelta, DatabricksSqlWarehouse, sql_warehouse};
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use data_components::unity_catalog::{
    CatalogId, Endpoint, UCTable, UnityCatalog as UnityCatalogClient,
};
use data_components::{Read, RefreshableCatalogProvider};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use opentelemetry::KeyValue;
use runtime::Runtime;
use runtime::catalogconnector::{CatalogConnector, Error as CatalogError, Result as CatalogResult};
use runtime::component::ComponentInitialization;
use runtime::component::ComponentType;
use runtime::component::catalog::Catalog;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::parameters::{ParameterSpec, Parameters};
use runtime::token_providers::databricks::{
    AuthCredentials, DatabricksM2MTokenProvider, DatabricksU2MTokenProvider,
};
use runtime_secrets::get_params_with_secrets;
#[cfg(feature = "spark")]
use secrecy::ExposeSecret;
use secrecy::SecretString;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use token_provider::registry::TokenProviderRegistry;
use token_provider::{StaticTokenProvider, TokenProvider};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

// ============================================================================
// Data Connector Error Types
// ============================================================================

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

    #[cfg(feature = "spark")]
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

    #[snafu(display(
        "Insufficient permissions to read table '{table_name}'. The current principal does not have a read-compatible privilege on this table. Grant SELECT or ALL PRIVILEGES on the table, and try again."
    ))]
    InsufficientPermissions { table_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

// ============================================================================
// Data Connector Parameters
// ============================================================================

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .required()
        .secret()
        .description("The endpoint of the Databricks instance."),
    ParameterSpec::component("sql_warehouse_id")
        .secret()
        .description("The SQL Warehouse ID to use when 'mode' is set to 'sql_warehouse'"),

    // Connection / resilience tuning (sql_warehouse mode)
    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent HTTP requests to the SQL Warehouse API.")
        .default("8"),
    ParameterSpec::runtime("http_max_retries")
        .description("Maximum number of HTTP-level retries for transient failures (429, 5xx).")
        .default("3"),
    ParameterSpec::runtime("backoff_method")
        .description("Backoff strategy for transient HTTP retries.")
        .one_of(&["fibonacci", "exponential"])
        .default("fibonacci"),
    ParameterSpec::runtime("statement_max_retries")
        .description("Maximum number of poll retries when waiting for async statement completion.")
        .default("14"),
    ParameterSpec::runtime("disable_on_permanent_error")
        .description("When true, non-retryable errors (401, 403, 404) permanently disable the connector to prevent a thundering herd of failed requests.")
        .default("true"),

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

// ============================================================================
// Databricks Data Connector
// ============================================================================

/// Databricks data connector.
pub struct Databricks {
    read_provider: Arc<dyn Read>,
    initialization: ComponentInitialization,
    metrics: Option<Arc<DatabricksMetrics>>,
    /// Unity Catalog client for table type detection and permission checking.
    /// Present when the connector was created with enough information to call UC APIs.
    uc_client: Option<Arc<UnityCatalogClient>>,
}

impl std::fmt::Debug for Databricks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Databricks").finish_non_exhaustive()
    }
}

impl Databricks {
    /// Creates a new Databricks connector instance.
    ///
    /// # Errors
    ///
    /// Returns an error if required parameters are missing or invalid,
    /// or if the connection to Databricks fails.
    pub async fn new(
        params: Parameters,
        io_runtime: Handle,
        token_provider_registry: Arc<TokenProviderRegistry>,
        shared_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Self> {
        let mode = params.get("mode").expose().ok().unwrap_or_default();
        let endpoint = params
            .get("endpoint")
            .expose()
            .ok_or_else(|p| MissingParameterSnafu { parameter: p.0 }.build())?;

        let auth_credentials = Self::build_auth_credentials(&params)?;
        let initialization = match auth_credentials {
            AuthCredentials::U2M(_) => ComponentInitialization::OnTrigger,
            _ => ComponentInitialization::default(),
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

                let uc_client = match UnityCatalogClient::new(
                    Endpoint(endpoint.to_string()),
                    Some(Arc::clone(&token_provider)),
                    shared_semaphore.clone(),
                ) {
                    Ok(client) => Some(Arc::new(client)),
                    Err(error) => {
                        tracing::warn!(
                            endpoint,
                            %error,
                            "Failed to initialize Unity Catalog client; UC table-type and permission validation is disabled"
                        );
                        None
                    }
                };

                let sql_warehouse_config =
                    runtime::catalogconnector::databricks::build_sql_warehouse_config(&params);

                let read_provider = DatabricksSqlWarehouse::with_config_and_semaphore(
                    endpoint,
                    sql_warehouse_id,
                    token_provider,
                    sql_warehouse_config,
                    shared_semaphore,
                )
                .context(UnableToConstructDatabricksSqlWarehouseSnafu)?;
                let metrics = Some(Arc::clone(read_provider.metrics()));

                Ok(Self {
                    read_provider: Arc::new(read_provider),
                    initialization,
                    metrics,
                    uc_client,
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

                let uc_client = match UnityCatalogClient::new(
                    Endpoint(endpoint.to_string()),
                    Some(Arc::clone(&token_provider)),
                    None,
                ) {
                    Ok(client) => Some(Arc::new(client)),
                    Err(error) => {
                        tracing::warn!(
                            endpoint,
                            %error,
                            "Failed to initialize Unity Catalog client; UC table-type and permission validation is disabled"
                        );
                        None
                    }
                };

                let read_provider = DatabricksDelta::new(
                    Endpoint(endpoint.to_string()),
                    storage_options,
                    token_provider,
                    io_runtime,
                );

                Ok(Self {
                    read_provider: Arc::new(read_provider),
                    initialization,
                    metrics: None,
                    uc_client,
                })
            }
            #[cfg(feature = "spark")]
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
            #[cfg(not(feature = "spark"))]
            "spark_connect" => Err(Error::InvalidMode {
                value: "spark_connect (feature disabled - requires spark-connect-rs with arrow 57)"
                    .to_string(),
            }),
            _ => Err(Error::InvalidMode {
                value: mode.to_string(),
            }),
        }
    }

    /// Gets a token provider based on the auth credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if token provider creation fails.
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

    /// Builds authentication credentials from the provided parameters.
    ///
    /// # Errors
    ///
    /// Returns an error if the authentication configuration is invalid.
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

    #[cfg(feature = "spark")]
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
            initialization: ComponentInitialization::default(),
            metrics: None,
            uc_client: None,
        })
    }

    /// Gets an M2M (machine-to-machine) token provider.
    ///
    /// # Errors
    ///
    /// Returns an error if token provider registration fails.
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

    /// Gets a U2M (user-to-machine) token provider.
    ///
    /// # Errors
    ///
    /// Returns an error if token provider registration fails.
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

    /// Validates that a Unity Catalog table is of a supported type and that the
    /// dataset metadata is compatible with Databricks query-time access checks.
    ///
    /// Returns `Ok(())` if validation passes or if it cannot be performed
    /// (e.g., table not found in UC — the table may not be a UC table at all).
    ///
    /// Returns an error only when the UC API definitively reports an unsupported
    /// table type. Permission prechecks are advisory because Databricks query-time
    /// validation is the authoritative source of truth for table access.
    async fn validate_uc_table(
        &self,
        uc_client: &UnityCatalogClient,
        table_reference: &TableReference,
        dataset: &Dataset,
    ) -> DataConnectorResult<()> {
        let full_name = table_reference.to_string();
        let should_validate_permissions = match uc_client.get_table(table_reference).await {
            Ok(Some(uc_table)) => {
                if !uc_table.is_queryable() {
                    return Err(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: "databricks".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        message: format!(
                            "Unsupported Unity Catalog table type '{}' for table '{}'. Only MANAGED, EXTERNAL, FOREIGN, and MATERIALIZED_VIEW tables can be queried.",
                            uc_table.table_type, full_name
                        ),
                    });
                }
                tracing::debug!(
                    table = %full_name,
                    table_type = %uc_table.table_type,
                    "Unity Catalog table type is supported"
                );

                if !uc_table.requires_read_permission_validation() {
                    tracing::debug!(
                        table = %full_name,
                        table_type = %uc_table.table_type,
                        "Skipping strict Unity Catalog permission precheck for foreign table; Databricks validates access at query time"
                    );
                    return Ok(());
                }

                true
            }
            Ok(None) => {
                tracing::debug!(
                    table = %full_name,
                    "Table not found in Unity Catalog; skipping UC validation"
                );
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    table = %full_name,
                    error = %e,
                    "Failed to check Unity Catalog table metadata; proceeding without validation"
                );
                return Ok(());
            }
        };

        // 2) Check permissions via UC effective-permissions endpoint.
        if should_validate_permissions {
            match uc_client.get_effective_permissions(&full_name).await {
                Ok(Some(perms)) => {
                    if !perms.has_read_permission() {
                        tracing::warn!(
                            table = %full_name,
                            principals = ?perms.principals(),
                            privileges = ?perms.all_privileges(),
                            "Unity Catalog effective-permissions did not report a read-compatible privilege; proceeding and deferring to Databricks query-time validation"
                        );
                    } else {
                        tracing::debug!(
                            table = %full_name,
                            principals = ?perms.principals(),
                            "Unity Catalog permission check passed"
                        );
                    }
                }
                Ok(None) => {
                    tracing::debug!(
                        table = %full_name,
                        "Table not found when checking permissions; proceeding"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        table = %full_name,
                        error = %e,
                        "Failed to check Unity Catalog permissions; proceeding without validation"
                    );
                }
            }
        }

        Ok(())
    }
}

// ============================================================================
// Data Connector Factory
// ============================================================================
#[derive(Default, Clone)]
pub struct DatabricksFactory;

impl DatabricksFactory {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for DatabricksFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        if let Some(runtime) = params.runtime {
            let aws_region = params
                .parameters
                .get("aws_region")
                .expose()
                .ok()
                .map(ToString::to_string);
            let param_map = params.parameters.to_secret_map();

            Box::pin(async move {
                // Initialize AWS SDK credentials if not using explicit credentials
                if !aws_sdk_credential_bridge::has_explicit_credentials(
                    &param_map,
                    "aws_access_key_id",
                    "aws_secret_access_key",
                ) && let Err(err) =
                    aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(
                        aws_region.as_deref(),
                    )
                    .await
                {
                    tracing::warn!(
                        "Unable to initialize AWS credentials for Databricks connector: {err}"
                    );
                }

                let shared_semaphore = if matches!(
                    params.parameters.get("mode").expose().ok(),
                    Some("sql_warehouse")
                ) {
                    match (
                        params.parameters.get("endpoint").expose().ok(),
                        params.parameters.get("sql_warehouse_id").expose().ok(),
                    ) {
                        (Some(endpoint), Some(warehouse_id)) => {
                            let config =
                                runtime::catalogconnector::databricks::build_sql_warehouse_config(
                                    &params.parameters,
                                );
                            Some(sql_warehouse::shared_request_semaphore(
                                endpoint,
                                warehouse_id,
                                config.max_concurrent_requests,
                            )?)
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                let databricks = Databricks::new(
                    params.parameters,
                    params.io_runtime,
                    runtime.token_provider_registry(),
                    shared_semaphore,
                )
                .await?;
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
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let table_reference = TableReference::from(dataset.path());

        // If we have a UC client and the table reference is fully qualified
        // (catalog.schema.table), validate the table type and permissions
        // upfront before attempting to create the table provider.
        if let Some(uc_client) = &self.uc_client
            && table_reference.catalog().is_some()
            && table_reference.schema().is_some()
        {
            self.validate_uc_table(uc_client, &table_reference, dataset)
                .await?;
        }

        self.read_provider
            .table_provider(table_reference)
            .await
            .map_err(|source| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "databricks".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            })
    }

    fn initialization(&self) -> ComponentInitialization {
        self.initialization
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.metrics.as_ref().map(|m| {
            Arc::new(DatabricksMetricsProvider {
                metrics: Arc::clone(m),
            }) as Arc<dyn MetricsProvider>
        })
    }
}

// ============================================================================
// Databricks Metrics Provider
// ============================================================================

#[derive(Debug, Clone)]
struct DatabricksMetricsProvider {
    metrics: Arc<DatabricksMetrics>,
}

const DATABRICKS_METRICS: &[MetricSpec] = &[
    // -- Request metrics --
    MetricSpec::new("requests_total", MetricType::ObservableCounterU64)
        .description("Total HTTP requests issued to the SQL Warehouse API"),
    MetricSpec::new("retries_total", MetricType::ObservableCounterU64)
        .description("Total HTTP retries performed for transient failures"),
    MetricSpec::new("permanent_errors_total", MetricType::ObservableCounterU64).description(
        "Total non-retryable errors (401, 403, 404) that permanently disabled the connector",
    ),
    MetricSpec::new("inflight_operations", MetricType::ObservableGaugeU64)
        .description(
            "Current number of in-flight SQL Warehouse operations holding a concurrency permit",
        )
        .auto_register(),
    // -- Statement metrics --
    MetricSpec::new(
        "statements_executed_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total SQL statements submitted for execution"),
    MetricSpec::new("statement_polls_total", MetricType::ObservableCounterU64)
        .description("Total polls made when waiting for async statement completion"),
    MetricSpec::new("statements_failed_total", MetricType::ObservableCounterU64)
        .description("Total SQL statements that completed with FAILED status"),
    // -- Connection pool metrics --
    MetricSpec::new("pool_connections_total", MetricType::ObservableCounterU64)
        .description("Total virtual pool connect() calls"),
    MetricSpec::new("pool_active_connections", MetricType::ObservableGaugeU64)
        .description("Current number of active connection handles"),
    // -- Concurrency metrics --
    MetricSpec::new(
        "semaphore_available_permits",
        MetricType::ObservableGaugeU64,
    )
    .description("Current number of available concurrency permits in the request semaphore"),
    // -- Data transfer metrics --
    MetricSpec::new("chunks_fetched_total", MetricType::ObservableCounterU64)
        .description("Total Arrow result chunks fetched from external links"),
    // -- Connector state --
    MetricSpec::new("connector_disabled", MetricType::ObservableGaugeU64)
        .description("Whether the connector is permanently disabled (1 = disabled, 0 = active)"),
];

impl MetricsProvider for DatabricksMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "databricks"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        DATABRICKS_METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        let metrics = Arc::clone(&self.metrics);
        match metric.name {
            "requests_total" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(
                    metrics
                        .requests_total
                        .load(std::sync::atomic::Ordering::Relaxed),
                    &attributes,
                );
            }))),
            "retries_total" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(
                    metrics
                        .retries_total
                        .load(std::sync::atomic::Ordering::Relaxed),
                    &attributes,
                );
            }))),
            "permanent_errors_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .permanent_errors_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "inflight_operations" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .inflight_operations
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "statements_executed_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .statements_executed_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "statement_polls_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .statement_polls_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "statements_failed_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .statements_failed_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "pool_connections_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .pool_connections_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "pool_active_connections" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .pool_active_connections
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "semaphore_available_permits" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    let permits = metrics
                        .semaphore
                        .as_ref()
                        .map_or(0, |s| s.available_permits() as u64);
                    instrument.observe(permits, &attributes);
                })))
            }
            "chunks_fetched_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(
                        metrics
                            .chunks_fetched_total
                            .load(std::sync::atomic::Ordering::Relaxed),
                        &attributes,
                    );
                })))
            }
            "connector_disabled" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                let disabled = u64::from(
                    metrics
                        .permanently_disabled
                        .load(std::sync::atomic::Ordering::Relaxed),
                );
                instrument.observe(disabled, &attributes);
            }))),
            _ => None,
        }
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "databricks";

/// Returns a new instance of the `Databricks` data connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    DatabricksFactory::new_arc()
}

// ============================================================================
// Catalog Connector
// ============================================================================

pub const CATALOG_PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .required()
        .secret()
        .description("The endpoint of the Databricks instance."),
    ParameterSpec::component("token")
        .secret()
        .description("The personal access token used to authenticate against the DataBricks API."),
    ParameterSpec::runtime("mode")
        .description("The execution mode for querying against Databricks.")
        .default("spark_connect"),
    ParameterSpec::runtime("client_timeout")
        .description("The timeout setting for object store client."),
    ParameterSpec::component("cluster_id").description("The ID of the compute cluster in Databricks to use for the query. Only valid when mode is spark_connect."),
    ParameterSpec::component("use_ssl").description("Use a TLS connection to connect to the Databricks Spark Connect endpoint.").default("true"),
    ParameterSpec::component("sql_warehouse_id")
        .secret()
        .description("The SQL Warehouse ID to use when 'mode' is set to 'sql_warehouse'"),

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

/// Databricks Unity Catalog connector.
#[derive(Clone)]
pub struct DatabricksCatalog {
    params: Parameters,
    initialization: ComponentInitialization,
}

impl DatabricksCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        let component_initialization = match Databricks::build_auth_credentials(&params.parameters)
        {
            Ok(AuthCredentials::U2M(_)) => ComponentInitialization::OnTrigger,
            _ => ComponentInitialization::default(),
        };

        Arc::new(Self {
            params: params.parameters,
            initialization: component_initialization,
        })
    }
}

#[async_trait]
impl CatalogConnector for DatabricksCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> CatalogResult<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(CatalogError::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: "A Catalog Name is required for the Databricks Unity Catalog. For details, visit: https://spiceai.org/docs/components/catalogs/databricks#from".into(),
                connector_component: ConnectorComponent::from(catalog)
            });
        };

        let endpoint = self.params.get("endpoint").expose().ok_or_else(|p| {
            CatalogError::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: format!("A required parameter was missing: {}. For details, visit: https://spiceai.org/docs/components/catalogs/databricks#params", p.0),
                connector_component: ConnectorComponent::from(catalog)
            }
        })?;

        let auth_credentials =
            Databricks::build_auth_credentials(&self.params).map_err(|source| {
                CatalogError::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                }
            })?;

        let token_provider = match auth_credentials {
            AuthCredentials::Token(token) => Arc::new(StaticTokenProvider::new(token.clone())),
            AuthCredentials::ServicePrincipal(client_id, client_secret) => {
                Databricks::get_m2m_token_provider(
                    endpoint,
                    client_id,
                    client_secret,
                    &runtime.token_provider_registry(),
                )
                .await
                .map_err(|source| CatalogError::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                })?
            }
            AuthCredentials::U2M(client_id) => Databricks::get_u2m_token_provider(
                endpoint,
                client_id,
                &runtime.token_provider_registry(),
            )
            .await
            .map_err(|source| CatalogError::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?,
        };

        let unity_catalog =
            UnityCatalogClient::new(Endpoint(endpoint.to_string()), Some(token_provider), None)
                .map_err(|source| CatalogError::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: Box::new(source),
                    connector_component: ConnectorComponent::from(catalog),
                })?;
        let client = Arc::new(unity_catalog);

        // Copy the catalog params into the dataset params, and allow user to override
        let mut dataset_params: HashMap<String, SecretString> =
            get_params_with_secrets(runtime.secrets(), &catalog.params).await;

        let secret_dataset_params =
            get_params_with_secrets(runtime.secrets(), &catalog.dataset_params).await;

        for (key, value) in secret_dataset_params {
            dataset_params.insert(key, value);
        }

        let params = Parameters::try_new(
            "connector databricks",
            dataset_params.into_iter().collect(),
            "databricks",
            runtime.secrets(),
            CATALOG_PARAMETERS,
        )
        .await
        .map_err(|source| CatalogError::InternalWithSource {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
            source,
        })?;

        let mode = self.params.get("mode").expose().ok();
        let (table_creator, table_reference_creator) = if mode == Some("delta_lake") {
            (
                Arc::new(DeltaTableFactory::new(
                    params.to_secret_map(),
                    runtime.tokio_io_runtime(),
                )) as Arc<dyn Read>,
                table_reference_creator_delta_lake as fn(&UCTable) -> Option<TableReference>,
            )
        } else {
            let shared_semaphore = if mode == Some("sql_warehouse") {
                match (
                    params.get("endpoint").expose().ok(),
                    params.get("sql_warehouse_id").expose().ok(),
                ) {
                    (Some(endpoint), Some(warehouse_id)) => {
                        let config =
                            runtime::catalogconnector::databricks::build_sql_warehouse_config(
                                &params,
                            );
                        Some(
                            sql_warehouse::shared_request_semaphore(
                                endpoint,
                                warehouse_id,
                                config.max_concurrent_requests,
                            )
                            .map_err(|source| {
                                CatalogError::UnableToGetCatalogProvider {
                                    connector: "databricks".to_string(),
                                    source: source.into(),
                                    connector_component: ConnectorComponent::from(catalog),
                                }
                            })?,
                        )
                    }
                    _ => None,
                }
            } else {
                None
            };

            let dataset_databricks = Databricks::new(
                params,
                runtime.tokio_io_runtime(),
                runtime.token_provider_registry(),
                shared_semaphore,
            )
            .await
            .map_err(|source| CatalogError::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?;

            (
                dataset_databricks.read_provider(),
                table_reference_creator_spark as fn(&UCTable) -> Option<TableReference>,
            )
        };

        let catalog_provider = UnityCatalogProvider::try_new(
            client,
            CatalogId(catalog_id),
            table_creator,
            table_reference_creator,
            catalog.include.clone(),
        )
        .await
        .map_err(|e| CatalogError::UnableToGetCatalogProvider {
            connector: "databricks".to_string(),
            source: Box::new(e),
            connector_component: ConnectorComponent::from(catalog),
        })?;

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }

    fn initialization(&self) -> ComponentInitialization {
        self.initialization
    }
}

#[expect(clippy::unnecessary_wraps)]
fn table_reference_creator_spark(uc_table: &UCTable) -> Option<TableReference> {
    let table_reference = TableReference::Full {
        catalog: uc_table.catalog_name.clone().into(),
        schema: uc_table.schema_name.clone().into(),
        table: uc_table.name.clone().into(),
    };
    Some(table_reference)
}

fn table_reference_creator_delta_lake(uc_table: &UCTable) -> Option<TableReference> {
    let storage_location = uc_table.storage_location.as_deref()?;
    Some(TableReference::bare(format!("{storage_location}/")))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use app::App;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::MemTable,
    };
    use runtime::component::{
        access::AccessMode,
        dataset::{CheckAvailability, ReadyState},
    };
    use secrecy::ExposeSecret;
    use spicepod::metric::Metrics;
    use std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::Mutex,
    };

    #[derive(Clone)]
    struct MockRead {
        call_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Read for MockRead {
        async fn table_provider(
            &self,
            _table_reference: TableReference,
        ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>>
        {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let table = MemTable::try_new(Arc::clone(&schema), vec![Vec::new()])?;

            Ok(Arc::new(table))
        }
    }

    #[derive(Clone)]
    struct MockHttpResponse {
        status_line: &'static str,
        headers: Vec<(String, String)>,
        body: String,
    }

    impl MockHttpResponse {
        fn json(status_line: &'static str, body: impl Into<String>) -> Self {
            Self {
                status_line,
                headers: vec![("Content-Type".to_string(), "application/json".to_string())],
                body: body.into(),
            }
        }

        fn empty(status_line: &'static str) -> Self {
            Self {
                status_line,
                headers: Vec::new(),
                body: String::new(),
            }
        }
    }

    async fn start_mock_server(
        responses: Vec<MockHttpResponse>,
    ) -> (String, Arc<AtomicUsize>, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("should bind to a port");
        let addr = listener
            .local_addr()
            .expect("should have a listener address");
        let queued_responses = Arc::new(Mutex::new(VecDeque::from(responses)));
        let requests = Arc::new(AtomicUsize::new(0));
        let captured_requests = Arc::new(Mutex::new(Vec::new()));

        let requests_for_server = Arc::clone(&requests);
        let captured_requests_for_server = Arc::clone(&captured_requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };

                let queued_responses = Arc::clone(&queued_responses);
                let requests = Arc::clone(&requests_for_server);
                let captured_requests = Arc::clone(&captured_requests_for_server);
                tokio::spawn(async move {
                    let captured_request = read_http_request(&mut stream).await;
                    requests.fetch_add(1, Ordering::SeqCst);
                    captured_requests
                        .lock()
                        .await
                        .push(String::from_utf8_lossy(&captured_request).into_owned());

                    let response = queued_responses
                        .lock()
                        .await
                        .pop_front()
                        .unwrap_or_else(|| MockHttpResponse::json("200 OK", r#"{"ok":true}"#));

                    let mut http_response = format!(
                        "HTTP/1.1 {}\r\nContent-Length: {}\r\n",
                        response.status_line,
                        response.body.len()
                    );
                    for (header_name, header_value) in response.headers {
                        let _ = std::fmt::Write::write_fmt(
                            &mut http_response,
                            format_args!("{header_name}: {header_value}\r\n"),
                        );
                    }
                    http_response.push_str("\r\n");
                    http_response.push_str(&response.body);

                    let _ = stream.write_all(http_response.as_bytes()).await;
                });
            }
        });

        (format!("http://{addr}"), requests, captured_requests)
    }

    async fn read_http_request(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut captured_request = Vec::with_capacity(4096);
        let mut buf = [0u8; 1024];
        let mut expected_total_len = None;

        loop {
            let bytes_read = match stream.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(bytes_read) => bytes_read,
            };

            captured_request.extend_from_slice(&buf[..bytes_read]);

            if expected_total_len.is_none() {
                expected_total_len = expected_http_request_len(&captured_request);
            }

            if let Some(expected_total_len) = expected_total_len
                && captured_request.len() >= expected_total_len
            {
                break;
            }
        }

        captured_request
    }

    fn expected_http_request_len(request: &[u8]) -> Option<usize> {
        let headers_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)?;

        let content_length = String::from_utf8_lossy(&request[..headers_end])
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.trim().eq_ignore_ascii_case("Content-Length") {
                    value.trim().parse::<usize>().ok()
                } else {
                    None
                }
            })
            .unwrap_or(0);

        Some(headers_end.saturating_add(content_length))
    }

    async fn make_dataset(from: &str, name: &str) -> Dataset {
        Dataset {
            from: from.to_string(),
            name: TableReference::bare(name),
            access: AccessMode::Read,
            params: HashMap::new(),
            metadata: HashMap::new(),
            columns: Vec::new(),
            has_metadata_table: false,
            replication: None,
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            acceleration: None,
            embeddings: Vec::new(),
            app: Arc::new(App::default()),
            unsupported_type_action: None,
            ready_state: ReadyState::default(),
            metrics: Metrics::default(),
            runtime: Arc::new(Runtime::builder().build().await),
            vectors: None,
            check_availability: CheckAvailability::default(),
        }
    }

    async fn run_read_provider_with_uc_responses(
        dataset_from: &str,
        responses: Vec<MockHttpResponse>,
    ) -> (DataConnectorResult<()>, usize, usize, Vec<String>) {
        let (endpoint, requests, captured_requests) = start_mock_server(responses).await;

        let read_call_count = Arc::new(AtomicUsize::new(0));
        let connector = Databricks {
            read_provider: Arc::new(MockRead {
                call_count: Arc::clone(&read_call_count),
            }),
            initialization: ComponentInitialization::default(),
            metrics: None,
            uc_client: Some(Arc::new(
                UnityCatalogClient::new(Endpoint(endpoint), None, None)
                    .expect("mock Unity Catalog client should be created"),
            )),
        };
        let dataset = make_dataset(dataset_from, "tpch_sf400_part").await;

        let result = DataConnector::read_provider(&connector, &dataset)
            .await
            .map(|_| ());
        let captured_requests = captured_requests.lock().await.clone();

        (
            result,
            read_call_count.load(Ordering::SeqCst),
            requests.load(Ordering::SeqCst),
            captured_requests,
        )
    }

    fn assert_request_seen(captured_requests: &[String], path_fragment: &str) {
        assert!(
            captured_requests
                .iter()
                .any(|request| request.contains(path_fragment)),
            "expected request containing '{path_fragment}', got: {captured_requests:?}"
        );
    }

    fn assert_request_not_seen(captured_requests: &[String], path_fragment: &str) {
        assert!(
            captured_requests
                .iter()
                .all(|request| !request.contains(path_fragment)),
            "did not expect request containing '{path_fragment}', got: {captured_requests:?}"
        );
    }

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

    #[tokio::test]
    async fn test_read_provider_proceeds_when_uc_permission_precheck_has_read_privilege() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"MANAGED","data_source_format":"DELTA","columns":[],"storage_location":null}"#,
                    ),
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"privilege_assignments":[{"principal":"analytics-team","privileges":[{"privilege":"SELECT"}]}]}"#,
                    ),
                ],
            )
            .await;

        assert!(
            result.is_ok(),
            "positive UC permission prechecks should still allow initialization"
        );
        assert_eq!(read_call_count, 1, "expected the Databricks read to be attempted");
        assert_eq!(request_count, 2, "expected table metadata and permission requests");
        assert_request_seen(&captured_requests, "/api/2.1/unity-catalog/tables/");
        assert_request_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }

    #[tokio::test]
    async fn test_read_provider_proceeds_when_uc_permission_precheck_lacks_read_privilege() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"MANAGED","data_source_format":"DELTA","columns":[],"storage_location":null}"#,
                    ),
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"privilege_assignments":[{"principal":"analytics-team","privileges":[{"privilege":"MODIFY"}]}]}"#,
                    ),
                ],
            )
            .await;

        assert!(
            result.is_ok(),
            "negative UC permission prechecks should not block Databricks dataset initialization"
        );
        assert_eq!(
            read_call_count, 1,
            "the authoritative Databricks read should still be attempted"
        );
        assert_eq!(
            request_count, 2,
            "expected table metadata and effective-permissions requests"
        );
        assert_request_seen(&captured_requests, "/api/2.1/unity-catalog/tables/");
        assert_request_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }

    #[tokio::test]
    async fn test_read_provider_proceeds_when_effective_permissions_are_missing() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"MANAGED","data_source_format":"DELTA","columns":[],"storage_location":null}"#,
                    ),
                    MockHttpResponse::empty("404 Not Found"),
                ],
            )
            .await;

        assert!(
            result.is_ok(),
            "missing effective-permissions responses should not block Databricks dataset initialization"
        );
        assert_eq!(read_call_count, 1, "expected the Databricks read to be attempted");
        assert_eq!(request_count, 2, "expected table metadata and permission requests");
        assert_request_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }

    #[tokio::test]
    async fn test_read_provider_proceeds_when_effective_permissions_check_errors() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![
                    MockHttpResponse::json(
                        "200 OK",
                        r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"MANAGED","data_source_format":"DELTA","columns":[],"storage_location":null}"#,
                    ),
                    MockHttpResponse::empty("500 Internal Server Error"),
                ],
            )
            .await;

        assert!(
            result.is_ok(),
            "effective-permissions lookup errors should not block Databricks dataset initialization"
        );
        assert_eq!(read_call_count, 1, "expected the Databricks read to be attempted");
        assert!(
            request_count >= 2,
            "expected at least the table metadata request and one permission attempt"
        );
        assert_request_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }

    #[tokio::test]
    async fn test_read_provider_skips_effective_permissions_for_foreign_tables() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![MockHttpResponse::json(
                    "200 OK",
                    r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"FOREIGN","data_source_format":"DELTA","columns":[],"storage_location":null}"#,
                )],
            )
            .await;

        assert!(
            result.is_ok(),
            "foreign tables should skip strict permission prechecks and proceed"
        );
        assert_eq!(read_call_count, 1, "expected the Databricks read to be attempted");
        assert_eq!(request_count, 1, "foreign tables should skip the permission request");
        assert_request_seen(&captured_requests, "/api/2.1/unity-catalog/tables/");
        assert_request_not_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }

    #[tokio::test]
    async fn test_read_provider_fails_for_unsupported_uc_table_types() {
        let (result, read_call_count, request_count, captured_requests) =
            run_read_provider_with_uc_responses(
                "databricks:workspace.tpch_sf400.part",
                vec![MockHttpResponse::json(
                    "200 OK",
                    r#"{"name":"part","catalog_name":"workspace","schema_name":"tpch_sf400","table_type":"VIEW","data_source_format":"VIEW","columns":[],"storage_location":null}"#,
                )],
            )
            .await;

        let err = result.expect_err("unsupported UC table types should still fail");

        assert!(
            err.to_string().contains("Unsupported Unity Catalog table type 'VIEW'"),
            "unexpected error: {err}"
        );
        assert_eq!(read_call_count, 0, "unsupported types should fail before reading");
        assert_eq!(request_count, 1, "unsupported types should stop after table metadata");
        assert_request_seen(&captured_requests, "/api/2.1/unity-catalog/tables/");
        assert_request_not_seen(
            &captured_requests,
            "/api/2.1/unity-catalog/effective-permissions/table/",
        );
    }
}
