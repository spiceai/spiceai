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

use super::CatalogConnector;
use super::ConnectorComponent;
use super::ParameterSpec;
use super::Parameters;
use crate::Runtime;
use crate::component::ComponentInitialization;
use crate::component::catalog::Catalog;
use crate::dataconnector::http_rate_control;
use crate::dataconnector::parameters::ConnectorParams;
use crate::token_providers::databricks::{
    AuthCredentials, build_auth_credentials, get_m2m_token_provider, get_u2m_token_provider,
};
use async_trait::async_trait;
use data_components::Read;
use data_components::RefreshableCatalogProvider;
use data_components::databricks::sql_warehouse::{SqlWarehouseConfig, shared_request_semaphore};
use data_components::databricks::{DatabricksSparkConnect, DatabricksSqlWarehouse};
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::CatalogId;
use data_components::unity_catalog::Endpoint;
use data_components::unity_catalog::UCTable;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use data_components::unity_catalog::credential_vending::VendedDeltaTableFactory;
use data_components::unity_catalog::provider::{
    ReadTableProviderFactory, UCTableProviderFactory, UnityCatalogProvider,
};
use datafusion::sql::TableReference;
use runtime_rate_control::RateController;
use runtime_secrets::get_params_with_secrets;
use secrecy::{ExposeSecret, SecretString};
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use token_provider::StaticTokenProvider;
use url::Url;

#[derive(Clone)]
pub struct Databricks {
    params: Parameters,
    initialization: ComponentInitialization,
}

impl Databricks {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        let component_initialization = match build_auth_credentials(&params.parameters) {
            Ok(AuthCredentials::U2M(_)) => ComponentInitialization::OnTrigger,
            _ => ComponentInitialization::default(),
        };

        Arc::new(Self {
            params: params.parameters,
            initialization: component_initialization,
        })
    }
}

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("endpoint")
        .required()
        .secret()
        .description("The endpoint of the Databricks instance."),
    ParameterSpec::component("token")
        .secret()
        .description("The personal access token used to authenticate against the DataBricks API."),
    ParameterSpec::component("credential_vending").description(
        "When set to 'enabled' (requires 'mode' to be 'delta_lake'), short-lived storage credentials for each table are fetched from the Unity Catalog credential vending API instead of using static storage credentials. Defaults to 'disabled'.",
    ),
    ParameterSpec::runtime("mode")
        .description("The execution mode for querying against Databricks.")
        .default("spark_connect"),
    ParameterSpec::runtime("client_timeout")
        .description("HTTP client request timeout. In 'delta_lake' mode, applies to the object store client. In 'sql_warehouse' mode, applies per-HTTP-call (statement submit, status poll, chunk fetch) — set to the longest expected single call, not total query duration. Accepts durations like '30s' or '5m'. Default: 30s."),
    ParameterSpec::runtime("connect_timeout")
        .description("Timeout for establishing TCP/TLS connections to the Databricks API. Applies in 'sql_warehouse' mode. Accepts durations like '10s'. Default: 10s."),
    ParameterSpec::component("cluster_id").description("The ID of the compute cluster in Databricks to use for the query. Only valid when mode is spark_connect."),
    ParameterSpec::component("use_ssl").description("Use a TLS connection to connect to the Databricks Spark Connect endpoint.").default("true"),
    ParameterSpec::component("sql_warehouse_id")
        .secret()
        .description("The SQL Warehouse ID to use when 'mode' is set to 'sql_warehouse'"),

    // Connection / resilience tuning (sql_warehouse mode)
    ParameterSpec::runtime("max_concurrent_requests")
        .description("Maximum number of concurrent HTTP requests to the Databricks endpoint. Also controls the SQL Warehouse API request semaphore in sql_warehouse mode.")
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
    ParameterSpec::runtime("requests_per_second_limit")
        .description("Maximum number of HTTP requests per second to the Databricks endpoint. Overrides runtime.params.http_requests_per_second_limit when set."),
    ParameterSpec::runtime("requests_per_minute_limit")
        .description("Maximum number of HTTP requests per minute to the Databricks endpoint. Overrides runtime.params.http_requests_per_minute_limit when set."),
    ParameterSpec::runtime("rate_control_jitter_min")
        .description("Minimum random delay added before Databricks HTTP requests when rate control is active. Overrides runtime.params.http_rate_control_jitter_min when set. Accepts durations such as '5ms' or '0ms'. Defaults to 5ms when a request-rate limit is configured, otherwise 0ms."),
    ParameterSpec::runtime("rate_control_jitter_max")
        .description("Maximum random delay added before Databricks HTTP requests when rate control is active. Overrides runtime.params.http_rate_control_jitter_max when set. Accepts durations such as '10ms' or '0ms'. Defaults to 10ms when a request-rate limit is configured, otherwise 0ms."),

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

fn databricks_rate_control_url(endpoint: &str) -> std::result::Result<Url, url::ParseError> {
    let endpoint = endpoint.trim_end_matches('/');
    let endpoint_url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("https://{endpoint}")
    };

    Url::parse(&endpoint_url)
}

async fn shared_databricks_catalog_rate_controller(
    params: &Parameters,
    runtime: &Arc<Runtime>,
    catalog: &Catalog,
) -> super::Result<Option<Arc<RateController>>> {
    let endpoint = params.get("endpoint").expose().ok_or_else(|p| {
        super::Error::InvalidConfigurationNoSource {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
            message: format!("A required parameter was missing: {}", p.0),
        }
    })?;
    let base_url = databricks_rate_control_url(endpoint).map_err(|source| {
        super::Error::InvalidConfigurationNoSource {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
            message: format!("Invalid Databricks endpoint '{endpoint}': {source}"),
        }
    })?;
    let connector_component = ConnectorComponent::from(catalog);
    let rate_control = http_rate_control::resolve_config_for_component(
        params,
        Some(&catalog.app.runtime.params),
        &connector_component,
        "databricks",
    )
    .map_err(|source| super::Error::UnableToGetCatalogProvider {
        connector: "databricks".to_string(),
        connector_component: ConnectorComponent::from(catalog),
        source: source.into(),
    })?;

    runtime
        .http_rate_control_registry()
        .shared_rate_controller_for_component(
            &base_url,
            &rate_control,
            catalog.app.name.as_str(),
            &connector_component,
            "databricks",
        )
        .await
        .map(|shared| shared.controller)
        .map_err(|source| super::Error::UnableToGetCatalogProvider {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
            source: source.into(),
        })
}

#[async_trait]
impl CatalogConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(super::Error::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: "A Catalog Name is required for the Databricks Unity Catalog. For details, visit: https://spiceai.org/docs/components/catalogs/databricks#from".into(),
                connector_component: ConnectorComponent::from(catalog)
            });
        };

        let endpoint = self.params.get("endpoint").expose().ok_or_else(|p| {
            super::Error::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: format!("A required parameter was missing: {}. For details, visit: https://spiceai.org/docs/components/catalogs/databricks#params", p.0),
                connector_component: ConnectorComponent::from(catalog)
            }
        })?;

        let auth_credentials = build_auth_credentials(&self.params).map_err(|source| {
            super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            }
        })?;

        let token_provider: Arc<dyn TokenProvider> = match auth_credentials {
            AuthCredentials::Token(token) => Arc::new(StaticTokenProvider::new(token.clone())),
            AuthCredentials::ServicePrincipal(client_id, client_secret) => get_m2m_token_provider(
                endpoint,
                client_id,
                client_secret,
                &runtime.token_provider_registry,
            )
            .await
            .map_err(|source| super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?,
            AuthCredentials::U2M(client_id) => {
                get_u2m_token_provider(endpoint, client_id, &runtime.token_provider_registry)
                    .await
                    .map_err(|source| super::Error::UnableToGetCatalogProvider {
                        connector: "databricks".to_string(),
                        source: source.into(),
                        connector_component: ConnectorComponent::from(catalog),
                    })?
            }
        };

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
            PARAMETERS,
        )
        .await
        .context(super::InternalWithSourceSnafu {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
        })?;

        let rate_controller =
            shared_databricks_catalog_rate_controller(&params, &runtime, catalog).await?;

        let unity_catalog = UnityCatalogClient::new_with_rate_controller(
            Endpoint(endpoint.to_string()),
            Some(Arc::clone(&token_provider)),
            None,
            rate_controller.clone(),
        )
        .map_err(|source| super::Error::UnableToGetCatalogProvider {
            connector: "databricks".to_string(),
            source: source.into(),
            connector_component: ConnectorComponent::from(catalog),
        })?;
        let client = Arc::new(unity_catalog);

        let mode = self.params.get("mode").expose().ok();
        let credential_vending = match params.get("credential_vending").expose().ok() {
            Some("enabled") => true,
            None | Some("disabled") => false,
            Some(other) => {
                return Err(super::Error::InvalidConfigurationNoSource {
                    connector: "databricks".into(),
                    message: format!(
                        "Invalid value '{other}' for 'databricks_credential_vending'. Valid values: 'enabled', 'disabled'."
                    ),
                    connector_component: ConnectorComponent::from(catalog),
                });
            }
        };
        if credential_vending && mode != Some("delta_lake") {
            return Err(super::Error::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message:
                    "'databricks_credential_vending' is only supported when 'mode' is 'delta_lake'."
                        .into(),
                connector_component: ConnectorComponent::from(catalog),
            });
        }
        let table_creator: Arc<dyn UCTableProviderFactory> = if mode == Some("delta_lake") {
            if credential_vending {
                Arc::new(VendedDeltaTableFactory::new(
                    Arc::clone(&client),
                    params.to_secret_map(),
                    runtime.tokio_io_runtime(),
                ))
            } else {
                Arc::new(ReadTableProviderFactory::new(
                    Arc::new(DeltaTableFactory::new(
                        params.to_secret_map(),
                        runtime.tokio_io_runtime(),
                    )) as Arc<dyn Read>,
                    table_reference_creator_delta_lake,
                ))
            }
        } else if mode == Some("sql_warehouse") {
            let sql_warehouse_id = params.get("sql_warehouse_id").expose().ok_or_else(|p| {
                super::Error::InvalidConfigurationNoSource {
                    connector: "databricks".into(),
                    message: format!("Missing required parameter: {}", p.0),
                    connector_component: ConnectorComponent::from(catalog),
                }
            })?;

            let token_provider = create_token_provider_for_catalog(
                endpoint,
                &params,
                Arc::clone(&runtime.token_provider_registry),
                catalog,
            )
            .await?;

            let config = build_sql_warehouse_config(&params);
            let shared_semaphore = shared_request_semaphore(
                endpoint,
                sql_warehouse_id,
                config.max_concurrent_requests,
            )
            .map_err(|source| super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?;

            let read_provider =
                DatabricksSqlWarehouse::with_config_semaphore_permissions_and_rate_controller(
                    endpoint,
                    sql_warehouse_id,
                    token_provider,
                    config,
                    Some(shared_semaphore),
                    Arc::new(data_components::schema_discovery::NoPermissionsCheck),
                    rate_controller.clone(),
                )
                .map_err(|source| super::Error::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                })?;

            Arc::new(ReadTableProviderFactory::new(
                Arc::new(read_provider) as Arc<dyn Read>,
                table_reference_creator_spark,
            ))
        } else {
            // Default to spark_connect
            let cluster_id = params.get("cluster_id").ok_or_else(|p| {
                super::Error::InvalidConfigurationNoSource {
                    connector: "databricks".into(),
                    message: format!("Missing required parameter: {}", p.0),
                    connector_component: ConnectorComponent::from(catalog),
                }
            })?;

            let use_ssl = !matches!(params.get("use_ssl").expose().ok(), Some("false"));

            let token_provider = create_token_provider_for_catalog(
                endpoint,
                &params,
                Arc::clone(&runtime.token_provider_registry),
                catalog,
            )
            .await?;

            let read_provider = DatabricksSparkConnect::from_token_provider_with_rate_controller(
                endpoint.to_string(),
                cluster_id.expose_secret().to_string(),
                use_ssl,
                token_provider,
                rate_controller.clone(),
            )
            .await
            .map_err(|source| super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source,
                connector_component: ConnectorComponent::from(catalog),
            })?;

            Arc::new(ReadTableProviderFactory::new(
                Arc::new(read_provider) as Arc<dyn Read>,
                table_reference_creator_spark,
            ))
        };

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            CatalogId(catalog_id),
            table_creator,
            catalog.include.clone(),
        )
        .await
        {
            Ok(provider) => provider,
            Err(e) => {
                return Err(super::Error::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: Box::new(e),
                    connector_component: ConnectorComponent::from(catalog),
                });
            }
        };

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
    // Don't append a trailing slash here — `DeltaTable::from` calls
    // `ensure_folder_location` which already adds one when needed.
    // Unconditionally appending caused double-slash paths (e.g.
    // "file:///path/to/table//") when the catalog API returned
    // locations that already ended with '/'.
    Some(TableReference::bare(storage_location.to_string()))
}

use token_provider::TokenProvider;
use token_provider::registry::TokenProviderRegistry;

async fn create_token_provider_for_catalog(
    endpoint: &str,
    params: &Parameters,
    token_provider_registry: Arc<TokenProviderRegistry>,
    catalog: &Catalog,
) -> super::Result<Arc<dyn TokenProvider>> {
    let auth_credentials = build_auth_credentials(params).map_err(|source| {
        super::Error::UnableToGetCatalogProvider {
            connector: "databricks".to_string(),
            source: source.into(),
            connector_component: ConnectorComponent::from(catalog),
        }
    })?;

    match auth_credentials {
        AuthCredentials::Token(token) => {
            let token_provider: Arc<dyn TokenProvider> =
                Arc::new(StaticTokenProvider::new(token.clone()));
            Ok(token_provider)
        }
        AuthCredentials::ServicePrincipal(client_id, client_secret) => {
            get_m2m_token_provider(endpoint, client_id, client_secret, &token_provider_registry)
                .await
                .map_err(|source| super::Error::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                })
        }
        AuthCredentials::U2M(client_id) => {
            get_u2m_token_provider(endpoint, client_id, &token_provider_registry)
                .await
                .map_err(|source| super::Error::UnableToGetCatalogProvider {
                    connector: "databricks".to_string(),
                    source: source.into(),
                    connector_component: ConnectorComponent::from(catalog),
                })
        }
    }
}

pub fn build_sql_warehouse_config(params: &Parameters) -> SqlWarehouseConfig {
    let mut config = SqlWarehouseConfig::default();

    if let Some(v) = params.get("max_concurrent_requests").expose().ok() {
        match v.parse::<usize>() {
            Ok(0) => {
                tracing::warn!(
                    parameter = "max_concurrent_requests",
                    value = v,
                    "Invalid Databricks SQL Warehouse config value; must be >= 1; using default"
                );
            }
            Ok(n) => config.max_concurrent_requests = n,
            Err(e) => {
                tracing::warn!(parameter = "max_concurrent_requests", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("http_max_retries").expose().ok() {
        match v.parse::<usize>() {
            Ok(n) => config.http_max_retries = n,
            Err(e) => {
                tracing::warn!(parameter = "http_max_retries", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("backoff_method").expose().ok() {
        match v.parse::<util::retry_strategy::BackoffMethod>() {
            Ok(m) => config.backoff_method = m,
            Err(e) => {
                tracing::warn!(parameter = "backoff_method", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("statement_max_retries").expose().ok() {
        match v.parse::<usize>() {
            Ok(n) => config.statement_max_retries = n,
            Err(e) => {
                tracing::warn!(parameter = "statement_max_retries", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("disable_on_permanent_error").expose().ok() {
        match v.parse::<bool>() {
            Ok(b) => config.disable_on_permanent_error = b,
            Err(e) => {
                tracing::warn!(parameter = "disable_on_permanent_error", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("connect_timeout").expose().ok() {
        match duration_parse::parse_duration(v) {
            Ok(d) => config.connect_timeout = d,
            Err(e) => {
                tracing::warn!(parameter = "connect_timeout", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }
    if let Some(v) = params.get("client_timeout").expose().ok() {
        match duration_parse::parse_duration(v) {
            Ok(d) => config.request_timeout = d,
            Err(e) => {
                tracing::warn!(parameter = "client_timeout", value = v, error = %e, "Invalid Databricks SQL Warehouse config value; using default");
            }
        }
    }

    config
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_parameters(entries: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            entries
                .iter()
                .map(|(key, value)| (key.to_string(), SecretString::from(*value)))
                .collect(),
            "databricks",
            PARAMETERS,
        )
    }

    fn make_uc_table(storage_location: Option<&str>) -> UCTable {
        UCTable {
            name: "my_table".to_string(),
            catalog_name: "my_catalog".to_string(),
            schema_name: "my_schema".to_string(),
            table_type: "MANAGED".to_string(),
            data_source_format: "DELTA".to_string(),
            columns: vec![],
            storage_location: storage_location.map(ToString::to_string),
            table_id: None,
        }
    }

    #[test]
    fn test_table_reference_creator_spark_returns_full_reference() {
        let table = make_uc_table(Some("s3://bucket/path"));
        let reference =
            table_reference_creator_spark(&table).expect("spark creator should always return Some");
        assert!(
            matches!(reference, TableReference::Full { .. }),
            "Expected Full table reference"
        );
        match reference {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "my_catalog");
                assert_eq!(schema.as_ref(), "my_schema");
                assert_eq!(table.as_ref(), "my_table");
            }
            _ => unreachable!("already asserted to be Full table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_spark_ignores_storage_location() {
        let table = make_uc_table(None);
        let reference = table_reference_creator_spark(&table)
            .expect("spark creator should return Some regardless of storage_location");
        assert!(
            matches!(reference, TableReference::Full { .. }),
            "Expected Full table reference"
        );
        match reference {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "my_catalog");
                assert_eq!(schema.as_ref(), "my_schema");
                assert_eq!(table.as_ref(), "my_table");
            }
            _ => unreachable!("already asserted to be Full table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_delta_lake_with_storage() {
        let table = make_uc_table(Some("s3://bucket/path"));
        let reference = table_reference_creator_delta_lake(&table)
            .expect("should return Some when storage_location is present");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(table.as_ref(), "s3://bucket/path");
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_delta_lake_without_storage() {
        let table = make_uc_table(None);
        assert!(
            table_reference_creator_delta_lake(&table).is_none(),
            "should return None when storage_location is None"
        );
    }

    #[test]
    fn test_table_reference_creator_delta_lake_preserves_location() {
        let table = make_uc_table(Some("abfss://container@account.dfs.core.windows.net/path"));
        let reference = table_reference_creator_delta_lake(&table).expect("should return Some");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(
                    table.as_ref(),
                    "abfss://container@account.dfs.core.windows.net/path",
                    "delta lake reference should preserve location without modification"
                );
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/7904>
    /// Storage locations ending with '/' must not get a second '/' appended.
    #[test]
    fn test_table_reference_creator_delta_lake_no_double_slash() {
        let table = make_uc_table(Some("s3://bucket/path/"));
        let reference = table_reference_creator_delta_lake(&table).expect("should return Some");
        match reference {
            TableReference::Bare { table } => {
                assert!(
                    !table.as_ref().ends_with("//"),
                    "must not produce double trailing slash, got: {}",
                    table.as_ref()
                );
                assert_eq!(table.as_ref(), "s3://bucket/path/");
            }
            _ => panic!("Expected Bare table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_spark_uses_all_name_fields() {
        let table = UCTable {
            name: "orders".to_string(),
            catalog_name: "prod_catalog".to_string(),
            schema_name: "sales".to_string(),
            table_type: "EXTERNAL".to_string(),
            data_source_format: "PARQUET".to_string(),
            columns: vec![],
            storage_location: Some("s3://bucket/orders".to_string()),
            table_id: None,
        };
        let reference =
            table_reference_creator_spark(&table).expect("spark creator should always return Some");
        assert!(
            matches!(reference, TableReference::Full { .. }),
            "Expected Full table reference"
        );
        match reference {
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                assert_eq!(catalog.as_ref(), "prod_catalog");
                assert_eq!(schema.as_ref(), "sales");
                assert_eq!(table.as_ref(), "orders");
            }
            _ => unreachable!("already asserted to be Full table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_delta_lake_preserves_full_storage_uri() {
        let table = make_uc_table(Some("s3://my-bucket/warehouse/catalog/schema/table"));
        let reference = table_reference_creator_delta_lake(&table).expect("should return Some");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(
                    table.as_ref(),
                    "s3://my-bucket/warehouse/catalog/schema/table"
                );
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    #[test]
    fn test_build_sql_warehouse_config_parses_valid_overrides() {
        let params = make_parameters(&[
            ("max_concurrent_requests", "4"),
            ("http_max_retries", "6"),
            ("backoff_method", "exponential"),
            ("statement_max_retries", "21"),
            ("disable_on_permanent_error", "false"),
            ("connect_timeout", "5s"),
            ("client_timeout", "2m"),
        ]);

        let config = build_sql_warehouse_config(&params);

        assert_eq!(config.max_concurrent_requests, 4);
        assert_eq!(config.http_max_retries, 6);
        assert_eq!(
            config.backoff_method,
            util::retry_strategy::BackoffMethod::Exponential
        );
        assert_eq!(config.statement_max_retries, 21);
        assert!(!config.disable_on_permanent_error);
        assert_eq!(config.connect_timeout, std::time::Duration::from_secs(5));
        assert_eq!(config.request_timeout, std::time::Duration::from_secs(120));
    }

    #[test]
    fn test_build_sql_warehouse_config_uses_defaults_for_invalid_values() {
        let params = make_parameters(&[
            ("max_concurrent_requests", "0"),
            ("http_max_retries", "NaN"),
            ("backoff_method", "quadratic"),
            ("statement_max_retries", "bad"),
            ("disable_on_permanent_error", "maybe"),
            ("connect_timeout", "not-a-duration"),
            ("client_timeout", ""),
        ]);

        let config = build_sql_warehouse_config(&params);
        let defaults = SqlWarehouseConfig::default();

        assert_eq!(
            config.max_concurrent_requests,
            defaults.max_concurrent_requests
        );
        assert_eq!(config.http_max_retries, defaults.http_max_retries);
        assert_eq!(config.backoff_method, defaults.backoff_method);
        assert_eq!(config.statement_max_retries, defaults.statement_max_retries);
        assert_eq!(
            config.disable_on_permanent_error,
            defaults.disable_on_permanent_error
        );
        assert_eq!(config.connect_timeout, defaults.connect_timeout);
        assert_eq!(config.request_timeout, defaults.request_timeout);
    }

    /// Regression test: every runtime param consumed by
    /// [`build_sql_warehouse_config`] must be declared in [`PARAMETERS`],
    /// otherwise `Parameters::try_new` strips the key before it reaches
    /// `build_sql_warehouse_config` and the override is silently ignored.
    #[test]
    fn test_sql_warehouse_config_params_are_declared_in_parameters_spec() {
        let expected = [
            "max_concurrent_requests",
            "http_max_retries",
            "backoff_method",
            "statement_max_retries",
            "disable_on_permanent_error",
            "connect_timeout",
            "client_timeout",
        ];
        for name in expected {
            assert!(
                PARAMETERS.iter().any(|p| p.name == name),
                "parameter `{name}` is consumed by build_sql_warehouse_config but not declared in PARAMETERS; Parameters::try_new would strip it"
            );
        }
    }
}
