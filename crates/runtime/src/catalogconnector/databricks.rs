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
use crate::dataconnector::databricks::Databricks as DatabricksDataConnector;
use crate::dataconnector::parameters::ConnectorParams;
use crate::get_params_with_secrets;
use crate::token_providers::databricks::AuthCredentials;
use async_trait::async_trait;
use data_components::Read;
use data_components::RefreshableCatalogProvider;
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::CatalogId;
use data_components::unity_catalog::Endpoint;
use data_components::unity_catalog::UCTable;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use token_provider::StaticTokenProvider;

#[derive(Clone)]
pub struct Databricks {
    params: Parameters,
    initialization: ComponentInitialization,
}

impl Databricks {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        let component_initialization =
            match DatabricksDataConnector::build_auth_credentials(&params.parameters) {
                Ok(AuthCredentials::U2M(_)) => ComponentInitialization::OnTrigger,
                _ => ComponentInitialization::OnStartup,
            };

        Arc::new(Self {
            params: params.parameters,
            initialization: component_initialization,
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
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

#[async_trait]
impl CatalogConnector for Databricks {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::too_many_lines)]
    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(super::Error::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: "A Catalog Name is required for the Databricks Unity Catalog.\nFor details, visit: https://spiceai.org/docs/components/catalogs/databricks#from".into(),
                connector_component: ConnectorComponent::from(catalog)
            });
        };

        let endpoint = self.params.get("endpoint").expose().ok_or_else(|p| {
            super::Error::InvalidConfigurationNoSource {
                connector: "databricks".into(),
                message: format!("A required parameter was missing: {}.\nFor details, visit: https://spiceai.org/docs/components/catalogs/databricks#params", p.0),
                connector_component: ConnectorComponent::from(catalog)
            }
        })?;

        let auth_credentials = DatabricksDataConnector::build_auth_credentials(&self.params)
            .map_err(|source| super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?;

        let token_provider = match auth_credentials {
            AuthCredentials::Token(token) => Arc::new(StaticTokenProvider::new(token.clone())),
            AuthCredentials::ServicePrincipal(client_id, client_secret) => {
                DatabricksDataConnector::get_m2m_token_provider(
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
                })?
            }
            AuthCredentials::U2M(client_id) => DatabricksDataConnector::get_u2m_token_provider(
                endpoint,
                client_id,
                &runtime.token_provider_registry,
            )
            .await
            .map_err(|source| super::Error::UnableToGetCatalogProvider {
                connector: "databricks".to_string(),
                source: source.into(),
                connector_component: ConnectorComponent::from(catalog),
            })?,
        };

        let unity_catalog =
            UnityCatalogClient::new(Endpoint(endpoint.to_string()), Some(token_provider));
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
            PARAMETERS,
        )
        .await
        .context(super::InternalWithSourceSnafu {
            connector: "databricks".to_string(),
            connector_component: ConnectorComponent::from(catalog),
        })?;

        let mode = self.params.get("mode").expose().ok();
        let (table_creator, table_reference_creator) = if let Some("delta_lake") = mode {
            (
                Arc::new(DeltaTableFactory::new(params.to_secret_map())) as Arc<dyn Read>,
                table_reference_creator_delta_lake as fn(&UCTable) -> Option<TableReference>,
            )
        } else {
            let dataset_databricks =
                match DatabricksDataConnector::new(params, runtime.token_provider_registry())
                    .await
                    .map_err(|source| super::Error::UnableToGetCatalogProvider {
                        connector: "databricks".to_string(),
                        source: source.into(),
                        connector_component: ConnectorComponent::from(catalog),
                    }) {
                    Ok(dataset_databricks) => dataset_databricks,
                    Err(e) => return Err(e),
                };

            (
                dataset_databricks.read_provider(),
                table_reference_creator_spark as fn(&UCTable) -> Option<TableReference>,
            )
        };

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            CatalogId(catalog_id),
            table_creator,
            table_reference_creator,
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

#[allow(clippy::unnecessary_wraps)]
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
