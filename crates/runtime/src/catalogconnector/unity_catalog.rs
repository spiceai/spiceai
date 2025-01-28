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
use crate::component::catalog::Catalog;
use crate::dataconnector::ConnectorParams;
use crate::get_params_with_secrets;
use crate::Runtime;
use async_trait::async_trait;
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::provider::UnityCatalogProvider;
use data_components::unity_catalog::UCTable;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use data_components::Read;
use data_components::RefreshableCatalogProvider;
use datafusion::sql::TableReference;
use secrecy::SecretString;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct UnityCatalog {
    params: Parameters,
}

impl UnityCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self {
            params: params.parameters,
        })
    }
}

pub(crate) const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("token").secret().description(
        "The personal access token used to authenticate against the Unity Catalog API.",
    ),
    // S3 storage options
    ParameterSpec::connector("aws_region")
        .description("The AWS region to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_access_key_id")
        .description("The AWS access key ID to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_secret_access_key")
        .description("The AWS secret access key to use for S3 storage.")
        .secret(),
    ParameterSpec::connector("aws_endpoint")
        .description("The AWS endpoint to use for S3 storage.")
        .secret(),
    // Azure storage options
    ParameterSpec::connector("azure_storage_account_name")
        .description("The storage account to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_account_key")
        .description("The storage account key to use for Azure storage.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_id")
        .description("The service principal client id for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_client_secret")
        .description("The service principal client secret for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_sas_key")
        .description("The shared access signature key for accessing the storage account.")
        .secret(),
    ParameterSpec::connector("azure_storage_endpoint")
        .description("The endpoint for the Azure Blob storage account.")
        .secret(),
    // GCS storage options
    ParameterSpec::connector("google_service_account")
        .description("Filesystem path to the Google service account JSON key file.")
        .secret(),
];

#[async_trait]
impl CatalogConnector for UnityCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "unity_catalog".into(),
                    message: "A Catalog Path is required for Unity Catalog.\nFor details, visit: https://spiceai.org/docs/components/catalogs/unity-catalog#from".into(),
                    connector_component: ConnectorComponent::from(catalog),
                },
            );
        };

        // The catalog_id for the unity_catalog provider is the full URL to the catalog like:
        // https://<host>/api/2.1/unity-catalog/catalogs/<catalog_id>
        let (endpoint, catalog_id) = match UnityCatalogClient::parse_catalog_url(&catalog_id)
            .map_err(|e| super::Error::InvalidConfiguration {
                connector: "unity_catalog".to_string(),
                connector_component: ConnectorComponent::from(catalog),
                message: e.to_string(),
                source: Box::new(e),
            }) {
            Ok((endpoint, catalog_id)) => (endpoint, catalog_id),
            Err(e) => return Err(e),
        };

        let client = Arc::new(UnityCatalogClient::new(
            endpoint,
            self.params.get("token").ok().cloned(),
        ));

        // Copy the catalog params into the dataset params, and allow user to override
        let mut dataset_params: HashMap<String, SecretString> =
            get_params_with_secrets(runtime.secrets(), &catalog.params).await;

        let secret_dataset_params =
            get_params_with_secrets(runtime.secrets(), &catalog.dataset_params).await;

        for (key, value) in secret_dataset_params {
            dataset_params.insert(key, value);
        }

        let params = Parameters::try_new(
            "connector unity catalog",
            dataset_params.into_iter().collect(),
            "unity_catalog",
            runtime.secrets(),
            PARAMETERS,
        )
        .await
        .context(super::InternalWithSourceSnafu {
            connector: "unity_catalog".to_string(),
            connector_component: ConnectorComponent::from(catalog),
        })?;

        let delta_table_creator =
            Arc::new(DeltaTableFactory::new(params.to_secret_map())) as Arc<dyn Read>;

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            catalog_id,
            delta_table_creator,
            table_reference_creator,
            catalog.include.clone(),
        )
        .await
        {
            Ok(provider) => provider,
            Err(e) => {
                return Err(super::Error::UnableToGetCatalogProvider {
                    connector: "unity_catalog".to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })
            }
        };

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
}

fn table_reference_creator(uc_table: &UCTable) -> Option<TableReference> {
    let storage_location = uc_table.storage_location.as_deref()?;
    Some(TableReference::bare(format!("{storage_location}/")))
}
