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
use crate::component::catalog::Catalog;
use crate::dataconnector::parameters::ConnectorParams;
use async_trait::async_trait;
use data_components::Read;
use data_components::RefreshableCatalogProvider;
use data_components::delta_lake::DeltaTableFactory;
use data_components::unity_catalog::UCTable;
use data_components::unity_catalog::UnityCatalog as UnityCatalogClient;
use data_components::unity_catalog::credential_vending::VendedDeltaTableFactory;
use data_components::unity_catalog::provider::{
    ReadTableProviderFactory, UCTableProviderFactory, UnityCatalogProvider,
};
use datafusion::sql::TableReference;
use runtime_secrets::get_params_with_secrets;
use secrecy::SecretString;
use snafu::ResultExt;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use token_provider::{StaticTokenProvider, TokenProvider};

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

pub const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("token").secret().description(
        "The personal access token used to authenticate against the Unity Catalog API.",
    ),
    ParameterSpec::component("credential_vending").description(
        "When set to 'enabled', short-lived storage credentials for each table are fetched from the Unity Catalog credential vending API instead of using static storage credentials. Defaults to 'disabled'.",
    ),
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
        .description("Enables insecure HTTP connections to the AWS endpoint. Defaults to false."),
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
impl CatalogConnector for UnityCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn RefreshableCatalogProvider>> {
        let Some(catalog_id) = catalog.catalog_id.clone() else {
            return Err(
                super::Error::InvalidConfigurationNoSource {
                    connector: "unity_catalog".into(),
                    message: "A Catalog Path is required for Unity Catalog. For details, visit: https://spiceai.org/docs/components/catalogs/unity-catalog#from".into(),
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

        let token_provider = self.params.get("token").ok().map(|token| {
            Arc::new(StaticTokenProvider::new(token.clone())) as Arc<dyn TokenProvider>
        });

        let client = UnityCatalogClient::new(endpoint, token_provider, None).map_err(|source| {
            super::Error::InternalWithSource {
                connector: "unity_catalog".to_string(),
                connector_component: ConnectorComponent::from(catalog),
                source: Box::new(source),
            }
        })?;
        let client = Arc::new(client);

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

        let credential_vending = match params.get("credential_vending").expose().ok() {
            Some("enabled") => true,
            None | Some("disabled") => false,
            Some(other) => {
                return Err(super::Error::InvalidConfigurationNoSource {
                    connector: "unity_catalog".into(),
                    message: format!(
                        "Invalid value '{other}' for 'unity_catalog_credential_vending'. Valid values: 'enabled', 'disabled'."
                    ),
                    connector_component: ConnectorComponent::from(catalog),
                });
            }
        };

        let table_creator: Arc<dyn UCTableProviderFactory> = if credential_vending {
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
                table_reference_creator,
            ))
        };

        let catalog_provider = match UnityCatalogProvider::try_new(
            client,
            catalog_id,
            table_creator,
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
                });
            }
        };

        Ok(Arc::new(catalog_provider) as Arc<dyn RefreshableCatalogProvider>)
    }
}

fn table_reference_creator(uc_table: &UCTable) -> Option<TableReference> {
    let storage_location = uc_table.storage_location.as_deref()?;
    // Don't append a trailing slash here — `DeltaTable::from` calls
    // `ensure_folder_location` which already adds one when needed.
    // Unconditionally appending caused double-slash paths (e.g.
    // "file:///path/to/table//") when the Unity Catalog API returned
    // locations that already ended with '/'.
    Some(TableReference::bare(storage_location.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_table_reference_creator_with_storage_location() {
        let table = make_uc_table(Some("s3://my-bucket/warehouse/table"));
        let reference = table_reference_creator(&table)
            .expect("should return Some when storage_location is present");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(table.as_ref(), "s3://my-bucket/warehouse/table");
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_without_storage_location() {
        let table = make_uc_table(None);
        assert!(
            table_reference_creator(&table).is_none(),
            "should return None when storage_location is None"
        );
    }

    #[test]
    fn test_table_reference_creator_preserves_location_as_is() {
        let table = make_uc_table(Some("gs://bucket/path"));
        let reference = table_reference_creator(&table).expect("should return Some");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(
                    table.as_ref(),
                    "gs://bucket/path",
                    "reference should preserve storage location without modification"
                );
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    #[test]
    fn test_table_reference_creator_preserves_full_uri() {
        let table = make_uc_table(Some(
            "abfss://container@account.dfs.core.windows.net/warehouse/table",
        ));
        let reference = table_reference_creator(&table).expect("should return Some for abfss URI");
        assert!(
            matches!(reference, TableReference::Bare { .. }),
            "Expected Bare table reference"
        );
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(
                    table.as_ref(),
                    "abfss://container@account.dfs.core.windows.net/warehouse/table"
                );
            }
            _ => unreachable!("already asserted to be Bare table reference"),
        }
    }

    /// Regression test for <https://github.com/spiceai/spiceai/issues/7904>
    /// Unity Catalog API returns `storage_location` with a trailing slash.
    /// Previously, `table_reference_creator` unconditionally appended another
    /// slash, creating a double-slash path like `file:///path/to/table//`
    /// which caused Delta Lake to fail with "Path does not exist".
    #[test]
    fn test_table_reference_creator_no_double_slash_when_location_ends_with_slash() {
        let table = make_uc_table(Some(
            "file:///home/unitycatalog/etc/data/managed/unity/default/tables/marksheet/",
        ));
        let reference = table_reference_creator(&table).expect("should return Some");
        match reference {
            TableReference::Bare { table } => {
                assert!(
                    !table.as_ref().ends_with("//"),
                    "reference must not end with double slash, got: {}",
                    table.as_ref()
                );
                assert_eq!(
                    table.as_ref(),
                    "file:///home/unitycatalog/etc/data/managed/unity/default/tables/marksheet/"
                );
            }
            _ => panic!("Expected Bare table reference"),
        }
    }

    /// Edge case: `storage_location` pointing to a bucket root with no key/path.
    #[test]
    fn test_table_reference_creator_bucket_root() {
        let table = make_uc_table(Some("s3://bucket"));
        let reference = table_reference_creator(&table).expect("should return Some");
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(table.as_ref(), "s3://bucket");
            }
            _ => panic!("Expected Bare table reference"),
        }
    }

    /// Edge case: `storage_location` with `file://` scheme for local paths.
    #[test]
    fn test_table_reference_creator_file_scheme() {
        let table = make_uc_table(Some("file:///tmp/marksheet_uniform/"));
        let reference = table_reference_creator(&table).expect("should return Some");
        match reference {
            TableReference::Bare { table } => {
                assert_eq!(table.as_ref(), "file:///tmp/marksheet_uniform/");
                assert!(
                    !table.as_ref().ends_with("//"),
                    "must not produce double trailing slash"
                );
            }
            _ => panic!("Expected Bare table reference"),
        }
    }
}
