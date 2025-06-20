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
use crate::{
    Runtime,
    component::catalog::Catalog,
    dataconnector::{
        ConnectorComponent,
        parameters::{
            self, ConnectorParams, Validator,
            aws::{AuthValidator, RegionValidator},
        },
    },
};
use async_trait::async_trait;
use aws_sdk_glue::{
    error::SdkError,
    operation::{get_databases::GetDatabasesError, get_tables::GetTablesError},
};
use data_components::RefreshableCatalogProvider as _;
use snafu::prelude::*;
use std::any::Any;
use std::sync::{Arc, LazyLock};

mod provider;

use provider::GlueCatalogProvider;

pub static PREFIX: &str = "glue";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| vec![Box::new(RegionValidator), Box::new(AuthValidator)]);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Cannot connect to AWS Glue to retrieve databases.\nVerify your AWS credentials and region are configured correctly.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue \n{source}"
    ))]
    GetDatabases { source: SdkError<GetDatabasesError> },

    #[snafu(display(
        "Cannot retrieve tables from Glue database '{database}'.\nVerify the database exists and you have permissions to access it.\n{source}"
    ))]
    GetTables {
        database: String,
        source: SdkError<GetTablesError>,
    },

    #[snafu(display(
        "Cannot access Iceberg table storage.\nVerify your S3 credentials and bucket permissions are configured correctly.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue \n{source}"
    ))]
    BuildFileIO { source: iceberg::Error },

    #[snafu(display(
        "Cannot read Iceberg metadata from location '{location}'.\nVerify the metadata file exists and is accessible.\n{source}",
    ))]
    CreateFileInput {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display(
        "Cannot read Iceberg table metadata from '{location}'.\nVerify the metadata file exists and is not corrupted.\n{source}"
    ))]
    ReadMetadata {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display(
        "Cannot parse Iceberg table metadata.\nThe metadata file may be corrupted or in an unsupported format.\n{source}"
    ))]
    DeserializeMetadata { source: serde_json::Error },

    #[snafu(display(
        "Cannot initialize Iceberg table.\nVerify the table metadata and schema are valid.\n{source}"
    ))]
    BuildIcebergTable { source: iceberg::Error },

    #[snafu(display(
        "Cannot create table provider for Iceberg table.\nVerify the table configuration and permissions.\n{source}"
    ))]
    CreateIcebergTableProvider { source: iceberg::Error },

    #[snafu(display(
        "Cannot find metadata location for Iceberg table '{table}'.\nEnsure the table has a 'metadata_location' parameter in AWS Glue."
    ))]
    MissingMetadataLocation { table: String },

    #[snafu(display(
        "Cannot find table parameters in AWS Glue.\nEnsure the table is properly configured with required parameters."
    ))]
    MissingParameters,

    #[snafu(display(
        "Invalid AWS configuration for Glue catalog.\nVerify your region, credentials, and other AWS parameters are correct.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue \n{source}",
    ))]
    ParameterValidation {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display(
        "Cannot load AWS configuration for Glue catalog.\nVerify your AWS credentials and region settings.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue \n{source}"
    ))]
    ConfigurationLoadingFailed {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display(
        "Cannot create dataset for table `{dataset}`.\nVerify the table configuration and format are supported.\nFor help with AWS Glue configuration, visit: https://docs.spiceai.org/components/catalogs/glue \n{source}"
    ))]
    CreatingDataset {
        dataset: String,
        source: Box<dyn std::error::Error + Sync + Send>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

type DatabaseName = String;

/// A catalog connector for AWS Glue, providing access to database and table metadata.
#[derive(Clone)]
pub struct GlueCatalog {
    params: ConnectorParams,
}

impl GlueCatalog {
    #[must_use]
    pub fn new_connector(params: ConnectorParams) -> Arc<dyn CatalogConnector> {
        Arc::new(Self { params })
    }
}

#[async_trait]
impl CatalogConnector for GlueCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn refreshable_catalog_provider(
        self: Arc<Self>,
        runtime: Arc<Runtime>,
        catalog: &Catalog,
    ) -> super::Result<Arc<dyn data_components::RefreshableCatalogProvider>> {
        let app = match runtime.app.read().await.as_ref() {
            Some(app) => Arc::clone(app),
            None => {
                return Err(super::Error::FailedToGetAppFromRuntime {});
            }
        };

        let refreshable_provider = Arc::new(
            GlueCatalogProvider::new(self.params.clone(), catalog, runtime, app)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        );

        refreshable_provider.refresh().await.map_err(|source| {
            super::Error::UnableToGetCatalogProvider {
                connector: PREFIX.to_string(),
                connector_component: ConnectorComponent::from(catalog),
                source,
            }
        })?;

        Ok(refreshable_provider)
    }
}
