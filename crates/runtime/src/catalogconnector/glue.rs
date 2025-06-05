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
use snafu::prelude::*;
use std::any::Any;
use std::sync::{Arc, LazyLock};

mod provider;
mod state;

use provider::GlueCatalogProvider;

pub static PREFIX: &str = "glue";

static VALIDATORS: LazyLock<
    Vec<Box<dyn Validator<Error = parameters::aws::Error> + Send + Sync + 'static>>,
> = LazyLock::new(|| vec![Box::new(RegionValidator), Box::new(AuthValidator)]);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to get Glue databases: {source}"))]
    GetDatabases { source: SdkError<GetDatabasesError> },

    #[snafu(display("Failed to get Glue table from database `{database}`: {source}"))]
    GetTables {
        database: String,
        source: SdkError<GetTablesError>,
    },

    #[snafu(display("Failed to build FileIO: {source}"))]
    BuildFileIO { source: iceberg::Error },

    #[snafu(display("Failed to create file input for metadata location '{location}': {source}",))]
    CreateFileInput {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display("Failed to read metadata from '{location}': {source}"))]
    ReadMetadata {
        source: iceberg::Error,
        location: String,
    },

    #[snafu(display("Failed to deserialize metadata: {source}"))]
    DeserializeMetadata { source: serde_json::Error },

    #[snafu(display("Failed to build Iceberg table: {source}"))]
    BuildIcebergTable { source: iceberg::Error },

    #[snafu(display("Failed to create Iceberg table provider: {source}"))]
    CreateIcebergTableProvider { source: iceberg::Error },

    #[snafu(display("No 'metadata_location' set on table '{table}'"))]
    MissingMetadataLocation { table: String },

    #[snafu(display("No 'parameters' set on table"))]
    MissingParameters,

    #[snafu(display("Parameter validation failed: {source}",))]
    ParameterValidation {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display("Configuration loading failed: {source}"))]
    ConfigurationLoadingFailed {
        #[snafu(source)]
        source: parameters::aws::Error,
    },

    #[snafu(display("Failed to refresh catalog: {source}"))]
    RefreshFailed {
        #[snafu(source)]
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
        Ok(Arc::new(
            GlueCatalogProvider::new(self.params.clone(), catalog, runtime)
                .await
                .map_err(|e| super::Error::UnableToGetCatalogProvider {
                    connector: PREFIX.to_string(),
                    connector_component: ConnectorComponent::from(catalog),
                    source: Box::new(e),
                })?,
        ))
    }
}
