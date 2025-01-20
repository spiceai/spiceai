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

//! Implementation of the `DataFusion` Catalog/Schema providers for Iceberg.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent};
use iceberg_datafusion::IcebergTableProvider;
use snafu::prelude::*;

use crate::RefreshableCatalogProvider;

use super::catalog::RestCatalog;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "An unknown error occurred while interacting with the Iceberg catalog.\nReport an issue at https://github.com/spiceai/spiceai/issues\n{source}"
    ))]
    Unknown { source: iceberg::Error },

    #[snafu(display("The data in the Iceberg table is invalid. The table may be corrupted or incomplete.\n{source}"))]
    DataInvalid { source: iceberg::Error },

    #[snafu(display("This Iceberg feature is not yet supported.\nReport an issue at https://github.com/spiceai/spiceai/issues\n{source}"))]
    FeatureUnsupported { source: iceberg::Error },

    #[snafu(display("The namespace '{namespace}' does not exist in the Iceberg catalog, verify the namespace name and try again."))]
    NamespaceDoesNotExist { namespace: String },

    #[snafu(display("Failed to connect to the Iceberg catalog or object store at {url}, verify the Iceberg catalog is accessible and try again."))]
    FailedToConnect { url: String, source: iceberg::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Provides an interface to manage and access multiple schemas
/// within an Iceberg [`Catalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
#[derive(Debug)]
pub struct IcebergCatalogProvider {
    /// A `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl IcebergCatalogProvider {
    /// Asynchronously tries to construct a new [`IcebergCatalogProvider`]
    /// using the given client to fetch and initialize schema providers for
    /// each namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves the list of namespace names
    /// attempts to create a schema provider for each namespace, and
    /// collects these providers into a `HashMap`.
    pub async fn try_new(
        client: Arc<RestCatalog>,
        root_namespace: Option<NamespaceIdent>,
    ) -> Result<Self> {
        let schema_names: Vec<_> = match client.list_namespaces(root_namespace.as_ref()).await {
            Ok(namespaces) => namespaces
                .iter()
                .flat_map(|ns| ns.as_ref().clone())
                .collect(),
            Err(e) => match e.kind() {
                iceberg::ErrorKind::DataInvalid => {
                    // Unfortunately, there isn't a better way to handle this
                    let err_msg = e.to_string();

                    if let Some(namespace) = root_namespace {
                        if err_msg.contains("NoSuchNamespaceException")
                            || err_msg.contains("Namespace does not exist")
                        {
                            return Err(Error::NamespaceDoesNotExist {
                                namespace: namespace.join("."),
                            });
                        }
                    }

                    return Err(handle_iceberg_error(e));
                }
                _ => return Err(handle_iceberg_error(e)),
            },
        };

        let providers = try_join_all(
            schema_names
                .iter()
                .map(|name| {
                    IcebergSchemaProvider::try_new(
                        Arc::clone(&client),
                        NamespaceIdent::new(name.clone()),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let schemas: HashMap<String, Arc<dyn SchemaProvider>> = schema_names
            .into_iter()
            .zip(providers.into_iter())
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;
                (name, provider)
            })
            .collect();

        Ok(IcebergCatalogProvider { schemas })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}

#[async_trait]
impl RefreshableCatalogProvider for IcebergCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Will be implemented in a future enhancement.
        Ok(())
    }
}

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// A `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Iceberg [`Catalog`].
    ///
    /// This method retrieves a list of table names
    /// attempts to create a table provider for each table name, and
    /// collects these providers into a `HashMap`.
    pub(crate) async fn try_new(
        client: Arc<RestCatalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await
            .map_err(handle_iceberg_error)?;

        let iceberg_tables = try_join_all(
            table_names
                .iter()
                .map(|name| client.load_table(name))
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(handle_iceberg_error)?;

        let table_providers: Vec<_> = try_join_all(
            iceberg_tables
                .into_iter()
                .map(IcebergTableProvider::try_new_from_table)
                .collect::<Vec<_>>(),
        )
        .await
        .map_err(handle_iceberg_error)?;

        let tables: HashMap<String, Arc<dyn TableProvider>> = table_names
            .into_iter()
            .zip(table_providers.into_iter())
            .map(|(name, provider)| {
                let provider = Arc::new(provider) as Arc<dyn TableProvider>;
                (name.name().to_string(), provider)
            })
            .collect();

        Ok(IcebergSchemaProvider { tables })
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }
}

fn handle_iceberg_error(e: iceberg::Error) -> Error {
    match e.kind() {
        iceberg::ErrorKind::DataInvalid => Error::DataInvalid { source: e },
        iceberg::ErrorKind::FeatureUnsupported => Error::FeatureUnsupported { source: e },
        iceberg::ErrorKind::Unexpected => {
            // This is also returned when we cannot connect to the Iceberg catalog, so check for that.
            // i.e. Unexpected => Failed to execute http request, source: error sending request for url (http://localhoster:8181/v1/config)
            let err_msg = e.to_string();
            if err_msg.contains("error sending request for url") {
                // Extract the URL from the error message
                let url = err_msg
                    .split("error sending request for url")
                    .nth(1)
                    .unwrap_or_default()
                    .trim();
                return Error::FailedToConnect {
                    url: url.to_string(),
                    source: e,
                };
            }

            Error::Unknown { source: e }
        }
        _ => Error::Unknown { source: e },
    }
}
