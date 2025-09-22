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
use globset::GlobSet;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use tokio::sync::Semaphore;

use crate::RefreshableCatalogProvider;
use crate::iceberg::catalog::Error;

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
        client: Arc<dyn Catalog>,
        root_namespace: Option<NamespaceIdent>,
        includes: Option<&GlobSet>,
    ) -> Result<Self> {
        // Create the semaphore first, so we can use it in the closures below
        let load_semaphore = Arc::new(Semaphore::new(10));

        let schema_names: Vec<_> = match client.list_namespaces(root_namespace.as_ref()).await {
            Ok(namespaces) => namespaces
                .iter()
                .flat_map(|ns| ns.as_ref().clone())
                .collect(),
            Err(e) => match e.kind() {
                iceberg::ErrorKind::DataInvalid => {
                    // Unfortunately, there isn't a better way to handle this
                    let err_msg = e.to_string();

                    if let Some(namespace) = root_namespace
                        && (err_msg.contains("NoSuchNamespaceException")
                            || err_msg.contains("Namespace does not exist"))
                    {
                        return Err(Error::NamespaceDoesNotExist {
                            namespace: namespace.join("."),
                        });
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
                    let semaphore_clone = Arc::clone(&load_semaphore);
                    IcebergSchemaProvider::try_new(
                        Arc::clone(&client),
                        NamespaceIdent::new(name.clone()),
                        semaphore_clone,
                        includes,
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
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        load_semaphore: Arc<Semaphore>,
        include: Option<&GlobSet>,
    ) -> Result<Self> {
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await
            .map_err(handle_iceberg_error)?
            .into_iter()
            .filter(|table| {
                // If include is None, we include all tables
                if let Some(glob_set) = &include {
                    // Check if the table name matches any of the glob patterns
                    glob_set.is_match(table.to_string())
                } else {
                    true // Include all tables if no glob patterns are specified
                }
            })
            .collect();

        // Transform each load_table call to return Result<(TableIdent, Option<Arc<dyn TableProvider>>)>
        let table_futures: Vec<_> = table_names
            .iter()
            .map(|name| {
                let client_clone = Arc::clone(&client);
                let name_clone = Arc::new(name.clone());
                let semaphore_clone = Arc::clone(&load_semaphore);
                async move {
                    // Map the inner Result to include the table name
                    Self::load_table(client_clone, Arc::clone(&name_clone), semaphore_clone)
                        .await
                        .map(|opt_provider| (name_clone, opt_provider))
                }
            })
            .collect();

        // Execute all futures in parallel, short-circuiting on first error
        let table_results = try_join_all(table_futures).await?;

        // Filter out None values, only keeping successful loads
        let mut tables = HashMap::new();
        for (name, opt_provider) in table_results {
            if let Some(provider) = opt_provider {
                tables.insert(name.name().to_string(), provider);
            }
        }

        Ok(IcebergSchemaProvider { tables })
    }

    async fn load_table(
        client: Arc<dyn Catalog>,
        table_name: Arc<TableIdent>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        // Acquire a permit from the semaphore to limit concurrent table loads
        let _permit = semaphore
            .acquire()
            .await
            .map_err(|e| Error::SemaphoreError { source: e })?;

        match client.load_table(&table_name).await {
            Ok(_table) => {
                match IcebergTableProvider::try_new(client, Arc::unwrap_or_clone(table_name)).await
                {
                    Ok(provider) => Ok(Some(Arc::new(provider) as Arc<dyn TableProvider>)),
                    Err(e) => Err(handle_iceberg_error(e)),
                }
            }
            Err(e) => {
                // If the table doesn't exist, return None instead of an error
                let err_msg = e.to_string();
                if err_msg.contains("NoSuchIcebergTableException") || err_msg.contains("code: 404")
                {
                    tracing::warn!(
                        "Failed to load '{}.{}' as an Iceberg table: table may not exist or is not in Iceberg format.",
                        table_name.namespace().join("."),
                        table_name.name()
                    );
                    Ok(None)
                } else {
                    Err(handle_iceberg_error(e))
                }
            }
        }
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
            let err_in_detail = format!("{e:?}");
            let err_in_detail_lc = err_in_detail.to_lowercase();
            if err_msg.contains("error sending request for url") {
                // Extract the URL from the error message
                let url = err_msg
                    .split("error sending request for url")
                    .nth(1)
                    .unwrap_or_default()
                    .trim();

                // Special case for detailed certificate errors
                if err_in_detail_lc.contains("certificate")
                    || err_in_detail_lc.contains("tls")
                    || err_in_detail_lc.contains("ssl")
                {
                    return Error::CertificateError {
                        url: url.to_string(),
                        detail: err_in_detail,
                        source: e,
                    };
                }

                // Return a generic connection error for all other cases
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
