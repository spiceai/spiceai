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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

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
    schemas: Arc<RwLock<HashMap<String, Arc<dyn SchemaProvider>>>>,

    pub client: Arc<dyn Catalog>,

    /// The root namespace to list namespaces from
    root_namespace: Option<NamespaceIdent>,

    /// The glob patterns for filtering tables
    includes: Option<GlobSet>,
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
        let includes_owned = includes.cloned();

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

        Ok(IcebergCatalogProvider {
            schemas: Arc::new(RwLock::new(schemas)),
            client,
            root_namespace,
            includes: includes_owned,
        })
    }
}

impl CatalogProvider for IcebergCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap_or_else(|e| e.into_inner());
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read().unwrap_or_else(|e| e.into_inner());
        schemas.get(name).cloned()
    }
}

#[async_trait]
impl RefreshableCatalogProvider for IcebergCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Create the semaphore for loading schemas
        let load_semaphore = Arc::new(Semaphore::new(10));

        // Get the current list of namespaces from the catalog
        let schema_names: Vec<_> = match self
            .client
            .list_namespaces(self.root_namespace.as_ref())
            .await
        {
            Ok(namespaces) => namespaces
                .iter()
                .flat_map(|ns| ns.as_ref().clone())
                .collect(),
            Err(e) => match e.kind() {
                iceberg::ErrorKind::DataInvalid => {
                    // Unfortunately, there isn't a better way to handle this
                    let err_msg = e.to_string();

                    if let Some(namespace) = &self.root_namespace {
                        if err_msg.contains("NoSuchNamespaceException")
                            || err_msg.contains("Namespace does not exist")
                        {
                            return Err(Box::new(Error::NamespaceDoesNotExist {
                                namespace: namespace.join("."),
                            }));
                        }
                    }

                    return Err(Box::new(handle_iceberg_error(e)));
                }
                _ => return Err(Box::new(handle_iceberg_error(e))),
            },
        };

        // Get read access to current schemas to determine what needs to be updated
        let (_current_schema_names, schemas_to_refresh_data) = {
            let current_schemas = self.schemas.read().unwrap_or_else(|e| e.into_inner());
            let current_schema_names: HashSet<String> = current_schemas.keys().cloned().collect();
            let new_schema_names: HashSet<String> = schema_names.iter().cloned().collect();

            // Determine which schemas to add and remove
            let schemas_to_add: Vec<String> = new_schema_names
                .difference(&current_schema_names)
                .cloned()
                .collect();
            let schemas_to_remove: Vec<String> = current_schema_names
                .difference(&new_schema_names)
                .cloned()
                .collect();
            let schemas_to_refresh: Vec<String> = current_schema_names
                .intersection(&new_schema_names)
                .cloned()
                .collect();

            // Collect the schema providers that need refreshing
            let schema_providers_to_refresh: Vec<(String, Arc<dyn SchemaProvider>)> =
                schemas_to_refresh
                    .iter()
                    .filter_map(|name| {
                        current_schemas
                            .get(name)
                            .map(|provider| (name.clone(), Arc::clone(provider)))
                    })
                    .collect();

            (
                current_schema_names,
                (
                    schemas_to_add,
                    schemas_to_remove,
                    schemas_to_refresh,
                    schema_providers_to_refresh,
                ),
            )
        };

        let (schemas_to_add, schemas_to_remove, schemas_to_refresh, schema_providers_to_refresh) =
            schemas_to_refresh_data;

        // If no changes are needed, return early
        if schemas_to_add.is_empty()
            && schemas_to_remove.is_empty()
            && schemas_to_refresh.is_empty()
        {
            return Ok(());
        }

        // Load new schema providers for schemas that need to be added
        let mut new_providers = Vec::new();
        if !schemas_to_add.is_empty() {
            let provider_futures: Vec<_> = schemas_to_add
                .iter()
                .map(|name| {
                    let semaphore_clone = Arc::clone(&load_semaphore);
                    IcebergSchemaProvider::try_new(
                        Arc::clone(&self.client),
                        NamespaceIdent::new(name.clone()),
                        semaphore_clone,
                        self.includes.as_ref(),
                    )
                })
                .collect();

            let providers = try_join_all(provider_futures)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

            new_providers = schemas_to_add
                .iter()
                .cloned()
                .zip(providers.into_iter())
                .map(|(name, provider)| {
                    let provider = Arc::new(provider) as Arc<dyn SchemaProvider>;
                    (name, provider)
                })
                .collect();
        }

        // Update the schemas map
        {
            let mut schemas_write = self.schemas.write().unwrap_or_else(|e| e.into_inner());

            // Remove schemas that no longer exist
            for schema_name in &schemas_to_remove {
                schemas_write.remove(schema_name);
            }

            // Add new schemas
            for (name, provider) in new_providers {
                schemas_write.insert(name, provider);
            }
        } // Drop the write lock here

        // Refresh existing schemas to pick up table changes
        for (schema_name, schema_provider) in schema_providers_to_refresh {
            if let Some(iceberg_schema) = schema_provider
                .as_any()
                .downcast_ref::<IcebergSchemaProvider>()
            {
                if let Err(e) = iceberg_schema.refresh().await {
                    tracing::warn!("Failed to refresh schema '{}': {}", schema_name, e);
                    // Continue with other schemas even if one fails
                }
            }
        }

        tracing::debug!(
            "Refreshed Iceberg catalog: added {} schemas, removed {} schemas, refreshed {} schemas",
            schemas_to_add.len(),
            schemas_to_remove.len(),
            schemas_to_refresh.len()
        );

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
    tables: Arc<RwLock<HashMap<String, Arc<dyn TableProvider>>>>,

    /// The Iceberg catalog client
    client: Arc<dyn Catalog>,

    /// The namespace this schema provider manages
    namespace: NamespaceIdent,

    /// The glob patterns for filtering tables
    includes: Option<GlobSet>,
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
        let includes_owned = include.cloned();

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

        Ok(IcebergSchemaProvider {
            tables: Arc::new(RwLock::new(tables)),
            client,
            namespace,
            includes: includes_owned,
        })
    }

    /// Refresh the schema provider by discovering new tables and removing deleted ones
    pub(crate) async fn refresh(&self) -> Result<()> {
        let load_semaphore = Arc::new(Semaphore::new(10));

        // Get the current list of tables from the catalog
        let table_names: Vec<_> = self
            .client
            .list_tables(&self.namespace)
            .await
            .map_err(handle_iceberg_error)?
            .into_iter()
            .filter(|table| {
                // If include is None, we include all tables
                if let Some(glob_set) = &self.includes {
                    // Check if the table name matches any of the glob patterns
                    glob_set.is_match(table.to_string())
                } else {
                    true // Include all tables if no glob patterns are specified
                }
            })
            .collect();

        // Get current tables to determine what needs to be updated
        let (tables_to_add, tables_to_remove) = {
            let current_tables = self.tables.read().unwrap_or_else(|e| e.into_inner());
            let current_table_names: HashSet<String> = current_tables.keys().cloned().collect();
            let new_table_names: HashSet<String> =
                table_names.iter().map(|t| t.name().to_string()).collect();

            // Determine which tables to add and remove
            let tables_to_add: Vec<TableIdent> = table_names
                .into_iter()
                .filter(|table| !current_table_names.contains(table.name()))
                .collect();
            let tables_to_remove: Vec<String> = current_table_names
                .difference(&new_table_names)
                .cloned()
                .collect();

            (tables_to_add, tables_to_remove)
        };

        // If no changes are needed, return early
        if tables_to_add.is_empty() && tables_to_remove.is_empty() {
            return Ok(());
        }

        // Load new table providers for tables that need to be added
        let mut new_providers = Vec::new();
        if !tables_to_add.is_empty() {
            let table_futures: Vec<_> = tables_to_add
                .iter()
                .map(|name| {
                    let client_clone = Arc::clone(&self.client);
                    let name_clone = Arc::new(name.clone());
                    let semaphore_clone = Arc::clone(&load_semaphore);
                    async move {
                        Self::load_table(client_clone, Arc::clone(&name_clone), semaphore_clone)
                            .await
                            .map(|opt_provider| (name_clone, opt_provider))
                    }
                })
                .collect();

            let table_results = try_join_all(table_futures).await?;

            // Filter out None values, only keeping successful loads
            for (name, opt_provider) in table_results {
                if let Some(provider) = opt_provider {
                    new_providers.push((name.name().to_string(), provider));
                }
            }
        }

        // Update the tables map
        {
            let mut tables_write = self.tables.write().unwrap_or_else(|e| e.into_inner());

            // Remove tables that no longer exist
            for table_name in &tables_to_remove {
                tables_write.remove(table_name);
            }

            // Add new tables
            for (name, provider) in new_providers {
                tables_write.insert(name, provider);
            }
        } // Drop the write lock here

        tracing::debug!(
            "Refreshed Iceberg schema '{}': added {} tables, removed {} tables",
            self.namespace.join("."),
            tables_to_add.len(),
            tables_to_remove.len()
        );

        Ok(())
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
        let tables = self.tables.read().unwrap_or_else(|e| e.into_inner());
        tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap_or_else(|e| e.into_inner());
        tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let tables = self.tables.read().unwrap_or_else(|e| e.into_inner());
        Ok(tables.get(name).cloned())
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
