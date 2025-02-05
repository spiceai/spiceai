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

//! Implementation of the `DataFusion` Catalog/Schema providers for Spice.ai.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::sql::TableReference;
use futures::future::try_join_all;
use globset::GlobSet;
use iceberg::{Catalog, NamespaceIdent};
use snafu::prelude::*;

use crate::{Read, RefreshableCatalogProvider};

use crate::iceberg::catalog::RestCatalog;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to load the Spice.ai table '{table}'.\n{source}\nReport an issue on GitHub: https://github.com/spiceai/spiceai/issues"))]
    TableProviderCreation {
        table: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to list namespaces for the Spice Cloud Catalog.\n{source}\nReport an issue on GitHub: https://github.com/spiceai/spiceai/issues"))]
    ListNamespaces { source: iceberg::Error },

    #[snafu(display("Failed to list tables for the Spice Cloud Catalog.\n{source}\nReport an issue on GitHub: https://github.com/spiceai/spiceai/issues"))]
    ListTables { source: iceberg::Error },

    #[snafu(display("Failed to load the table '{table}'.\n{source}\nReport an issue on GitHub: https://github.com/spiceai/spiceai/issues"))]
    LoadTable {
        source: iceberg::Error,
        table: String,
    },

    #[snafu(display("Failed to find a schema for the table '{table}'.\nVerify the table exists in Spice Cloud, and try again."))]
    NoSchemaFound { table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Provides an interface to manage and access multiple schemas
/// within an Iceberg [`Catalog`].
///
/// Acts as a centralized catalog provider that aggregates
/// multiple [`SchemaProvider`], each associated with distinct namespaces.
#[derive(Debug)]
pub struct SpiceCloudPlatformCatalogProvider {
    /// A `HashMap` where keys are namespace names
    /// and values are dynamic references to objects implementing the
    /// [`SchemaProvider`] trait.
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl SpiceCloudPlatformCatalogProvider {
    /// Asynchronously tries to construct a new [`SpiceCloudPlatformCatalogProvider`]
    /// using the given client to fetch and initialize schema providers for
    /// each namespace in the Spice.ai [`Catalog`].
    ///
    /// This method retrieves the list of namespace names
    /// attempts to create a schema provider for each namespace, and
    /// collects these providers into a `HashMap`.
    pub async fn try_new(
        client: Arc<RestCatalog>,
        root_namespace: NamespaceIdent,
        connector: Arc<dyn Read>,
        include: Option<GlobSet>,
    ) -> Result<Self> {
        let schema_names: Vec<_> = client
            .list_namespaces(Some(&root_namespace))
            .await
            .context(ListNamespacesSnafu)?;

        let include = include.map(Arc::new);

        let providers = try_join_all(
            schema_names
                .iter()
                .map(|name| {
                    let mut child_namespace_vec = root_namespace.clone().inner();
                    let name_inner = name.clone().inner();
                    let Some(last_name) = name_inner.last() else {
                        unreachable!("The namespace should have at least one element");
                    };
                    child_namespace_vec.push(last_name.to_string());
                    let Ok(child_namespace) = NamespaceIdent::from_vec(child_namespace_vec) else {
                        unreachable!("This only panics if the vec is empty");
                    };
                    tracing::debug!(
                        "Creating Spice.ai schema provider for namespace: {:?}",
                        child_namespace
                    );
                    SpiceCloudPlatformSchemaProvider::try_new(
                        Arc::clone(&client),
                        child_namespace,
                        Arc::clone(&connector),
                        include.clone(),
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
                let name_inner = name.inner();
                let Some(last_name) = name_inner.last() else {
                    unreachable!("The namespace should have at least one element");
                };
                (last_name.to_string(), provider)
            })
            .collect();

        Ok(SpiceCloudPlatformCatalogProvider { schemas })
    }
}

impl CatalogProvider for SpiceCloudPlatformCatalogProvider {
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
impl RefreshableCatalogProvider for SpiceCloudPlatformCatalogProvider {
    async fn refresh(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Will be implemented in a future enhancement.
        Ok(())
    }
}

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
pub(crate) struct SpiceCloudPlatformSchemaProvider {
    /// A `HashMap` where keys are table names
    /// and values are dynamic references to objects implementing the
    /// [`TableProvider`] trait.
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for SpiceCloudPlatformSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpiceCloudPlatformSchemaProvider")
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl SpiceCloudPlatformSchemaProvider {
    /// Asynchronously tries to construct a new [`SpiceCloudPlatformSchemaProvider`]
    /// using the given client to fetch and initialize table providers for
    /// the provided namespace in the Spice.ai [`Catalog`].
    ///
    /// This method retrieves a list of table names
    /// attempts to create a table provider for each table name, and
    /// collects these providers into a `HashMap`.
    pub(crate) async fn try_new(
        client: Arc<RestCatalog>,
        namespace: NamespaceIdent,
        connector: Arc<dyn Read>,
        include: Option<Arc<GlobSet>>,
    ) -> Result<Self> {
        let table_names: Vec<_> = client
            .list_tables(&namespace)
            .await
            .context(ListTablesSnafu)?;

        let included_table_names = table_names
            .clone()
            .into_iter()
            .filter_map(|ref table_name| {
                let (table_reference, schema_and_table) =
                    match table_name.namespace().clone().inner().as_slice() {
                        [.., catalog, schema] => (
                            TableReference::full(
                                Arc::from(catalog.as_str()),
                                Arc::from(schema.as_str()),
                                Arc::from(table_name.name()),
                            ),
                            format!("{}.{}", schema, table_name.name()),
                        ),
                        [schema] => (
                            TableReference::partial(
                                Arc::from(schema.as_str()),
                                Arc::from(table_name.name()),
                            ),
                            format!("{}.{}", schema, table_name.name()),
                        ),
                        [] => (
                            TableReference::bare(Arc::from(table_name.name())),
                            table_name.name().to_string(),
                        ),
                    };
                if let Some(include) = &include {
                    if !include.is_match(schema_and_table) {
                        tracing::debug!("Table {} is not included", table_reference);
                        return None;
                    }
                }
                Some(table_reference)
            })
            .collect::<Vec<_>>();

        let table_providers = try_join_all(
            included_table_names
                .iter()
                .map(|name| {
                    let connector = Arc::clone(&connector);
                    async move {
                        match connector.table_provider(name.clone(), None).await {
                            Ok(provider) => Ok(provider),
                            Err(e) => Err(Error::TableProviderCreation {
                                table: name.to_string(),
                                source: e,
                            }),
                        }
                    }
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        let tables: HashMap<String, Arc<dyn TableProvider>> = included_table_names
            .into_iter()
            .zip(table_providers.into_iter())
            .map(|(name, provider)| (name.table().to_string(), provider))
            .collect();

        Ok(SpiceCloudPlatformSchemaProvider { tables })
    }
}

#[async_trait]
impl SchemaProvider for SpiceCloudPlatformSchemaProvider {
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
