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

use async_trait::async_trait;
use datafusion::{
    catalog::{CatalogProvider, SchemaProvider},
    datasource::TableProvider,
    error::DataFusionError,
    sql::TableReference,
};
use futures::{StreamExt, TryStreamExt};
use globset::GlobSet;
use snafu::prelude::*;
use std::{
    collections::HashMap,
    fmt::Write,
    sync::{Arc, RwLock},
};

use crate::{Read, RefreshableCatalogProvider};

use super::{CatalogId, Result, UCSchema, UCTable, UnityCatalog};

/// Creates `DataFusion` table providers for Unity Catalog tables.
///
/// Unlike [`Read`], implementations receive the full [`UCTable`] so they can
/// use table metadata beyond the storage location — e.g. the `table_id`
/// needed for credential vending.
#[async_trait]
pub trait UCTableProviderFactory: Send + Sync {
    /// The reference used to construct and identify the table.
    ///
    /// Returns `None` when the table cannot be materialized (e.g. it has no
    /// storage location); such tables are skipped.
    fn table_reference(&self, table: &UCTable) -> Option<TableReference>;

    async fn table_provider(
        &self,
        table: &UCTable,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Adapts an `(Arc<dyn Read>, table_reference_creator)` pair to
/// [`UCTableProviderFactory`] for table creators that only need the table
/// reference.
pub struct ReadTableProviderFactory {
    read: Arc<dyn Read>,
    table_reference_creator: fn(&UCTable) -> Option<TableReference>,
}

impl ReadTableProviderFactory {
    pub fn new(
        read: Arc<dyn Read>,
        table_reference_creator: fn(&UCTable) -> Option<TableReference>,
    ) -> Self {
        Self {
            read,
            table_reference_creator,
        }
    }
}

#[async_trait]
impl UCTableProviderFactory for ReadTableProviderFactory {
    fn table_reference(&self, table: &UCTable) -> Option<TableReference> {
        (self.table_reference_creator)(table)
    }

    async fn table_provider(
        &self,
        _table: &UCTable,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        self.read.table_provider(table_reference).await
    }
}

#[derive(Debug)]
pub struct UnityCatalogProvider {
    schemas: HashMap<String, Arc<UnityCatalogSchemaProvider>>,
}

impl UnityCatalogProvider {
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_id: CatalogId,
        table_creator: Arc<dyn UCTableProviderFactory>,
        include: Option<GlobSet>,
    ) -> Result<Self> {
        let schemas =
            client
                .list_schemas(&catalog_id.0)
                .await?
                .context(super::CatalogDoesntExistSnafu {
                    catalog_id: catalog_id.0,
                })?;

        let include = include.map(Arc::new);

        let mut schemas_map = HashMap::new();
        for schema in schemas {
            if schema.name == "information_schema" {
                continue;
            }
            let schema_provider = UnityCatalogSchemaProvider::try_new(
                Arc::clone(&client),
                &schema,
                Arc::clone(&table_creator),
                include.clone(),
            )
            .await?;
            schemas_map.insert(schema.name, Arc::new(schema_provider));
        }
        Ok(Self {
            schemas: schemas_map,
        })
    }
}

impl CatalogProvider for UnityCatalogProvider {
    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(name)
            .cloned()
            .map(|s| s as Arc<dyn SchemaProvider>)
    }
}

#[async_trait]
impl RefreshableCatalogProvider for UnityCatalogProvider {
    async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let max_concurrent = 5;
        let futures = self
            .schemas
            .values()
            .map(Arc::clone)
            .map(|schema| async move { schema.refresh().await });

        futures::stream::iter(futures)
            .buffer_unordered(max_concurrent)
            .try_collect::<Vec<_>>()
            .await?;
        Ok(())
    }
}

pub struct UnityCatalogSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    client: Arc<UnityCatalog>,
    schema: UCSchema,
    include: Option<Arc<GlobSet>>,
    table_creator: Arc<dyn UCTableProviderFactory>,
}

impl std::fmt::Debug for UnityCatalogSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnityCatalogSchemaProvider")
            .field("schema", &self.schema)
            .field("tables", &self.tables)
            .finish_non_exhaustive()
    }
}

impl UnityCatalogSchemaProvider {
    /// Creates a new instance of the [`UnityCatalogSchemaProvider`].
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created.
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        schema: &UCSchema,
        table_creator: Arc<dyn UCTableProviderFactory>,
        include: Option<Arc<GlobSet>>,
    ) -> Result<Self> {
        let tables = client
            .list_tables(&schema.catalog_name, &schema.name)
            .await?
            .context(super::SchemaDoesntExistSnafu {
                schema: schema.name.clone(),
                catalog_id: schema.catalog_name.clone(),
            })?;

        // First pass: filter to queryable, included tables with valid references.
        let mut candidates: Vec<(UCTable, TableReference)> = Vec::new();
        for table in tables {
            if !table.is_queryable() {
                tracing::debug!(
                    table = %table.full_name(),
                    table_type = %table.table_type,
                    "Skipping unsupported Unity Catalog table type"
                );
                continue;
            }

            let Some(table_reference) = table_creator.table_reference(&table) else {
                continue;
            };

            let schema_with_table = format!("{}.{}", schema.name, table.name);
            tracing::debug!("Checking if table {} should be included", schema_with_table);
            if let Some(include) = &include
                && !include.is_match(&schema_with_table)
            {
                tracing::debug!("Table {} is not included", schema_with_table);
                continue;
            }

            candidates.push((table, table_reference));
        }

        // Second pass: check permissions concurrently (bounded). Explicitly
        // denied tables are excluded; ambiguous/unreachable cases are kept.
        let max_concurrent_permission_checks = 5;
        let permission_results: Vec<Option<(UCTable, TableReference)>> =
            futures::stream::iter(candidates.into_iter().map(|(table, table_ref)| {
                let client = Arc::clone(&client);
                async move {
                    if !table.requires_read_permission_validation() {
                        tracing::debug!(
                            table = %table.full_name(),
                            table_type = %table.table_type,
                            "Skipping strict Unity Catalog permission precheck for foreign table during catalog discovery"
                        );
                        return Some((table, table_ref));
                    }

                    match client.get_effective_permissions(&table.full_name()).await {
                        Ok(Some(perms)) if !perms.has_read_permission() => {
                            // Explicit denial: skip this table for the current
                            // catalog discovery pass. It can be discovered on a
                            // later refresh or restart if permissions change.
                            tracing::warn!(
                                table = %table.full_name(),
                                "Skipping table during catalog discovery: no read-compatible privilege found in effective-permissions response"
                            );
                            return None;
                        }
                        Ok(None) => {
                            tracing::debug!(
                                table = %table.full_name(),
                                "Permission check returned no table during catalog discovery; proceeding without permission validation"
                            );
                        }
                        Err(e) => {
                            tracing::debug!(
                                table = %table.full_name(),
                                error = %e,
                                "Failed to check permissions during catalog discovery; proceeding without permission validation"
                            );
                        }
                        Ok(Some(_)) => {}
                    }

                    Some((table, table_ref))
                }
            }))
            .buffer_unordered(max_concurrent_permission_checks)
            .collect()
            .await;

        // Third pass: create table providers for permitted tables.
        let mut tables_map = HashMap::new();
        for (table, table_reference) in permission_results.into_iter().flatten() {
            let table_provider = match table_creator
                .table_provider(&table, table_reference.clone())
                .await
            {
                Ok(provider) => provider,
                Err(source) => {
                    tracing::warn!("Couldn't get table provider for {table_reference}: {source}");
                    continue;
                }
            };
            tables_map.insert(table.name.clone(), table_provider);
        }

        Ok(Self {
            tables: RwLock::new(tables_map),
            client,
            schema: schema.clone(),
            include,
            table_creator,
        })
    }

    pub async fn refresh(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let previous_table_names = self.table_names();
        let current_tables = self
            .client
            .list_tables(&self.schema.catalog_name, &self.schema.name)
            .await?
            .context(super::SchemaDoesntExistSnafu {
                schema: self.schema.name.clone(),
                catalog_id: self.schema.catalog_name.clone(),
            })?;

        let mut new_tables = Vec::new();
        let mut removed_tables = Vec::new();

        for table_name in &previous_table_names {
            if !current_tables.iter().any(|t| t.name == *table_name) {
                removed_tables.push(table_name.clone());
            }
        }

        for table in current_tables {
            if !previous_table_names.contains(&table.name) {
                new_tables.push(table);
            }
        }

        let mut new_table_providers = HashMap::new();
        for table in new_tables {
            let Some(provider) = Self::provider_for_uc_table(
                &self.schema,
                &table,
                Arc::clone(&self.table_creator),
                self.include.clone(),
                Arc::clone(&self.client),
            )
            .await
            else {
                continue;
            };
            new_table_providers.insert(table.name.clone(), provider);
        }

        let mut guard = match self.tables.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        if !removed_tables.is_empty() || !new_table_providers.is_empty() {
            let mut message = format!(
                "Refreshed schema {}.{}. ",
                self.schema.catalog_name, self.schema.name
            );
            if !removed_tables.is_empty() {
                let _ = write!(message, "Tables removed: {}.", removed_tables.join(", "));
            }
            if !new_table_providers.is_empty() {
                if !removed_tables.is_empty() {
                    message.push(' ');
                }

                let _ = write!(
                    message,
                    "Tables added: {}.",
                    new_table_providers
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                );
            }
            tracing::info!("{}", message);
        }
        for table_name in removed_tables {
            guard.remove(&table_name);
        }
        for (table_name, provider) in new_table_providers {
            guard.insert(table_name, provider);
        }
        Ok(())
    }

    async fn provider_for_uc_table(
        schema: &UCSchema,
        table: &UCTable,
        table_creator: Arc<dyn UCTableProviderFactory>,
        include: Option<Arc<GlobSet>>,
        client: Arc<UnityCatalog>,
    ) -> Option<Arc<dyn TableProvider>> {
        if !table.is_queryable() {
            tracing::debug!(
                table = %table.full_name(),
                table_type = %table.table_type,
                "Skipping unsupported Unity Catalog table type"
            );
            return None;
        }

        let table_name = table.name.clone();
        let table_reference = table_creator.table_reference(table)?;

        let schema_with_table = format!("{}.{}", schema.name, table_name);
        tracing::debug!("Checking if table {} should be included", schema_with_table);
        if let Some(include) = &include
            && !include.is_match(&schema_with_table)
        {
            tracing::debug!("Table {} is not included", schema_with_table);
            return None;
        }

        if table.requires_read_permission_validation() {
            match client.get_effective_permissions(&table.full_name()).await {
                Ok(Some(perms)) if !perms.has_read_permission() => {
                    // Explicit denial: skip this table so a later refresh or
                    // restart can discover it if permissions change.
                    tracing::warn!(
                        table = %table.full_name(),
                        "Skipping table during catalog refresh: no read-compatible privilege found in effective-permissions response"
                    );
                    return None;
                }
                Ok(None) => {
                    tracing::debug!(
                        table = %table.full_name(),
                        "Permission check returned no table during catalog refresh; proceeding without permission validation"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        table = %table.full_name(),
                        error = %e,
                        "Failed to check permissions during catalog refresh; proceeding without permission validation"
                    );
                }
                Ok(Some(_)) => {}
            }
        } else {
            tracing::debug!(
                table = %table.full_name(),
                table_type = %table.table_type,
                "Skipping strict Unity Catalog permission precheck for foreign table during catalog refresh"
            );
        }

        let table_provider = match table_creator
            .table_provider(table, table_reference.clone())
            .await
        {
            Ok(provider) => provider,
            Err(source) => {
                tracing::warn!("Couldn't get table provider for {table_reference}: {source}");
                return None;
            }
        };
        Some(table_provider)
    }
}

#[async_trait]
impl SchemaProvider for UnityCatalogSchemaProvider {
    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `Ok(None)`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        let Some(table) = guard.get(name) else {
            return Ok(None);
        };

        Ok(Some(Arc::clone(table)))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        let guard = match self.tables.read() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        guard.contains_key(name)
    }
}
