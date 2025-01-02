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
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{Read, RefreshableCatalogProvider};

use super::{CatalogId, Result, UCSchema, UCTable, UnityCatalog};

#[derive(Debug)]
pub struct UnityCatalogProvider {
    schemas: HashMap<String, Arc<UnityCatalogSchemaProvider>>,
}

impl UnityCatalogProvider {
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_id: CatalogId,
        table_creator: Arc<dyn Read>,
        table_reference_creator: fn(&UCTable) -> Option<TableReference>,
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
                table_reference_creator,
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
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            .cloned()
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
    table_reference_creator: fn(&UCTable) -> Option<TableReference>,
    include: Option<Arc<GlobSet>>,
    table_creator: Arc<dyn Read>,
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
        table_creator: Arc<dyn Read>,
        table_reference_creator: fn(&UCTable) -> Option<TableReference>,
        include: Option<Arc<GlobSet>>,
    ) -> Result<Self> {
        let tables = client
            .list_tables(&schema.catalog_name, &schema.name)
            .await?
            .context(super::SchemaDoesntExistSnafu {
                schema: schema.name.to_string(),
                catalog_id: schema.catalog_name.to_string(),
            })?;

        let mut tables_map = HashMap::new();
        for table in tables {
            let table_name = table.name.to_string();
            let table_reference = table_reference_creator(&table);

            let Some(table_reference) = table_reference else {
                continue;
            };

            let schema_with_table = format!("{}.{}", schema.name, table_name);
            tracing::debug!("Checking if table {} should be included", schema_with_table);
            if let Some(include) = &include {
                if !include.is_match(&schema_with_table) {
                    tracing::debug!("Table {} is not included", schema_with_table);
                    continue;
                }
            }

            let table_provider = match table_creator
                .table_provider(table_reference.clone(), None)
                .await
            {
                Ok(provider) => provider,
                Err(source) => {
                    tracing::warn!("Couldn't get table provider for {table_reference}: {source}");
                    continue;
                }
            };
            tables_map.insert(table_name, table_provider);
        }

        Ok(Self {
            tables: RwLock::new(tables_map),
            client,
            schema: schema.clone(),
            table_reference_creator,
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
                schema: self.schema.name.to_string(),
                catalog_id: self.schema.catalog_name.to_string(),
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
                self.table_reference_creator,
                self.include.clone(),
            )
            .await
            else {
                continue;
            };
            new_table_providers.insert(table.name.to_string(), provider);
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
                message.push_str(&format!("Tables removed: {}.", removed_tables.join(", ")));
            }
            if !new_table_providers.is_empty() {
                if !removed_tables.is_empty() {
                    message.push(' ');
                }
                message.push_str(&format!(
                    "Tables added: {}.",
                    new_table_providers
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .as_slice()
                        .join(", ")
                ));
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
        table_creator: Arc<dyn Read>,
        table_reference_creator: fn(&UCTable) -> Option<TableReference>,
        include: Option<Arc<GlobSet>>,
    ) -> Option<Arc<dyn TableProvider>> {
        let table_name = table.name.to_string();
        let table_reference = table_reference_creator(table)?;

        let schema_with_table = format!("{}.{}", schema.name, table_name);
        tracing::debug!("Checking if table {} should be included", schema_with_table);
        if let Some(include) = &include {
            if !include.is_match(&schema_with_table) {
                tracing::debug!("Table {} is not included", schema_with_table);
                return None;
            }
        }

        let table_provider = match table_creator
            .table_provider(table_reference.clone(), None)
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
    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

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
