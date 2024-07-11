/*
Copyright 2024 The Spice.ai OSS Authors

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
    catalog::{schema::SchemaProvider, CatalogProvider},
    datasource::TableProvider,
    error::DataFusionError,
    sql::TableReference,
};
use snafu::prelude::*;
use std::{any::Any, collections::HashMap, sync::Arc};

use super::{Result, UCSchema, UnityCatalog};

pub type TableCreatorFn =
    dyn Fn(
        TableReference,
    )
        -> std::result::Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;

pub struct UnityCatalogProvider {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl UnityCatalogProvider {
    pub async fn try_new(
        client: Arc<UnityCatalog>,
        catalog_id: &str,
        table_creator: Arc<TableCreatorFn>,
    ) -> Result<Self> {
        let schemas =
            client
                .list_schemas(catalog_id)
                .await?
                .context(super::CatalogDoesntExistSnafu {
                    catalog_id: catalog_id.to_string(),
                })?;

        let mut schemas_map = HashMap::new();
        for schema in schemas {
            let schema_provider = UnityCatalogSchemaProvider::try_new(
                Arc::clone(&client),
                &schema,
                Arc::clone(&table_creator),
            )
            .await?;
            schemas_map.insert(
                schema.name,
                Arc::new(schema_provider) as Arc<dyn SchemaProvider>,
            );
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
        self.schemas.get(name).cloned()
    }
}

pub struct UnityCatalogSchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
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
        table_creator: Arc<TableCreatorFn>,
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
            let table_reference = TableReference::Full {
                catalog: table.catalog_name.into(),
                schema: table.schema_name.into(),
                table: table.name.into(),
            };
            let table_name = table_reference.table().to_string();
            let table_provider = table_creator(table_reference.clone()).context(
                super::UnableToGetTableProviderSnafu {
                    table_reference: table_reference.to_string(),
                },
            )?;
            tables_map.insert(table_name, table_provider);
        }

        Ok(Self { tables: tables_map })
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
        self.tables.keys().cloned().collect()
    }

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `Ok(None)`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Some(table) = self.tables.get(name) else {
            return Ok(None);
        };

        Ok(Some(Arc::clone(table)))
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
