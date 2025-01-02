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

use std::{any::Any, sync::Arc};

use crate::embeddings::table::EmbeddingTable;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    sql::TableReference,
};
use snafu::prelude::*;

// Copy of default MemorySchemaProvider that allows `register_table` to atomically overwrite any existing tables
// https://github.com/apache/datafusion/blob/deebda78a34251b2bddf0c5f66edfaa112c4559b/datafusion/core/src/catalog/schema.rs#L84
#[derive(Debug)]
pub struct SpiceSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl SpiceSchemaProvider {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }
}

impl Default for SpiceSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for SpiceSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).map(|table| Arc::clone(table.value())))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

pub(crate) fn ensure_schema_exists(
    ctx: &SessionContext,
    catalog: &str,
    table_reference: &TableReference,
) -> Result<(), super::Error> {
    let catalog_provider = ctx
        .catalog(catalog)
        .context(super::CatalogMissingSnafu { catalog })?;

    // This TableReference doesn't have a schema component, nothing to do.
    let Some(schema_name) = table_reference.schema() else {
        return Ok(());
    };

    // If the schema exists, nothing to do.
    if catalog_provider.schema(schema_name).is_some() {
        return Ok(());
    };

    // Create the schema
    let schema_provider = Arc::new(SpiceSchemaProvider::new());
    match catalog_provider.register_schema(schema_name, schema_provider) {
        Ok(_) => Ok(()),
        Err(_) => unreachable!("register_schema will never fail"),
    }
}

pub struct BaseSchema {}

impl BaseSchema {
    pub fn get_schema(provider: &Arc<dyn TableProvider>) -> SchemaRef {
        if let Some(embedding_table) = provider.as_any().downcast_ref::<EmbeddingTable>() {
            return embedding_table.get_base_table_schema();
        }
        provider.schema()
    }
}
