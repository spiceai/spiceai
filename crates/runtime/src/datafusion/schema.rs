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

use std::sync::Arc;

use crate::embeddings::table::EmbeddingTable;
use arrow_schema::SchemaRef;
use datafusion::{
    datasource::TableProvider, error::Result, execution::context::SessionContext,
    sql::TableReference,
};
use runtime_datafusion::schema_provider::SpiceSchemaProvider;
use snafu::prelude::*;

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
    }

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
