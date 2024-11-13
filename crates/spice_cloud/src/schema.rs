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
use datafusion::{catalog::SchemaProvider, datasource::TableProvider, error::DataFusionError};
use futures::{stream::FuturesUnordered, StreamExt};
use runtime::{
    component::dataset::Dataset,
    dataconnector::{DataConnector, DataConnectorError},
};
use std::{any::Any, collections::HashMap, sync::Arc};
use tracing::instrument;

#[derive(Debug)]
pub struct SpiceAISchemaProvider {
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl SpiceAISchemaProvider {
    /// Creates a new instance of the [`SpiceAISchemaProvider`].
    ///
    /// # Errors
    ///
    /// Returns an error if the schema cannot be created.
    #[instrument(level = "debug", skip(data_connector, tables))]
    pub async fn try_new(
        schema: &str,
        data_connector: Arc<dyn DataConnector>,
        tables: HashMap<String, Dataset>,
    ) -> Result<Self, DataConnectorError> {
        let futures: Vec<_> = tables
            .into_iter()
            .map(|(name, dataset)| {
                let data_connector = Arc::clone(&data_connector);
                async move {
                    let table_provider = data_connector.read_provider(&dataset).await?;
                    Ok((name, table_provider))
                }
            })
            .collect();

        tracing::debug!("Creating {} tables for schema '{schema}'", futures.len());
        let mut futures_unordered = FuturesUnordered::new();
        futures_unordered.extend(futures);

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        while let Some(table_result) = futures_unordered.next().await {
            let (name, table_provider) = table_result?;
            tables.insert(name, table_provider);
            tracing::trace!("{} tables processed", tables.len());
        }

        Ok(SpiceAISchemaProvider { tables })
    }
}

#[async_trait]
impl SchemaProvider for SpiceAISchemaProvider {
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
