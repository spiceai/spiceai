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
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};
use runtime::{component::dataset::Dataset, dataconnector::DataConnector};
use std::{any::Any, collections::HashMap, sync::Arc};

pub struct SpiceAISchemaProvider {
    tables: HashMap<String, Dataset>,
    data_connector: Arc<dyn DataConnector>,
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
    /// otherwise returns `None`.
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let Some(dataset) = self.tables.get(name) else {
            return Ok(None);
        };

        self.data_connector
            .read_provider(dataset)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
            .map(Some)
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, _name: &str) -> bool {
        todo!();
    }
}
