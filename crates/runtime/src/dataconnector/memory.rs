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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::arrow::write::MemTable;
use snafu::ResultExt;

use std::{any::Any, pin::Pin, sync::Arc};

use crate::{component::dataset::Dataset, tools::memory::MEMORY_TABLE_SCHEMA};
use datafusion::datasource::TableProvider;
use futures::Future;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};

/// A connector that wraps a [`MemTable`] initialised without data, that can be
/// written to, and has predefined schema based on the dataset path (see [`Self::schema_from_path`]).
#[derive(Debug, Clone, Default)]
pub struct MemoryConnector {}

impl MemoryConnector {
    pub(crate) fn schema_from_path(path: &str) -> Option<SchemaRef> {
        match path {
            "store" => Some(Arc::clone(&MEMORY_TABLE_SCHEMA)),
            _ => None,
        }
    }
}

#[derive(Default, Copy, Clone)]
pub struct MemoryConnectorFactory {}

impl MemoryConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for MemoryConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move { Ok(Arc::new(MemoryConnector::default()) as Arc<dyn DataConnector>) })
    }

    fn prefix(&self) -> &'static str {
        "memory"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[async_trait]
impl DataConnector for MemoryConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let Some(schema) = Self::schema_from_path(path) else {
            return Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "memory".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Invalid path: {path}"
                )),
            });
        };
        let table = MemTable::try_new(schema, vec![]).boxed().map_err(|e| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: "memory".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: e,
            }
        })?;

        Ok(Arc::new(table) as Arc<dyn TableProvider>)
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let path = dataset.path();
        let Some(schema) = Self::schema_from_path(path) else {
            return Some(Err(DataConnectorError::UnableToGetReadProvider {
                dataconnector: "memory".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "Invalid path: {path}"
                )),
            }));
        };

        match MemTable::try_new(schema, vec![]) {
            Ok(table) => Some(Ok(Arc::new(table) as Arc<dyn TableProvider>)),
            Err(e) => Some(Err(DataConnectorError::UnableToGetReadWriteProvider {
                dataconnector: "memory".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::<dyn std::error::Error + Send + Sync>::from(e.message().to_string()),
            })),
        }
    }
}
