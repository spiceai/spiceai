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

use crate::component::dataset::Dataset;
use crate::dataconnector::AnyErrorResult;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use llms::embeddings::Embed;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorResult;

use super::table::EmbeddingTable;

pub struct EmbeddingConnector {
    inner_connector: Arc<dyn DataConnector>,

    embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
}

impl EmbeddingConnector {
    pub fn new(
        inner_connector: Arc<dyn DataConnector>,
        embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
    ) -> Self {
        Self {
            inner_connector,
            embedding_models,
        }
    }

    /// Wrap an existing [`TableProvider`] with a [`EmbeddingTable`] provider. If no embeddings
    /// are needed for the [`Dataset`], it is not unnecessarily nested.
    async fn wrap(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        if dataset.embeddings.is_empty() {
            return Ok(inner_table_provider);
        }

        println!("I, {}, have been wrapped", dataset.name);
        let embed_columns: HashMap<String, String, _> = HashMap::from_iter(
            dataset
                .embeddings
                .iter()
                .map(|e| (e.column.clone(), e.model.clone())),
        );

        Ok(Arc::new(
            EmbeddingTable::new(
                dataset.name.clone(),
                inner_table_provider,
                embed_columns,
                self.embedding_models.clone(),
            )
            .await,
        ) as Arc<dyn TableProvider>)
    }
}

#[async_trait]
impl DataConnector for EmbeddingConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        self.wrap(self.inner_connector.read_provider(dataset).await?, dataset)
            .await
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(self.wrap(inner, dataset).await),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    async fn stream_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<AnyErrorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(self.wrap(inner, dataset).await.map_err(|e| e.into())),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }

    async fn metadata_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        self.inner_connector.metadata_provider(dataset).await
    }
}
