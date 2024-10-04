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
use crate::dataconnector::DataConnectorError;
use crate::model::EmbeddingModelStore;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use llms::chunking::ChunkingConfig;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::dataconnector::DataConnector;
use crate::dataconnector::DataConnectorResult;

use super::table::EmbeddingTable;

pub struct EmbeddingConnector {
    inner_connector: Arc<dyn DataConnector>,

    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl EmbeddingConnector {
    pub fn new(
        inner_connector: Arc<dyn DataConnector>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    ) -> Self {
        Self {
            inner_connector,
            embedding_models,
        }
    }

    /// Wrap an existing [`TableProvider`] with a [`EmbeddingTable`] provider. If no embeddings
    /// are needed for the [`Dataset`], it is not unnecessarily nested.
    pub(crate) async fn wrap_table(
        &self,
        inner_table_provider: Arc<dyn TableProvider>,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        if dataset.embeddings.is_empty() {
            return Ok(inner_table_provider);
        }

        // Runtime isn't built with model support, but user specified a dataset to use embeddings.
        if !cfg!(feature = "models") {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: dataset.source(),
                message: format!(
                "Dataset '{}' expects to use an embedding model, but the runtime is not built with model support. Either: \n  1) `spice install ai` \n  2) Build spiced binary with flag `--features models`.",
                dataset.name
            )});
        }

        let embed_columns: HashMap<String, String, _> = dataset
            .embeddings
            .iter()
            .map(|e| (e.column.clone(), e.model.clone()))
            .collect::<HashMap<_, _>>();

        // Early check if embedding models are available.
        for (column, model) in &embed_columns {
            if !self.embedding_models.read().await.contains_key(model) {
                return Err(DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "EmbeddingConnector".to_string(),
                    message: format!(
                    "Dataset '{}' expects to use embedding model '{model}' to embed column '{column}', but the model '{model}' is not defined in Spicepod (as an 'embeddings').",
                    dataset.name
                )});
            }
        }

        let embed_chunker_config: HashMap<String, ChunkingConfig> = dataset
            .embeddings
            .iter()
            .filter(|e| e.chunking.as_ref().is_some_and(|s| s.enabled))
            .filter_map(|e| {
                e.chunking.as_ref().map(|chunk_cfg| {
                    (
                        e.column.clone(),
                        ChunkingConfig {
                            target_chunk_size: chunk_cfg.target_chunk_size,
                            overlap_size: chunk_cfg.overlap_size,
                            trim_whitespace: chunk_cfg.trim_whitespace,
                            file_format: dataset.params.get("file_format").map(String::as_str),
                        },
                    )
                })
            })
            .collect::<HashMap<_, _>>();

        Ok(Arc::new(
            EmbeddingTable::new(
                inner_table_provider,
                embed_columns,
                Arc::clone(&self.embedding_models),
                embed_chunker_config,
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
        self.wrap_table(self.inner_connector.read_provider(dataset).await?, dataset)
            .await
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self.inner_connector.read_write_provider(dataset).await {
            Some(Ok(inner)) => Some(self.wrap_table(inner, dataset).await),
            Some(Err(e)) => Some(Err(e)),
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
