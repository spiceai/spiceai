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

use arrow::array::RecordBatch;
use arrow_schema::Field;
use async_openai::types::EmbeddingInput;
use async_trait::async_trait;
use data_components::s3_vectors::{
    MetadataColumns, list_provider::S3VectorsListTable, query_provider::S3VectorsQueryTable,
};
use llms::embeddings::Embed;
use runtime_datafusion_index::Index;
use snafu::ResultExt;

use crate::model::EmbeddingModelStore;
use datafusion::catalog::TableProvider;
use tokio::sync::RwLock;

pub(crate) mod query_table;
mod retry_client;
pub mod s3;
pub(crate) mod scan_table;
pub use query_table::VectorQueryTableProvider;
pub use scan_table::VectorScanTableProvider;

#[derive(Debug, Clone)]
pub struct IndexEmbeddingConfig {
    pub model_name: String,
    pub embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

#[async_trait]
pub trait VectorIndex: std::fmt::Debug + Send + Sync {
    fn embedded_column(&self) -> String;
    fn primary_fields(&self) -> Vec<Field>;
    fn list_table_provider(&self) -> Arc<dyn TableProvider>;
    fn metadata_columns(&self) -> &MetadataColumns;
    fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider>;
    async fn write(&self, record: &RecordBatch);
    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Implementations of indexes that can produce embedding vectors for a column in the associated [`IndexedTableProvider`], and some, provide efficient search mechanism for it.
#[derive(Debug, Clone)]
pub struct S3Vector {
    index: s3::S3VectorIndex,
    cfg: IndexEmbeddingConfig,
}

impl S3Vector {
    #[must_use]
    pub fn new(index: s3::S3VectorIndex, cfg: IndexEmbeddingConfig) -> Self {
        Self { index, cfg }
    }

    pub async fn embedding_model(&self) -> Option<Arc<dyn Embed>> {
        let model_lock = self.cfg.embedding_models.read().await;
        let model = model_lock.get(&self.cfg.model_name)?;
        Some(Arc::clone(model))
    }
}

#[async_trait]
impl VectorIndex for S3Vector {
    fn embedded_column(&self) -> String {
        self.index.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.index.primary_key.clone()
    }

    fn list_table_provider(&self) -> Arc<dyn TableProvider> {
        Arc::new(S3VectorsListTable::from(self.index.table.clone()))
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.index.metadata_columns
    }

    fn augment_table(self: Arc<Self>, table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
        Arc::new(VectorScanTableProvider::new(table, self))
    }

    async fn write(&self, record: &RecordBatch) {
        s3::write(&self.index, &self.cfg, record).await;
    }

    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        let models = self.cfg.embedding_models.read().await;
        let Some(embedding_model) = models.get(&self.cfg.model_name) else {
            return Err(Box::from(format!(
                "Vector index requires '{}' embedding model, but is not available.",
                self.cfg.model_name
            )));
        };
        let mut resp = embedding_model
            .embed(EmbeddingInput::String(query.to_string()))
            .await
            .boxed()?;
        let Some(query_vector) = resp.pop() else {
            return Err(Box::from(format!(
                "Embedding model '{}' produced no embedding for the query '{query}'.",
                self.cfg.model_name,
            )));
        };

        Ok(Arc::new(S3VectorsQueryTable::new(
            self.index.table.clone(),
            query_vector,
        )))
    }
}

#[async_trait]
impl Index for S3Vector {
    fn name(&self) -> &'static str {
        "s3_vector_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        self.index.required_columns()
    }

    async fn compute_index(&self, batches: Vec<RecordBatch>) {
        for rb in batches {
            self.write(&rb).await;
        }
    }
}
