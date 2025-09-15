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

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    logical_expr::{Expr, TableProviderFilterPushDown},
    scalar::ScalarValue,
};
use runtime_table_partition::{
    Partition,
    creator::{CreatePartitionSnafu, Error, PartitionCreator},
};
use snafu::prelude::*;
use spicepod::vector::VectorStore;
use tokio::sync::RwLock;

use crate::{
    embeddings::index::{VectorIndex as _, s3::S3Vector},
    secrets::Secrets,
};

pub struct S3VectorPartitionCreator {
    pub s3_vector: S3Vector,
    pub vector_store_config: VectorStore,
    pub secrets: Arc<RwLock<Secrets>>,
}

impl std::fmt::Debug for S3VectorPartitionCreator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorPartitionCreator")
            .field("s3_vector", &self.s3_vector)
            .field("vector_store_config", &self.vector_store_config)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PartitionCreator for S3VectorPartitionCreator {
    async fn create_partition(&self, partition_value: ScalarValue) -> Result<Partition, Error> {
        let partition_str = partition_value.to_string();

        let params = super::get_store_params(&self.vector_store_config, Arc::clone(&self.secrets))
            .await
            .context(CreatePartitionSnafu)?;

        let table = super::try_vector_table(
            self.s3_vector.metadata_columns.clone(),
            params,
            format!(
                "{}-{}-{}",
                self.s3_vector.table.index_name(),
                self.s3_vector.embedded_column,
                partition_str
            )
            .replace('_', "-")
            .as_str(),
            Arc::clone(&self.s3_vector.embedding_models),
            self.s3_vector.model_name.as_str(),
        )
        .await
        .context(CreatePartitionSnafu)?;

        let s3_vector = S3Vector::new(
            table,
            self.s3_vector.embedded_column.clone(),
            self.s3_vector.primary_key.clone(),
            self.s3_vector.metadata_columns.clone(),
            self.s3_vector.model_name.clone(),
            Arc::clone(&self.s3_vector.embedding_models),
            None, // No nested partitioning
        );

        let table_provider = s3_vector
            .list_table_provider()
            .context(CreatePartitionSnafu)?;

        Ok(Partition {
            table_provider,
            partition_value,
        })
    }

    async fn infer_existing_partitions(&self) -> Result<Vec<Partition>, Error> {
        // TODO
        Ok(vec![])
    }

    fn supports_filters_pushdown(
        &self,
        _filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        // For now, let DataFusion handle all filtering after the scan.
        Ok(vec![])
    }
}
