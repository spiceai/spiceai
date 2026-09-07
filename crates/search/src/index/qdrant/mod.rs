/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

mod write;

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use futures::future::try_join_all;
use llms::embeddings::Embed;
use qdrant::QdrantStore;
use spice_table::{Index, WriteWindow};

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::{SearchIndex, VectorIndex, embedding_col};
use crate::metadata::MetadataColumns;
use data_components::qdrant::query_provider::{
    QdrantListTable, QdrantQueryTable, QdrantScoreSemantics, QueryEmbedder,
};

const DEFAULT_QDRANT_VECTOR_SEARCH_LIMIT: usize = 1000;

/// Maps the collection's distance metric onto Spice `_score` semantics so every
/// metric ranks higher-is-better and cosine matches 1 - cosine_distance.
fn score_semantics_for_metric(distance_metric: &str) -> QdrantScoreSemantics {
    match distance_metric {
        "euclidean" | "manhattan" => QdrantScoreSemantics::NegatedDistance,
        "cosine" => QdrantScoreSemantics::NormalizedCosine,
        _ => QdrantScoreSemantics::Similarity,
    }
}

#[cfg(test)]
mod score_semantics_tests {
    use super::score_semantics_for_metric;
    use data_components::qdrant::query_provider::QdrantScoreSemantics;

    #[test]
    fn metrics_map_to_score_semantics() {
        assert_eq!(
            score_semantics_for_metric("cosine"),
            QdrantScoreSemantics::NormalizedCosine
        );
        assert_eq!(
            score_semantics_for_metric("dot"),
            QdrantScoreSemantics::Similarity
        );
        assert_eq!(
            score_semantics_for_metric("euclidean"),
            QdrantScoreSemantics::NegatedDistance
        );
        assert_eq!(
            score_semantics_for_metric("manhattan"),
            QdrantScoreSemantics::NegatedDistance
        );
    }
}

#[derive(Debug)]
struct EmbedQueryAdapter(Arc<dyn Embed>);

#[async_trait]
impl QueryEmbedder for EmbedQueryAdapter {
    async fn embed_query(&self, query: &str) -> Result<Vec<f32>, DataFusionError> {
        let mut vectors = self
            .0
            .embed(llms::embeddings::EmbeddingInput::String(query.to_string()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        vectors.pop().ok_or_else(|| {
            DataFusionError::Execution("No embedding vector computed for query".to_string())
        })
    }
}

#[derive(Debug, Clone)]
pub struct QdrantIndex {
    pub client: Arc<dyn QdrantStore>,

    pub collection: String,

    pub embedded_column: String,

    pub primary_key: Vec<Field>,

    pub compute_query: Arc<dyn Embed>,

    pub dims: i32,

    pub distance_metric: String,

    pub metadata_columns: MetadataColumns,

    pub batch_write_rows: usize,

    /// Payload field name that holds the partition value, when partitioning is enabled.
    pub partition_key: Option<String>,

    /// Source column the partition value is read from.
    pub partition_column: Option<String>,
}

#[async_trait]
impl SearchIndex for QdrantIndex {
    fn search_column(&self) -> String {
        self.embedded_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_key.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        write::write(self, record)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let schema = self.query_result_schema();
        let table: Arc<dyn TableProvider> = Arc::new(QdrantQueryTable::new(
            Arc::clone(&self.client),
            self.collection.clone(),
            vec![],
            DEFAULT_QDRANT_VECTOR_SEARCH_LIMIT,
            Arc::clone(&schema),
            embedding_col(&self.embedded_column),
            self.dims,
            Some(query.to_string()),
            Some(Arc::new(EmbedQueryAdapter(Arc::clone(&self.compute_query)))),
            score_semantics_for_metric(&self.distance_metric),
        ));

        Ok(
            LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?
                .build()?
                .into(),
        )
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(Arc::clone(&self) as Arc<dyn VectorIndex>)
    }
}

impl VectorIndex for QdrantIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        let schema = self.list_result_schema();
        let table: Arc<dyn TableProvider> = Arc::new(QdrantListTable::new(
            Arc::clone(&self.client),
            self.collection.clone(),
            Arc::clone(&schema),
            embedding_col(&self.embedded_column),
            self.dims,
        ));

        LogicalPlanBuilder::scan("tbl", Arc::new(DefaultTableSource::new(table)), None)?.build()
    }

    fn dimension(&self) -> i32 {
        self.dims
    }
}

#[async_trait]
impl Index for QdrantIndex {
    fn name(&self) -> &'static str {
        "qdrant_index"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut cols: Vec<_> = self.primary_key.iter().map(|f| f.name().clone()).collect();
        cols.push(self.embedded_column.clone());

        let derived_embedding_column = embedding_col(&self.embedded_column);
        for name in self.metadata_columns.all_names() {
            if name == derived_embedding_column {
                continue;
            }
            if !cols.contains(&name) {
                cols.push(name);
            }
        }

        cols
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches.into_iter().map(|rb| async move {
            write::write(self, rb)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        });
        try_join_all(futs).await
    }

    async fn on_write_start(&self, _window: WriteWindow) -> Result<(), DataFusionError> {
        Ok(())
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        write::delete_by_keys(self, &keys)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}

impl QdrantIndex {
    fn metadata_fields(&self) -> Vec<Field> {
        let embedding_name = embedding_col(&self.embedded_column);
        self.metadata_columns
            .iter()
            .filter(|c| c.name() != embedding_name)
            .map(|c| Arc::unwrap_or_clone(c.field()))
            .collect()
    }

    fn query_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
        fields.extend(self.metadata_fields());
        fields.push(Field::new(
            embedding_col(&self.embedded_column),
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                self.dims,
            ),
            true,
        ));
        fields.push(Field::new(
            SEARCH_SCORE_COLUMN_NAME,
            DataType::Float64,
            true,
        ));
        Arc::new(Schema::new(fields))
    }

    fn list_result_schema(&self) -> SchemaRef {
        let mut fields: Vec<Field> = self.primary_key.clone();
        fields.extend(self.metadata_fields());
        fields.push(Field::new(
            embedding_col(&self.embedded_column),
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                self.dims,
            ),
            true,
        ));
        Arc::new(Schema::new(fields))
    }
}
