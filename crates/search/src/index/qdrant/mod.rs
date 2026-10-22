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
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use futures::{StreamExt, TryStreamExt};
use llms::embeddings::Embed;
use parking_lot::Mutex;
use qdrant::QdrantStore;
use qdrant::proto::{Condition, Filter, PointId};
use spice_table::{GroupPruning, Index, WriteWindow};

use crate::SEARCH_SCORE_COLUMN_NAME;
use crate::index::{MAX_CONCURRENT_INDEX_WRITES, SearchIndex, VectorIndex, embedding_col};
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
        let vectors = self
            .0
            .embed(llms::embeddings::EmbeddingInput::String(query.to_string()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        vectors.first().cloned().ok_or_else(|| {
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

    /// The dataset whose points this index writes, reads, and deletes. Other points in the
    /// collection are never touched.
    pub dataset: String,

    pub write_generation: Arc<WriteGeneration>,
}

/// The generation stamped on every point a [`QdrantIndex`] writes.
///
/// A [`WriteWindow::ReplaceAll`] window starts a new generation, and when it completes the
/// dataset's points of every other generation are deleted in one filtered request. A write
/// that lands while the window is open, a CDC upsert included, carries the new generation and
/// survives. Sink windows on one table never overlap, so each window start ends the previous
/// window, including one whose write was dropped before it completed or failed.
#[derive(Debug)]
pub struct WriteGeneration {
    /// Held for reading across each write, and for writing while a full refresh starts a new
    /// generation, so no write stamps the old generation after the window opened.
    current: tokio::sync::RwLock<i64>,
    replacing: Mutex<Option<i64>>,
}

impl Default for WriteGeneration {
    fn default() -> Self {
        Self {
            current: tokio::sync::RwLock::new(next_generation(0)),
            replacing: Mutex::new(None),
        }
    }
}

/// Microseconds since the Unix epoch, and always above `current`, so a new generation never
/// repeats one a point already carries, even when the clock moves back.
fn next_generation(current: i64) -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX));
    now.max(current.saturating_add(1))
}

impl WriteGeneration {
    async fn start(&self, window: WriteWindow) {
        if window == WriteWindow::ReplaceAll {
            let mut current = self.current.write().await;
            *current = next_generation(*current);
            *self.replacing.lock() = Some(*current);
        } else {
            *self.replacing.lock() = None;
        }
    }

    fn take_replacing(&self) -> Option<i64> {
        self.replacing.lock().take()
    }
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
            self.dataset.clone(),
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
            self.dataset.clone(),
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
        futures::stream::iter(futs)
            .buffered(MAX_CONCURRENT_INDEX_WRITES)
            .try_collect()
            .await
    }

    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        self.write_generation.start(window).await;
        Ok(())
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.write_generation.take_replacing();
        Ok(())
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        let Some(generation) = self.write_generation.take_replacing() else {
            return Ok(());
        };
        let mut stale = Filter::must([self.dataset_condition()]);
        stale.must_not.push(Condition::matches(
            qdrant::payload::GENERATION_KEY,
            generation,
        ));
        self.client
            .delete_by_filter(&self.collection, stale)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        true
    }

    async fn truncate(&self) -> Result<(), DataFusionError> {
        self.client
            .delete_by_filter(&self.collection, Filter::must([self.dataset_condition()]))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        write::delete_by_keys(self, &keys)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn deletes_by_partial_key(&self) -> bool {
        true
    }

    async fn delete_group_remainder(
        &self,
        group_columns: &[String],
        members: RecordBatch,
    ) -> Result<(), DataFusionError> {
        write::delete_group_remainder(self, group_columns, &members)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn group_pruning(&self) -> GroupPruning {
        GroupPruning::Complete
    }
}

impl QdrantIndex {
    /// Point ids include the dataset, so two datasets sharing a collection never overwrite
    /// each other's points.
    fn point_id(&self, key: &str) -> PointId {
        qdrant::payload::point_id_from_values(&[self.dataset.clone(), key.to_string()])
    }

    fn dataset_condition(&self) -> Condition {
        Condition::matches(qdrant::payload::DATASET_KEY, self.dataset.clone())
    }

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
