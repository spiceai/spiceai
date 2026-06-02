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

//! [`NativeVectorIndex`] — a zero-external-dependency vector index that relies
//! on Spice's registered similarity UDFs to score rows in place. The
//! `vector_search` UDTF compiles per-metric plans against:
//!   - `cosine_distance` for `DistanceMetric::Cosine`
//!   - `array_distance` for `DistanceMetric::L2`
//!   - `inner_product` (SIMD-backed) for `DistanceMetric::Dot`
//!
//! A standalone SIMD-backed `l2_distance` UDF is also registered for direct use
//! from SQL; it is intentionally not the default path for `DistanceMetric::L2`
//! to preserve compatibility with generic list-typed columns.
//!
//! This is the index used by the Cayenne accelerator: the embedding column lives
//! in the underlying table, so there is no separate index store. The index acts
//! as a marker — discovery by the search engine (via
//! [`SearchIndex::as_vector_index`]) routes the `vector_search` UDTF to its
//! indexed-scan codepath, which compiles a SQL plan using the registered
//! distance UDFs rather than calling `embed()` on the fly.

use std::any::Any;
use std::sync::Arc;

use crate::index::{SearchIndex, VectorIndex};

use arrow::array::RecordBatch;
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan, sql::TableReference};
use runtime_datafusion_index::Index;

/// A vector index that scores rows in-table via Spice's SIMD similarity UDFs.
///
/// Unlike [`crate::index::s3_vectors::S3Vector`] or
/// [`crate::index::elasticsearch::ElasticsearchIndex`], this index does not push
/// embeddings to an external store — the embedding column is assumed to live on
/// the underlying table (e.g. a Cayenne-accelerated Vortex column of
/// `FixedSizeList<Float32, N>`). Scoring relies on the registered
/// `cosine_distance` / `inner_product` / `l2_distance` UDFs at query time.
///
/// The index does not implement an ANN data structure; it signals to the search
/// engine that the table has precomputed embeddings and should use the indexed
/// scan path rather than an on-the-fly `embed()` fallback.
#[derive(Clone)]
pub struct NativeVectorIndex {
    table_ref: TableReference,
    search_column: String,
    primary_fields: Vec<Field>,
    dimension: i32,
}

impl std::fmt::Debug for NativeVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeVectorIndex")
            .field("table_ref", &self.table_ref)
            .field("search_column", &self.search_column)
            .field("dimension", &self.dimension)
            .field("primary_fields_len", &self.primary_fields.len())
            .finish()
    }
}

impl NativeVectorIndex {
    #[must_use]
    pub fn new(
        table_ref: TableReference,
        search_column: String,
        primary_fields: Vec<Field>,
        dimension: i32,
    ) -> Self {
        Self {
            table_ref,
            search_column,
            primary_fields,
            dimension,
        }
    }

    #[must_use]
    pub fn table_ref(&self) -> &TableReference {
        &self.table_ref
    }
}

#[async_trait]
impl Index for NativeVectorIndex {
    fn name(&self) -> &'static str {
        "NativeVectorIndex"
    }

    fn required_columns(&self) -> Vec<String> {
        vec![self.search_column.clone()]
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        // No augmentation needed — the embedding column is already in the underlying
        // table, written through the normal accelerator sink.
        Ok(batches)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for NativeVectorIndex {
    fn search_column(&self) -> String {
        self.search_column.clone()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.primary_fields.clone()
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        // Pass-through: the embedding data is written by the underlying sink.
        Ok(record)
    }

    fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        // The `vector_search` UDTF never invokes this path — it compiles its own
        // plan via `VectorUDTFGeneration` using the registered distance UDFs.
        // Direct invocation (e.g. by `SearchIndexProvider` for full-text search)
        // is not applicable to a native vector index, so we explicitly reject it.
        Err(DataFusionError::NotImplemented(
            "NativeVectorIndex::query_table_provider is not used by vector_search — \
             the runtime compiles the plan directly using registered distance UDFs"
                .to_string(),
        ))
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(self as Arc<dyn VectorIndex>)
    }
}

impl VectorIndex for NativeVectorIndex {
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        // See comment on `query_table_provider`. The embedding data lives on the
        // underlying table; the search engine accesses it by scanning the table
        // directly, not via the index trait.
        Err(DataFusionError::NotImplemented(
            "NativeVectorIndex::list_table_provider is not used by vector_search — \
             the runtime scans the underlying table directly"
                .to_string(),
        ))
    }

    fn dimension(&self) -> i32 {
        self.dimension
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn idx() -> NativeVectorIndex {
        NativeVectorIndex::new(
            TableReference::bare("docs"),
            "embedding".to_string(),
            vec![Field::new("id", DataType::Int64, false)],
            768,
        )
    }

    #[test]
    fn search_column_round_trips() {
        let i = idx();
        assert_eq!(i.search_column(), "embedding");
        assert_eq!(i.dimension(), 768);
        assert_eq!(i.primary_fields().len(), 1);
    }

    #[test]
    fn as_vector_index_returns_self() {
        let arc = Arc::new(idx());
        assert!(
            SearchIndex::as_vector_index(arc).is_some(),
            "NativeVectorIndex must report itself as a VectorIndex"
        );
    }

    #[test]
    fn required_columns_exposes_search_column() {
        let i = idx();
        let cols = i.required_columns();
        assert_eq!(cols, vec!["embedding".to_string()]);
    }

    #[tokio::test]
    async fn compute_index_is_passthrough() {
        let i = idx();
        let batches = vec![]; // empty is fine — we only care that compute_index doesn't mutate
        let out = i.compute_index(batches).await.expect("ok");
        assert!(out.is_empty());
    }

    #[test]
    fn query_table_provider_is_rejected() {
        // vector_search uses VectorUDTFGeneration; direct invocation of the index
        // trait path must surface a clear error instead of silently returning.
        let i = idx();
        let err = i
            .query_table_provider("hello")
            .expect_err("should be not-implemented");
        assert!(matches!(err, DataFusionError::NotImplemented(_)));
    }
}
