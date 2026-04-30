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

use arrow::array::RecordBatch;
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::{error::DataFusionError, logical_expr::LogicalPlan};
use runtime_datafusion_index::Index;

pub mod chunking;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
pub mod native_vector;
#[cfg(feature = "s3_vectors")]
pub mod s3_vectors;

pub mod vector_table;
use crate::index::chunking::ChunkedVectorIndex;
pub use native_vector::NativeVectorIndex;
pub use vector_table::VectorScanTableProvider;

#[cfg(feature = "duckdb")]
use crate::index::duckdb::DuckDBVectorIndex;
#[cfg(feature = "elasticsearch")]
use crate::index::elasticsearch::ElasticsearchIndex;
#[cfg(feature = "s3_vectors")]
use crate::index::s3_vectors::S3Vector;

/// A [`SearchIndex`] is a table index that can provide search results for arbitrary queries (see [`SearchIndex::query_table_provider`]).
/// This trait supports both vector similarity search and full-text search implementations.
///
/// A [`SearchIndex`] can have additional metadata columns to improve the filter capabilities of
/// [`SearchIndex::query_table_provider`], or to reduce the need for joining the [`TableProvider`]s
///  of the search index and underlying table.
#[async_trait]
pub trait SearchIndex: Index + std::fmt::Debug + Send + Sync + 'static {
    /// The name of the column, in the underlying table, of the column for which search is performed against.
    /// For vector indexes, this is the column that gets embedded. For FTS indexes, this is the text column being searched.
    fn search_column(&self) -> String;

    /// All [`Field`]s that define a primary key between the underlying table and the [`SearchIndex`].
    fn primary_fields(&self) -> Vec<Field>;

    /// Update the index based on a [`RecordBatch`] from the underlying table.
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>>;

    /// A [`TableProvider`] containing the [`SearchIndex::primary_fields`], additional metadata
    /// columns, the associated vectors/indexed content of the [`SearchIndex::search_column`] and the
    ///  search score between `query` and the [`SearchIndex::search_column`].
    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError>;

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        None
    }
}

/// Extracts the derived column names from a vector index implementation.
///
/// This function attempts to downcast the given index to known vector index types
/// and retrieve their derived columns (e.g., embedding columns). Returns `None` if
/// the index is not a recognized vector index type.
pub fn derived_columns_from_vector_index(
    index: &Arc<dyn Index + Send + Sync>,
) -> Option<Vec<String>> {
    #[cfg(feature = "elasticsearch")]
    if let Some(vec) = index.as_any().downcast_ref::<ElasticsearchIndex>() {
        return Some(vec.derived_columns());
    }
    #[cfg(feature = "duckdb")]
    if let Some(vec) = index.as_any().downcast_ref::<DuckDBVectorIndex>() {
        return Some(vec.derived_columns());
    }
    #[cfg(feature = "s3_vectors")]
    if let Some(vec) = index.as_any().downcast_ref::<S3Vector>() {
        return Some(vec.derived_columns());
    }
    if let Some(vec) = index.as_any().downcast_ref::<ChunkedVectorIndex>() {
        return Some(vec.derived_columns());
    }
    None
}

/// A [`SearchIndex`] that supports vector similarity (ANN) search.
///
/// Implementations fall into two distinct storage models:
///
/// ## External-store model
///
/// Embeddings are written to a dedicated external store (e.g. S3 Vectors, Elasticsearch)
/// that is separate from the base/accelerated table. [`VectorIndex::list_table_provider`]
/// returns a [`LogicalPlan`] that enumerates the store's contents (primary keys +
/// embedding vectors), and [`VectorScanTableProvider`] left-joins the base table against
/// that plan to expose the embedding column for table scans.
///
/// Implementations: [`crate::index::s3_vectors::S3Vector`], [`crate::index::elasticsearch::ElasticsearchIndex`]
///
/// ## Co-located model
///
/// Embeddings are stored in the underlying accelerated table itself — there is no
/// separate novel store. [`VectorIndex::list_table_provider`] must return
/// [`DataFusionError::NotImplemented`] to signal this: the data is not held by the
/// index and cannot be enumerated via this trait. ANN search is provided at query time
/// through [`SearchIndex::query_table_provider`], which may build a transient index
/// structure (e.g. HNSW) over the stored embeddings.
///
/// Implementations: [`NativeVectorIndex`], [`crate::index::duckdb::DuckDBVectorIndex`]
pub trait VectorIndex: SearchIndex {
    /// A [`LogicalPlan`] enumerating the contents of the index (primary keys + embedding
    /// vectors). Required only for the **external-store model** — implementations that
    /// store embeddings in the underlying accelerated table must return
    /// [`DataFusionError::NotImplemented`] instead.
    ///
    /// When implemented, the schema must contain:
    ///  - The [`SearchIndex::primary_fields`]
    ///  - The embedding vector column: [`SearchIndex::search_column`] with `_embedding`
    ///    appended (e.g. `body_embedding`)
    ///
    /// [`VectorScanTableProvider`] calls this at construction time to build the join plan
    /// that exposes the embedding column on table scans. Co-located implementations must
    /// not be wrapped by [`VectorScanTableProvider`].
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError>;

    fn dimension(&self) -> i32;

    /// Returns column names in [`VectorIndex::list_table_provider`] and/or [`SearchIndex::query_table_provider`] that
    /// are derived by the vector index (e.g., embedding columns). These are columns that the index computes and adds,
    ///  rather than columns from the original data.
    fn derived_columns(&self) -> Vec<String> {
        vec![embedding_col(self.search_column().as_str())]
    }
}

fn embedding_col(search_column: &str) -> String {
    format!("{search_column}_embedding")
}
