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
pub mod s3_vectors;

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

pub trait VectorIndex: SearchIndex {
    /// A [`LogicalPlan`] representation of the data within the index. The [`LogicalPlan::schema`] must contain
    ///  - The [`SearchIndex::primary_fields`]
    ///  - All columns in [`SearchIndex::metadata_columns`]
    ///  - The associated embedding vectors of the [`SearchIndex::search_column`].
    ///
    /// The associated embedding vector column will be [`SearchIndex::search_column`] with `_embedding` appended (e.g. `body_embedding`).
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError>;

    fn dimension(&self) -> i32;
}

fn embedding_col(search_column: &str) -> String {
    format!("{search_column}_embedding")
}
