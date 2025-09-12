use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow_schema::{DataType, Field};
use async_trait::async_trait;
use chunking::Chunker;
use datafusion::catalog::TableProvider;

use crate::{
    index::SearchIndex,
    metadata::{MetadataColumn, MetadataColumns},
};

/// A [`SearchIndex`] that chunks the [`SearchIndex::search_column`] before each [`SearchIndex::write`].
///
/// Two new [`FieldRef`]s augment the table:
///   1. An index of the chunks position in the underlying search column. This is an additional element in [`SearchIndex::primary_fields`].
///   2. The start and end index of the chunk into the underlying search column. This is an additional [`MetadataColumn::NonFilterable`] in  [`SearchIndex::metadata_columns`].
pub struct ChunkedSearchIndex {
    inner: Arc<dyn SearchIndex>,
    chunker: Arc<dyn Chunker>,

    /// inner.metadata_columns() + chunk_offsets. Must store in struct for ref.
    metadata: MetadataColumns,
}

impl std::fmt::Debug for ChunkedSearchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedSearchIndex")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ChunkedSearchIndex {
    pub fn new(inner: Arc<dyn SearchIndex>, chunker: Arc<dyn Chunker>) -> Self {
        let mut metadata: Vec<MetadataColumn> =
            inner.metadata_columns().clone().into_iter().collect();
        metadata.push(MetadataColumn::NonFilterable(
            Field::new(
                "_spice.chunk_offset",
                DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 2),
                false,
            )
            .into(),
        ));

        Self {
            inner,
            chunker,
            metadata: metadata.into(),
        }
    }
}

#[async_trait]
impl SearchIndex for ChunkedSearchIndex {
    fn search_column(&self) -> String {
        // TODO: this might need a separate name?
        self.inner.search_column()
    }

    fn primary_fields(&self) -> Vec<Field> {
        [
            self.inner.primary_fields(),
            vec![Field::new("_spice.chunk_id", DataType::UInt64, false)],
        ]
        .concat()
    }

    fn metadata_columns(&self) -> &MetadataColumns {
        &self.metadata
    }

    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        Ok(record)
    }

    async fn query_table_provider(
        &self,
        query: &str,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::from("blame jeadie".to_string()))
    }
}
