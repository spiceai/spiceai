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

//! [`MemoryVectorIndex`] — an in-memory, external-store [`VectorIndex`].
//!
//! Follows the same storage model as [`crate::index::s3_vectors::S3Vector`]:
//! `write()` embeds the search column and stores primary keys, metadata, and
//! embedding vectors in the index's own store — here, RAM — while
//! [`VectorIndex::list_table_provider`] and
//! [`crate::index::SearchIndex::query_table_provider`] expose the store
//! contents as `LogicalPlan`s. Nearest-neighbor search is brute-force exact
//! k-NN over the SIMD distance kernels in `runtime-datafusion-udfs`.

use std::{any::Any, sync::Arc};

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::compute::filter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    logical_expr::LogicalPlan,
};
use datafusion_expr::{LogicalPlanBuilder, ScalarUDF};
use futures::future::try_join_all;
use itertools::Itertools;
use llms::embeddings::Embed;
use parking_lot::RwLock;
use runtime_datafusion_index::{Index, WriteWindow};
use snafu::{ResultExt, Snafu, ensure};
use spice_table::Index;

use crate::index::{
    SearchIndex, VectorIndex, embedding_col,
    memory::{
        provider::{MemoryVectorListTable, MemoryVectorQueryTable},
        store::MemoryVectorStore,
    },
    write_util,
};
use crate::metadata::{MetadataColumn, MetadataColumns};

mod provider;
mod store;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(transparent)]
    WriteUtil { source: write_util::Error },

    #[snafu(display(
        "Failed to create vector index '{index}' (memory): dimension must be positive, got {dimension}."
    ))]
    InvalidDimension { index: String, dimension: i32 },

    #[snafu(display(
        "Failed to create vector index '{index}' (memory): at least one primary key field is required."
    ))]
    NoPrimaryKeyField { index: String },

    #[snafu(display(
        "Cannot write to '{index}' index, as provided data has mismatch lengths. Embedding column '{column}' has {embedding_rows} rows, whilst the primary key has {primary_key_rows} rows."
    ))]
    LengthMismatch {
        index: String,
        column: String,
        embedding_rows: usize,
        primary_key_rows: usize,
    },

    #[snafu(display("Cannot write to '{index}' index, data does not have column '{column}'."))]
    ColumnNotFound { index: String, column: String },

    #[snafu(display(
        "Cannot write to '{index}' index, an issue processing arrow records: {source}."
    ))]
    IssueWithArrowProcessing {
        index: String,
        source: arrow::error::ArrowError,
    },
}

/// The distance metric used to score stored vectors against a query vector.
///
/// Score conventions match the other vector indexes (higher is better):
/// `Cosine` scores with cosine similarity, `L2` with negated Euclidean
/// distance, and `Dot` with the raw inner product.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemoryDistanceMetric {
    #[default]
    Cosine,
    L2,
    Dot,
}

impl TryFrom<&str> for MemoryDistanceMetric {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(Self::Cosine),
            "l2" | "l2_norm" | "euclidean" | "l2sq" => Ok(Self::L2),
            "ip" | "inner_product" | "dot" | "dot_product" | "max_inner_product" => Ok(Self::Dot),
            other => Err(format!(
                "Invalid memory vector distance metric '{other}'. Expected one of: cosine | l2 | dot."
            )),
        }
    }
}

/// An in-memory, external-store [`VectorIndex`] with brute-force exact k-NN.
///
/// - `write()` embeds the search column via the held [`Embed`] model and
///   upserts rows (keyed by formatted primary key — a re-written key replaces
///   the stored row) into an Arrow-native in-RAM store.
/// - [`VectorIndex::list_table_provider`] enumerates the store; the plan reads
///   the store lazily at scan time, so rows written after plan construction
///   are visible.
/// - [`SearchIndex::query_table_provider`] embeds the query lazily at scan
///   time and scores every stored row with SIMD distance kernels, ordered by
///   descending [`SEARCH_SCORE_COLUMN_NAME`].
#[derive(Debug, Clone)]
pub struct MemoryVectorIndex {
    embedded_column: String,
    primary_key: Vec<Field>,
    metadata_columns: MetadataColumns,
    embedder: Arc<dyn Embed>,
    embed_udf: Arc<ScalarUDF>,
    model_name: String,
    dimension: i32,
    metric: MemoryDistanceMetric,
    store: Arc<RwLock<MemoryVectorStore>>,
}

impl MemoryVectorIndex {
    /// Create an empty index.
    ///
    /// `dimension` must match the vectors produced by `embedder`.
    /// `query_scoring` is used to build query-time scoring plans.
    /// Any metadata column named like the embedding column is ignored
    /// (the embedding column is always stored).
    pub fn try_new(
        embedded_column: String,
        primary_key: Vec<Field>,
        metadata_columns: MetadataColumns,
        embedder: Arc<dyn Embed>,
        embed_udf: Arc<ScalarUDF>,
        model_name: String,
        metric: MemoryDistanceMetric,
    ) -> Result<Self, Error> {
        let dimension = embedder.size();
        ensure!(
            dimension > 0,
            InvalidDimensionSnafu {
                index: INDEX_NAME.to_string(),
                dimension,
            }
        );
        ensure!(
            !primary_key.is_empty(),
            NoPrimaryKeyFieldSnafu {
                index: INDEX_NAME.to_string(),
            }
        );
        let stored_schema =
            stored_schema(&embedded_column, &primary_key, &metadata_columns, dimension);
        Ok(Self {
            embedded_column,
            primary_key,
            metadata_columns,
            embedder,
            embed_udf,
            model_name,
            dimension,
            metric,
            store: Arc::new(RwLock::new(MemoryVectorStore::new(stored_schema))),
        })
    }

    /// Project the write-output batch down to the stored schema, dropping
    /// rows that cannot be indexed (null primary key, or a null/invalid
    /// embedding). Returns the filtered batch and its formatted keys.
    fn batch_for_store(
        &self,
        output: &RecordBatch,
        primary_keys: &[Option<String>],
        embedding_vectors: &[Option<Vec<f32>>],
    ) -> Result<(RecordBatch, Vec<String>), Error> {
        let mut keys = Vec::with_capacity(primary_keys.len());
        let mask: BooleanArray = primary_keys
            .iter()
            .zip(embedding_vectors.iter())
            .map(|(key, vector)| {
                let keep = match (key, vector) {
                    (Some(key), Some(vector)) => {
                        // All-zero / all-NaN vectors have no defined direction and
                        // would corrupt similarity scores — skip them.
                        let valid = !vector.iter().all(|&v| v == 0.0 || v.is_nan());
                        if valid {
                            keys.push(key.clone());
                        } else {
                            tracing::warn!(
                                "Skipping record '{key}' for memory vector index '{INDEX_NAME}': Embedding vector is all zeroes or contains only invalid values"
                            );
                        }
                        valid
                    }
                    (None, _) => {
                        tracing::warn!(
                            "Skipping a record for memory vector index '{INDEX_NAME}': the primary key is NULL"
                        );
                        false
                    }
                    (Some(_), None) => false, // NULL/empty search text — nothing to index.
                };
                Some(keep)
            })
            .collect();

        let output_schema = output.schema();
        let target_schema = Arc::clone(&self.store.read().stored_schema);
        let mut columns = Vec::with_capacity(target_schema.fields().len());
        for field in target_schema.fields() {
            let Some((idx, _)) = output_schema.column_with_name(field.name()) else {
                return ColumnNotFoundSnafu {
                    index: INDEX_NAME.to_string(),
                    column: field.name().clone(),
                }
                .fail();
            };
            let filtered: ArrayRef =
                filter(output.column(idx), &mask).context(IssueWithArrowProcessingSnafu {
                    index: INDEX_NAME.to_string(),
                })?;
            columns.push(filtered);
        }
        let batch = RecordBatch::try_new(target_schema, columns).context(
            IssueWithArrowProcessingSnafu {
                index: INDEX_NAME.to_string(),
            },
        )?;
        Ok((batch, keys))
    }
}

static INDEX_NAME: &str = "memory_vector_index";

/// Build the stored schema: primary-key fields + metadata fields + the
/// embedding column, alphabetically sorted by name (the order
/// `VectorScanTableProvider` and the index's `write()` output use).
fn stored_schema(
    embedded_column: &str,
    primary_key: &[Field],
    metadata_columns: &MetadataColumns,
    dimension: i32,
) -> SchemaRef {
    let embedding_column_name = embedding_col(embedded_column);
    let fields = primary_key
        .iter()
        .map(|f| Arc::new(f.clone()))
        .chain(
            metadata_columns
                .iter()
                .filter(|c| c.name() != embedding_column_name)
                .map(MetadataColumn::field),
        )
        .chain(std::iter::once(Arc::new(Field::new(
            &embedding_column_name,
            DataType::FixedSizeList(
                Arc::new(Field::new_list_field(DataType::Float32, false)),
                dimension,
            ),
            true,
        ))))
        .sorted_by(|a, b| a.name().cmp(b.name()))
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

#[async_trait]
impl Index for MemoryVectorIndex {
    fn name(&self) -> &'static str {
        INDEX_NAME
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn required_columns(&self) -> Vec<String> {
        let mut columns: Vec<_> = self
            .primary_key
            .iter()
            .map(arrow_schema::Field::name)
            .cloned()
            .collect();
        columns.push(self.embedded_column.clone());
        columns.extend(
            self.metadata_columns
                .iter()
                .filter(|c| c.name() != embedding_col(&self.embedded_column))
                .map(|c| c.name().to_string()),
        );
        columns
    }

    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let futs = batches
            .into_iter()
            .map(|rb| async { self.write(rb).await.map_err(DataFusionError::External) });
        try_join_all(futs).await
    }

    async fn delete_by_keys(&self, keys: RecordBatch) -> Result<(), DataFusionError> {
        let key_strings =
            write_util::extract_and_format_primary_key(INDEX_NAME, &self.primary_key, &keys)
                .map_err(|e| DataFusionError::External(Box::new(*e)))?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

        self.store.write().delete_by_keys(&key_strings)
    }

    /// Entries live in this index's own store, not in the accelerated table row, so a
    /// replacing write has to clear them: it removes a row by not re-sending it, which
    /// neither [`Index::compute_index`] nor [`Index::delete_by_keys`] can observe.
    ///
    /// The clear is staged rather than applied. The store is process-local, so the whole
    /// window can be built alongside the current contents and swapped in at
    /// [`Index::on_write_complete`] — a query during the refresh reads the previous
    /// contents, never a half-rebuilt index, and a refresh that fails leaves them in place.
    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        let mut store = self.store.write();
        match window {
            WriteWindow::ReplaceAll => store.begin_replace_window(),
            // An append has to land in the rows readers already see. A replace window whose
            // terminators never ran leaves the store staging, so without this the append would
            // be staged too and the `on_write_complete` that follows it would publish that
            // staged set as the whole index — dropping every row the abandoned window had
            // not re-sent. Discarding is a no-op in the usual case of no window open.
            WriteWindow::Append => store.abandon_replace_window(),
        }
        Ok(())
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.store.write().commit_replace_window();
        Ok(())
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.store.write().abandon_replace_window();
        Ok(())
    }

    // `write_start_failure_is_fatal` / `write_complete_failure_is_fatal` are deliberately left
    // at their `false` default even though the tantivy index overrides both for its own staged
    // window. All three callbacks above are infallible — the store is a lock away, with no I/O —
    // so there is no failure of this index's for either flag to classify. Returning `true`
    // anyway would not be inert: `CompoundVectorIndex` ORs the flags across its halves, so it
    // would promote the *other* half's best-effort failure (an Elasticsearch `refresh_interval`
    // override that could not be applied, a `_forcemerge` that failed) into one that fails the
    // whole write.
}

#[async_trait]
impl SearchIndex for MemoryVectorIndex {
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
        let Some((embedded_column_idx, _)) = record
            .schema()
            .column_with_name(self.embedded_column.as_str())
        else {
            tracing::warn!(
                "Cannot write to '{INDEX_NAME}' index, data does not have column '{}'.",
                self.embedded_column
            );
            return Ok(record);
        };

        // All awaits happen before the store lock is taken (the lock is
        // synchronous and must never be held across an await).
        let embedding_vectors =
            write_util::embed_column(&record, embedded_column_idx, Arc::clone(&self.embedder))
                .await
                .map_err(Error::from)?;
        let primary_keys =
            write_util::extract_and_format_primary_key(INDEX_NAME, &self.primary_key, &record)
                .map_err(|e| Error::from(*e))?;

        ensure!(
            primary_keys.len() == embedding_vectors.len(),
            LengthMismatchSnafu {
                index: INDEX_NAME.to_string(),
                column: self.embedded_column.clone(),
                embedding_rows: embedding_vectors.len(),
                primary_key_rows: primary_keys.len(),
            }
        );

        let updated = write_util::update_embedding_column_in_batch(
            &record,
            &self.embedded_column,
            &embedding_vectors,
            self.dimension,
        )
        .map_err(|e| Error::from(*e))?;

        // Because of limitations of `DFSchema::logically_equivalent_names_and_types` and its
        // use in `MemTable`, this must be in the same order as outputted by
        // `VectorScanTableProvider`.
        let output =
            write_util::sort_columns_alphabetically(updated).map_err(|e| Error::from(*e))?;

        let (store_batch, keys) =
            self.batch_for_store(&output, &primary_keys, &embedding_vectors)?;
        self.store.write().upsert(store_batch, keys)?;

        Ok(output)
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(self as Arc<dyn VectorIndex>)
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        Ok(LogicalPlanBuilder::scan(
            "tbl",
            Arc::new(DefaultTableSource::new(
                Arc::new(MemoryVectorQueryTable::new(
                    INDEX_NAME.to_string(),
                    Arc::clone(&self.store),
                    Arc::clone(&self.embed_udf),
                    self.model_name.clone(),
                    query.to_string(),
                    self.metric,
                    embedding_col(&self.embedded_column),
                )) as Arc<dyn TableProvider>,
            )),
            None,
        )?
        .build()?
        .into())
    }
}

impl VectorIndex for MemoryVectorIndex {
    fn dimension(&self) -> i32 {
        self.dimension
    }

    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        LogicalPlanBuilder::scan(
            "tbl",
            Arc::new(DefaultTableSource::new(
                Arc::new(MemoryVectorListTable::new(Arc::clone(&self.store)))
                    as Arc<dyn TableProvider>,
            )),
            None,
        )?
        .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use datafusion_expr::{Volatility, create_udf};
    use llms::embeddings::EmbeddingInput;

    const DIM: i32 = 3;

    /// Deterministic, model-free embedder: maps a string to a vector derived from its bytes.
    #[derive(Debug)]
    struct ByteEmbed;

    fn byte_vector(text: &str) -> Vec<f32> {
        let dim = usize::try_from(DIM).expect("DIM is positive");
        let mut vector = vec![0.0_f32; dim];
        for (i, b) in text.bytes().enumerate() {
            vector[i % dim] += f32::from(b) / 255.0;
        }
        vector
    }

    #[async_trait]
    impl Embed for ByteEmbed {
        async fn embed(&self, input: EmbeddingInput) -> llms::embeddings::Result<Vec<Vec<f32>>> {
            match input {
                EmbeddingInput::String(s) => Ok(vec![byte_vector(&s)]),
                EmbeddingInput::StringArray(v) => Ok(v.iter().map(|s| byte_vector(s)).collect()),
                _ => Ok(vec![]),
            }
        }

        fn size(&self) -> i32 {
            DIM
        }
    }

    /// These tests never execute a query plan, so the query-time `embed(text, model)` UDF is
    /// only needed to construct the index.
    fn embed_udf() -> Arc<ScalarUDF> {
        Arc::new(create_udf(
            "embed",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
            Volatility::Volatile,
            Arc::new(|_args| {
                Err(DataFusionError::Execution(
                    "the memory index tests do not execute query plans".to_string(),
                ))
            }),
        ))
    }

    fn memory_index() -> MemoryVectorIndex {
        MemoryVectorIndex::try_new(
            "content".to_string(),
            vec![Field::new("id", DataType::Int64, false)],
            MetadataColumns::none(),
            Arc::new(ByteEmbed),
            embed_udf(),
            "model_name".to_string(),
            MemoryDistanceMetric::Cosine,
        )
        .expect("valid memory index")
    }

    /// One row per id, with `content` derived from the id so each row embeds distinctly.
    fn batch(ids: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
        ]));
        let contents: Vec<String> = ids.iter().map(|id| format!("row {id}")).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(contents)),
            ],
        )
        .expect("valid test batch")
    }

    /// The ids a query would find in the index, in ascending order.
    fn indexed_ids(index: &MemoryVectorIndex) -> Vec<i64> {
        let mut ids: Vec<i64> = index
            .store
            .read()
            .batches()
            .iter()
            .flat_map(|b| {
                let (idx, _) = b
                    .schema()
                    .column_with_name("id")
                    .expect("the stored schema carries the primary key");
                b.column(idx)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id is Int64")
                    .values()
                    .to_vec()
            })
            .collect();
        ids.sort_unstable();
        ids
    }

    async fn write_window(index: &MemoryVectorIndex, window: WriteWindow, ids: &[i64]) {
        index
            .on_write_start(window)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(ids)])
            .await
            .expect("the rows are indexed");
        index
            .on_write_complete()
            .await
            .expect("the write window closes");
    }

    /// The regression test. A `refresh_mode: full` refresh removes a row by not re-sending
    /// it — it announces no deletion — so before this fix the index kept a vector for row 2
    /// and searches went on returning it after `SELECT` had stopped.
    #[tokio::test]
    async fn a_full_refresh_drops_entries_for_rows_it_did_not_resend() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2, 3]).await;
        assert_eq!(indexed_ids(&index), vec![1, 2, 3]);

        write_window(&index, WriteWindow::ReplaceAll, &[1, 3]).await;

        assert_eq!(
            indexed_ids(&index),
            vec![1, 3],
            "a replacing write reproduces the whole table, so a row it does not carry is a \
             row the source dropped"
        );
    }

    /// The clear is staged, not applied: the window is built alongside the current contents
    /// so a search during the refresh is served the previous rows rather than a partially
    /// rebuilt index.
    #[tokio::test]
    async fn rows_stay_readable_while_a_full_refresh_rebuilds_them() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2, 3]).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(&[4])])
            .await
            .expect("the rows are indexed");

        assert_eq!(
            indexed_ids(&index),
            vec![1, 2, 3],
            "an uncommitted refresh must not be visible, in whole or in part"
        );

        index
            .on_write_complete()
            .await
            .expect("the write window closes");
        assert_eq!(indexed_ids(&index), vec![4]);
    }

    /// A refresh that fails partway leaves the index exactly as it was, rather than serving
    /// whatever fraction of the new contents had arrived.
    #[tokio::test]
    async fn a_failed_full_refresh_leaves_the_previous_rows_in_place() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2, 3]).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(&[9])])
            .await
            .expect("the rows are indexed");
        index
            .on_write_failed()
            .await
            .expect("the write window is abandoned");

        assert_eq!(indexed_ids(&index), vec![1, 2, 3]);
    }

    /// An append adds to what the index holds. Clearing on this window would drop every row
    /// the append did not happen to carry — the failure the `WriteWindow` distinction exists
    /// to avoid, and the one a CDC change batch would hit.
    #[tokio::test]
    async fn an_append_keeps_the_rows_it_does_not_resend() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2]).await;

        write_window(&index, WriteWindow::Append, &[3]).await;

        assert_eq!(indexed_ids(&index), vec![1, 2, 3]);
    }

    /// A write that never opened a window at all — the CDC path, which the sink lifecycle
    /// does not wrap — keeps writing straight into the readable rows.
    #[tokio::test]
    async fn a_write_outside_any_window_lands_in_the_readable_rows() {
        let index = memory_index();
        index
            .compute_index(vec![batch(&[1, 2])])
            .await
            .expect("the rows are indexed");

        assert_eq!(indexed_ids(&index), vec![1, 2]);
    }

    /// A refresh abandoned without either terminator running — a cancelled refresh, a
    /// restart — must not have its rows swept into the next one's commit.
    #[tokio::test]
    async fn an_abandoned_window_does_not_leak_into_the_next_one() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1]).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(&[7])])
            .await
            .expect("the rows are indexed");

        // No `on_write_complete` / `on_write_failed`; the next refresh simply starts.
        write_window(&index, WriteWindow::ReplaceAll, &[2]).await;

        assert_eq!(
            indexed_ids(&index),
            vec![2],
            "row 7 belonged to a refresh that never completed"
        );
    }

    /// The same abandoned window, followed by an append rather than another refresh. The
    /// append has to land in the rows readers see: staged into the window the cancelled
    /// refresh left open, its `on_write_complete` would publish that staged set as the
    /// entire index and drop every row the abandoned refresh had not re-sent.
    #[tokio::test]
    async fn an_abandoned_window_does_not_capture_the_next_append() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2]).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(&[7])])
            .await
            .expect("the rows are indexed");

        // No `on_write_complete` / `on_write_failed`; an append simply follows.
        write_window(&index, WriteWindow::Append, &[3]).await;

        assert_eq!(
            indexed_ids(&index),
            vec![1, 2, 3],
            "row 7 belonged to a refresh that never completed, and an append must not \
             replace the rows it did not carry"
        );
    }

    /// A delete arriving inside a replace window acts on the rows being staged, so the
    /// deleted row does not reappear when the window is published.
    #[tokio::test]
    async fn a_delete_inside_a_replace_window_applies_to_the_staged_rows() {
        let index = memory_index();
        write_window(&index, WriteWindow::Append, &[1, 2]).await;

        index
            .on_write_start(WriteWindow::ReplaceAll)
            .await
            .expect("the write window opens");
        index
            .compute_index(vec![batch(&[1, 2, 3])])
            .await
            .expect("the rows are indexed");
        index
            .delete_by_keys(batch(&[2]))
            .await
            .expect("the delete applies");
        index
            .on_write_complete()
            .await
            .expect("the write window closes");

        assert_eq!(indexed_ids(&index), vec![1, 3]);
    }
}
