use std::{
    any::Any,
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, VectorIndex, embedding_col},
    metadata::MetadataColumn,
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, FixedSizeListBuilder, Int32Builder,
        LargeStringArray, ListArray, RecordBatch, StringArray, StringViewArray, UInt64Array,
    },
    buffer::{BooleanBuffer, OffsetBuffer},
    compute::{concat, filter_record_batch},
    row::{RowConverter, SortField},
};

use crate::index::primary_key_projection;
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chunking::Chunker;
use datafusion::{
    common::Column,
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionContext,
    functions_aggregate::expr_fn::{array_agg, first_value},
    logical_expr::{Aggregate, LogicalPlan, LogicalPlanBuilder, Sort, SortExpr, expr::Alias},
    prelude::{Expr, ExprFunctionExt, col},
    sql::TableReference,
};
use datafusion_expr::ident;
use futures::future::try_join_all;
use itertools::Itertools;
use snafu::{ResultExt, Snafu};
use spice_table::{Index, WriteWindow, build_key_match_predicate};
use util::{arrow::repeat, convert_string_arrow_to_iterator};

/// Additional primary key column to uniquely identify chunks within a single database row.
pub static CHUNKED_INDEX_CHUNK_KEY: &str = "_spice.chunk_id";

/// Soft cap on the number of chunk rows passed to a single [`SearchIndex::write`] call on the
/// inner index. A single input row whose chunk count exceeds this is processed atomically — the
/// cap is a per-group budget, not a hard split point. Picked to align with `DataFusion`'s default
/// `execution.batch_size` so the intermediate chunked batch never significantly exceeds it.
const INNER_WRITE_TARGET_CHUNKS: usize = 8192;

/// Additional metadata field to store in underlying search index. This is only used when the
/// underlying index has [`SearchIndex::search_column`] in [`SearchIndex::metadata_columns`].
pub static CHUNKED_INDEX_FULL_SEARCH_FIELD: &str = "_spice.search_field";

/// A [`SearchIndex`] that chunks the [`SearchIndex::search_column`] before each [`SearchIndex::write`].
///
/// Two new [`FieldRef`]s augment the table:
///   1. An index of the chunks position in the underlying search column. This is an additional element in [`SearchIndex::primary_fields`].
///   2. The start and end index of the chunk into the underlying search column. This is an additional [`MetadataColumn::NonFilterable`] in  [`SearchIndex::metadata_columns`].
pub struct ChunkedSearchIndex {
    inner: Arc<dyn SearchIndex>,
    chunker: Arc<dyn Chunker>,
    /// Cached `{search_column}_embedding` — avoids reallocating the name on every write.
    embedding_col_name: String,
    /// Cached `{search_column}_offset` — avoids reallocating the name on every write.
    offset_col_name: String,
    /// Cached names of the base row's primary-key columns — avoids re-deriving them from
    /// [`SearchIndex::primary_fields`], which clones every `Field`, on every write.
    base_key_names: Vec<String>,
    /// Set for the duration of a [`WriteWindow::ReplaceAll`] window. See
    /// [`Self::evict_rows_chunked_to_nothing`], which is where it is read and why it exists.
    replace_window: AtomicBool,
}

#[async_trait]
impl Index for ChunkedSearchIndex {
    fn name(&self) -> &'static str {
        "ChunkedSearchIndex"
    }

    /// Columns that are required for the index to be computed.
    fn required_columns(&self) -> Vec<String> {
        let mut cols = self.inner.required_columns();
        cols.retain(|s| {
            s != CHUNKED_INDEX_CHUNK_KEY
                && *s != Self::chunking_offset_col(self.search_column().as_str())
        });
        cols
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

    /// Records whether this window replaces the table's contents before forwarding, so
    /// [`ChunkedSearchIndex::evict_rows_chunked_to_nothing`] can skip a window where the inner
    /// index is staging rather than serving what it is about to hold.
    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        self.replace_window
            .store(window == WriteWindow::ReplaceAll, Ordering::Release);
        self.inner.on_write_start(window).await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.replace_window.store(false, Ordering::Release);
        self.inner.on_write_failed().await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.replace_window.store(false, Ordering::Release);
        self.inner.on_write_complete().await
    }

    /// `keys` is shaped by [`SearchIndex::primary_fields`], which excludes
    /// [`CHUNKED_INDEX_CHUNK_KEY`] — so every chunk row for a given outer key must go, not just
    /// one. `self.inner`'s own key includes the chunk id, whose values this doesn't know.
    ///
    /// An inner index that reports [`Index::deletes_by_partial_key`] deletes that whole group
    /// from the outer key alone, so hand it straight over. Otherwise the exact chunk-keyed rows
    /// have to be resolved out of `self.inner`'s own data first, which needs a listable
    /// [`VectorIndex`] (see [`delete_chunked_vector_by_outer_keys`]; plain full-text indexes have
    /// no generic "list everything" surface to query).
    async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
        if self.inner.deletes_by_partial_key() {
            return self.inner.delete_by_keys(keys).await;
        }
        let Some(inner_vector) = Arc::clone(&self.inner).as_vector_index() else {
            return Err(DataFusionError::NotImplemented(
                "Deleting from a chunked non-vector search index is not yet supported (no way to \
                 enumerate its existing chunks for a given primary key)"
                    .to_string(),
            ));
        };
        delete_chunked_vector_by_outer_keys(&inner_vector, keys).await
    }

    fn deletes_by_partial_key(&self) -> bool {
        self.inner.deletes_by_partial_key()
    }

    fn write_start_failure_is_fatal(&self) -> bool {
        self.inner.write_start_failure_is_fatal()
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        self.inner.write_complete_failure_is_fatal()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Deletes every entry in `inner` whose primary-key columns match a row of `outer_keys` (which
/// carry only the outer/pre-chunk key columns — `inner`'s own key additionally has the chunk id,
/// whose values aren't known here). Resolves the exact matching entries by scanning `inner`'s own
/// [`VectorIndex::list_all_entry_keys`] with a predicate built from `outer_keys`, then deletes
/// those resolved (chunk-key-included) rows via `inner`'s normal [`Index::delete_by_keys`].
///
/// Resolution goes through [`VectorIndex::list_all_entry_keys`], not
/// [`VectorIndex::list_table_provider`]: the read listing can be narrower than what the index
/// stores (a compound index's read mode may serve only its warm primary), and a chunk entry that
/// does not resolve is never passed to [`Index::delete_by_keys`] at all — the delete then reports
/// success having removed nothing.
async fn delete_chunked_vector_by_outer_keys(
    inner: &Arc<dyn VectorIndex>,
    outer_keys: RecordBatch,
) -> DataFusionResult<()> {
    // Only the true outer primary key — not every column `outer_keys` happens to carry (it's
    // shaped by `required_columns`, a superset that includes the search column and other
    // metadata). `inner`'s stored value for those extra columns is chunk-specific (e.g. a
    // fragment of the original search column), so matching on them would never equal the
    // original row and silently leave chunks undeleted.
    let outer_columns = ChunkedSearchIndex::base_key_columns(&inner.primary_fields());
    let Some(predicate) = build_key_match_predicate(&outer_keys, &outer_columns)? else {
        return Ok(());
    };

    // `delete_by_keys` reads only the primary-key columns, so project to them and drop the
    // duplicates a multi-store listing can carry (the same entry held by more than one half).
    // This also keeps the embedding vectors — the bulk of the listing — out of a delete.
    let key_projection = primary_key_projection(&inner.primary_fields());

    let list_plan = inner.list_all_entry_keys()?;
    let filtered_plan = LogicalPlanBuilder::from(list_plan)
        .filter(predicate)?
        .project(key_projection)?
        .distinct()?
        .build()?;

    let ctx = SessionContext::new();
    let matches = ctx
        .execute_logical_plan(filtered_plan)
        .await?
        .collect()
        .await?;

    for batch in matches {
        if batch.num_rows() > 0 {
            inner.delete_by_keys(batch).await?;
        }
    }

    Ok(())
}

/// The warning a write emits when a row's search value is now empty and the chunks its previous
/// text produced cannot be reached — the index can only be addressed by a complete key, and the
/// chunk ids making up that key are not knowable without listing what it holds.
///
/// A pure function so the wording is asserted in a unit test: it is the only account a user gets
/// of why a search still returns text a row no longer has.
fn unreachable_chunk_eviction_warning(index: &str, search_column: &str) -> String {
    format!(
        "Failed to remove the search index entries for a row whose '{search_column}' value is now empty, so that row's previous text stays searchable and a search can still return content the row no longer has. The `{index}` index can only be addressed by a complete key and cannot list what it holds, so those entries cannot be found to remove them. Re-create the search index to rebuild it from the rows the dataset holds now. See: https://spiceai.org/docs/features/search"
    )
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Could not write to search index. Provided data does not have search column '{search_column}'. Columns present: {}.", schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .join(", ")
    ))]
    WriteFailedNoSearchColumn {
        search_column: String,
        schema: SchemaRef,
    },

    #[snafu(display(
        "Cannot write search column '{search_column}' into search index. Expecting string-like type, found {data_type}"
    ))]
    WriteFailedSearchColumnNoString {
        search_column: String,
        data_type: DataType,
    },

    #[snafu(display("Failed to write search index: could not construct chunked data: {source}"))]
    WriteFailedConstructRecordBatch { source: ArrowError },

    #[snafu(display(
        "Writing chunked data to search index failed due to underlying index error: {source}"
    ))]
    InnerIndexWriteError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub fn is_chunked(idx: &Arc<dyn SearchIndex>) -> bool {
    idx.as_any().downcast_ref::<ChunkedSearchIndex>().is_some()
}

impl std::fmt::Debug for ChunkedSearchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedSearchIndex")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl ChunkedSearchIndex {
    #[must_use]
    pub fn chunking_offset_col(search_column: &str) -> String {
        format!("{search_column}_offset")
    }

    /// The names of the *base* row's primary-key columns, given a chunked index's primary key:
    /// everything [`augment_primary_key`](Self::augment_primary_key) did not add. These identify
    /// a source row, and so the whole group of chunk entries stored under it.
    #[must_use]
    pub fn base_key_columns(pk: &[Field]) -> Vec<String> {
        pk.iter()
            .filter(|f| f.name() != CHUNKED_INDEX_CHUNK_KEY)
            .map(|f| f.name().clone())
            .collect()
    }

    #[must_use]
    pub fn augment_primary_key(pk: Vec<Field>) -> Vec<Field> {
        [
            pk,
            vec![Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false)],
        ]
        .concat()
    }

    #[must_use]
    pub fn additional_metadata(
        search_column: &str,
        search_field: Option<MetadataColumn>,
    ) -> Vec<MetadataColumn> {
        let mut additional = vec![MetadataColumn::NonFilterable(
            Field::new(
                Self::chunking_offset_col(search_column),
                DataType::FixedSizeList(Field::new("item", DataType::Int32, false).into(), 2),
                false,
            )
            .into(),
        )];

        // Need to add `CHUNKED_INDEX_FULL_SEARCH_FIELD` as metadata field to underlying index.
        if let Some(search_field_metadata) = search_field {
            let new_field = Arc::unwrap_or_clone(search_field_metadata.field())
                .with_name(CHUNKED_INDEX_FULL_SEARCH_FIELD);
            match search_field_metadata {
                MetadataColumn::Filterable(_) => {
                    additional.push(MetadataColumn::Filterable(new_field.into()));
                }
                MetadataColumn::NonFilterable(_) => {
                    additional.push(MetadataColumn::NonFilterable(new_field.into()));
                }
            }
        }
        additional
    }

    pub fn new(inner: Arc<dyn SearchIndex>, chunker: Arc<dyn Chunker>) -> Self {
        let search_column = inner.search_column();
        Self {
            embedding_col_name: embedding_col(search_column.as_str()),
            offset_col_name: Self::chunking_offset_col(search_column.as_str()),
            base_key_names: Self::base_key_columns(&inner.primary_fields()),
            inner,
            chunker,
            replace_window: AtomicBool::new(false),
        }
    }

    /// The index this chunking wrapper writes chunked batches through to.
    #[must_use]
    pub fn inner(&self) -> &Arc<dyn SearchIndex> {
        &self.inner
    }

    /// The rows of `record` whose chunks the inner index has to be told to drop, or `None` when
    /// there are none.
    ///
    /// Those are the rows this write chunked into nothing — a NULL search value, an empty one,
    /// or text the chunker yields no chunk for — **minus** any row whose key this same write
    /// also produced chunks for. A row that chunked into nothing contributes no rows *at all* to
    /// the batch the inner index receives, so the inner index never sees that key on this write
    /// and nothing tells it to drop what the row's previous text produced.
    ///
    /// The subtraction is what keeps the eviction from ever removing a chunk this write just
    /// produced. It only bites when one batch carries the same key twice — once with text and
    /// once without — which is already resolved last-write-wins downstream and is not resolved
    /// here (#13713). Without it, evicting after the writes would delete the chunks the other
    /// row wrote.
    ///
    /// `repeats` is parallel to `record`'s rows and holds each row's chunk count.
    fn rows_to_evict(
        &self,
        record: &RecordBatch,
        repeats: &[usize],
    ) -> DataFusionResult<Option<RecordBatch>> {
        // Short-circuit before anything is allocated: on an ordinary write every row chunks into
        // something, and this is the only work that write should pay for.
        let emptied = repeats.iter().filter(|n| **n == 0).count();
        if emptied == 0 {
            return Ok(None);
        }

        // A key of no columns addresses nothing, so there is no group to evict.
        if self.base_key_names.is_empty() {
            return Ok(None);
        }

        let key_indices: Vec<usize> = self
            .base_key_names
            .iter()
            .map(|name| record.schema().index_of(name).ok())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Failed to update the search index on '{}': the rows being written do not \
                     carry the key column(s) {:?} the index is addressed by, so a row whose \
                     value is now empty cannot have its previous entries removed and a search \
                     would keep returning them.",
                    self.search_column(),
                    self.base_key_names
                ))
            })?;

        // `delete_by_keys` reads the key columns out of whatever batch it is handed and ignores
        // the rest, so narrow to them first: `project` re-slices the column vec without copying,
        // where filtering the whole batch would run a kernel over the search text and the
        // embedding vectors only to discard them.
        let keys = record.project(&key_indices)?;

        // Compare whole key tuples, whatever their column types, through Arrow's comparable row
        // encoding.
        let converter = RowConverter::new(
            keys.columns()
                .iter()
                .map(|a| SortField::new(a.data_type().clone()))
                .collect(),
        )?;
        let rows = converter.convert_columns(keys.columns())?;

        // Keyed on the emptied rows rather than the written ones: the emptied set is the small
        // one, and it is the side being subtracted from.
        let mut candidates = HashSet::with_capacity(emptied);
        for (i, _) in repeats.iter().enumerate().filter(|(_, n)| **n == 0) {
            candidates.insert(rows.row(i));
        }
        for (i, _) in repeats.iter().enumerate().filter(|(_, n)| **n > 0) {
            candidates.remove(&rows.row(i));
        }
        if candidates.is_empty() {
            return Ok(None);
        }

        // Non-nullable by construction, so `filter_record_batch` takes its fast path.
        let evict = BooleanArray::new(
            BooleanBuffer::collect_bool(keys.num_rows(), |i| {
                repeats[i] == 0 && candidates.contains(&rows.row(i))
            }),
            None,
        );

        filter_record_batch(&keys, &evict)
            .map(Some)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    /// Tell the inner index to drop what it still holds for [`Self::rows_to_evict`], so a row
    /// whose text went away stops answering searches with content it no longer has.
    ///
    /// Runs **after** this write's chunks have landed, not before: the deletion is externally
    /// visible and nothing restores it, so ordering it first would let a failed embedding or
    /// bulk write leave the row present and its old chunks already gone.
    ///
    /// Skipped inside a [`WriteWindow::ReplaceAll`] window, where it would be destructive: an
    /// index that stages a replacing write keeps serving its *previous* rows until it commits,
    /// so an eviction resolved against that listing resolves the previous contents' keys and
    /// applies the delete to the staged rows — removing rows this same write just wrote. The
    /// backends that *don't* stage discard nothing at all for that window, including entries for
    /// rows the refresh dropped outright, which is #12413 rather than this path.
    ///
    /// An inner index that can neither delete by partial key nor be enumerated as a vector index
    /// has no way to reach those chunks at all. That is asked up front, so the write reports it
    /// and stands — failing it after the chunks have landed would not remove the stale entries
    /// either — and every error from the delete itself stays an error.
    async fn evict_rows_chunked_to_nothing(
        &self,
        record: &RecordBatch,
        repeats: &[usize],
    ) -> DataFusionResult<()> {
        if self.replace_window.load(Ordering::Acquire) {
            return Ok(());
        }

        // Ask the capability rather than inferring it from the shape of a failure: a
        // `NotImplemented` raised deeper down is a real failure of a supported path, and
        // reporting it as "this index cannot be addressed" would tell the user a story the
        // error never told.
        if !self.deletes_by_partial_key() && Arc::clone(&self.inner).as_vector_index().is_none() {
            if repeats.contains(&0) {
                tracing::warn!(
                    "{}",
                    unreachable_chunk_eviction_warning(self.inner.name(), &self.search_column())
                );
            }
            return Ok(());
        }

        let Some(keys) = self.rows_to_evict(record, repeats)? else {
            return Ok(());
        };

        self.delete_by_keys(keys).await
    }

    /// Build the intermediate "chunked" [`RecordBatch`] for a contiguous group of input rows
    /// (`record[start..start+length]`). Non-search columns are repeated per chunk; the search
    /// column is replaced with the flattened chunk strings; `_spice.chunk_id` and the offset
    /// column are appended. Pre-existing embedding/offset columns on the input are dropped.
    #[expect(clippy::too_many_arguments)]
    fn build_chunked_record_batch(
        &self,
        record: &RecordBatch,
        schema: &SchemaRef,
        search_field_idx: usize,
        start: usize,
        length: usize,
        offsets: &[Vec<(usize, usize)>],
        chunks: &[Vec<&str>],
        repeats: &[usize],
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let group_record = record.slice(start, length);
        let group_offsets = &offsets[start..start + length];
        let group_chunks = &chunks[start..start + length];
        let group_repeats = &repeats[start..start + length];

        let group_chunk_index: Vec<u64> = group_chunks
            .iter()
            .flat_map(|v| 0..(v.len() as u64))
            .collect();
        let group_flatten_chunks: Vec<&str> = group_chunks
            .iter()
            .flat_map(|v| v.iter().copied())
            .collect();

        let search_field_array = group_record.column(search_field_idx);

        // Names of columns that may be present on the input but must NOT survive into the
        // intermediate chunked batch — they are recomputed by the inner index and re-attached
        // as list columns on the final output. Filtering them out *before* `repeat()` avoids
        // expensive `take()` work on nested list columns that would be discarded immediately.
        let (mut fields, mut arrays): (Vec<Field>, Vec<ArrayRef>) = group_record
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, arr)| {
                let field = schema.field(i).clone();
                if field.name() == &self.embedding_col_name || field.name() == &self.offset_col_name
                {
                    return None;
                }
                let result = if i == search_field_idx {
                    // Build string arrays from `&str` slices rather than an intermediate `Vec<String>`.
                    let chunked_array: ArrayRef = match field.data_type() {
                        DataType::LargeUtf8 => {
                            Arc::new(LargeStringArray::from(group_flatten_chunks.clone()))
                        }
                        DataType::Utf8View => {
                            Arc::new(StringViewArray::from(group_flatten_chunks.clone()))
                        }
                        _ => Arc::new(StringArray::from(group_flatten_chunks.clone())),
                    };
                    Ok((field, chunked_array))
                } else if schema
                    .column_with_name(CHUNKED_INDEX_FULL_SEARCH_FIELD)
                    .is_some_and(|(idx, _)| i == idx)
                {
                    // If self.search_field is in self.inner.metadata_columns, we must add an
                    // additional column. This column will have the full content. During
                    // list/search we shall ask for this column instead of the chunked version.
                    // The chunked version must be provided to `self.inner` so that it can be
                    // indexed appropriately (e.g. embedded).
                    repeat(search_field_array, group_repeats).map(|a| (field, a))
                } else {
                    repeat(arr, group_repeats).map(|a| (field, a))
                };
                Some(result)
            })
            .collect::<Result<Vec<_>, ArrowError>>()?
            .into_iter()
            .unzip();

        fields.push(Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false));
        arrays.push(Arc::new(UInt64Array::from(group_chunk_index)) as ArrayRef);

        fields.push(Field::new(
            self.offset_col_name.clone(),
            DataType::new_fixed_size_list(DataType::Int32, 2, false),
            false,
        ));
        arrays.push(Arc::new(to_offset_array(group_offsets, false)) as ArrayRef);

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .context(WriteFailedConstructRecordBatchSnafu)
            .boxed()
    }
}

/// Group consecutive input rows into slices `(start, length)` such that each slice's
/// `sum(repeats[start..start+length])` does not exceed `budget`, unless a single row's chunk
/// count already exceeds `budget` — in which case that row forms a slice of one. Rows are never
/// split across slices.
fn group_rows_by_chunk_budget(repeats: &[usize], budget: usize) -> Vec<(usize, usize)> {
    if repeats.is_empty() {
        return Vec::new();
    }
    let mut groups = Vec::new();
    let mut start = 0;
    let mut acc = 0usize;
    for (i, &c) in repeats.iter().enumerate() {
        if i > start && acc.saturating_add(c) > budget {
            groups.push((start, i - start));
            start = i;
            acc = 0;
        }
        acc = acc.saturating_add(c);
    }
    groups.push((start, repeats.len() - start));
    groups
}

/// Concatenate the per-group `values` arrays (one per row group, length = total chunks in that
/// group) into a single flat value buffer, then wrap as a [`ListArray`] with one outer entry per
/// original row using `OffsetBuffer::from_lengths(repeats)`. Writes the result into `arrs`,
/// replacing the existing column at `name` if present, otherwise appending.
fn attach_list_column(
    values: &[ArrayRef],
    name: &str,
    repeats: &[usize],
    schema: &SchemaRef,
    arrs: &mut Vec<ArrayRef>,
    fields: &mut Vec<Arc<Field>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if values.is_empty() {
        return Ok(());
    }
    let value_refs: Vec<&dyn Array> = values.iter().map(AsRef::as_ref).collect();
    let concatenated = concat(&value_refs).boxed()?;
    let item_field = Arc::new(Field::new("item", concatenated.data_type().clone(), true));
    let list_arr = Arc::new(
        ListArray::try_new(
            Arc::clone(&item_field),
            OffsetBuffer::from_lengths(repeats.iter().copied()),
            concatenated,
            None,
        )
        .boxed()?,
    );
    if let Some((i, _)) = schema.column_with_name(name) {
        arrs[i] = list_arr;
    } else {
        arrs.push(list_arr);
        fields.push(Arc::new(Field::new_list(
            name,
            Arc::unwrap_or_clone(item_field),
            false,
        )));
    }
    Ok(())
}

#[async_trait]
impl SearchIndex for ChunkedSearchIndex {
    fn search_column(&self) -> String {
        self.inner.search_column()
    }

    fn primary_fields(&self) -> Vec<Field> {
        self.inner
            .primary_fields()
            .into_iter()
            .filter(|pk| pk.name() != CHUNKED_INDEX_CHUNK_KEY)
            .collect::<Vec<_>>()
    }

    /// The vector view delegates its chunking back to *this* object rather than to a copy, so
    /// the write window one of them opens is the window the other one reads.
    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        let inner = Arc::clone(&self.inner).as_vector_index()?;
        Some(Arc::new(ChunkedVectorIndex {
            inner,
            delegate: self,
        }))
    }

    /// Consider a [`RecordBatch`] like this where `question` is the search column, and `id` is the primary key.
    /// +-----+--------------------------------------------------------------------------------------------------------------------------------+--------------------+
    /// | id  | question                                                                                                                       | source             |
    /// +-----+--------------------------------------------------------------------------------------------------------------------------------+--------------------+
    /// | 33  | Are there drug interactions with sipuleucel-T?                                                                                 | textbook_reasoning |
    /// | 49  | Can a router in Area 0 running OSPF process ID 2 swap LSAs with a router in Area 0 running OSPF process ID 10?                 | textbook_reasoning |
    /// | 87  | Convert the sentence "A series converges whenever it converges absolutely" into a sentence having the form "If $P$, then $Q$." | textbook_reasoning |
    /// | 115 | Do low frequencies mask high ones easily?                                                                                      | textbook_reasoning |
    /// | 116 | Do planning and scheduling mean the same thing? (Yes | No)                                                                     | textbook_reasoning |
    /// +-----+--------------------------------------------------------------------------------------------------------------------------------+--------------------+
    ///
    /// Becomes
    /// +-----+------------------------------------------------------+----------|-----------|--------------------+
    /// | id  | question                                             | chunk_id | offsets   | source             |
    /// +-----+------------------------------------------------------+----------|-----------|--------------------+
    /// | 33  | Are there drug interactions                          | 0        | [0, 27]   | textbook_reasoning |
    /// | 33  | with sipuleucel-T?                                   | 1        | [27, 45]  | textbook_reasoning |
    /// | 49  | Can a router in Area 0 running OSPF process          | 0        | [0, 44]   | textbook_reasoning |
    /// | 49  |  ID 2 swap LSAs with a router in Area 0 running      | 1        | [44, 90]  | textbook_reasoning |
    /// | 49  |  OSPF process ID 10?                                 | 2        | [90, 110] | textbook_reasoning |
    /// | 87  | Convert the sentence "A series converges whenever it | 0        | [0, 52]   | textbook_reasoning |
    /// | 87  | converges absolutely" into a sentence having         | 0        | [52, 98]  | textbook_reasoning |
    /// | 87  | the form "If $P$, then $Q$."                         | 0        | [98, 126] | textbook_reasoning |
    /// | 115 | Do low frequencies mask high ones easily?            | 0        | [0, 41]   | textbook_reasoning |
    /// | 116 | Do planning and scheduling mean the                  | 0        | [0, 35]   | textbook_reasoning |
    /// | 116 | same thing? (Yes | No)                               | 0        | [35, 57]  | textbook_reasoning |
    /// +-----+------------------------------------------------------+----------|-----------|--------------------+
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let schema = record.schema();
        let Some((search_field_idx, _)) = schema.column_with_name(self.search_column().as_str())
        else {
            return WriteFailedNoSearchColumnSnafu {
                search_column: self.search_column(),
                schema: record.schema(),
            }
            .fail()
            .boxed();
        };
        let search_field_array = record.column(search_field_idx);

        let Some(arr_str) = convert_string_arrow_to_iterator!(search_field_array) else {
            return WriteFailedSearchColumnNoStringSnafu {
                search_column: self.search_column(),
                data_type: search_field_array.data_type().clone(),
            }
            .fail()
            .boxed();
        };

        // For each element of the search column, chunk and keep offsets. `chunks` holds
        // zero-copy &str slices into the underlying Arrow buffer, so this step is cheap
        // even for very large documents.
        let (offsets, chunks): (Vec<Vec<(usize, usize)>>, Vec<Vec<_>>) = arr_str
            .map(|s_opt| {
                if let Some(s) = s_opt {
                    // Character offsets, not byte offsets: the read path extracts
                    // the snippet with DataFusion `substring` (1-based, char-counted).
                    // See issue #11269.
                    self.chunker
                        .chunk_with_char_offsets(s)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .unzip::<_, _, Vec<(usize, usize)>, Vec<&str>>()
                } else {
                    (vec![], vec![])
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .unzip();

        let repeats = offsets.iter().map(Vec::len).collect::<Vec<_>>();

        // Group input rows so that each call to `self.inner.write` sees at most
        // `INNER_WRITE_TARGET_CHUNKS` chunk rows in its intermediate batch. Each group is a
        // contiguous, row-atomic slice of the input. If a single row has more chunks than the
        // budget, it forms a group of one and exceeds the budget — this is intentional, and
        // accepted given typical chunk counts per document.
        let row_groups = group_rows_by_chunk_budget(&repeats, INNER_WRITE_TARGET_CHUNKS);

        // Process each row group, collecting the per-chunk `offset` / `embedding` value arrays
        // emitted by the inner index. After all groups complete, concatenate those arrays to
        // recover the same flat value buffer the single-call path would have produced, and wrap
        // it with an OffsetBuffer over the full `repeats` vector to produce the per-document
        // list columns.
        let mut group_offset_arrays: Vec<ArrayRef> = Vec::with_capacity(row_groups.len());
        let mut group_embedding_arrays: Vec<ArrayRef> = Vec::with_capacity(row_groups.len());

        for &(start, length) in &row_groups {
            let group_chunked_rb = self.build_chunked_record_batch(
                &record,
                &schema,
                search_field_idx,
                start,
                length,
                &offsets,
                &chunks,
                &repeats,
            )?;

            let inner_rb = self
                .inner
                .write(group_chunked_rb)
                .await
                .context(InnerIndexWriteSnafu)
                .boxed()?;

            if let Some(arr) = inner_rb.column_by_name(&self.offset_col_name) {
                group_offset_arrays.push(Arc::clone(arr));
            }
            if let Some(arr) = inner_rb.column_by_name(&self.embedding_col_name) {
                group_embedding_arrays.push(Arc::clone(arr));
            }
        }

        // Every chunk this write produces has landed, so the rows it chunked into nothing can
        // now have their previous chunks dropped — see `evict_rows_chunked_to_nothing` for why
        // this is ordered after the writes rather than before them.
        self.evict_rows_chunked_to_nothing(&record, &repeats)
            .await?;

        // From the concatenated inner outputs we need {}_embedding and {}_offset, then convert
        // them from `<inner_type>` -> `List(<inner_type>)` (one list per original row, length
        // `repeats[i]`) so they can be added back to the original `record`. This is so any
        // downstream acceleration has them in the expected format on the write path.
        let (schema, mut arrs, _) = record.into_parts();
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();

        attach_list_column(
            &group_offset_arrays,
            &self.offset_col_name,
            &repeats,
            &schema,
            &mut arrs,
            &mut fields,
        )?;
        attach_list_column(
            &group_embedding_arrays,
            &self.embedding_col_name,
            &repeats,
            &schema,
            &mut arrs,
            &mut fields,
        )?;

        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrs).boxed()
    }

    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        let pk_names: Vec<_> = self
            .primary_fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let pk_expr: Vec<_> = pk_names
            .iter()
            .map(|c| Expr::Column(Column::new_unqualified(c.clone())))
            .collect();

        let tbl = self.inner.query_table_provider(query)?;
        let schema = tbl.schema();

        let mut sort_order_by = vec![SortExpr::new(col(SEARCH_SCORE_COLUMN_NAME), false, false)];

        let pk_order_by: Vec<SortExpr> = pk_expr
            .iter()
            .map(|e| SortExpr::new(e.clone(), true, false))
            .collect();
        sort_order_by.extend(pk_order_by); // `sort_order_by` needs to be first (i.e. first sort by 'score').

        let mut aggr_expr: Vec<_> = schema
            .fields()
            .iter()
            // group expressions (primary keys) are in output by default.
            .filter(|f| {
                !pk_names.contains(f.name())
                    && f.name() != CHUNKED_INDEX_FULL_SEARCH_FIELD
                    && *f.name() != self.search_column()
                    && f.name() != CHUNKED_INDEX_CHUNK_KEY
            })
            .map(|f| {
                first_value(
                    Expr::Column(Column::new_unqualified(f.name().clone())),
                    sort_order_by.clone(),
                )
                .alias(f.name().clone())
            })
            .collect();

        // If present, alias `CHUNKED_INDEX_FULL_SEARCH_FIELD` -> self.search_field
        if !schema
            .columns_with_unqualified_name(CHUNKED_INDEX_FULL_SEARCH_FIELD)
            .is_empty()
        {
            aggr_expr.push(
                first_value(
                    ident(CHUNKED_INDEX_FULL_SEARCH_FIELD),
                    sort_order_by.clone(),
                )
                .alias(self.search_column()),
            );
        }

        let agg = LogicalPlan::Aggregate(Aggregate::try_new(tbl, pk_expr, aggr_expr.clone())?);

        let final_sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(col(SEARCH_SCORE_COLUMN_NAME), false, false)],
            input: agg.into(),
            fetch: None,
        });

        Ok(Arc::new(final_sort))
    }
}

#[expect(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn to_offset_array(x: &[Vec<(usize, usize)>], nullable: bool) -> FixedSizeListArray {
    let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 2)
        .with_field(Field::new_list_field(DataType::Int32, nullable));

    for row in x {
        for (start, end) in row {
            builder.values().append_value(*start as i32);
            builder.values().append_value(*end as i32);
            builder.append(true);
        }
    }
    builder.finish()
}

#[derive(Clone)]
pub struct ChunkedVectorIndex {
    inner: Arc<dyn VectorIndex>,
    /// The chunking itself, which this index is a vector-typed view of. Holding the delegate
    /// rather than the pieces to rebuild it is what lets the two share one write window: a
    /// delegate rebuilt per call would carry its own, always reading "no window open".
    ///
    /// `delegate.inner` is the same index as [`Self::inner`], reached as a [`SearchIndex`].
    delegate: Arc<ChunkedSearchIndex>,
}

impl ChunkedVectorIndex {
    /// Builds the vector wrapper and its delegate together. Production reaches this type through
    /// [`ChunkedSearchIndex::as_vector_index`], which hands over an existing delegate so the two
    /// share one write window; this constructor is what the tests build a standalone one with.
    #[cfg(test)]
    fn new(inner: Arc<dyn VectorIndex>, chunker: Arc<dyn Chunker>) -> Self {
        let delegate = Arc::new(ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker,
        ));
        Self { inner, delegate }
    }
}

impl std::fmt::Debug for ChunkedVectorIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkedVectorIndex")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl VectorIndex for ChunkedVectorIndex {
    fn derived_columns(&self) -> Vec<String> {
        vec![
            embedding_col(self.search_column().as_str()),
            ChunkedSearchIndex::chunking_offset_col(self.search_column().as_str()),
        ]
    }

    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        let base_index_table = self.inner.list_table_provider()?;
        let primary_key_names = ChunkedSearchIndex::base_key_columns(&self.inner.primary_fields());

        // Primary key, offsets and embeddings.
        //// Need to `order by _spice.chunk_id`.
        let mut aggr_expr = vec![
            Expr::Alias(Alias::new(
                array_agg(Expr::Column(Column::new_unqualified(
                    ChunkedSearchIndex::chunking_offset_col(self.search_column().as_str()),
                )))
                .order_by(vec![SortExpr::new(
                    Expr::Column(Column::new_unqualified(CHUNKED_INDEX_CHUNK_KEY)),
                    true,
                    false,
                )])
                .build()?,
                None::<TableReference>,
                ChunkedSearchIndex::chunking_offset_col(self.search_column().as_str()),
            )),
            Expr::Alias(Alias::new(
                array_agg(Expr::Column(Column::new_unqualified(embedding_col(
                    self.search_column().as_str(),
                ))))
                .order_by(vec![SortExpr::new(
                    Expr::Column(Column::new_unqualified(CHUNKED_INDEX_CHUNK_KEY)),
                    true,
                    false,
                )])
                .build()?,
                None::<TableReference>,
                embedding_col(self.search_column().as_str()),
            )),
        ];
        aggr_expr.extend(base_index_table.schema().columns().iter().filter_map(|c| {
            if [
                ChunkedSearchIndex::chunking_offset_col(self.search_column().as_str()),
                embedding_col(self.search_column().as_str()),
                CHUNKED_INDEX_CHUNK_KEY.to_string(),
                self.search_column(),
            ]
            .contains(&c.name)
                || primary_key_names.contains(&c.name)
            {
                return None;
            }
            Some(Expr::Alias(Alias::new(
                first_value(Expr::Column(c.clone()), vec![]),
                None::<TableReference>,
                c.name.clone(),
            )))
        }));

        if base_index_table
            .schema()
            .has_column_with_unqualified_name(CHUNKED_INDEX_FULL_SEARCH_FIELD)
        {
            aggr_expr.push(Expr::Alias(Alias::new(
                first_value(
                    Expr::Column(Column::new_unqualified(CHUNKED_INDEX_FULL_SEARCH_FIELD)),
                    vec![],
                ),
                None::<TableReference>,
                self.search_column(),
            )));
        }

        Ok(LogicalPlan::Aggregate(
            Aggregate::try_new(
                base_index_table.into(),
                primary_key_names
                    .into_iter()
                    .map(|pk| Expr::Column(Column::new_unqualified(pk)))
                    .collect(),
                aggr_expr,
            )
            .boxed()?,
        ))
    }

    /// Forwards to the index this wraps. The inner index's keys carry the chunk id on top of the
    /// base key, which is a superset of this index's own key — the contract asks for at least the
    /// key columns, so no aggregation is needed here.
    fn list_all_entry_keys(&self) -> Result<LogicalPlan, DataFusionError> {
        self.inner.list_all_entry_keys()
    }

    fn dimension(&self) -> i32 {
        self.inner.dimension()
    }
}

#[async_trait]
impl Index for ChunkedVectorIndex {
    fn name(&self) -> &'static str {
        "ChunkedVectorIndex"
    }

    /// Columns that are required for the index to be computed.
    fn required_columns(&self) -> Vec<String> {
        self.delegate.required_columns()
    }

    /// Compute the index - if the index data is represented in the batch itself (i.e. a vector
    /// "*_embedding" column) then modify the provided batches to include the computed column.
    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        self.delegate.compute_index(batches).await
    }

    /// Through the delegate, which records the window and forwards to the same inner index —
    /// so the object that decides whether a write may evict is the object that was told.
    async fn on_write_start(&self, window: WriteWindow) -> Result<(), DataFusionError> {
        self.delegate.on_write_start(window).await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.delegate.on_write_failed().await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.delegate.on_write_complete().await
    }

    /// See [`ChunkedSearchIndex::delete_by_keys`] — same outer-key-to-chunk resolution, with the
    /// same partial-key shortcut, specialized here since `self.inner` is already known to be a
    /// [`VectorIndex`].
    async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
        if self.inner.deletes_by_partial_key() {
            return self.inner.delete_by_keys(keys).await;
        }
        delete_chunked_vector_by_outer_keys(&self.inner, keys).await
    }

    fn deletes_by_partial_key(&self) -> bool {
        self.inner.deletes_by_partial_key()
    }

    fn write_start_failure_is_fatal(&self) -> bool {
        self.inner.write_start_failure_is_fatal()
    }

    fn write_complete_failure_is_fatal(&self) -> bool {
        self.inner.write_complete_failure_is_fatal()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[async_trait]
impl SearchIndex for ChunkedVectorIndex {
    fn search_column(&self) -> String {
        self.inner.search_column()
    }

    /// All [`Field`]s that define a primary key between the underlying table and the [`SearchIndex`].
    fn primary_fields(&self) -> Vec<Field> {
        self.delegate.primary_fields()
    }

    /// Update the index based on a [`RecordBatch`] from the underlying table.
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        self.delegate.write(record).await
    }

    /// A [`TableProvider`] containing the [`SearchIndex::primary_fields`], additional metadata
    /// columns, the associated vectors/indexed content of the [`SearchIndex::search_column`] and the
    ///  search score between `query` and the [`SearchIndex::search_column`].
    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        self.delegate.query_table_provider(query)
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(self as Arc<dyn VectorIndex>)
    }
}

#[cfg(test)]
#[expect(
    clippy::cast_precision_loss,
    clippy::float_cmp,
    reason = "test data uses small integer values encoded in f32 to verify ordering; \
              precision and exact equality are both fine in this synthetic setup"
)]
mod tests {
    use super::*;
    use crate::index::compound::{CompoundReadMode, CompoundVectorIndex};
    use arrow::array::{Float32Array, Int32Array, Int64Array, StringArray};
    use chunking::Chunker;
    use std::fmt::Write as _;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Build a repeating space-separated token string deterministically — used by the multi-
    /// group write tests to produce inputs with known chunk counts. Written as an explicit fold
    /// to avoid the `format!`-in-`collect` allocation pattern.
    fn repeated_tokens(n: usize) -> String {
        (0..n).fold(String::with_capacity(n * 8), |mut s, i| {
            write!(&mut s, "w{i} ").expect("writing to a String never fails");
            s
        })
    }

    #[test]
    fn group_rows_by_chunk_budget_empty() {
        assert!(group_rows_by_chunk_budget(&[], 100).is_empty());
    }

    #[test]
    fn group_rows_by_chunk_budget_under_budget() {
        // Sum is 6, budget is 100 — one group covering everything.
        let groups = group_rows_by_chunk_budget(&[1, 2, 3], 100);
        assert_eq!(groups, vec![(0, 3)]);
    }

    #[test]
    fn group_rows_by_chunk_budget_splits_at_boundary() {
        // [2, 1, 3, 4, 1] with budget=5: 2+1=3, +3=6>5 → flush; 3, +4=7>5 → flush; 4, +1=5 → ok.
        let groups = group_rows_by_chunk_budget(&[2, 1, 3, 4, 1], 5);
        assert_eq!(groups, vec![(0, 2), (2, 1), (3, 2)]);
    }

    /// Regression for issue #11269: the chunk offsets persisted by the write
    /// path (`to_offset_array` of `chunk_with_char_offsets`) must round-trip
    /// through the read path's `substring` extraction and recover each chunk
    /// exactly — including for non-ASCII text, where byte offsets (the old
    /// behavior) produced shifted/garbled snippets.
    #[tokio::test]
    async fn substring_read_path_recovers_chunks_including_unicode() {
        use arrow::array::ArrayRef;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use chunking::{ChunkingConfig, RecursiveSplittingChunker};
        use datafusion::datasource::MemTable;
        use datafusion::logical_expr::Operator;
        use datafusion::prelude::{
            SessionContext, array_element, binary_expr, cast, col, lit, substring,
        };
        use std::sync::Arc;

        let cfg = ChunkingConfig {
            target_chunk_size: 4,
            overlap_size: 0,
            trim_whitespace: false,
            file_format: None,
        };
        let text_chunker =
            RecursiveSplittingChunker::with_character_sizer(&cfg).expect("create chunker");

        // Multi-byte characters so byte and character offsets diverge — the heart
        // of the bug. The first chunk also exercises the 0-based→1-based fix.
        let text = "café über señor data points";
        let chunked: Vec<((usize, usize), String)> = text_chunker
            .chunk_with_char_offsets(text)
            .map(|(off, c)| (off, c.to_string()))
            .collect();
        assert!(
            chunked.len() >= 2,
            "need multiple chunks to exercise offsets, got {}",
            chunked.len()
        );

        let offsets: Vec<Vec<(usize, usize)>> = vec![chunked.iter().map(|(off, _)| *off).collect()];
        let offset_arr = to_offset_array(&offsets, false);
        let n = chunked.len();

        let schema = Arc::new(Schema::new(vec![
            Field::new("q", DataType::Utf8, false),
            Field::new("q_offset", offset_arr.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![text; n])) as ArrayRef,
                Arc::new(offset_arr) as ArrayRef,
            ],
        )
        .expect("build record batch");

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("create memtable");
        let df = ctx.read_table(Arc::new(table)).expect("read table");

        // Mirrors the extraction in `SearchTableProvider::add_match_column`
        // (crates/search/src/provider.rs) and the candidate vector read path
        // (crates/runtime-search/src/candidate/vector.rs): 0-based character
        // offsets, 1-based char-counted `substring`, so start = offset[1] + 1
        // and length = offset[2] - offset[1].
        let start = array_element(col("q_offset"), lit(1));
        let extracted = df
            .select(vec![
                cast(
                    substring(
                        col("q"),
                        binary_expr(start.clone(), Operator::Plus, lit(1)),
                        binary_expr(
                            array_element(col("q_offset"), lit(2)),
                            Operator::Minus,
                            start,
                        ),
                    ),
                    DataType::Utf8,
                )
                .alias("match"),
            ])
            .expect("select substring");

        let results = extracted.collect().await.expect("collect results");
        let total: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, n, "one extracted snippet per chunk");

        let col0 = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 match column");
        for (i, (_, chunk)) in chunked.iter().enumerate() {
            assert_eq!(
                col0.value(i),
                chunk.as_str(),
                "extracted snippet must equal the original chunk (row {i})"
            );
        }
    }

    #[test]
    fn group_rows_by_chunk_budget_single_row_exceeds_budget() {
        // A single row larger than the budget forms a group of one. The next rows start fresh.
        let groups = group_rows_by_chunk_budget(&[10, 1, 1], 5);
        assert_eq!(groups, vec![(0, 1), (1, 2)]);
    }

    #[test]
    fn group_rows_by_chunk_budget_exact_fit() {
        // 2+3=5 (== budget), then 1+4=5 — both groups hit the budget exactly.
        let groups = group_rows_by_chunk_budget(&[2, 3, 1, 4], 5);
        assert_eq!(groups, vec![(0, 2), (2, 2)]);
    }

    #[test]
    fn group_rows_by_chunk_budget_zero_chunks() {
        // Rows with zero chunks coalesce into the running group without forcing a flush.
        // Total here is 6, budget 6 — everything fits in a single group.
        let groups = group_rows_by_chunk_budget(&[0, 0, 3, 0, 3], 6);
        assert_eq!(groups, vec![(0, 5)]);
    }

    /// A [`Chunker`] that splits on a single delimiter — used for tests where we want
    /// deterministic, observable chunk boundaries without pulling in tokenizer config.
    struct DelimChunker {
        delim: char,
    }

    impl Chunker for DelimChunker {
        fn chunk_indices<'a>(
            &self,
            text: &'a str,
        ) -> Box<dyn Iterator<Item = (usize, &'a str)> + 'a> {
            let mut out: Vec<(usize, &'a str)> = Vec::new();
            let mut start = 0usize;
            for (i, c) in text.char_indices() {
                if c == self.delim {
                    if i > start {
                        out.push((start, &text[start..i]));
                    }
                    start = i + c.len_utf8();
                }
            }
            if start < text.len() {
                out.push((start, &text[start..]));
            }
            Box::new(out.into_iter())
        }
    }

    /// A pass-through [`SearchIndex`] that records how many times `write` is called and the size
    /// of each input. It also appends a fake `<col>_embedding` column (4-dim FixedSizeList of
    /// Float32) and an `<col>_offset` column copied from the input. The chunking layer reads
    /// these back and folds them into per-document list columns on the final output.
    #[derive(Debug)]
    #[expect(
        clippy::struct_excessive_bools,
        reason = "a test double's independently-settable knobs; grouping them would obscure the \
                  `..RecordingInner::new(..)` struct updates every test here is written with"
    )]
    struct RecordingInner {
        search_column: String,
        calls: AtomicUsize,
        row_counts: std::sync::Mutex<Vec<usize>>,
        /// What this mock reports from [`Index::write_start_failure_is_fatal`].
        write_start_fatal: bool,
        /// What this mock reports from [`Index::write_complete_failure_is_fatal`].
        write_complete_fatal: bool,
        /// What this mock reports from [`Index::deletes_by_partial_key`].
        deletes_partial_key: bool,
        /// Makes [`SearchIndex::write`] fail, for the paths that must not act on a write that
        /// did not land.
        write_fails: bool,
        /// Whether [`SearchIndex::as_vector_index`] hands one back. `false` is the shape of a
        /// full-text index: not enumerable, so a chunked index over it can only reach its chunks
        /// if it deletes by partial key.
        is_vector_index: bool,
        /// What this mock reports from [`SearchIndex::primary_fields`]. A chunked index's inner
        /// index carries [`CHUNKED_INDEX_CHUNK_KEY`] here.
        primary_fields: Vec<Field>,
        /// Rows this mock claims to store, served by [`VectorIndex::list_table_provider`].
        /// `None` reports no listing at all; `Some` of an empty batch is Elasticsearch, whose
        /// list plan is a correctly-shaped empty table because it cannot enumerate its vectors.
        listed: Option<Vec<RecordBatch>>,
        /// Key batches handed to [`Index::delete_by_keys`].
        deleted: std::sync::Mutex<Vec<RecordBatch>>,
        /// How many times [`VectorIndex::list_table_provider`] was asked for a plan — the
        /// enumeration of everything the index holds, which is what resolving a chunked delete
        /// against an exact-key index costs.
        listings: AtomicUsize,
    }

    impl RecordingInner {
        fn new(search_column: &str) -> Self {
            Self {
                search_column: search_column.to_string(),
                calls: AtomicUsize::new(0),
                row_counts: std::sync::Mutex::new(Vec::new()),
                write_start_fatal: false,
                write_complete_fatal: false,
                deletes_partial_key: false,
                write_fails: false,
                is_vector_index: true,
                primary_fields: vec![Field::new("id", DataType::Int64, false)],
                listed: None,
                deleted: std::sync::Mutex::new(Vec::new()),
                listings: AtomicUsize::new(0),
            }
        }

        fn with_fatal_write_complete(search_column: &str) -> Self {
            Self {
                write_complete_fatal: true,
                ..Self::new(search_column)
            }
        }

        fn with_fatal_write_start(search_column: &str) -> Self {
            Self {
                write_start_fatal: true,
                ..Self::new(search_column)
            }
        }

        /// A mock shaped like a chunked index's inner index: a chunk-key-augmented primary key,
        /// and `listed` rows keyed by it.
        fn chunked(listed: Vec<RecordBatch>) -> Self {
            Self {
                primary_fields: ChunkedSearchIndex::augment_primary_key(vec![Field::new(
                    "id",
                    DataType::Int64,
                    false,
                )]),
                listed: Some(listed),
                ..Self::new("content")
            }
        }

        fn listings(&self) -> usize {
            self.listings.load(Ordering::SeqCst)
        }

        /// The key batches passed to [`Index::delete_by_keys`], as
        /// `(column names, values of the first column)` per call.
        fn deletes(&self) -> Vec<(Vec<String>, Vec<i64>)> {
            self.deleted
                .lock()
                .expect("mutex")
                .iter()
                .map(|batch| {
                    let names = batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                    let ids = batch
                        .column_by_name("id")
                        .expect("delete keys carry the base key")
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("id is Int64")
                        .values()
                        .to_vec();
                    (names, ids)
                })
                .collect()
        }
    }

    #[async_trait]
    impl Index for RecordingInner {
        fn name(&self) -> &'static str {
            "RecordingInner"
        }
        fn required_columns(&self) -> Vec<String> {
            vec![self.search_column.clone()]
        }
        async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
            self.deleted.lock().expect("mutex").push(keys);
            Ok(())
        }
        fn deletes_by_partial_key(&self) -> bool {
            self.deletes_partial_key
        }
        fn write_start_failure_is_fatal(&self) -> bool {
            self.write_start_fatal
        }
        fn write_complete_failure_is_fatal(&self) -> bool {
            self.write_complete_fatal
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[async_trait]
    impl VectorIndex for RecordingInner {
        fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
            self.listings.fetch_add(1, Ordering::SeqCst);
            let Some(batches) = self.listed.as_ref() else {
                return Err(DataFusionError::NotImplemented(
                    "RecordingInner stores embeddings in the underlying table".to_string(),
                ));
            };
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .expect("a listing needs at least one (possibly empty) batch");
            let table = datafusion::datasource::MemTable::try_new(schema, vec![batches.clone()])?;
            LogicalPlanBuilder::scan(
                "inner",
                Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
                    table,
                ))),
                None,
            )?
            .build()
        }

        fn dimension(&self) -> i32 {
            4
        }
    }

    #[async_trait]
    impl SearchIndex for RecordingInner {
        fn search_column(&self) -> String {
            self.search_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            self.primary_fields.clone()
        }

        fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
            self.is_vector_index.then_some(self as Arc<dyn VectorIndex>)
        }

        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            if self.write_fails {
                return Err(Box::new(DataFusionError::Execution(
                    "inner write failed".to_string(),
                )));
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.row_counts
                .lock()
                .expect("mutex")
                .push(record.num_rows());

            // Build a synthetic embedding column matching the row count. The first element
            // encodes the row's position within this call so tests can verify ordering
            // end-to-end after concat across multiple inner.write calls.
            let n = record.num_rows();
            let mut emb_values: Vec<f32> = Vec::with_capacity(n * 4);
            for i in 0..n {
                emb_values.extend_from_slice(&[i as f32, 0.0, 0.0, 0.0]);
            }
            let emb_field = Arc::new(Field::new("item", DataType::Float32, true));
            let emb = FixedSizeListArray::try_new(
                Arc::clone(&emb_field),
                4,
                Arc::new(Float32Array::from(emb_values)),
                None,
            )?;

            let offset_col = ChunkedSearchIndex::chunking_offset_col(&self.search_column);
            let offset_arr = Arc::clone(
                record
                    .column_by_name(&offset_col)
                    .expect("input chunked rb must have offset col"),
            );

            let mut fields: Vec<Field> = record
                .schema()
                .fields()
                .iter()
                .map(|f| Arc::unwrap_or_clone(Arc::clone(f)))
                .collect();
            let mut cols: Vec<ArrayRef> = record.columns().iter().map(Arc::clone).collect();
            fields.push(Field::new(
                embedding_col(&self.search_column),
                DataType::FixedSizeList(Arc::clone(&emb_field), 4),
                true,
            ));
            cols.push(Arc::new(emb) as ArrayRef);
            // Offset col is already in the input; ensure it's also exported under the canonical
            // name in case the chunker dropped/re-added it.
            if record.column_by_name(&offset_col).is_none() {
                fields.push(Field::new(
                    offset_col,
                    DataType::new_fixed_size_list(DataType::Int32, 2, false),
                    false,
                ));
                cols.push(offset_arr);
            }

            Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
        }

        fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
            Err(DataFusionError::NotImplemented("unused in tests".into()))
        }
    }

    fn build_input(rows: &[(&str, i64)]) -> RecordBatch {
        let opt: Vec<(Option<&str>, i64)> = rows.iter().map(|(c, id)| (Some(*c), *id)).collect();
        build_input_opt(&opt)
    }

    /// [`build_input`] with a nullable search value, for the rewrite cases where a row's text
    /// goes away entirely.
    fn build_input_opt(rows: &[(Option<&str>, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = rows.iter().map(|(_, id)| *id).collect();
        let contents: Vec<Option<&str>> = rows.iter().map(|(c, _)| *c).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(contents)),
            ],
        )
        .expect("valid batch")
    }

    /// The chunking layer must not downgrade a fatal inner index to best-effort — a
    /// wrapper inheriting the trait default is exactly the silent no-op #12038 fixes.
    #[test]
    fn chunked_search_index_forwards_write_complete_fatality() {
        let chunker = || Arc::new(DelimChunker { delim: ' ' }) as Arc<dyn Chunker>;

        let best_effort = ChunkedSearchIndex::new(
            Arc::new(RecordingInner::new("content")) as Arc<dyn SearchIndex>,
            chunker(),
        );
        assert!(!best_effort.write_complete_failure_is_fatal());

        let fatal = ChunkedSearchIndex::new(
            Arc::new(RecordingInner::with_fatal_write_complete("content")) as Arc<dyn SearchIndex>,
            chunker(),
        );
        assert!(fatal.write_complete_failure_is_fatal());
    }

    #[test]
    fn chunked_vector_index_forwards_write_complete_fatality() {
        let chunker = || Arc::new(DelimChunker { delim: ' ' }) as Arc<dyn Chunker>;

        let best_effort = ChunkedVectorIndex::new(
            Arc::new(RecordingInner::new("content")) as Arc<dyn VectorIndex>,
            chunker(),
        );
        assert!(!best_effort.write_complete_failure_is_fatal());

        let fatal = ChunkedVectorIndex::new(
            Arc::new(RecordingInner::with_fatal_write_complete("content")) as Arc<dyn VectorIndex>,
            chunker(),
        );
        assert!(fatal.write_complete_failure_is_fatal());
    }

    /// The start-fatality flag has to forward on its own, not ride along with the
    /// finalize one — a wrapper that forwards only `write_complete` silently downgrades an
    /// inner index whose *prepare* is load-bearing back to best-effort (#12421).
    #[test]
    fn chunked_search_index_forwards_write_start_fatality() {
        let chunker = || Arc::new(DelimChunker { delim: ' ' }) as Arc<dyn Chunker>;

        let best_effort = ChunkedSearchIndex::new(
            Arc::new(RecordingInner::new("content")) as Arc<dyn SearchIndex>,
            chunker(),
        );
        assert!(!best_effort.write_start_failure_is_fatal());

        let fatal = ChunkedSearchIndex::new(
            Arc::new(RecordingInner::with_fatal_write_start("content")) as Arc<dyn SearchIndex>,
            chunker(),
        );
        assert!(fatal.write_start_failure_is_fatal());
        assert!(
            !fatal.write_complete_failure_is_fatal(),
            "a fatal start must not be reported as a fatal finalize"
        );
    }

    #[test]
    fn chunked_vector_index_forwards_write_start_fatality() {
        let chunker = || Arc::new(DelimChunker { delim: ' ' }) as Arc<dyn Chunker>;

        let best_effort = ChunkedVectorIndex::new(
            Arc::new(RecordingInner::new("content")) as Arc<dyn VectorIndex>,
            chunker(),
        );
        assert!(!best_effort.write_start_failure_is_fatal());

        let fatal = ChunkedVectorIndex::new(
            Arc::new(RecordingInner::with_fatal_write_start("content")) as Arc<dyn VectorIndex>,
            chunker(),
        );
        assert!(fatal.write_start_failure_is_fatal());
        assert!(
            !fatal.write_complete_failure_is_fatal(),
            "a fatal start must not be reported as a fatal finalize"
        );
    }

    fn chunker() -> Arc<dyn Chunker> {
        Arc::new(DelimChunker { delim: ' ' }) as Arc<dyn Chunker>
    }

    /// The base key a chunked index is asked to delete — the shape of
    /// [`ChunkedSearchIndex::primary_fields`], with no chunk id.
    fn outer_keys(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
        )
        .expect("valid batch")
    }

    /// The chunk-keyed entries an inner index stores: one row per `(id, chunk_id)`.
    fn chunk_keyed_rows(rows: &[(i64, u64)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(ChunkedSearchIndex::augment_primary_key(vec![
                Field::new("id", DataType::Int64, false),
            ]))),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, chunk)| *chunk).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("valid batch")
    }

    /// An inner index that deletes by a partial key gets the base key handed straight to it, and
    /// deletes every chunk under it in one operation.
    ///
    /// Regression test for #12088. Elasticsearch's list plan is a correctly-shaped *empty* table
    /// — it cannot enumerate its own vectors — so resolving the chunk-keyed rows first (the only
    /// path this had) matched nothing, and a chunked Elasticsearch index reported a successful
    /// delete while keeping every document for the deleted row.
    #[tokio::test]
    async fn a_partial_key_inner_index_deletes_from_the_base_key_alone() {
        for listed in [
            vec![chunk_keyed_rows(&[])],
            vec![chunk_keyed_rows(&[(1, 0), (1, 1), (2, 0)])],
        ] {
            let inner = Arc::new(RecordingInner {
                deletes_partial_key: true,
                ..RecordingInner::chunked(listed)
            });
            let idx =
                ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

            idx.delete_by_keys(outer_keys(&[1, 2]))
                .await
                .expect("delete succeeds");

            assert_eq!(
                inner.deletes(),
                vec![(vec!["id".to_string()], vec![1, 2])],
                "the base key must reach the inner index unchanged, whatever it lists"
            );
        }
    }

    /// An inner index addressed by an exact key still has its chunk-keyed entries resolved first,
    /// and only the deleted row's chunks are removed.
    #[tokio::test]
    async fn an_exact_key_inner_index_resolves_its_chunk_keyed_entries_first() {
        let inner = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
            (1, 0),
            (1, 1),
            (2, 0),
        ])]));
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        idx.delete_by_keys(outer_keys(&[1]))
            .await
            .expect("delete succeeds");

        // Asserted over the union of the delete calls rather than a single batch: the resolving
        // query ends in a `distinct()`, whose hash aggregate emits one batch per non-empty
        // partition, so how many calls the inner index sees tracks DataFusion's partitioning —
        // and hence the host's CPU count — not this fix. Which keys get deleted is the guarantee.
        let deletes = inner.deletes();
        assert!(
            !deletes.is_empty(),
            "the resolved chunks reach the inner index"
        );
        assert!(
            deletes
                .iter()
                .all(|(columns, _)| columns.contains(&CHUNKED_INDEX_CHUNK_KEY.to_string())),
            "resolved keys carry the chunk id: {deletes:?}"
        );
        assert_eq!(
            resolved_ids(&deletes),
            vec![1, 1],
            "both chunks of id 1, and nothing else"
        );
    }

    /// An inner index shaped like the S3-Vectors-with-warm-tier case: a warm `primary` holding
    /// only what the write path has passed through it, over an authoritative `secondary`.
    fn compound_inner(
        warm: &Arc<RecordingInner>,
        durable: &Arc<RecordingInner>,
        read_mode: CompoundReadMode,
    ) -> Arc<CompoundVectorIndex> {
        Arc::new(
            CompoundVectorIndex::try_new(
                Arc::clone(warm) as Arc<dyn VectorIndex>,
                Arc::clone(durable) as Arc<dyn VectorIndex>,
                read_mode,
            )
            .expect("the two mocks share a search column, primary key and dimension"),
        )
    }

    /// Deletes base key 1 through a chunked index over a compound inner, returning what each half
    /// was asked to delete.
    async fn delete_through_compound(
        warm: &Arc<RecordingInner>,
        durable: &Arc<RecordingInner>,
        read_mode: CompoundReadMode,
    ) -> (Vec<(Vec<String>, Vec<i64>)>, Vec<(Vec<String>, Vec<i64>)>) {
        let idx = ChunkedSearchIndex::new(
            compound_inner(warm, durable, read_mode) as Arc<dyn SearchIndex>,
            chunker(),
        );

        idx.delete_by_keys(outer_keys(&[1]))
            .await
            .expect("delete succeeds");

        (warm.deletes(), durable.deletes())
    }

    /// The resolved chunk ids across every `delete_by_keys` call a half received.
    fn resolved_ids(deletes: &[(Vec<String>, Vec<i64>)]) -> Vec<i64> {
        deletes.iter().flat_map(|(_, ids)| ids.clone()).collect()
    }

    /// Regression test for #12266. A compound inner index serves *reads* from its warm primary
    /// (`PrimaryOnly`) or falls back per-plan (`FallbackToSecondary`) — neither is authoritative
    /// for what is stored. Resolving the chunk-keyed entries against that read listing found
    /// nothing for a row the warm tier does not hold, so `delete_by_keys` was never called for it
    /// and the delete reported success having removed nothing from the durable store.
    #[tokio::test]
    async fn a_compound_inner_resolves_chunks_the_warm_primary_does_not_hold() {
        for read_mode in [
            CompoundReadMode::PrimaryOnly,
            CompoundReadMode::FallbackToSecondary,
        ] {
            let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[])]));
            let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
                (1, 0),
                (1, 1),
            ])]));

            let (warm_deletes, durable_deletes) =
                delete_through_compound(&warm, &durable, read_mode).await;

            assert_eq!(
                resolved_ids(&durable_deletes),
                vec![1, 1],
                "both chunks the durable half holds must be deleted ({read_mode:?})"
            );
            assert!(
                durable_deletes
                    .iter()
                    .all(|(columns, _)| columns.contains(&CHUNKED_INDEX_CHUNK_KEY.to_string())),
                "resolved keys carry the chunk id: {durable_deletes:?}"
            );
            assert_eq!(
                resolved_ids(&warm_deletes),
                vec![1, 1],
                "the delete fans out to both halves, whichever resolved the entries ({read_mode:?})"
            );
        }
    }

    /// The narrower half of the same bug: a *partially* populated warm tier. `FallbackToSecondary`
    /// falls back only when the primary's plan is entirely empty, so a warm tier holding one of
    /// two chunks resolved just that one and left the other in the durable store.
    #[tokio::test]
    async fn a_compound_inner_resolves_chunks_a_partial_warm_primary_is_missing() {
        for read_mode in [
            CompoundReadMode::PrimaryOnly,
            CompoundReadMode::FallbackToSecondary,
        ] {
            let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 0)])]));
            let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
                (1, 0),
                (1, 1),
            ])]));

            let (_, durable_deletes) = delete_through_compound(&warm, &durable, read_mode).await;

            assert_eq!(
                resolved_ids(&durable_deletes),
                vec![1, 1],
                "the chunk the warm tier is missing must still be resolved ({read_mode:?})"
            );
        }
    }

    /// The other direction: resolving from the durable half *alone* would be equally wrong. The
    /// two halves can disagree either way, so an entry only the warm tier holds must also be
    /// resolved — hence a union rather than a switch to the secondary.
    #[tokio::test]
    async fn a_compound_inner_resolves_an_entry_only_the_warm_primary_holds() {
        let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 7)])]));
        let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[])]));

        let (warm_deletes, _) =
            delete_through_compound(&warm, &durable, CompoundReadMode::FallbackToSecondary).await;

        assert_eq!(
            resolved_ids(&warm_deletes),
            vec![1],
            "an entry only the warm tier holds must still be resolved and deleted"
        );
    }

    /// The union must not turn one stored entry into two deletes just because both halves hold it.
    #[tokio::test]
    async fn a_compound_inner_resolves_a_shared_entry_once() {
        let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 0)])]));
        let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 0)])]));

        let (warm_deletes, durable_deletes) =
            delete_through_compound(&warm, &durable, CompoundReadMode::PrimaryOnly).await;

        assert_eq!(
            resolved_ids(&durable_deletes),
            vec![1],
            "an entry both halves hold resolves once, not once per half"
        );
        assert_eq!(resolved_ids(&warm_deletes), vec![1]);
    }

    /// Resolving from the union must not widen *which* rows are deleted — only the requested base
    /// key's chunks may be removed, from either half.
    #[tokio::test]
    async fn a_compound_inner_delete_leaves_other_base_keys_alone() {
        let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(2, 0)])]));
        let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
            (1, 0),
            (2, 0),
            (3, 0),
        ])]));

        let (warm_deletes, durable_deletes) =
            delete_through_compound(&warm, &durable, CompoundReadMode::PrimaryOnly).await;

        assert_eq!(
            resolved_ids(&durable_deletes),
            vec![1],
            "only base key 1's chunk is deleted: {durable_deletes:?}"
        );
        assert_eq!(
            resolved_ids(&warm_deletes),
            vec![1],
            "the other base keys the warm tier holds are untouched: {warm_deletes:?}"
        );
    }

    /// `ChunkedVectorIndex` is a wrapper, so it must forward `list_all_entry_keys` to its inner index
    /// rather than inherit the default (which would resolve against the read listing again).
    #[tokio::test]
    async fn chunked_vector_index_forwards_list_all_entry_keys_to_its_inner_index() {
        let warm = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[])]));
        let durable = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
            (1, 0),
            (1, 1),
        ])]));
        let idx = ChunkedVectorIndex::new(
            compound_inner(&warm, &durable, CompoundReadMode::PrimaryOnly) as Arc<dyn VectorIndex>,
            chunker(),
        );

        // The compound inner unions its two halves, so the forward is visible in the plan. Had
        // `ChunkedVectorIndex` inherited the default, this would be the *read* listing — the warm
        // primary's scan alone under `PrimaryOnly`, with no `Union` in it.
        let plan = format!(
            "{}",
            idx.list_all_entry_keys()
                .expect("authoritative plan builds")
                .display_indent()
        );

        assert!(
            plan.contains("Union"),
            "the authoritative listing must reach both halves of the compound inner:\n{plan}"
        );
    }

    /// A chunked index with no listing and no partial-key delete has no way to reach its inner
    /// index's chunks, and must say so rather than report a delete it did not perform.
    #[tokio::test]
    async fn a_chunked_index_over_an_unlistable_inner_index_reports_not_implemented() {
        let inner = Arc::new(RecordingInner::new("content"));
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        let err = idx
            .delete_by_keys(outer_keys(&[1]))
            .await
            .expect_err("no way to enumerate chunks");

        assert!(
            matches!(err, DataFusionError::NotImplemented(_)),
            "unexpected error: {err}"
        );
        assert!(inner.deletes().is_empty());
    }

    /// Both chunked wrappers must forward the capability — inheriting the trait default would
    /// send a partial-key-capable index down the enumerate-first path.
    #[test]
    fn chunked_wrappers_forward_partial_key_deletion() {
        for partial in [false, true] {
            let inner = || {
                Arc::new(RecordingInner {
                    deletes_partial_key: partial,
                    ..RecordingInner::new("content")
                })
            };

            let search = ChunkedSearchIndex::new(inner() as Arc<dyn SearchIndex>, chunker());
            assert_eq!(search.deletes_by_partial_key(), partial);

            let vector = ChunkedVectorIndex::new(inner() as Arc<dyn VectorIndex>, chunker());
            assert_eq!(vector.deletes_by_partial_key(), partial);
        }
    }

    /// Smoke test: a tiny input that fits comfortably under the budget should result in exactly
    /// one inner.write call and an output row count equal to the input row count.
    #[tokio::test]
    async fn write_single_group_when_under_budget() {
        let inner = Arc::new(RecordingInner::new("content"));
        let chunker = Arc::new(DelimChunker { delim: ' ' });
        let idx = ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker as Arc<dyn Chunker>,
        );

        // "a b" -> 2 chunks; "c" -> 1; "d e f" -> 3. Total = 6 chunks across 3 rows.
        let input = build_input(&[("a b", 1), ("c", 2), ("d e f", 3)]);
        let input_rows = input.num_rows();

        let out = idx.write(input).await.expect("write ok");

        assert_eq!(out.num_rows(), input_rows);
        assert_eq!(
            inner.calls.load(Ordering::SeqCst),
            1,
            "should call inner.write exactly once under budget"
        );
        let row_counts = inner.row_counts.lock().expect("mutex").clone();
        assert_eq!(row_counts, vec![6], "intermediate chunked batch is 6 rows");

        // Verify the per-row offset/embedding list columns exist and have the right outer shape.
        let offset_col = ChunkedSearchIndex::chunking_offset_col("content");
        let embed_col = embedding_col("content");
        let off_list = out
            .column_by_name(&offset_col)
            .expect("offset col")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let emb_list = out
            .column_by_name(&embed_col)
            .expect("embed col")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        assert_eq!(off_list.len(), input_rows);
        assert_eq!(emb_list.len(), input_rows);
        // Lengths of the inner lists must match per-row chunk counts.
        let lengths: Vec<i32> = (0..off_list.len())
            .map(|i| off_list.value_length(i))
            .collect();
        assert_eq!(lengths, vec![2, 1, 3]);
    }

    /// Core regression test for #7507: when the total chunk count exceeds the configured budget,
    /// the indexer must split into multiple inner.write calls so the intermediate batch never
    /// exceeds the budget. Output must still be one batch with row count == input row count.
    #[tokio::test]
    async fn write_splits_into_multiple_groups_over_budget() {
        let inner = Arc::new(RecordingInner::new("content"));
        let chunker = Arc::new(DelimChunker { delim: ' ' });
        let idx = ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker as Arc<dyn Chunker>,
        );

        // Build N rows of ~K chunks each, total chunks well above the 8192 budget.
        // K=200 chunks/row * 50 rows = 10,000 chunks > 8192 budget.
        let big_doc = repeated_tokens(200);
        let rows: Vec<(String, i64)> = (0i64..50)
            .map(|i| (big_doc.trim_end().to_string(), i))
            .collect();
        let row_refs: Vec<(&str, i64)> = rows.iter().map(|(s, i)| (s.as_str(), *i)).collect();
        let input = build_input(&row_refs);
        let input_rows = input.num_rows();

        let out = idx.write(input).await.expect("write ok");

        assert_eq!(out.num_rows(), input_rows);
        let calls = inner.calls.load(Ordering::SeqCst);
        assert!(
            calls >= 2,
            "expected multiple inner.write calls when total chunks > budget, got {calls}"
        );

        // No single inner.write call should exceed the budget (rows are atomic, but each row's
        // chunk count here is ~200, well under the 8192 budget).
        let row_counts = inner.row_counts.lock().expect("mutex").clone();
        for c in &row_counts {
            assert!(
                *c <= INNER_WRITE_TARGET_CHUNKS,
                "intermediate batch size {c} exceeded budget {INNER_WRITE_TARGET_CHUNKS}",
            );
        }
        // Total chunks across all calls must equal total chunks across the full input.
        let total: usize = row_counts.iter().sum();
        assert_eq!(total, 50 * 200);

        // Each output row's list lengths must equal the chunk count for that row.
        let off_list = out
            .column_by_name(&ChunkedSearchIndex::chunking_offset_col("content"))
            .expect("offset col")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        for i in 0..off_list.len() {
            assert_eq!(off_list.value_length(i), 200);
        }
    }

    /// A single row whose chunk count exceeds the budget is intentionally processed atomically:
    /// it gets its own inner.write call (which exceeds the budget) and subsequent rows start a
    /// fresh group. This documents the row-atomic semantics of the bound.
    #[tokio::test]
    async fn write_oversized_single_row_is_atomic() {
        let inner = Arc::new(RecordingInner::new("content"));
        let chunker = Arc::new(DelimChunker { delim: ' ' });
        let idx = ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker as Arc<dyn Chunker>,
        );

        // Row 0: budget + 100 chunks (single oversized row).
        // Row 1: 3 chunks.
        let big = repeated_tokens(INNER_WRITE_TARGET_CHUNKS + 100);
        let big_trimmed = big.trim_end();
        let input = build_input(&[(big_trimmed, 1), ("a b c", 2)]);

        let out = idx.write(input).await.expect("write ok");
        assert_eq!(out.num_rows(), 2);

        let row_counts = inner.row_counts.lock().expect("mutex").clone();
        // First call: the oversized row alone (above budget — expected, since the row is atomic).
        assert_eq!(row_counts[0], INNER_WRITE_TARGET_CHUNKS + 100);
        // Second call: the small row (3 chunks).
        assert_eq!(row_counts.get(1).copied(), Some(3));
    }

    /// Concatenated value arrays across multiple inner.write calls must preserve order, so the
    /// per-document list slices come out aligned. `RecordingInner` writes a synthetic embedding
    /// where the first f32 is the row's position within that single `inner.write` call.
    /// Row-atomic grouping puts a whole input row into exactly one `inner.write`, so after the
    /// per-group outputs are concatenated and wrapped with `OffsetBuffer::from_lengths(repeats)`
    /// each output row's embedding list must contain values whose first f32 runs 0..C_i in order.
    /// If the concat order or the offset buffer were wrong, this sequence would skip or repeat.
    #[tokio::test]
    async fn write_preserves_chunk_order_across_groups() {
        let inner = Arc::new(RecordingInner::new("content"));
        let chunker = Arc::new(DelimChunker { delim: ' ' });
        let idx = ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker as Arc<dyn Chunker>,
        );

        // 3 rows of 5000 chunks each → 15,000 chunks total, budget 8192 → 3 groups (one row
        // per group, since two rows would exceed the budget).
        let big_doc = repeated_tokens(5000);
        let rows: Vec<(String, i64)> = (0i64..3)
            .map(|i| (big_doc.trim_end().to_string(), i))
            .collect();
        let row_refs: Vec<(&str, i64)> = rows.iter().map(|(s, i)| (s.as_str(), *i)).collect();
        let input = build_input(&row_refs);

        let out = idx.write(input).await.expect("write ok");
        assert_eq!(out.num_rows(), 3);

        let emb_list = out
            .column_by_name(&embedding_col("content"))
            .expect("embed")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");

        // Every output list has length 5000 (chunks per row) and its values appear in 0..5000
        // order — proves the concat seam between groups doesn't misalign chunks within a row.
        for row_idx in 0..emb_list.len() {
            assert_eq!(emb_list.value_length(row_idx), 5000);
            let row_emb = emb_list.value(row_idx);
            let fsl = row_emb
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .expect("fsl");
            assert_eq!(fsl.len(), 5000);
            for chunk_idx in 0..fsl.len() {
                let vec = fsl.value(chunk_idx);
                let arr = vec.as_any().downcast_ref::<Float32Array>().expect("f32");
                assert_eq!(
                    arr.value(0),
                    chunk_idx as f32,
                    "row {row_idx} chunk {chunk_idx} embedding[0] mismatch — chunks reordered across the concat seam",
                );
            }
        }

        // Total flat-value length equals sum(per-row chunks).
        let off_list = out
            .column_by_name(&ChunkedSearchIndex::chunking_offset_col("content"))
            .expect("offset col")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        assert_eq!(off_list.values().len(), 3 * 5000);
    }

    /// Per-row offset entries must point at the correct character spans of the source document.
    /// This is the property that downstream search relies on for highlighting / retrieval.
    #[tokio::test]
    async fn write_offsets_match_source_spans() {
        let inner = Arc::new(RecordingInner::new("content"));
        let chunker = Arc::new(DelimChunker { delim: ' ' });
        let idx = ChunkedSearchIndex::new(
            Arc::clone(&inner) as Arc<dyn SearchIndex>,
            chunker as Arc<dyn Chunker>,
        );

        // "hello world" => chunks ["hello", "world"] at offsets (0,5) and (6,11).
        let input = build_input(&[("hello world", 1)]);
        let out = idx.write(input).await.expect("write ok");

        let off_list = out
            .column_by_name(&ChunkedSearchIndex::chunking_offset_col("content"))
            .expect("offset col")
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let row0 = off_list.value(0);
        let fsl = row0
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("fsl");
        assert_eq!(fsl.len(), 2);

        let read_pair = |i: usize| -> (i32, i32) {
            let pair = fsl.value(i);
            let pair_arr = pair.as_any().downcast_ref::<Int32Array>().expect("int32");
            (pair_arr.value(0), pair_arr.value(1))
        };
        assert_eq!(read_pair(0), (0, 5));
        assert_eq!(read_pair(1), (6, 11));
    }

    /// An inner index that materializes its chunk-keyed rows as a mutable set. Unlike
    /// [`RecordingInner`], which records the keys handed to `delete_by_keys`, this one applies
    /// them — so a test can assert the surviving `(id, chunk_id)` rows after a delete, which is
    /// the property the chunked bridge exists to guarantee: every chunk of a deleted outer key
    /// goes, and only those.
    #[derive(Debug)]
    struct StatefulChunkInner {
        rows: std::sync::Mutex<Vec<(i64, u64)>>,
        deletes_partial_key: bool,
    }

    impl StatefulChunkInner {
        fn new(rows: &[(i64, u64)], deletes_partial_key: bool) -> Self {
            Self {
                rows: std::sync::Mutex::new(rows.to_vec()),
                deletes_partial_key,
            }
        }

        fn remaining(&self) -> Vec<(i64, u64)> {
            let mut out = self.rows.lock().expect("mutex").clone();
            out.sort_unstable();
            out
        }
    }

    #[async_trait]
    impl Index for StatefulChunkInner {
        fn name(&self) -> &'static str {
            "StatefulChunkInner"
        }
        fn required_columns(&self) -> Vec<String> {
            vec!["content".to_string()]
        }
        async fn delete_by_keys(&self, keys: RecordBatch) -> DataFusionResult<()> {
            let ids = keys
                .column_by_name("id")
                .expect("delete keys carry the base key")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");

            // The bridge either hands over the base key alone (partial-key path) or the resolved
            // chunk-keyed rows (exact-key path). Match on whichever columns are present.
            let mut rows = self.rows.lock().expect("mutex");
            if let Some(chunk_col) = keys.column_by_name(CHUNKED_INDEX_CHUNK_KEY) {
                let chunks = chunk_col
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("chunk id is UInt64");
                let doomed: std::collections::HashSet<(i64, u64)> = (0..keys.num_rows())
                    .map(|r| (ids.value(r), chunks.value(r)))
                    .collect();
                rows.retain(|pair| !doomed.contains(pair));
            } else {
                let doomed: std::collections::HashSet<i64> =
                    (0..keys.num_rows()).map(|r| ids.value(r)).collect();
                rows.retain(|(id, _)| !doomed.contains(id));
            }
            Ok(())
        }
        fn deletes_by_partial_key(&self) -> bool {
            self.deletes_partial_key
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[async_trait]
    impl VectorIndex for StatefulChunkInner {
        fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
            let rows = self.rows.lock().expect("mutex");
            let batch = chunk_keyed_rows(&rows);
            let schema = batch.schema();
            let table = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]])?;
            LogicalPlanBuilder::scan(
                "inner",
                Arc::new(datafusion::datasource::DefaultTableSource::new(Arc::new(
                    table,
                ))),
                None,
            )?
            .build()
        }
        fn dimension(&self) -> i32 {
            4
        }
    }

    #[async_trait]
    impl SearchIndex for StatefulChunkInner {
        fn search_column(&self) -> String {
            "content".to_string()
        }
        fn primary_fields(&self) -> Vec<Field> {
            ChunkedSearchIndex::augment_primary_key(vec![Field::new("id", DataType::Int64, false)])
        }
        fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
            Some(self)
        }
        /// Upserts the `(id, chunk_id)` rows of the chunked batch it is handed, so a test can
        /// write through [`ChunkedSearchIndex::write`] and then assert what the index holds.
        /// Returns the input with a synthetic embedding column, which is the shape the chunking
        /// layer folds back into per-document list columns.
        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            let ids = record
                .column_by_name("id")
                .expect("chunked batch carries the base key")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id is Int64");
            let chunks = record
                .column_by_name(CHUNKED_INDEX_CHUNK_KEY)
                .expect("chunked batch carries the chunk id")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("chunk id is UInt64");

            {
                let mut rows = self.rows.lock().expect("mutex");
                for r in 0..record.num_rows() {
                    let pair = (ids.value(r), chunks.value(r));
                    if !rows.contains(&pair) {
                        rows.push(pair);
                    }
                }
            }

            let n = record.num_rows();
            let emb_field = Arc::new(Field::new("item", DataType::Float32, true));
            let emb = FixedSizeListArray::try_new(
                Arc::clone(&emb_field),
                4,
                Arc::new(Float32Array::from(vec![0.0f32; n * 4])),
                None,
            )?;

            let mut fields: Vec<Field> = record
                .schema()
                .fields()
                .iter()
                .map(|f| Arc::unwrap_or_clone(Arc::clone(f)))
                .collect();
            let mut cols: Vec<ArrayRef> = record.columns().iter().map(Arc::clone).collect();
            fields.push(Field::new(
                embedding_col("content"),
                DataType::FixedSizeList(Arc::clone(&emb_field), 4),
                true,
            ));
            cols.push(Arc::new(emb) as ArrayRef);

            Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)?)
        }
        fn query_table_provider(&self, _query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
            Err(DataFusionError::NotImplemented(
                "unused in delete tests".into(),
            ))
        }
    }

    /// A row rewritten to a value that produces no chunks — NULL, empty, or whitespace the
    /// chunker yields nothing for — contributes no rows at all to the batch the inner index
    /// receives. The inner index therefore never sees that key on this write, so unless the
    /// chunking layer removes them first, every chunk the row's previous text produced stays
    /// searchable at its old vector and a search returns content the row no longer has.
    ///
    /// Regression test for #13704. Run over both inner-index shapes, because the two reach the
    /// existing chunks by different routes: a partial-key index deletes the whole group from the
    /// base key, an exact-key index has to enumerate the chunk-keyed rows first.
    #[tokio::test]
    async fn a_row_chunked_to_nothing_loses_the_chunks_its_previous_text_produced() {
        for partial_key in [false, true] {
            for emptied in [None, Some(""), Some("   ")] {
                let inner = Arc::new(StatefulChunkInner::new(&[], partial_key));
                let idx =
                    ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

                idx.write(build_input(&[("a b c", 1), ("d e", 2)]))
                    .await
                    .expect("first write ok");
                assert_eq!(
                    inner.remaining(),
                    vec![(1, 0), (1, 1), (1, 2), (2, 0), (2, 1)],
                    "the text each row started with is indexed chunk by chunk"
                );

                idx.write(build_input_opt(&[(emptied, 1)]))
                    .await
                    .expect("rewrite ok");

                assert_eq!(
                    inner.remaining(),
                    vec![(2, 0), (2, 1)],
                    "id 1 rewritten to {emptied:?} keeps nothing searchable \
                     (partial-key inner: {partial_key}); id 2 is untouched"
                );
            }
        }
    }

    /// A row that still chunks into something is written through to the inner index under the
    /// same key, so there is nothing for the chunking layer to reach around and remove.
    ///
    /// Reaching around costs an *enumeration* of everything the inner index holds — a full
    /// listing of an external store — so paying for it on every write would put that listing on
    /// the ordinary append and CDC paths. This asserts the listing count, not the delete count:
    /// a delete resolved from an empty key set issues no delete either way, so counting deletes
    /// would pass with the guard removed.
    #[tokio::test]
    async fn only_a_write_that_empties_a_row_resolves_the_inner_indexs_entries() {
        let inner = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[
            (1, 0),
            (1, 1),
            (1, 2),
        ])]));
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        idx.write(build_input(&[("a", 1), ("b c", 2)]))
            .await
            .expect("write ok");
        assert_eq!(
            inner.listings(),
            0,
            "an ordinary write must not enumerate the inner index"
        );
        assert!(
            inner.deletes().is_empty(),
            "no row emptied, so no eviction: {:?}",
            inner.deletes()
        );

        idx.write(build_input_opt(&[(None, 1)]))
            .await
            .expect("write ok");
        assert_eq!(
            inner.listings(),
            1,
            "emptying a row is what pays for the enumeration, and it pays once"
        );
        assert_eq!(resolved_ids(&inner.deletes()), vec![1, 1, 1]);
    }

    /// The write window decides whether a write may evict, and both chunked wrappers have to
    /// reach the same answer.
    ///
    /// A replacing write reproduces the table's whole contents, so an index that stages it keeps
    /// serving its previous rows until it commits. Evicting inside that window would resolve the
    /// previous contents' keys and apply the delete to the staged rows, taking out rows the same
    /// write just wrote — and there is nothing to evict anyway, since the commit replaces
    /// everything. An appending window is the opposite: its rows are added to what the index
    /// already holds, so an emptied row's previous chunks are still there and still have to go.
    ///
    /// `ChunkedVectorIndex` is covered by the same table because it delegates its writes to a
    /// `ChunkedSearchIndex`; were the two not sharing one window, the delegate would read "no
    /// window open" and evict the rows a replacing write is staging.
    #[tokio::test]
    async fn the_write_window_decides_whether_a_write_evicts() {
        for (window, evicts) in [
            (WriteWindow::ReplaceAll, false),
            (WriteWindow::Append, true),
        ] {
            for vector_wrapper in [false, true] {
                let inner = Arc::new(RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 0)])]));
                let idx: Arc<dyn Index> = if vector_wrapper {
                    Arc::new(ChunkedVectorIndex::new(
                        Arc::clone(&inner) as Arc<dyn VectorIndex>,
                        chunker(),
                    ))
                } else {
                    Arc::new(ChunkedSearchIndex::new(
                        Arc::clone(&inner) as Arc<dyn SearchIndex>,
                        chunker(),
                    ))
                };

                idx.on_write_start(window).await.expect("window opens");
                idx.compute_index(vec![build_input_opt(&[(None, 1)])])
                    .await
                    .expect("write ok");

                let expected = if evicts { vec![1] } else { vec![] };
                assert_eq!(
                    resolved_ids(&inner.deletes()),
                    expected,
                    "{window:?} on {} wrapper",
                    if vector_wrapper { "vector" } else { "search" }
                );

                // Closing the window always puts the index back on the evicting path.
                idx.on_write_complete().await.expect("window closes");
                idx.compute_index(vec![build_input_opt(&[(None, 1)])])
                    .await
                    .expect("write ok");
                assert!(
                    resolved_ids(&inner.deletes()).contains(&1),
                    "an emptied row evicts once the window is closed"
                );
            }
        }
    }

    /// The eviction must never remove a chunk this same write produced. One batch can carry the
    /// same key twice — once with text, once without — and that shape is resolved last-write-wins
    /// downstream but not here (#13713), so evicting on the emptied row would delete the chunks
    /// the other row just wrote and leave the row with nothing searchable at all.
    #[tokio::test]
    async fn a_key_this_write_also_chunked_is_not_evicted() {
        for rows in [
            vec![(Some("a b"), 1i64), (None, 1)],
            vec![(None, 1i64), (Some("a b"), 1)],
        ] {
            let inner = Arc::new(StatefulChunkInner::new(&[], false));
            let idx =
                ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

            idx.write(build_input_opt(&rows)).await.expect("write ok");

            assert_eq!(
                inner.remaining(),
                vec![(1, 0), (1, 1)],
                "the chunks this write produced survive it, whichever order the batch carries \
                 ({rows:?})"
            );
        }
    }

    /// The eviction is an externally visible delete that nothing restores, so it must not run
    /// ahead of the write it belongs to. A failed inner write leaves the row's previous chunks
    /// stale — which is the bug this PR narrows — but stale beats gone: evicting first would
    /// leave the row present in the table with nothing searchable under it at all.
    #[tokio::test]
    async fn a_write_that_fails_evicts_nothing() {
        let inner = Arc::new(RecordingInner {
            write_fails: true,
            ..RecordingInner::chunked(vec![chunk_keyed_rows(&[(1, 0)])])
        });
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        idx.write(build_input_opt(&[(None, 1), (Some("a b"), 2)]))
            .await
            .expect_err("the inner write fails");

        assert!(
            inner.deletes().is_empty(),
            "nothing landed, so nothing may be removed: {:?}",
            inner.deletes()
        );
    }

    /// The warning is the only account a user gets of why a search still returns text a row no
    /// longer has, so a reword must not quietly drop the column, the consequence, or the fix.
    #[test]
    fn the_unreachable_eviction_warning_says_what_the_user_will_see() {
        let msg = unreachable_chunk_eviction_warning("SomeIndex", "content");
        assert!(msg.contains("'content'"), "names the search column: {msg}");
        assert!(msg.contains("`SomeIndex`"), "names the index: {msg}");
        assert!(
            msg.contains("can still return content the row no longer has"),
            "says what the user will observe: {msg}"
        );
        assert!(
            msg.contains("Re-create the search index"),
            "gives an actionable fix: {msg}"
        );
        assert!(
            msg.contains("https://spiceai.org/docs/features/search"),
            "links the docs: {msg}"
        );
        assert!(!msg.contains('\n'), "stays on one line: {msg}");
    }

    /// An inner index that neither deletes by partial key nor can be enumerated as a vector
    /// index cannot be reached at all. The write still has to land: failing it would take the
    /// rest of the batch's new content with it and leave the stale chunks exactly where they are.
    #[tokio::test]
    async fn a_write_over_an_unreachable_inner_index_still_lands() {
        let inner = Arc::new(RecordingInner {
            is_vector_index: false,
            ..RecordingInner::new("content")
        });
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        let out = idx
            .write(build_input_opt(&[(None, 1), (Some("a b"), 2)]))
            .await
            .expect("the write lands even though the eviction cannot");

        assert_eq!(out.num_rows(), 2);
        assert!(inner.deletes().is_empty());
    }

    #[tokio::test]
    async fn deleting_an_outer_key_removes_its_every_chunk_from_a_partial_key_inner() {
        // Partial-key inner: the bridge forwards the base key straight to `delete_by_keys`.
        let inner = Arc::new(StatefulChunkInner::new(&[(1, 0), (1, 1), (2, 0)], true));
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        idx.delete_by_keys(outer_keys(&[1]))
            .await
            .expect("delete succeeds");

        assert_eq!(
            inner.remaining(),
            vec![(2, 0)],
            "both chunks of id 1 are gone; id 2's chunk stays"
        );
    }

    #[tokio::test]
    async fn deleting_an_outer_key_removes_its_every_chunk_from_an_exact_key_inner() {
        // Exact-key inner: the bridge resolves the chunk-keyed rows first, then deletes them.
        let inner = Arc::new(StatefulChunkInner::new(&[(1, 0), (1, 1), (2, 0)], false));
        let idx = ChunkedSearchIndex::new(Arc::clone(&inner) as Arc<dyn SearchIndex>, chunker());

        idx.delete_by_keys(outer_keys(&[1]))
            .await
            .expect("delete succeeds");

        assert_eq!(
            inner.remaining(),
            vec![(2, 0)],
            "both chunks of id 1 are resolved and removed; id 2's chunk stays"
        );
    }
}
