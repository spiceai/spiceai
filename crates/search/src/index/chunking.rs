use std::{any::Any, sync::Arc};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, VectorIndex, embedding_col},
    metadata::MetadataColumn,
};

use arrow::{
    array::{
        Array, ArrayRef, FixedSizeListArray, FixedSizeListBuilder, Int32Builder, LargeStringArray,
        ListArray, RecordBatch, StringArray, StringViewArray, UInt64Array,
    },
    buffer::OffsetBuffer,
    compute::concat,
};

use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use chunking::Chunker;
use datafusion::{
    common::Column,
    error::DataFusionError,
    functions_aggregate::expr_fn::{array_agg, first_value},
    logical_expr::{Aggregate, LogicalPlan, Sort, SortExpr, expr::Alias},
    prelude::{Expr, ExprFunctionExt, col},
    sql::TableReference,
};
use datafusion_expr::ident;
use futures::future::try_join_all;
use itertools::Itertools;
use runtime_datafusion_index::Index;
use snafu::{ResultExt, Snafu};
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
#[derive(Clone)]
pub struct ChunkedSearchIndex {
    inner: Arc<dyn SearchIndex>,
    chunker: Arc<dyn Chunker>,
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

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_start().await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_failed().await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_complete().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
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
        Self { inner, chunker }
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
        let embedding_col_name = embedding_col(self.search_column().as_str());
        let offset_col_name = Self::chunking_offset_col(self.search_column().as_str());

        let (mut fields, mut arrays): (Vec<Field>, Vec<ArrayRef>) = group_record
            .columns()
            .iter()
            .enumerate()
            .filter_map(|(i, arr)| {
                let field = schema.field(i).clone();
                if field.name() == &embedding_col_name || field.name() == &offset_col_name {
                    return None;
                }
                let result = if i == search_field_idx {
                    let chunked_array: ArrayRef = match field.data_type() {
                        DataType::LargeUtf8 => {
                            let values: Vec<String> = group_flatten_chunks
                                .iter()
                                .map(|s| (*s).to_string())
                                .collect();
                            Arc::new(LargeStringArray::from(values))
                        }
                        DataType::Utf8View => {
                            Arc::new(StringViewArray::from(group_flatten_chunks.clone()))
                        }
                        _ => {
                            let values: Vec<String> = group_flatten_chunks
                                .iter()
                                .map(|s| (*s).to_string())
                                .collect();
                            Arc::new(StringArray::from(values))
                        }
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
            Self::chunking_offset_col(self.search_column().as_str()),
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

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        let chunker = Arc::clone(&self.chunker);
        let vector_index = Arc::clone(&self.inner).as_vector_index()?;
        Some(Arc::new(ChunkedVectorIndex {
            inner: vector_index,
            chunker,
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
                    self.chunker
                        .chunk_with_offsets(s)
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
        let offsets_col_name = Self::chunking_offset_col(self.search_column().as_str());
        let embeddings_col_name = embedding_col(self.search_column().as_str());

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

            if let Some(arr) = inner_rb.column_by_name(&offsets_col_name) {
                group_offset_arrays.push(Arc::clone(arr));
            }
            if let Some(arr) = inner_rb.column_by_name(&embeddings_col_name) {
                group_embedding_arrays.push(Arc::clone(arr));
            }
        }

        // From the concatenated inner outputs we need {}_embedding and {}_offset, then convert
        // them from `<inner_type>` -> `List(<inner_type>)` (one list per original row, length
        // `repeats[i]`) so they can be added back to the original `record`. This is so any
        // downstream acceleration has them in the expected format on the write path.
        let (schema, mut arrs, _) = record.into_parts();
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();

        attach_list_column(
            &group_offset_arrays,
            &offsets_col_name,
            &repeats,
            &schema,
            &mut arrs,
            &mut fields,
        )?;
        attach_list_column(
            &group_embedding_arrays,
            &embeddings_col_name,
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
    chunker: Arc<dyn Chunker>,
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
        let primary_key_names: Vec<_> = self
            .inner
            .primary_fields()
            .iter()
            .filter(|f| f.name() != CHUNKED_INDEX_CHUNK_KEY)
            .map(|f| f.name().clone())
            .collect();

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
        ChunkedSearchIndex {
            inner: Arc::clone(&self.inner) as Arc<dyn SearchIndex>,
            chunker: Arc::clone(&self.chunker),
        }
        .required_columns()
    }

    /// Compute the index - if the index data is represented in the batch itself (i.e. a vector
    /// "*_embedding" column) then modify the provided batches to include the computed column.
    async fn compute_index(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        ChunkedSearchIndex {
            inner: Arc::clone(&self.inner) as Arc<dyn SearchIndex>,
            chunker: Arc::clone(&self.chunker),
        }
        .compute_index(batches)
        .await
    }

    async fn on_write_start(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_start().await
    }

    async fn on_write_failed(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_failed().await
    }

    async fn on_write_complete(&self) -> Result<(), DataFusionError> {
        self.inner.on_write_complete().await
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
        ChunkedSearchIndex {
            inner: Arc::clone(&self.inner) as Arc<dyn SearchIndex>,
            chunker: Arc::clone(&self.chunker),
        }
        .primary_fields()
    }

    /// Update the index based on a [`RecordBatch`] from the underlying table.
    async fn write(
        &self,
        record: RecordBatch,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        ChunkedSearchIndex {
            inner: Arc::clone(&self.inner) as Arc<dyn SearchIndex>,
            chunker: Arc::clone(&self.chunker),
        }
        .write(record)
        .await
    }

    /// A [`TableProvider`] containing the [`SearchIndex::primary_fields`], additional metadata
    /// columns, the associated vectors/indexed content of the [`SearchIndex::search_column`] and the
    ///  search score between `query` and the [`SearchIndex::search_column`].
    fn query_table_provider(&self, query: &str) -> Result<Arc<LogicalPlan>, DataFusionError> {
        ChunkedSearchIndex {
            inner: Arc::clone(&self.inner) as Arc<dyn SearchIndex>,
            chunker: Arc::clone(&self.chunker),
        }
        .query_table_provider(query)
    }

    fn as_vector_index(self: Arc<Self>) -> Option<Arc<dyn VectorIndex>> {
        Some(self as Arc<dyn VectorIndex>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Int32Array, Int64Array, StringArray};
    use chunking::Chunker;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    struct RecordingInner {
        search_column: String,
        calls: AtomicUsize,
        row_counts: std::sync::Mutex<Vec<usize>>,
    }

    impl RecordingInner {
        fn new(search_column: &str) -> Self {
            Self {
                search_column: search_column.to_string(),
                calls: AtomicUsize::new(0),
                row_counts: std::sync::Mutex::new(Vec::new()),
            }
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
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[async_trait]
    impl SearchIndex for RecordingInner {
        fn search_column(&self) -> String {
            self.search_column.clone()
        }

        fn primary_fields(&self) -> Vec<Field> {
            vec![Field::new("id", DataType::Int64, false)]
        }

        async fn write(
            &self,
            record: RecordBatch,
        ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.row_counts
                .lock()
                .expect("mutex")
                .push(record.num_rows());

            // Build a synthetic embedding column matching the row count.
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = rows.iter().map(|(_, id)| *id).collect();
        let contents: Vec<&str> = rows.iter().map(|(c, _)| *c).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(contents)),
            ],
        )
        .expect("valid batch")
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
        let big_doc: String = (0..200).map(|i| format!("w{i} ")).collect::<String>();
        let rows: Vec<(String, i64)> = (0..50)
            .map(|i| (big_doc.trim_end().to_string(), i as i64))
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
        let big = (0..INNER_WRITE_TARGET_CHUNKS + 100)
            .map(|i| format!("w{i} "))
            .collect::<String>();
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
        let big_doc: String = (0..5000).map(|i| format!("w{i} ")).collect::<String>();
        let rows: Vec<(String, i64)> = (0..3)
            .map(|i| (big_doc.trim_end().to_string(), i as i64))
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
}
