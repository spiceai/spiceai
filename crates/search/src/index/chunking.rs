use std::{any::Any, sync::Arc};

use crate::{
    SEARCH_SCORE_COLUMN_NAME,
    index::{SearchIndex, VectorIndex, embedding_col},
    metadata::{MetadataColumn, MetadataColumns},
};

use arrow::{
    array::{
        ArrayRef, FixedSizeListArray, FixedSizeListBuilder, Int32Builder, LargeStringArray,
        ListArray, RecordBatch, StringArray, StringViewArray, UInt64Array,
    },
    buffer::OffsetBuffer,
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

    #[snafu(display("Cannot write search index. Could not contruct chunked Array data: {source}"))]
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

    fn metadata_columns(&self) -> &MetadataColumns {
        self.inner.metadata_columns()
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
    #[allow(clippy::too_many_lines)]
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

        // For each element of the search column, chunk and keep offsets
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

        let chunk_index: Vec<_> = chunks
            .iter()
            .flat_map(|v| (0..(v.len() as u64)).collect::<Vec<_>>())
            .collect();
        let flatten_chunks: Vec<_> = chunks.into_iter().flatten().collect();

        let (mut fields, mut arrays): (Vec<Field>, Vec<ArrayRef>) = record
            .columns()
            .iter()
            .enumerate()
            .map(|(i, arr)| {
                let field = schema.field(i).clone();
                if i == search_field_idx {
                    Ok((
                        field,
                        Arc::new(StringArray::from(
                            flatten_chunks
                                .iter()
                                .map(|s| (*s).to_string())
                                .collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ))
                } else if schema
                    .column_with_name(CHUNKED_INDEX_FULL_SEARCH_FIELD)
                    .is_some_and(|(idx, _)| i == idx)
                {
                    // If self.search_field is in self.inner.metadata_columns, we must add additional column. This colum will have full content.
                    // During list/search, we shall ask for this column instead of the chunked version.
                    // The chunked version must be provided to `self.inner` so that it can be indexed appropriately (e.g. embedded).
                    Ok((field, repeat(search_field_array, &repeats)?))
                } else {
                    Ok((field, repeat(arr, &repeats)?))
                }
            })
            .collect::<Result<Vec<_>, ArrowError>>()?
            .into_iter()
            .filter(|(f, _)| {
                *f.name() != embedding_col(self.search_column().as_str())
                    && *f.name() != Self::chunking_offset_col(self.search_column().as_str())
            })
            .unzip();

        fields.push(Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false));
        arrays.push(Arc::new(UInt64Array::from(chunk_index)) as ArrayRef);

        fields.push(Field::new(
            Self::chunking_offset_col(self.search_column().as_str()),
            DataType::new_fixed_size_list(DataType::Int32, 2, false),
            false,
        ));
        arrays.push(Arc::new(to_offset_array(&offsets, false)) as ArrayRef);

        let rb = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .context(WriteFailedConstructRecordBatchSnafu)
            .boxed()?;

        let inner_rb = self
            .inner
            .write(rb)
            .await
            .context(InnerIndexWriteSnafu)
            .boxed()?;

        // From `inner_rb` we need to get {}_embedding, and {}_offset
        //   then convert them from FixedSizeList() -> List(FixedSizeList())
        //   so they can be added back to original `record`.
        // This is so any acceleration has them in the expected format on the write path.
        let (schema, mut arrs, _) = record.into_parts();
        let mut fields: Vec<_> = schema.fields().iter().cloned().collect();

        let offsets = Self::chunking_offset_col(self.search_column().as_str());
        if let Some(arr) = inner_rb.column_by_name(&offsets) {
            let f = Arc::new(Field::new("item", arr.data_type().clone(), true));
            let arr = Arc::new(
                ListArray::try_new(
                    Arc::clone(&f),
                    OffsetBuffer::from_lengths(repeats.iter().copied()),
                    Arc::clone(arr),
                    None,
                )
                .boxed()?,
            );
            if let Some((i, _)) = schema.column_with_name(&offsets) {
                arrs[i] = arr;
            } else {
                arrs.push(arr);
                fields.push(f);
            }
        }

        let embeddings = embedding_col(&self.search_column());
        if let Some(arr) = inner_rb.column_by_name(&embeddings) {
            let f = Arc::new(Field::new("item", arr.data_type().clone(), true));
            let arr = Arc::new(
                ListArray::try_new(
                    Arc::clone(&f),
                    OffsetBuffer::from_lengths(repeats.iter().copied()),
                    Arc::clone(arr),
                    None,
                )
                .boxed()?,
            );
            if let Some((i, _)) = schema.column_with_name(&embeddings) {
                arrs[i] = arr;
            } else {
                arrs.push(arr);
                fields.push(f);
            }
        }

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
            .filter(|f| !pk_names.contains(f.name()) && f.name() != CHUNKED_INDEX_FULL_SEARCH_FIELD)
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

        let agg =
            LogicalPlan::Aggregate(Aggregate::try_new(tbl, pk_expr.clone(), aggr_expr.clone())?);

        let final_sort = LogicalPlan::Sort(Sort {
            expr: vec![SortExpr::new(col(SEARCH_SCORE_COLUMN_NAME), false, false)],
            input: agg.into(),
            fetch: None,
        });

        Ok(Arc::new(final_sort))
    }
}

#[allow(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]
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
    fn list_table_provider(&self) -> Result<LogicalPlan, DataFusionError> {
        let base_index_table = self.inner.list_table_provider()?;
        let group_by_pks: Vec<_> = self
            .inner
            .primary_fields()
            .iter()
            .filter(|f| f.name() != CHUNKED_INDEX_CHUNK_KEY)
            .map(|f| Expr::Column(Column::new_unqualified(f.name())))
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
        aggr_expr.extend(
            self.inner
                .metadata_columns()
                .all_names()
                .iter()
                .filter_map(|c| {
                    if [
                        ChunkedSearchIndex::chunking_offset_col(self.search_column().as_str()),
                        embedding_col(self.search_column().as_str()),
                        CHUNKED_INDEX_CHUNK_KEY.to_string(),
                        self.search_column(),
                    ]
                    .contains(c)
                    {
                        return None;
                    }
                    Some(Expr::Alias(Alias::new(
                        first_value(Expr::Column(Column::new_unqualified(c)), vec![]),
                        None::<TableReference>,
                        c.clone(),
                    )))
                })
                .collect::<Vec<_>>(),
        );

        if self
            .inner
            .metadata_columns()
            .all_names()
            .contains(&CHUNKED_INDEX_FULL_SEARCH_FIELD.to_string())
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
            Aggregate::try_new(base_index_table.into(), group_by_pks, aggr_expr).boxed()?,
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

    /// The additional columns available in the [`SearchIndex`].
    /// For FTS indexes, this may return empty metadata columns.
    fn metadata_columns(&self) -> &MetadataColumns {
        self.inner.metadata_columns()
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
