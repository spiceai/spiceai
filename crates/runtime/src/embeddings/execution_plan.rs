/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Int32Array, ListArray, RecordBatch,
    StringArray, StringViewArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, SchemaRef};

use arrow::error::ArrowError;
use async_openai::types::EmbeddingInput;
use async_stream::stream;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use llms::chunking::Chunker;
use llms::embeddings::Embed;
use snafu::ResultExt;
use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use std::fmt;
use tokio::sync::RwLock;

use crate::model::EmbeddingModelStore;
use crate::{embedding_col, offset_col};

use super::table::EmbeddingColumnConfig;

pub struct EmbeddingTableExec {
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,

    base_plan: Arc<dyn ExecutionPlan>,

    embedded_columns: HashMap<String, EmbeddingColumnConfig>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
}

impl std::fmt::Debug for EmbeddingTableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "EmbeddingTable with columns {}, with inner={:#?}",
            self.embedded_columns.keys().join(", "),
            self.base_plan
        )
    }
}

impl DisplayAs for EmbeddingTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "EmbeddingTable: inner={:?}", self.base_plan)
    }
}

impl ExecutionPlan for EmbeddingTableExec {
    fn name(&self) -> &'static str {
        "EmbeddingTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.base_plan.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            &Arc::clone(&self.projected_schema),
            &self.filters,
            self.limit,
            Arc::clone(&self.base_plan).with_new_children(children)?,
            self.embedded_columns.clone(),
            Arc::clone(&self.embedding_models),
        )) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let s = self.base_plan.execute(partition, context)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            to_sendable_stream(
                s,
                Arc::clone(&self.projected_schema),
                self.embedded_columns.clone(),
                Arc::clone(&self.embedding_models),
            ),
        )))
    }
}

/// All [`Self::embedded_columns`] must be in [`Self::projected_schema`].
impl EmbeddingTableExec {
    pub(crate) fn new(
        projected_schema: &SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        base_plan: Arc<dyn ExecutionPlan>,
        embedded_columns: HashMap<String, EmbeddingColumnConfig>,
        embedding_models: Arc<RwLock<EmbeddingModelStore>>,
    ) -> Self {
        Self {
            projected_schema: Arc::clone(projected_schema),
            filters: filters.to_vec(),
            limit,
            properties: Self::compute_properties(&base_plan, projected_schema),
            base_plan,
            embedded_columns,
            embedding_models,
        }
    }

    fn compute_properties(
        base_plan: &Arc<dyn ExecutionPlan>,
        projected_schema: &SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(Arc::clone(projected_schema));
        let partitioning = base_plan.properties().partitioning.clone();
        let execution_mode = base_plan.properties().execution_mode();
        PlanProperties::new(eq_properties, partitioning, execution_mode)
    }
}

fn to_sendable_stream(
    mut base_stream: SendableRecordBatchStream,
    projected_schema: SchemaRef,
    embedded_columns: HashMap<String, EmbeddingColumnConfig>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static {
    stream! {
        while let Some(batch_result) = base_stream.next().await {
            match batch_result {
                Ok(batch) => {
                    match compute_additional_embedding_columns(&batch, &embedded_columns, Arc::clone(&embedding_models)).await {
                        Ok(embeddings) => {

                            match construct_record_batch(
                                &batch,
                                &Arc::clone(&projected_schema),
                                &embeddings,
                            ) {
                                Ok(embedded_batch) => yield Ok(embedded_batch),
                                Err(e) => {
                                    tracing::debug!("Failed to construct record batch");
                                    yield Err(DataFusionError::ArrowError(e, None))
                                },
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Error when getting embedding columns: {:?}", e);
                            yield Err(DataFusionError::Internal(e.to_string()));

                        },
                    };
                },
                Err(e) => {
                    tracing::debug!("Error in underlying base stream: {e:?}");
                    yield Err(e)
                },
            }
        }
    }
}

fn construct_record_batch(
    batch: &RecordBatch,
    projected_schema: &SchemaRef,
    embedding_cols: &HashMap<String, ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    let cols: Vec<ArrayRef> = projected_schema
        .fields()
        .iter()
        .filter_map(|f| match embedding_cols.get(f.name()).cloned() {
            Some(embedded_col) => Some(embedded_col),
            None => batch.column_by_name(f.name()).cloned(),
        })
        .collect_vec();

    RecordBatch::try_new(Arc::clone(projected_schema), cols)
}

/// Get the additional, embedding, columns to add to the [`RecordBatch`]. The columns are
///     1. Embedding vectors for each column in `embedded_columns`.
///     2. If a [`Chunker`] is provided for a given column's [`EmbeddingColumnConfig`], an additional column of offsets. For
///         each string, these offsets map the substrings used for each embeddding vector.
///
/// For columns that are in the base table, no additional columns are calculated.
///
/// The additional columns returned here should match those specified in [`super::table::EmbeddingTable::embedding_fields`]
pub(crate) async fn compute_additional_embedding_columns(
    rb: &RecordBatch,
    embedded_columns: &HashMap<String, EmbeddingColumnConfig>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
) -> Result<HashMap<String, ArrayRef>, Box<dyn std::error::Error + Send + Sync>> {
    let additional_embedding_columns: HashMap<_, _> = embedded_columns
        .iter()
        .filter(|(_, cfg)| !cfg.in_base_table)
        .collect();

    tracing::trace!(
        "Of embedding columns: {:?}, only need to create embeddings for columns: {:?}",
        embedded_columns.keys().collect::<Vec<_>>(),
        additional_embedding_columns.keys().collect::<Vec<_>>()
    );

    let mut embed_arrays: HashMap<String, ArrayRef> = HashMap::with_capacity(
        additional_embedding_columns.len()
            + additional_embedding_columns
                .values()
                .filter(|cfg| cfg.chunker.is_some())
                .count(),
    );

    for (col, cfg) in embedded_columns {
        let EmbeddingColumnConfig {
            model_name,
            chunker: chunker_opt,
            ..
        } = cfg;
        tracing::trace!("Embedding column '{col}' with model {model_name}");
        let read_guard = embedding_models.read().await;
        let Some(model) = read_guard.get(model_name) else {
            tracing::debug!(
                "When embedding col='{col}', model {model_name} expected, but not found"
            );
            continue;
        };

        let Some(raw_data) = rb.column_by_name(col) else {
            tracing::debug!("When embedding col='{col}', column not found in record batch");
            continue;
        };

        let arr_iter: Box<dyn Iterator<Item = Option<&str>> + Send> =
            if let Some(arr) = raw_data.as_any().downcast_ref::<StringArray>() {
                Box::new(arr.iter())
            } else if let Some(arr) = raw_data.as_any().downcast_ref::<StringViewArray>() {
                Box::new(arr.iter())
            } else {
                tracing::debug!(
                    "Expected StringArray or StringViewArray for column '{}', but got {}",
                    col,
                    raw_data.data_type()
                );
                continue;
            };

        let list_array = if let Some(chunker) = chunker_opt {
            let (vectors, offsets) =
                get_vectors_with_chunker(arr_iter, Arc::clone(chunker), &**model).await?;
            tracing::trace!("Successfully embedded column '{col}' with chunking");
            embed_arrays.insert(offset_col!(col), Arc::new(offsets) as ArrayRef);

            Arc::new(vectors) as ArrayRef
        } else {
            let fixed_size_array = get_vectors(arr_iter, &**model).await?;
            tracing::trace!("Successfully embedded column '{col}'");
            Arc::new(fixed_size_array) as ArrayRef
        };
        embed_arrays.insert(embedding_col!(col), list_array);
    }
    Ok(embed_arrays)
}

/// Embed a [`StringArray`] using the provided [`Embed`] model. The output is a [`FixedSizeListArray`],
/// where each [`String`] gets embedded into a single [`f32`] vector.
///
/// ```text
///                +- Embedding Model -+
///                |                   v
/// +---------------------+      +-------------+
/// | "Hello, World!"     |      | [0.1, 1.2] |
/// | "How are you doing?"|      | [0.5, 0.6] |
/// | "I'm doing well."   |      | [1.1, 1.2] |
/// +---------------------+      +-------------+
///     [`StringArray`]        [`FixedSizeListArray`]
/// ```
async fn get_vectors(
    arr: impl Iterator<Item = Option<&str>>,
    model: &dyn Embed,
) -> Result<FixedSizeListArray, Box<dyn std::error::Error + Send + Sync>> {
    let field = Arc::new(Field::new("item", DataType::Float32, false));
    let column: Vec<String> = arr.filter_map(|s| s.map(ToString::to_string)).collect();
    let embedded_data = model.embed(EmbeddingInput::StringArray(column)).await?;
    let vector_length = embedded_data.first().map(Vec::len).unwrap_or_default();
    let processed = embedded_data.iter().flatten().copied().collect_vec();

    let values = Float32Array::try_new(processed.into(), None)?;
    FixedSizeListArray::try_new(
        Arc::clone(&field),
        i32::try_from(vector_length)?,
        Arc::new(values),
        None,
    )
    .boxed()
}

/// Embed a [`StringArray`] using the provided [`Embed`] model and [`Chunker`]. The output is a [`ListArray`],
/// where each input [`String`] gets chunked and embedded into a [`FixedSizeListArray`].
///
/// ```text
///                 +--- Chunker ---+               +--- Embedding Model ---+
///                 |               v               |                       v
/// +---------------------+  +-------------------------------+  +--------------------------------------+
/// | "Hello, World!"     |  | ["Hello, ", ", World!"]       |  | [[0.1, 1.2], [0.3, 0.4]]             |
/// | "How are you doing?"|  | ["How ", "are you ", "doing?"]|  | [[0.5, 0.6], [0.7, 0.8], [0.9, 1.0]] |
/// | "I'm doing well."   |  | ["I'm doing ", "doing well."] |  | [[1.1, 1.2], [1.3, 1.4]]             |
/// +---------------------+  +-------------------------------+  +--------------------------------------+
///     [`StringArray`]                     +                             [`ListArray`]
///                          +-------------------------------+
///                          | [[0, 7], [7, 15]              |
///                          | [[0, 4], [4, 12], [12, 18]]   |
///                          | [[0, 10], [10, 21]]           |
///                          +-------------------------------+
/// ```
async fn get_vectors_with_chunker(
    arr: impl Iterator<Item = Option<&str>>,
    chunker: Arc<dyn Chunker>,
    model: &dyn Embed,
) -> Result<(ListArray, ListArray), Box<dyn std::error::Error + Send + Sync>> {
    // Iterate over (chunks per row, (starting_offset into row, chunk))
    let (chunks_per_row, chunks_in_row): (Vec<_>, Vec<_>) = arr
        .filter_map(|s| match s {
            // TODO: filter_map doesn't handle nulls
            Some(s) => {
                let chunks = chunker
                    .chunk_indices(s)
                    .map(|(idx, s)| (idx, s.to_string()))
                    .collect_vec();
                Some((chunks.len(), chunks))
            }
            None => None,
        })
        .unzip();

    let (chunk_offsets, chunks): (Vec<_>, Vec<_>) = chunks_in_row.into_iter().flatten().unzip();

    let embedded_data = model
        .embed(EmbeddingInput::StringArray(chunks.clone()))
        .await
        .boxed()?;

    // `model.size()` is a `u32` for for compatibility with `FixedSizeList(FieldRef, i32)`.
    #[allow(clippy::cast_sign_loss)]
    let vector_length = model.size() as usize;

    let mut values = Float32Array::builder(embedded_data.len() * vector_length);
    let mut chunk_values = Int32Array::builder(embedded_data.len() * 2);

    let mut lengths = Vec::with_capacity(chunks_per_row.len());
    let mut curr = 0;

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    for chunkz_in_row in chunks_per_row {
        lengths.push(chunkz_in_row);

        // Get the actual vectors
        let inner = embedded_data.as_slice()[curr..curr + chunkz_in_row]
            .iter()
            .flatten()
            .copied()
            .collect_vec();
        values.append_slice(&inner); // I believe this is a clone under the hood.

        // Create flattened version of [(start, end), (start, end), ...]
        // Explicitly find `end` to handle when chunks have overlap.
        let mut inner_offsets = Vec::with_capacity(2 * chunkz_in_row);
        for i in 0..chunkz_in_row {
            let start = chunk_offsets[curr + i];
            let end = start
                + chunks
                    .as_slice()
                    .get(curr + i)
                    .map(String::len)
                    .unwrap_or_default();
            inner_offsets.push(start as i32);
            inner_offsets.push(end as i32);
        }
        chunk_values.append_slice(inner_offsets.as_slice());

        curr += chunkz_in_row;
    }

    // These are offsets for both the vectors and the content offsets
    let offsets = OffsetBuffer::<i32>::from_lengths(lengths.into_iter());

    let vectors = {
        let scalar_field = Arc::new(Field::new("item", DataType::Float32, false));

        // Inner FixedSizeListArray
        let fixed_size_list_array = FixedSizeListArray::try_new(
            Arc::clone(&scalar_field),
            i32::try_from(vector_length).boxed()?,
            Arc::new(values.finish()),
            None,
        )
        .boxed()?;

        ListArray::try_new(
            Arc::new(Field::new_fixed_size_list(
                "item",
                Arc::clone(&scalar_field),
                i32::try_from(vector_length).boxed()?,
                false,
            )),
            offsets.clone(),
            Arc::new(fixed_size_list_array),
            None,
        )
        .boxed()?
    };

    let content_offsets = {
        let scalar_field = Arc::new(Field::new("item", DataType::Int32, false));
        let fixed_size_list_array = FixedSizeListArray::try_new(
            Arc::clone(&scalar_field),
            2,
            Arc::new(chunk_values.finish()),
            None,
        )
        .boxed()?;

        ListArray::try_new(
            Arc::new(Field::new_fixed_size_list(
                "item",
                Arc::clone(&scalar_field),
                2,
                false,
            )),
            offsets,
            Arc::new(fixed_size_list_array),
            None,
        )
        .boxed()?
    };

    Ok((vectors, content_offsets))
}
