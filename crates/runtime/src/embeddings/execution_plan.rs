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

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, FixedSizeListBuilder, LargeStringArray, ListArray,
    PrimitiveBuilder, RecordBatch, StringArray, StringViewArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Float32Type, Int32Type, SchemaRef};

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
use crate::{convert_string_arrow_to_iterator, embedding_col, offset_col};

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
                            yield Err(DataFusionError::External(e.to_string().into()));

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

        let Some(arr_iter) = convert_string_arrow_to_iterator!(raw_data) else {
            tracing::warn!(
                    "Expected 'StringArray', 'StringViewArray' or 'LargeStringArray' for column '{}', but got {}",
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
///
/// Elements of `arr` that are `Some("")` or `None` should not be passed into
/// [`Embed`]. This means that `arr` must be partitioned, then reconstructed
/// after the non empty/null inputs are embedded.
///
/// ```text
///                                     +- Embedding Model -+
/// +---------------------+             |                   v
/// | "Hello"             |      +--------------+    +------------+
/// | None                | ---> | "Hello"      |    | [0.1, 1.2] |
/// | ""                  |      | "Valid text" |    | [0.8, 0.9] |
/// | "Valid text"        |      +--------------+    +------------+
/// +---------------------+                             |  |
///    [`StringArray`]                                  |  |
///                   +------------+                    |  |
///                   | [0.1, 0.2] | <------------------+  |
///                   | None       |                       |
///                   | None       |                       |
///                   | [0.8, 0.9] | <---------------------+
///                   +------------+
///
///                 [`FixedSizeListArray`]
/// ```
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
async fn get_vectors(
    arr: impl Iterator<Item = Option<&str>>,
    model: &dyn Embed,
) -> Result<FixedSizeListArray, Box<dyn std::error::Error + Send + Sync>> {
    // Filter out nulls or empty strings before calling [`Embed::embed`].
    let (null_pairs, values): (Vec<_>, Vec<_>) = arr
        .enumerate()
        .partition(|(_, o)| o.is_none() || o.is_some_and(str::is_empty));
    let nulls: Vec<usize> = null_pairs.into_iter().map(|(i, _)| i).collect();
    let column: Vec<String> = values
        .iter()
        .filter_map(|(_, s)| s.map(ToString::to_string))
        .collect();

    let embedded_data = model.embed(EmbeddingInput::StringArray(column)).await?;
    let vector_length = embedded_data.first().map(Vec::len).unwrap_or_default();

    let mut builder = FixedSizeListBuilder::with_capacity(
        PrimitiveBuilder::<Float32Type>::with_capacity(
            (embedded_data.len() + nulls.len()) * vector_length,
        ),
        vector_length as i32,
        embedded_data.len() + nulls.len(),
    )
    .with_field(Arc::new(Field::new("item", DataType::Float32, false)));

    // Current index into offset of the outputted [`FixedSizeList`].
    let mut output_ptr: usize = 0;

    // Index into `nulls` array. Value at i'th position is index into output order that should be nulled.
    let mut null_ptr = 0;

    // Reconstruct correct output order by adding back nulls based on original indexes of nulls.
    for vector in embedded_data {
        // Keep inserting nulls until we reach the next non-null value.
        while nulls.get(null_ptr).is_some_and(|&idx| idx == output_ptr) {
            builder.values().append_nulls(vector_length);
            null_ptr += 1;
            output_ptr += 1;
            builder.append(false);
        }

        builder.values().append_slice(&vector);
        output_ptr += 1;
        builder.append(true);
    }

    // Handle any trailing nulls/empty strings.
    while nulls.get(null_ptr).is_some_and(|&idx| idx == output_ptr) {
        builder.values().append_nulls(vector_length);
        null_ptr += 1;
        output_ptr += 1;
        builder.append(false);
    }

    Ok(builder.finish())
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
/// | ""                  |  | []                            |  | []                                   |
/// | "I'm doing well."   |  | ["I'm doing ", "doing well."] |  | [[1.1, 1.2], [1.3, 1.4]]             |
/// +---------------------+  +-------------------------------+  +--------------------------------------+
///     [`StringArray`]                     +                             [`ListArray`]
///                          +-------------------------------+
///                          | [[0, 7], [7, 15]              |
///                          | [[0, 4], [4, 12], [12, 18]]   |
///                          | []                            |
///                          | [[0, 10], [10, 21]]           |
///                          +-------------------------------+
/// ```
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
async fn get_vectors_with_chunker(
    arr: impl Iterator<Item = Option<&str>>,
    chunker: Arc<dyn Chunker>,
    model: &dyn Embed,
) -> Result<(ListArray, ListArray), Box<dyn std::error::Error + Send + Sync>> {
    // Iterate over (chunks per row, (starting_offset into row, chunk))
    let (chunks_per_row, chunks_in_row): (Vec<_>, Vec<_>) = arr
        // TODO: filter_map doesn't handle nulls
        .map(|s| match s {
            Some(s) if !s.is_empty() => {
                let chunks = chunker
                    .chunk_indices(s)
                    .map(|(idx, s)| (idx, s.to_string()))
                    .collect_vec();
                (chunks.len(), chunks)
            }
            // None, or empty string.
            _ => (0, vec![]),
        })
        .unzip();

    let (chunk_offsets, chunks): (Vec<_>, Vec<_>) = chunks_in_row.into_iter().flatten().unzip();

    let embedded_data = model
        .embed(EmbeddingInput::StringArray(chunks.clone()))
        .await
        .boxed()?;

    #[allow(clippy::cast_sign_loss)]
    let vector_length = model.size();

    let capacity = chunks_per_row.iter().sum();

    #[allow(clippy::cast_sign_loss)]
    let mut vectors_builder = FixedSizeListBuilder::with_capacity(
        PrimitiveBuilder::<Float32Type>::with_capacity(capacity * (vector_length as usize)),
        vector_length,
        capacity,
    )
    .with_field(Arc::new(Field::new("item", DataType::Float32, false)));

    let mut chunks_builder = FixedSizeListBuilder::with_capacity(
        PrimitiveBuilder::<Int32Type>::with_capacity(capacity),
        2,
        capacity,
    )
    .with_field(Arc::new(Field::new("item", DataType::Int32, false)));

    let mut lengths = Vec::with_capacity(chunks_per_row.len());
    let mut curr = 0;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_possible_wrap,
        clippy::cast_sign_loss
    )]
    for chunkz_in_row in chunks_per_row {
        // Get the actual vectors
        for i in curr..curr + chunkz_in_row {
            if let Some(v) = embedded_data.get(i) {
                vectors_builder.values().append_slice(v); // I believe this is a clone under the hood.
                vectors_builder.append(true);
            }
        }

        // Explicitly find `end` to handle when chunks have overlap.
        for i in 0..chunkz_in_row {
            let start = chunk_offsets[curr + i];
            let end = start
                + chunks
                    .as_slice()
                    .get(curr + i)
                    .map(String::len)
                    .unwrap_or_default();

            chunks_builder.values().append_value(start as i32);
            chunks_builder.values().append_value(end as i32);
            chunks_builder.append(true);
        }
        curr += chunkz_in_row;
        lengths.push(chunkz_in_row);
    }

    // These are offsets for both the vectors and the content offsets.
    // They tell the [`ListArray`] how many vectors/offsets are in each row of the input/output table.
    let offsets = OffsetBuffer::<i32>::from_lengths(lengths.into_iter());

    let vectors = ListArray::try_new(
        Arc::new(Field::new_fixed_size_list(
            "item",
            Arc::new(Field::new("item", DataType::Float32, false)),
            vector_length,
            false,
        )),
        offsets.clone(),
        Arc::new(vectors_builder.finish()),
        None,
    )
    .boxed()?;

    let content_offsets = ListArray::try_new(
        Arc::new(Field::new_fixed_size_list(
            "item",
            Arc::new(Field::new("item", DataType::Int32, false)),
            2,
            false,
        )),
        offsets,
        Arc::new(chunks_builder.finish()),
        None,
    )
    .boxed()?;

    Ok((vectors, content_offsets))
}

#[allow(clippy::float_cmp)]
#[cfg(test)]
mod tests {

    use crate::embeddings::execution_plan::get_vectors;
    use arrow::{
        array::{Array, AsArray},
        datatypes::Float32Type,
    };
    use async_openai::types::EmbeddingInput;
    use async_trait::async_trait;
    use llms::embeddings::{self, Embed};
    use std::collections::HashMap;

    #[derive(Default)]
    pub(crate) struct MockEmbedder {
        pub map: HashMap<String, Vec<f32>>,
    }

    impl MockEmbedder {
        pub fn with_pair(mut self, input: &'static str, output: Vec<f32>) -> Self {
            self.map.insert(input.to_string(), output);
            self
        }
    }

    #[async_trait]
    impl Embed for MockEmbedder {
        fn size(&self) -> i32 {
            -1
        }

        async fn embed(&self, input: EmbeddingInput) -> Result<Vec<Vec<f32>>, embeddings::Error> {
            match input {
                EmbeddingInput::String(s) => {
                    let v = self.map.get(&s).cloned().unwrap_or_default();
                    Ok(vec![v])
                }
                EmbeddingInput::StringArray(arr) => {
                    let v = arr
                        .iter()
                        .map(|s| self.map.get(s).cloned().unwrap_or_default())
                        .collect();
                    Ok(v)
                }
                _ => Err(embeddings::Error::FailedToCreateEmbedding {
                    source: Box::<dyn std::error::Error + Send + Sync>::from(
                        "Unsupported input type",
                    ),
                }),
            }
        }
    }

    #[tokio::test]
    async fn test_get_vectors_basic() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let fixed_size_array = get_vectors(
            vec![Some("hello"), Some("world")].into_iter(),
            &MockEmbedder::default()
                .with_pair("hello", vec![0.1, 0.2])
                .with_pair("world", vec![0.3, 0.4]),
        )
        .await?;

        let values = fixed_size_array.values().as_primitive::<Float32Type>();

        assert_eq!(fixed_size_array.len(), 2);
        assert_eq!(values.value(0), 0.1);
        assert_eq!(values.value(1), 0.2);
        assert_eq!(values.value(2), 0.3);
        assert_eq!(values.value(3), 0.4);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_vectors_with_nulls_and_empty(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = get_vectors(
            vec![None, Some("world"), Some(""), Some("hello"), None].into_iter(),
            &MockEmbedder::default()
                .with_pair("hello", vec![0.1, 0.2])
                .with_pair("world", vec![0.3, 0.4]),
        )
        .await?;

        assert_eq!(result.len(), 5);

        assert!(result.is_null(0));
        assert!(!result.is_null(1));
        assert!(result.is_null(2));
        assert!(!result.is_null(3));
        assert!(result.is_null(4));

        let values = result.values().as_primitive::<Float32Type>();
        assert_eq!(values.len(), 10);
        assert_eq!(values.value(2), 0.3);
        assert_eq!(values.value(3), 0.4);
        assert_eq!(values.value(6), 0.1);
        assert_eq!(values.value(7), 0.2);

        Ok(())
    }
}
