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

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray};
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
use std::collections::HashMap;
use std::{any::Any, sync::Arc};

use std::fmt;
use tokio::sync::RwLock;

use crate::EmbeddingModelStore;

pub struct EmbeddingTableExec {
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,

    base_plan: Arc<dyn ExecutionPlan>,

    embedded_columns: HashMap<String, String>,
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
        embedded_columns: HashMap<String, String>,
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
    embedded_columns: HashMap<String, String>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static {
    stream! {
        while let Some(batch_result) = base_stream.next().await {
            match batch_result {
                Ok(batch) => {
                    match get_embeddings(&batch, &embedded_columns, Arc::clone(&embedding_models)).await {
                        Ok(embeddings) => {

                            match construct_record_batch(
                                &batch,
                                &Arc::clone(&projected_schema),
                                &embeddings,
                            ) {
                                Ok(embedded_batch) => yield Ok(embedded_batch),
                                Err(e) => {
                                    yield Err(DataFusionError::ArrowError(e, None))
                                },
                            }
                        }
                        Err(e) => {
                            yield Err(DataFusionError::Internal(e.to_string()));

                        },
                    };
                },
                Err(e) => yield Err(e),
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
        .all_fields()
        .iter()
        .filter_map(|&f| match embedding_cols.get(f.name()).cloned() {
            Some(embedded_col) => Some(embedded_col),
            None => batch.column_by_name(f.name()).cloned(),
        })
        .collect_vec();
    RecordBatch::try_new(Arc::clone(projected_schema), cols)
}

async fn get_embeddings(
    rb: &RecordBatch,
    embedded_columns: &HashMap<String, String>,
    embedding_models: Arc<RwLock<EmbeddingModelStore>>,
) -> Result<HashMap<String, ArrayRef>, Box<dyn std::error::Error + Send + Sync>> {
    let field = Arc::new(Field::new("item", DataType::Float32, false));

    let mut embed_arrays: HashMap<String, ArrayRef> =
        HashMap::with_capacity(embedded_columns.len());
    for (col, model_name) in embedded_columns {
        let read_guard = embedding_models.read().await;
        let model_lock_opt = read_guard.get(model_name);

        let Some(model_lock) = model_lock_opt else {
            continue;
        };

        let mut model = model_lock.write().await;

        let raw_data = match rb.column_by_name(col) {
            None => {
                continue;
            }
            Some(data) => data,
        };

        let string_array = raw_data.as_any().downcast_ref::<StringArray>();

        let Some(arr) = string_array else {
            continue;
        };

        let column: Vec<String> = arr
            .iter()
            .filter_map(|s| s.map(ToString::to_string))
            .collect();

        let embedded_data = model.embed(EmbeddingInput::StringArray(column)).await?;
        let vector_length = embedded_data.first().map(Vec::len).unwrap_or_default();
        let processed = embedded_data.iter().flatten().copied().collect_vec();

        let values = Float32Array::try_new(processed.into(), None)?;
        let list_array = FixedSizeListArray::try_new(
            Arc::clone(&field),
            i32::try_from(vector_length)?,
            Arc::new(values),
            None,
        )?;
        embed_arrays.insert(format!("{col}_embedding"), Arc::new(list_array));
    }
    Ok(embed_arrays)
}
