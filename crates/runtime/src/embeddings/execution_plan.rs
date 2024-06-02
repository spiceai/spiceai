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
    ArrayData, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Float32Type, SchemaRef};

use arrow::error::ArrowError;
use async_stream::stream;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{Stream, StreamExt};
use itertools::Itertools;
use std::collections::HashMap;
use std::hash::Hash;
use std::{any::Any, sync::Arc};

use llms::embeddings::{Embed, EmbeddingInput};
use std::fmt;
use tokio::sync::RwLock;

pub struct EmbeddingTableExec {
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,

    base_plan: Arc<dyn ExecutionPlan>,

    embedded_columns: HashMap<String, String>,
    embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
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
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.base_plan.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.projected_schema.clone(),
            &self.filters,
            self.limit,
            self.base_plan.clone().with_new_children(children)?,
            self.embedded_columns.clone(),
            self.embedding_models.clone(),
        )) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        println!("In execute");
        let s = self.base_plan.execute(partition, context)?;
        println!("In execute, after base: {:#?}", self.schema());
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            to_sendable_stream(
                s,
                self.projected_schema.clone(),
                self.embedded_columns.clone(),
                self.embedding_models.clone(),
            ),
        )))
    }
}

/// All [`Self::embedded_columns`] must be in [`Self::projected_schema`].
impl EmbeddingTableExec {
    pub(crate) fn new(
        projected_schema: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
        base_plan: Arc<dyn ExecutionPlan>,
        embedded_columns: HashMap<String, String>,
        embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
    ) -> Self {
        Self {
            projected_schema: Arc::clone(&projected_schema),
            filters: filters.to_vec(),
            limit,
            properties: base_plan.properties().clone(),
            base_plan,
            embedded_columns,
            embedding_models,
        }
    }
}

fn to_sendable_stream(
    mut base_stream: SendableRecordBatchStream,
    projected_schema: SchemaRef,
    embedded_columns: HashMap<String, String>,
    embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> + 'static {
    stream! {
        while let Some(batch_result) = base_stream.next().await {
            match batch_result {
                Ok(batch) => {
                    match get_embeddings(&batch, &embedded_columns, embedding_models.clone()).await {
                        Ok(embeddings) => {

                            match construct_record_batch(
                                &batch,
                                projected_schema.clone(),
                                embeddings,
                            ) {
                                Ok(embedded_batch) => yield Ok(embedded_batch),
                                Err(e) => {
                                    yield Err(DataFusionError::ArrowError(e, None))
                                },
                            }
                        }
                        Err(e) => {
                            println!("Error: {:#?}", e);
                            // yield Err(e.into())
                        }, // yield Err(DataFusionError::Internal(format!("{e}"))),
                    };
                },
                Err(e) => yield Err(e.into()),
            }
        }
    }
}

fn construct_record_batch(
    batch: &RecordBatch,
    projected_schema: SchemaRef,
    embedding_cols: HashMap<String, ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    let cols: Vec<ArrayRef> = projected_schema
        .all_fields()
        .iter()
        .map(|&f| match embedding_cols.get(f.name()).map(|c| c.clone()) {
            Some(embedded_col) => Some(embedded_col),
            None => batch.column_by_name(f.name()).cloned(),
        })
        .flatten()
        .collect_vec();
    RecordBatch::try_new(projected_schema.clone(), cols)
}

async fn get_embeddings(
    rb: &RecordBatch,
    embedded_columns: &HashMap<String, String>,
    embedding_models: Arc<RwLock<HashMap<String, RwLock<Box<dyn Embed>>>>>,
) -> Result<HashMap<String, ArrayRef>, Box<dyn std::error::Error + Send + Sync>> {
    let field = Arc::new(Field::new("item", DataType::Float32, false));

    let mut embed_arrays: HashMap<String, ArrayRef> =
        HashMap::with_capacity(embedded_columns.len());
    for (col, model_name) in embedded_columns.iter() {
        let read_guard = embedding_models.read().await;
        println!("model_names: {:#?}", read_guard.keys().join(", "));
        let model_lock_opt = read_guard.get(model_name);

        let Some(model_lock) = model_lock_opt else {
            println!("model local baddie: {:#?}", model_name);
            continue;
        };

        let mut model = model_lock.write().await;

        let raw_data = match rb.column_by_name(col) {
            None => {
                println!("column not found: {:#?}", col);
                continue;
            }
            Some(data) => data,
        };

        let column: Vec<String> = raw_data
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray")
            .iter()
            .filter_map(|s| s.map(|ss| ss.to_string()))
            .collect();

        let embedded_data = model.embed(EmbeddingInput::StringBatch(column)).await?;
        let vector_length = embedded_data.first().map(|f| f.len()).unwrap_or_default();
        let processed = embedded_data.iter().flatten().map(|&x| x).collect_vec();

        let values = Float32Array::try_new(processed.into(), None).unwrap();
        let list_array = FixedSizeListArray::try_new(
            field.clone(),
            vector_length as i32,
            Arc::new(values),
            None,
        )
        .unwrap();
        embed_arrays.insert(format!("{col}_embedding"), Arc::new(list_array));
    }
    println!("embed_arrays: {:#?}", embed_arrays.len());
    Ok(embed_arrays)
}
