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
use std::{any::Any, sync::Arc};

use crate::s3_vectors::{
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME, vector_table::S3VectorsTable,
};

/// Num of segments to use for parallel `ListVectors` API calls.
const LIST_S3_VECTORS_NUM_READ_SEGMENTS: usize = 10;

use super::S3VectorIdentifier;
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
    json::ReaderBuilder,
};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::Constraints,
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::TableProviderFilterPushDown,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchReceiverStream,
    },
    prelude::Expr,
};
use futures::{StreamExt, stream::FuturesUnordered};
use s3_vectors::{
    LIST_VECTORS_MAX_RESULTS, ListOutputVector, ListVectorsInput, ListVectorsOutput, S3Vectors,
    VectorData,
};
use s3_vectors_metadata_filter::document_to_json_map;
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

/// An S3 Vector index that implements a [`TableProvider`] as a list records operation.
#[derive(Debug, Clone)]
pub struct S3VectorsListTable(S3VectorsTable);

impl From<S3VectorsTable> for S3VectorsListTable {
    fn from(tbl: S3VectorsTable) -> Self {
        Self(tbl)
    }
}

#[async_trait]
impl TableProvider for S3VectorsListTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.as_ref().schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.as_ref().constraints)
    }

    /// S3 vectors ListVectors API operation does not support filtering.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(S3VectorsListExec::new(self, projection, limit)) as Arc<dyn ExecutionPlan>)
    }
}

impl std::fmt::Debug for S3VectorsListExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorsListExec").finish_non_exhaustive()
    }
}

impl AsRef<S3VectorsTable> for S3VectorsListTable {
    fn as_ref(&self) -> &S3VectorsTable {
        &self.0
    }
}

struct S3VectorsListExec {
    idx: S3VectorIdentifier,
    client: Arc<dyn S3Vectors + Send + Sync>,
    plan_properties: PlanProperties,
    limit: Option<usize>,
}

impl DisplayAs for S3VectorsListExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "S3VectorsListExec")
    }
}

impl S3VectorsListExec {
    pub fn new(
        table: &S3VectorsListTable,
        projection: Option<&Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = match projection {
            Some(proj) => {
                let fields = proj
                    .iter()
                    .map(|&i| table.schema().field(i).clone())
                    .collect::<Vec<_>>();
                Arc::new(Schema::new(fields))
            }
            None => table.schema(),
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            idx: table.as_ref().idx.clone(),
            client: Arc::clone(&table.as_ref().client),
            plan_properties: properties,
            limit,
        }
    }
}

impl ExecutionPlan for S3VectorsListExec {
    fn name(&self) -> &'static str {
        "S3VectorsListExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let schema = Arc::clone(self.properties().equivalence_properties().schema());
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 2);
        let tx: Sender<DataFusionResult<RecordBatch, DataFusionError>> = builder.tx();

        let client = Arc::clone(&self.client);
        let idx = self.idx.clone();
        let limit = self.limit.unwrap_or(usize::MAX);

        builder.spawn(async move {
            if let Err(e) =
                list_vector_stream(client, idx, Arc::clone(&schema), limit, tx.clone()).await
            {
                let _ = tx.send(Err(e)).await;
            }
            Ok(())
        });

        Ok(builder.build())
    }
}

/// Streams S3 Vectors using segmented parallel API calls.
///
/// Launches up to `LIST_S3_VECTORS_NUM_READ_SEGMENTS` parallel segments, distributing the requested limit eventually.
/// Each segment fetches a portion of the total results concurrently, improving throughput.
///
/// Results are sent to the provided channel as soon as they are available and may arrive out of order
/// as they read in parallel using multiple segments.
#[allow(clippy::cast_possible_wrap)]
async fn list_vector_stream(
    client: Arc<dyn S3Vectors + Send + Sync>,
    idx: S3VectorIdentifier,
    schema: SchemaRef,
    limit: usize,
    tx: Sender<DataFusionResult<RecordBatch, DataFusionError>>,
) -> DataFusionResult<()> {
    let start = std::time::Instant::now();

    let segments_count = if limit == usize::MAX {
        LIST_S3_VECTORS_NUM_READ_SEGMENTS
    } else {
        (limit / LIST_VECTORS_MAX_RESULTS).clamp(1, LIST_S3_VECTORS_NUM_READ_SEGMENTS)
    };

    let mut tasks = FuturesUnordered::new();
    let mut total_vectors_retrieved = 0;

    for segment_idx in 0..segments_count {
        let segment_limit = if limit == usize::MAX {
            usize::MAX
        } else {
            // Distribute limit across segments
            limit / segments_count + usize::from(segment_idx < (limit % segments_count))
        };

        let task = list_vector_segment(
            Arc::clone(&client),
            idx.clone(),
            Arc::clone(&schema),
            segment_limit,
            segment_idx,
            segments_count,
            tx.clone(),
        );
        tasks.push(task);
    }

    // Process results as they complete
    while let Some(result) = tasks.next().await {
        match result {
            Ok(vectors_count) => {
                total_vectors_retrieved += vectors_count;
            }
            Err(e) => {
                let _ = tx.send(Err(e)).await;
                break;
            }
        }
    }

    let duration = start.elapsed();
    tracing::trace!(
        "S3 Vectors retrieved {total_vectors_retrieved} vectors in {duration:?} using {segments_count} parallel segments"
    );
    Ok(())
}

async fn list_vector_segment(
    client: Arc<dyn S3Vectors + Send + Sync>,
    idx: S3VectorIdentifier,
    schema: SchemaRef,
    limit: usize,
    segment_index: usize,
    segment_count: usize,
    tx: Sender<DataFusionResult<RecordBatch, DataFusionError>>,
) -> DataFusionResult<usize> {
    let start_segment = std::time::Instant::now();

    let (arn, bucket_name, index_name) = idx.index_identifier_variables();
    let mut decoder = ReaderBuilder::new(Arc::clone(&schema)).build_decoder()?;

    let mut remaining_limit = limit;
    let mut next_token = None;
    let mut segment_vectors_retrieved = 0;

    while remaining_limit > 0 {
        let ListVectorsOutput {
            next_token: next_token_opt,
            vectors,
            ..
        } = client
            .list_vectors(
                ListVectorsInput::builder()
                    .set_vector_bucket_name(bucket_name.clone())
                    .set_index_arn(arn.clone())
                    .set_index_name(index_name.clone())
                    .max_results(
                        i32::try_from(remaining_limit.min(LIST_VECTORS_MAX_RESULTS))
                            .unwrap_or(i32::MAX),
                    )
                    .set_next_token(next_token.clone())
                    .return_data(true)
                    .return_metadata(true)
                    .segment_count(i32::try_from(segment_count).unwrap_or(i32::MAX))
                    .segment_index(i32::try_from(segment_index).unwrap_or(i32::MAX))
                    .build()
                    .boxed()
                    .map_err(DataFusionError::External)?,
            )
            .await
            .boxed()
            .map_err(DataFusionError::External)?;

        remaining_limit = remaining_limit.saturating_sub(vectors.len());
        let num_vectors = vectors.len();
        segment_vectors_retrieved += num_vectors;
        next_token = next_token_opt;

        let rows: Vec<_> = vectors.into_iter().map(to_flat_value).collect();
        decoder.serialize(rows.as_slice()).map_err(|e| {
            DataFusionError::ArrowError(
                e,
                Some(
                    "could not convert ListVectors JSON response into expected Arrow format"
                        .to_string(),
                ),
            )
        })?;

        match decoder.flush() {
            Ok(Some(rb)) => {
                let _ = tx.send(Ok(rb)).await;
            }
            Ok(None) => {}
            Err(e) => {
                let _ = tx
                    .send(Err(DataFusionError::ArrowError(
                        e,
                        Some("Received only partial JSON payload from ListVectors".to_string()),
                    )))
                    .await;
            }
        }

        // No more results for this segment
        if next_token.is_none() {
            break;
        }
    }

    let duration_segment = start_segment.elapsed();
    tracing::trace!(
        "Segment {segment_index}/{segment_count} completed: retrieved {segment_vectors_retrieved} vectors in {duration_segment:?}"
    );

    Ok(segment_vectors_retrieved)
}

/// Converts a `ListOutputVector` into a flat JSON value (i.e unnest metadata fields).
fn to_flat_value(output: ListOutputVector) -> serde_json::Value {
    let ListOutputVector {
        metadata,
        data,
        key,
        ..
    } = output;
    let mut result = document_to_json_map(metadata.unwrap_or_default());
    if let Some(VectorData::Float32(vec)) = data {
        result.insert(
            S3_VECTOR_EMBEDDING_NAME.into(),
            serde_json::Value::Array(
                vec.into_iter()
                    .filter_map(|f| serde_json::Number::from_f64(f64::from(f)))
                    .map(serde_json::Value::Number)
                    .collect::<Vec<_>>(),
            ),
        );
    }
    result.insert(
        S3_VECTOR_PRIMARY_KEY_NAME.to_string(),
        serde_json::Value::String(key),
    );

    serde_json::Value::Object(result)
}
