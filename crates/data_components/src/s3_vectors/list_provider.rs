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
    Error, S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    partition::{BelongsWith, PartitionedIndexName},
    vector_table::{S3VectorsTable, loosen_vector_schema, send_vector_data},
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
    common::{Constraints, exec_err, project_schema},
    datasource::TableType,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::TableProviderFilterPushDown,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        empty::EmptyExec,
        execution_plan::{Boundedness, EmissionType},
        limit::GlobalLimitExec,
        stream::RecordBatchReceiverStream,
        union::UnionExec,
    },
    prelude::Expr,
};
use futures::{StreamExt, stream::FuturesUnordered};
use s3_vectors::{
    LIST_VECTORS_MAX_RESULTS, ListIndexesInput, ListOutputVector, ListVectorsInput,
    ListVectorsOutput, S3Vectors, VectorData,
};
use s3_vectors_metadata_filter::document_to_json_map;
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

/// An S3 Vector index that implements a [`TableProvider`] as a list records operation.
#[derive(Debug, Clone)]
pub struct S3VectorsListTable {
    table: S3VectorsTable,
    column_name: String,
    partition_by: Vec<Expr>,
}

impl S3VectorsListTable {
    #[must_use]
    pub fn new(table: S3VectorsTable, column_name: String, partition_by: Vec<Expr>) -> Self {
        Self {
            table,
            column_name,
            partition_by,
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsListTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    /// S3 vectors `ListVectors` API operation does not support filtering.
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
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if self.partition_by.is_empty() {
            return Ok(
                Arc::new(S3VectorsListExec::new(self, projection, limit)) as Arc<dyn ExecutionPlan>
            );
        }

        let (_, Some(bucket_name), Some(index_name)) = self.table.idx.index_identifier_variables()
        else {
            return exec_err!("No bucket name or index name for bucket query");
        };

        let prefix =
            PartitionedIndexName::common_prefix(&index_name, &self.column_name, &self.partition_by)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;

        let list_indexes_output = self
            .table
            .client
            .list_indexes(
                ListIndexesInput::builder()
                    .set_vector_bucket_name(Some(bucket_name.clone()))
                    .set_prefix(Some(prefix))
                    .build()
                    .boxed()
                    .map_err(DataFusionError::External)?,
            )
            .await
            .map_err(|e| {
                DataFusionError::External(
                    Error::S3VectorListIndexesError {
                        source: e.into_service_error(),
                    }
                    .into(),
                )
            })?;

        let index_names: Vec<_> = list_indexes_output
            .indexes()
            .iter()
            .filter_map(|idx| {
                let Ok(partitioned_index_name) =
                    PartitionedIndexName::from_index_name(idx.index_name())
                else {
                    return None;
                };

                if matches!(
                    partitioned_index_name.belongs_with(
                        &index_name,
                        &self.column_name,
                        &self.partition_by
                    ),
                    BelongsWith::ThisDataset
                ) {
                    Some(idx.index_name().to_string())
                } else {
                    tracing::debug!(
                        "S3 index {} returned but does not belong with this dataset: {index_name}",
                        idx.index_name()
                    );
                    None
                }
            })
            .collect();

        if index_names.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.schema(),
                projection,
            )?)));
        }

        let mut index_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for index_name in index_names {
            let index_table_identifier = S3VectorIdentifier::Index {
                bucket_name: bucket_name.clone(),
                index_name,
            };

            let index_table = S3VectorsTable {
                client: Arc::clone(&self.table.client),
                schema: Arc::clone(&self.table.schema),
                constraints: self.table.constraints.clone(),
                idx: index_table_identifier,
                dimension: self.table.dimension,
                columns: self.table.columns.clone(),
                distance_metric: self.table.distance_metric.clone(),
            };

            let list_table = S3VectorsListTable::new(index_table, self.column_name.clone(), vec![]);

            let index_plan = list_table.scan(state, projection, filters, limit).await?;
            index_plans.push(index_plan);
        }

        let union_plan = match index_plans.len() {
            0 => {
                return Ok(Arc::new(EmptyExec::new(project_schema(
                    &self.schema(),
                    projection,
                )?)));
            }
            1 => return Ok(Arc::clone(&index_plans[0])),
            _ => Arc::new(UnionExec::new(index_plans)),
        };

        let limit_plan = Arc::new(GlobalLimitExec::new(union_plan, 0, limit));

        Ok(limit_plan)
    }
}

impl std::fmt::Debug for S3VectorsListExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorsListExec").finish_non_exhaustive()
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
            idx: table.table.idx.clone(),
            client: Arc::clone(&table.table.client),
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
    let (json_schema, vector_sizes) = loosen_vector_schema(&schema);
    let mut decoder = ReaderBuilder::new(Arc::clone(&json_schema)).build_decoder()?;

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
                Box::new(e),
                Some(
                    "could not convert ListVectors JSON response into expected Arrow format"
                        .to_string(),
                ),
            )
        })?;

        match decoder.flush() {
            Ok(Some(rb)) => send_vector_data(&tx, rb, &vector_sizes).await,
            Ok(None) => {}
            Err(e) => {
                let _ = tx
                    .send(Err(DataFusionError::ArrowError(
                        Box::new(e),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::s3_vectors::MetadataColumns;

    use super::*;

    use arrow::datatypes::{DataType, Field};
    use datafusion::{logical_expr::col, prelude::SessionContext, scalar::ScalarValue};
    use s3_vectors::{
        DistanceMetric, IndexSummary,
        mock::{DateTime, MockClient},
    };

    #[tokio::test]
    async fn scan_plan_with_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test_bucket";
        let index_name_prefix = "test_index";
        let column_name = "my_col";

        let partition_by = &[col(column_name)];

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create 2 partitions
        for i in 0..2 {
            let partition_value = ScalarValue::Int32(Some(i));
            let index_name = PartitionedIndexName::new(
                index_name_prefix,
                column_name,
                partition_by,
                &partition_value,
            )?
            .to_index_name();
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(index_name.clone())
                    .build()?,
            );
            vectors_map.insert(index_name, vec![]);
        }

        // Add an index that shouldn't be included
        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name("another_index")
                .build()?,
        );

        mock_client
            .data
            .lock()
            .expect("lock")
            .indexes
            .insert(bucket_name.to_string(), indexes);

        for (index, vectors) in vectors_map {
            mock_client
                .data
                .lock()
                .expect("lock")
                .vectors
                .insert(index, vectors);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(column_name, DataType::Utf8, true),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name_prefix.to_string(),
            },
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let list_table =
            S3VectorsListTable::new(s3_table, column_name.to_string(), vec![col(column_name)]);

        let session_state = SessionContext::new().state();
        let plan = list_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        // The plan should be a GlobalLimitExec -> UnionExec
        let limit_plan = plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .expect("downcast");
        let union_plan = limit_plan
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .expect("downcast");

        // There should be 2 partitions, so 2 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 2);

        Ok(())
    }
}
