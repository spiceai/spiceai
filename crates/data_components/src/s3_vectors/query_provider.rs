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
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    vector_table::{S3VectorsTable, loosen_vector_schema, send_vector_data},
};

use super::{Error, S3VectorIdentifier, compute_query::ComputeQueryVector};
use arrow::{array::RecordBatch, datatypes::SchemaRef, json::ReaderBuilder};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    common::{Constraints, HashMap, project_schema},
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
use s3_vectors::{
    Document, GetVectorsInput, GetVectorsOutput, QueryOutputVector, QueryVectorsInput, S3Vectors,
    SdkError, VectorData,
};
use s3_vectors_metadata_filter::{convert_datafusion_filters_to_s3_vectors, document_to_json_map};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tracing::{Instrument, info_span};

/// The JSON key within a `QueryVector` response that contains the distance to the query vector.
pub static S3_VECTOR_DISTANCE_NAME: &str = "distance";

/// Maximum topK results retrievable by a `QueryVector` operation. <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html>
pub static S3_VECTOR_MAX_TOPK: i32 = 100;

/// Maximum number of keys per `GetVectors` API call. <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html>
pub static GET_VECTORS_MAX_KEYS: usize = 100;

/// An S3 Vector index that implements [`TableProvider`] as a `QueryVector` API operation for a given query vector.
#[derive(Debug)]
pub struct S3VectorsQueryTable {
    table: S3VectorsTable,
    compute_vector: Arc<dyn ComputeQueryVector>,
    query: String,
}

impl S3VectorsQueryTable {
    #[must_use]
    pub fn new(
        table: S3VectorsTable,
        compute_vector: Arc<dyn ComputeQueryVector>,
        query: String,
    ) -> Self {
        Self {
            table,
            compute_vector,
            query,
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.query_provider_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table.constraints)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(self.table.query_provider_supports_filters_pushdown(filters))
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let query_vector = self
            .compute_vector
            .compute_vector(self.query.as_str())
            .await
            .map_err(DataFusionError::External)?;

        let limit_i32: i32 = match limit.map(i32::try_from) {
            Some(Ok(l)) if l > S3_VECTOR_MAX_TOPK => {
                tracing::warn!(
                    "S3VectorsQueryTable: limit {l} exceeds maximum of {S3_VECTOR_MAX_TOPK}, truncating."
                );
                S3_VECTOR_MAX_TOPK
            }
            Some(Ok(l)) => l,
            // No limit, or failed conversion
            None | Some(Err(_)) => S3_VECTOR_MAX_TOPK,
        };
        return Ok(Arc::new(S3VectorsQueryExec::new(
            &self.table,
            projection,
            i64::from(limit_i32),
            query_vector,
            filters.to_vec(),
        )));
    }
}

impl std::fmt::Debug for S3VectorsQueryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorsQueryExec").finish_non_exhaustive()
    }
}

pub(super) struct S3VectorsQueryExec {
    idx: S3VectorIdentifier,
    client: Arc<dyn S3Vectors + Send + Sync>,
    plan_properties: PlanProperties,
    query: Vec<f32>,
    limit: i32,
    filters: Vec<Expr>,
}

impl DisplayAs for S3VectorsQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "S3VectorsQueryExec ({}): ", self.idx)?;
        if let Ok(Some(filter)) = convert_datafusion_filters_to_s3_vectors(&self.filters) {
            write!(f, "filter={filter} ")?;
        }
        write!(f, "limit={}", self.limit)
    }
}

impl S3VectorsQueryExec {
    pub fn new(
        table: &S3VectorsTable,
        projection: Option<&Vec<usize>>,
        limit: i64,
        query: Vec<f32>,
        filters: Vec<Expr>,
    ) -> Self {
        let schema = table.query_provider_schema();
        let projected_schema =
            project_schema(&schema, projection).unwrap_or_else(|_| Arc::clone(&schema));

        Self {
            idx: Arc::unwrap_or_clone(Arc::clone(&table.idx)),
            client: Arc::clone(&table.client),
            plan_properties: PlanProperties::new(
                EquivalenceProperties::new(projected_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            query,
            limit: i32::try_from(limit).unwrap_or(i32::MAX),
            filters,
        }
    }
}

impl ExecutionPlan for S3VectorsQueryExec {
    fn name(&self) -> &'static str {
        "S3VectorsQueryExec"
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
        let limit = self.limit;
        let q = self.query.clone();
        let filters = self.filters.clone();

        builder.spawn(async move {
            if let Err(e) = query_vector_stream(
                client,
                idx,
                q,
                Arc::clone(&schema),
                limit,
                filters,
                tx.clone(),
            )
            .await
            {
                let _ = tx.send(Err(e)).await;
            }
            Ok(())
        });

        Ok(builder.build())
    }
}

/// Wraps a single [`S3Vectors::get_vectors`] for better input, output and error handling.
/// Batches requests if there are more than `GET_VECTORS_MAX_KEYS` keys.
async fn get_vectors_call(
    client: Arc<dyn S3Vectors + Send + Sync>,
    idx: &S3VectorIdentifier,
    keys: Vec<String>,
) -> DataFusionResult<HashMap<String, VectorData>> {
    let (arn, bucket_name, index_name) = idx.index_identifier_variables();
    let mut result = HashMap::new();

    // Batch keys into chunks of GET_VECTORS_MAX_KEYS
    for chunk in keys.chunks(GET_VECTORS_MAX_KEYS) {
        let GetVectorsOutput {
            vectors: output_vectors,
            ..
        } = client
            .get_vectors(
                GetVectorsInput::builder()
                    .set_keys(Some(chunk.to_vec()))
                    .set_vector_bucket_name(bucket_name.clone())
                    .set_index_arn(arn.clone())
                    .set_index_name(index_name.clone())
                    .set_return_data(Some(true))
                    .build()
                    .boxed()
                    .map_err(DataFusionError::External)?,
            )
            .instrument(info_span!(
                target: "task_history",
                "s3_get_vectors",
                bucket_name = bucket_name,
                index_name = index_name,
                arn = arn,
                keys_count = chunk.len(),
            ))
            .await
            .map_err(|e| {
                DataFusionError::External(
                    Error::S3VectorGetVectorsError {
                        source: Box::new(e.into_service_error()),
                    }
                    .into(),
                )
            })?;

        result.extend(
            output_vectors
                .into_iter()
                .filter_map(|v| Some((v.key, v.data?))),
        );
    }

    Ok(result)
}

/// Wraps a single [`S3Vectors::query_vectors`] for better input, output and error handling.
async fn query_vectors_call(
    client: Arc<dyn S3Vectors + Send + Sync>,
    idx: &S3VectorIdentifier,
    query: Vec<f32>,
    filters: Vec<Expr>,
    limit: i32,
) -> DataFusionResult<Vec<QueryOutputVector>> {
    let (arn, bucket_name, index_name) = idx.index_identifier_variables();
    let s3_filter_pre = convert_datafusion_filters_to_s3_vectors(&filters)?;
    let s3_filter: Option<Document> = s3_filter_pre.clone().map(Into::into);
    let output = client
        .query_vectors(
            QueryVectorsInput::builder()
                .query_vector(VectorData::Float32(query))
                .return_distance(true)
                .top_k(limit)
                .set_filter(s3_filter.clone())
                .set_vector_bucket_name(bucket_name.clone())
                .set_index_arn(arn.clone())
                .set_index_name(index_name.clone())
                .return_metadata(true)
                .build()
                .boxed()
                .map_err(DataFusionError::External)?,
        )
        .instrument(info_span!(
            target: "task_history",
            "s3_query_vectors",
            bucket_name = bucket_name,
            index_name = index_name,
            arn = arn,
            top_k = limit
        ))
        .await
        .map_err(|e| {
            if let SdkError::ServiceError(service_error) = &e
                && let s3_vectors::QueryVectorsError::ValidationException(validation_exception) =
                    service_error.err()
                && validation_exception
                    .message()
                    .contains("Invalid query filter")
                && let (Some(s3_filter), Some(s3_filter_pre)) = (s3_filter, s3_filter_pre)
            {
                return DataFusionError::External(
                    Error::S3VectorQueryVectorsInvalidFilterError {
                        filter_pre: s3_filter_pre,
                        filter: s3_filter,
                    }
                    .into(),
                );
            }

            DataFusionError::External(
                Error::S3VectorQueryVectorsError {
                    source: Box::new(e.into_service_error()),
                }
                .into(),
            )
        })?;

    Ok(output.vectors)
}

async fn query_vector_stream(
    client: Arc<dyn S3Vectors + Send + Sync>,
    idx: S3VectorIdentifier,
    query: Vec<f32>,
    schema: SchemaRef,
    limit: i32,
    filters: Vec<Expr>,
    tx: Sender<DataFusionResult<RecordBatch, DataFusionError>>,
) -> DataFusionResult<()> {
    let start = std::time::Instant::now();

    let (arn, bucket_name, index_name) = idx.index_identifier_variables();
    let (json_schema, vector_sizes) = loosen_vector_schema(&schema);
    let mut decoder = ReaderBuilder::new(Arc::clone(&json_schema)).build_decoder()?;

    let combined_span = info_span!(
        target: "task_history",
        "s3_vector_query_and_get",
        bucket_name = bucket_name,
        index_name = index_name,
        arn = arn,
        top_k = limit
    );

    let query_vectors = query_vectors_call(Arc::clone(&client), &idx, query, filters, limit)
        .instrument(combined_span.clone())
        .await?;

    // Only fetch vector data if the embeddings column is in the projection.
    // Check if "data" column is present in the schema.
    let vector_data = if schema.column_with_name(S3_VECTOR_EMBEDDING_NAME).is_some() {
        let vector_data = get_vectors_call(
            Arc::clone(&client),
            &idx,
            query_vectors.iter().map(|v| v.key.clone()).collect(),
        )
        .instrument(combined_span.clone())
        .await?;

        let missing_keys: Vec<_> = query_vectors
            .iter()
            .filter(|v| !vector_data.contains_key(&v.key))
            .map(|v| &v.key)
            .collect();

        // Warn if GetVectors returned incomplete data
        if !missing_keys.is_empty() {
            tracing::warn!(
                "GetVectors returned incomplete data for {} keys: {:?}",
                missing_keys.len(),
                missing_keys
            );
        }
        Some(vector_data)
    } else {
        None
    };

    let rows: Vec<_> = query_vectors
        .into_iter()
        .map(|v| {
            let data = vector_data.as_ref().and_then(|vd| vd.get(&v.key).cloned());
            to_flat_value(v, data)
        })
        .collect();
    decoder.serialize(rows.as_slice()).map_err(|e| {
        DataFusionError::ArrowError(
            Box::new(e),
            Some(
                "could not convert QueryVectors JSON response into expected Arrow format"
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
                    Some("Received only partial JSON payload from QueryVectors".to_string()),
                )))
                .await;
        }
    }
    let duration = start.elapsed();
    tracing::trace!(
        "S3 Vectors Query retrieved {} vectors in {duration:?}",
        rows.len()
    );
    Ok(())
}

/// Converts a `QueryOutputVector` into a flat JSON value (i.e unnest metadata fields).
fn to_flat_value(output: QueryOutputVector, data: Option<VectorData>) -> serde_json::Value {
    let QueryOutputVector {
        metadata,
        key,
        distance,
        ..
    } = output;
    let mut result = document_to_json_map(metadata.unwrap_or_default()).unwrap_or_default();
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

    if let Some(distance) = distance
        && let Some(d) = serde_json::Number::from_f64(f64::from(distance))
    {
        result.insert(
            S3_VECTOR_DISTANCE_NAME.to_string(),
            serde_json::Value::Number(d),
        );
    }

    serde_json::Value::Object(result)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::s3_vectors::MetadataColumns;
    use crate::s3_vectors::spill::query_provider::S3VectorsSpillQueryTable;
    use s3_vectors::QueryOutputVector;

    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::prelude::SessionContext;
    use s3_vectors::{DateTime, DistanceMetric, IndexSummary, mock::MockClient};

    #[derive(Debug)]
    struct MockComputeVector {
        vector: Vec<f32>,
    }

    impl MockComputeVector {
        fn new(vector: Vec<f32>) -> Self {
            Self { vector }
        }
    }

    #[async_trait]
    impl ComputeQueryVector for MockComputeVector {
        async fn compute_vector(
            &self,
            _query: &str,
        ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.vector.clone())
        }
    }

    #[tokio::test]
    async fn scan_plan_with_index_spilling() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let virtual_index_name = "virtual-index";

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create main virtual index
        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name(virtual_index_name.to_string())
                .build()?,
        );
        vectors_map.insert(virtual_index_name.to_string(), vec![]);

        // Create 2 spill indexes
        for i in 1..=2 {
            let spill_index_name = format!("{virtual_index_name}-{i:02}");
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(spill_index_name.clone())
                    .build()?,
            );
            vectors_map.insert(spill_index_name, vec![]);
        }

        // Add an unrelated index that shouldn't be included
        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name("another-index")
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
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema: Arc::clone(&schema),
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: virtual_index_name.to_string(),
            }),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsSpillQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();
        let plan = query_table
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

        // There should be 3 indexes (main + 2 spills), so 3 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn scan_plan_with_index_spilling_from_spill_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let virtual_index_name = "virtual-index";

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create main virtual index
        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name(virtual_index_name.to_string())
                .build()?,
        );
        vectors_map.insert(virtual_index_name.to_string(), vec![]);

        // Create 2 spill indexes
        for i in 1..=2 {
            let spill_index_name = format!("{virtual_index_name}-{i:02}");
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(spill_index_name.clone())
                    .build()?,
            );
            vectors_map.insert(spill_index_name, vec![]);
        }

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
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        // Test accessing index through a spill index name
        let s3_table = S3VectorsTable {
            client: mock_client,
            schema: Arc::clone(&schema),
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: "virtual-index-01".to_string(), // Access via spill name
            }),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsSpillQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();
        let plan = query_table
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

        // There should be 3 indexes (main + 2 spills), so 3 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn scan_plan_single_index() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let index_name = "single-index";

        let indexes = vec![
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name(index_name.to_string())
                .build()?,
        ];

        mock_client
            .data
            .lock()
            .expect("lock")
            .indexes
            .insert(bucket_name.to_string(), indexes);

        mock_client
            .data
            .lock()
            .expect("lock")
            .vectors
            .insert(index_name.to_string(), vec![]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name.to_string(),
            }),
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();
        let plan = query_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        // The plan should be S3VectorsQueryExec directly
        assert!(plan.as_any().downcast_ref::<S3VectorsQueryExec>().is_some());

        Ok(())
    }

    #[test]
    fn test_s3_vector_max_topk_value() {
        // Verify the constant is set to 100 as per updated S3 Vectors API limits
        assert_eq!(S3_VECTOR_MAX_TOPK, 100);
    }

    #[test]
    fn test_get_vectors_max_keys_value() {
        // Verify the constant is set to 100 as per S3 Vectors API limits
        assert_eq!(GET_VECTORS_MAX_KEYS, 100);
    }

    #[tokio::test]
    async fn test_s3_vectors_query_exec_limit_clamped() {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let index_name = "test-index";

        mock_client.data.lock().expect("lock").indexes.insert(
            bucket_name.to_string(),
            vec![
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(index_name.to_string())
                    .build()
                    .expect("build"),
            ],
        );

        mock_client
            .data
            .lock()
            .expect("lock")
            .vectors
            .insert(index_name.to_string(), vec![]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name.to_string(),
            }),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();

        // Test with limit exceeding S3_VECTOR_MAX_TOPK (100)
        // The scan should clamp the limit to S3_VECTOR_MAX_TOPK
        let plan = query_table
            .scan(&session_state, None, &[], Some(200))
            .await
            .expect("scan should succeed");

        // The plan should be S3VectorsQueryExec with clamped limit
        let exec = plan
            .as_any()
            .downcast_ref::<S3VectorsQueryExec>()
            .expect("should be S3VectorsQueryExec");

        // The limit should be clamped to S3_VECTOR_MAX_TOPK (100)
        assert_eq!(exec.limit, S3_VECTOR_MAX_TOPK);
    }

    #[tokio::test]
    async fn test_s3_vectors_query_exec_limit_within_bounds() {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let index_name = "test-index";

        mock_client.data.lock().expect("lock").indexes.insert(
            bucket_name.to_string(),
            vec![
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(index_name.to_string())
                    .build()
                    .expect("build"),
            ],
        );

        mock_client
            .data
            .lock()
            .expect("lock")
            .vectors
            .insert(index_name.to_string(), vec![]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name.to_string(),
            }),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();

        // Test with limit within bounds
        let plan = query_table
            .scan(&session_state, None, &[], Some(50))
            .await
            .expect("scan should succeed");

        // The plan should be S3VectorsQueryExec with the original limit
        let exec = plan
            .as_any()
            .downcast_ref::<S3VectorsQueryExec>()
            .expect("should be S3VectorsQueryExec");

        // The limit should remain 50
        assert_eq!(exec.limit, 50);
    }

    #[tokio::test]
    async fn test_s3_vectors_query_exec_no_limit_uses_default() {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let index_name = "test-index";

        mock_client.data.lock().expect("lock").indexes.insert(
            bucket_name.to_string(),
            vec![
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(index_name.to_string())
                    .build()
                    .expect("build"),
            ],
        );

        mock_client
            .data
            .lock()
            .expect("lock")
            .vectors
            .insert(index_name.to_string(), vec![]);

        let schema = Arc::new(Schema::new(vec![
            Field::new(S3_VECTOR_PRIMARY_KEY_NAME, DataType::Utf8, false),
            Field::new(
                S3_VECTOR_EMBEDDING_NAME,
                DataType::new_list(DataType::Float32, true),
                false,
            ),
            Field::new(S3_VECTOR_DISTANCE_NAME, DataType::Float64, false),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: index_name.to_string(),
            }),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table =
            S3VectorsQueryTable::new(s3_table, compute_vector, "test query".to_string());

        let session_state = SessionContext::new().state();

        // Test with no limit - should use S3_VECTOR_MAX_TOPK as default
        let plan = query_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan should succeed");

        // The plan should be S3VectorsQueryExec with the default limit
        let exec = plan
            .as_any()
            .downcast_ref::<S3VectorsQueryExec>()
            .expect("should be S3VectorsQueryExec");

        // The limit should be S3_VECTOR_MAX_TOPK (100)
        assert_eq!(exec.limit, S3_VECTOR_MAX_TOPK);
    }

    #[test]
    fn test_to_flat_value_with_vector_data() {
        // Test that to_flat_value correctly includes vector data when provided
        let query_output = QueryOutputVector::builder()
            .key("test-key".to_string())
            .distance(0.5_f32)
            .build()
            .expect("build");

        let vector_data = VectorData::Float32(vec![1.0, 2.0, 3.0]);
        let result = super::to_flat_value(query_output, Some(vector_data));

        let obj = result.as_object().expect("should be object");

        // Verify key is present
        assert_eq!(
            obj.get(S3_VECTOR_PRIMARY_KEY_NAME),
            Some(&serde_json::Value::String("test-key".to_string()))
        );

        // Verify distance is present
        let distance = obj.get(S3_VECTOR_DISTANCE_NAME).expect("distance");
        assert_eq!(distance.as_f64(), Some(0.5));

        // Verify vector data is present
        let embedding = obj
            .get(S3_VECTOR_EMBEDDING_NAME)
            .expect("embedding should be present");
        let arr = embedding.as_array().expect("should be array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_f64(), Some(1.0));
        assert_eq!(arr[1].as_f64(), Some(2.0));
        assert_eq!(arr[2].as_f64(), Some(3.0));
    }

    #[test]
    fn test_to_flat_value_without_vector_data() {
        // Test that to_flat_value correctly excludes vector data when not provided
        let query_output = QueryOutputVector::builder()
            .key("test-key".to_string())
            .distance(0.75_f32)
            .build()
            .expect("build");

        let result = super::to_flat_value(query_output, None);

        let obj = result.as_object().expect("should be object");

        // Verify key is present
        assert_eq!(
            obj.get(S3_VECTOR_PRIMARY_KEY_NAME),
            Some(&serde_json::Value::String("test-key".to_string()))
        );

        // Verify distance is present
        let distance = obj.get(S3_VECTOR_DISTANCE_NAME).expect("distance");
        assert_eq!(distance.as_f64(), Some(0.75));

        // Verify vector data is NOT present
        assert!(
            obj.get(S3_VECTOR_EMBEDDING_NAME).is_none(),
            "embedding should not be present when data is None"
        );
    }

    #[test]
    fn test_to_flat_value_with_metadata() {
        use aws_smithy_types::{Document, Number};

        // Test that metadata fields are correctly flattened
        let metadata = Document::Object(
            vec![
                ("field1".to_string(), Document::String("value1".to_string())),
                ("field2".to_string(), Document::Number(Number::Float(42.0))),
            ]
            .into_iter()
            .collect(),
        );

        let query_output = QueryOutputVector::builder()
            .key("meta-key".to_string())
            .distance(0.1_f32)
            .metadata(metadata)
            .build()
            .expect("build");

        let result = super::to_flat_value(query_output, None);

        let obj = result.as_object().expect("should be object");

        // Verify key is present
        assert_eq!(
            obj.get(S3_VECTOR_PRIMARY_KEY_NAME),
            Some(&serde_json::Value::String("meta-key".to_string()))
        );

        // Verify metadata fields are flattened
        assert_eq!(
            obj.get("field1"),
            Some(&serde_json::Value::String("value1".to_string()))
        );
        assert_eq!(
            obj.get("field2").and_then(serde_json::Value::as_f64),
            Some(42.0)
        );
    }
}
