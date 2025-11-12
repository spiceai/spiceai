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
use std::{
    any::Any,
    sync::{Arc, atomic::AtomicU8},
};

use crate::s3_vectors::{
    S3_VECTOR_EMBEDDING_NAME, S3_VECTOR_PRIMARY_KEY_NAME,
    partition::{BelongsWith, PartitionedIndexName},
    vector_table::{S3VectorsTable, loosen_vector_schema, send_vector_data},
};

use super::{Error, S3VectorIdentifier, SpillIndex};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
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
use s3_vectors::{
    Document, GetVectorsInput, GetVectorsOutput, QueryOutputVector, QueryVectorsInput,
    QueryVectorsOutput, S3Vectors, SdkError, VectorData,
};
use s3_vectors_metadata_filter::{convert_datafusion_filters_to_s3_vectors, document_to_json_map};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tracing::{Instrument, info_span};

/// The JSON key within a `QueryVector` response that contains the distance to the query vector.
pub static S3_VECTOR_DISTANCE_NAME: &str = "distance";

/// Maximum topK results retrievable by a `QueryVector` operation. // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html>
pub static S3_VECTOR_MAX_TOPK: i32 = 30;

/// [`ComputeQueryVector`] allows [`S3VectorsQueryTable`] to be instantiated in a non-async setting.
#[async_trait]
pub trait ComputeQueryVector: std::fmt::Debug + Send + Sync {
    async fn compute_vector(
        &self,
        query: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + Send + Sync>>;
}

/// An S3 Vector index that implements [`TableProvider`] as a `QueryVector` API operation for a given query vector.
#[derive(Debug)]
pub struct S3VectorsQueryTable {
    table: S3VectorsTable,
    compute_vector: Arc<dyn ComputeQueryVector>,
    query: String,
    column_name: String,
    partition_by: Vec<Expr>,
}

#[allow(clippy::too_many_arguments)]
fn create_spill_plan_query(
    client: &Arc<dyn S3Vectors + Send + Sync>,
    bucket_name: &str,
    index_name: &str,
    table: &S3VectorsQueryTable,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    query_vector: &[f32],
    all_index_names: &[String],
) -> Option<Arc<dyn ExecutionPlan>> {
    let virtual_index_names =
        SpillIndex::get_all_indexes_for_virtual_index(index_name, all_index_names);

    if virtual_index_names.len() > 1 {
        let mut index_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
        for spill_index_name in virtual_index_names {
            let index_table_identifier = S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: spill_index_name.clone(),
            };

            let index_table = S3VectorsTable {
                client: Arc::clone(client),
                schema: Arc::clone(&table.table.schema),
                constraints: table.table.constraints.clone(),
                idx: Arc::new(index_table_identifier),
                spill_index: Arc::new(AtomicU8::new(0)),
                dimension: table.table.dimension,
                columns: table.table.columns.clone(),
                distance_metric: table.table.distance_metric.clone(),
            };

            let query_table = S3VectorsQueryTable::new(
                index_table,
                Arc::clone(&table.compute_vector),
                table.query.clone(),
                table.column_name.clone(),
                vec![],
            );

            let limit_i32: i32 = match limit {
                Some(l) => {
                    // Safe conversion: check against i32::MAX first, then compare with limit
                    let l_i32 = i32::try_from(l).unwrap_or(i32::MAX);
                    if l_i32 > S3_VECTOR_MAX_TOPK {
                        tracing::warn!(
                            "S3VectorsQueryTable: limit {l} exceeds maximum of {S3_VECTOR_MAX_TOPK}, truncating."
                        );
                        S3_VECTOR_MAX_TOPK
                    } else {
                        l_i32
                    }
                }
                None => S3_VECTOR_MAX_TOPK,
            };
            let index_plan = Arc::new(S3VectorsQueryExec::new(
                &query_table,
                projection,
                i64::from(limit_i32),
                query_vector.to_owned(),
                filters.to_vec(),
            ));
            index_plans.push(index_plan);
        }

        let union_plan = Arc::new(UnionExec::new(index_plans));
        let limit_plan = Arc::new(GlobalLimitExec::new(union_plan, 0, limit));

        Some(limit_plan)
    } else {
        None
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_partition_plan_query(
    client: &Arc<dyn S3Vectors + Send + Sync>,
    bucket_name: &str,
    index_name: &str,
    table: &S3VectorsQueryTable,
    projection: Option<&Vec<usize>>,
    filters: &[Expr],
    limit: Option<usize>,
    state: &dyn Session,
    all_index_names: &[String],
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let index_names: Vec<_> = all_index_names
        .iter()
        .filter_map(|idx_name| {
            let Ok(partitioned_index_name) =
                PartitionedIndexName::from_index_name(idx_name)
            else {
                return None;
            };

            if matches!(
                partitioned_index_name.belongs_with(
                    index_name,
                    &table.column_name,
                    &table.partition_by
                ),
                BelongsWith::ThisDataset
            ) {
                Some(idx_name.clone())
            } else {
                tracing::debug!(
                    "S3 index {idx_name} returned but does not belong with this dataset: {index_name}",
                );
                None
            }
        })
        .collect();

    if index_names.is_empty() {
        return Ok(Arc::new(EmptyExec::new(project_schema(
            &table.schema(),
            projection,
        )?)));
    }

    let mut index_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    for index_name in index_names {
        let index_table_identifier = S3VectorIdentifier::Index {
            bucket_name: bucket_name.to_string(),
            index_name,
        };

        let index_table = S3VectorsTable {
            client: Arc::clone(client),
            schema: Arc::clone(&table.table.schema),
            constraints: table.table.constraints.clone(),
            idx: Arc::new(index_table_identifier),
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: table.table.dimension,
            columns: table.table.columns.clone(),
            distance_metric: table.table.distance_metric.clone(),
        };

        let query_table = S3VectorsQueryTable::new(
            index_table,
            Arc::clone(&table.compute_vector),
            table.query.clone(),
            table.column_name.clone(),
            vec![],
        );

        let index_plan = query_table.scan(state, projection, filters, limit).await?;
        index_plans.push(index_plan);
    }

    let union_plan = match index_plans.len() {
        0 => {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &table.schema(),
                projection,
            )?)));
        }
        1 => return Ok(Arc::clone(&index_plans[0])),
        _ => Arc::new(UnionExec::new(index_plans)),
    };

    let limit_plan = Arc::new(GlobalLimitExec::new(union_plan, 0, limit));

    Ok(limit_plan)
}

impl S3VectorsQueryTable {
    #[must_use]
    pub fn new(
        table: S3VectorsTable,
        compute_vector: Arc<dyn ComputeQueryVector>,
        query: String,
        column_name: String,
        partition_by: Vec<Expr>,
    ) -> Self {
        Self {
            table,
            compute_vector,
            query,
            column_name,
            partition_by,
        }
    }
}

#[async_trait]
impl TableProvider for S3VectorsQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let mut base_fields = self
            .table
            .schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();

        base_fields.push(Arc::new(Field::new(
            S3_VECTOR_DISTANCE_NAME,
            DataType::Float64,
            false,
        )));

        Arc::new(Schema::new(base_fields))
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
        // Filters can only possibly be pushed down for columns in underlying metadata (i.e. not derived columns like `S3_VECTOR_DISTANCE_NAME`).
        let columns: Vec<_> = self
            .table
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|c| self.table.is_filterable_column(c.as_str()))
            .collect();

        Ok(filters
            .iter()
            .map(|f| {
                if s3_vectors_metadata_filter::supports_filter_expr(columns.as_slice(), f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let query_vector = self
            .compute_vector
            .compute_vector(self.query.as_str())
            .await
            .map_err(DataFusionError::External)?;

        let current_index = self.table.current_index();
        let (_, bucket_name, index_name) = current_index.index_identifier_variables();

        let all_index_names = super::fetch_all_index_names(
            &self.table.client,
            bucket_name.as_deref(),
            index_name.as_deref(),
        )
        .await?;

        if let (Some(bucket_name), Some(index_name), Some(all_index_names)) =
            (bucket_name, index_name, all_index_names.as_ref())
            && let Some(plan) = create_spill_plan_query(
                &self.table.client,
                &bucket_name,
                &index_name,
                self,
                projection,
                filters,
                limit,
                &query_vector,
                all_index_names,
            )
        {
            return Ok(plan);
        }

        if self.partition_by.is_empty() {
            let limit_i32: i32 = match limit {
                Some(l) => {
                    // Safe conversion: check against i32::MAX first, then compare with limit
                    let l_i32 = i32::try_from(l).unwrap_or(i32::MAX);
                    if l_i32 > S3_VECTOR_MAX_TOPK {
                        tracing::warn!(
                            "S3VectorsQueryTable: limit {l} exceeds maximum of {S3_VECTOR_MAX_TOPK}, truncating."
                        );
                        S3_VECTOR_MAX_TOPK
                    } else {
                        l_i32
                    }
                }
                None => S3_VECTOR_MAX_TOPK,
            };
            return Ok(Arc::new(S3VectorsQueryExec::new(
                self,
                projection,
                i64::from(limit_i32),
                query_vector.clone(),
                filters.to_vec(),
            )));
        }

        let current_index = self.table.current_index();
        let (_, bucket_name, index_name) = current_index.index_identifier_variables();
        let (Some(bucket_name), Some(index_name)) = (bucket_name, index_name) else {
            return exec_err!("No bucket name or index name for bucket query");
        };

        let all_index_names = all_index_names.unwrap_or_default();

        create_partition_plan_query(
            &self.table.client,
            &bucket_name,
            &index_name,
            self,
            projection,
            filters,
            limit,
            state,
            &all_index_names,
        )
        .await
    }
}

impl std::fmt::Debug for S3VectorsQueryExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3VectorsQueryExec").finish_non_exhaustive()
    }
}

struct S3VectorsQueryExec {
    idx: S3VectorIdentifier,
    client: Arc<dyn S3Vectors + Send + Sync>,
    plan_properties: PlanProperties,
    query: Vec<f32>,
    limit: i32,
    filters: Vec<Expr>,
}

impl DisplayAs for S3VectorsQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "S3VectorsQueryExec: ")?;
        if let Ok(Some(filter)) = convert_datafusion_filters_to_s3_vectors(&self.filters) {
            write!(f, "filter={filter} ")?;
        }
        write!(f, "limit={}", self.limit)?;
        Ok(())
    }
}

impl S3VectorsQueryExec {
    pub fn new(
        table: &S3VectorsQueryTable,
        projection: Option<&Vec<usize>>,
        limit: i64,
        query: Vec<f32>,
        filters: Vec<Expr>,
    ) -> Self {
        let projected_schema =
            project_schema(&table.schema(), projection).unwrap_or_else(|_| table.schema());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        let idx = table.table.current_index();

        Self {
            idx,
            client: Arc::clone(&table.table.client),
            plan_properties: properties,
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

#[allow(clippy::too_many_lines)]
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

    let s3_filter_pre = convert_datafusion_filters_to_s3_vectors(&filters)?;
    let s3_filter: Option<Document> = s3_filter_pre.clone().map(Into::into);

    let combined_span = info_span!(
        target: "task_history",
        "s3_vector_query_and_get",
        bucket_name = bucket_name,
        index_name = index_name,
        arn = arn,
        top_k = limit
    );

    let QueryVectorsOutput {
        vectors: mut query_vectors,
        ..
    } = client
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
        .instrument(combined_span.clone())
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

    let num_vectors = query_vectors.len();

    // Only fetch vector data if the embeddings column is in the projection.
    // Check if "data" column is present in the schema.
    let needs_embeddings = schema.column_with_name(S3_VECTOR_EMBEDDING_NAME).is_some();

    if needs_embeddings {
        // Get the vector data for each output using GetVectors API.
        let keys = query_vectors.iter().map(|v| v.key.clone()).collect();
        let GetVectorsOutput {
            vectors: output_vectors,
            ..
        } = client
            .get_vectors(
                GetVectorsInput::builder()
                    .set_keys(Some(keys))
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
            ))
            .instrument(combined_span)
            .await
            .map_err(|e| {
                DataFusionError::External(
                    Error::S3VectorGetVectorsError {
                        source: Box::new(e.into_service_error()),
                    }
                    .into(),
                )
            })?;

        // Put the vector data in the query_vectors
        // Use HashMap for O(n) lookup instead of O(n²) nested loop
        let output_map: std::collections::HashMap<_, _> = output_vectors
            .into_iter()
            .map(|v| (v.key.clone(), v))
            .collect();

        let mut missing_keys = Vec::new();
        for query_vector in &mut query_vectors {
            if let Some(output_vector) = output_map.get(&query_vector.key) {
                query_vector.data.clone_from(&output_vector.data);
            } else {
                missing_keys.push(&query_vector.key);
            }
        }

        // Warn if GetVectors returned incomplete data
        if !missing_keys.is_empty() {
            tracing::warn!(
                "GetVectors returned incomplete data for {} keys: {:?}",
                missing_keys.len(),
                missing_keys
            );
        }
    }

    let rows: Vec<_> = query_vectors.into_iter().map(to_flat_value).collect();
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
    tracing::trace!("S3 Vectors Query retrieved {num_vectors} vectors in {duration:?}");
    Ok(())
}

/// Converts a `QueryOutputVector` into a flat JSON value (i.e unnest metadata fields).
fn to_flat_value(output: QueryOutputVector) -> serde_json::Value {
    let QueryOutputVector {
        metadata,
        data,
        key,
        distance,
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

    use super::*;

    use arrow::datatypes::{DataType, Field};
    use datafusion::{
        prelude::{SessionContext, col},
        scalar::ScalarValue,
    };
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
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table = S3VectorsQueryTable::new(
            s3_table,
            compute_vector,
            "test query".to_string(),
            "test_column".to_string(),
            vec![],
        );

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
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: 3,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table = S3VectorsQueryTable::new(
            s3_table,
            compute_vector,
            "test query".to_string(),
            "test_column".to_string(),
            vec![],
        );

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
    #[allow(clippy::too_many_lines)]
    async fn scan_plan_with_partitioned_index_spilling() -> Result<(), Box<dyn std::error::Error>> {
        let mock_client = Arc::new(MockClient::new());
        let bucket_name = "test-bucket";
        let base_index_name = "base-index";
        let column_name = "my-col";

        let partition_by = &[col(column_name)];

        let mut indexes = vec![];
        let mut vectors_map = HashMap::new();

        // Create 2 partitions, each with spilling
        for i in 1..=2 {
            let partition_value = ScalarValue::Int32(Some(i));
            let partition_index_name = PartitionedIndexName::new(
                base_index_name,
                column_name,
                partition_by,
                &partition_value,
            )?
            .to_index_name();

            // Main partition index
            indexes.push(
                IndexSummary::builder()
                    .vector_bucket_name(bucket_name)
                    .set_index_arn(Some("arn".to_string()))
                    .creation_time(DateTime::from_secs(1))
                    .index_name(partition_index_name.clone())
                    .build()?,
            );
            vectors_map.insert(partition_index_name.clone(), vec![]);

            // Spill indexes for this partition
            for j in 1..=2 {
                let spill_index_name = format!("{partition_index_name}.{j:02}");
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
        }

        indexes.push(
            IndexSummary::builder()
                .vector_bucket_name(bucket_name)
                .set_index_arn(Some("arn".to_string()))
                .creation_time(DateTime::from_secs(1))
                .index_name("another-index")
                .build()?,
        ); // add unrelated index

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
            Field::new(column_name, DataType::Int32, true),
        ]));

        let s3_table = S3VectorsTable {
            client: mock_client,
            schema,
            constraints: Constraints::default(),
            idx: Arc::new(S3VectorIdentifier::Index {
                bucket_name: bucket_name.to_string(),
                index_name: base_index_name.to_string(),
            }),
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table = S3VectorsQueryTable::new(
            s3_table,
            compute_vector,
            "test query".to_string(),
            column_name.to_string(),
            vec![col(column_name)],
        );

        let session_state = SessionContext::new().state();
        let plan = query_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        let limit_plan = plan
            .as_any()
            .downcast_ref::<GlobalLimitExec>()
            .expect("downcast");
        let union_plan = limit_plan
            .input()
            .as_any()
            .downcast_ref::<UnionExec>()
            .expect("downcast");

        // There should be 2 partitions, each with 3 indexes (main + 2 spills), so 2 input plans to the UnionExec
        assert_eq!(union_plan.children().len(), 2);

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
            spill_index: Arc::new(AtomicU8::new(0)),
            dimension: 0,
            columns: MetadataColumns::none(),
            distance_metric: DistanceMetric::Cosine,
        };

        let compute_vector = Arc::new(MockComputeVector::new(vec![1.0, 2.0, 3.0]));
        let query_table = S3VectorsQueryTable::new(
            s3_table,
            compute_vector,
            "test query".to_string(),
            "test_column".to_string(),
            vec![],
        );

        let session_state = SessionContext::new().state();
        let plan = query_table
            .scan(&session_state, None, &[], None)
            .await
            .expect("scan");

        // The plan should be S3VectorsQueryExec directly
        assert!(plan.as_any().downcast_ref::<S3VectorsQueryExec>().is_some());

        Ok(())
    }
}
