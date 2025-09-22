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

use super::{Error, S3VectorIdentifier};
use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, Schema, SchemaRef},
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
use s3_vectors::{
    Document, QueryOutputVector, QueryVectorsInput, QueryVectorsOutput, S3Vectors, SdkError,
    VectorData,
};
use s3_vectors_metadata_filter::{convert_datafusion_filters_to_s3_vectors, document_to_json_map};
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;

/// The JSON key within a `QueryVector` response that contains the distance to the query vector.
pub static S3_VECTOR_DISTANCE_NAME: &str = "distance";

/// Maximum topK results retrievable by a `QueryVector` operation. // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-vectors-limitations.html>
pub static S3_VECTOR_MAX_TOPK: i64 = 30;

/// An S3 Vector index that implements [`TableProvider`] as a `QueryVector` API operation for a given query vector.
#[derive(Debug)]
pub struct S3VectorsQueryTable {
    table: S3VectorsTable,
    query: Vec<f32>,
}
impl S3VectorsQueryTable {
    #[must_use]
    pub fn new(table: S3VectorsTable, query: Vec<f32>) -> Self {
        Self { table, query }
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
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        #[allow(clippy::cast_possible_wrap)]
        let limit: i64 = match limit {
            Some(l) if (l as i64) > S3_VECTOR_MAX_TOPK => {
                tracing::warn!(
                    "S3VectorsQueryTable: limit {l} exceeds maximum of {S3_VECTOR_MAX_TOPK}, truncating."
                );
                S3_VECTOR_MAX_TOPK
            }
            None => S3_VECTOR_MAX_TOPK,
            Some(l) => l as i64,
        };
        Ok(Arc::new(S3VectorsQueryExec::new(
            self,
            projection,
            limit,
            self.query.clone(),
            filters.to_vec(),
        )) as Arc<dyn ExecutionPlan>)
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
            query,
            limit: i32::try_from(limit).unwrap_or(30_i32),
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

    let QueryVectorsOutput { vectors, .. } = client
        .query_vectors(
            QueryVectorsInput::builder()
                .query_vector(VectorData::Float32(query))
                .return_distance(true)
                .top_k(limit)
                .set_filter(s3_filter.clone())
                .set_vector_bucket_name(bucket_name.clone())
                .set_index_arn(arn.clone())
                .set_return_data(Some(true))
                .set_index_name(index_name.clone())
                .return_metadata(true)
                .build()
                .boxed()
                .map_err(DataFusionError::External)?,
        )
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
                    source: e.into_service_error(),
                }
                .into(),
            )
        })?;

    let num_vectors = vectors.len();

    let rows: Vec<_> = vectors.into_iter().map(to_flat_value).collect();
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
