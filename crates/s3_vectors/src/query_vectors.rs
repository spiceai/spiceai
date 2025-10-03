/*
Copyright 2025 The Spice.ai OSS Authors

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

use std::{any::Any, fmt, sync::Arc};

use aws_sdk_s3vectors::{
    operation::query_vectors::QueryVectorsInput,
    types::{Index, VectorData},
};
use datafusion::{
    arrow::{
        array::{Float32Builder, ListBuilder, RecordBatch, StringBuilder},
        datatypes::SchemaRef,
    },
    common::{Result, Statistics},
    datasource::source::DataSource,
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use futures::stream;

use crate::S3Vectors;

static NAME: &str = "QueryVectorsSource";

pub struct QueryVectorsSource {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
    query_vector: VectorData,
    top_k: i32,
    partitioning: Partitioning,
    eq_properties: EquivalenceProperties,
}

impl fmt::Debug for QueryVectorsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl QueryVectorsSource {
    pub fn new(
        client: Arc<dyn S3Vectors + Send + Sync>,
        index: Index,
        schema: SchemaRef,
        query_vector: VectorData,
        top_k: i32,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let partitioning = Partitioning::UnknownPartitioning(1);

        Self {
            client,
            index,
            schema,
            query_vector,
            top_k,
            partitioning,
            eq_properties,
        }
    }
}

impl DisplayAs for QueryVectorsSource {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl DataSource for QueryVectorsSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn open(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let schema = Arc::clone(&self.schema);
        let query_vector = self.query_vector.clone();
        let top_k = self.top_k;

        let schema_ = Arc::clone(&schema);
        let stream = stream::once(async move {
            let input = QueryVectorsInput::builder()
                .vector_bucket_name(index.vector_bucket_name())
                .index_name(index.index_name())
                .query_vector(query_vector)
                .top_k(top_k)
                .return_data(true)
                .return_distance(true)
                .build()
                .map_err(|e| DataFusionError::External(e.into()))?;

            let output = client
                .query_vectors(input)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;

            let vectors = output.vectors;

            if vectors.is_empty() {
                return Ok(RecordBatch::new_empty(schema_));
            }

            let mut key_builder = StringBuilder::new();
            let mut data_builder = ListBuilder::new(Float32Builder::new());
            let mut distance_builder = Float32Builder::new();
            // TODO: (function) add metadata
            // TODO: (optimization) if we know/store the vector dimension we can make a fixed size list

            for vector in vectors {
                key_builder.append_value(vector.key);
                if let Some(VectorData::Float32(data)) = vector.data {
                    data_builder.values().append_slice(&data);
                    data_builder.append(true);
                } else {
                    data_builder.append(false);
                }
                if let Some(distance) = vector.distance {
                    distance_builder.append_value(distance);
                } else {
                    distance_builder.append_null();
                }
            }

            let batch = RecordBatch::try_new(
                schema_,
                vec![
                    Arc::new(key_builder.finish()),
                    Arc::new(data_builder.finish()),
                    Arc::new(distance_builder.finish()),
                ],
            )?;

            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.eq_properties.clone()
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_client::MockClient;
    use aws_sdk_s3vectors::types::{DistanceMetric, Index, ListOutputVector, VectorData};
    use aws_smithy_types::DateTime;
    use datafusion::{
        arrow::{
            array::{Float32Array, ListArray, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        catalog::memory::DataSourceExec,
        common::Result,
        execution::TaskContext,
        physical_plan::collect,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn query_vectors_source() -> Result<()> {
        let mock_client = Arc::new(MockClient::new());
        let index_name = "test_index";
        let index_arn = "test_arn";
        let bucket_name = "test_bucket";

        let query_vector = VectorData::Float32(vec![1.0, 2.0, 3.0]);
        let top_k = 10;

        {
            let mut data = mock_client.data.lock().expect("lock");
            let vectors = vec![
                ListOutputVector::builder()
                    .key("v1")
                    .data(VectorData::Float32(vec![1.0, 2.0, 3.0]))
                    .build()
                    .expect("build"),
                ListOutputVector::builder()
                    .key("v2")
                    .data(VectorData::Float32(vec![4.0, 5.0, 6.0]))
                    .build()
                    .expect("build"),
            ];
            data.vectors.insert(index_name.to_string(), vectors);
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
            Field::new("distance", DataType::Float32, true),
        ]));

        let index = Index::builder()
            .index_name(index_name)
            .vector_bucket_name(bucket_name)
            .index_arn(index_arn)
            .creation_time(DateTime::from_secs(7))
            .data_type(aws_sdk_s3vectors::types::DataType::Float32)
            .dimension(3)
            .distance_metric(DistanceMetric::Cosine)
            .build()
            .expect("valid index");

        let source = Arc::new(QueryVectorsSource::new(
            Arc::clone(&mock_client) as Arc<dyn S3Vectors + Send + Sync>,
            index,
            Arc::clone(&schema),
            query_vector,
            top_k,
        ));

        let plan = Arc::new(DataSourceExec::new(source));

        let context = Arc::new(TaskContext::default());
        let batches = collect(plan, context).await?;

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(key_col.value(0), "v1");
        assert_eq!(key_col.value(1), "v2");

        let data_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        let data_v1 = data_col
            .value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array")
            .values()
            .to_vec();
        assert_eq!(data_v1, vec![1.0, 2.0, 3.0]);
        let data_v2 = data_col
            .value(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array")
            .values()
            .to_vec();
        assert_eq!(data_v2, vec![4.0, 5.0, 6.0]);

        let distance_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("Float32Array");
        assert_eq!(distance_col.value(0), 0.5);
        assert_eq!(distance_col.value(1), 0.5);

        Ok(())
    }
}
