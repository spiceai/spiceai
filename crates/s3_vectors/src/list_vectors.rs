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
    operation::list_vectors::ListVectorsInput,
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
    physical_plan::{DisplayFormatType, Partitioning, stream::RecordBatchStreamAdapter},
};
use futures::stream;

use crate::S3Vectors;

static NAME: &str = "ListVectorsSource";

// We can parallelize ListVectors by using the `segmentCount` and `segmentIndex` parameters.
const DEFAULT_PARTITIONS: usize = 16;

pub struct ListVectorsSource {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
    partitioning: Partitioning,
    eq_properties: EquivalenceProperties,
}

impl fmt::Debug for ListVectorsSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl ListVectorsSource {
    pub fn new(client: Arc<dyn S3Vectors + Send + Sync>, index: Index, schema: SchemaRef) -> Self {
        let eq_properties = EquivalenceProperties::new(Arc::clone(&schema));
        let partitioning = Partitioning::UnknownPartitioning(DEFAULT_PARTITIONS);

        Self {
            client,
            index,
            schema,
            partitioning,
            eq_properties,
        }
    }
}

impl DataSource for ListVectorsSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let schema = Arc::clone(&self.schema);
        let num_partitions = self.output_partitioning().partition_count();

        // The state for the stream is `Option<String>`, which is the `next_token` for pagination.
        // We start with `Some(String::new())` to trigger the first request.
        // The stream ends when the state becomes `None`.
        let stream_state = Some(String::new());

        let schema_ = Arc::clone(&schema);
        let stream = stream::try_unfold(stream_state, move |maybe_token| {
            let client = Arc::clone(&client);
            let index = index.clone();
            let schema = Arc::clone(&schema_);

            async move {
                let token = match maybe_token {
                    Some(t) => t,
                    None => return Ok(None), // End of stream
                };

                let next_token = if token.is_empty() { None } else { Some(token) };

                let input = ListVectorsInput::builder()
                    .vector_bucket_name(index.vector_bucket_name())
                    .index_name(index.index_name())
                    .segment_index(partition as i32)
                    .segment_count(num_partitions as i32)
                    .set_next_token(next_token)
                    .build()
                    .map_err(|e| DataFusionError::External(e.into()))?;

                let output = client
                    .list_vectors(input)
                    .await
                    .map_err(|e| DataFusionError::External(e.into()))?;
                let vectors = output.vectors;
                let next_token = output.next_token;

                if vectors.is_empty() {
                    return Ok(None);
                }

                let mut key_builder = StringBuilder::new();
                let mut data_builder = ListBuilder::new(Float32Builder::new());
                // TODO: (function) add metadata
                // TODO: (optimization) if we know/store the vector dimension we can make a fixed size list

                for vector in vectors {
                    key_builder.append_value(vector.key);
                    if let Some(VectorData::Float32(data)) = vector.data {
                        data_builder.values().append_slice(&data);
                    }
                    data_builder.append(true);
                }

                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(key_builder.finish()),
                        Arc::new(data_builder.finish()),
                    ],
                )?;

                Result::Ok(Some((batch, next_token)))
            }
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

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &datafusion::physical_plan::projection::ProjectionExec,
    ) -> Result<Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>> {
        todo!()
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
    async fn test_list_vectors_data_source() -> Result<()> {
        let mock_client = MockClient::new();
        let index_name = "test_index";
        let index_arn = "test_arn";
        let bucket_name = "test_bucket";

        let vectors = vec![
            ListOutputVector::builder()
                .key("v1")
                .data(VectorData::Float32(vec![1.0, 2.0, 3.0]))
                .build()
                .expect("valid vector"),
            ListOutputVector::builder()
                .key("v2")
                .data(VectorData::Float32(vec![4.0, 5.0, 6.0]))
                .build()
                .expect("valid vector"),
        ];

        {
            let mut data = mock_client.data.lock().expect("lock");
            data.vectors.insert(index_name.to_string(), vectors.clone());
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "data",
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                true,
            ),
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

        let source = Arc::new(ListVectorsSource::new(
            Arc::new(mock_client),
            index,
            Arc::clone(&schema),
        ));

        let plan = Arc::new(DataSourceExec::new(source));

        let context = Arc::new(TaskContext::default());
        let batches = collect(plan, context).await?;

        // The mock client doesn't partition, so we get all vectors in each partition.
        // With DEFAULT_PARTITIONS=16, we expect 16 batches, each with 2 rows.
        assert_eq!(batches.len(), DEFAULT_PARTITIONS);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2 * DEFAULT_PARTITIONS);

        let batch = &batches[0];

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_col.value(0), "v1");
        assert_eq!(key_col.value(1), "v2");

        let data_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let data_v1 = data_col
            .value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(data_v1, vec![1.0, 2.0, 3.0]);

        let data_v2 = data_col
            .value(1)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(data_v2, vec![4.0, 5.0, 6.0]);

        Ok(())
    }
}
