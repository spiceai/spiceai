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

use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use aws_sdk_s3vectors::{
    operation::put_vectors::PutVectorsInput,
    types::{Index, PutInputVector},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::sink::DataSink,
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt as _;

use crate::{PUT_VECTORS_MAX_ITEMS, S3Vectors};

static NAME: &str = "PutVectorsSink";

pub struct PutVectorsSink {
    client: Arc<dyn S3Vectors + Send + Sync>,
    index: Index,
    schema: SchemaRef,
}

impl std::fmt::Debug for PutVectorsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{NAME} bucket={} index={}",
            self.index.vector_bucket_name(),
            self.index.index_name
        )
    }
}

impl DisplayAs for PutVectorsSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[async_trait]
impl DataSink for PutVectorsSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        let mut count = 0;

        while let Some(batch) = data.next().await {
            let batch = batch?;

            // TODO: Create a Vec<PutInputVector> from the record batch

            let vectors: Vec<PutInputVector> = vec![];

            for chunk in vectors.chunks(PUT_VECTORS_MAX_ITEMS) {
                let input = PutVectorsInput::builder()
                    .index_name(self.index.index_name())
                    .vector_bucket_name(self.index.vector_bucket_name())
                    .set_vectors(Some(chunk.to_vec()))
                    .build()
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                self.client
                    .put_vectors(input)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                count += chunk.len();
            }
        }

        Ok(count as _)
    }
}
