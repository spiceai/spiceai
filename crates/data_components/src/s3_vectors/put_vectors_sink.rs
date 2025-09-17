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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::sink::DataSink,
    error::Result as DataFusionResult,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType, PlanProperties},
};
use s3_vectors::S3Vectors;

use super::S3VectorIdentifier;

struct PutVectorsSink {
    idx: S3VectorIdentifier,
    client: Arc<dyn S3Vectors + Send + Sync>,
    plan_properties: PlanProperties,
    schema: SchemaRef,
}

impl std::fmt::Debug for PutVectorsSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutVectorsExec").finish_non_exhaustive()
    }
}

impl DisplayAs for PutVectorsSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PutVectorsExec")
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
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> DataFusionResult<u64> {
        todo!()
    }
}
