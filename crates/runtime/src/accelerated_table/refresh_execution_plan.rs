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

use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};
use futures::StreamExt;
use tokio::sync::Mutex;

use super::DataFusionResult;

use async_stream::stream;

pub struct RefreshExecutionPlan {
    record_batch_stream: Arc<Mutex<SendableRecordBatchStream>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl RefreshExecutionPlan {
    #[must_use]
    pub fn new(record_batch_stream: SendableRecordBatchStream) -> Self {
        let schema = record_batch_stream.schema();
        Self {
            record_batch_stream: Arc::new(Mutex::new(record_batch_stream)),
            schema: Arc::clone(&schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl std::fmt::Debug for RefreshExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RefreshExecutionPlan")
    }
}

impl DisplayAs for RefreshExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "RefreshExecutionPlan")
    }
}

impl ExecutionPlan for RefreshExecutionPlan {
    fn name(&self) -> &'static str {
        "RefreshExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
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
        let schema = Arc::clone(&self.schema);

        let record_batch_stream = Arc::clone(&self.record_batch_stream);

        let stream = RecordBatchStreamAdapter::new(Arc::clone(&schema), {
            stream! {
                let mut stream = record_batch_stream.lock().await;
                while let Some(batch) = stream.next().await {
                    yield batch;
                }
            }
        });
        Ok(Box::pin(stream))
    }
}
