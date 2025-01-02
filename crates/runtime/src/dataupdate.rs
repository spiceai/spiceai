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

use std::{any::Any, fmt, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{StreamExt, TryStreamExt};
use tokio::sync::Mutex;

use crate::datafusion::error::find_datafusion_root;

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateType {
    Append,
    Overwrite,
    Changes,
}

#[derive(Debug, Clone)]
pub struct DataUpdate {
    pub schema: SchemaRef,
    pub data: Vec<RecordBatch>,
    /// The type of update to perform.
    /// If `UpdateType::Append`, the runtime will append the data to the existing dataset.
    /// If `UpdateType::Overwrite`, the runtime will overwrite the existing data with the new data.
    /// If `UpdateType::Changes`, the runtime will apply the changes to the existing data.
    pub update_type: UpdateType,
}

pub struct StreamingDataUpdate {
    pub data: SendableRecordBatchStream,
    pub update_type: UpdateType,
}

impl StreamingDataUpdate {
    #[must_use]
    pub fn new(data: SendableRecordBatchStream, update_type: UpdateType) -> Self {
        Self { data, update_type }
    }

    pub async fn collect_data(self) -> Result<DataUpdate, DataFusionError> {
        let schema = self.data.schema();
        let data = self
            .data
            .try_collect::<Vec<_>>()
            .await
            .map_err(find_datafusion_root)?;
        Ok(DataUpdate {
            schema,
            data,
            update_type: self.update_type,
        })
    }
}

impl TryFrom<DataUpdate> for StreamingDataUpdate {
    type Error = DataFusionError;

    fn try_from(data_update: DataUpdate) -> std::result::Result<Self, Self::Error> {
        let data = Box::pin(
            MemoryStream::try_new(data_update.data, data_update.schema, None)
                .map_err(find_datafusion_root)?,
        ) as SendableRecordBatchStream;
        Ok(Self {
            data,
            update_type: data_update.update_type,
        })
    }
}

pub struct StreamingDataUpdateExecutionPlan {
    record_batch_stream: Arc<Mutex<SendableRecordBatchStream>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl StreamingDataUpdateExecutionPlan {
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

impl std::fmt::Debug for StreamingDataUpdateExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingDataUpdateExecutionPlan")
    }
}

impl DisplayAs for StreamingDataUpdateExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingDataUpdateExecutionPlan")
    }
}

impl ExecutionPlan for StreamingDataUpdateExecutionPlan {
    fn name(&self) -> &'static str {
        "StreamingDataUpdateExecutionPlan"
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
