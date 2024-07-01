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

use std::sync::RwLock;
use std::{any::Any, fmt, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use async_stream::stream;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{stream, StreamExt, TryStreamExt};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateType {
    Append,
    Overwrite,
}

#[derive(Debug, Clone)]
pub struct DataUpdate {
    pub schema: SchemaRef,
    pub data: Vec<RecordBatch>,
    /// The type of update to perform.
    /// If `UpdateType::Append`, the runtime will append the data to the existing dataset.
    /// If `UpdateType::Overwrite`, the runtime will overwrite the existing data with the new data.
    pub update_type: UpdateType,
}

pub struct StreamingDataUpdate {
    pub schema: SchemaRef,
    pub data: SendableRecordBatchStream,
    pub update_type: UpdateType,
}

impl StreamingDataUpdate {
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        data: SendableRecordBatchStream,
        update_type: UpdateType,
    ) -> Self {
        Self {
            schema,
            data,
            update_type,
        }
    }

    pub async fn collect_data(self) -> Result<DataUpdate, DataFusionError> {
        let data = self.data.try_collect::<Vec<_>>().await?;
        Ok(DataUpdate {
            schema: self.schema,
            data,
            update_type: self.update_type,
        })
    }
}

pub struct DataUpdateExecutionPlan {
    pub data_update: RwLock<DataUpdate>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl DataUpdateExecutionPlan {
    #[must_use]
    pub fn new(data_update: DataUpdate) -> Self {
        let schema = Arc::clone(&data_update.schema);
        Self {
            data_update: RwLock::new(data_update),
            schema: Arc::clone(&schema),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        }
    }
}

impl std::fmt::Debug for DataUpdateExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataUpdateExecutionPlan")
    }
}

impl DisplayAs for DataUpdateExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataUpdateExecutionPlan")
    }
}

impl ExecutionPlan for DataUpdateExecutionPlan {
    fn name(&self) -> &'static str {
        "DataUpdateExecutionPlan"
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
        let mut data_update_guard = match self.data_update.write() {
            Ok(guard) => guard,
            Err(e) => e.into_inner(),
        };
        let data = std::mem::take(&mut data_update_guard.data);
        let stream_adapter =
            RecordBatchStreamAdapter::new(self.schema(), stream::iter(data.into_iter().map(Ok)));

        Ok(Box::pin(stream_adapter))
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
