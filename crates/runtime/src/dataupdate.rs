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
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::stream;

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
    /// If UpdateType::Append, the runtime will append the data to the existing dataset.
    /// If UpdateType::Overwrite, the runtime will overwrite the existing data with the new data.
    pub update_type: UpdateType,
}

pub struct DataUpdateExecutionPlan {
    pub data_update: RwLock<DataUpdate>,
    schema: SchemaRef,
}

impl DataUpdateExecutionPlan {
    #[must_use]
    pub fn new(data_update: DataUpdate) -> Self {
        let schema = Arc::clone(&data_update.schema);
        Self {
            data_update: RwLock::new(data_update),
            schema,
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
