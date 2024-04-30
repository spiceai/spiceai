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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::{stream, StreamExt};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use async_stream::stream;

use super::TableScanParams;

pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
}

impl SchemaCastScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self { input, schema }
    }
}

impl DisplayAs for SchemaCastScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl fmt::Debug for SchemaCastScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaCastScanExec").field("input", &self.input).field("schema", &self.schema).finish()
    }
}

impl ExecutionPlan for SchemaCastScanExec {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream  = self.input.execute(partition, context)?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema, stream! {
            while let batch = stream.next().await {
                yield batch;
            }
        })))
    }
}
