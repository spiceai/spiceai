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
use arrow_tools::record_batch::convert_to;
use async_stream::stream;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::StreamExt;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

#[allow(clippy::module_name_repetitions)]
pub struct SchemaCastScanExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl SchemaCastScanExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();
        let properties = PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            execution_mode,
        );
        Self { input, properties }
    }
}

impl DisplayAs for SchemaCastScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SchemaCastScanExec")
    }
}

impl fmt::Debug for SchemaCastScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaCastScanExec")
            .field("input", &self.input)
            .field("properties", &self.properties)
            .finish()
    }
}

impl ExecutionPlan for SchemaCastScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![Arc::clone(&self.input)]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
        } else {
            Err(DataFusionError::Execution(
                "SchemaCastScanExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut stream = self.input.execute(partition, context)?;
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema(), {
            stream! {
                while let Some(batch) = stream.next().await {
                    let batch = match batch {
                        Ok(batch) => {
                            let batch = convert_to(batch, Arc::clone(&schema));
                            match batch {
                                Ok(batch) => Ok(batch),
                                Err(e) => Err(DataFusionError::External(Box::new(e))),
                            }
                        },
                        Err(e) => Err(e),
                    };

                    yield batch;
                }
            }
        })))
    }
}
