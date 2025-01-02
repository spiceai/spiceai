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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// `SliceExec` slices an `ExecutionPlan` and returns output from a single partition.
pub struct SliceExec {
    /// The input execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// The partition of the execution plan to return.
    partition: usize,
    properties: PlanProperties,
}

impl SliceExec {
    /// Create a new `SliceExec`.
    pub fn new(input: Arc<dyn ExecutionPlan>, partition: usize) -> Self {
        let eq_properties = input.equivalence_properties().clone();
        let execution_mode = input.execution_mode();
        Self {
            input,
            partition,
            properties: PlanProperties::new(
                eq_properties,
                Partitioning::UnknownPartitioning(1),
                execution_mode,
            ),
        }
    }
}

impl fmt::Debug for SliceExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SliceExec partition: {:?}", self.partition)
    }
}

impl DisplayAs for SliceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SliceExec partition: {:?}", self.partition)
    }
}

#[async_trait]
impl ExecutionPlan for SliceExec {
    fn name(&self) -> &'static str {
        "SliceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(SliceExec::new(
                Arc::clone(&children[0]),
                self.partition,
            )))
        } else {
            Err(DataFusionError::Execution(
                "SliceExec expects exactly one input".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition > 0 {
            return Err(DataFusionError::Execution(format!(
                "SliceExec only supports 1 partitions, but partition {partition} was requested",
            )));
        }

        let sliced_input = self.input.execute(self.partition, context)?;

        Ok(sliced_input)
    }
}
