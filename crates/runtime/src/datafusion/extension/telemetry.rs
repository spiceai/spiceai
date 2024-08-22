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

use async_stream::stream;
use datafusion::{
    common::DFSchemaRef,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore},
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
    },
    prelude::Expr,
};
use futures::StreamExt;
use std::{
    any::Any,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::datafusion::query::Protocol;

/// Adds telemetry to leaf nodes (i.e. `TableScans`) to track the number of bytes scanned during query execution.
pub(crate) struct BytesScannedNode {
    pub(super) input: LogicalPlan,
}

impl BytesScannedNode {
    pub(crate) fn new(input: LogicalPlan) -> Self {
        assert!(input.inputs().is_empty(), "should have no inputs");
        Self { input }
    }
}

impl Debug for BytesScannedNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for BytesScannedNode {
    fn name(&self) -> &str {
        "BytesScannedNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.input)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "should have one input");
        assert_eq!(exprs.len(), 0, "should have no expressions");
        let Some(input) = inputs.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Self { input })
    }
}

impl PartialEq<BytesScannedNode> for BytesScannedNode {
    fn eq(&self, other: &BytesScannedNode) -> bool {
        self.input == other.input
    }
}

impl Eq for BytesScannedNode {}

impl Hash for BytesScannedNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

pub(crate) struct BytesScannedExec {
    input_exec: Arc<dyn ExecutionPlan>,
}

impl BytesScannedExec {
    pub(crate) fn new(input_exec: Arc<dyn ExecutionPlan>) -> Self {
        Self { input_exec }
    }
}

impl std::fmt::Debug for BytesScannedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BytesScannedExec")
    }
}

impl DisplayAs for BytesScannedExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BytesScannedExec")
            }
        }
    }
}

impl ExecutionPlan for BytesScannedExec {
    fn name(&self) -> &str {
        "BytesScannedExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.input_exec.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input_exec]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "should have one input");
        let Some(input) = children.into_iter().next() else {
            panic!("should have one input");
        };
        Ok(Arc::new(Self { input_exec: input }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut stream = self.input_exec.execute(partition, context)?;
        let schema = stream.schema();

        let bytes_scanned_stream = stream! {
            let mut bytes_scanned = 0u64;
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => {
                        bytes_scanned += batch.get_array_memory_size() as u64;
                        yield Ok(batch)
                    }
                    Err(e) => {
                        yield Err(e);
                    }
                }
            }
            telemetry::track_bytes_scanned(bytes_scanned, Protocol::Internal.as_arc_str());
        };

        let stream_adapter = RecordBatchStreamAdapter::new(schema, bytes_scanned_stream);

        Ok(Box::pin(stream_adapter))
    }
}
