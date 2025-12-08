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

//! Streaming execution plan for Cayenne write operations.
//!
//! This module provides `StreamingExec`, an execution plan that forwards
//! record batches from a stream without buffering.

use arrow_schema::SchemaRef;
use datafusion::execution::SendableRecordBatchStream as DFStream;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType, Partitioning};
use datafusion_physical_plan::DisplayAs;
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::PlanProperties;
use futures::StreamExt;
use std::any::Any;
use std::sync::Arc;

/// A streaming execution plan that forwards batches without buffering.
///
/// This is used during chunk writes to efficiently stream data to the Vortex writer
/// without unnecessary buffering or copies.
pub struct StreamingExec {
    /// Arrow schema for the data
    pub schema: SchemaRef,
    /// The input stream wrapped in a mutex for async access
    pub stream: tokio::sync::Mutex<Option<DFStream>>,
    /// Plan properties
    pub properties: PlanProperties,
}

impl StreamingExec {
    /// Create a new `StreamingExec` from a record batch stream.
    pub fn new(schema: SchemaRef, stream: DFStream) -> Self {
        use datafusion_physical_expr::EquivalenceProperties;

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        Self {
            schema,
            stream: tokio::sync::Mutex::new(Some(stream)),
            properties,
        }
    }
}

impl std::fmt::Debug for StreamingExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingExec").finish()
    }
}

impl DisplayAs for StreamingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StreamingExec")
    }
}

impl ExecutionPlan for StreamingExec {
    fn name(&self) -> &'static str {
        "StreamingExec"
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
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<DFStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        // Use async-aware RecordBatchStreamAdapter to properly forward the stream
        let schema = Arc::clone(&self.schema);
        let stream_mutex = Arc::new(tokio::sync::Mutex::new(
            self.stream
                .try_lock()
                .map_err(|_| {
                    datafusion_common::DataFusionError::Execution(
                        "Stream is locked (concurrent access detected)".to_string(),
                    )
                })?
                .take()
                .ok_or_else(|| {
                    datafusion_common::DataFusionError::Execution(
                        "Stream already consumed".to_string(),
                    )
                })?,
        ));

        let adapter = RecordBatchStreamAdapter::new(
            schema,
            async_stream::stream! {
                let mut stream = stream_mutex.lock().await;
                while let Some(batch) = stream.next().await {
                    yield batch;
                }
            },
        );

        Ok(Box::pin(adapter))
    }
}
