/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

#![allow(clippy::missing_errors_doc)]
use std::{error::Error, sync::Arc};

use ::arrow::{
    array::{ArrayRef, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema},
};
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};

#[async_trait]
pub trait DeletionSink: Send + Sync {
    async fn delete_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>>;

    /// Same as [`Self::delete_from`], but given the live [`TaskContext`] the plan is being
    /// executed under. The default ignores it and delegates to [`Self::delete_from`].
    /// Sinks that must execute an inner plan under the caller's context (e.g. write-back
    /// DML, so an accelerator write inside a Cayenne transaction STAGES rather than
    /// publishing) override this to use `context`.
    async fn delete_from_with_context(
        &self,
        _context: Arc<TaskContext>,
    ) -> Result<u64, Box<dyn Error + Send + Sync>> {
        self.delete_from().await
    }
}

pub struct DeletionExec {
    deletion_sink: Arc<dyn DeletionSink + 'static>,
    properties: Arc<PlanProperties>,
}

impl DeletionExec {
    pub fn new(deletion_sink: Arc<dyn DeletionSink>) -> Self {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            deletion_sink,
            properties,
        }
    }
}

impl std::fmt::Debug for DeletionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteExec").finish_non_exhaustive()
    }
}

impl DisplayAs for DeletionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DeleteExec")
            }
        }
    }
}

impl ExecutionPlan for DeletionExec {
    fn name(&self) -> &'static str {
        "DeletionExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let deletion_sink = Arc::clone(&self.deletion_sink);
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, {
            futures::stream::once(async move {
                let count = deletion_sink
                    .delete_from_with_context(context)
                    .await
                    .map_err(datafusion::error::DataFusionError::from)?;
                let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
                if let Ok(batch) =
                    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)])
                {
                    Ok(batch)
                } else {
                    Err(DataFusionError::Execution(
                        "failed to create record batch".to_string(),
                    ))
                }
            })
        })))
    }
}
