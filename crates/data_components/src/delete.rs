#![allow(clippy::missing_errors_doc)]
use std::{any::Any, error::Error, sync::Arc};

use ::arrow::{
    array::{ArrayRef, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};

#[async_trait]
pub trait DeletionSink: Send + Sync {
    async fn delete_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>>;
}

pub struct Exec {
    deletion_sink: Arc<dyn DeletionSink + 'static>,
    properties: PlanProperties,
}

impl Exec {
    pub fn new(deletion_sink: Arc<dyn DeletionSink>, schema: &SchemaRef) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );
        Self {
            deletion_sink,
            properties,
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for Exec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteExec").finish()
    }
}

impl DisplayAs for Exec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeleteExec")
            }
        }
    }
}

impl ExecutionPlan for Exec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let deletion_sink = self.deletion_sink.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(count_schema, {
            futures::stream::once(async move {
                let count = deletion_sink
                    .delete_from()
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
