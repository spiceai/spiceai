#![allow(clippy::missing_errors_doc)]
use std::{any::Any, error::Error, sync::Arc};

#[cfg(feature = "duckdb")]
use crate::duckdb::write::DuckDBTableWriter;
#[cfg(feature = "postgres")]
use crate::postgres::write::PostgresTableWriter;
#[cfg(feature = "sqlite")]
use crate::sqlite::write::SqliteTableWriter;

use crate::arrow::write::MemTable;

use ::arrow::{
    array::{ArrayRef, RecordBatch, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties,
    },
};

#[async_trait]
pub trait DeletionTableProvider: TableProvider {
    async fn delete_from(
        &self,
        _state: &SessionState,
        _filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan("Not implemented".to_string()))
    }
}

#[async_trait]
pub trait DeletionSink: Send + Sync {
    async fn delete_from(&self) -> Result<u64, Box<dyn Error + Send + Sync>>;
}

pub struct DeletionExec {
    deletion_sink: Arc<dyn DeletionSink + 'static>,
    properties: PlanProperties,
}

impl DeletionExec {
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
impl std::fmt::Debug for DeletionExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeleteExec").finish()
    }
}

impl DisplayAs for DeletionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DeleteExec")
            }
        }
    }
}

impl ExecutionPlan for DeletionExec {
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

// There is no good way to allow inter trait casting yet as TableProvider is not controlled
pub fn cast_to_deleteable<'a>(
    from: &'a dyn TableProvider,
) -> Option<&'a (dyn DeletionTableProvider + 'a)> {
    if let Some(p) = from.as_any().downcast_ref::<MemTable>() {
        return Some(p);
    }

    #[cfg(feature = "postgres")]
    if let Some(p) = from.as_any().downcast_ref::<PostgresTableWriter>() {
        return Some(p);
    }

    #[cfg(feature = "duckdb")]
    if let Some(p) = from.as_any().downcast_ref::<DuckDBTableWriter>() {
        return Some(p);
    }

    #[cfg(feature = "sqlite")]
    if let Some(p) = from.as_any().downcast_ref::<SqliteTableWriter>() {
        return Some(p);
    }

    None
}
