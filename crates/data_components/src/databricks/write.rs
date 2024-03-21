use std::{any::Any, fmt, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_flight::sql::client::FlightSqlServiceClient;
use async_trait::async_trait;
use datafusion::{
    common::OwnedTableReference,
    datasource::{TableProvider, TableType},
    execution::{context::SessionState, SendableRecordBatchStream, TaskContext},
    logical_expr::Expr,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan},
};

struct DatabricksWriter {
    schema: SchemaRef,
}

impl DatabricksWriter {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for DatabricksWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("DatabricksWriter does not support scan")
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    }
}

#[derive(Clone)]
struct DatabricksWriterExec {
    table_reference: OwnedTableReference,
    input: Arc<dyn ExecutionPlan>,
}

impl DatabricksWriterExec {
    fn new(table_reference: &OwnedTableReference, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            table_reference: table_reference.clone(),
            input,
        }
    }
}

impl std::fmt::Debug for DatabricksWriterExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "DatabricksWriterExec table_reference={}",
            self.table_reference
        )
    }
}

impl DisplayAs for DatabricksWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        let sql = self.sql().unwrap_or_default();
        write!(
            f,
            "DatabricksWriterExec table_reference={}",
            self.table_reference
        )
    }
}

impl ExecutionPlan for DatabricksWriterExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let sql = match self.sql().map_err(to_execution_error) {
            Ok(sql) => sql,
            Err(error) => return Err(error),
        };

        Ok(Box::pin(StreamConverter::new(
            self.client.clone(),
            sql.as_str(),
            self.schema(),
        )))
    }
}
