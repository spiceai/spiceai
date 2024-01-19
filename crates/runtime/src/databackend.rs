use std::{any::Any, sync::Arc};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::{
    datasource::TableProvider,
    execution::context::SessionState,
    logical_expr::{Expr, TableType},
    physical_plan::ExecutionPlan,
};

pub struct InMemoryBackend {
    pub schema: SchemaRef,
    pub data: Vec<RecordBatch>,
}

impl TableProvider for InMemoryBackend {
    fn as_any(&self) -> &dyn Any {
        return self;
    }

    fn schema(&self) -> SchemaRef {
        return self.schema.clone();
    }

    fn table_type(&self) -> TableType {
        return TableType::Base;
    }

    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, Box<dyn std::error::Error>> {
        todo!()
    }
}
