use arrow::array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableProvider;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use runtime_proto::UdtfArgs;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::cluster::datafusion::codec::udtf_args::UdtfArgsExt;
use crate::execution_plan::UdtfExec;

/// UDTF name constant for `list_udfs`
pub const LIST_UDFS_UDTF_NAME: &str = "list_udfs";

pub struct ListUDFTableFunc {
    context: Arc<SessionContext>,
}

impl ListUDFTableFunc {
    pub fn new(context: Arc<SessionContext>) -> Self {
        Self { context }
    }
}

impl Debug for ListUDFTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListUDFTableFunc").finish()
    }
}

impl TableFunctionImpl for ListUDFTableFunc {
    fn call(&self, _exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let udf_names = self.context.udfs();
        Ok(Arc::new(ListUDFTable::new(udf_names.into_iter().collect())))
    }
}

/// The `TableProvider` produced by the `list_udfs()` UDTF.
///
/// This table contains a single column "name" with all registered UDF names.
#[derive(Debug)]
pub struct ListUDFTable {
    schema: SchemaRef,
    udf_names: Vec<String>,
}

impl ListUDFTable {
    pub fn new(udf_names: Vec<String>) -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            udf_names,
        }
    }
}
#[async_trait]
impl TableProvider for ListUDFTable {
    fn as_any(&self) -> &dyn std::any::Any {
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
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let udf_name_array = Arc::new(StringArray::from(self.udf_names.clone())) as ArrayRef;

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), vec![udf_name_array])?;

        let memory_source =
            MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&self.schema), None)?;

        let inner_exec = Arc::new(DataSourceExec::new(Arc::new(memory_source)));

        // Wrap in UdtfExec for distributed execution support.
        // The UdtfArgs allow the UDTF to be re-invoked on remote executors.
        let udtf_exec = UdtfExec::new(UdtfArgs::list_udfs(), inner_exec);

        Ok(Arc::new(udtf_exec))
    }
}
