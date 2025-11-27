use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::Display;
use std::sync::Arc;
use std::{any::Any, fmt};

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::TableProvider,
    error::Result as DataFusionResult,
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
};

use crate::sql::sql_provider_datafusion::{expr, SqlExec};

use super::ClickHouseTable;

#[async_trait]
impl TableProvider for ClickHouseTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let filter_push_down: Vec<TableProviderFilterPushDown> = filters
            .iter()
            .map(|f| match expr::to_sql_with_engine(f, None) {
                Ok(_) => TableProviderFilterPushDown::Exact,
                Err(_) => TableProviderFilterPushDown::Unsupported,
            })
            .collect();
        Ok(filter_push_down)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let exec = SqlExec::new(
            projection,
            &self.schema(),
            &self.table_reference,
            self.pool.clone(),
            filters,
            limit,
            None, // engine - ClickHouse doesn't use specific engine
        )?
        .with_custom_table_expr(self.table_expr.clone());

        Ok(Arc::new(exec))
    }
}

impl Display for ClickHouseTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClickHouseTable {}", self.table_reference)
    }
}
