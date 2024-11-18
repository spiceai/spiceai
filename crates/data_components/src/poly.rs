use std::{any::Any, borrow::Cow, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Constraints,
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::{dml::InsertOp, LogicalPlan},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use datafusion_federation::{
    FederatedTableProviderAdaptor, FederatedTableSource, FederationProvider,
};

use crate::delete::DeletionTableProvider;

#[derive(Debug, Clone)]
pub struct PolyTableProvider {
    write: Arc<dyn TableProvider>,
    delete: Arc<dyn DeletionTableProvider>,
    fed: Arc<dyn TableProvider>,
}

impl PolyTableProvider {
    pub fn new(
        write: Arc<dyn TableProvider>,
        delete: Arc<dyn DeletionTableProvider>,
        fed: Arc<dyn TableProvider>,
    ) -> Self {
        PolyTableProvider { write, delete, fed }
    }

    fn get_federation_provider(&self) -> Option<Arc<dyn FederationProvider>> {
        self.fed
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()
            .map(|x| x.source.federation_provider())
    }

    #[must_use]
    pub fn get_table_source(&self) -> Option<Arc<dyn FederatedTableSource>> {
        let adaptor = self
            .fed
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>();

        adaptor.map(|f| Arc::clone(&f.source))
    }
}

#[async_trait]
impl DeletionTableProvider for PolyTableProvider {
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        self.delete.delete_from(state, filters).await
    }
}

impl FederationProvider for PolyTableProvider {
    fn name(&self) -> &str {
        "FederationProviderForPolyTableProvider"
    }

    fn compute_context(&self) -> Option<String> {
        self.get_federation_provider()
            .and_then(|f| f.compute_context())
    }

    fn analyzer(&self) -> Option<Arc<datafusion::optimizer::Analyzer>> {
        self.get_federation_provider().and_then(|f| f.analyzer())
    }
}

#[async_trait]
impl TableProvider for PolyTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.write.schema()
    }
    fn constraints(&self) -> Option<&Constraints> {
        self.write.constraints()
    }
    fn table_type(&self) -> TableType {
        self.write.table_type()
    }
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        self.write.get_logical_plan()
    }
    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.write.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.write.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        self.write.insert_into(state, input, overwrite).await
    }
}
