use async_trait::async_trait;
use datafusion_federation::sql::{
    LogicalOptimizer, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use std::sync::Arc;

use crate::function_support::unfederate_plan_with_unsupported_functions;
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    sql::{TableReference, unparser::dialect::Dialect},
};

use super::{FlightTable, query_to_stream};

impl FlightTable {
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_name = self.table_reference.clone();
        tracing::trace!(
            table_reference = %self.table_reference.to_quoted_string(),
            "create_federated_table_source"
        );
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        ))
    }

    pub fn create_federated_table_provider(self: Arc<Self>) -> FederatedTableProviderAdaptor {
        let table_source = Self::create_federated_table_source(Arc::clone(&self));
        FederatedTableProviderAdaptor::new_with_provider(table_source, self)
    }
}

#[async_trait]
impl SQLExecutor for FlightTable {
    fn name(&self) -> &str {
        self.name
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.join_push_down_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::clone(&self.dialect)
    }

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        // Don't federate plans referencing a deny-listed Spice-only function
        // (e.g. json_get_str); the Flight server has no such function, so
        // evaluate those locally instead. v0.5.3 federation has no
        // `can_execute_plan` veto, so unwrap such plans in a logical
        // optimizer instead. See issue #10703.
        let function_support = self.function_support.clone()?;
        Some(Box::new(move |plan| {
            unfederate_plan_with_unsupported_functions(plan, &function_support)
        }))
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            query_to_stream(self.client.clone(), query.to_string()),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        FlightTable::get_schema(self.client.clone(), TableReference::bare(table_name))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}
