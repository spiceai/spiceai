use crate::sql::db_connection_pool::dbconnection::{get_schema, Error as DbError};
use crate::sql::db_connection_pool::{DbConnectionPool, JoinPushDown};
use crate::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    ast_analyzer::AstAnalyzer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;
use std::sync::Arc;

use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

use super::ClickHouseTable;

impl ClickHouseTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_reference = self.table_reference.clone();
        let schema = self.schema.clone();
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_reference),
            schema,
        )))
    }

    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self))?;
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[async_trait]
impl SQLExecutor for ClickHouseTable {
    fn name(&self) -> &str {
        "clickhouse"
    }

    fn compute_context(&self) -> Option<String> {
        match self.pool.join_push_down() {
            JoinPushDown::Disallow => Some(format!("{}", std::ptr::from_ref(self) as usize)),
            JoinPushDown::AllowedFor(s) => Some(s),
        }
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.dialect.clone()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        None
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(self.pool.clone(), query.to_string(), Arc::clone(&schema));
        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        get_schema(
            self.pool.connect().await?,
            &TableReference::from(table_name),
        )
        .await
        .boxed()
        .map_err(|e| DbError::UnableToGetSchema { source: e })
        .map_err(to_execution_error)
    }
}
