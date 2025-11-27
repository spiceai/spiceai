use crate::sql::db_connection_pool::{dbconnection::get_schema, JoinPushDown};
use async_trait::async_trait;
use datafusion_federation::sql::{
    RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::TryStreamExt;
use snafu::prelude::*;
use std::sync::Arc;

use crate::sql::sql_provider_datafusion::{
    get_stream, to_execution_error, SqlTable, UnableToGetSchemaSnafu,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    sql::{
        unparser::dialect::{DefaultDialect, Dialect},
        TableReference,
    },
};

impl<T, P> SqlTable<T, P> {
    #[allow(dead_code)]
    fn arc_dialect(&self) -> Arc<dyn Dialect + Send + Sync> {
        match &self.dialect {
            Some(dialect) => Arc::clone(dialect),
            None => Arc::new(DefaultDialect {}),
        }
    }

    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_reference = self.table_reference.clone();
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            RemoteTableRef::from(table_reference),
            schema,
        ))
    }

    /// Creates a federated table provider.
    ///
    /// # Errors
    ///
    /// This function currently never returns an error, but the Result type is kept for API compatibility.
    pub fn create_federated_table_provider(
        self: Arc<Self>,
    ) -> DataFusionResult<FederatedTableProviderAdaptor> {
        let table_source = Self::create_federated_table_source(Arc::clone(&self));
        Ok(FederatedTableProviderAdaptor::new_with_provider(
            table_source,
            self,
        ))
    }
}

#[async_trait]
impl<T, P> SQLExecutor for SqlTable<T, P> {
    fn name(&self) -> &str {
        self.name
    }

    fn compute_context(&self) -> Option<String> {
        match self.pool.join_push_down() {
            JoinPushDown::AllowedFor(context) => Some(context),
            // Don't return None here - it will cause incorrect federation with other providers of the same name that also have a compute_context of None.
            // Instead return a random string that will never match any other provider's context.
            JoinPushDown::Disallow => Some(format!("{}", std::ptr::from_ref(self) as usize)),
        }
    }

    /// Return the provided [`Dialect`], defaulting to [`DefaultDialect`].
    fn dialect(&self) -> Arc<dyn Dialect> {
        let Some(ref dialect) = self.dialect else {
            // TODO: Derive default from [`SQLExecutor::engine`].
            return Arc::new(DefaultDialect {});
        };
        Arc::clone(dialect) as Arc<_>
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(
            Arc::clone(&self.pool),
            query.to_string(),
            Arc::clone(&schema),
        );

        let stream = futures::stream::once(fut).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        let conn = self.pool.connect().await.map_err(to_execution_error)?;
        get_schema(conn, &TableReference::from(table_name))
            .await
            .context(UnableToGetSchemaSnafu)
            .map_err(to_execution_error)
    }
}
