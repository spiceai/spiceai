use crate::sql::db_connection_pool::dbconnection::{get_schema, Error as DbError};
use crate::sql::sql_provider_datafusion::{get_stream, to_execution_error};
use crate::util::supported_functions::contains_unsupported_functions;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::{self, VisitMut};
use datafusion::sql::unparser::dialect::Dialect;
use datafusion_federation::sql::{
    ast_analyzer::AstAnalyzer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::TryStreamExt;
use snafu::ResultExt;
use std::sync::Arc;

use super::between::SQLiteBetweenVisitor;
use super::sql_table::SQLiteTable;
use super::sqlite_interval::SQLiteIntervalVisitor;
use datafusion::{
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::TableReference,
};

impl<T, P> SQLiteTable<T, P> {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_reference = self.base_table.table_reference.clone();
        let schema = Arc::clone(&Arc::clone(&self).base_table.schema());
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

fn sqlite_ast_analyzer(
    decimal_between: bool,
) -> impl Fn(ast::Statement) -> Result<ast::Statement, DataFusionError> {
    move |ast: ast::Statement| -> Result<ast::Statement, DataFusionError> {
        match ast {
            ast::Statement::Query(query) => {
                let mut new_query = query.clone();

                // normalize INTERVAL usage into SQLite-compatible datetime expressions
                let mut interval_visitor = SQLiteIntervalVisitor::default();
                let _ = new_query.visit(&mut interval_visitor);

                // rewrite BETWEEN clauses with numeric operands to decimal comparisons
                if decimal_between {
                    let mut between_visitor = SQLiteBetweenVisitor::default();
                    let _ = new_query.visit(&mut between_visitor);
                }

                Ok(ast::Statement::Query(new_query))
            }
            _ => Ok(ast),
        }
    }
}

#[async_trait]
impl<T, P> SQLExecutor for SQLiteTable<T, P> {
    fn name(&self) -> &str {
        self.base_table.name()
    }

    fn compute_context(&self) -> Option<String> {
        self.base_table.compute_context()
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        self.base_table.dialect()
    }

    fn ast_analyzer(&self) -> Option<AstAnalyzer> {
        let rule = Box::new(sqlite_ast_analyzer(self.decimal_between));
        Some(AstAnalyzer::new(vec![rule]))
    }
    fn can_execute_plan(&self, plan: &LogicalPlan) -> bool {
        // Default to not federate if [`Self::function_support`] provided, otherwise true.
        self.function_support.as_ref().is_none_or(|func_supp| {
            !contains_unsupported_functions(plan, func_supp).unwrap_or(false)
        })
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let fut = get_stream(
            self.base_table.clone_pool(),
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
        let conn = self
            .base_table
            .clone_pool()
            .connect()
            .await
            .map_err(to_execution_error)?;
        get_schema(conn, &TableReference::from(table_name))
            .await
            .boxed()
            .map_err(|e| DbError::UnableToGetSchema { source: e })
            .map_err(to_execution_error)
    }
}
