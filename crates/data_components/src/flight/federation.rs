use async_trait::async_trait;
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_federation_sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    sql::{
        sqlparser::ast::{self, Interval},
        unparser::dialect::{CustomDialect, DefaultDialect, Dialect},
        TableReference,
    },
};

use super::{query_to_stream, FlightTable};

impl FlightTable {
    fn create_federated_table_source(
        self: Arc<Self>,
    ) -> DataFusionResult<Arc<dyn FederatedTableSource>> {
        let table_name = self.table_reference.to_string();
        tracing::trace!(
            table_name,
            %self.table_reference,
            "create_federated_table_source"
        );
        let schema = Arc::clone(&self.schema);
        let fed_provider = Arc::new(SQLFederationProvider::new(self));
        Ok(Arc::new(SQLTableSource::new_with_schema(
            fed_provider,
            table_name,
            schema,
        )?))
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

struct SpiceAIDialect {}

impl Dialect for SpiceAIDialect {
    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn custom_scalar_to_sql(
        &self,
        scalar: &datafusion::scalar::ScalarValue,
    ) -> Option<datafusion::common::Result<ast::Expr>> {
        dbg!(scalar);
        match scalar {
            datafusion::scalar::ScalarValue::IntervalYearMonth(_) => {
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        "1".to_string(),
                    ))),
                    leading_field: Some(ast::DateTimeField::Month),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                };
                Some(Ok(ast::Expr::Interval(interval)))
            }
            datafusion::scalar::ScalarValue::IntervalDayTime(_) => {
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        "1".to_string(),
                    ))),
                    leading_field: Some(ast::DateTimeField::Day),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                };
                Some(Ok(ast::Expr::Interval(interval)))
            }
            datafusion::scalar::ScalarValue::IntervalMonthDayNano(v) => {
                let Some(v) = v else {
                    return None;
                };
                let interval = Interval {
                    value: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(
                        v.months.to_string(),
                    ))),
                    leading_field: Some(ast::DateTimeField::Month),
                    leading_precision: None,
                    last_field: None,
                    fractional_seconds_precision: None,
                };
                Some(Ok(ast::Expr::Interval(interval)))
            }
            _ => None,
        }
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('"')
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
        Arc::new(SpiceAIDialect {})
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            query_to_stream(self.client.clone(), query),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        FlightTable::get_schema(self.client.clone(), &TableReference::bare(table_name))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}
