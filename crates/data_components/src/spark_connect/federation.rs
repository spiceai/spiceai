/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::sync::Arc;

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::{
    error::{DataFusionError, Result as DataFusionResult},
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    sql::unparser::dialect::{CustomDialect, CustomDialectBuilder, Dialect},
};
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use futures::Stream;
use util::sql_string_literal::{BackslashEscapedStrings, QuoteEscape, refuse_variable_reference};

use crate::function_support::contains_unsupported_functions;
use crate::spark_connect::map_error_to_datafusion_err;
use datafusion::logical_expr::LogicalPlan;

use super::{SparkConnect, SparkConnectTableProvider};

impl SparkConnectTableProvider {
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_name = self.table_reference.clone();
        tracing::trace!(
            %self.table_reference,
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
impl SQLExecutor for SparkConnectTableProvider {
    fn name(&self) -> &'static str {
        "SparkConnect"
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.join_push_down_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(spark_sql_dialect())
    }

    /// Refuses to federate a plan naming a function Spark cannot evaluate, so
    /// `DataFusion` runs it locally instead of unparsing it into the statement
    /// sent to Spark -- which answers `[UNRESOLVED_ROUTINE]` for every
    /// Spice-only UDF (#13664).
    ///
    /// Fails safe, as `SqlTable` does: a support check that itself errors is
    /// read as "unsupported", so a plan is never sent on the strength of a
    /// check that did not complete.
    fn can_execute_plan(&self, plan: &LogicalPlan) -> bool {
        !contains_unsupported_functions(plan, self.spark_connect.function_support()).unwrap_or(true)
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        refuse_variable_reference(query)?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            spark_query_to_stream(self.spark_connect.clone(), query.to_string()),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        let table_name = table_name.to_string();
        self.spark_connect
            .with_session_retry(move |session| {
                let table_name = table_name.clone();
                async move {
                    Ok(session
                        .table(&table_name)?
                        .limit(0)
                        .collect()
                        .await?
                        .schema())
                }
            })
            .await
            .map_err(map_error_to_datafusion_err)
    }
}

/// Builds a record-batch stream for a federated SQL query, rerunning the query
/// against a freshly rebuilt session if the current one has gone stale/broken.
fn spark_query_to_stream(
    spark_connect: SparkConnect,
    query: String,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    stream! {
        let data = spark_connect
            .with_session_retry(|session| {
                let query = query.clone();
                async move { session.sql(&query).await?.collect().await }
            })
            .await
            .map_err(map_error_to_datafusion_err)?;
        yield (Ok(data))
    }
}

fn spark_sql_dialect() -> BackslashEscapedStrings<CustomDialect> {
    BackslashEscapedStrings::new(
        CustomDialectBuilder::new()
            .with_interval_style(datafusion::sql::unparser::dialect::IntervalStyle::SQLStandard)
            .with_identifier_quote_style('`')
            .with_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
            .with_large_utf8_cast_dtype(datafusion::sql::sqlparser::ast::DataType::String(None))
            .build(),
        QuoteEscape::Unicode,
    )
}

#[cfg(test)]
mod tests {
    use datafusion::error::Result as DataFusionResult;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::sql::unparser::Unparser;
    use util::sql_string_literal::refuse_variable_reference;
    use util::sql_string_literal::test_support::{
        UNICODE_QUOTE_LITERALS, assert_no_variable_reference_sent, assert_string_literals,
        read_spark, variable_reference_plans,
    };

    use super::spark_sql_dialect;

    #[test]
    fn string_literals_read_back_as_their_value() {
        let dialect = spark_sql_dialect();
        let unparser = Unparser::new(&dialect);
        assert_string_literals(
            |expr| unparser.expr_to_sql(expr).expect("unparse").to_string(),
            "`c`",
            UNICODE_QUOTE_LITERALS,
            read_spark,
        );
    }

    /// Federation unparses a plan with this dialect and `execute` checks the
    /// statement before it sends it.
    #[test]
    fn statements_holding_a_variable_reference_are_refused() {
        let dialect = spark_sql_dialect();
        let send = |plan: &LogicalPlan| -> DataFusionResult<String> {
            let sql = Unparser::new(&dialect).plan_to_sql(plan)?.to_string();
            refuse_variable_reference(&sql)?;
            Ok(sql)
        };
        let (refused, literal) = variable_reference_plans();
        for plan in &refused {
            assert_no_variable_reference_sent(send(plan));
        }
        send(&literal).expect("a string literal that holds ${ is sent");
    }
}
