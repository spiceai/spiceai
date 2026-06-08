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

use async_trait::async_trait;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion_federation::sql::{SQLExecutor, SQLFederationProvider, SQLTableSource};
use datafusion_federation::{FederatedTableProviderAdaptor, FederatedTableSource};
use datafusion_table_providers::util::supported_functions::contains_unsupported_functions;
use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::LogicalPlan,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    sql::{
        TableReference,
        unparser::dialect::{DefaultDialect, Dialect},
    },
};

use super::{FlightSQLTable, query_to_stream};

impl FlightSQLTable {
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
impl SQLExecutor for FlightSQLTable {
    fn name(&self) -> &str {
        self.name
    }

    fn compute_context(&self) -> Option<String> {
        Some(self.join_push_down_context.clone())
    }

    fn dialect(&self) -> Arc<dyn Dialect> {
        Arc::new(DefaultDialect {})
    }

    fn can_execute_plan(&self, logical_plan: &LogicalPlan) -> bool {
        // Don't federate plans referencing a deny-listed Spice-only function
        // (e.g. json_get_str); the Flight SQL server has no such function, so
        // evaluate those locally instead.
        if let Some(func_supp) = self.function_support.as_ref()
            && contains_unsupported_functions(logical_plan, func_supp).unwrap_or(false)
        {
            return false;
        }

        // FlightSQL federation currently cannot safely unparse arbitrary custom
        // extension nodes. If any are present in the subtree — including inside
        // subquery expressions (IN, EXISTS, scalar subqueries) — do not federate.
        let mut found = false;
        let _ = logical_plan.apply_with_subqueries(|p| {
            if matches!(p, LogicalPlan::Extension(_)) {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        });
        !found
    }

    fn execute(
        &self,
        query: &str,
        schema: SchemaRef,
        _filters: &[Arc<dyn datafusion::physical_plan::PhysicalExpr>],
    ) -> DataFusionResult<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            query_to_stream(
                self.client.clone(),
                query.to_string(),
                Arc::clone(&self.cookie_store),
            ),
        )))
    }

    async fn table_names(&self) -> DataFusionResult<Vec<String>> {
        Err(DataFusionError::NotImplemented(
            "table inference not implemented".to_string(),
        ))
    }

    async fn get_table_schema(&self, table_name: &str) -> DataFusionResult<SchemaRef> {
        FlightSQLTable::get_schema(self.client.clone(), TableReference::bare(table_name))
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }
}
