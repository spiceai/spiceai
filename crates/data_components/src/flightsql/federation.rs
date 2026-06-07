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
use datafusion_federation::sql::{
    LogicalOptimizer, RemoteTableRef, SQLExecutor, SQLFederationProvider, SQLTableSource,
};
use datafusion_federation::{FederatedPlanNode, FederatedTableProviderAdaptor, FederatedTableSource};
use std::sync::Arc;

use crate::function_support::{FunctionSupport, contains_unsupported_functions};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::tree_node::TreeNodeRecursion,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::LogicalPlan,
    physical_plan::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter},
    sql::{
        TableReference,
        unparser::dialect::{DefaultDialect, Dialect},
    },
};

/// Unwrap a federated plan that Flight SQL cannot safely push down: plans
/// referencing a deny-listed Spice-only function (e.g. `json_get_str` — the
/// Flight SQL server has no such function), and plans containing custom
/// extension nodes anywhere in the subtree (including inside IN/EXISTS/scalar
/// subqueries), which the unparser cannot safely render. Unwrapping returns
/// the inner plan so `DataFusion` evaluates it locally instead.
fn unfederate_unsupported_flightsql_plan(
    plan: LogicalPlan,
    function_support: Option<&FunctionSupport>,
) -> DataFusionResult<LogicalPlan> {
    let LogicalPlan::Extension(extension) = &plan else {
        return Ok(plan);
    };
    let Some(federated) = extension.node.as_any().downcast_ref::<FederatedPlanNode>() else {
        return Ok(plan);
    };

    if let Some(func_supp) = function_support
        && contains_unsupported_functions(federated.plan(), func_supp)?
    {
        return Ok(federated.plan().clone());
    }

    let mut found_extension = false;
    federated.plan().apply_with_subqueries(|p| {
        if matches!(p, LogicalPlan::Extension(_)) {
            found_extension = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    if found_extension {
        return Ok(federated.plan().clone());
    }

    Ok(plan)
}

use super::{FlightSQLTable, query_to_stream};

impl FlightSQLTable {
    fn create_federated_table_source(self: Arc<Self>) -> Arc<dyn FederatedTableSource> {
        let table_name = RemoteTableRef::from(self.table_reference.clone());
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

    fn logical_optimizer(&self) -> Option<LogicalOptimizer> {
        // v0.5.3 federation has no `can_execute_plan` veto; instead, install a
        // logical optimizer that unwraps federated plans Flight SQL cannot
        // safely push down (deny-listed functions, custom extension nodes).
        let function_support = self.function_support.clone();
        Some(Box::new(move |plan| {
            unfederate_unsupported_flightsql_plan(plan, function_support.as_ref())
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
