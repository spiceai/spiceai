/*
Copyright 2026, Spice AI, Inc.

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

//! The [`CatalogDmlHandler`] trait and operation parameter types.
//!
//! Implementors know how to build [`ExecutionPlan`]s for DML operations on a
//! specific catalog backend. They do not need to know about `LogicalPlan`
//! rewriting or `UserDefinedLogicalNode` plumbing.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, dml::InsertOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::TableReference;

/// Parameters for [`CatalogDmlHandler::delete_exec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeleteParams {
    pub table_name: TableReference,
    /// Predicates to apply to the target table.
    ///
    /// Empty means "apply to all rows".
    pub filters: Vec<Expr>,
}

/// Parameters for [`CatalogDmlHandler::update_exec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpdateParams {
    pub table_name: TableReference,
    /// Predicates to apply to the target table.
    ///
    /// Empty means "apply to all rows".
    pub filters: Vec<Expr>,
    /// `(column_name, value_expr)` pairs.
    pub assignments: Vec<(String, Expr)>,
}

/// Parameters for [`CatalogDmlHandler::insert_exec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InsertParams {
    pub table_name: TableReference,
    pub insert_op: InsertOp,
}

/// Parameters for [`CatalogDmlHandler::merge_exec`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MergeParams {
    pub target_table: TableReference,
    pub source_table: TableReference,
    pub target_qualifier: String,
    pub source_qualifier: String,
    /// Equi-join key pairs as `(target_col, source_col)`.
    pub on_keys: Vec<(String, String)>,
    /// `SET` assignments as `(target_col, value_expr)` pairs.
    pub assignments: Vec<(String, Expr)>,
    /// Original SQL text for implementations that forward the full statement.
    pub original_sql: Option<String>,
}

/// Catalog-specific DML handler.
///
/// Handlers are paired with [`crate::DmlExtensionNode`] and the shared,
/// stateless [`crate::DmlExtensionPlanner`].
///
/// This trait models DML as an **optional overlay** over `DataFusion`'s
/// default DML operations. Default implementations, when present, are equal
/// to the DML operations that would occur to the associated [`TableProvider`].
///
/// Any returned plan should follow the standard DML output contract: a
/// single-row result with a non-null `count: UInt64` column.
#[async_trait]
pub trait CatalogDmlHandler: fmt::Debug + Send + Sync {
    /// Short identifier used for diagnostics (e.g. `"cayenne"`).
    fn name(&self) -> &'static str;

    /// Build a custom [`ExecutionPlan`] for `DELETE`.
    ///
    /// `physical_inputs` are the already-planned children from `DataFusion`
    /// (for example, filtered table scans for distributed forwarding paths).
    async fn delete_exec(
        &self,
        params: DeleteParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        resolve_table_provider(session_state, &params.table_name)
            .await?
            .delete_from(session_state, params.filters)
            .await
    }

    /// Build a custom [`ExecutionPlan`] for `UPDATE`.
    async fn update_exec(
        &self,
        params: UpdateParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        resolve_table_provider(session_state, &params.table_name)
            .await?
            .update(session_state, params.assignments, params.filters)
            .await
    }

    /// Build a custom [`ExecutionPlan`] for `INSERT`.
    async fn insert_exec(
        &self,
        params: InsertParams,
        physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [input] = physical_inputs.as_slice() else {
            return Err(DataFusionError::Internal(
                "DML INSERT extension node requires exactly one physical input".to_string(),
            ));
        };

        resolve_table_provider(session_state, &params.table_name)
            .await?
            .insert_into(session_state, Arc::clone(input), params.insert_op)
            .await
    }

    /// Build a custom [`ExecutionPlan`] for `MERGE`.
    async fn merge_exec(
        &self,
        _params: MergeParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "MERGE DML extension node was not handled and has no default fallback".to_string(),
        ))
    }
}

async fn resolve_table_provider(
    session_state: &SessionState,
    table_name: &datafusion::sql::TableReference,
) -> DFResult<Arc<dyn datafusion::datasource::TableProvider>> {
    session_state
        .schema_for_ref(table_name.clone())?
        .table(table_name.table())
        .await?
        .ok_or_else(|| DataFusionError::Plan(format!("Table '{table_name}' not found")))
}
