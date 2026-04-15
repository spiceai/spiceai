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
use datafusion::error::Result as DFResult;
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
/// default DML machinery. Implementors return `Ok(Some(plan))` when they want
/// to intercept/augment execution for an operation, or `Ok(None)` to opt out
/// and let default planning/execution proceed.
///
/// Any returned plan should follow the standard DML output contract: a
/// single-row result with a non-null `count: UInt64` column.
#[async_trait]
pub trait CatalogDmlHandler: fmt::Debug + Send + Sync {
    /// Short identifier used for diagnostics (e.g. `"cayenne"`).
    fn name(&self) -> &'static str;

    /// Build an overlay [`ExecutionPlan`] for `DELETE`.
    ///
    /// `physical_inputs` are the already-planned children from `DataFusion`
    /// (for example, filtered table scans for distributed forwarding paths).
    ///
    /// Default implementation opts out (`Ok(None)`).
    async fn delete_exec(
        &self,
        _params: DeleteParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Build an overlay [`ExecutionPlan`] for `UPDATE`.
    ///
    /// Default implementation opts out (`Ok(None)`).
    async fn update_exec(
        &self,
        _params: UpdateParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Build an overlay [`ExecutionPlan`] for `INSERT`.
    ///
    /// Default implementation opts out (`Ok(None)`).
    async fn insert_exec(
        &self,
        _params: InsertParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    /// Build an overlay [`ExecutionPlan`] for `MERGE`.
    ///
    /// Default implementation opts out (`Ok(None)`).
    async fn merge_exec(
        &self,
        _params: MergeParams,
        _physical_inputs: Vec<Arc<dyn ExecutionPlan>>,
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}
