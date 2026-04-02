/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Unified SQL statement planner.
//!
//! All DML statements (DELETE, UPDATE, INSERT) targeting Cayenne catalog tables
//! are intercepted at the statement level and converted into
//! [`LogicalPlan::Extension`] nodes directly, instead of relying on
//! DataFusion's DML planning followed by post-hoc interception.
//!
//! For everything else, the planner delegates to DataFusion's standard
//! `session.statement_to_plan()` path.

mod delete;
pub mod logical_nodes;
pub mod physical_execs;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;

use crate::config::ClusterRole;

use super::SPICE_DEFAULT_CATALOG;

/// The type of catalog backing the planner's DML interception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMode {
    /// At least one DDL-enabled catalog is Cayenne-backed.
    /// DML targeting Cayenne tables is intercepted at the statement level.
    Cayenne,
    /// No Cayenne catalogs are registered. All statements are delegated
    /// to DataFusion's standard planner.
    Standard,
}

/// Context for the statement planner, carrying catalog and cluster information.
pub struct PlannerContext {
    /// The catalog mode determines whether DML interception is active.
    pub catalog_mode: CatalogMode,

    /// The cluster role, if any. When `Some(ClusterRole::Scheduler)`, Cayenne
    /// DML is rewritten into distributed extension nodes that forward operations
    /// to executor nodes.
    pub cluster_role: Option<ClusterRole>,
}

/// Create a [`LogicalPlan`] from SQL, intercepting Cayenne DML at the statement level.
pub async fn create_logical_plan(
    sql: &str,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    // Fast path: if Cayenne is not active, skip statement-level parsing and
    // delegate entirely to DataFusion.
    if ctx.catalog_mode != CatalogMode::Cayenne {
        return session.create_logical_plan(sql).await;
    }

    // Step 1: Parse SQL into a DataFusion Statement (wraps sqlparser AST)
    let dialect = session.config().options().sql_parser.dialect;
    let statement = session.sql_to_statement(sql, &dialect)?;

    // Step 2: Check if this is a DML statement we should intercept
    if let Statement::Statement(ref sql_stmt) = statement {
        match sql_stmt.as_ref() {
            SQLStatement::Delete(_) => {
                return plan_cayenne_delete(statement, session, ctx).await;
            }
            // Future: SQLStatement::Update { .. }, SQLStatement::Merge { .. }
            _ => {}
        }
    }

    // Step 3: Everything else goes through DataFusion's standard planner
    session.statement_to_plan(statement).await
}

/// Plan a DELETE statement, producing either a local or distributed Cayenne
/// extension node.
///
/// For local mode, returns the standard DataFusion plan unchanged — Cayenne's
/// `TableProvider` implementation handles DELETE natively via `DeletionExec`.
///
/// For distributed (scheduler) mode, wraps the plan into a
/// `DistributedCayenneDeleteNode` that forwards the operation to executors.
async fn plan_cayenne_delete(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    // Let DataFusion plan the DELETE to get the validated DmlStatement
    let df_plan = session.statement_to_plan(statement).await?;

    // If not in distributed mode, Cayenne's TableProvider handles DELETE
    // natively through DataFusion's standard physical planning. Return as-is.
    if !matches!(ctx.cluster_role, Some(ClusterRole::Scheduler)) {
        return Ok(df_plan);
    }

    let LogicalPlan::Dml(dml) = &df_plan else {
        return Err(DataFusionError::Internal(
            "Expected LogicalPlan::Dml for DELETE statement".to_string(),
        ));
    };

    if !matches!(&dml.op, datafusion::logical_expr::WriteOp::Delete) {
        return Err(DataFusionError::Internal(format!(
            "Expected WriteOp::Delete, got {:?}",
            dml.op
        )));
    }

    // Check if the target table is in a Cayenne catalog
    let catalog_name = dml.table_name.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);

    let is_cayenne = {
        let catalog_list = session.catalog_list();
        if let Some(catalog) = catalog_list.catalog(catalog_name) {
            super::cayenne_ddl::is_cayenne_catalog(catalog.as_ref())
        } else {
            false
        }
    };

    // If not a Cayenne table, return the standard DF plan unchanged
    if !is_cayenne {
        return Ok(df_plan);
    }

    delete::plan_distributed_delete(dml)
}
