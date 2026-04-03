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
//! Intercepts SQL statements at the AST level, before `DataFusion`'s standard
//! planner, for two purposes:
//!
//! 1. **DDL extensions** — `CREATE TABLE` with `WITH (...)` options
//!    (`acceleration.*`, `dataset.*`) and distribution clauses
//!    (`PARTITION BY`, `REPLICATED`) that
//!    `DataFusion`'s `SqlToRel` does not support. Extensions are extracted from
//!    the AST, stored in the [`DdlExtensionStore`], and stripped before
//!    delegating to `DataFusion`.
//!
//! 2. **DML interception** — DELETE and UPDATE statements targeting Cayenne
//!    catalog tables are converted into [`LogicalPlan::Extension`] nodes
//!    directly for distributed mode. Support for additional DML types
//!    (INSERT, MERGE) may be added in the future.
//!
//! For everything else, the planner delegates to `DataFusion`'s standard
//! `session.statement_to_plan()` path.

mod create_table;
mod delete;
pub mod logical_nodes;
pub mod physical_execs;
mod update;

use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion_expr::WriteOp;

use crate::config::ClusterRole;
use crate::datafusion::ddl::acceleration_options::SharedDdlExtensionStore;

use super::SPICE_DEFAULT_CATALOG;

/// The type of catalog backing the planner's DML interception.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogMode {
    /// At least one DDL-enabled catalog is Cayenne-backed.
    /// DML targeting Cayenne tables is intercepted at the statement level.
    Cayenne,
    /// No Cayenne catalogs are registered. All statements are delegated
    /// to `DataFusion`'s standard planner.
    Standard,
}

/// Context for the statement planner, carrying catalog and cluster information.
pub struct PlannerContext {
    /// The catalog mode determines whether statement-level interception is active.
    pub catalog_mode: CatalogMode,

    /// The cluster role, if any. When `Some(ClusterRole::Scheduler)`, Cayenne
    /// DML is rewritten into distributed extension nodes that forward operations
    /// to executor nodes.
    pub cluster_role: Option<ClusterRole>,

    /// Shared store for DDL extensions extracted from `CREATE TABLE` statements.
    /// Populated by the planner, consumed by the analyzer rules.
    pub ddl_extension_store: SharedDdlExtensionStore,
}

/// Create a [`LogicalPlan`] from SQL, intercepting DDL extensions and
/// Cayenne DML at the statement level.
pub async fn create_logical_plan(
    sql: &str,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    // `sqlparser` does not support `REPLICATED` in CREATE TABLE, so strip it
    // before parsing and propagate the flag into CREATE TABLE extension handling.
    let (effective_sql, is_replicated) = create_table::strip_replicated_keyword(sql);

    // Step 1: Parse SQL into a DataFusion Statement (wraps sqlparser AST).
    let dialect = session.config().options().sql_parser.dialect;
    let statement = session.sql_to_statement(&effective_sql, &dialect)?;

    // Step 2: Dispatch based on statement type
    if let Statement::Statement(ref sql_stmt) = statement {
        match sql_stmt.as_ref() {
            // DDL: CREATE TABLE with extensions (WITH options, PARTITION BY, REPLICATED).
            // Intercepted regardless of catalog mode — extensions apply to
            // all catalog types (Cayenne, Iceberg, etc.).
            SQLStatement::CreateTable(ct) if create_table::has_ddl_extensions(ct, is_replicated) => {
                return create_table::plan_create_table(
                    statement,
                    session,
                    &ctx.ddl_extension_store,
                    is_replicated,
                )
                .await;
            }

            // DML: DELETE on Cayenne tables (only when Cayenne is active)
            SQLStatement::Delete(_) if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_cayenne_dml(statement, session, ctx, WriteOp::Delete).await;
            }

            // DML: UPDATE on Cayenne tables (only when Cayenne is active)
            SQLStatement::Update { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_cayenne_dml(statement, session, ctx, WriteOp::Update).await;
            }

            // Future: SQLStatement::Merge { .. }
            _ => {}
        }
    }

    // Step 3: Everything else goes through DataFusion's standard planner
    session.statement_to_plan(statement).await
}

/// Plan a DML statement (DELETE or UPDATE), producing either a local or
/// distributed Cayenne extension node.
///
/// For local mode, returns the standard `DataFusion` plan unchanged — Cayenne's
/// `TableProvider` implementation handles DML natively.
///
/// For distributed (scheduler) mode, wraps the plan into a distributed
/// extension node that forwards the operation to executors.
async fn plan_cayenne_dml(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
    expected_op: WriteOp,
) -> DFResult<LogicalPlan> {
    // Let DataFusion plan the DML to get the validated DmlStatement
    let df_plan = session.statement_to_plan(statement).await?;

    // If not in distributed mode, Cayenne's TableProvider handles DML
    // natively through DataFusion's standard physical planning. Return as-is.
    if !matches!(ctx.cluster_role, Some(ClusterRole::Scheduler)) {
        return Ok(df_plan);
    }

    let LogicalPlan::Dml(dml) = &df_plan else {
        return Err(DataFusionError::Internal(format!(
            "Expected LogicalPlan::Dml for {expected_op:?} statement"
        )));
    };

    if !matches_write_op(&dml.op, &expected_op) {
        return Err(DataFusionError::Internal(format!(
            "Expected WriteOp::{expected_op:?}, got {:?}",
            dml.op
        )));
    }

    // Check if the target table is in a Cayenne catalog
    if !is_cayenne_table(session, &dml.table_name) {
        return Ok(df_plan);
    }

    match expected_op {
        WriteOp::Delete => delete::plan_distributed_delete(dml),
        WriteOp::Update => update::plan_distributed_update(dml),
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported DML operation: {expected_op:?}"
        ))),
    }
}

/// Check if a table is in a Cayenne-backed catalog.
fn is_cayenne_table(session: &SessionState, table_name: &TableReference) -> bool {
    let catalog_name = table_name.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let catalog_list = session.catalog_list();
    if let Some(catalog) = catalog_list.catalog(catalog_name) {
        super::cayenne_ddl::is_cayenne_catalog(catalog.as_ref())
    } else {
        false
    }
}

/// Check if `WriteOp` matches the expected operation.
/// `Insert` variants carry data, so use discriminant-level matching.
fn matches_write_op(actual: &WriteOp, expected: &WriteOp) -> bool {
    std::mem::discriminant(actual) == std::mem::discriminant(expected)
}
