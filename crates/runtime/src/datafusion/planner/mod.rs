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
//!    (`acceleration.*`, `dataset.*`) and `PARTITION BY` clauses that
//!    `DataFusion`'s `SqlToRel` does not support. Extensions are extracted from
//!    the AST, stored in the [`DdlExtensionStore`], and stripped before
//!    delegating to `DataFusion`.
//!
//! 2. **DML interception** — DELETE and UPDATE statements targeting Cayenne
//!    catalog tables, plus INSERT statements targeting distributed
//!    write-through tables, are converted into [`LogicalPlan::Extension`]
//!    nodes directly for distributed mode. Support for additional DML types
//!    (MERGE) may be added in the future.
//!
//! For everything else, the planner delegates to `DataFusion`'s standard
//! `session.statement_to_plan()` path.

mod create_table;
mod delete;
mod insert;
pub mod logical_nodes;
mod merge;
pub mod physical_execs;
mod update;

use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::TableReference;
use datafusion::sql::parser::Statement;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion_expr::WriteOp;
use datafusion_expr::dml::InsertOp;
use datafusion_federation::FederatedTableProviderAdaptor;

use crate::accelerated_table::AcceleratedTable;
use crate::config::ClusterRole;
use crate::datafusion::ddl::acceleration_options::SharedDdlExtensionStore;

use super::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};

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
/// distributed DML at the statement level.
pub async fn create_logical_plan(
    sql: &str,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    // Step 1: Parse SQL into a DataFusion Statement (wraps sqlparser AST).
    let dialect = session.config().options().sql_parser.dialect;
    let statement = session.sql_to_statement(sql, &dialect)?;

    // Step 2: Dispatch based on statement type
    if let Statement::Statement(ref sql_stmt) = statement {
        match sql_stmt.as_ref() {
            // DDL: CREATE TABLE with extensions (WITH options, PARTITION BY).
            // Intercepted regardless of catalog mode — extensions apply to
            // all catalog types (Cayenne, Iceberg, etc.).
            SQLStatement::CreateTable(ct) if create_table::has_ddl_extensions(ct) => {
                return create_table::plan_create_table(
                    statement,
                    session,
                    &ctx.ddl_extension_store,
                )
                .await;
            }

            // DML: DELETE on Cayenne tables (only when Cayenne is active)
            SQLStatement::Delete(_) if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Delete).await;
            }

            // DML: UPDATE on Cayenne tables (only when Cayenne is active)
            SQLStatement::Update { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Update).await;
            }

            // DML: INSERT on distributed write-through tables.
            SQLStatement::Insert(_) => {
                return plan_distributed_dml(
                    statement,
                    session,
                    ctx,
                    WriteOp::Insert(InsertOp::Append),
                )
                .await;
            }

            // DML: MERGE on Cayenne tables
            SQLStatement::Merge { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return merge::plan_merge(statement, session, ctx, sql).await;
            }

            _ => {}
        }
    }

    // Step 3: Everything else goes through DataFusion's standard planner
    session.statement_to_plan(statement).await
}

/// Plan a DML statement, producing either a local or distributed extension
/// node.
///
/// For local mode, returns the standard `DataFusion` plan unchanged.
///
/// For distributed (scheduler) mode, wraps the plan into a distributed
/// extension node that forwards the operation to executors when the target
/// table supports scheduler-side routing.
async fn plan_distributed_dml(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
    expected_op: WriteOp,
) -> DFResult<LogicalPlan> {
    // Let DataFusion plan the DML to get the validated DmlStatement
    let df_plan = session.statement_to_plan(statement).await?;

    // If not in distributed mode, keep the standard DataFusion plan.
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

    let should_rewrite = match &expected_op {
        WriteOp::Insert(_) => is_distributed_insert_table(session, &dml.table_name).await,
        _ => is_cayenne_table(session, &dml.table_name),
    };

    if !should_rewrite {
        return Ok(df_plan);
    }

    match expected_op {
        WriteOp::Delete => delete::plan_distributed_delete(dml),
        WriteOp::Update => update::plan_distributed_update(dml),
        WriteOp::Insert(_) => Ok(insert::plan_distributed_insert(dml)),
        WriteOp::Ctas => Err(DataFusionError::Internal(
            "CTAS should not reach DML planner".to_string(),
        )),
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

async fn is_distributed_insert_table(session: &SessionState, table_name: &TableReference) -> bool {
    if is_cayenne_table(session, table_name) {
        return true;
    }

    let catalog_name = table_name.catalog().unwrap_or(SPICE_DEFAULT_CATALOG);
    let schema_name = table_name.schema().unwrap_or(SPICE_DEFAULT_SCHEMA);

    let Some(catalog) = session.catalog_list().catalog(catalog_name) else {
        return false;
    };

    let Some(schema) = catalog.schema(schema_name) else {
        return false;
    };

    let Ok(Some(table_provider)) = schema.table(table_name.table()).await else {
        return false;
    };

    is_write_through_table_provider(&table_provider)
}

fn is_write_through_table_provider(table_provider: &Arc<dyn TableProvider>) -> bool {
    if let Some(accelerated) = table_provider
        .as_any()
        .downcast_ref::<AcceleratedTable>()
    {
        return accelerated.is_write_through();
    }

    if let Some(adaptor) = table_provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
        && let Some(inner_provider) = adaptor.table_provider.as_ref()
    {
        if let Some(accelerated) = inner_provider
            .as_any()
            .downcast_ref::<AcceleratedTable>()
        {
            return accelerated.is_write_through();
        }
    }

    false
}

/// Check if `WriteOp` matches the expected operation.
/// `Insert` variants carry data, so use discriminant-level matching.
fn matches_write_op(actual: &WriteOp, expected: &WriteOp) -> bool {
    std::mem::discriminant(actual) == std::mem::discriminant(expected)
}
