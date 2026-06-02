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
//! 2. **DML interception (optional overlay)** — only statements that need
//!    non-default behavior are rewritten to generic `datafusion_dml`
//!    extension nodes:
//!    - DELETE/UPDATE targeting Cayenne tables in scheduler mode,
//!    - INSERT targeting distributed write-through tables,
//!    - MERGE targeting Cayenne tables.
//!
//!    All other DML uses standard `DataFusion` planning/execution unchanged.
//!
//! For everything else, the planner delegates to `DataFusion`'s standard
//! `session.statement_to_plan()` path.

mod create_table;
mod delete;
mod insert;
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
use datafusion::sql::sqlparser::ast::CreateTableOptions;
use datafusion::sql::sqlparser::ast::Statement as SQLStatement;
use datafusion_dml::CatalogDmlHandler;
use datafusion_expr::WriteOp;
use datafusion_expr::dml::InsertOp;
use datafusion_federation::FederatedTableProviderAdaptor;
use tokio::runtime::Handle;

use crate::accelerated_table::AcceleratedTable;
use crate::config::ClusterRole;
use crate::datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use datafusion_ddl::{SharedDdlExtensionStore, has_ddl_extensions};

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
    /// DML is rewritten into generic extension nodes backed by the distributed
    /// Cayenne DML handler.
    pub cluster_role: Option<ClusterRole>,

    /// Shared store for DDL extensions extracted from `CREATE TABLE` statements.
    /// Populated by the planner, consumed by the analyzer rules.
    pub ddl_extension_store: SharedDdlExtensionStore,

    /// Executor registry, if running in a distributed cluster.
    /// Used by `CREATE TABLE ... LIKE` to resolve auto-generated partition
    /// labels (e.g. `expr0`) back to the original SQL expression.
    pub executor_registry: Option<Arc<crate::cluster::ExecutorRegistry>>,

    /// IO runtime handle used by the distributed Cayenne DML handler.
    pub io_runtime: Handle,

    /// DDL handler for `CREATE TABLE ... LIKE`.
    /// Used to produce a [`datafusion_ddl::DdlExtensionNode`] for the LIKE path,
    /// bypassing the standard analyzer rule.
    pub ddl_handler: Option<Arc<dyn datafusion_ddl::CatalogDdlHandler>>,
}

impl PlannerContext {
    fn cayenne_dml_handler(&self) -> DFResult<Arc<dyn CatalogDmlHandler>> {
        match self.cluster_role {
            Some(ClusterRole::Scheduler) => {
                let Some(executor_registry) = &self.executor_registry else {
                    return Err(DataFusionError::Internal(
                        "Scheduler-mode Cayenne DML planning requires an executor registry"
                            .to_string(),
                    ));
                };

                Ok(Arc::new(
                    crate::datafusion::cayenne_ddl::DistributedCayenneDmlHandler::new(
                        Arc::clone(executor_registry),
                        Some(self.io_runtime.clone()),
                    ),
                ) as Arc<dyn CatalogDmlHandler>)
            }
            _ => Ok(Arc::new(cayenne::ddl::CayenneDmlHandler::new(
                SPICE_DEFAULT_CATALOG,
                SPICE_DEFAULT_SCHEMA,
            )) as Arc<dyn CatalogDmlHandler>),
        }
    }
}

/// Create a [`LogicalPlan`] from SQL, intercepting DDL extensions and
/// distributed DML at the statement level.
pub async fn create_logical_plan(
    sql: &str,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    let dialect = session.config().options().sql_parser.dialect;
    let statement = session.sql_to_statement(sql, &dialect)?;
    create_logical_plan_from_statement(sql, statement, session, ctx).await
}

pub async fn create_logical_plan_from_statement(
    sql: &str,
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
) -> DFResult<LogicalPlan> {
    if let Statement::Statement(ref sql_stmt) = statement {
        match sql_stmt.as_ref() {
            SQLStatement::CreateTable(ct) if ct.like.is_some() => {
                let has_columns = !ct.columns.is_empty();
                let has_partition_by = ct.partition_by.is_some();
                let has_with = !matches!(ct.table_options, CreateTableOptions::None);
                if has_columns || has_partition_by || has_with || has_ddl_extensions(ct) {
                    return Err(DataFusionError::Plan(
                        "CREATE TABLE ... (LIKE ...) cannot be combined with PARTITION BY, WITH \
                         options, or additional column definitions. The new table inherits all \
                         properties \
                         from the source table."
                            .to_string(),
                    ));
                }
                return create_table::plan_create_table_like(statement, session, ctx).await;
            }
            SQLStatement::CreateTable(ct) if has_ddl_extensions(ct) => {
                return create_table::plan_create_table(
                    statement,
                    session,
                    &ctx.ddl_extension_store,
                )
                .await;
            }
            SQLStatement::Delete(_) if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Delete).await;
            }
            SQLStatement::Update { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return plan_distributed_dml(statement, session, ctx, WriteOp::Update).await;
            }
            SQLStatement::Insert(_) => {
                return plan_distributed_dml(
                    statement,
                    session,
                    ctx,
                    WriteOp::Insert(InsertOp::Append),
                )
                .await;
            }
            SQLStatement::Merge { .. } if ctx.catalog_mode == CatalogMode::Cayenne => {
                return merge::plan_distributed_merge(statement, session, ctx, sql).await;
            }
            _ => {}
        }
    }

    session.statement_to_plan(statement).await
}

async fn plan_distributed_dml(
    statement: Statement,
    session: &SessionState,
    ctx: &PlannerContext,
    expected_op: WriteOp,
) -> DFResult<LogicalPlan> {
    let df_plan = session.statement_to_plan(statement).await?;

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

    let handler = ctx.cayenne_dml_handler()?;

    match expected_op {
        WriteOp::Delete => delete::plan_distributed_delete(dml, Arc::clone(&handler)),
        WriteOp::Update => update::plan_distributed_update(dml, Arc::clone(&handler)),
        WriteOp::Insert(_) => insert::plan_distributed_insert(dml, handler),
        WriteOp::Ctas => Err(DataFusionError::Internal(
            "CTAS should not reach DML planner".to_string(),
        )),
        WriteOp::Truncate => Err(DataFusionError::Internal(
            "TRUNCATE should not reach DML planner".to_string(),
        )),
    }
}

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
    // Distributed scheduler-mode INSERT rewriting applies to both:
    // 1. Cayenne catalog tables, which must be forwarded to executors, and
    // 2. dual-write accelerated tables (Iceberg federated catalog cache), which
    //    also forward writes remotely.
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

    is_dual_write_table_provider(&table_provider)
}

fn is_dual_write_table_provider(table_provider: &Arc<dyn TableProvider>) -> bool {
    if let Some(accelerated) = table_provider.as_any().downcast_ref::<AcceleratedTable>() {
        return accelerated.is_dual_write();
    }

    if let Some(adaptor) = table_provider
        .as_any()
        .downcast_ref::<FederatedTableProviderAdaptor>()
        && let Some(inner_provider) = adaptor.table_provider.as_ref()
        && let Some(accelerated) = inner_provider.as_any().downcast_ref::<AcceleratedTable>()
    {
        return accelerated.is_dual_write();
    }

    false
}

fn matches_write_op(actual: &WriteOp, expected: &WriteOp) -> bool {
    std::mem::discriminant(actual) == std::mem::discriminant(expected)
}
