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

//! Physical execution plans for broadcast Cayenne DDL and distributed DML.
//!
//! **DDL (broadcast)**: `CayenneCreateTableExec` and `CayenneDropTableExec` call
//! the single-node `cayenne::ddl::operations::*` functions, then add distributed
//! steps (executor connectivity check, partition metadata init, DDL forwarding,
//! LIKE assignment copy). `CayenneCreateSchemaExec` and `CayenneMergeExec` are
//! re-exported directly from `cayenne::ddl::physical_plans` — schema creation
//! and local merge need no broadcast step.
//!
//! **DML (distributed)**: `DistributedCayenneDeleteExec`, `DistributedCayenneUpdateExec`,
//! `DistributedCayenneInsertExec`, `DistributedCayenneMergeExec` forward DML SQL
//! verbatim to all connected executor nodes via FlightSQL.

use std::any::Any;
use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::ddl::operations::{self, create_table, drop_table};
use datafusion::catalog::CatalogProviderList;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_ddl::arrow_datatype_to_sql;

use super::get_cayenne_provider;
use crate::cluster::executor_registry::ExecutorRegistry;

// Re-export single-node schema/merge execs (no broadcast needed for those).
pub use cayenne::ddl::physical_plans::{CayenneCreateSchemaExec, CayenneMergeExec};

fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
}

fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn ddl_plan_properties(schema: SchemaRef) -> PlanProperties {
    PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    )
}

// ── Executor forwarding helpers ───────────────────────────────────────────────

async fn forward_ddl_to_executors(registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    forward_to_executors(registry, sql, false).await
}

async fn forward_dml_to_executors(registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    forward_to_executors(registry, sql, true).await
}

const MAX_DML_RETRIES: u32 = 3;

async fn forward_to_executors(
    registry: &ExecutorRegistry,
    sql: &str,
    require_all: bool,
) -> DFResult<()> {
    let clients = registry.flight_sql_clients.read().await;
    if clients.is_empty() {
        tracing::debug!(sql, "Skipping Cayenne forwarding: no executors connected");
        return Ok(());
    }
    let futures: Vec<_> = clients
        .iter()
        .map(|(executor_id, client)| {
            let client = client.clone();
            let sql = sql.to_string();
            let executor_id = executor_id.clone();
            async move {
                let max_attempts = if require_all { MAX_DML_RETRIES + 1 } else { 1 };
                let mut last_err: Option<String> = None;
                for attempt in 0..max_attempts {
                    if attempt > 0 {
                        let backoff = std::time::Duration::from_millis(100 * 2u64.pow(attempt - 1));
                        tokio::time::sleep(backoff).await;
                    }
                    match forward_sql_to_executor(client.clone(), &sql).await {
                        Ok(()) => {
                            tracing::debug!(executor_id, sql, "Forwarded Cayenne DDL/DML");
                            return (executor_id, Ok(()));
                        }
                        Err(e) => last_err = Some(e),
                    }
                }
                (executor_id, Err(last_err.unwrap_or_default()))
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;
    if require_all {
        let failures: Vec<_> = results
            .iter()
            .filter_map(|(id, r)| r.as_ref().err().map(|e| format!("{id}: {e}")))
            .collect();
        if !failures.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "Failed to forward statement to executors: {}",
                failures.join(", ")
            )));
        }
    } else if results.iter().all(|(_, r)| r.is_err()) && !results.is_empty() {
        return Err(DataFusionError::Execution(
            "Failed to forward DDL to any executor".to_string(),
        ));
    }
    Ok(())
}

async fn forward_sql_to_executor(
    mut client: data_components::flightsql::FlightSqlClient,
    sql: &str,
) -> Result<(), String> {
    client
        .execute(sql.to_string(), None)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

// ── Broadcast CayenneCreateTableExec ─────────────────────────────────────────

/// Broadcast physical plan for `CREATE TABLE` on a Cayenne catalog.
///
/// 1. Checks that at least one executor is connected (distributed mode guard).
/// 2. Calls [`create_table`] to register in metadata + DataFusion.
/// 3. Initialises partition metadata on the scheduler.
/// 4. Forwards the `CREATE TABLE` DDL SQL to all executor nodes.
/// 5. Copies partition-to-executor assignments for `LIKE` tables.
pub struct CayenneCreateTableExec {
    params: operations::CreateTableParams,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    /// DDL log for recording this CREATE TABLE so late-joining executors replay it.
    ddl_log: Option<Arc<crate::cluster::DdlLog>>,
    // Stashed for DDL SQL construction when forwarding to executors.
    arrow_schema_for_fwd: Arc<arrow::datatypes::Schema>,
    primary_key_for_fwd: Vec<String>,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateTableExec (broadcast)")
            .field("table_name", &self.params.table_name)
            .field("catalog_name", &self.params.catalog_name)
            .finish_non_exhaustive()
    }
}

impl CayenneCreateTableExec {
    #[must_use]
    pub fn new(
        params: operations::CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        ddl_log: Option<Arc<crate::cluster::DdlLog>>,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Self {
        let arrow_schema_for_fwd = Arc::clone(&params.arrow_schema);
        let primary_key_for_fwd = params.primary_key.clone();
        let schema = ddl_result_schema();
        Self {
            params,
            catalog_list,
            executor_registry,
            ddl_log,
            arrow_schema_for_fwd,
            primary_key_for_fwd,
            runtime_env,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for CayenneCreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneCreateTableExec(broadcast): {}.{}.{}",
            self.params.catalog_name, self.params.schema_name, self.params.table_name
        )
    }
}

impl ExecutionPlan for CayenneCreateTableExec {
    fn name(&self) -> &'static str {
        "CayenneCreateTableExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let catalog_name = self.params.catalog_name.clone();
        let schema_name = self.params.schema_name.clone();
        let table_name = self.params.table_name.clone();
        let arrow_schema = Arc::clone(&self.params.arrow_schema);
        let primary_key = self.params.primary_key.clone();
        let partition_expr_sql = self.params.partition_expr_sql.clone();
        let if_not_exists = self.params.if_not_exists;
        let like_source_table = self.params.like_source_table.clone();
        let ctx_opt = self.params.ctx.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = self.executor_registry.clone();
        let ddl_log = self.ddl_log.clone();
        let arrow_schema_fwd = Arc::clone(&self.arrow_schema_for_fwd);
        let primary_key_fwd = self.primary_key_for_fwd.clone();
        let runtime_env = Arc::clone(&self.runtime_env);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            // 1. Executor connectivity guard (distributed mode only).
            if let Some(ref registry) = executor_registry
                && registry.flight_sql_clients.read().await.is_empty()
            {
                return Err(DataFusionError::Execution(format!(
                    "Failed to create table '{table_name}' in Cayenne catalog '{catalog_name}': \
                     no executors are currently connected. At least one executor must be \
                     connected before creating tables."
                )));
            }

            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            // 2. Core create — metadata catalog + DataFusion registration.
            let ops_params = operations::CreateTableParams {
                table_name: table_name.clone(),
                schema_name: schema_name.clone(),
                catalog_name: catalog_name.clone(),
                arrow_schema,
                primary_key,
                partition_expr_sql: partition_expr_sql.clone(),
                if_not_exists,
                like_source_table: like_source_table.clone(),
                ctx: ctx_opt,
            };
            let outcome =
                create_table(ops_params, cayenne_provider, Arc::clone(&runtime_env)).await?;

            let table_ref = datafusion::sql::TableReference::full(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
            );

            // 3. Initialise partition metadata so the scheduler can route queries.
            if let Some(ref registry) = executor_registry {
                let expr_sql = partition_expr_sql.as_ref().cloned();
                if let Some(expr_sql) = expr_sql {
                    let pm = registry.federated_partition_manager();
                    if let Err(e) = pm.initialize_metadata(&table_ref, vec![expr_sql]).await {
                        tracing::warn!(
                            table = %table_ref,
                            error = %e,
                            "Failed to initialize partition metadata"
                        );
                    }
                }
            }

            // 4. Build and forward CREATE TABLE DDL SQL.
            if let Some(ref registry) = executor_registry {
                let columns_sql: Vec<String> = arrow_schema_fwd
                    .fields()
                    .iter()
                    .map(|f| {
                        let null_str = if f.is_nullable() { "" } else { " NOT NULL" };
                        let sql_type = arrow_datatype_to_sql(f.data_type())?;
                        Ok(format!("\"{}\" {sql_type}{null_str}", f.name()))
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                let mut table_elements = columns_sql;
                if !primary_key_fwd.is_empty() {
                    let pk_cols = primary_key_fwd
                        .iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", ");
                    table_elements.push(format!("PRIMARY KEY ({pk_cols})"));
                }
                let ddl_sql = format!(
                    "CREATE TABLE IF NOT EXISTS \
                     \"{catalog_name}\".\"{schema_name}\".\"{table_name}\" ({})",
                    table_elements.join(", ")
                );
                forward_ddl_to_executors(registry, &ddl_sql).await?;

                // Record in the DDL log so late-joining executors can replay it.
                // Only append after successful forwarding (the `?` above ensures that).
                if let Some(ref log) = ddl_log {
                    log.append(&ddl_sql);
                }
            }

            // 5. Copy partition assignments for LIKE tables.
            if let Some(ref source) = like_source_table
                && let Some(ref registry) = executor_registry
            {
                let pm = registry.federated_partition_manager();
                if let Err(e) = pm.copy_assignments(&source, &table_ref).await {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to create table '{table_name}': could not copy partition \
                         assignments from source table {source}: {e}"
                    )));
                }
            }

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![outcome.message]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── Broadcast CayenneDropTableExec ────────────────────────────────────────────

/// Broadcast physical plan for `DROP TABLE` on a Cayenne catalog.
pub struct CayenneDropTableExec {
    table_name: String,
    if_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    /// DDL log for recording this DROP TABLE so late-joining executors replay it.
    ddl_log: Option<Arc<crate::cluster::DdlLog>>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneDropTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDropTableExec (broadcast)")
            .field("table_name", &self.table_name)
            .field("catalog_name", &self.df_catalog_name)
            .finish_non_exhaustive()
    }
}

impl CayenneDropTableExec {
    #[must_use]
    pub fn new(
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        ddl_log: Option<Arc<crate::cluster::DdlLog>>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            executor_registry,
            ddl_log,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for CayenneDropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneDropTableExec(broadcast): {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }
}

impl ExecutionPlan for CayenneDropTableExec {
    fn name(&self) -> &'static str {
        "CayenneDropTableExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let if_exists = self.if_exists;
        let catalog_name = self.df_catalog_name.clone();
        let schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = self.executor_registry.clone();
        let ddl_log = self.ddl_log.clone();
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            // Executor connectivity guard.
            if let Some(ref registry) = executor_registry
                && registry.flight_sql_clients.read().await.is_empty()
            {
                return Err(DataFusionError::Execution(format!(
                    "Failed to drop table '{table_name}' from Cayenne catalog '{catalog_name}': \
                     no executors are currently connected."
                )));
            }

            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            // Core drop — metadata catalog + DataFusion deregistration.
            let outcome = drop_table(
                &table_name,
                &schema_name,
                &catalog_name,
                if_exists,
                cayenne_provider,
                &df_catalog,
            )
            .await?;

            // Forward DROP TABLE to executors and record in the DDL log.
            if outcome.message.contains("dropped") {
                let ddl_sql = format!(
                    "DROP TABLE IF EXISTS \
                     \"{catalog_name}\".\"{schema_name}\".\"{table_name}\""
                );
                if let Some(ref registry) = executor_registry {
                    forward_ddl_to_executors(registry, &ddl_sql).await?;
                }
                // Record in the DDL log so late-joining executors can replay it.
                if let Some(ref log) = ddl_log {
                    log.append(&ddl_sql);
                }
            }

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![outcome.message]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── DistributedCayenneDeleteExec ──────────────────────────────────────────────

pub struct DistributedCayenneDeleteExec {
    table_name: datafusion::sql::TableReference,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    filter_sql: Option<String>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDeleteExec")
            .field("table_name", &self.table_name.to_string())
            .field("filter_sql", &self.filter_sql)
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneDeleteExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        filter_sql: Option<String>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = ddl_plan_properties(schema);
        Self {
            table_name,
            executor_registry,
            filter_sql,
            input,
            properties,
        }
    }
}

impl DisplayAs for DistributedCayenneDeleteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneDeleteExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneDeleteExec {
    fn name(&self) -> &'static str {
        "CayenneDeleteExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("CayenneDeleteExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            self.executor_registry.clone(),
            self.filter_sql.clone(),
            input,
        )))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let executor_registry = self.executor_registry.clone();
        let filter_sql = self.filter_sql.clone();
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
            let Some(ref registry) = executor_registry else {
                return Err(DataFusionError::Execution(format!(
                    "DELETE on '{table_name}' cannot be forwarded: no executor registry"
                )));
            };
            let mut sql = format!("DELETE FROM {table_name}");
            if let Some(ref filter) = filter_sql {
                let _ = write!(sql, " WHERE {filter}");
            }
            forward_dml_to_executors(registry, &sql).await?;
            RecordBatch::try_new(result_schema, vec![Arc::new(UInt64Array::from(vec![0u64]))])
                .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            dml_count_schema(),
            stream,
        )))
    }
}

// ── DistributedCayenneUpdateExec ──────────────────────────────────────────────

pub struct DistributedCayenneUpdateExec {
    table_name: datafusion::sql::TableReference,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    filter_sql: Option<String>,
    assignments_sql: Vec<(String, String)>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneUpdateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneUpdateExec")
            .field("table_name", &self.table_name.to_string())
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneUpdateExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        filter_sql: Option<String>,
        assignments_sql: Vec<(String, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = ddl_plan_properties(schema);
        Self {
            table_name,
            executor_registry,
            filter_sql,
            assignments_sql,
            input,
            properties,
        }
    }
}

impl DisplayAs for DistributedCayenneUpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneUpdateExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneUpdateExec {
    fn name(&self) -> &'static str {
        "CayenneUpdateExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("CayenneUpdateExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            self.executor_registry.clone(),
            self.filter_sql.clone(),
            self.assignments_sql.clone(),
            input,
        )))
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let executor_registry = self.executor_registry.clone();
        let filter_sql = self.filter_sql.clone();
        let assignments_sql = self.assignments_sql.clone();
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
            let Some(ref registry) = executor_registry else {
                return Err(DataFusionError::Execution(format!(
                    "UPDATE on '{table_name}' cannot be forwarded: no executor registry"
                )));
            };
            if assignments_sql.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "UPDATE on '{table_name}' has no SET assignments"
                )));
            }
            let set_clause = assignments_sql
                .iter()
                .map(|(col, val)| format!("\"{col}\" = {val}"))
                .collect::<Vec<_>>()
                .join(", ");
            let mut sql = format!("UPDATE {table_name} SET {set_clause}");
            if let Some(ref filter) = filter_sql {
                let _ = write!(sql, " WHERE {filter}");
            }
            forward_dml_to_executors(registry, &sql).await?;
            RecordBatch::try_new(result_schema, vec![Arc::new(UInt64Array::from(vec![0u64]))])
                .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            dml_count_schema(),
            stream,
        )))
    }
}

// ── DistributedCayenneInsertExec ──────────────────────────────────────────────

pub struct DistributedCayenneInsertExec {
    table_name: datafusion::sql::TableReference,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneInsertExec")
            .field("table_name", &self.table_name.to_string())
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneInsertExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        ctx: Arc<datafusion::prelude::SessionContext>,
        io_runtime: tokio::runtime::Handle,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = ddl_plan_properties(schema);
        Self {
            table_name,
            executor_registry,
            ctx,
            io_runtime,
            input,
            properties,
        }
    }
}

impl DisplayAs for DistributedCayenneInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CayenneInsertExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneInsertExec {
    fn name(&self) -> &'static str {
        "CayenneInsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("CayenneInsertExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            self.executor_registry.clone(),
            Arc::clone(&self.ctx),
            self.io_runtime.clone(),
            input,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let executor_registry = self.executor_registry.clone();
        let ctx = Arc::clone(&self.ctx);
        let io_runtime = self.io_runtime.clone();
        let input = Arc::clone(&self.input);
        let result_schema = dml_count_schema();
        let task_ctx = context;
        let stream = futures::stream::once(async move {
            let Some(ref registry) = executor_registry else {
                return Err(DataFusionError::Execution(format!(
                    "INSERT on '{table_name}' cannot be forwarded: no executor registry"
                )));
            };
            if registry.flight_sql_clients.read().await.is_empty() {
                return Err(DataFusionError::Execution(format!(
                    "INSERT on '{table_name}' cannot be forwarded: no executors connected"
                )));
            }
            let partition_expr = crate::datafusion::DataFusion::get_table_partition_expr_from_ctx(
                &ctx,
                registry,
                &table_name,
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to resolve partition expression for '{table_name}': {e}"
                ))
            })?;
            let Some(partition_expr) = partition_expr else {
                return Err(DataFusionError::Execution(format!(
                    "INSERT on '{table_name}' cannot be forwarded: table has no partition expression"
                )));
            };
            let input_schema = input.schema();
            let child_stream = input.execute(partition, task_ctx)?;
            let row_count = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let row_counter = Arc::clone(&row_count);
            let counting_stream: std::pin::Pin<
                Box<
                    dyn futures::Stream<
                            Item = std::result::Result<
                                arrow::array::RecordBatch,
                                crate::cluster::partition::write_through::Error,
                            >,
                        > + Send,
                >,
            > = Box::pin(futures::StreamExt::filter_map(
                child_stream,
                move |result| {
                    let counter = Arc::clone(&row_counter);
                    async move {
                        match result {
                        Ok(batch) if batch.num_rows() > 0 => {
                            counter.fetch_add(batch.num_rows() as u64, std::sync::atomic::Ordering::Relaxed);
                            Some(Ok(batch))
                        }
                        Ok(_) => None,
                        Err(e) => Some(Err(crate::cluster::partition::write_through::Error::UpstreamExecution { source: e })),
                    }
                    }
                },
            ));
            crate::cluster::partition::write_through::forward_partitioned_batches(
                registry,
                Arc::clone(&ctx),
                io_runtime,
                &table_name,
                &input_schema,
                counting_stream,
                &[partition_expr],
            )
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to forward INSERT batches for '{table_name}': {e}"
                ))
            })?;
            let total_rows = row_count.load(std::sync::atomic::Ordering::Relaxed);
            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(arrow::array::UInt64Array::from(vec![total_rows]))],
            )
            .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            dml_count_schema(),
            stream,
        )))
    }
}

// ── DistributedCayenneMergeExec ───────────────────────────────────────────────

pub struct DistributedCayenneMergeExec {
    target_table: datafusion::sql::TableReference,
    source_table: datafusion::sql::TableReference,
    on_keys: Vec<(String, String)>,
    original_sql: String,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    ctx: Arc<datafusion::prelude::SessionContext>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneMergeExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneMergeExec")
            .field("target_table", &self.target_table.to_string())
            .field("source_table", &self.source_table.to_string())
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneMergeExec {
    #[must_use]
    pub fn new(
        target_table: datafusion::sql::TableReference,
        source_table: datafusion::sql::TableReference,
        on_keys: Vec<(String, String)>,
        original_sql: String,
        executor_registry: Option<Arc<ExecutorRegistry>>,
        ctx: Arc<datafusion::prelude::SessionContext>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = ddl_plan_properties(schema);
        Self {
            target_table,
            source_table,
            on_keys,
            original_sql,
            executor_registry,
            ctx,
            properties,
        }
    }
}

impl DisplayAs for DistributedCayenneMergeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedCayenneMergeExec: target={}, source={}",
            self.target_table, self.source_table
        )
    }
}

impl ExecutionPlan for DistributedCayenneMergeExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneMergeExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "DistributedCayenneMergeExec has no children".to_string(),
            ));
        }
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let target_table = self.target_table.clone();
        let source_table = self.source_table.clone();
        let on_keys = self.on_keys.clone();
        let original_sql = self.original_sql.clone();
        let executor_registry = self.executor_registry.clone();
        let ctx = Arc::clone(&self.ctx);
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
            let Some(ref registry) = executor_registry else {
                return Err(DataFusionError::Execution(format!(
                    "MERGE on '{target_table}' cannot be forwarded: no executor registry"
                )));
            };
            validate_partition_compatibility(
                registry,
                &ctx,
                &target_table,
                &source_table,
                &on_keys,
            )
            .await?;
            tracing::info!(target = %target_table, source = %source_table, "Distributed MERGE: forwarding to executors");
            forward_dml_to_executors(registry, &original_sql).await?;
            tracing::info!(target = %target_table, "Distributed MERGE: all executors completed");
            RecordBatch::try_new(result_schema, vec![Arc::new(UInt64Array::from(vec![0u64]))])
                .map_err(Into::into)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            dml_count_schema(),
            stream,
        )))
    }
}

async fn validate_partition_compatibility(
    registry: &ExecutorRegistry,
    ctx: &datafusion::prelude::SessionContext,
    target_table: &datafusion::sql::TableReference,
    source_table: &datafusion::sql::TableReference,
    on_keys: &[(String, String)],
) -> DFResult<()> {
    use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Visit, Visitor};
    use datafusion::sql::sqlparser::dialect::GenericDialect;
    use datafusion::sql::sqlparser::parser::Parser;
    use std::ops::ControlFlow;

    let target_partition = crate::datafusion::DataFusion::get_table_partition_expr_from_ctx(
        ctx,
        registry,
        target_table,
    )
    .await?;
    let source_partition = crate::datafusion::DataFusion::get_table_partition_expr_from_ctx(
        ctx,
        registry,
        source_table,
    )
    .await?;

    let Some(target_part) = target_partition else {
        return Err(DataFusionError::Plan(format!(
            "Distributed MERGE requires '{target_table}' to have PARTITION BY configured"
        )));
    };
    let Some(source_part) = source_partition else {
        return Err(DataFusionError::Plan(format!(
            "Distributed MERGE requires '{source_table}' to have PARTITION BY configured"
        )));
    };
    if target_part != source_part {
        return Err(DataFusionError::Plan(format!(
            "Distributed MERGE requires identical partition expressions. Target: '{target_part}', Source: '{source_part}'"
        )));
    }

    struct ColumnCollector {
        columns: Vec<String>,
    }
    impl Visitor for ColumnCollector {
        type Break = ();
        fn pre_visit_expr(&mut self, expr: &SqlExpr) -> ControlFlow<Self::Break> {
            match expr {
                SqlExpr::Identifier(ident) => self.columns.push(ident.value.clone()),
                SqlExpr::CompoundIdentifier(idents) => {
                    if let Some(last) = idents.last() {
                        self.columns.push(last.value.clone());
                    }
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&target_part)
        .map_err(|e| {
            DataFusionError::Plan(format!(
                "Failed to parse partition expression '{target_part}': {e}"
            ))
        })?;
    let sql_expr = parser.parse_expr().map_err(|e| {
        DataFusionError::Plan(format!(
            "Failed to parse partition expression '{target_part}': {e}"
        ))
    })?;
    let mut collector = ColumnCollector {
        columns: Vec::new(),
    };
    let _ = sql_expr.visit(&mut collector);
    let partition_cols = collector.columns;

    let all_covered = !partition_cols.is_empty()
        && partition_cols
            .iter()
            .all(|pc| on_keys.iter().any(|(target_col, _)| target_col == pc));
    if !all_covered {
        return Err(DataFusionError::Plan(format!(
            "Distributed MERGE requires partition column(s) from '{target_part}' in the ON clause"
        )));
    }
    Ok(())
}
