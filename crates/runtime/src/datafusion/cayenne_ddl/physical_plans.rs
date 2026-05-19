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
//! **DDL (broadcast)**: `DistributedCayenneCreateTableExec`, `DistributedCayenneDropTableExec`,
//! and `DistributedCayenneCreateSchemaExec` call the single-node
//! `cayenne::ddl::operations::*` functions, then forward the DDL to all connected
//! executor nodes. `CayenneMergeExec` is re-exported directly from
//! `cayenne::ddl::physical_plans` — local merge needs no broadcast step.
//!
//! **DML (distributed)**: `DistributedCayenneDeleteExec`, `DistributedCayenneUpdateExec`,
//! `DistributedCayenneInsertExec`, `DistributedCayenneMergeExec` forward DML SQL
//! verbatim to all connected executor nodes via `FlightSQL`.

use std::any::Any;
use std::fmt;
use std::fmt::Write as _;
use std::sync::Arc;

use super::get_cayenne_provider;
use crate::cluster::ExecutorRegistry;
use arrow::array::{RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::ddl::operations::{self, create_schema, create_table, drop_table};
use datafusion::catalog::CatalogProviderList;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_ddl::DdlStatement;
use datafusion_ddl::arrow_datatype_to_sql;
use datafusion_ddl::handler::CreateTableParams as DdlCreateTableParams;
use futures::StreamExt;

// Re-export single-node merge exec (no broadcast needed for local merge).
pub use cayenne::ddl::physical_plans::CayenneMergeExec;

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
    let clients = registry.flight_sql_clients_snapshot().await;
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
    let flight_info = client
        .execute(sql.to_string(), None)
        .await
        .map_err(|e| e.to_string())?;

    for endpoint in flight_info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };
        let mut stream = client.do_get(ticket).await.map_err(|e| e.to_string())?;
        while let Some(batch) = stream.next().await {
            batch.map_err(|e| e.to_string())?;
        }
    }

    Ok(())
}

// ── DistributedCayenneCreateTableExec ────────────────────────────────────────

/// Broadcast physical plan for `CREATE TABLE` on a Cayenne catalog.
///
/// 1. Checks that at least one executor is connected (distributed mode guard).
/// 2. Calls [`create_table`] to register in metadata + `DataFusion`.
/// 3. Initialises partition metadata on the scheduler.
/// 4. Forwards the `CREATE TABLE` DDL SQL to all executor nodes.
/// 5. Copies partition-to-executor assignments for `LIKE` tables.
pub struct DistributedCayenneCreateTableExec {
    params: operations::CreateTableParams,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Arc<ExecutorRegistry>,
    // Stashed for DDL SQL construction when forwarding to executors.
    arrow_schema_for_fwd: Arc<arrow::datatypes::Schema>,
    primary_key_for_fwd: Vec<String>,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneCreateTableExec")
            .field("table_name", &self.params.table_name)
            .field("catalog_name", &self.params.catalog_name)
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneCreateTableExec {
    #[must_use]
    pub fn new(
        params: operations::CreateTableParams,
        catalog_list: Arc<dyn CatalogProviderList>,
        executor_registry: Arc<ExecutorRegistry>,
        runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> Self {
        let arrow_schema_for_fwd = Arc::clone(&params.arrow_schema);
        let primary_key_for_fwd = params.primary_key.clone();
        let schema = ddl_result_schema();
        Self {
            params,
            catalog_list,
            executor_registry,
            arrow_schema_for_fwd,
            primary_key_for_fwd,
            runtime_env,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for DistributedCayenneCreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedCayenneCreateTableExec: {}.{}.{}",
            self.params.catalog_name, self.params.schema_name, self.params.table_name
        )?;
        if let Some(ref source) = self.params.like_source_table {
            write!(f, " (LIKE {source})")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for DistributedCayenneCreateTableExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneCreateTableExec"
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let arrow_schema_fwd = Arc::clone(&self.arrow_schema_for_fwd);
        let primary_key_fwd = self.primary_key_for_fwd.clone();
        let runtime_env = Arc::clone(&self.runtime_env);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            // 1. Executor connectivity guard.
            if !executor_registry.has_flight_sql_clients().await {
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
            let _ = create_table(
                operations::CreateTableParams {
                    table_name: table_name.clone(),
                    schema_name: schema_name.clone(),
                    catalog_name: catalog_name.clone(),
                    arrow_schema,
                    primary_key,
                    partition_expr_sql: partition_expr_sql.clone(),
                    if_not_exists,
                    like_source_table: like_source_table.clone(),
                    ctx: ctx_opt,
                },
                cayenne_provider,
                Arc::clone(&runtime_env),
            )
            .await?;

            let table_ref = datafusion::sql::TableReference::full(
                catalog_name.clone(),
                schema_name.clone(),
                table_name.clone(),
            );

            // 3. Initialise partition metadata so the scheduler can route queries.
            if let Some(expr_sql) = partition_expr_sql.clone() {
                let pm = executor_registry.federated_partition_store();
                if let Err(e) = pm.initialize_metadata(&table_ref, vec![expr_sql]).await {
                    tracing::warn!(
                        table = %table_ref,
                        error = %e,
                        "Failed to initialize partition metadata"
                    );
                }
            }

            // 4. Build and forward CREATE TABLE DDL SQL.
            // Intentionally omit PARTITION BY when forwarding to executors:
            // the scheduler owns distributed partition metadata/routing, while
            // executors store plain local Cayenne tables for the assigned rows.
            {
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

                executor_registry
                    .ddl_log()
                    .append(DdlStatement::CreateTable {
                        params: Box::new(DdlCreateTableParams {
                            catalog_name: catalog_name.clone(),
                            schema_name: schema_name.clone(),
                            table_name: table_name.clone(),
                            arrow_schema: Arc::clone(&arrow_schema_fwd),
                            primary_key: primary_key_fwd.clone(),
                            extension: datafusion_ddl::CreateTableStatementExtension::default(),
                            if_not_exists,
                            or_replace: false,
                            like_source_table: like_source_table.clone(),
                        }),
                        sql: ddl_sql.clone(),
                    })
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to append DDL to cluster log: {e}"
                        ))
                    })?;
                forward_ddl_to_executors(&executor_registry, &ddl_sql).await?;
            }

            // 5. Copy partition assignments for LIKE tables.
            let like_detail = if let Some(ref source) = like_source_table {
                use crate::cluster::partition::CopyAssignmentsResult;
                let pm = executor_registry.federated_partition_store();
                tracing::info!(
                    source = %source,
                    target = %table_ref,
                    "Copying partition assignments from LIKE source table"
                );
                match pm.copy_assignments(source, &table_ref).await {
                    Ok(CopyAssignmentsResult::Copied { partition_count }) => {
                        tracing::info!(
                            source = %source,
                            target = %table_ref,
                            partition_count,
                            "Partition assignments copied from LIKE source table"
                        );
                        Some(format!(
                            "LIKE '{source}': schema, partition expression, \
                             partition assignments copied"
                        ))
                    }
                    Ok(CopyAssignmentsResult::NoAssignments) => {
                        tracing::info!(
                            source = %source,
                            target = %table_ref,
                            "Source table has no partition assignments to copy"
                        );
                        Some(format!(
                            "LIKE '{source}': schema and partition expression copied; \
                             no partition assignments to copy"
                        ))
                    }
                    Ok(CopyAssignmentsResult::NoSourceMetadata) => {
                        tracing::info!(
                            source = %source,
                            target = %table_ref,
                            "Source table has no partition metadata"
                        );
                        Some(format!("LIKE '{source}': schema copied"))
                    }
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!(
                            "Failed to create table '{table_name}': could not copy partition \
                             assignments from source table {source}: {e}"
                        )));
                    }
                }
            } else {
                None
            };

            let result_msg = if let Some(detail) = like_detail {
                format!("Table '{table_name}' created ({detail})")
            } else {
                format!("Table '{table_name}' created")
            };

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![result_msg]))],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

// ── DistributedCayenneDropTableExec ──────────────────────────────────────────

/// Broadcast physical plan for `DROP TABLE` on a Cayenne catalog.
pub struct DistributedCayenneDropTableExec {
    table_name: String,
    if_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Arc<ExecutorRegistry>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneDropTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneDropTableExec")
            .field("table_name", &self.table_name)
            .field("catalog_name", &self.df_catalog_name)
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneDropTableExec {
    #[must_use]
    pub fn new(
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        executor_registry: Arc<ExecutorRegistry>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            executor_registry,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for DistributedCayenneDropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedCayenneDropTableExec: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }
}

impl ExecutionPlan for DistributedCayenneDropTableExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneDropTableExec"
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            // Executor connectivity guard.
            if !executor_registry.has_flight_sql_clients().await {
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

            // Forward DROP TABLE to executors.
            if outcome.message.contains("dropped") {
                let ddl_sql = format!(
                    "DROP TABLE IF EXISTS \
                     \"{catalog_name}\".\"{schema_name}\".\"{table_name}\""
                );
                executor_registry
                    .ddl_log()
                    .drop_table(
                        catalog_name.clone(),
                        schema_name.clone(),
                        table_name.clone(),
                    )
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to append DDL to cluster log: {e}"
                        ))
                    })?;
                forward_ddl_to_executors(&executor_registry, &ddl_sql).await?;
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

// ── DistributedCayenneCreateSchemaExec ───────────────────────────────────────

/// Broadcast physical plan for `CREATE SCHEMA` on a Cayenne catalog.
///
/// 1. Calls [`create_schema`] to register the schema in metadata + `DataFusion`.
/// 2. Forwards the `CREATE SCHEMA IF NOT EXISTS` DDL SQL to all executor nodes.
pub struct DistributedCayenneCreateSchemaExec {
    schema_name: String,
    if_not_exists: bool,
    catalog_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Arc<ExecutorRegistry>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneCreateSchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneCreateSchemaExec")
            .field("schema_name", &self.schema_name)
            .field("catalog_name", &self.catalog_name)
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneCreateSchemaExec {
    #[must_use]
    pub fn new(
        schema_name: String,
        if_not_exists: bool,
        catalog_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        executor_registry: Arc<ExecutorRegistry>,
    ) -> Self {
        let schema = ddl_result_schema();
        Self {
            schema_name,
            if_not_exists,
            catalog_name,
            catalog_list,
            executor_registry,
            properties: ddl_plan_properties(schema),
        }
    }
}

impl DisplayAs for DistributedCayenneCreateSchemaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedCayenneCreateSchemaExec: {}.{}",
            self.catalog_name, self.schema_name
        )
    }
}

impl ExecutionPlan for DistributedCayenneCreateSchemaExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneCreateSchemaExec"
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
        context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let schema_name = self.schema_name.clone();
        let if_not_exists = self.if_not_exists;
        let catalog_name = self.catalog_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = Arc::clone(&self.executor_registry);
        let result_schema = ddl_result_schema();
        let runtime_env = context.runtime_env();

        let stream = futures::stream::once(async move {
            let df_catalog = catalog_list.catalog(&catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{catalog_name}' not found"))
            })?;
            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            // 1. Core create — metadata catalog + DataFusion registration.
            let message = create_schema(
                &schema_name,
                &catalog_name,
                if_not_exists,
                cayenne_provider,
                runtime_env,
            )?;

            // 2. Forward to executors only when the schema was actually created.
            if message.contains("created") {
                let ddl_sql =
                    format!("CREATE SCHEMA IF NOT EXISTS \"{catalog_name}\".\"{schema_name}\"");
                executor_registry
                    .ddl_log()
                    .create_schema(catalog_name.clone(), schema_name.clone())
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to append DDL to cluster log: {e}"
                        ))
                    })?;
                forward_ddl_to_executors(&executor_registry, &ddl_sql).await?;
            }

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![message]))],
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
    executor_registry: Arc<ExecutorRegistry>,
    filter_sql: Option<String>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneDeleteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneDeleteExec")
            .field("table_name", &self.table_name.to_string())
            .field("filter_sql", &self.filter_sql)
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneDeleteExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Arc<ExecutorRegistry>,
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
        write!(f, "DistributedCayenneDeleteExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneDeleteExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneDeleteExec"
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
            DataFusionError::Internal(
                "DistributedCayenneDeleteExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            Arc::clone(&self.executor_registry),
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let filter_sql = self.filter_sql.clone();
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
            let mut sql = format!("DELETE FROM {table_name}");
            if let Some(ref filter) = filter_sql {
                let _ = write!(sql, " WHERE {filter}");
            }
            forward_dml_to_executors(&executor_registry, &sql).await?;
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
    executor_registry: Arc<ExecutorRegistry>,
    filter_sql: Option<String>,
    assignments_sql: Vec<(String, String)>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneUpdateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneUpdateExec")
            .field("table_name", &self.table_name.to_string())
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneUpdateExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Arc<ExecutorRegistry>,
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
        write!(f, "DistributedCayenneUpdateExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneUpdateExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneUpdateExec"
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
            DataFusionError::Internal(
                "DistributedCayenneUpdateExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            Arc::clone(&self.executor_registry),
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let filter_sql = self.filter_sql.clone();
        let assignments_sql = self.assignments_sql.clone();
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
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
            forward_dml_to_executors(&executor_registry, &sql).await?;
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
    executor_registry: Arc<ExecutorRegistry>,
    ctx: Arc<datafusion::prelude::SessionContext>,
    io_runtime: tokio::runtime::Handle,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedCayenneInsertExec")
            .field("table_name", &self.table_name.to_string())
            .finish_non_exhaustive()
    }
}

impl DistributedCayenneInsertExec {
    #[must_use]
    pub fn new(
        table_name: datafusion::sql::TableReference,
        executor_registry: Arc<ExecutorRegistry>,
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
        write!(f, "DistributedCayenneInsertExec: {}", self.table_name)
    }
}

impl ExecutionPlan for DistributedCayenneInsertExec {
    fn name(&self) -> &'static str {
        "DistributedCayenneInsertExec"
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
            DataFusionError::Internal(
                "DistributedCayenneInsertExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            Arc::clone(&self.executor_registry),
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let ctx = Arc::clone(&self.ctx);
        let io_runtime = self.io_runtime.clone();
        let input = Arc::clone(&self.input);
        let result_schema = dml_count_schema();
        let task_ctx = context;
        let stream = futures::stream::once(async move {
            if !executor_registry.has_flight_sql_clients().await {
                return Err(DataFusionError::Execution(format!(
                    "INSERT on '{table_name}' cannot be forwarded: no executors connected"
                )));
            }
            let partition_expr = crate::datafusion::DataFusion::get_table_partition_expr_from_ctx(
                &ctx,
                &executor_registry,
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
                &executor_registry,
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
    executor_registry: Arc<ExecutorRegistry>,
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
        executor_registry: Arc<ExecutorRegistry>,
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
        let executor_registry = Arc::clone(&self.executor_registry);
        let ctx = Arc::clone(&self.ctx);
        let result_schema = dml_count_schema();
        let stream = futures::stream::once(async move {
            validate_partition_compatibility(
                &executor_registry,
                &ctx,
                &target_table,
                &source_table,
                &on_keys,
            )
            .await?;
            tracing::info!(target = %target_table, source = %source_table, "Distributed MERGE: forwarding to executors");
            forward_dml_to_executors(&executor_registry, &original_sql).await?;
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

    let target_part_sql_expr: SqlExpr = match (source_partition, target_partition) {
        (None, _) => {
            return Err(DataFusionError::Plan(format!(
                "Distributed MERGE requires '{source_table}' to have PARTITION BY configured"
            )));
        }
        (_, None) => {
            return Err(DataFusionError::Plan(format!(
                "Distributed MERGE requires '{target_table}' to have PARTITION BY configured"
            )));
        }
        (Some(source_part), Some(target_part)) if source_part != target_part => {
            return Err(DataFusionError::Plan(format!(
                "Distributed MERGE requires identical partition expressions. Target: '{target_part}', Source: '{source_part}'"
            )));
        }
        (Some(_), Some(target_part)) => {
            let dialect = GenericDialect {};
            let mut parser = Parser::new(&dialect)
                .try_with_sql(&target_part)
                .map_err(|e| {
                    DataFusionError::Plan(format!(
                        "Failed to parse partition expression '{target_part}': {e}"
                    ))
                })?;
            parser.parse_expr().map_err(|e| {
                DataFusionError::Plan(format!(
                    "Failed to parse partition expression '{target_part}': {e}"
                ))
            })?
        }
    };

    let mut collector = ColumnCollector {
        columns: Vec::new(),
    };
    let _ = target_part_sql_expr.visit(&mut collector);

    if collector.columns.is_empty()
        || !collector
            .columns
            .iter()
            .all(|pc| on_keys.iter().any(|(target_col, _)| target_col == pc))
    {
        return Err(DataFusionError::Plan(format!(
            "Distributed MERGE requires partition column(s) from '{target_part_sql_expr}' in the ON clause",
        )));
    }
    Ok(())
}
