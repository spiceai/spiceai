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

//! Physical execution plans for Cayenne DDL operations.
//!
//! `CayenneCreateTableExec` registers a new table definition in the Cayenne
//! metadata catalog via `metadata_catalog.create_table(...)`, then opens and
//! registers the corresponding `TableProvider` in the `DataFusion` catalog.
//! Depending on the presence of a `PARTITION BY` expression, it constructs
//! either a partitioned `PartitionTableProvider` wrapped in
//! `DeletionTableProviderAdapter` or a non-partitioned provider, with data
//! stored in Vortex columnar format on local filesystem paths managed by the
//! Cayenne catalog provider. S3 Express One Zone applies to the Cayenne
//! accelerator path, not Cayenne DDL catalog storage.
//!
//! `CayenneCreateSchemaExec` registers a schema namespace in the `DataFusion`
//! catalog for Cayenne-backed DDL catalogs.
//!
//! `CayenneDropTableExec` removes a table from both the Cayenne metadata
//! catalog and the `DataFusion` catalog.

use std::any::Any;
use std::fmt;
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::CayenneTableProviderBuilder;
use cayenne::metadata::CreateTableOptions;
use data_components::delete::{DeletionTableProvider, DeletionTableProviderAdapter};
use data_components::flightsql::FlightSqlClient;
use datafusion::catalog::{CatalogProviderList, SchemaProvider};
use datafusion::common::ToDFSchema;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, ExprSchemable};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_table_providers::UnsupportedTypeAction;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use runtime_table_partition::expression::PartitionedBy;
use runtime_table_partition::provider::PartitionTableProvider;

use super::get_cayenne_provider;
use crate::catalogconnector::cayenne::provider::CayenneSchemaProvider;
use crate::cluster::executor_registry::ExecutorRegistry;
use crate::dataaccelerator::cayenne::CayennePartitionCreator;
use crate::dataaccelerator::cayenne::transform_schema_for_vortex;
use crate::datafusion::cayenne_ddl::create_table_if_not_exists;

/// Builds a filesystem-safe partition label for persisted metadata and Hive-style paths.
///
/// Column expressions keep their column name when safe; non-column expressions use
/// a stable generated label (`expr0`).
fn partition_label_for_expr(partition_expr: &Expr) -> String {
    let candidate = match partition_expr {
        Expr::Column(col) => col.name.as_str(),
        _ => "expr0",
    };

    let sanitized: String = candidate
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();

    if sanitized.is_empty() {
        return "expr0".to_string();
    }

    sanitized
}

/// Maps an Arrow [`DataType`] to a SQL type string suitable for DDL forwarding.
///
/// Returns a SQL type that `DataFusion`'s SQL parser can understand in a
/// `CREATE TABLE` statement.
pub(super) fn arrow_datatype_to_sql(dt: &DataType) -> DFResult<String> {
    match dt {
        DataType::Boolean => Ok("BOOLEAN".to_string()),
        DataType::Int8 => Ok("TINYINT".to_string()),
        DataType::Int16 => Ok("SMALLINT".to_string()),
        DataType::Int32 => Ok("INT".to_string()),
        DataType::Int64 => Ok("BIGINT".to_string()),
        DataType::UInt8 => Ok("TINYINT UNSIGNED".to_string()),
        DataType::UInt16 => Ok("SMALLINT UNSIGNED".to_string()),
        DataType::UInt32 => Ok("INT UNSIGNED".to_string()),
        DataType::UInt64 => Ok("BIGINT UNSIGNED".to_string()),
        DataType::Float16 | DataType::Float32 => Ok("FLOAT".to_string()),
        DataType::Float64 => Ok("DOUBLE".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR".to_string()),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok("BYTEA".to_string()),
        DataType::Date32 | DataType::Date64 => Ok("DATE".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("TIME".to_string()),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP".to_string()),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Ok(format!("DECIMAL({p},{s})")),
        DataType::Dictionary(_, value_type) => arrow_datatype_to_sql(value_type.as_ref()),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported Arrow type for forwarded Cayenne DDL: {other}"
        ))),
    }
}

/// Forward a DDL statement (CREATE/DROP TABLE) to all connected executors.
///
/// Succeeds as long as at least one executor processes the statement.
/// DDL is idempotent and self-heals via periodic catalog refresh.
async fn forward_ddl_to_executors(executor_registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    forward_to_executors(executor_registry, sql, false).await
}

/// Forward a DML statement (DELETE/UPDATE) to all connected executors,
/// requiring every executor to succeed (with retries for transient failures).
async fn forward_dml_to_executors(executor_registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    forward_to_executors(executor_registry, sql, true).await
}

/// Maximum number of retry attempts for DML forwarding per executor.
const MAX_DML_RETRIES: u32 = 3;

/// Forward a SQL statement to all connected executors.
///
/// When `require_all` is `false`, succeeds if at least one executor processes
/// the statement. When `true`, **every** executor must succeed; transient
/// failures are retried up to [`MAX_DML_RETRIES`] times with exponential
/// back-off.
async fn forward_to_executors(
    executor_registry: &ExecutorRegistry,
    sql: &str,
    require_all: bool,
) -> DFResult<()> {
    let clients = executor_registry.flight_sql_clients.read().await;
    if clients.is_empty() {
        tracing::debug!(
            sql,
            "Skipping Cayenne DDL forwarding because no executors are connected"
        );
        return Ok(());
    }

    let futures: Vec<_> = clients
        .iter()
        .map(|(executor_id, client)| {
            let client = client.clone();
            let executor_id = executor_id.clone();
            let sql = sql.to_string();
            async move {
                let max_attempts = if require_all { MAX_DML_RETRIES + 1 } else { 1 };
                let mut last_err: Option<String> = None;

                for attempt in 0..max_attempts {
                    if attempt > 0 {
                        let backoff = std::time::Duration::from_millis(100 * 2u64.pow(attempt - 1));
                        tracing::debug!(
                            executor_id,
                            attempt,
                            backoff_ms = backoff.as_millis(),
                            "Retrying DML forward to executor"
                        );
                        tokio::time::sleep(backoff).await;
                    }

                    let result = forward_sql_to_executor(client.clone(), &sql).await;
                    match result {
                        Ok(()) => {
                            if attempt > 0 {
                                tracing::info!(
                                    executor_id,
                                    attempt,
                                    sql,
                                    "DML forwarded to executor after retry"
                                );
                            } else {
                                tracing::debug!(
                                    executor_id,
                                    sql,
                                    "Forwarded Cayenne DDL/DML to executor"
                                );
                            }
                            return (executor_id, Ok(()));
                        }
                        Err(e) => {
                            last_err = Some(e);
                        }
                    }
                }

                let err_msg = last_err.unwrap_or_else(|| "unknown error".to_string());
                tracing::warn!(
                    executor_id,
                    sql,
                    error = %err_msg,
                    attempts = max_attempts,
                    require_all,
                    "Failed to forward Cayenne DDL/DML to executor"
                );
                (executor_id, Err(err_msg))
            }
        })
        .collect();

    // Release the read lock before awaiting the futures.
    drop(clients);

    let results = futures::future::join_all(futures).await;
    let total = results.len();
    let mut failed_executors: Vec<(String, String)> = Vec::new();
    let mut success_count = 0usize;

    for (executor_id, result) in results {
        match result {
            Ok(()) => success_count += 1,
            Err(e) => failed_executors.push((executor_id, e)),
        }
    }

    if require_all && !failed_executors.is_empty() {
        let executor_errors: Vec<String> = failed_executors
            .iter()
            .map(|(id, e)| format!("{id}: {e}"))
            .collect();
        return Err(DataFusionError::Execution(format!(
            "DML forwarding failed on {}/{} executor(s): [{}]. SQL: {}",
            failed_executors.len(),
            total,
            executor_errors.join("; "),
            sql
        )));
    }

    if success_count == 0 && total > 0 {
        return Err(DataFusionError::Execution(format!(
            "Failed to forward Cayenne DDL to any executor: {sql}"
        )));
    }

    Ok(())
}

/// Send a single SQL statement to one executor via `FlightSQL` execute + `do_get`.
pub async fn forward_sql_to_executor(
    mut client: data_components::flightsql::FlightSqlClient,
    sql: &str,
) -> Result<(), String> {
    use futures::StreamExt;

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

/// Creates a result schema for DDL operations (single "result" column).
#[must_use]
pub fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
}

pub async fn forward_ddl_to_executor(
    sql: String,
    mut client: FlightSqlClient,
) -> Result<(), String> {
    use futures::StreamExt;

    let result: Result<(), String> = async {
        let flight_info = client
            .execute(sql.clone(), None)
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
    .await;
    result
}

/// Physical plan for creating a Cayenne table.
///
/// Executes the following steps:
/// 1. Retrieves the [`CayenneCatalogProvider`] from the `DataFusion` catalog.
/// 2. Transforms the Arrow schema for Vortex compatibility.
/// 3. Creates the table via [`CayenneTableProviderBuilder::create`].
/// 4. Registers the resulting [`CayenneTableProvider`] in the schema provider.
///
/// Write-through inserts are handled natively by the `CayenneTableProvider`'s
/// `insert_into()` implementation; no additional wrapping is needed.
pub struct CayenneCreateTableExec {
    table_name: String,
    arrow_schema: Arc<Schema>,
    if_not_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    primary_key: Vec<String>,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    partition_expr: Option<Expr>,
    partition_expr_sql: Option<String>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateTableExec")
            .field("table_name", &self.table_name)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_not_exists", &self.if_not_exists)
            .field("primary_key", &self.primary_key)
            .finish_non_exhaustive()
    }
}

pub struct CayenneCreateTableExecBuilder {
    table_name: String,
    arrow_schema: Arc<Schema>,
    if_not_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    primary_key: Vec<String>,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    partition_expr: Option<Expr>,
    partition_expr_sql: Option<String>,
}

impl CayenneCreateTableExecBuilder {
    pub fn new(
        table_name: String,
        arrow_schema: Arc<Schema>,
        df_catalog_name: String,
        df_schema_name: String,
        primary_key: Vec<String>,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        Self {
            table_name,
            arrow_schema,
            if_not_exists: false,
            df_catalog_name,
            df_schema_name,
            primary_key,
            catalog_list,
            executor_registry: None,
            partition_expr: None,
            partition_expr_sql: None,
        }
    }

    #[must_use]
    pub fn if_not_exists(mut self, if_not_exists: bool) -> Self {
        self.if_not_exists = if_not_exists;
        self
    }

    #[must_use]
    pub fn executor_registry(mut self, executor_registry: Option<Arc<ExecutorRegistry>>) -> Self {
        self.executor_registry = executor_registry;
        self
    }

    #[must_use]
    pub fn partition_expr(mut self, partition_expr: Option<Expr>) -> Self {
        self.partition_expr = partition_expr;
        self
    }

    #[must_use]
    pub fn partition_expr_sql(mut self, partition_expr_sql: Option<String>) -> Self {
        self.partition_expr_sql = partition_expr_sql;
        self
    }

    #[must_use]
    pub fn build(self) -> CayenneCreateTableExec {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        CayenneCreateTableExec {
            table_name: self.table_name,
            arrow_schema: self.arrow_schema,
            if_not_exists: self.if_not_exists,
            df_catalog_name: self.df_catalog_name,
            df_schema_name: self.df_schema_name,
            primary_key: self.primary_key,
            catalog_list: self.catalog_list,
            executor_registry: self.executor_registry,
            partition_expr: self.partition_expr,
            partition_expr_sql: self.partition_expr_sql,
            properties,
        }
    }
}

impl DisplayAs for CayenneCreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneCreateTableExec: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
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
        context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let arrow_schema = Arc::clone(&self.arrow_schema);
        let if_not_exists = self.if_not_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let primary_key = self.primary_key.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = self.executor_registry.clone();
        let partition_expr = self.partition_expr.clone();
        let partition_expr_sql = self.partition_expr_sql.clone();
        let partition_label = partition_expr.as_ref().map(partition_label_for_expr);
        let result_schema = ddl_result_schema();
        let runtime_env = context.runtime_env();

        let stream = futures::stream::once(async move {
            // In distributed mode, at least one executor must be connected
            // before creating Cayenne catalog tables.
            if let Some(ref registry) = executor_registry
                && registry.flight_sql_clients.read().await.is_empty()
            {
                return Err(DataFusionError::Execution(format!(
                    "Failed to create table '{table_name}' in Cayenne catalog '{df_catalog_name}': no executors are currently connected. At least one executor must be connected before creating tables. Ensure an executor is running and connected to the scheduler."
                )));
            }

            // Get the Cayenne catalog provider
            let df_catalog = catalog_list.catalog(&df_catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{df_catalog_name}' not found"))
            })?;

            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{df_catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            // Get catalog references before the async boundary
            let metadata_catalog = Arc::clone(cayenne_provider.metadata_catalog());
            let data_base_path = cayenne_provider.data_base_path().to_string();
            let vortex_config = cayenne_provider.vortex_config().clone();

            // Use namespace-prefixed name for metadata storage
            let metadata_table_name = format!("{df_schema_name}/{table_name}");

            // Check if table already exists via the metadata catalog
            let exists = metadata_catalog
                .get_table(&metadata_table_name)
                .await
                .is_ok();

            if exists {
                if if_not_exists {
                    // Table exists in metadata (e.g. after restart). Ensure it is also
                    // registered in the in-memory DataFusion schema provider immediately
                    // rather than waiting for periodic catalog refresh.
                    let schema_provider =
                        if let Some(s) = cayenne_provider.schema_provider(&df_schema_name) {
                            s
                        } else {
                            let new_schema = Arc::new(CayenneSchemaProvider::new_empty(
                                Arc::clone(&metadata_catalog),
                                df_schema_name.clone(),
                                Arc::clone(&runtime_env),
                            ));
                            cayenne_provider.register_schema_provider(
                                &df_schema_name,
                                Arc::clone(&new_schema) as Arc<dyn SchemaProvider>,
                            )?;
                            Arc::clone(&new_schema) as Arc<dyn SchemaProvider>
                        };

                    // Open and register the existing table provider if not already present.
                    if !schema_provider.table_exist(&table_name) {
                        let builder = CayenneTableProviderBuilder::new(
                            Arc::clone(&metadata_catalog),
                            Arc::clone(&runtime_env),
                        );
                        if let Ok(provider) = builder.open(&metadata_table_name).await {
                            let provider = Arc::new(provider);
                            let deletion_provider: Arc<dyn DeletionTableProvider> = provider;
                            let wrapped_provider: Arc<dyn datafusion::catalog::TableProvider> =
                                Arc::new(DeletionTableProviderAdapter::new(deletion_provider));
                            if let Err(e) =
                                schema_provider.register_table(table_name.clone(), wrapped_provider)
                            {
                                tracing::error!(table_name, error = %e, "Failed to register existing Cayenne table in schema provider");
                            }
                        }
                    }

                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![format!(
                            "Table '{table_name}' already exists"
                        )]))],
                    )?;
                    return Ok(batch);
                }
                return Err(DataFusionError::Execution(format!(
                    "Table '{table_name}' already exists in catalog '{df_catalog_name}'"
                )));
            }

            // Transform schema for Vortex compatibility
            let vortex_schema = transform_schema_for_vortex(
                &arrow_schema,
                UnsupportedTypeAction::Error,
            )
            .map_err(|e| {
                DataFusionError::Execution(format!("Failed to transform schema for Vortex: {e}"))
            })?;

            let table_data_path = format!(
                "{}{metadata_table_name}/",
                data_base_path.trim_end_matches('/').to_string() + "/"
            );

            let vortex_schema = Arc::new(vortex_schema);
            let on_conflict = if primary_key.is_empty() {
                None
            } else {
                Some(OnConflict::Upsert(ColumnReference::new(
                    primary_key.clone(),
                )))
            };

            // Create table options with namespace-prefixed name
            let create_options = CreateTableOptions {
                table_name: metadata_table_name.clone(),
                schema: Arc::clone(&vortex_schema),
                primary_key: primary_key.clone(),
                on_conflict: on_conflict.clone(),
                base_path: table_data_path.clone(),
                partition_column: partition_label.clone(),
                vortex_config: vortex_config.clone(),
            };

            // Register the table in the Cayenne metadata catalog
            let table_id = metadata_catalog
                .create_table(create_options)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create Cayenne table '{table_name}': {e}"
                    ))
                })?;

            // Build the table provider: partitioned or non-partitioned
            let wrapped_provider: Arc<dyn datafusion::catalog::TableProvider> =
                if let Some(ref partition_expr) = partition_expr {
                    let df_schema = vortex_schema.as_ref().clone().to_dfschema()?;
                    let partition_expr_for_error = partition_expr_sql
                        .clone()
                        .unwrap_or_else(|| partition_expr.to_string());
                    partition_expr.to_field(&df_schema).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Invalid PARTITION BY expression '{partition_expr_for_error}': {e}",
                        ))
                    })?;

                    let partition_name = partition_label
                        .clone()
                        .unwrap_or_else(|| partition_label_for_expr(partition_expr));

                    let partition_by = vec![PartitionedBy {
                        name: partition_name,
                        expression: partition_expr.clone(),
                    }];

                    let creator = Arc::new(CayennePartitionCreator::new(
                        metadata_table_name.clone(),
                        PathBuf::from(&table_data_path),
                        partition_by.clone(),
                        Arc::clone(&vortex_schema),
                        Arc::clone(&metadata_catalog),
                        table_id,
                        UnsupportedTypeAction::Error,
                        Vec::new(), // retention_filters
                        None,       // time_retention_filter_builder
                        vortex_config.clone(),
                        None, // object_store_config (local filesystem)
                        primary_key.clone(),
                        on_conflict.clone(),
                        Arc::clone(&runtime_env),
                    ));

                    let partition_provider = PartitionTableProvider::new(
                        creator,
                        partition_by,
                        Arc::clone(&vortex_schema),
                    )
                    .await
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to create partitioned table '{table_name}': {e}"
                        ))
                    })?;

                    let partition_provider = Arc::new(partition_provider);
                    let deletion_provider: Arc<dyn DeletionTableProvider> = partition_provider;
                    Arc::new(DeletionTableProviderAdapter::new(deletion_provider))
                        as Arc<dyn datafusion::catalog::TableProvider>
                } else {
                    // Non-partitioned: open the table we just created
                    let builder = CayenneTableProviderBuilder::new(
                        Arc::clone(&metadata_catalog),
                        Arc::clone(&runtime_env),
                    );
                    let provider = builder.open(&metadata_table_name).await.map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to open Cayenne table '{table_name}': {e}"
                        ))
                    })?;
                    let provider = Arc::new(provider);
                    let deletion_provider: Arc<dyn DeletionTableProvider> = provider;
                    Arc::new(DeletionTableProviderAdapter::new(deletion_provider))
                        as Arc<dyn datafusion::catalog::TableProvider>
                };

            // Ensure the schema exists, creating it on demand if needed
            let schema_provider = if let Some(s) = cayenne_provider.schema_provider(&df_schema_name)
            {
                s
            } else {
                let new_schema = Arc::new(CayenneSchemaProvider::new_empty(
                    Arc::clone(&metadata_catalog),
                    df_schema_name.clone(),
                    Arc::clone(&runtime_env),
                ));
                cayenne_provider.register_schema_provider(
                    &df_schema_name,
                    Arc::clone(&new_schema) as Arc<dyn SchemaProvider>,
                )?;
                Arc::clone(&new_schema) as Arc<dyn SchemaProvider>
            };

            schema_provider.register_table(table_name.clone(), Arc::clone(&wrapped_provider))?;

            // Initialize partition metadata so the scheduler can route queries by partition.
            let table_ref = datafusion::sql::TableReference::full(
                df_catalog_name.clone(),
                df_schema_name.clone(),
                table_name.clone(),
            );
            if let Some(ref pe) = partition_expr
                && let Some(ref registry) = executor_registry
            {
                let expr_sql = partition_expr_sql.clone().unwrap_or_else(|| pe.to_string());
                let pm = registry.federated_partition_manager();
                if let Err(e) = pm.initialize_metadata(&table_ref, vec![expr_sql]).await {
                    tracing::warn!(
                        table = %table_ref,
                        error = %e,
                        "Failed to initialize partition metadata for table"
                    );
                }
            }

            // Forward the CREATE TABLE DDL to executor nodes
            if let Some(ref registry) = executor_registry
                && let Some(ddl_sql) =
                    create_table_if_not_exists(&table_ref, &wrapped_provider).await?
            {
                forward_ddl_to_executors(registry, &ddl_sql).await?;
            }

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "Table '{table_name}' created"
                )]))],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

/// Physical plan for creating a Cayenne schema.
pub struct CayenneCreateSchemaExec {
    schema_name: String,
    if_not_exists: bool,
    df_catalog_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneCreateSchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateSchemaExec")
            .field("schema_name", &self.schema_name)
            .field("if_not_exists", &self.if_not_exists)
            .field("df_catalog_name", &self.df_catalog_name)
            .finish_non_exhaustive()
    }
}

impl CayenneCreateSchemaExec {
    #[must_use]
    pub fn new(
        schema_name: String,
        if_not_exists: bool,
        df_catalog_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
    ) -> Self {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            schema_name,
            if_not_exists,
            df_catalog_name,
            catalog_list,
            properties,
        }
    }
}

impl DisplayAs for CayenneCreateSchemaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneCreateSchemaExec: {}.{}",
            self.df_catalog_name, self.schema_name
        )
    }
}

impl ExecutionPlan for CayenneCreateSchemaExec {
    fn name(&self) -> &'static str {
        "CayenneCreateSchemaExec"
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
        let df_catalog_name = self.df_catalog_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();
        let runtime_env = context.runtime_env();

        let stream = futures::stream::once(async move {
            let df_catalog = catalog_list.catalog(&df_catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{df_catalog_name}' not found"))
            })?;

            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{df_catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            if cayenne_provider.schema_provider(&schema_name).is_some() {
                if if_not_exists {
                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![format!(
                            "Schema '{schema_name}' already exists"
                        )]))],
                    )?;
                    return Ok(batch);
                }

                return Err(DataFusionError::Execution(format!(
                    "Schema '{schema_name}' already exists in catalog '{df_catalog_name}'"
                )));
            }

            let schema_provider = Arc::new(CayenneSchemaProvider::new_empty(
                Arc::clone(cayenne_provider.metadata_catalog()),
                schema_name.clone(),
                runtime_env,
            ));

            cayenne_provider
                .register_schema_provider(
                    &schema_name,
                    Arc::clone(&schema_provider) as Arc<dyn SchemaProvider>,
                )
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create schema '{schema_name}' in catalog '{df_catalog_name}': {e}"
                    ))
                })?;

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "Schema '{schema_name}' created"
                )]))],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

/// Physical plan for dropping a Cayenne table.
pub struct CayenneDropTableExec {
    table_name: String,
    if_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneDropTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneDropTableExec")
            .field("table_name", &self.table_name)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_exists", &self.if_exists)
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
    ) -> Self {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            executor_registry,
            properties,
        }
    }
}

impl DisplayAs for CayenneDropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CayenneDropTableExec: {}.{}.{}",
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
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = self.executor_registry.clone();
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            // In distributed mode, at least one executor must be connected
            // before dropping Cayenne catalog tables.
            if let Some(ref registry) = executor_registry
                && registry.flight_sql_clients.read().await.is_empty()
            {
                return Err(DataFusionError::Execution(format!(
                    "Failed to drop table '{table_name}' from Cayenne catalog '{df_catalog_name}': no executors are currently connected. At least one executor must be connected before modifying tables. Ensure an executor is running and connected to the scheduler."
                )));
            }

            // Get the Cayenne catalog provider
            let df_catalog = catalog_list.catalog(&df_catalog_name).ok_or_else(|| {
                DataFusionError::Execution(format!("Catalog '{df_catalog_name}' not found"))
            })?;

            let cayenne_provider = get_cayenne_provider(df_catalog.as_ref()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Catalog '{df_catalog_name}' is not a Cayenne catalog"
                ))
            })?;

            let metadata_catalog = Arc::clone(cayenne_provider.metadata_catalog());

            // Use namespace-prefixed name for metadata lookup
            let metadata_table_name = format!("{df_schema_name}/{table_name}");

            // Drop from the Cayenne metadata catalog
            let was_dropped = metadata_catalog
                .drop_table(&metadata_table_name)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to drop Cayenne table '{table_name}': {e}"
                    ))
                })?;

            if !was_dropped {
                if if_exists {
                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![format!(
                            "Table '{table_name}' does not exist"
                        )]))],
                    )?;
                    return Ok(batch);
                }
                return Err(DataFusionError::Execution(format!(
                    "Table '{table_name}' does not exist in catalog '{df_catalog_name}'"
                )));
            }

            // Deregister from the `DataFusion` catalog
            if let Some(schema_provider) = df_catalog.schema(&df_schema_name)
                && let Err(err) = schema_provider.deregister_table(&table_name)
            {
                tracing::error!(table_name, error = %err, "Failed to deregister Cayenne table from DataFusion schema provider");
            }

            // Forward the DROP TABLE DDL to executor nodes
            if let Some(ref registry) = executor_registry {
                let ddl_sql = format!(
                    "DROP TABLE IF EXISTS \"{df_catalog_name}\".\"{df_schema_name}\".\"{table_name}\""
                );
                forward_ddl_to_executors(registry, &ddl_sql).await?;
            }

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "Table '{table_name}' dropped"
                )]))],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

/// Physical plan to forward `DELETE` DML operations to Cayenne table across relevant executors in distributed mode.
///
/// Forwards the DELETE statement to all connected executor nodes via `FlightSQL`.
pub struct DistributedCayenneDeleteExec {
    table_name: datafusion::sql::TableReference,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    /// SQL text of the WHERE clause, if any.
    filter_sql: Option<String>,
    /// The child physical plan (produces the delete filter/rows).
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
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
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
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            let Some(ref registry) = executor_registry else {
                return Err(DataFusionError::Execution(format!(
                    "DELETE on '{table_name}' cannot be forwarded: no executor registry available"
                )));
            };
            let mut sql = format!("DELETE FROM {table_name}");
            if let Some(ref filter) = filter_sql {
                let _ = write!(sql, " WHERE {filter}");
            }
            forward_dml_to_executors(registry, &sql).await?;

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "DELETE from '{table_name}' forwarded"
                )]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

/// Physical plan to forward `UPDATE` DML operations to Cayenne table across relevant executors in distributed mode.
///
/// Forwards the UPDATE statement to all connected executor nodes via `FlightSQL`.
pub struct DistributedCayenneUpdateExec {
    table_name: datafusion::sql::TableReference,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    /// SQL text of the WHERE clause, if any.
    filter_sql: Option<String>,
    /// SET assignments as `(column_name, value_sql)` pairs.
    assignments_sql: Vec<(String, String)>,
    /// The child physical plan (produces the update assignments/filter).
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl fmt::Debug for DistributedCayenneUpdateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneUpdateExec")
            .field("table_name", &self.table_name.to_string())
            .field("filter_sql", &self.filter_sql)
            .field("assignments_sql", &self.assignments_sql)
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
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
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
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            if let Some(ref registry) = executor_registry {
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
            }

            RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "UPDATE on '{table_name}' forwarded"
                )]))],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::col;

    use super::partition_label_for_expr;

    #[test]
    fn partition_label_for_expr_uses_column_name_when_safe() {
        let expr = col("order_date");
        assert_eq!(partition_label_for_expr(&expr), "order_date");
    }

    #[test]
    fn partition_label_for_expr_uses_generated_label_for_non_column_expr() {
        let expr = col("id").eq(datafusion::logical_expr::lit(1_i64));
        assert_eq!(partition_label_for_expr(&expr), "expr0");
    }

    #[test]
    fn partition_label_for_expr_sanitizes_unsafe_column_names() {
        let expr = col("tenant/../id");
        assert_eq!(partition_label_for_expr(&expr), "tenant____id");
    }
}
