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
//! `CayenneCreateTableExec` creates a new table in the Cayenne metadata catalog
//! with data stored in S3 Express One Zone via Vortex columnar format.
//!
//! `CayenneCreateSchemaExec` registers a schema namespace in the `DataFusion` catalog
//! for Cayenne-backed DDL catalogs.
//!
//! `CayenneDropTableExec` removes a table from both the Cayenne metadata catalog
//! and the `DataFusion` catalog.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::CayenneTableProviderBuilder;
use cayenne::metadata::CreateTableOptions;
use datafusion::catalog::{CatalogProviderList, SchemaProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_table_providers::UnsupportedTypeAction;
use data_components::delete::{DeletionTableProvider, DeletionTableProviderAdapter};

use super::get_cayenne_provider;
use crate::catalogconnector::cayenne::provider::CayenneSchemaProvider;
use crate::cluster::executor_registry::ExecutorRegistry;
use crate::dataaccelerator::cayenne::transform_schema_for_vortex;

/// Maps an Arrow [`DataType`] to a SQL type string suitable for DDL forwarding.
///
/// Returns a SQL type that `DataFusion`'s SQL parser can understand in a
/// `CREATE TABLE` statement.
fn arrow_datatype_to_sql(dt: &DataType) -> DFResult<String> {
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

/// Forwards a DDL SQL statement to connected executor nodes.
///
/// Iterates over [`ExecutorRegistry::flight_sql_clients`] and sends the SQL
/// via `FlightSqlClient::execute`, then drains returned endpoints with `do_get`
/// so the statement is actually executed on the remote executor.
///
/// Returns an error when at least one executor was targeted but all forwards failed.
async fn forward_ddl_to_executors(executor_registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    let clients = executor_registry.flight_sql_clients.read().await;
    if clients.is_empty() {
        tracing::debug!(
            sql,
            "Skipping Cayenne DDL forwarding because no executors are connected"
        );
        return Ok(());
    }

    let mut success_count = 0usize;
    for (executor_id, client) in clients.iter() {
        let mut client = client.clone();
        let result: Result<(), String> = async {
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
        .await;

        match result {
            Ok(()) => {
                success_count += 1;
                tracing::debug!(executor_id, sql, "Forwarded Cayenne DDL to executor");
            }
            Err(e) => {
                tracing::warn!(executor_id, sql, error = %e, "Failed to forward Cayenne DDL to executor");
            }
        }
    }

    if success_count == 0 {
        return Err(DataFusionError::Execution(format!(
            "Failed to forward Cayenne DDL to any executor: {sql}"
        )));
    }

    Ok(())
}

/// Creates a result schema for DDL operations (single "result" column).
fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
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
    catalog_list: Arc<dyn CatalogProviderList>,
    executor_registry: Option<Arc<ExecutorRegistry>>,
    properties: PlanProperties,
}

impl fmt::Debug for CayenneCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CayenneCreateTableExec")
            .field("table_name", &self.table_name)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_not_exists", &self.if_not_exists)
            .finish_non_exhaustive()
    }
}

impl CayenneCreateTableExec {
    #[must_use]
    pub fn new(
        table_name: String,
        arrow_schema: Arc<Schema>,
        if_not_exists: bool,
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
            arrow_schema,
            if_not_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            executor_registry,
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
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let arrow_schema = Arc::clone(&self.arrow_schema);
        let if_not_exists = self.if_not_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let executor_registry = self.executor_registry.clone();
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
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
                            ));
                            cayenne_provider.register_schema_provider(
                                &df_schema_name,
                                Arc::clone(&new_schema) as Arc<dyn SchemaProvider>,
                            )?;
                            Arc::clone(&new_schema) as Arc<dyn SchemaProvider>
                        };

                    // Open and register the existing table provider if not already present.
                    if !schema_provider.table_exist(&table_name) {
                        let builder =
                            CayenneTableProviderBuilder::new(Arc::clone(&metadata_catalog));
                        if let Ok(provider) = builder.open(&metadata_table_name).await {
                            let provider = Arc::new(provider);
                            let deletion_provider: Arc<dyn DeletionTableProvider> = provider;
                            let wrapped_provider: Arc<dyn datafusion::catalog::TableProvider> =
                                Arc::new(DeletionTableProviderAdapter::new(deletion_provider));
                            if let Err(e) = schema_provider
                                .register_table(table_name.clone(), wrapped_provider)
                            {
                                tracing::error!(table_name, error = %e, "Failed to register existing Cayenne table in schema provider");
                            }
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

            // Create table options with namespace-prefixed name
            let create_options = CreateTableOptions {
                table_name: metadata_table_name.clone(),
                schema: Arc::new(vortex_schema),
                primary_key: Vec::new(),
                on_conflict: None,
                base_path: table_data_path,
                partition_column: None,
                vortex_config,
            };

            // Create the table via Cayenne
            let builder = CayenneTableProviderBuilder::new(Arc::clone(&metadata_catalog));

            let provider = builder.create(create_options).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to create Cayenne table '{table_name}': {e}"
                ))
            })?;

            let provider = Arc::new(provider);
            let deletion_provider: Arc<dyn DeletionTableProvider> = provider;
            let wrapped_provider: Arc<dyn datafusion::catalog::TableProvider> =
                Arc::new(DeletionTableProviderAdapter::new(deletion_provider));

            // Ensure the schema exists, creating it on demand if needed
            let schema_provider = if let Some(s) = cayenne_provider.schema_provider(&df_schema_name)
            {
                s
            } else {
                let new_schema = Arc::new(CayenneSchemaProvider::new_empty(
                    Arc::clone(&metadata_catalog),
                    df_schema_name.clone(),
                ));
                cayenne_provider.register_schema_provider(
                    &df_schema_name,
                    Arc::clone(&new_schema) as Arc<dyn SchemaProvider>,
                )?;
                Arc::clone(&new_schema) as Arc<dyn SchemaProvider>
            };

            schema_provider.register_table(table_name.clone(), wrapped_provider)?;

            // Forward the CREATE TABLE DDL to executor nodes
            if let Some(ref registry) = executor_registry {
                let columns_sql: Vec<String> = arrow_schema
                    .fields()
                    .iter()
                    .map(|f| {
                        let null_str = if f.is_nullable() { "" } else { " NOT NULL" };
                        let sql_type = arrow_datatype_to_sql(f.data_type())?;
                        Ok(format!("\"{}\" {sql_type}{null_str}", f.name()))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                let ddl_sql = format!(
                    "CREATE TABLE IF NOT EXISTS \"{df_catalog_name}\".\"{df_schema_name}\".\"{table_name}\" ({})",
                    columns_sql.join(", ")
                );
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
        _context: Arc<TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let schema_name = self.schema_name.clone();
        let if_not_exists = self.if_not_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();

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
