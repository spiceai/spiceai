/*
Copyright 2024-2025, Spice AI, Inc.

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

//! Physical execution plans for Iceberg DDL operations.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as _;
use std::sync::{Arc, Weak};

use app::App;
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::TableReference;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use spicepod::acceleration::Acceleration;

use super::acceleration_options::DatasetOptions;
use crate::accelerated_table::AcceleratedTable;
use crate::cluster::ExecutorRegistry;
use crate::datafusion::DataFusion;
use crate::datafusion::composed_catalog::ComposedCatalogProvider;
use data_components::RefreshableCatalogProvider;
use data_components::iceberg::provider::IcebergCatalogProvider;
use datafusion::catalog::CatalogProviderList;

use crate::component::dataset::acceleration::{Acceleration as RuntimeAcceleration, Mode};
use crate::dataaccelerator::AccelerationSource;

/// Creates a result schema for DDL operations (single "result" column).
fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
}

#[derive(Debug)]
struct IcebergDdlAccelerationSource {
    app: Arc<App>,
    name: TableReference,
    acceleration: RuntimeAcceleration,
    time_column: Option<String>,
}

impl IcebergDdlAccelerationSource {
    fn new(
        app_name: String,
        name: TableReference,
        acceleration: RuntimeAcceleration,
        time_column: Option<String>,
    ) -> Self {
        let app = App {
            name: app_name,
            ..App::default()
        };

        Self {
            app: Arc::new(app),
            name,
            acceleration,
            time_column,
        }
    }
}

impl AccelerationSource for IcebergDdlAccelerationSource {
    fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
        Arc::new(Self {
            app: Arc::clone(&self.app),
            name: self.name.clone(),
            acceleration: self.acceleration.clone(),
            time_column: self.time_column.clone(),
        })
    }

    fn is_file_accelerated(&self) -> bool {
        matches!(self.acceleration.mode, Mode::File | Mode::FileCreate)
    }

    fn app(&self) -> Arc<App> {
        Arc::clone(&self.app)
    }

    fn runtime(&self) -> Arc<crate::Runtime> {
        unreachable!("DDL-created Iceberg acceleration source does not provide a runtime")
    }

    fn acceleration(&self) -> Option<&RuntimeAcceleration> {
        Some(&self.acceleration)
    }

    fn name(&self) -> &TableReference {
        &self.name
    }

    fn time_column(&self) -> Option<&str> {
        self.time_column.as_deref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Physical plan for creating an Iceberg table.
pub struct IcebergCreateTableExec {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    table_name: String,
    arrow_schema: Arc<Schema>,
    if_not_exists: bool,
    _or_replace: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    acceleration: Option<Acceleration>,
    dataset_options: DatasetOptions,
    datafusion: Weak<DataFusion>,
    partition_expr_sql: Option<String>,
    properties: PlanProperties,
}

/// Physical plan for creating an Iceberg schema.
pub struct IcebergCreateSchemaExec {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    if_not_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    datafusion: Weak<DataFusion>,
    properties: PlanProperties,
}

impl fmt::Debug for IcebergCreateSchemaExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergCreateSchemaExec")
            .field("namespace", &self.namespace)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_not_exists", &self.if_not_exists)
            .finish_non_exhaustive()
    }
}

impl IcebergCreateSchemaExec {
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        if_not_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        datafusion: Weak<DataFusion>,
    ) -> Self {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            catalog,
            namespace,
            if_not_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            datafusion,
            properties,
        }
    }
}

impl DisplayAs for IcebergCreateSchemaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergCreateSchemaExec: {}.{}",
            self.df_catalog_name, self.df_schema_name
        )
    }
}

impl ExecutionPlan for IcebergCreateSchemaExec {
    fn name(&self) -> &'static str {
        "IcebergCreateSchemaExec"
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
        let catalog = Arc::clone(&self.catalog);
        let namespace = self.namespace.clone();
        let if_not_exists = self.if_not_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();
        let datafusion = Weak::<DataFusion>::clone(&self.datafusion);

        let stream = futures::stream::once(async move {
            let exists = catalog.namespace_exists(&namespace).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to check namespace existence '{}': {e}",
                    namespace.join(".")
                ))
            })?;

            if exists {
                refresh_iceberg_catalog_provider(&catalog_list, &df_catalog_name).await?;

                if if_not_exists {
                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![format!(
                            "Schema '{}' already exists",
                            namespace.join(".")
                        )]))],
                    )?;
                    return Ok(batch);
                }

                return Err(DataFusionError::Execution(format!(
                    "Schema '{}' already exists in catalog '{}'",
                    namespace.join("."),
                    df_catalog_name
                )));
            }

            catalog
                .create_namespace(&namespace, HashMap::new())
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create Iceberg schema '{}': {e}",
                        namespace.join(".")
                    ))
                })?;

            refresh_iceberg_catalog_provider(&catalog_list, &df_catalog_name).await?;

            if let Some(df) = datafusion.upgrade()
                && matches!(
                    df.cluster_config.effective_role(),
                    Some(crate::config::ClusterRole::Scheduler)
                )
                && let Some(registry) = df.executor_registry()
            {
                let forward_sql = build_forwarded_create_schema_sql(
                    &df_catalog_name,
                    &df_schema_name,
                    if_not_exists,
                );
                registry.append_ddl(forward_sql.clone()).await;
                forward_ddl_to_executors(registry, &forward_sql).await?;
            }

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![format!(
                    "Schema '{}' created",
                    namespace.join(".")
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

impl fmt::Debug for IcebergCreateTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergCreateTableExec")
            .field("namespace", &self.namespace)
            .field("table_name", &self.table_name)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_not_exists", &self.if_not_exists)
            .field("acceleration", &self.acceleration.is_some())
            .finish_non_exhaustive()
    }
}

impl IcebergCreateTableExec {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        table_name: String,
        arrow_schema: Arc<Schema>,
        if_not_exists: bool,
        or_replace: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        acceleration: Option<Acceleration>,
        dataset_options: DatasetOptions,
        partition_expr_sql: Option<String>,
        datafusion: Weak<DataFusion>,
    ) -> Self {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            catalog,
            namespace,
            table_name,
            arrow_schema,
            if_not_exists,
            _or_replace: or_replace,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            acceleration,
            dataset_options,
            partition_expr_sql,
            datafusion,
            properties,
        }
    }
}

impl DisplayAs for IcebergCreateTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergCreateTableExec: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }
}

impl ExecutionPlan for IcebergCreateTableExec {
    fn name(&self) -> &'static str {
        "IcebergCreateTableExec"
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
        let catalog = Arc::clone(&self.catalog);
        let namespace = self.namespace.clone();
        let table_name = self.table_name.clone();
        let arrow_schema = Arc::clone(&self.arrow_schema);
        let if_not_exists = self.if_not_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();
        let acceleration = self.acceleration.clone();
        let dataset_options = self.dataset_options.clone();
        let partition_expr_sql = self.partition_expr_sql.clone();
        let datafusion = Weak::<DataFusion>::clone(&self.datafusion);

        let stream = futures::stream::once(async move {
            let namespace_exists = catalog.namespace_exists(&namespace).await.map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to check namespace existence '{}': {e}",
                    namespace.join(".")
                ))
            })?;

            if !namespace_exists {
                return Err(DataFusionError::Execution(format!(
                    "Schema '{}' does not exist in catalog '{}'. Create it first with CREATE SCHEMA.",
                    namespace.join("."),
                    df_catalog_name
                )));
            }

            refresh_iceberg_catalog_provider(&catalog_list, &df_catalog_name).await?;

            let Some(df_catalog) = catalog_list.catalog(&df_catalog_name) else {
                return Err(DataFusionError::Execution(format!(
                    "Catalog '{df_catalog_name}' not found"
                )));
            };
            if df_catalog.schema(&df_schema_name).is_none() {
                return Err(DataFusionError::Execution(format!(
                    "Schema '{df_schema_name}' not found in catalog '{df_catalog_name}'"
                )));
            }

            // Coerce Arrow types to Iceberg-compatible equivalents
            let arrow_schema = Arc::new(super::coerce_arrow_schema_for_iceberg_v2(&arrow_schema));

            // Convert Arrow schema to Iceberg schema
            let iceberg_schema =
                iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(arrow_schema.as_ref())
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to convert Arrow schema to Iceberg schema: {e}"
                        ))
                    })?;

            let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

            // Check if table already exists
            let exists = catalog.table_exists(&table_ident).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to check table existence: {e}"))
            })?;

            if exists {
                if if_not_exists {
                    let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(
                        IcebergTableProvider::try_new(
                            Arc::clone(&catalog),
                            namespace.clone(),
                            table_name.clone(),
                        )
                        .await
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to create table provider for existing Iceberg table: {e}"
                            ))
                        })?,
                    );

                    let Some(df_catalog) = catalog_list.catalog(&df_catalog_name) else {
                        return Err(DataFusionError::Execution(format!(
                            "Catalog '{df_catalog_name}' not found"
                        )));
                    };
                    let Some(schema_provider) = df_catalog.schema(&df_schema_name) else {
                        return Err(DataFusionError::Execution(format!(
                            "Schema '{df_schema_name}' not found in catalog '{df_catalog_name}'"
                        )));
                    };

                    let message;
                    if let Some(accel) = acceleration.as_ref().filter(|accel| accel.enabled) {
                        let wrapped_provider = build_registered_provider(
                            &catalog,
                            namespace.clone(),
                            table_name.clone(),
                            Arc::clone(&provider),
                            &schema_provider,
                            &catalog_list,
                            &df_catalog_name,
                            &df_schema_name,
                            accel,
                            &dataset_options,
                            partition_expr_sql.as_ref(),
                            &datafusion,
                        )
                        .await?;

                        synchronize_distributed_write_through_registration(
                            &datafusion,
                            accel,
                            &df_catalog_name,
                            &df_schema_name,
                            &table_name,
                            arrow_schema.as_ref(),
                            &dataset_options,
                            partition_expr_sql.as_ref(),
                        )
                        .await?;

                        if schema_provider.table_exist(&table_name) {
                            let _ = schema_provider.deregister_table(&table_name);
                        }
                        schema_provider.register_table(table_name.clone(), wrapped_provider)?;
                        message = format!(
                            "Table '{table_name}' already exists and acceleration was registered"
                        );
                    } else {
                        let deletion_provider =
                            data_components::iceberg::delete::IcebergDeletionProvider::new(
                                Arc::clone(&catalog),
                                namespace.clone(),
                                table_name.clone(),
                                provider,
                            );
                        schema_provider
                            .register_table(table_name.clone(), Arc::new(deletion_provider))?;
                        message = format!("Table '{table_name}' already exists");
                    }

                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![message]))],
                    )?;
                    return Ok(batch);
                }
                return Err(DataFusionError::Execution(format!(
                    "Table '{table_name}' already exists in namespace '{}'",
                    namespace.join(".")
                )));
            }

            // Try to derive a table location from the namespace properties.
            // Some catalogs (e.g. AWS Glue) require an explicit `location` when
            // creating tables.  We look up the namespace's `location` property
            // and, if present, derive `{namespace_location}/{table_name}`.
            let table_location: Option<String> = match catalog.get_namespace(&namespace).await {
                Ok(ns) => ns.properties().get("location").map(|loc| {
                    let base = loc.trim_end_matches('/');
                    format!("{base}/{table_name}")
                }),
                Err(e) => {
                    tracing::debug!(
                        "Could not fetch namespace properties for '{}' \
                         (table location will not be set): {e}",
                        namespace.join(".")
                    );
                    None
                }
            };

            // Create the table in the Iceberg catalog
            let table_creation = TableCreation::builder()
                .name(table_name.clone())
                .location_opt(table_location)
                .schema(iceberg_schema)
                .build();

            catalog
                .create_table(&namespace, table_creation)
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Failed to create Iceberg table: {e}"))
                })?;

            // Create an IcebergTableProvider for the new table
            let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(
                IcebergTableProvider::try_new(
                    Arc::clone(&catalog),
                    namespace.clone(),
                    table_name.clone(),
                )
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create table provider for new Iceberg table: {e}"
                    ))
                })?,
            );
            // Register in the DataFusion catalog's schema provider
            let Some(df_catalog) = catalog_list.catalog(&df_catalog_name) else {
                return Err(DataFusionError::Execution(format!(
                    "Catalog '{df_catalog_name}' not found"
                )));
            };
            let Some(schema_provider) = df_catalog.schema(&df_schema_name) else {
                return Err(DataFusionError::Execution(format!(
                    "Schema '{df_schema_name}' not found in catalog '{df_catalog_name}'"
                )));
            };
            let register_raw_provider =
                |raw_provider: Arc<dyn datafusion::datasource::TableProvider>| -> DFResult<()> {
                    let deletion_provider =
                        data_components::iceberg::delete::IcebergDeletionProvider::new(
                            Arc::clone(&catalog),
                            namespace.clone(),
                            table_name.clone(),
                            raw_provider,
                        );
                    let adapted: Arc<dyn datafusion::datasource::TableProvider> =
                        Arc::new(deletion_provider);
                    schema_provider.register_table(table_name.clone(), adapted)?;
                    Ok(())
                };

            let message = if let Some(ref accel) = acceleration
                && accel.enabled
            {
                let wrapped_provider = match build_registered_provider(
                    &catalog,
                    namespace.clone(),
                    table_name.clone(),
                    Arc::clone(&provider),
                    &schema_provider,
                    &catalog_list,
                    &df_catalog_name,
                    &df_schema_name,
                    accel,
                    &dataset_options,
                    partition_expr_sql.as_ref(),
                    &datafusion,
                )
                .await
                {
                    Ok(provider) => provider,
                    Err(e) => {
                        let rollback_error = rollback_created_iceberg_table(
                            &catalog,
                            &namespace,
                            &table_name,
                            Some((&schema_provider, &table_name)),
                            e,
                        )
                        .await;
                        return Err(rollback_error);
                    }
                };

                if let Err(e) = synchronize_distributed_write_through_registration(
                    &datafusion,
                    accel,
                    &df_catalog_name,
                    &df_schema_name,
                    &table_name,
                    arrow_schema.as_ref(),
                    &dataset_options,
                    partition_expr_sql.as_ref(),
                )
                .await
                {
                    let rollback_error = rollback_created_iceberg_table(
                        &catalog,
                        &namespace,
                        &table_name,
                        Some((&schema_provider, &table_name)),
                        e,
                    )
                    .await;
                    return Err(rollback_error);
                }

                if schema_provider.table_exist(&table_name) {
                    let _ = schema_provider.deregister_table(&table_name);
                }
                if let Err(e) = schema_provider.register_table(table_name.clone(), wrapped_provider)
                {
                    let rollback_error = rollback_created_iceberg_table(
                        &catalog,
                        &namespace,
                        &table_name,
                        Some((&schema_provider, &table_name)),
                        e,
                    )
                    .await;
                    return Err(rollback_error);
                }

                format!(
                    "Table '{table_name}' created with acceleration (engine={})",
                    accel.engine.as_deref().unwrap_or("cayenne")
                )
            } else {
                // No acceleration — register raw IcebergTableProvider
                if let Err(e) = register_raw_provider(provider) {
                    let rollback_error = rollback_created_iceberg_table(
                        &catalog,
                        &namespace,
                        &table_name,
                        Some((&schema_provider, &table_name)),
                        e,
                    )
                    .await;
                    return Err(rollback_error);
                }
                format!("Table '{table_name}' created")
            };

            let batch = RecordBatch::try_new(
                result_schema,
                vec![Arc::new(StringArray::from(vec![message]))],
            )?;
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            ddl_result_schema(),
            stream,
        )))
    }
}

/// Create an [`AcceleratedTable`] wrapping an Iceberg table provider.
///
/// This is a streamlined version of `DataFusion::create_accelerated_table` for
/// DDL-created tables. It is typically used with full-refresh mode (the common
/// case for ad-hoc CREATE TABLE) but honors the refresh mode configured in
/// `acceleration`.
async fn create_accelerated_iceberg_table(
    datafusion: &Weak<DataFusion>,
    source_provider: Arc<dyn datafusion::datasource::TableProvider>,
    acceleration: &Acceleration,
    dataset_options: &DatasetOptions,
    dataset_name: TableReference,
    partition_expr_sql: Option<&str>,
) -> Result<AcceleratedTable, DataFusionError> {
    use crate::accelerated_table::refresh::Refresh;
    use crate::component::dataset::TimeFormat;
    use crate::component::dataset::acceleration::RefreshMode;
    use crate::federated_table::FederatedTable;

    let df = datafusion.upgrade().ok_or_else(|| {
        DataFusionError::Execution(
            "DataFusion runtime is no longer available for accelerated table creation".to_string(),
        )
    })?;

    let table_name = dataset_name.table();
    let catalog_name = dataset_name.catalog().unwrap_or_default();
    let schema_name = dataset_name.schema().unwrap_or_default();

    // Convert spicepod Acceleration → runtime Acceleration (parses durations, engine, etc.)
    let mut runtime_accel = RuntimeAcceleration::try_from(acceleration.clone()).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse acceleration settings for table '{table_name}': {e}"
        ))
    })?;

    if runtime_accel.partition_by.is_empty()
        && let Some(partition_expr_sql) = partition_expr_sql
    {
        runtime_accel
            .partition_by
            .push(spicepod::partitioning::PartitionedBy {
                name: "expr0".to_string(),
                expression: partition_expr_sql.to_string(),
            });
    }

    validate_write_through_acceleration(acceleration)?;
    validate_ddl_acceleration_runtime_requirements(acceleration)?;

    let source_string = format!("{catalog_name}.{schema_name}.{table_name}");

    let source_schema = source_provider.schema();
    let federated_source = Arc::new(FederatedTable::new_unchecked(source_provider));

    // Determine refresh mode from the acceleration settings
    let refresh_mode = runtime_accel.refresh_mode.unwrap_or(RefreshMode::Full);

    let ddl_source = IcebergDdlAccelerationSource::new(
        catalog_name.to_string(),
        dataset_name.clone(),
        runtime_accel.clone(),
        dataset_options.time_column.clone(),
    );

    // Create the accelerator engine table (Arrow/DuckDB/SQLite in-memory or file)
    let accelerated_table_provider = df
        .accelerator_engine_registry
        .create_accelerator_table(
            dataset_name.clone(),
            Arc::clone(&source_schema),
            None, // no constraints for DDL tables
            &runtime_accel,
            Arc::new(tokio::sync::RwLock::new(crate::secrets::Secrets::default())),
            Some(&ddl_source),
            Arc::clone(&df.ctx),
        )
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to create acceleration engine table: {e}"))
        })?;

    // Build refresh configuration
    let mut refresh = Refresh::new(refresh_mode);
    if let Some(ref time_column) = dataset_options.time_column {
        refresh = refresh.time_column(time_column.clone());
    }
    if let Some(ref time_format) = dataset_options.time_format {
        refresh = refresh.time_format(TimeFormat::from(time_format.clone()));
    }

    // Build the AcceleratedTable with write-through mode
    let mut builder = AcceleratedTable::builder(
        Arc::clone(&df.runtime_status),
        dataset_name,
        federated_source,
        source_string,
        accelerated_table_provider,
        refresh,
        df.io_runtime.clone(),
    );
    builder.write_through();
    let accelerated_table = builder.build().await.map_err(|e| {
        DataFusionError::Execution(format!("Failed to build accelerated table: {e}"))
    })?;

    Ok(accelerated_table)
}

fn validate_write_through_acceleration(acceleration: &Acceleration) -> Result<(), DataFusionError> {
    let engine = acceleration.engine.as_deref().unwrap_or("cayenne");
    if !engine.eq_ignore_ascii_case("cayenne") {
        return Err(DataFusionError::Plan(format!(
            "Accelerated Iceberg catalog tables currently support only the Cayenne accelerator, got '{engine}'"
        )));
    }
    Ok(())
}

fn validate_ddl_acceleration_runtime_requirements(
    acceleration: &Acceleration,
) -> Result<(), DataFusionError> {
    let requires_runtime = acceleration.params.as_ref().is_some_and(|params| {
        let param_data: HashMap<String, String> = params.as_string_map();

        param_data.contains_key("cayenne_s3_zone_ids")
            || param_data
                .get("cayenne_file_path")
                .is_some_and(|path| path.starts_with("s3://"))
            || param_data.keys().any(|key| key.starts_with("cayenne_s3_"))
    });

    if requires_runtime {
        return Err(DataFusionError::Plan(
            "Accelerated Iceberg DDL tables do not yet support Cayenne S3 configuration because runtime-backed secret expansion is unavailable in this path"
                .to_string(),
        ));
    }

    Ok(())
}

#[expect(clippy::too_many_arguments)]
async fn build_registered_provider(
    _catalog: &Arc<dyn Catalog>,
    _namespace: NamespaceIdent,
    table_name: String,
    raw_provider: Arc<dyn datafusion::datasource::TableProvider>,
    _schema_provider: &Arc<dyn datafusion::catalog::SchemaProvider>,
    _catalog_list: &Arc<dyn CatalogProviderList>,
    df_catalog_name: &str,
    df_schema_name: &str,
    acceleration: &Acceleration,
    dataset_options: &DatasetOptions,
    partition_expr_sql: Option<&String>,
    datafusion: &Weak<DataFusion>,
) -> Result<Arc<dyn datafusion::datasource::TableProvider>, DataFusionError> {
    let accelerated = create_accelerated_iceberg_table(
        datafusion,
        raw_provider,
        acceleration,
        dataset_options,
        TableReference::full(
            df_catalog_name.to_string(),
            df_schema_name.to_string(),
            table_name,
        ),
        partition_expr_sql.map(String::as_str),
    )
    .await?;

    let provider: Arc<dyn datafusion::datasource::TableProvider> = Arc::new(accelerated);

    Ok(provider)
}

async fn initialize_partition_metadata(
    executor_registry: Option<&ExecutorRegistry>,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    partition_expr_sql: Option<&String>,
) {
    if let Some(expr_sql) = partition_expr_sql
        && let Some(registry) = executor_registry
    {
        let table_ref = datafusion::sql::TableReference::full(
            catalog_name.to_string(),
            schema_name.to_string(),
            table_name.to_string(),
        );
        if let Err(error) = registry
            .federated_partition_store()
            .initialize_metadata(&table_ref, vec![expr_sql.clone()])
            .await
        {
            tracing::warn!(table = %table_ref, error = %error, "Failed to initialize partition metadata for table");
        }
    }
}

#[expect(clippy::too_many_arguments)]
async fn synchronize_distributed_write_through_registration(
    datafusion: &Weak<DataFusion>,
    acceleration: &Acceleration,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &Schema,
    dataset_options: &DatasetOptions,
    partition_expr_sql: Option<&String>,
) -> Result<(), DataFusionError> {
    let Some(df) = datafusion.upgrade() else {
        return Ok(());
    };

    initialize_partition_metadata(
        df.executor_registry().map(Arc::as_ref),
        catalog_name,
        schema_name,
        table_name,
        partition_expr_sql,
    )
    .await;

    if matches!(
        df.cluster_config.effective_role(),
        Some(crate::config::ClusterRole::Scheduler)
    ) && let Some(registry) = df.executor_registry()
    {
        let forward_sql = build_forwarded_create_sql(
            catalog_name,
            schema_name,
            table_name,
            arrow_schema,
            acceleration,
            dataset_options,
            partition_expr_sql,
        )?;
        registry.append_ddl(forward_sql.clone()).await;
        forward_ddl_to_executors(registry, &forward_sql).await?;
    }

    Ok(())
}

async fn forward_ddl_to_executors(executor_registry: &ExecutorRegistry, sql: &str) -> DFResult<()> {
    let clients = executor_registry.flight_sql_clients_snapshot().await;
    if clients.is_empty() {
        return Ok(());
    }

    let futures: Vec<_> = clients
        .values()
        .cloned()
        .map(|mut client| {
            let sql = sql.to_string();
            async move {
                let flight_info = client.execute(sql, None).await.map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to forward Iceberg DDL to executor: {e}"
                    ))
                })?;
                for endpoint in flight_info.endpoint {
                    if let Some(ticket) = endpoint.ticket {
                        let mut stream = client.do_get(ticket).await.map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to read Iceberg DDL executor result: {e}"
                            ))
                        })?;
                        while let Some(batch) = futures::StreamExt::next(&mut stream).await {
                            batch.map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Executor Iceberg DDL stream failed: {e}"
                                ))
                            })?;
                        }
                    }
                }
                Ok::<(), DataFusionError>(())
            }
        })
        .collect();

    for result in futures::future::join_all(futures).await {
        result?;
    }

    Ok(())
}

fn build_forwarded_create_schema_sql(
    catalog_name: &str,
    schema_name: &str,
    if_not_exists: bool,
) -> String {
    let if_not_exists_sql = if if_not_exists { " IF NOT EXISTS" } else { "" };
    format!("CREATE SCHEMA{if_not_exists_sql} \"{catalog_name}\".\"{schema_name}\"")
}

fn build_forwarded_create_sql(
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    arrow_schema: &Schema,
    acceleration: &Acceleration,
    dataset_options: &DatasetOptions,
    partition_expr_sql: Option<&String>,
) -> Result<String, DataFusionError> {
    let columns_sql: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let null_str = if field.is_nullable() { "" } else { " NOT NULL" };
            let sql_type = arrow_datatype_to_sql(field.data_type())?;
            Ok(format!("\"{}\" {sql_type}{null_str}", field.name()))
        })
        .collect::<Result<_, DataFusionError>>()?;

    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{catalog_name}\".\"{schema_name}\".\"{table_name}\" ({})",
        columns_sql.join(", ")
    );

    let mut options = Vec::new();
    options.push(format!(
        "\"acceleration.engine\" = '{}'",
        acceleration.engine.as_deref().unwrap_or("cayenne")
    ));
    options.push(format!("\"acceleration.mode\" = '{}'", acceleration.mode));
    if let Some(refresh_mode) = &acceleration.refresh_mode {
        options.push(format!(
            "\"acceleration.refresh_mode\" = '{}'",
            render_refresh_mode(refresh_mode)
        ));
    }
    if let Some(time_column) = &dataset_options.time_column {
        options.push(format!("\"dataset.time_column\" = '{time_column}'"));
    }
    if let Some(time_format) = &dataset_options.time_format {
        options.push(format!("\"dataset.time_format\" = '{time_format}'"));
    }

    if !options.is_empty() {
        let _ = write!(sql, " WITH ({})", options.join(", "));
    }

    if let Some(partition_expr_sql) = partition_expr_sql {
        let _ = write!(sql, " PARTITION BY {partition_expr_sql}");
    }

    Ok(sql)
}

use datafusion_ddl::arrow_datatype_to_sql;

fn render_refresh_mode(mode: &spicepod::acceleration::RefreshMode) -> &'static str {
    match mode {
        spicepod::acceleration::RefreshMode::Full => "full",
        spicepod::acceleration::RefreshMode::Append => "append",
        spicepod::acceleration::RefreshMode::Changes => "changes",
        spicepod::acceleration::RefreshMode::Caching => "caching",
        spicepod::acceleration::RefreshMode::Snapshot => "snapshot",
    }
}

async fn refresh_iceberg_catalog_provider(
    catalog_list: &Arc<dyn CatalogProviderList>,
    df_catalog_name: &str,
) -> DFResult<()> {
    let Some(df_catalog) = catalog_list.catalog(df_catalog_name) else {
        return Err(DataFusionError::Execution(format!(
            "Catalog '{df_catalog_name}' not found"
        )));
    };

    if let Some(iceberg_provider) = df_catalog.as_any().downcast_ref::<IcebergCatalogProvider>() {
        iceberg_provider.refresh().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to refresh Iceberg catalog '{df_catalog_name}': {e}"
            ))
        })?;
        return Ok(());
    }

    if let Some(composed) = df_catalog
        .as_any()
        .downcast_ref::<ComposedCatalogProvider>()
        && let Some(iceberg_provider) = composed
            .external()
            .as_any()
            .downcast_ref::<IcebergCatalogProvider>()
    {
        iceberg_provider.refresh().await.map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to refresh Iceberg catalog '{df_catalog_name}': {e}"
            ))
        })?;
        return Ok(());
    }

    Err(DataFusionError::Execution(format!(
        "Catalog '{df_catalog_name}' is not an Iceberg catalog"
    )))
}

async fn rollback_created_iceberg_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    table_name: &str,
    local_registration: Option<(&Arc<dyn datafusion::catalog::SchemaProvider>, &str)>,
    original_error: DataFusionError,
) -> DataFusionError {
    if let Some((schema_provider, registered_table_name)) = local_registration
        && schema_provider.table_exist(registered_table_name)
    {
        let _ = schema_provider.deregister_table(registered_table_name);
    }

    let table_ident = TableIdent::new(namespace.clone(), table_name.to_string());
    match catalog.drop_table(&table_ident).await {
        Ok(()) => original_error,
        Err(rollback_error) => DataFusionError::Execution(format!(
            "{original_error}; additionally failed to roll back Iceberg table '{}.{}': {rollback_error}",
            namespace.join("."),
            table_name
        )),
    }
}

/// Physical plan for dropping an Iceberg table.
pub struct IcebergDropTableExec {
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    table_name: String,
    if_exists: bool,
    df_catalog_name: String,
    df_schema_name: String,
    catalog_list: Arc<dyn CatalogProviderList>,
    _datafusion: Weak<DataFusion>,
    properties: PlanProperties,
}

impl fmt::Debug for IcebergDropTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergDropTableExec")
            .field("namespace", &self.namespace)
            .field("table_name", &self.table_name)
            .field("df_catalog_name", &self.df_catalog_name)
            .field("df_schema_name", &self.df_schema_name)
            .field("if_exists", &self.if_exists)
            .finish_non_exhaustive()
    }
}

impl IcebergDropTableExec {
    #[must_use]
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        table_name: String,
        if_exists: bool,
        df_catalog_name: String,
        df_schema_name: String,
        catalog_list: Arc<dyn CatalogProviderList>,
        datafusion: Weak<DataFusion>,
    ) -> Self {
        let schema = ddl_result_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            catalog,
            namespace,
            table_name,
            if_exists,
            df_catalog_name,
            df_schema_name,
            catalog_list,
            _datafusion: datafusion,
            properties,
        }
    }
}

impl DisplayAs for IcebergDropTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergDropTableExec: {}.{}.{}",
            self.df_catalog_name, self.df_schema_name, self.table_name
        )
    }
}

impl ExecutionPlan for IcebergDropTableExec {
    fn name(&self) -> &'static str {
        "IcebergDropTableExec"
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
        let catalog = Arc::clone(&self.catalog);
        let namespace = self.namespace.clone();
        let table_name = self.table_name.clone();
        let if_exists = self.if_exists;
        let df_catalog_name = self.df_catalog_name.clone();
        let df_schema_name = self.df_schema_name.clone();
        let catalog_list = Arc::clone(&self.catalog_list);
        let result_schema = ddl_result_schema();

        let stream = futures::stream::once(async move {
            let table_ident = TableIdent::new(namespace.clone(), table_name.clone());

            // Check existence
            let exists = catalog.table_exists(&table_ident).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to check table existence: {e}"))
            })?;

            if !exists {
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
                    "Table '{table_name}' does not exist in namespace '{}'",
                    namespace.join(".")
                )));
            }

            // Drop from Iceberg catalog
            catalog.drop_table(&table_ident).await.map_err(|e| {
                DataFusionError::Execution(format!("Failed to drop Iceberg table: {e}"))
            })?;

            // Deregister from DataFusion catalog after successful Iceberg drop.
            // This preserves consistency if Iceberg drop fails.
            if let Some(df_catalog) = catalog_list.catalog(&df_catalog_name)
                && let Some(schema_provider) = df_catalog.schema(&df_schema_name)
            {
                let _ = schema_provider.deregister_table(&table_name);
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
