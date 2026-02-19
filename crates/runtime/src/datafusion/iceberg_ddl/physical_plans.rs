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
use std::fmt;
use std::sync::{Arc, Weak};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_datafusion::IcebergTableProvider;
use spicepod::acceleration::Acceleration;

use crate::accelerated_table::AcceleratedTable;
use crate::datafusion::DataFusion;
use datafusion::catalog::CatalogProviderList;

/// Creates a result schema for DDL operations (single "result" column).
fn ddl_result_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]))
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
    datafusion: Weak<DataFusion>,
    properties: PlanProperties,
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
        let datafusion = Weak::<DataFusion>::clone(&self.datafusion);

        let stream = futures::stream::once(async move {
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
                    let batch = RecordBatch::try_new(
                        result_schema,
                        vec![Arc::new(StringArray::from(vec![format!(
                            "Table '{table_name}' already exists"
                        )]))],
                    )?;
                    return Ok(batch);
                }
                return Err(DataFusionError::Execution(format!(
                    "Table '{table_name}' already exists in namespace '{}'",
                    namespace.join(".")
                )));
            }

            // Create the table in the Iceberg catalog
            let table_creation = TableCreation::builder()
                .name(table_name.clone())
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
                        Arc::new(data_components::delete::DeletionTableProviderAdapter::new(
                            Arc::new(deletion_provider),
                        ));
                    schema_provider.register_table(table_name.clone(), adapted)?;
                    Ok(())
                };

            let message = if let Some(ref accel) = acceleration
                && accel.enabled
            {
                // Wrap in AcceleratedTable
                match create_accelerated_iceberg_table(
                    &datafusion,
                    Arc::clone(&provider),
                    accel,
                    &df_catalog_name,
                    &df_schema_name,
                    &table_name,
                )
                .await
                {
                    Ok(accel_table) => {
                        schema_provider
                            .register_table(table_name.clone(), Arc::new(accel_table))?;
                        format!(
                            "Table '{table_name}' created with acceleration (engine={})",
                            accel.engine.as_deref().unwrap_or("arrow")
                        )
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to create accelerated table '{table_name}', \
                             falling back to direct Iceberg reads: {e}"
                        );
                        // Fall back to registering the raw provider
                        register_raw_provider(Arc::clone(&provider))?;
                        format!("Table '{table_name}' created (acceleration failed: {e})")
                    }
                }
            } else {
                // No acceleration — register raw IcebergTableProvider
                register_raw_provider(provider)?;
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
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<AcceleratedTable, DataFusionError> {
    use crate::accelerated_table::refresh::Refresh;
    use crate::component::dataset::acceleration::{
        Acceleration as RuntimeAcceleration, RefreshMode,
    };
    use crate::federated_table::FederatedTable;
    use datafusion::common::TableReference;

    let df = datafusion.upgrade().ok_or_else(|| {
        DataFusionError::Execution(
            "DataFusion runtime is no longer available for accelerated table creation".to_string(),
        )
    })?;

    // Convert spicepod Acceleration → runtime Acceleration (parses durations, engine, etc.)
    let runtime_accel = RuntimeAcceleration::try_from(acceleration.clone()).map_err(|e| {
        DataFusionError::Execution(format!(
            "Failed to parse acceleration settings for table '{table_name}': {e}"
        ))
    })?;

    let dataset_name = TableReference::full(
        catalog_name.to_string(),
        schema_name.to_string(),
        table_name.to_string(),
    );
    let source_string = format!("{catalog_name}.{schema_name}.{table_name}");

    let source_schema = source_provider.schema();
    let federated_source = Arc::new(FederatedTable::new_unchecked(source_provider));

    // Determine refresh mode from the acceleration settings
    let refresh_mode = runtime_accel.refresh_mode.unwrap_or(RefreshMode::Full);

    // Create the accelerator engine table (Arrow/DuckDB/SQLite in-memory or file)
    let accelerated_table_provider = df
        .accelerator_engine_registry
        .create_accelerator_table(
            dataset_name.clone(),
            Arc::clone(&source_schema),
            None, // no constraints for DDL tables
            &runtime_accel,
            Arc::new(tokio::sync::RwLock::new(crate::secrets::Secrets::default())),
            None, // no AccelerationSource
            Arc::clone(&df.ctx),
        )
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!("Failed to create acceleration engine table: {e}"))
        })?;

    // Build refresh configuration
    let mut refresh = Refresh::new(refresh_mode);
    if let Some(check_interval) = runtime_accel.refresh_check_interval {
        refresh = refresh.check_interval(check_interval);
    }

    // Build the AcceleratedTable
    let accelerated_table = AcceleratedTable::builder(
        Arc::clone(&df.runtime_status),
        dataset_name,
        federated_source,
        source_string,
        accelerated_table_provider,
        refresh,
        df.io_runtime.clone(),
    )
    .build()
    .await
    .map_err(|e| DataFusionError::Execution(format!("Failed to build accelerated table: {e}")))?;

    Ok(accelerated_table)
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
