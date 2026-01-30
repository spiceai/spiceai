/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::datafusion::error::find_datafusion_root;
use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
pub use connector_traits::{
    AnyErrorResult, ComponentInitialization, ComponentType, ConnectorAcceleratedTable,
    ConnectorApp, ConnectorComponent, ConnectorDataset, ConnectorFederatedTable, ConnectorParams,
    ConnectorRuntime, DATA_CONNECTOR_REGISTRATIONS, DataConnector, DataConnectorError,
    DataConnectorFactory, DataConnectorRegistration, DataConnectorResult, DatasetHealthMonitor,
    InvalidConfigurationNoSourceSnafu, InvalidConfigurationSnafu, InvalidGlobPatternSnafu,
    MetricSpec, MetricType, MetricsProvider, MetricsProviderComponent, NewDataConnectorResult,
    ObserveMetricCallback, ParameterSpec, Parameters, RefreshMode, StartupOptions,
    UnableToGetReadProviderSnafu, UnableToGetReadWriteProviderSnafu, default_spice_client,
    register_data_connector,
};
use datafusion::common::Column;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::prelude::ident;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::Unparser;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tracing::Level;

pub mod listing;

pub mod abfs;
#[cfg(feature = "debezium")]
pub mod debezium;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod file;

pub mod git;
pub mod github;
pub mod https;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod localpod;
pub mod memory;

pub const ODBC_DATACONNECTOR: &str = "odbc"; // const needs to be accessible when ODBC isn't built
pub mod deferred;
pub mod gcs;
pub mod glue;
pub mod iceberg;
pub mod parameters;
pub mod s3;
pub mod sink;
pub mod spiceai;

static DATA_CONNECTOR_FACTORY_REGISTRY: LazyLock<
    Mutex<HashMap<String, Arc<dyn DataConnectorFactory>>>,
> = LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn register_connector_factory(
    name: &str,
    connector_factory: Arc<dyn DataConnectorFactory>,
) {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    registry.insert(name.to_string(), connector_factory);
}

/// Create a new `DataConnector` by name.
///
/// # Returns
///
/// `None` if the connector for `name` is not registered, otherwise a `Result` containing the result of calling the constructor to create a `DataConnector`.
pub async fn create_new_connector(
    name: &str,
    params: ConnectorParams,
) -> Option<AnyErrorResult<Arc<dyn DataConnector>>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;

    let connector_factory = guard.get(name);

    let factory = connector_factory?;

    let Some(table_name) = params.component.dataset_table_name() else {
        unreachable!("Component is always a dataset at this point")
    };

    if factory
        .reserved_keywords()
        .contains(&table_name.to_ascii_lowercase().as_str())
    {
        return Some(Err(DataConnectorError::UseOfProtectedKeyword {
            dataconnector: name.to_string(),
            keyword: table_name.to_string(),
        }
        .into()));
    }

    if params.unsupported_type_action.is_some() && !factory.supports_unsupported_type_action() {
        return Some(Err(DataConnectorError::UnsupportedTypeAction {
            dataconnector: name.to_string(),
            connector_component: params.component.clone(),
        }
        .into()));
    }

    let result = factory.create(params).await;
    Some(result)
}

pub async fn register_all() {
    for registration in DATA_CONNECTOR_REGISTRATIONS {
        register_connector_factory(registration.name, (registration.constructor)()).await;
    }
}

pub async fn unregister_all() {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    registry.clear();
}
// Gets data from a table provider and returns it as a vector of RecordBatches.
pub async fn get_data(
    ctx: &mut SessionContext,
    table_name: TableReference,
    table_provider: Arc<dyn TableProvider>,
    sql: Option<String>,
    filters: Vec<Expr>,
) -> Result<SendableRecordBatchStream, DataFusionError> {
    let mut df = match sql {
        None => {
            let table_source = Arc::new(DefaultTableSource::new(Arc::clone(&table_provider)));

            // Get the columns so we can add projection to the plan. This
            // converts the plan to federated where the correct dialect is
            // applied
            let schema = table_provider.schema();
            let columns: Vec<Expr> = schema.fields().iter().map(|f| ident(f.name())).collect();

            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .map_err(find_datafusion_root)?
                .project(columns)?
                .build()
                .map_err(find_datafusion_root)?;

            DataFrame::new(ctx.state(), logical_plan)
        }
        Some(sql) => {
            let session = ctx.state();
            let mut plan = session
                .create_logical_plan(&sql)
                .await
                .map_err(find_datafusion_root)?;

            // If the refresh SQL defines a subset of columns to fetch, computed columns such as embeddings
            // are not included automatically, so we verify their presence and add them manually if needed.
            plan = include_computed_columns(plan, &table_provider.schema())?;

            DataFrame::new(session, plan)
        }
    };

    for filter in filters {
        df = df.filter(filter).map_err(find_datafusion_root)?;
    }

    if tracing::enabled!(Level::TRACE)
        && let Ok(explained) = df.clone().explain(false, false)
        && let Ok(explained) = explained.to_string().await
    {
        tracing::trace!("Data refresh plan for {}:\n{}", table_name, explained);
    }

    let sql = Unparser::default()
        .plan_to_sql(df.logical_plan())
        .map_err(find_datafusion_root)?;
    tracing::info!(target: "task_history", sql = %sql, "labels");

    let record_batch_stream = df.execute_stream().await.map_err(find_datafusion_root)?;
    Ok(record_batch_stream)
}

/// Ensures that the associated computed columns (e.g., embeddings) are included
/// in the `LogicalPlan::Projection` node.
/// If any required computed columns are missing, they are automatically added to the projection.
fn include_computed_columns(
    plan: LogicalPlan,
    source_table_schema: &SchemaRef,
) -> DataFusionResult<LogicalPlan> {
    let plan = plan
        .transform_down(|plan| {
            match plan {
                LogicalPlan::Projection(mut proj) => {
                    for (idx, col) in proj.schema.columns().iter().enumerate() {
                        if let Some(computed_columns) = schema_meta_get_computed_columns(
                            source_table_schema.as_ref(),
                            col.name(),
                        ) {
                            for computed_column in computed_columns {
                                if !proj
                                    .schema
                                    .has_column_with_unqualified_name(computed_column.name())
                                {
                                    proj.expr.push(Expr::Column(Column::new(
                                        proj.schema.qualified_field(idx).0.cloned(),
                                        computed_column.name().clone(),
                                    )));
                                }
                            }
                        }
                    }
                    // The Transformed flag is not used, so we always specify it as transformed for simplicity.
                    Ok(Transformed::yes(LogicalPlan::Projection(proj)))
                }
                _ => Ok(Transformed::no(plan)),
            }
        })?
        .data;

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use datafusion_table_providers::UnsupportedTypeAction;
    use tokio::runtime::Handle;
    use tokio::sync::RwLock;

    use super::*;
    use crate::component::dataset::UnsupportedTypeAction as DatasetUnsupportedTypeAction;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::parameters::ConnectorParamsBuilder;
    use crate::secrets::Secrets;
    use std::any::Any;
    use std::future::Future;
    use std::pin::Pin;

    #[tokio::test]
    async fn test_connector_params_builder_unsupported_type_action() {
        // Register a test connector factory
        struct TestConnectorFactory;
        impl DataConnectorFactory for TestConnectorFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn create(
                &self,
                _params: ConnectorParams,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
                Box::pin(async { Ok(Arc::new(TestConnector) as Arc<dyn DataConnector>) })
            }

            fn prefix(&self) -> &'static str {
                "test"
            }

            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
            }

            fn supports_unsupported_type_action(&self) -> bool {
                true
            }
        }

        #[derive(Debug)]
        struct TestConnector;

        #[async_trait]
        impl DataConnector for TestConnector {
            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn read_provider(
                &self,
                _dataset: &dyn ConnectorDataset,
            ) -> DataConnectorResult<Arc<dyn TableProvider>> {
                unimplemented!()
            }
        }

        register_connector_factory("test", Arc::new(TestConnectorFactory)).await;

        // Create a test dataset with unsupported_type_action
        let app = app::AppBuilder::new("test_app").build();
        let rt = crate::Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new("test:test_dataset".to_string(), "test_dataset")
            .expect("Failed to create builder")
            .with_app(Arc::new(app))
            .with_runtime(Arc::new(rt))
            .build()
            .expect("Failed to build dataset");
        dataset.unsupported_type_action = Some(DatasetUnsupportedTypeAction::Ignore);

        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let builder = ConnectorParamsBuilder::new("test".into(), Arc::new(dataset));

        let result = builder.build(secrets, Handle::current()).await;
        assert!(result.is_ok());

        let params = result.expect("failed to build connector params");
        assert_eq!(
            params.unsupported_type_action,
            Some(UnsupportedTypeAction::Ignore),
            "Unsupported type action should be properly set in connector params"
        );
    }
}
