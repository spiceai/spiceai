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

use crate::accelerated_table::{self, AcceleratedTable};
use crate::component::ComponentInitialization;
use crate::component::catalog::Catalog;
use crate::component::dataset::Dataset;
use crate::component::dataset::acceleration::RefreshMode;
use crate::datafusion::error::find_datafusion_root;
use crate::federated_table::FederatedTable;
pub use crate::parameters::ParameterSpec;
pub use crate::parameters::Parameters;
use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
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
use linkme::distributed_slice;
pub use parameters::ConnectorParams;
use runtime_metrics::component::MetricsProvider;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tracing::Level;

use std::future::Future;
use std::time::Duration;

pub mod client_identity;
pub mod http_rate_control;
pub mod listing;

/// Creates a default reqwest client with standard Spice settings.
///
/// # Errors
///
/// Returns an error if the client cannot be built.
pub fn default_spice_client(content_type: &'static str) -> reqwest::Result<reqwest::Client> {
    use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};

    let mut headers = HeaderMap::new();
    headers.append(CONTENT_TYPE, HeaderValue::from_static(content_type));

    reqwest::Client::builder()
        .user_agent(util::spiceai_user_agent())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .default_headers(headers)
        .build()
}

#[derive(Clone, Copy)]
pub struct DataConnectorRegistration {
    pub name: &'static str,
    pub constructor: fn() -> Arc<dyn DataConnectorFactory>,
}

impl DataConnectorRegistration {
    pub const fn new(
        name: &'static str,
        constructor: fn() -> Arc<dyn DataConnectorFactory>,
    ) -> Self {
        Self { name, constructor }
    }
}

/// Distributed slice that automatically collects all data connector registrations at link time
/// via the `linkme` crate. Entries are added using the [`register_data_connector!`] macro.
#[distributed_slice]
pub static DATA_CONNECTOR_REGISTRATIONS: [DataConnectorRegistration] = [..];

/// Registers a data connector factory by name.
///
/// This macro creates a constructor function for the specified connector factory type and
/// registers it in the global distributed slice of data connectors. This allows
/// the runtime to discover and instantiate connectors without updating a central registry.
///
/// # Example (simple form)
///
/// ```
/// register_data_connector!("file", FileFactory);
/// ```
///
/// # Example (explicit form)
///
/// ```
/// register_data_connector!(
///     register_file_connector,
///     FILE_CONNECTOR_REGISTRATION,
///     "file",
///     FileFactory
/// );
/// ```
///
/// Using this macro automatically adds the connector to the distributed slice,
/// making it available for discovery by the runtime.
///
/// # Linking (connectors in their own crate)
///
/// The registration this generates is a `#[linkme::distributed_slice]` static, and a static
/// is included only when its crate is actually linked — merely being a Cargo dependency is
/// **not** enough, because the linker drops the unreferenced static. So a connector defined in
/// its own crate (e.g. a `connector-*` crate) must be **force-linked** in every binary/tool that
/// should see it, via `use <crate> as _;`: currently `bin/spiced` (so `register_all()` registers
/// it) and `tools/spicepodschema` (so it appears in the generated schema). Miss that line and the
/// connector silently vanishes from both. Connectors defined inside `runtime` itself need nothing
/// extra, since `runtime` is always linked.
#[macro_export]
macro_rules! register_data_connector {
    ($fn_name:ident, $static_name:ident, $name:expr, $factory:path) => {
        fn $fn_name() -> ::std::sync::Arc<dyn $crate::dataconnector::DataConnectorFactory> {
            <$factory>::new_arc()
        }

        #[linkme::distributed_slice($crate::dataconnector::DATA_CONNECTOR_REGISTRATIONS)]
        pub static $static_name: $crate::dataconnector::DataConnectorRegistration =
            $crate::dataconnector::DataConnectorRegistration::new($name, $fn_name);
    };

    ($name:expr, $factory:ident) => {
        ::paste::paste! {
            $crate::register_data_connector!(
                [<__register_data_connector_fn_ $factory:snake>],
                [<__REGISTER_DATA_CONNECTOR_ $factory:upper>],
                $name,
                $factory
            );
        }
    };
}

// abfs: moved to crates/data-connectors/connector-abfs
// #[deprecated] pub mod abfs;
// adbc: moved to crates/data-connectors/connector-adbc
// #[cfg(feature = "adbc")] pub mod adbc;
// cosmosdb: moved to crates/data-connectors/connector-cosmosdb
// #[cfg(feature = "cosmosdb")] pub mod cosmosdb;
#[cfg(feature = "debezium")]
pub mod cdc_ingest;
#[cfg(feature = "debezium")]
pub mod debezium;
pub mod file;

// git: moved to crates/data-connectors/connector-git
// github: moved to crates/data-connectors/connector-github
pub mod https;
// kafka connector moved to crates/data-connectors/connector-kafka; module kept for debezium sidecar types
#[cfg(feature = "debezium")]
pub mod kafka;
pub mod localpod;
pub mod memory;

pub const ODBC_DATACONNECTOR: &str = "odbc"; // const needs to be accessible when ODBC isn't built
pub mod deferred;
// ducklake: moved to crates/data-connectors/connector-ducklake
// gcs: moved to crates/data-connectors/connector-gcs
// glue: registration moved to crates/data-connectors/connector-glue; module kept for catalog connector
pub mod glue;
pub mod iceberg;
pub mod iceberg_cluster;
pub mod parameters;
pub mod s3;
pub mod schema_projection;
pub mod sink;
// spiceai: registration moved to crates/data-connectors/connector-spiceai; module kept for catalog connector
pub mod spiceai;

// The connector contract — the component configuration a connector is built for
// and the errors it reports — lives in `data-connector-api`, below `runtime`, so
// connector crates can name it without depending on the orchestrator. Re-exported
// here so existing `crate::dataconnector::…` paths keep resolving.
pub use data_connector_api::*;

pub type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

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

/// Look up a registered connector factory by name.
pub async fn get_connector_factory(name: &str) -> Option<Arc<dyn DataConnectorFactory>> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    guard.get(name).map(Arc::clone)
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
    let factory = {
        let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
        Arc::clone(guard.get(name)?)
    };

    let ConnectorComponent::Dataset(ds) = &params.component else {
        unreachable!("Component is always a dataset at this point")
    };

    if factory
        .reserved_keywords()
        .contains(&ds.name.table().to_ascii_lowercase().as_str())
    {
        return Some(Err(DataConnectorError::UseOfProtectedKeyword {
            dataconnector: name.to_string(),
            keyword: ds.name.table().to_string(),
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

/// Names of every registered data connector. Useful for generating helpful
/// "did you mean?" suggestions when a user references an unknown connector.
pub async fn registered_connector_names() -> Vec<String> {
    let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    let mut names: Vec<String> = guard.keys().cloned().collect();
    names.sort();
    names
}

/// Returns the registered connector name whose Levenshtein distance to `name`
/// is lowest (bounded so short typos only match very close names). Routes
/// through [`util::levenshtein::closest_match`] so the "did you mean" UX is
/// consistent with runtime tunables and component-level parameters.
pub async fn suggest_connector(name: &str) -> Option<String> {
    util::levenshtein::closest_match(name, &registered_connector_names().await)
}

pub async fn unregister_all() {
    let mut registry = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
    registry.clear();
}
pub trait DataConnectorFactory: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>>;

    fn supports_unsupported_type_action(&self) -> bool {
        false
    }

    /// The prefix to use for parameters and secrets for this `DataConnector`.
    ///
    /// This prefix is applied to any `ParameterType::Connector` parameters.
    ///
    /// ## Example
    ///
    /// If the prefix is `pg` then the following parameters are accepted:
    ///
    /// - `pg_host` -> `host`
    /// - `pg_port` -> `port`
    ///
    /// The prefix will be stripped from the parameter name before being passed to the data connector.
    fn prefix(&self) -> &'static str;

    /// Returns a list of parameters that the data connector requires to be able to connect to the data source.
    ///
    /// Any parameter provided by a user that isn't in this list will be filtered out and a warning logged.
    fn parameters(&self) -> &'static [ParameterSpec];

    /// Returns a list of keywords that are reserved by the data connector.
    /// Used to ensure that any table name isn't a reserved keyword.
    fn reserved_keywords(&self) -> &'static [&'static str] {
        &[]
    }

    /// Returns a static schema for the given dataset if this connector
    /// **intrinsically** knows the schema from configuration alone
    /// — i.e. without contacting the source and without relying on a
    /// user-declared `columns:` block. (User-declared columns are
    /// handled separately by the runtime's deferral dispatch as a
    /// fallback when this method returns `None`.)
    ///
    /// Called during dataset registration **before** the connector itself
    /// is built (no `create` call is required first). Implementations may
    /// consult `params` (e.g. a configured file format) and `dataset`
    /// (e.g. declared content type, JSON column decomposition) but must
    /// not perform any I/O.
    ///
    /// When `Some(schema)` is returned, the runtime is allowed to register
    /// the dataset using that schema and defer building the connector and
    /// calling [`DataConnector::read_provider`] until the dataset is
    /// actually referenced. The connector is still expected to return a
    /// `TableProvider` whose schema matches on the first `read_provider`
    /// call; mismatches surface at first scan as a hard error rather than
    /// being silently retried (the static schema is configuration, not
    /// source state).
    ///
    /// Default: `None`. Most connectors do not have an intrinsic
    /// configuration-only schema and instead rely on either source
    /// inference or the user-declared `columns:` fallback.
    fn static_schema(&self, _params: &ConnectorParams, _dataset: &Dataset) -> Option<SchemaRef> {
        None
    }
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    /// Resolves the default refresh mode for the data connector.
    ///
    /// Most data connectors should keep this as `RefreshMode::Full`.
    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Full)
    }

    async fn read_provider(&self, dataset: &Dataset)
    -> DataConnectorResult<Arc<dyn TableProvider>>;

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    fn supports_changes_stream(&self) -> bool {
        false
    }

    fn changes_stream(
        &self,
        _federated_table: Arc<FederatedTable>,
        _dataset: &Dataset,
    ) -> Option<ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(&self, _federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        None
    }

    /// Whether this connector can accept durable federated write-back delivery
    /// without risking the silent loss of a committed write.
    ///
    /// Delivery reconciles a committed accelerator row to the source. Emulating
    /// an upsert as a standalone `DELETE` followed by a separate `INSERT` is
    /// **not** safe for a CDC-fed accelerator: the two are distinct source
    /// commits, so the source echoes the `DELETE` back over CDC and erases the
    /// committed row from the accelerator. If the follow-up `INSERT` then fails,
    /// the next delivery pass sees the key as absent, treats the delete as
    /// complete, and clears the marker — the write is gone from both sides with
    /// no error raised.
    ///
    /// Returning `true` therefore asserts that delivery is atomic from the
    /// source's point of view: either a single transaction covering both legs,
    /// or a native conditional upsert (`INSERT … ON CONFLICT DO UPDATE`) that
    /// removes the delete leg entirely for present keys.
    ///
    /// Defaults to `false` — the conservative answer. A connector that has not
    /// been audited for this makes the dataset fail registration with an
    /// actionable error rather than silently risk losing writes.
    ///
    /// **Wrappers must forward this.** Inheriting the default would report a
    /// perfectly safe inner connector as unsafe and reject a valid dataset.
    fn supports_durable_write_back_delivery(&self) -> bool {
        false
    }

    async fn metadata_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    /// Pre-register any object stores this connector needs in order to execute
    /// scans for `dataset` against the supplied `runtime_env`.
    ///
    /// Called on cluster executor startup so that physical plans decoded from
    /// the scheduler can resolve their object stores via
    /// `runtime_env().object_store(url)` even when the per-scan
    /// `parquet_file_reader_factory` (or equivalent) is dropped during proto
    /// round-trip.
    ///
    /// The default implementation is a no-op. Connectors backed by per-table
    /// object stores (object-store-style connectors, Delta on S3/Azure/GCS,
    /// Iceberg, etc.) should override this to register the appropriate stores
    /// using the dataset's already secret-expanded params.
    async fn register_object_stores(
        &self,
        _dataset: &Dataset,
        _runtime_env: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
    ) -> DataConnectorResult<()> {
        Ok(())
    }

    /// A hook called **before** the accelerated table is built, giving the
    /// connector a chance to wrap or replace the accelerator provider on the
    /// [`Builder`](crate::accelerated_table::Builder).
    ///
    /// Any provider set here will be shared with the [`Refresher`] that is
    /// created during [`Builder::build`]. Use this hook instead of
    /// [`on_accelerated_table_registration`](Self::on_accelerated_table_registration)
    /// when the wrapped provider must be visible to the refresh pipeline
    /// (e.g. to recreate indexes after a data refresh).
    async fn on_accelerator_setup(
        &self,
        _dataset: &Dataset,
        _builder: &mut accelerated_table::Builder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// A hook that is called when an accelerated table is registered to the
    /// `DataFusion` context for this data connector.
    ///
    /// Allows running any setup logic specific to the data connector when its
    /// accelerated table is registered, i.e. setting up a file watcher to refresh
    /// the table when the file is updated.
    async fn on_accelerated_table_registration(
        &self,
        _dataset: &Dataset,
        _accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    /// Returns a `MetricsProvider` for the data connector.
    ///
    /// If the data connector does not support metrics, return `None`.
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        None
    }

    /// Returns whether the data connector should be initialized on startup or on trigger.
    fn initialization(&self) -> ComponentInitialization {
        ComponentInitialization::default()
    }

    /// Returns whether the data connector should be initialized on startup or on trigger,
    /// with dataset-specific logic.
    ///
    /// This method allows connectors to make initialization decisions based on the specific
    /// dataset configuration. The default implementation delegates to `initialization()`.
    fn initialization_for_dataset(&self, _dataset: &Dataset) -> ComponentInitialization {
        self.initialization()
    }
}

pub trait MetricsProviderComponent: Debug + Send + Sync + 'static {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>>;
}

impl<T: DataConnector + Debug + 'static> MetricsProviderComponent for T {
    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        self.metrics_provider()
    }
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

impl From<&Dataset> for ConnectorComponent {
    fn from(dataset: &Dataset) -> Self {
        ConnectorComponent::Dataset(Arc::new(dataset.spec.clone()))
    }
}

impl From<&Catalog> for ConnectorComponent {
    fn from(catalog: &Catalog) -> Self {
        ConnectorComponent::Catalog(Arc::new(catalog.spec.clone()))
    }
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
    use datafusion_table_providers::UnsupportedTypeAction;
    use tokio::runtime::Handle;
    use tokio::sync::{Barrier, RwLock};
    use tokio::time::{Duration, timeout};

    use super::*;

    // The closest-match algorithm is exercised in
    // crates/util/src/levenshtein.rs (`test_closest_match_*`). `suggest_connector`
    // / `suggest_catalog_connector` just plumb the registry's name list through
    // it, so no per-call wrapper test is needed here.

    use crate::component::dataset::UnsupportedTypeAction as DatasetUnsupportedTypeAction;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::dataconnector::parameters::ConnectorParamsBuilder;
    use crate::secrets::Secrets;

    async fn build_test_connector_params(
        connector_name: &str,
        dataset_name: &str,
        app: Arc<app::App>,
        runtime: Arc<crate::Runtime>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> ConnectorParams {
        let dataset =
            DatasetBuilder::try_new(format!("{connector_name}:{dataset_name}"), dataset_name)
                .expect("Failed to create builder")
                .with_app(app)
                .with_runtime(runtime)
                .build()
                .expect("Failed to build dataset");

        ConnectorParamsBuilder::for_dataset(connector_name.into(), &dataset)
            .build(secrets, Handle::current())
            .await
            .expect("failed to build connector params")
    }

    #[tokio::test]
    async fn test_static_schema_default_returns_none() {
        // Any factory that doesn't override `static_schema` should return
        // None. This is the contract relied on by the deferred-dataset
        // path: a None return falls back to the eager source-contact
        // registration flow.
        struct DefaultFactory;
        impl DataConnectorFactory for DefaultFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn create(
                &self,
                _params: ConnectorParams,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
                unimplemented!("static_schema must not require create()")
            }
            fn prefix(&self) -> &'static str {
                "default_factory"
            }
            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
            }
        }

        register_connector_factory("default_factory", Arc::new(DefaultFactory)).await;

        let app = Arc::new(app::AppBuilder::new("test_app").build());
        let rt = Arc::new(crate::Runtime::builder().build().await);
        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let dataset = DatasetBuilder::try_new("default_factory:tbl".to_string(), "tbl")
            .expect("Failed to create builder")
            .with_app(Arc::clone(&app))
            .with_runtime(Arc::clone(&rt))
            .build()
            .expect("Failed to build dataset");

        let params = ConnectorParamsBuilder::for_dataset("default_factory".into(), &dataset)
            .build(secrets, Handle::current())
            .await
            .expect("failed to build connector params");

        let factory = DefaultFactory;
        assert!(factory.static_schema(&params, &dataset).is_none());
    }

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
                Box::pin(async {
                    let connector: Arc<dyn DataConnector> = Arc::new(TestConnector);
                    Ok(connector)
                })
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
                _dataset: &Dataset,
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
        let builder = ConnectorParamsBuilder::for_dataset("test".into(), &dataset);

        let result = builder.build(secrets, Handle::current()).await;
        assert!(result.is_ok());

        let params = result.expect("failed to build connector params");
        assert_eq!(
            params.unsupported_type_action,
            Some(UnsupportedTypeAction::Ignore),
            "Unsupported type action should be properly set in connector params"
        );
    }

    #[tokio::test]
    async fn test_create_new_connector_allows_concurrent_factory_initialization() {
        struct ConcurrentConnectorFactory {
            barrier: Arc<Barrier>,
        }

        impl DataConnectorFactory for ConcurrentConnectorFactory {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn create(
                &self,
                _params: ConnectorParams,
            ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
                let barrier = Arc::clone(&self.barrier);

                Box::pin(async move {
                    barrier.wait().await;

                    let connector: Arc<dyn DataConnector> = Arc::new(TestConnector);
                    Ok(connector)
                })
            }

            fn prefix(&self) -> &'static str {
                "test_concurrent"
            }

            fn parameters(&self) -> &'static [ParameterSpec] {
                &[]
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
                _dataset: &Dataset,
            ) -> DataConnectorResult<Arc<dyn TableProvider>> {
                unimplemented!()
            }
        }

        register_connector_factory(
            "test_concurrent",
            Arc::new(ConcurrentConnectorFactory {
                barrier: Arc::new(Barrier::new(2)),
            }),
        )
        .await;

        let app = Arc::new(app::AppBuilder::new("test_app").build());
        let runtime = Arc::new(crate::Runtime::builder().build().await);
        let secrets = Arc::new(RwLock::new(Secrets::default()));

        let params_one = build_test_connector_params(
            "test_concurrent",
            "first",
            Arc::clone(&app),
            Arc::clone(&runtime),
            Arc::clone(&secrets),
        )
        .await;
        let params_two =
            build_test_connector_params("test_concurrent", "second", app, runtime, secrets).await;

        let (result_one, result_two) = timeout(Duration::from_secs(5), async move {
            tokio::join!(
                create_new_connector("test_concurrent", params_one),
                create_new_connector("test_concurrent", params_two),
            )
        })
        .await
        .expect("create_new_connector should not serialize concurrent factory initialization");

        assert!(result_one.is_some(), "first factory should be registered");
        assert!(result_two.is_some(), "second factory should be registered");
        assert!(
            result_one.expect("first factory should exist").is_ok(),
            "first connector should initialize successfully"
        );
        assert!(
            result_two.expect("second factory should exist").is_ok(),
            "second connector should initialize successfully"
        );
    }

    /// A source that advertises safe durable write-back delivery, so a wrapper
    /// that silently inherits the trait default is visible as `false`.
    #[derive(Debug)]
    struct SafeDeliveryConnector;

    #[async_trait]
    impl DataConnector for SafeDeliveryConnector {
        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn read_provider(
            &self,
            _dataset: &Dataset,
        ) -> DataConnectorResult<Arc<dyn TableProvider>> {
            unimplemented!("capability-forwarding test never reads")
        }

        fn supports_durable_write_back_delivery(&self) -> bool {
            true
        }
    }

    /// `supports_durable_write_back_delivery` has a `false` default, so any
    /// wrapper that forgets to forward it silently reports a safe source as
    /// unsafe and the registration gate rejects a valid dataset. That is the
    /// defaulted-no-op wrapper bug (#10460) applied to this capability, and it
    /// compiles cleanly — only a test catches it.
    ///
    /// `ElasticsearchFullTextConnector` forwards the same one-liner but is
    /// behind the `elasticsearch` feature and needs a params-bearing dataset to
    /// construct, so it is not exercised here.
    #[test]
    fn every_wrapper_forwards_durable_write_back_delivery_support() {
        let inner: Arc<dyn DataConnector> = Arc::new(SafeDeliveryConnector);
        assert!(
            inner.supports_durable_write_back_delivery(),
            "precondition: the wrapped source advertises safe delivery"
        );

        let deferred = crate::dataconnector::deferred::DeferredConnector::new(Arc::clone(&inner));
        assert!(
            deferred.supports_durable_write_back_delivery(),
            "DeferredConnector must forward the source's delivery capability"
        );

        let full_text =
            crate::search::full_text::connector::FullTextConnector::new(Arc::clone(&inner));
        assert!(
            full_text.supports_durable_write_back_delivery(),
            "FullTextConnector must forward the source's delivery capability"
        );

        let embedding = crate::embeddings::connector::EmbeddingConnector::new(
            Arc::clone(&inner),
            Arc::new(RwLock::new(std::collections::HashMap::new())),
            Arc::new(RwLock::new(Secrets::default())),
        );
        assert!(
            embedding.supports_durable_write_back_delivery(),
            "EmbeddingConnector must forward the source's delivery capability"
        );

        let drasi = crate::drasi::connector::DrasiConnector::new(
            Arc::clone(&inner),
            crate::drasi::DeliveryMode::Acknowledged(Arc::new(
                runtime_drasi::DrasiSink::try_new(runtime_drasi::DrasiSinkConfig {
                    dataset: "test".to_string(),
                    source_id: "test".to_string(),
                    mapping: runtime_drasi::ElementMapping::new("test".to_string(), vec!["test".to_string()]),
                    // Never connected to: building the sink only builds a client.
                    transport: runtime_drasi::TransportConfig::Http {
                        endpoint: url::Url::parse("http://127.0.0.1:1").expect("valid url"),
                        request_timeout: Duration::from_secs(1),
                    },
                    on_delivery_error: runtime_drasi::OnDeliveryError::Block,
                })
                .expect("builds a sink"),
            )),
        );
        assert!(
            drasi.supports_durable_write_back_delivery(),
            "DrasiConnector must forward the source's delivery capability"
        );
    }
}
