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

use crate::accelerated_table::AcceleratedTable;
use crate::catalogconnector::CATALOG_CONNECTOR_FACTORY_REGISTRY;
use crate::component::catalog::Catalog;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::Dataset;
use crate::datafusion::error::find_datafusion_root;
use crate::federated_table::FederatedTable;
use crate::get_params_with_secrets;
use crate::parameters::ParameterSpec;
use crate::parameters::Parameters;
use crate::secrets::Secrets;
use arrow_schema::SchemaRef;
use arrow_tools::schema::schema_meta_get_computed_columns;
use async_trait::async_trait;
use data_components::cdc::ChangesStream;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::SessionContext;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::TableReference;
use datafusion_table_providers::UnsupportedTypeAction;
use snafu::prelude::*;
use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use std::future::Future;

pub mod listing;

pub mod abfs;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "databricks")]
pub mod databricks;
#[cfg(feature = "debezium")]
pub mod debezium;
#[cfg(feature = "delta_lake")]
pub mod delta_lake;
#[cfg(feature = "dremio")]
pub mod dremio;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod file;
#[cfg(feature = "flightsql")]
pub mod flightsql;
#[cfg(feature = "ftp")]
pub mod ftp;
pub mod github;
pub mod graphql;
pub mod https;
pub mod localpod;
pub mod memory;
#[cfg(feature = "mssql")]
pub mod mssql;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "odbc")]
pub mod odbc;
pub const ODBC_DATACONNECTOR: &str = "odbc"; // const needs to be accessible when ODBC isn't built

#[cfg(feature = "imap")]
pub mod imap;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod s3;
#[cfg(feature = "ftp")]
pub mod sftp;
#[cfg(feature = "sharepoint")]
pub mod sharepoint;
pub mod sink;
#[cfg(feature = "snowflake")]
pub mod snowflake;
#[cfg(feature = "spark")]
pub mod spark;
pub mod spiceai;

#[derive(Debug, Snafu)]
pub enum DataConnectorError {
    #[snafu(display("Cannot connect to the {connector_component} ({dataconnector}).\n{source}"))]
    UnableToConnectInternal {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot connect to the {connector_component} ({dataconnector}) on {host}:{port}.\nEnsure that the host and port are correctly configured in the spicepod, and that the host is reachable."))]
    UnableToConnectInvalidHostOrPort {
        dataconnector: String,
        connector_component: ConnectorComponent,
        host: String,
        port: String,
    },

    #[snafu(display("Cannot connect to the {connector_component} ({dataconnector}). Authentication failed.\nEnsure that the username and password are correctly configured in the spicepod."))]
    UnableToConnectInvalidUsernameOrPassword {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display("Cannot connect to the {connector_component} ({dataconnector}). A TLS error occurred.\nEnsure that the corresponding TLS/secure option is configured to match the data connector's TLS security requirements."))]
    UnableToConnectTlsError {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}).\n{source}"))]
    UnableToGetReadProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}).\n{source}"))]
    UnableToGetReadWriteProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to setup the {connector_component} ({dataconnector}).\n{source}"))]
    UnableToGetCatalogProvider {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The {connector_component} ({dataconnector}) has been rate limited.\n{source}"
    ))]
    RateLimited {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration.\n{message}"))]
    InvalidConfiguration {
        dataconnector: String,
        connector_component: ConnectorComponent,
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration.\n{source}"))]
    InvalidConfigurationSourceOnly {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Cannot setup the {connector_component} ({dataconnector}) with an invalid configuration.\n{message}"))]
    InvalidConfigurationNoSource {
        dataconnector: String,
        connector_component: ConnectorComponent,
        message: String,
    },

    #[snafu(display("Cannot setup the {connector_component} ({dataconnector}).\nThe connector '{dataconnector}' is not a valid connector.\nFor details, visit: https://spiceai.org/docs/components/data-connectors"))]
    InvalidConnectorType {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}).\n An invalid glob pattern was provided '{pattern}'. Ensure the glob pattern is valid.\n{source}"))]
    InvalidGlobPattern {
        dataconnector: String,
        connector_component: ConnectorComponent,
        pattern: String,
        source: globset::Error,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}).\nThe table, '{table_name}', was not found. Verify the source table name in the Spicepod configuration."
    ))]
    InvalidTableName {
        dataconnector: String,
        connector_component: ConnectorComponent,
        table_name: String,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}).\nFailed to detect a table schema. Ensure the table, '{table_name}', exists in the data source."
    ))]
    UnableToGetSchema {
        dataconnector: String,
        connector_component: ConnectorComponent,
        table_name: String,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}).\nAn unknown Data Connector Error occurred: {source}\nReport a bug on GitHub: https://github.com/spiceai/spiceai/issues"))]
    InternalWithSource {
        dataconnector: String,
        connector_component: ConnectorComponent,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}).\nAn internal error occurred in the {dataconnector} Data Connector.\nReport a bug on GitHub (https://github.com/spiceai/spiceai/issues) and reference the code: {code}"
    ))]
    Internal {
        dataconnector: String,
        connector_component: ConnectorComponent,
        code: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to load the {connector_component} ({dataconnector}).\nUnsupported type action is not enabled for the {dataconnector} Data Connector.\nRemove the parameter from your dataset configuration."
    ))]
    UnsupportedTypeAction {
        dataconnector: String,
        connector_component: ConnectorComponent,
    },

    #[snafu(display("Failed to load the {connector_component} ({dataconnector}).\nThe field '{field_name}' has an unsupported data type: {data_type}.\nSkip loading this field by setting the `unsupported_type_action` parameter to `ignore` or `warn` in the dataset configuration.\nFor details, visit: https://spiceai.org/docs/reference/spicepod/datasets#unsupported_type_action"))]
    UnsupportedDataType {
        dataconnector: String,
        connector_component: ConnectorComponent,
        data_type: String,
        field_name: String,
    },

    #[snafu(display("Failed to initialize the {connector_component} (ODBC).\nThe runtime is built without ODBC support.\nBuild Spice.ai OSS with the `odbc` feature enabled or use the Docker image that includes ODBC support.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/odbc"))]
    OdbcNotInstalled {
        connector_component: ConnectorComponent,
    },
}

pub type Result<T, E = DataConnectorError> = std::result::Result<T, E>;
pub type AnyErrorResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type DataConnectorResult<T> = std::result::Result<T, DataConnectorError>;

type NewDataConnectorResult = AnyErrorResult<Arc<dyn DataConnector>>;

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
    register_connector_factory("sink", sink::SinkConnectorFactory::new_arc()).await;
    #[cfg(feature = "databricks")]
    register_connector_factory("databricks", databricks::DatabricksFactory::new_arc()).await;
    #[cfg(feature = "delta_lake")]
    register_connector_factory("delta_lake", delta_lake::DeltaLakeFactory::new_arc()).await;
    #[cfg(feature = "dremio")]
    register_connector_factory("dremio", dremio::DremioFactory::new_arc()).await;
    register_connector_factory("file", file::FileFactory::new_arc()).await;
    #[cfg(feature = "flightsql")]
    register_connector_factory("flightsql", flightsql::FlightSQLFactory::new_arc()).await;
    register_connector_factory("s3", s3::S3Factory::new_arc()).await;
    register_connector_factory("abfs", abfs::AzureBlobFSFactory::new_arc()).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("ftp", ftp::FTPFactory::new_arc()).await;
    #[cfg(feature = "imap")]
    register_connector_factory("imap", imap::ImapFactory::new_arc()).await;
    register_connector_factory("http", https::HttpsFactory::new_arc()).await;
    register_connector_factory("https", https::HttpsFactory::new_arc()).await;
    register_connector_factory("github", github::GithubFactory::new_arc()).await;
    #[cfg(feature = "ftp")]
    register_connector_factory("sftp", sftp::SFTPFactory::new_arc()).await;
    register_connector_factory("spice.ai", spiceai::SpiceAIFactory::new_arc()).await;
    register_connector_factory("memory", memory::MemoryConnectorFactory::new_arc()).await;
    #[cfg(feature = "mssql")]
    register_connector_factory("mssql", mssql::SqlServerFactory::new_arc()).await;
    #[cfg(feature = "mysql")]
    register_connector_factory("mysql", mysql::MySQLFactory::new_arc()).await;
    #[cfg(feature = "postgres")]
    register_connector_factory("postgres", postgres::PostgresFactory::new_arc()).await;
    #[cfg(feature = "duckdb")]
    register_connector_factory("duckdb", duckdb::DuckDBFactory::new_arc()).await;
    #[cfg(feature = "clickhouse")]
    register_connector_factory("clickhouse", clickhouse::ClickhouseFactory::new_arc()).await;
    register_connector_factory("graphql", graphql::GraphQLFactory::new_arc()).await;
    #[cfg(feature = "odbc")]
    register_connector_factory("odbc", odbc::ODBCFactory::new_arc()).await;
    #[cfg(feature = "sharepoint")]
    register_connector_factory("sharepoint", sharepoint::SharepointFactory::new_arc()).await;
    #[cfg(feature = "spark")]
    register_connector_factory("spark", spark::SparkFactory::new_arc()).await;
    #[cfg(feature = "snowflake")]
    register_connector_factory("snowflake", snowflake::SnowflakeFactory::new_arc()).await;
    #[cfg(feature = "debezium")]
    register_connector_factory("debezium", debezium::DebeziumFactory::new_arc()).await;
    register_connector_factory("localpod", localpod::LocalPodFactory::new_arc()).await;
    #[cfg(feature = "dynamodb")]
    register_connector_factory("dynamodb", dynamodb::DynamoDBFactory::new_arc()).await;
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
}

/// A `DataConnector` knows how to retrieve and optionally write or stream data.
#[async_trait]
pub trait DataConnector: Send + Sync {
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

    fn changes_stream(&self, _federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        None
    }

    fn supports_append_stream(&self) -> bool {
        false
    }

    fn append_stream(&self, _federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        None
    }

    async fn metadata_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        None
    }

    /// A hook that is called when an accelerated table is registered to the
    /// DataFusion context for this data connector.
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
            let logical_plan = LogicalPlanBuilder::scan(table_name.clone(), table_source, None)
                .map_err(find_datafusion_root)?
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

    let sql = Unparser::default()
        .plan_to_sql(df.logical_plan())
        .map_err(find_datafusion_root)?;
    tracing::info!(target: "task_history", sql = %sql, "labels");

    let record_batch_stream = df.execute_stream().await.map_err(find_datafusion_root)?;
    Ok(record_batch_stream)
}

#[derive(Debug, Clone)]
pub enum ConnectorComponent {
    Catalog(Arc<Catalog>),
    Dataset(Arc<Dataset>),
}

impl From<&Dataset> for ConnectorComponent {
    fn from(dataset: &Dataset) -> Self {
        ConnectorComponent::Dataset(Arc::new(dataset.clone()))
    }
}

impl From<&Arc<Dataset>> for ConnectorComponent {
    fn from(dataset: &Arc<Dataset>) -> Self {
        ConnectorComponent::Dataset(Arc::clone(dataset))
    }
}

impl From<&Catalog> for ConnectorComponent {
    fn from(catalog: &Catalog) -> Self {
        ConnectorComponent::Catalog(Arc::new(catalog.clone()))
    }
}

impl std::fmt::Display for ConnectorComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorComponent::Catalog(catalog) => write!(f, "catalog {}", catalog.name),
            ConnectorComponent::Dataset(dataset) => write!(f, "dataset {}", dataset.name),
        }
    }
}

pub struct ConnectorParams {
    pub(crate) parameters: Parameters,
    pub(crate) unsupported_type_action: Option<UnsupportedTypeAction>,
    pub(crate) component: ConnectorComponent,
}

pub struct ConnectorParamsBuilder {
    connector: Arc<str>,
    component: ConnectorComponent,
}

impl ConnectorParamsBuilder {
    #[must_use]
    pub fn new(connector: Arc<str>, component: ConnectorComponent) -> Self {
        Self {
            connector,
            component,
        }
    }

    pub async fn build(
        &self,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<ConnectorParams, Box<dyn std::error::Error + Send + Sync>> {
        let name = self.connector.to_string();
        let mut unsupported_type_action = None;
        let (params, prefix, parameters) = match &self.component {
            ConnectorComponent::Catalog(catalog) => {
                let guard = CATALOG_CONNECTOR_FACTORY_REGISTRY.lock().await;
                let connector_factory = guard.get(&name);

                let factory =
                    connector_factory.ok_or_else(|| DataConnectorError::InvalidConnectorType {
                        dataconnector: name.clone(),
                        connector_component: self.component.clone(),
                    })?;

                (
                    get_params_with_secrets(Arc::clone(&secrets), &catalog.params).await,
                    factory.prefix(),
                    factory.parameters(),
                )
            }
            ConnectorComponent::Dataset(dataset) => {
                let guard = DATA_CONNECTOR_FACTORY_REGISTRY.lock().await;
                let connector_factory = guard.get(&name);

                unsupported_type_action = dataset.unsupported_type_action;

                let factory = connector_factory.ok_or_else(|| {
                    if name == ODBC_DATACONNECTOR {
                        DataConnectorError::OdbcNotInstalled {
                            connector_component: self.component.clone(),
                        }
                    } else {
                        DataConnectorError::InvalidConnectorType {
                            dataconnector: name.clone(),
                            connector_component: self.component.clone(),
                        }
                    }
                })?;

                let params = get_params_with_secrets(Arc::clone(&secrets), &dataset.params).await;

                (params, factory.prefix(), factory.parameters())
            }
        };

        let parameters = Parameters::try_new(
            &format!("connector {name}"),
            params.into_iter().collect(),
            prefix,
            secrets,
            parameters,
        )
        .await?;

        Ok(ConnectorParams {
            parameters,
            unsupported_type_action: unsupported_type_action.map(UnsupportedTypeAction::from),
            component: self.component.clone(),
        })
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
                                        computed_column.name().to_string(),
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
    use super::*;
    use crate::component::dataset::UnsupportedTypeAction as DatasetUnsupportedTypeAction;

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
        let mut dataset = Dataset::try_new("test:test_dataset".to_string(), "test_dataset")
            .expect("failed to create dataset");
        dataset.unsupported_type_action = Some(DatasetUnsupportedTypeAction::Ignore);

        let secrets = Arc::new(RwLock::new(Secrets::default()));
        let builder = ConnectorParamsBuilder::new(
            "test".into(),
            ConnectorComponent::Dataset(Arc::new(dataset)),
        );

        let result = builder.build(secrets).await;
        assert!(result.is_ok());

        let params = result.expect("failed to build connector params");
        assert_eq!(
            params.unsupported_type_action,
            Some(UnsupportedTypeAction::Ignore),
            "Unsupported type action should be properly set in connector params"
        );
    }
}
