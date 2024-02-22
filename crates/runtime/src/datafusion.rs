use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::databackend::{self, DataBackendBuilder};
use crate::dataconnector::DataConnector;
use crate::datapublisher::DataPublisher;
use datafusion::datasource::ViewTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use datafusion::sql::parser;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{self, SetExpr, TableFactor};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use futures::StreamExt;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tokio::{spawn, task};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file {file}: {source}"))]
    RegisterParquet {
        source: DataFusionError,
        file: String,
    },

    #[snafu(display("Table already exists"))]
    TableAlreadyExists {},

    #[snafu(display("Unable to get table: {source}"))]
    DatasetConfigurationError { source: databackend::Error },

    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Unable to create dataset acceleration: {source}"))]
    UnableToCreateBackend { source: databackend::Error },

    #[snafu(display("Unable to create view: {reason}"))]
    UnableToCreateView { reason: String },

    #[snafu(display("Unable to delete table: {reason}"))]
    UnableToDeleteTable { reason: String },

    #[snafu(display("Unable to parse SQL: {source}"))]
    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unable to get table: {source}"))]
    UnableToGetTable { source: DataFusionError },

    #[snafu(display("Unable to create view: {source}"))]
    InvalidSQLView {
        source: spicepod::component::dataset::Error,
    },

    #[snafu(display("Expected a SQL view statement, received nothing."))]
    ExpectedSqlView,
}

type PublisherList = Arc<RwLock<Vec<Arc<Box<dyn DataPublisher>>>>>;

type DatasetAndPublishers = (Arc<Dataset>, PublisherList);

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    connectors_tasks: HashMap<String, task::JoinHandle<()>>,
    data_publishers: HashMap<String, DatasetAndPublishers>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        let mut df_config = SessionConfig::new().with_information_schema(true);
        df_config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
        DataFusion {
            ctx: Arc::new(SessionContext::new_with_config(
                SessionConfig::new().with_information_schema(true),
            )),
            connectors_tasks: HashMap::new(),
            data_publishers: HashMap::new(),
        }
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(RegisterParquetSnafu { file: path })
    }

    pub async fn attach_publisher(
        &mut self,
        table_name: &str,
        dataset: Dataset,
        publisher: Arc<Box<dyn DataPublisher>>,
    ) -> Result<()> {
        let entry = self
            .data_publishers
            .entry(table_name.to_string())
            .or_insert_with(|| {
                // If it does not exist, initialize it with the dataset and a new Vec for publishers
                (Arc::new(dataset), Arc::new(RwLock::new(Vec::new())))
            });

        entry.1.write().await.push(publisher);

        Ok(())
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn new_accelerated_backend(
        &self,
        dataset: impl Borrow<Dataset>,
    ) -> Result<Box<dyn DataPublisher>> {
        let dataset = dataset.borrow();
        let table_name = dataset.name.to_string();
        let acceleration =
            dataset
                .acceleration
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration {
                    msg: "No acceleration configuration found".to_string(),
                })?;

        let params: Arc<Option<HashMap<String, String>>> = Arc::new(dataset.params.clone());

        let data_backend: Box<dyn DataPublisher> =
            DataBackendBuilder::new(Arc::clone(&self.ctx), table_name)
                .engine(acceleration.engine())
                .mode(acceleration.mode())
                .params(params)
                .build()
                .context(DatasetConfigurationSnafu)?;

        Ok(data_backend)
    }

    #[must_use]
    #[allow(clippy::borrowed_box)]
    pub fn get_publishers(&self, dataset: &str) -> Option<&DatasetAndPublishers> {
        self.data_publishers.get(dataset)
    }

    #[must_use]
    pub fn has_publishers(&self, dataset: &str) -> bool {
        self.data_publishers.contains_key(dataset)
    }

    pub async fn get_arrow_schema(&self, dataset: &str) -> Result<arrow::datatypes::Schema> {
        let data_frame = self
            .ctx
            .table(dataset)
            .await
            .context(UnableToGetTableSnafu)?;
        Ok(arrow::datatypes::Schema::from(data_frame.schema()))
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn attach_connector_to_publisher(
        &mut self,
        dataset: Dataset,
        data_connector: Box<dyn DataConnector>,
        publisher: Arc<Box<dyn DataPublisher>>,
    ) -> Result<()> {
        let table_name = dataset.name.clone();
        let table_exists = self.ctx.table_exist(table_name.as_str()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let task_handle = task::spawn(async move {
            let dataset = Arc::new(dataset);
            let mut stream = data_connector.get_data(&dataset);
            loop {
                let future_result = stream.next().await;
                match future_result {
                    Some(data_update) => {
                        match publisher.add_data(Arc::clone(&dataset), data_update).await {
                            Ok(()) => (),
                            Err(e) => tracing::error!("Error adding data: {e:?}"),
                        }
                    }
                    None => break,
                };
            }
        });

        self.connectors_tasks.insert(table_name, task_handle);

        Ok(())
    }

    #[must_use]
    pub fn table_exists(&self, dataset_name: &str) -> bool {
        self.ctx.table_exist(dataset_name).unwrap_or(false)
    }

    pub fn remove_table(&mut self, dataset_name: &str) -> Result<()> {
        if !self.ctx.table_exist(dataset_name).unwrap_or(false) {
            return Ok(());
        }

        if let Err(e) = self.ctx.deregister_table(dataset_name) {
            return UnableToDeleteTableSnafu {
                reason: e.to_string(),
            }
            .fail();
        }

        if self.connectors_tasks.contains_key(dataset_name) {
            if let Some(data_connector_stream) = self.connectors_tasks.remove(dataset_name) {
                data_connector_stream.abort();
            }
        }

        if self.data_publishers.contains_key(dataset_name) {
            self.data_publishers.remove(dataset_name);
        }

        Ok(())
    }

    pub fn attach_view(&self, dataset: impl Borrow<Dataset>) -> Result<()> {
        let dataset = dataset.borrow();
        let table_exists = self.ctx.table_exist(dataset.name.as_str()).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let Some(sql) = dataset.view_sql().context(InvalidSQLViewSnafu)? else {
            return ExpectedSqlViewSnafu.fail();
        };
        let statements = DFParser::parse_sql_with_dialect(sql.as_str(), &PostgreSqlDialect {})
            .context(UnableToParseSqlSnafu)?;
        if statements.len() != 1 {
            return UnableToCreateViewSnafu {
                reason: format!(
                    "Expected 1 statement to create view from, received {}",
                    statements.len()
                )
                .to_string(),
            }
            .fail();
        }

        let ctx = self.ctx.clone();
        let table_name = dataset.name.clone();
        spawn(async move {
            // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
            // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
            // the tables are created. To handle this, wait until all tables are created.
            let dependent_table_names = DataFusion::get_dependent_table_names(&statements[0]);
            for dependent_table_name in dependent_table_names {
                let mut attempts = 0;
                loop {
                    if !ctx.table_exist(&dependent_table_name).unwrap_or(false) {
                        if attempts % 10 == 0 {
                            tracing::warn!("Dependent table {dependent_table_name} for view {table_name} does not exist, retrying...");
                        }
                        attempts += 1;
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    break;
                }
            }

            let plan = match ctx.state().statement_to_plan(statements[0].clone()).await {
                Ok(plan) => plan,
                Err(e) => {
                    tracing::error!("Unable to create view: {e:?}");
                    return;
                }
            };

            let view = match ViewTable::try_new(plan, Some(sql.to_string())) {
                Ok(view) => view,
                Err(e) => {
                    tracing::error!("Unable to create view: {e:?}");
                    return;
                }
            };
            if let Err(e) = ctx.register_table(table_name.as_str(), Arc::new(view)) {
                tracing::error!("Unable to create view: {e:?}");
            };

            tracing::info!("Created view {table_name}");
        });

        Ok(())
    }

    fn get_dependent_table_names(statement: &parser::Statement) -> Vec<String> {
        let mut table_names = Vec::new();
        let mut cte_names = HashSet::new();

        if let parser::Statement::Statement(statement) = statement.clone() {
            if let ast::Statement::Query(statement) = *statement {
                // Collect names of CTEs
                if let Some(with) = statement.with {
                    for table in with.cte_tables {
                        cte_names.insert(table.alias.name.to_string());
                        let cte_table_names =
                            DataFusion::get_dependent_table_names(&parser::Statement::Statement(
                                Box::new(ast::Statement::Query(table.query)),
                            ));
                        // Extend table_names with names found in CTEs if they reference actual tables
                        table_names.extend(cte_table_names);
                    }
                }
                // Process the main query body
                if let SetExpr::Select(select_statement) = *statement.body {
                    for from in select_statement.from {
                        let mut relations = vec![];
                        relations.push(from.relation.clone());
                        for join in from.joins {
                            relations.push(join.relation.clone());
                        }

                        for relation in relations {
                            match relation {
                                TableFactor::Table {
                                    name,
                                    alias: _,
                                    args: _,
                                    with_hints: _,
                                    version: _,
                                    partitions: _,
                                } => {
                                    table_names.push(name.to_string());
                                }
                                TableFactor::Derived {
                                    lateral: _,
                                    subquery,
                                    alias: _,
                                } => {
                                    table_names.extend(DataFusion::get_dependent_table_names(
                                        &parser::Statement::Statement(Box::new(
                                            ast::Statement::Query(subquery),
                                        )),
                                    ));
                                }
                                _ => (),
                            }
                        }
                    }
                }
            }
        }

        // Filter out CTEs and temporary views (aliases of subqueries)
        table_names
            .into_iter()
            .filter(|name| !cte_names.contains(name))
            .collect()
    }
}

impl Drop for DataFusion {
    fn drop(&mut self) {
        for task in self.connectors_tasks.values() {
            task.abort();
        }

        self.connectors_tasks.clear();
    }
}

impl Default for DataFusion {
    fn default() -> Self {
        Self::new()
    }
}
