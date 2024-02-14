use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::databackend::{self, DataBackend};
use crate::dataconnector::DataConnector;
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
use tokio::time::sleep;
use tokio::{spawn, task};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file {}", file))]
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

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    tasks: Vec<task::JoinHandle<()>>,
    backends: HashMap<String, Arc<Box<dyn DataBackend>>>,
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
            tasks: Vec::new(),
            backends: HashMap::new(),
        }
    }

    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(RegisterParquetSnafu { file: path })
    }

    pub fn attach_backend(
        &mut self,
        table_name: &str,
        backend: Box<dyn DataBackend>,
    ) -> Result<()> {
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        self.backends
            .insert(table_name.to_string(), Arc::new(backend));

        Ok(())
    }

    pub fn new_backend(&self, dataset: &Dataset) -> Result<Box<dyn DataBackend>> {
        let table_name = dataset.name.as_str();
        let acceleration =
            dataset
                .acceleration
                .as_ref()
                .ok_or_else(|| Error::InvalidConfiguration {
                    msg: "No acceleration configuration found".to_string(),
                })?;

        let params: Arc<Option<HashMap<String, String>>> = Arc::new(dataset.params.clone());

        let data_backend: Box<dyn DataBackend> = <dyn DataBackend>::new(
            &self.ctx,
            table_name,
            acceleration.engine(),
            acceleration.mode(),
            params,
        )
        .context(DatasetConfigurationSnafu)?;

        Ok(data_backend)
    }

    #[must_use]
    #[allow(clippy::borrowed_box)]
    pub fn get_backend(&self, dataset: &str) -> Option<&Arc<Box<dyn DataBackend>>> {
        self.backends.get(dataset)
    }

    #[must_use]
    pub fn has_backend(&self, dataset: &str) -> bool {
        self.backends.contains_key(dataset)
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
    pub fn attach(
        &mut self,
        dataset: &Dataset,
        data_connector: &'static mut dyn DataConnector,
        backend: Box<dyn DataBackend>,
    ) -> Result<()> {
        let table_name = dataset.name.as_str();
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let dataset_clone = dataset.clone();
        let task_handle = task::spawn(async move {
            let mut stream = data_connector.get_data(&dataset_clone);
            loop {
                let future_result = stream.next().await;
                match future_result {
                    Some(data_update) => match backend.add_data(data_update).await {
                        Ok(()) => (),
                        Err(e) => tracing::error!("Error adding data: {e:?}"),
                    },
                    None => break,
                };
            }
        });

        self.tasks.push(task_handle);

        Ok(())
    }

    pub fn attach_view(&self, dataset: &Dataset) -> Result<()> {
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
                            tracing::error!("Dependent table {dependent_table_name} for {table_name} does not exist, retrying...");
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
        let mut with_to_tables = HashMap::new();

        match statement.clone() {
            parser::Statement::Statement(statement) => match *statement {
                ast::Statement::Query(statement) => {
                    if let Some(with) = statement.with {
                        for table in with.cte_tables {
                            let with_table_names = DataFusion::get_dependent_table_names(
                                &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                    table.query,
                                ))),
                            );
                            with_to_tables.insert(table.alias.name.to_string(), with_table_names);
                        }
                    };
                    match *statement.body {
                        SetExpr::Select(select_statement) => {
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
                                            table_names.extend(
                                                DataFusion::get_dependent_table_names(
                                                    &parser::Statement::Statement(Box::new(
                                                        ast::Statement::Query(subquery),
                                                    )),
                                                ),
                                            );
                                        }
                                        _ => (),
                                    }
                                }
                            }
                        }
                        _ => {
                            return table_names;
                        }
                    }
                }
                _ => {
                    return table_names;
                }
            },
            _ => {
                return table_names;
            }
        }

        // Filter out CTE names if they're not also referenced outside their definition
        let mut result = vec![];

        for table in table_names {
            let included_in_with_table =
                with_to_tables.get(&table).map_or(false, |v| !v.is_empty());
            if !included_in_with_table {
                result.push(table);
            } else if let Some(dependencies) = with_to_tables.get(&table) {
                // Ensure dependencies of a CTE are still considered if they reference other, non-CTE tables
                for dependency in dependencies {
                    if !with_to_tables.contains_key(dependency) {
                        result.push(dependency.clone());
                    }
                }
            }
        }

        result
    }
}

impl Drop for DataFusion {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl Default for DataFusion {
    fn default() -> Self {
        Self::new()
    }
}
