use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::databackend::{DataBackend, DataBackendType};
use crate::datasource::DataSource;
use datafusion::datasource::ViewTable;
use datafusion::error::DataFusionError;
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use datafusion::sql::parser;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::ast::{self, SetExpr, TableFactor};
use datafusion::sql::sqlparser::dialect::AnsiDialect;
use futures::StreamExt;
use snafu::prelude::*;
use spicepod::component::dataset::Dataset;
use tokio::task;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file {}", file))]
    RegisterParquet {
        source: DataFusionError,
        file: String,
    },

    DataFusion {
        source: DataFusionError,
    },

    UnableToCreateBackend {
        source: crate::databackend::Error,
    },

    TableAlreadyExists {},

    #[snafu(display("Unable to create view: {}", reason))]
    UnableToCreateView {
        reason: String,
    },

    #[snafu(display("Unable to create view"))]
    UnableToCreateViewDataFusion {
        source: DataFusionError,
    },

    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    tasks: Vec<task::JoinHandle<()>>,
    backends: HashMap<String, Arc<Box<dyn DataBackend>>>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        DataFusion {
            ctx: Arc::new(SessionContext::new()),
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

    #[allow(clippy::needless_pass_by_value)]
    pub fn attach_backend(&mut self, table_name: &str, backend: DataBackendType) -> Result<()> {
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let data_backend: Box<dyn DataBackend> =
            <dyn DataBackend>::new(&self.ctx, table_name, &backend);

        self.backends
            .insert(table_name.to_string(), Arc::new(data_backend));

        Ok(())
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

    #[allow(clippy::needless_pass_by_value)]
    pub fn attach(
        &mut self,
        dataset: &Dataset,
        data_source: &'static mut dyn DataSource,
        backend: DataBackendType,
    ) -> Result<()> {
        let table_name = dataset.name.as_str();
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let data_backend: Box<dyn DataBackend> =
            <dyn DataBackend>::new(&self.ctx, table_name, &backend);

        let dataset_clone = dataset.clone();
        let task_handle = task::spawn(async move {
            let mut stream = data_source.get_data(&dataset_clone);
            loop {
                let future_result = stream.next().await;
                match future_result {
                    Some(data_update) => match data_backend.add_data(data_update).await {
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

    pub async fn attach_view(&self, dataset: &Dataset) -> Result<()> {
        let table_name = dataset.name.as_str();
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let sql = dataset.sql.clone().unwrap_or_default();
        let statements = DFParser::parse_sql_with_dialect(sql.as_str(), &AnsiDialect {})
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

        // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
        // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
        // the tables are created. To handle this, wait until all tables are created.
        let dependent_table_names = DataFusion::get_dependent_table_names(&statements[0]);
        for dependent_table_name in dependent_table_names {
            loop {
                if !self.ctx.table_exist(&dependent_table_name).unwrap_or(false) {
                    tracing::error!(
                        "Dependent table {dependent_table_name} for {table_name} does not exist, retrying in 1s..."
                    );
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
                break;
            }
        }

        let plan = self
            .ctx
            .state()
            .statement_to_plan(statements[0].clone())
            .await
            .context(UnableToCreateViewDataFusionSnafu)?;
        let view = ViewTable::try_new(plan, Some(sql.to_string()))
            .context(UnableToCreateViewDataFusionSnafu)?;
        self.ctx
            .register_table(table_name, Arc::new(view))
            .context(UnableToCreateViewDataFusionSnafu)?;

        Ok(())
    }

    fn get_dependent_table_names(statement: &parser::Statement) -> Vec<String> {
        let mut table_names = Vec::new();
        match statement.clone() {
            parser::Statement::Statement(statement) => match *statement {
                ast::Statement::Query(statement) => match *statement.body {
                    SetExpr::Select(select_statement) => {
                        for from in select_statement.from {
                            if let TableFactor::Table {
                                name,
                                alias: _,
                                args: _,
                                with_hints: _,
                                version: _,
                                partitions: _,
                            } = from.relation
                            {
                                table_names.push(name.to_string());
                            }

                            for join in from.joins {
                                if let TableFactor::Table {
                                    name,
                                    alias: _,
                                    args: _,
                                    with_hints: _,
                                    version: _,
                                    partitions: _,
                                } = join.relation
                                {
                                    table_names.push(name.to_string());
                                }
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
            },
            _ => {
                return table_names;
            }
        }
        table_names
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
