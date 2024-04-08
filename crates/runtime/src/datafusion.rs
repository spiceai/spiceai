/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::accelerated_table::AcceleratedTable;
use crate::dataaccelerator::{self, create_accelerator_table};
use crate::dataconnector::DataConnector;
use crate::get_dependent_table_names;
use arrow::datatypes::Schema;
use datafusion::datasource::ViewTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::{Dataset, Mode};
use tokio::spawn;
use tokio::time::{sleep, Instant};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table already exists"))]
    TableAlreadyExists {},

    #[snafu(display("Unable to create dataset acceleration: {source}"))]
    UnableToCreateDataAccelerator { source: dataaccelerator::Error },

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

    #[snafu(display("Unable to resolve table provider: {source}"))]
    UnableToResolveTableProvider {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Table {table_name} was marked as read_write, but the underlying provider only supports reads."))]
    WriteProviderNotImplemented { table_name: String },

    #[snafu(display("Unable to register table: {source}"))]
    UnableToRegisterTable { source: crate::dataconnector::Error },

    #[snafu(display("Unable to register table in DataFusion: {source}"))]
    UnableToRegisterTableToDataFusion { source: DataFusionError },

    #[snafu(display("Expected acceleration settings for {name}, found None"))]
    ExpectedAccelerationSettings { name: String },

    #[snafu(display("Unable to get object store configuration: {source}"))]
    InvalidObjectStore {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub(crate) enum Table {
    Accelerated {
        source: Arc<dyn DataConnector>,
        acceleration_secret: Option<Secret>,
    },
    Federated(Arc<dyn DataConnector>),
    View(String),
}

pub struct DataFusion {
    pub ctx: Arc<SessionContext>,
    data_writers: HashSet<String>,
}

impl DataFusion {
    #[must_use]
    pub fn new() -> Self {
        let mut df_config = SessionConfig::new().with_information_schema(true);
        df_config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
        DataFusion {
            ctx: Arc::new(SessionContext::new_with_config(df_config)),
            data_writers: HashSet::new(),
        }
    }

    pub async fn register_table(
        &mut self,
        dataset: impl Borrow<Dataset>,
        table: Table,
    ) -> Result<()> {
        let dataset = dataset.borrow();
        match table {
            Table::Accelerated {
                source,
                acceleration_secret,
            } => {
                self.register_accelerated_table(dataset, source, acceleration_secret)
                    .await?
            }
            Table::Federated(source) => self.register_federated_table(dataset, source).await?,
            Table::View(sql) => self.register_view(&dataset.name, sql)?,
        }

        Ok(())
    }

    #[must_use]
    pub fn is_writable(&self, dataset: &str) -> bool {
        self.data_writers.iter().any(|s| s.as_str() == dataset)
    }

    pub async fn get_arrow_schema(&self, dataset: &str) -> Result<Schema> {
        let data_frame = self
            .ctx
            .table(dataset)
            .await
            .context(UnableToGetTableSnafu)?;
        Ok(Schema::from(data_frame.schema()))
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

        if self.is_writable(dataset_name) {
            self.data_writers.remove(dataset_name);
        }

        Ok(())
    }

    async fn register_accelerated_table(
        &mut self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
        acceleration_secret: Option<Secret>,
    ) -> Result<()> {
        let source_table_provider = match dataset.mode() {
            Mode::Read => source
                .read_provider(dataset)
                .await
                .context(UnableToResolveTableProviderSnafu)?,
            Mode::ReadWrite => source
                .read_write_provider(dataset)
                .await
                .ok_or_else(|| {
                    WriteProviderNotImplementedSnafu {
                        table_name: dataset.name.to_string(),
                    }
                    .build()
                })?
                .context(UnableToResolveTableProviderSnafu)?,
        };

        let source_schema = source_table_provider.schema();
        let acceleration_settings =
            dataset
                .acceleration
                .clone()
                .ok_or_else(|| Error::ExpectedAccelerationSettings {
                    name: dataset.name.to_string(),
                })?;

        let accelerated_table_provider = create_accelerator_table(
            &dataset.name,
            source_schema,
            &acceleration_settings,
            acceleration_secret,
        )
        .await
        .context(UnableToCreateDataAcceleratorSnafu)?;

        let accelerated_table = AcceleratedTable::new(
            dataset.name.to_string(),
            source_table_provider,
            accelerated_table_provider,
            acceleration_settings.refresh_mode.clone(),
            dataset.refresh_interval(),
        );

        self.ctx
            .register_table(&dataset.name, Arc::new(accelerated_table))
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        Ok(())
    }

    /// Federated tables are attached directly as tables visible in the public DataFusion context.
    async fn register_federated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
    ) -> Result<()> {
        if let Some(obj_store_result) = source.get_object_store(dataset) {
            let (key, store) = obj_store_result.context(InvalidObjectStoreSnafu)?;

            self.ctx
                .runtime_env()
                .register_object_store(&key, Arc::clone(&store));
        }

        let table_exists = self.ctx.table_exist(&dataset.name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let source_table_provider = match dataset.mode() {
            Mode::Read => source
                .read_provider(dataset)
                .await
                .context(UnableToResolveTableProviderSnafu)?,
            Mode::ReadWrite => source
                .read_write_provider(dataset)
                .await
                .ok_or_else(|| {
                    WriteProviderNotImplementedSnafu {
                        table_name: dataset.name.to_string(),
                    }
                    .build()
                })?
                .context(UnableToResolveTableProviderSnafu)?,
        };

        self.ctx
            .register_table(&dataset.name, source_table_provider)
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        Ok(())
    }

    fn register_view(&self, table_name: &str, view: String) -> Result<()> {
        let table_exists = self.ctx.table_exist(table_name).unwrap_or(false);
        if table_exists {
            return TableAlreadyExistsSnafu.fail();
        }

        let statements = DFParser::parse_sql_with_dialect(view.as_str(), &PostgreSqlDialect {})
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
        spawn(async move {
            // Tables are currently lazily created (i.e. not created until first data is received) so that we know the table schema.
            // This means that we can't create a view on top of a table until the first data is received for all dependent tables and therefore
            // the tables are created. To handle this, wait until all tables are created.

            let deadline = Instant::now() + Duration::from_secs(60);
            let mut unresolved_dependent_table: Option<String> = None;
            let dependent_table_names = get_dependent_table_names(&statements[0]);
            for dependent_table_name in dependent_table_names {
                let mut attempts = 0;

                if unresolved_dependent_table.is_some() {
                    break;
                }

                loop {
                    if !ctx.table_exist(&dependent_table_name).unwrap_or(false) {
                        if Instant::now() >= deadline {
                            unresolved_dependent_table = Some(dependent_table_name.clone());
                            break;
                        }

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

            if let Some(missing_table) = unresolved_dependent_table {
                tracing::error!("Failed to create view {table_name}. Dependent table {missing_table} does not exist.");
                return;
            }

            let plan = match ctx.state().statement_to_plan(statements[0].clone()).await {
                Ok(plan) => plan,
                Err(e) => {
                    tracing::error!("Failed to create view: {e}");
                    return;
                }
            };

            let view_table = match ViewTable::try_new(plan, Some(view)) {
                Ok(view_table) => view_table,
                Err(e) => {
                    tracing::error!("Failed to create view: {e}");
                    return;
                }
            };
            if let Err(e) = ctx.register_table(table_name, Arc::new(view_table)) {
                tracing::error!("Failed to create view: {e}");
            };

            tracing::info!("Created view {table_name}");
        });

        Ok(())
    }
}

impl Default for DataFusion {
    fn default() -> Self {
        Self::new()
    }
}
