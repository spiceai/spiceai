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
use crate::dataupdate::{DataUpdate, DataUpdateExecutionPlan, UpdateType};
use crate::get_dependent_table_names;
use arrow::datatypes::Schema;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::ViewTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_plan::collect;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::RefreshMode;
use spicepod::component::dataset::{Dataset, Mode, TimeFormat};
use tokio::spawn;
use tokio::time::{sleep, Instant};

pub mod filter_converter;
pub mod refresh_sql;

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

    #[snafu(display("{source}"))]
    RefreshSql { source: refresh_sql::Error },

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

    #[snafu(display("The table {table_name} is not writable"))]
    TableNotWritable { table_name: String },

    #[snafu(display("Unable to plan the table insert for {table_name}: {source}"))]
    UnableToPlanTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Unable to execute the table insert for {table_name}: {source}"))]
    UnableToExecuteTableInsert {
        table_name: String,
        source: DataFusionError,
    },

    #[snafu(display("Unable to trigger refresh for {table_name}: {source}"))]
    UnableToTriggerRefresh {
        table_name: String,
        source: crate::accelerated_table::Error,
    },

    #[snafu(display("Table {table_name} is not accelerated"))]
    NotAcceleratedTable { table_name: String },
}

pub enum Table {
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

pub(crate) struct Retention {
    pub(crate) time_column: String,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) period: Duration,
    pub(crate) check_interval: Duration,
}

impl Retention {
    pub(crate) fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        retention_period: Option<Duration>,
        retention_check_interval: Option<Duration>,
        retention_check_enabled: bool,
    ) -> Option<Self> {
        if !retention_check_enabled {
            return None;
        }
        if let (Some(time_column), Some(period), Some(check_interval)) =
            (time_column, retention_period, retention_check_interval)
        {
            Some(Self {
                time_column,
                time_format,
                period,
                check_interval,
            })
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Refresh {
    pub(crate) time_column: Option<String>,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) check_interval: Option<Duration>,
    pub(crate) sql: Option<String>,
    pub(crate) mode: RefreshMode,
    pub(crate) period: Option<Duration>,
}

impl Refresh {
    #[allow(clippy::needless_pass_by_value)]
    #[must_use]
    pub(crate) fn new(
        time_column: Option<String>,
        time_format: Option<TimeFormat>,
        check_interval: Option<Duration>,
        sql: Option<String>,
        mode: RefreshMode,
        period: Option<Duration>,
    ) -> Self {
        Self {
            time_column,
            time_format,
            check_interval,
            sql,
            mode,
            period,
        }
    }
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
                    .await?;
            }
            Table::Federated(source) => self.register_federated_table(dataset, source).await?,
            Table::View(sql) => self.register_view(&dataset.name, sql)?,
        }

        Ok(())
    }

    #[must_use]
    pub fn is_writable(&self, table_name: &str) -> bool {
        self.data_writers.iter().any(|s| s.as_str() == table_name)
    }

    pub async fn write_data(&self, table_name: &str, data_update: DataUpdate) -> Result<()> {
        if !self.is_writable(table_name) {
            TableNotWritableSnafu {
                table_name: table_name.to_string(),
            }
            .fail()?;
        }

        let table_provider = self
            .ctx
            .table_provider(OwnedTableReference::bare(table_name.to_string()))
            .await
            .context(UnableToGetTableSnafu)?;

        let overwrite = data_update.update_type == UpdateType::Overwrite;
        let insert_plan = table_provider
            .insert_into(
                &self.ctx.state(),
                Arc::new(DataUpdateExecutionPlan::new(data_update)),
                overwrite,
            )
            .await
            .context(UnableToPlanTableInsertSnafu {
                table_name: table_name.to_string(),
            })?;

        let _ = collect(insert_plan, self.ctx.task_ctx()).await.context(
            UnableToExecuteTableInsertSnafu {
                table_name: table_name.to_string(),
            },
        )?;

        Ok(())
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
        tracing::debug!("Registering accelerated table {dataset:?}");
        let obj_store = source
            .get_object_store(dataset)
            .transpose()
            .context(InvalidObjectStoreSnafu)?;

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

        let refresh_sql = dataset.refresh_sql();
        if let Some(refresh_sql) = &refresh_sql {
            refresh_sql::validate_refresh_sql(&dataset.name, refresh_sql.as_str())
                .context(RefreshSqlSnafu)?;
        }

        let accelerated_table = AcceleratedTable::new(
            dataset.name.to_string(),
            source_table_provider,
            accelerated_table_provider,
            Refresh::new(
                dataset.time_column.clone(),
                dataset.time_format.clone(),
                dataset.refresh_check_interval(),
                refresh_sql.clone(),
                acceleration_settings.refresh_mode.clone(),
                dataset.refresh_period(),
            ),
            Retention::new(
                dataset.time_column.clone(),
                dataset.time_format.clone(),
                dataset.retention_period(),
                dataset.retention_check_interval(),
                acceleration_settings.retention_check_enabled,
            ),
            obj_store,
        )
        .await;

        self.ctx
            .register_table(&dataset.name, Arc::new(accelerated_table))
            .context(UnableToRegisterTableToDataFusionSnafu)?;

        Ok(())
    }

    pub async fn refresh_table(&self, dataset_name: &str) -> Result<()> {
        let table = self
            .ctx
            .table_provider(OwnedTableReference::bare(dataset_name.to_string()))
            .await
            .context(UnableToGetTableSnafu)?;

        if let Some(accelerated_table) = table.as_any().downcast_ref::<AcceleratedTable>() {
            accelerated_table
                .trigger_refresh()
                .await
                .context(UnableToTriggerRefreshSnafu {
                    table_name: dataset_name.to_string(),
                })?;
        } else {
            NotAcceleratedTableSnafu {
                table_name: dataset_name.to_string(),
            }
            .fail()?;
        }

        Ok(())
    }

    /// Federated tables are attached directly as tables visible in the public `DataFusion` context.
    async fn register_federated_table(
        &self,
        dataset: &Dataset,
        source: Arc<dyn DataConnector>,
    ) -> Result<()> {
        tracing::debug!("Registering federated table {dataset:?}");
        if let Some(obj_store_result) = source.get_object_store(dataset) {
            let (key, store) = obj_store_result.context(InvalidObjectStoreSnafu)?;

            tracing::debug!("Registered object_store for {key}");
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

        let ctx = Arc::clone(&self.ctx);
        let table_name = table_name.to_string();
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
            if let Err(e) = ctx.register_table(
                OwnedTableReference::bare(table_name.clone()),
                Arc::new(view_table),
            ) {
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
