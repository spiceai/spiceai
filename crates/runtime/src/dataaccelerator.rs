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

use crate::component::dataset::acceleration::{self, Acceleration, Engine, IndexType, Mode};
use crate::secrets::ExposeSecret;
use crate::secrets::Secret;
use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use data_components::util::{column_reference::ColumnReference, on_conflict::OnConflict};
use datafusion::{
    common::{Constraints, TableReference, ToDFSchema},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use lazy_static::lazy_static;
use snafu::prelude::*;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use self::arrow::ArrowAccelerator;

#[cfg(feature = "duckdb")]
use self::duckdb::DuckDBAccelerator;
#[cfg(feature = "postgres")]
use self::postgres::PostgresAccelerator;
#[cfg(feature = "sqlite")]
use self::sqlite::SqliteAccelerator;

pub mod arrow;
#[cfg(feature = "duckdb")]
pub mod duckdb;
// #[cfg(feature = "mysql")]
// pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Unknown engine: {engine}"))]
    UnknownEngine { engine: Arc<str> },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

lazy_static! {
    static ref DATA_ACCELERATOR_ENGINES: Mutex<HashMap<Engine, Arc<dyn DataAccelerator>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_accelerator_engine(
    name: Engine,
    accelerator_engine: Arc<dyn DataAccelerator>,
) {
    let mut registry = DATA_ACCELERATOR_ENGINES.lock().await;

    registry.insert(name, accelerator_engine);
}

pub async fn register_all() {
    register_accelerator_engine(Engine::Arrow, Arc::new(ArrowAccelerator::new())).await;
    #[cfg(feature = "duckdb")]
    register_accelerator_engine(Engine::DuckDB, Arc::new(DuckDBAccelerator::new())).await;
    #[cfg(feature = "postgres")]
    register_accelerator_engine(Engine::PostgreSQL, Arc::new(PostgresAccelerator::new())).await;
    #[cfg(feature = "sqlite")]
    register_accelerator_engine(Engine::Sqlite, Arc::new(SqliteAccelerator::new())).await;
}

pub async fn get_accelerator_engine(engine: Engine) -> Option<Arc<dyn DataAccelerator>> {
    let guard = DATA_ACCELERATOR_ENGINES.lock().await;

    let engine = guard.get(&engine);

    match engine {
        Some(engine_ref) => Some(Arc::clone(engine_ref)),
        None => None,
    }
}

/// A `DataAccelerator` knows how to read, write and create new tables.
#[async_trait]
pub trait DataAccelerator: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>>;
}

pub struct AcceleratorExternalTableBuilder {
    table_name: TableReference,
    schema: SchemaRef,
    engine: Engine,
    mode: Mode,
    options: Option<HashMap<String, String>>,
    secret: Option<Secret>,
    indexes: HashMap<ColumnReference, IndexType>,
    constraints: Option<Constraints>,
    on_conflict: Option<OnConflict>,
}

impl AcceleratorExternalTableBuilder {
    #[must_use]
    pub fn new(table_name: TableReference, schema: SchemaRef, engine: Engine) -> Self {
        Self {
            table_name,
            schema,
            engine,
            mode: Mode::Memory,
            options: None,
            secret: None,
            indexes: HashMap::new(),
            constraints: None,
            on_conflict: None,
        }
    }

    #[must_use]
    pub fn indexes(mut self, indexes: HashMap<ColumnReference, IndexType>) -> Self {
        self.indexes = indexes;
        self
    }

    #[must_use]
    pub fn on_conflict(mut self, on_conflict: OnConflict) -> Self {
        self.on_conflict = Some(on_conflict);
        self
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = Some(options);
        self
    }

    #[must_use]
    pub fn secret(mut self, secret: Option<Secret>) -> Self {
        self.secret = secret;
        self
    }

    #[must_use]
    pub fn constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = Some(constraints);
        self
    }

    fn validate_arrow(&self) -> Result<(), Error> {
        if Mode::File == self.mode {
            InvalidConfigurationSnafu {
                msg: "File mode not supported for Arrow engine".to_string(),
            }
            .fail()?;
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        match self.engine {
            Engine::Arrow => self.validate_arrow(),
            _ => Ok(()),
        }
    }

    pub fn build(self) -> Result<CreateExternalTable> {
        self.validate()?;

        let mut options = self.options.unwrap_or_default();

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&self.schema));

        let mode = self.mode;
        options.insert("mode".to_string(), mode.to_string());

        if let Some(secret) = self.secret {
            for (k, v) in secret.iter() {
                options.insert(k.to_string(), v.expose_secret().to_string());
            }
        }

        if !self.indexes.is_empty() {
            let indexes_option_str = Acceleration::hashmap_to_option_string(&self.indexes);
            options.insert("indexes".to_string(), indexes_option_str);
        }

        if let Some(on_conflict) = self.on_conflict {
            options.insert("on_conflict".to_string(), on_conflict.to_string());
        }

        let constraints = match self.constraints {
            Some(constraints) => constraints,
            None => Constraints::empty(),
        };

        let external_table = CreateExternalTable {
            schema: df_schema.map_err(|e| {
                InvalidConfigurationSnafu {
                    msg: format!("Failed to convert schema: {e}"),
                }
                .build()
            })?,
            name: self.table_name.clone(),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints,
            column_defaults: HashMap::default(),
        };

        Ok(external_table)
    }
}

pub async fn create_accelerator_table(
    table_name: TableReference,
    schema: SchemaRef,
    acceleration_settings: &acceleration::Acceleration,
    acceleration_secret: Option<Secret>,
) -> Result<Arc<dyn TableProvider>> {
    let engine = acceleration_settings.engine;

    let accelerator_guard = DATA_ACCELERATOR_ENGINES.lock().await;
    let accelerator =
        accelerator_guard
            .get(&engine)
            .ok_or_else(|| Error::InvalidConfiguration {
                msg: format!("Unknown engine: {engine}"),
            })?;

    if let Err(e) = acceleration_settings.validate_indexes(&schema) {
        InvalidConfigurationSnafu {
            msg: format!("{e}"),
        }
        .fail()?;
    };

    let mut external_table_builder =
        AcceleratorExternalTableBuilder::new(table_name, Arc::clone(&schema), engine)
            .mode(acceleration_settings.mode)
            .options(acceleration_settings.params.clone())
            .indexes(acceleration_settings.indexes.clone())
            .secret(acceleration_secret);

    if let Some(on_conflict) =
        acceleration_settings
            .on_conflict()
            .map_err(|e| Error::InvalidConfiguration {
                msg: format!("on_conflict invalid: {e}"),
            })?
    {
        external_table_builder = external_table_builder.on_conflict(on_conflict);
    };

    match acceleration_settings.table_constraints(Arc::clone(&schema)) {
        Ok(Some(constraints)) => {
            external_table_builder = external_table_builder.constraints(constraints);
        }
        Ok(None) => {}
        Err(e) => {
            InvalidConfigurationSnafu {
                msg: format!("{e}"),
            }
            .fail()?;
        }
    }

    let external_table = external_table_builder.build()?;

    let table_provider = accelerator
        .create_external_table(&external_table)
        .await
        .context(AccelerationCreationFailedSnafu)?;

    Ok(table_provider)
}
