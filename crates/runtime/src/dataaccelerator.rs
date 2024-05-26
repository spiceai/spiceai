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

use crate::component::dataset::acceleration::{self, Engine, Mode};
use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{parsers::CompressionTypeVariant, Constraints, TableReference, ToDFSchema},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use lazy_static::lazy_static;
use secrets::ExposeSecret;
use secrets::Secret;
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
    params: Option<HashMap<String, String>>,
    secret: Option<Secret>,
}

impl AcceleratorExternalTableBuilder {
    #[must_use]
    pub fn new(table_name: TableReference, schema: SchemaRef, engine: Engine) -> Self {
        Self {
            table_name,
            schema,
            engine,
            mode: Mode::Memory,
            params: None,
            secret: None,
        }
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn params(mut self, params: HashMap<String, String>) -> Self {
        self.params = Some(params);
        self
    }

    #[must_use]
    pub fn secret(mut self, secret: Option<Secret>) -> Self {
        self.secret = secret;
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

        let mut params = self.params.unwrap_or_default();

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&self.schema));

        let mode = self.mode;
        params.insert("mode".to_string(), mode.to_string());

        if let Some(secret) = self.secret {
            for (k, v) in secret.iter() {
                params.insert(k.to_string(), v.expose_secret().to_string());
            }
        }

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
            has_header: false,
            delimiter: ',',
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            file_compression_type: CompressionTypeVariant::UNCOMPRESSED,
            order_exprs: vec![],
            unbounded: false,
            options: params,
            constraints: Constraints::empty(),
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

    let external_table = AcceleratorExternalTableBuilder::new(table_name, schema, engine)
        .mode(acceleration_settings.mode)
        .params(acceleration_settings.params.clone())
        .secret(acceleration_secret)
        .build()?;

    let table_provider = accelerator
        .create_external_table(&external_table)
        .await
        .context(AccelerationCreationFailedSnafu)?;

    Ok(table_provider)
}
