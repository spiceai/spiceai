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

use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{parsers::CompressionTypeVariant, Constraints, OwnedTableReference, ToDFSchema},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use lazy_static::lazy_static;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{self, Mode};
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use self::arrow::ArrowAccelerator;
// #[cfg(feature = "duckdb")]
// use self::duckdb::DuckDBBackend;

pub mod arrow;
// #[cfg(feature = "duckdb")]
// pub mod duckdb;
// #[cfg(feature = "mysql")]
// pub mod mysql;
// #[cfg(feature = "postgres")]
// pub mod postgres;
// #[cfg(feature = "sqlite")]
// pub mod sqlite;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

lazy_static! {
    static ref DATA_ACCELERATOR_ENGINES: Mutex<HashMap<String, Arc<dyn DataAccelerator>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_accelerator_engine(name: &str, accelerator_engine: Arc<dyn DataAccelerator>) {
    let mut registry = DATA_ACCELERATOR_ENGINES.lock().await;

    registry.insert(name.to_string(), accelerator_engine);
}

pub async fn register_all() {
    register_accelerator_engine("arrow", Arc::new(ArrowAccelerator::new())).await;
    //register_connector_factory("dremio", Dremio::create).await;
    // register_connector_factory("flightsql", flightsql::FlightSQL::create).await;
    // register_connector_factory("s3", s3::S3::create).await;
    // register_connector_factory("spiceai", spiceai::SpiceAI::create).await;
    // #[cfg(feature = "mysql")]
    // register_connector_factory("mysql", mysql::MySQL::create).await;
    // #[cfg(feature = "postgres")]
    // register_connector_factory("postgres", postgres::Postgres::create).await;
}

pub async fn get_accelerator_engine(engine_name: &str) -> Option<Arc<dyn DataAccelerator>> {
    let guard = DATA_ACCELERATOR_ENGINES.lock().await;

    let engine = guard.get(engine_name);

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

pub struct AcceleratorBuilder {
    engine: String,
    mode: Mode,
    params: Arc<Option<HashMap<String, String>>>,
    secret: Option<Secret>,
}

impl AcceleratorBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: "arrow".to_string(),
            mode: Mode::Memory,
            params: Arc::new(None),
            secret: None,
        }
    }

    #[must_use]
    pub fn engine(mut self, engine: String) -> Self {
        self.engine = engine;
        self
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn params(mut self, params: Arc<Option<HashMap<String, String>>>) -> Self {
        self.params = params;
        self
    }

    #[must_use]
    pub fn secret(mut self, secret: Option<Secret>) -> Self {
        self.secret = secret;
        self
    }

    /// Build the data accelerator, panicking if it fails
    ///
    /// # Panics
    ///
    /// Panics if the accelerator fails to build
    #[must_use]
    pub async fn must_build(self) -> Arc<dyn DataAccelerator> {
        match self.build().await {
            Ok(accelerator) => accelerator,
            Err(e) => panic!("Failed to build backend: {e}"),
        }
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
        match self.engine.as_ref() {
            "arrow" => self.validate_arrow(),
            _ => Ok(()),
        }
    }

    pub async fn build(self) -> std::result::Result<Arc<dyn DataAccelerator>, Error> {
        self.validate()?;
        let engine = self.engine;
        let mode = self.mode;

        let mut params = HashMap::new();
        if let Some(p) = self.params.as_ref() {
            params = p.clone();
        }

        params.insert("mode".to_string(), mode.to_string());

        let accelerator_guard = DATA_ACCELERATOR_ENGINES.lock().await;
        let accelerator =
            accelerator_guard
                .get(&engine)
                .ok_or_else(|| Error::InvalidConfiguration {
                    msg: format!("Unknown engine: {engine}"),
                })?;

        Ok(Arc::clone(accelerator))
    }
}

impl Default for AcceleratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn create_accelerator_table(
    table_name: &str,
    schema: SchemaRef,
    acceleration_settings: &acceleration::Acceleration,
    acceleration_secret: Option<Secret>,
) -> Result<Arc<dyn TableProvider>> {
    let params: Arc<Option<HashMap<String, String>>> =
        Arc::new(acceleration_settings.params.clone());

    let table_name = table_name.to_string();

    let data_accelerator: Arc<dyn DataAccelerator> = AcceleratorBuilder::new()
        .engine(acceleration_settings.engine())
        .mode(acceleration_settings.mode())
        .params(params)
        .secret(acceleration_secret)
        .build()
        .await?;

    let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema));
    let table_provider = data_accelerator
        .create_external_table(&CreateExternalTable {
            schema: df_schema.map_err(|e| {
                InvalidConfigurationSnafu {
                    msg: format!("Failed to convert schema: {e}"),
                }
                .build()
            })?,
            name: OwnedTableReference::bare(table_name.clone()),
            location: String::new(),
            file_type: String::new(),
            has_header: false,
            delimiter: ',',
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            file_compression_type: CompressionTypeVariant::UNCOMPRESSED,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::default(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::default(),
        })
        .await
        .context(AccelerationCreationFailedSnafu)?;

    Ok(table_provider)
}
