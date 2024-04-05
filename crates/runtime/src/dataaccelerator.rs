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

use crate::datapublisher::DataPublisher;
use ::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    common::{parsers::CompressionTypeVariant, Constraints, DFSchema, OwnedTableReference},
    datasource::TableProvider,
    logical_expr::CreateExternalTable,
};
use lazy_static::lazy_static;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

use self::arrow::ArrowAccelerator;
#[cfg(feature = "duckdb")]
use self::duckdb::DuckDBBackend;

pub mod arrow;
#[cfg(feature = "duckdb")]
pub mod duckdb;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Backend creation failed: {source}"))]
    BackendCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

lazy_static! {
    static ref DATA_ACCELERATOR_ENGINES: Mutex<HashMap<String, Arc<dyn DataAccelerator>>> =
        Mutex::new(HashMap::new());
}

pub async fn register_accelerator_engine(name: &str, accelerator_engine: Arc<dyn DataAccelerator>) {
    let mut registry = DATA_ACCELERATOR_ENGINES.lock().await;

    registry.insert(name.to_string(), accelerator_engine);
}

pub async fn register_all() {
    register_accelerator_engine("arrow", ArrowAccelerator::default()).await;
    //register_connector_factory("dremio", Dremio::create).await;
    // register_connector_factory("flightsql", flightsql::FlightSQL::create).await;
    // register_connector_factory("s3", s3::S3::create).await;
    // register_connector_factory("spiceai", spiceai::SpiceAI::create).await;
    // #[cfg(feature = "mysql")]
    // register_connector_factory("mysql", mysql::MySQL::create).await;
    // #[cfg(feature = "postgres")]
    // register_connector_factory("postgres", postgres::Postgres::create).await;
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
    table_name: OwnedTableReference,
    schema: SchemaRef,
    engine: String,
    mode: Mode,
    params: Arc<Option<HashMap<String, String>>>,
    secret: Option<Secret>,
}

impl AcceleratorBuilder {
    #[must_use]
    pub fn new(table_name: OwnedTableReference, schema: SchemaRef) -> Self {
        Self {
            table_name,
            schema,
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

    /// Build the data backend, panicking if it fails
    ///
    /// # Panics
    ///
    /// Panics if the backend fails to build
    #[must_use]
    pub async fn must_build(self) -> Box<dyn DataPublisher> {
        match self.build().await {
            Ok(backend) => backend,
            Err(e) => panic!("Failed to build backend: {e}"),
        }
    }

    fn validate_arrow(&self) -> Result<(), Error> {
        if let Some(Mode::File) = self.mode {
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

    pub async fn build(self) -> std::result::Result<Box<dyn DataPublisher>, Error> {
        self.validate()?;
        let engine = self.engine;
        let mode = self.mode;

        let mut params = HashMap::new();
        if let Some(p) = self.params.as_ref() {
            params = p.clone();
        }

        params.insert("mode".to_string(), mode.to_string());

        let cmd = CreateExternalTable {
            schema: Arc::new(DFSchema::try_from(self.schema).map_err(|e| {
                InvalidConfigurationSnafu {
                    msg: format!("Failed to convert schema: {e}"),
                }
                .build()
            })?),
            name: self.table_name,
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
            options: Default::default(),
            constraints: Constraints::empty(),
            column_defaults: Default::default(),
        };

        let accelerator = DATA_ACCELERATOR_ENGINES
            .lock()
            .await
            .get(&engine)
            .ok_or_else(|| Error::InvalidConfiguration {
                msg: format!("Unknown engine: {engine}"),
            })?;
    }
}
