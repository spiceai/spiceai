use crate::datapublisher::DataPublisher;
use datafusion::execution::context::SessionContext;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{collections::HashMap, sync::Arc};

use self::{duckdb::DuckDBBackend, memtable::MemTableBackend};

#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod memtable;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Backend creation failed: {source}"))]
    BackendCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct DataBackendBuilder {
    ctx: Arc<SessionContext>,
    name: String,
    engine: Engine,
    mode: Mode,
    params: Arc<Option<HashMap<String, String>>>,
    primary_keys: Option<Vec<String>>,
}

impl DataBackendBuilder {
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, name: String) -> Self {
        Self {
            ctx,
            name,
            engine: Engine::Arrow,
            mode: Mode::Memory,
            params: Arc::new(None),
            primary_keys: None,
        }
    }

    #[must_use]
    pub fn with_engine(mut self, engine: Engine) -> Self {
        self.engine = engine;
        self
    }

    #[must_use]
    pub fn with_mode(mut self, mode: Mode) -> Self {
        self.mode = mode;
        self
    }

    #[must_use]
    pub fn with_params(mut self, params: Arc<Option<HashMap<String, String>>>) -> Self {
        self.params = params;
        self
    }

    #[must_use]
    pub fn with_primary_keys(mut self, primary_keys: Option<Vec<String>>) -> Self {
        self.primary_keys = primary_keys;
        self
    }

    pub fn build(self) -> std::result::Result<Box<dyn DataPublisher>, Error> {
        match self.engine {
            Engine::Arrow => match self.mode {
                Mode::Memory => Ok(Box::new(MemTableBackend::new(
                    Arc::clone(&self.ctx),
                    self.name.as_str(),
                ))),
                Mode::File => InvalidConfigurationSnafu {
                    msg: "File mode not supported for Arrow engine".to_string(),
                }
                .fail()?,
            },
            #[cfg(feature = "duckdb")]
            Engine::DuckDB => Ok(Box::new(
                DuckDBBackend::new(
                    Arc::clone(&self.ctx),
                    self.name.as_str(),
                    self.mode.into(),
                    self.params,
                )
                .boxed()
                .context(BackendCreationFailedSnafu)?,
            )),
        }
    }
}
