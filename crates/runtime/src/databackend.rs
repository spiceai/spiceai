/*
Copyright 2024 Spice AI, Inc.

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
use datafusion::execution::context::SessionContext;
use secrets::Secret;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{collections::HashMap, sync::Arc};

use self::{duckdb::DuckDBBackend, memtable::MemTableBackend};

#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod memtable;
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

pub struct DataBackendBuilder {
    ctx: Arc<SessionContext>,
    name: String,
    engine: Option<Engine>,
    mode: Option<Mode>,
    params: Arc<Option<HashMap<String, String>>>,
    primary_keys: Option<Vec<String>>,
    secret: Option<Secret>,
}

impl DataBackendBuilder {
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>, name: String) -> Self {
        Self {
            ctx,
            name,
            engine: None,
            mode: None,
            params: Arc::new(None),
            primary_keys: None,
            secret: None,
        }
    }

    #[must_use]
    pub fn engine(mut self, engine: Engine) -> Self {
        self.engine = Some(engine);
        self
    }

    #[must_use]
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = Some(mode);
        self
    }

    #[must_use]
    pub fn params(mut self, params: Arc<Option<HashMap<String, String>>>) -> Self {
        self.params = params;
        self
    }

    #[must_use]
    pub fn primary_keys(mut self, primary_keys: Option<Vec<String>>) -> Self {
        self.primary_keys = primary_keys;
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

    fn validate_arrow(&self) -> std::result::Result<(), Error> {
        if let Some(Mode::File) = self.mode {
            InvalidConfigurationSnafu {
                msg: "File mode not supported for Arrow engine".to_string(),
            }
            .fail()?;
        } else if self.primary_keys.is_some() {
            InvalidConfigurationSnafu {
                msg: "Primary keys not supported for Arrow engine".to_string(),
            }
            .fail()?;
        }
        Ok(())
    }

    fn validate(&self) -> std::result::Result<(), Error> {
        match self.engine {
            Some(Engine::Arrow) => self.validate_arrow(),
            #[cfg(feature = "duckdb")]
            Some(Engine::DuckDB) => Ok(()),
            #[cfg(feature = "postgres")]
            Some(Engine::Postgres) => Ok(()),
            #[cfg(feature = "sqlite")]
            Some(Engine::Sqlite) => Ok(()),
            _ => Ok(()),
        }
    }

    pub async fn build(self) -> std::result::Result<Box<dyn DataPublisher>, Error> {
        self.validate()?;
        let engine = self.engine.unwrap_or_default();
        let mode = self.mode.unwrap_or_default();

        match engine {
            Engine::Arrow => Ok(Box::new(MemTableBackend::new(
                Arc::clone(&self.ctx),
                self.name.as_str(),
            ))),
            #[cfg(feature = "duckdb")]
            Engine::DuckDB => Ok(Box::new(
                DuckDBBackend::new(
                    Arc::clone(&self.ctx),
                    self.name.as_str(),
                    mode.into(),
                    self.params,
                    self.primary_keys,
                )
                .boxed()
                .context(BackendCreationFailedSnafu)?,
            )),
            #[cfg(feature = "postgres")]
            Engine::Postgres => Ok(Box::new(
                postgres::PostgresBackend::new(
                    Arc::clone(&self.ctx),
                    self.name.as_str(),
                    self.params,
                    self.primary_keys,
                    self.secret,
                )
                .await
                .boxed()
                .context(BackendCreationFailedSnafu)?,
            )),
            #[cfg(feature = "sqlite")]
            Engine::Sqlite => Ok(Box::new(
                sqlite::SqliteBackend::new(
                    Arc::clone(&self.ctx),
                    self.name.as_str(),
                    self.params,
                    mode.into(),
                    self.primary_keys,
                )
                .await
                .boxed()
                .context(BackendCreationFailedSnafu)?,
            )),
        }
    }
}
