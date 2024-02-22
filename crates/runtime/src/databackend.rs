use crate::datapublisher::DataPublisher;
use datafusion::execution::context::SessionContext;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{collections::HashMap, sync::Arc};

use self::{duckdb::DuckDBBackend, memtable::MemTableBackend};

#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod memtable;
pub mod postgres;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Backend creation failed: {source}"))]
    BackendCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub struct DataBackend;

impl DataBackend {
    #[allow(clippy::needless_pass_by_value)]
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        ctx: &Arc<SessionContext>,
        name: &str,
        engine: Engine,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> std::result::Result<Box<dyn DataPublisher>, Error> {
        match engine {
            Engine::Arrow => match mode {
                Mode::Memory => Ok(Box::new(MemTableBackend::new(Arc::clone(ctx), name))),
                Mode::File => InvalidConfigurationSnafu {
                    msg: "File mode not supported for Arrow engine".to_string(),
                }
                .fail()?,
            },
            #[cfg(feature = "duckdb")]
            Engine::DuckDB => Ok(Box::new(
                DuckDBBackend::new(Arc::clone(ctx), name, mode.into(), params)
                    .await
                    .boxed()
                    .context(BackendCreationFailedSnafu)?,
            )),
            Engine::Postgres => Ok(Box::new(
                postgres::PostgresBackend::new(Arc::clone(ctx), name, mode.into(), params)
                    .await
                    .boxed()
                    .context(BackendCreationFailedSnafu)?,
            )),
        }
    }
}
