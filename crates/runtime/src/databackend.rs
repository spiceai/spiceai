use crate::dataupdate::DataUpdate;
use datafusion::execution::context::SessionContext;
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use self::{duckdb::DuckDBBackend, memtable::MemTableBackend};

#[cfg(feature = "duckdb")]
pub mod duckdb;
pub mod memtable;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },

    #[snafu(display("Backend creation failed"))]
    BackendCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type AddDataResult<'a> =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'a>>;

pub trait DataBackend: Send + Sync {
    fn add_data(&self, data_update: DataUpdate) -> AddDataResult;
}

impl dyn DataBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        ctx: &Arc<SessionContext>,
        name: &str,
        engine: Engine,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> std::result::Result<Box<Self>, Error> {
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
                    .boxed()
                    .context(BackendCreationFailedSnafu)?,
            )),
        }
    }
}
