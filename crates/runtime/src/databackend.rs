use crate::dataupdate::DataUpdate;
use datafusion::{execution::context::SessionContext, logical_expr::Expr};
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
    InvalidConfiguration { msg: &'static str },

    #[snafu(display("Backend creation failed"))]
    BackendCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Backend doesn't support deleting data"))]
    BackendDeleteUnsupported,
}

pub type BackendAsyncResult<'a> =
    Pin<Box<dyn Future<Output = Result<(), Box<dyn std::error::Error>>> + Send + 'a>>;

pub trait DataBackend: Send + Sync {
    fn add_data(&self, data_update: DataUpdate) -> BackendAsyncResult;

    fn set_primary_keys(&mut self, _primary_keys: Vec<String>) -> BackendAsyncResult {
        Box::pin(async {
            Err(Error::InvalidConfiguration {
                msg: "Backend doesn't support setting primary keys",
            }
            .into())
        })
    }

    /// Delete data from the backend matching the given filter
    /// The filter is a list of expressions that are `ANDed` together
    fn delete_data(&self, _delete_filter: &[Expr]) -> BackendAsyncResult {
        Box::pin(async { Err(Error::BackendDeleteUnsupported.into()) })
    }
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
                    msg: "File mode not supported for Arrow engine",
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
