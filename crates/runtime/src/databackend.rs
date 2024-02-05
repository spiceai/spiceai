use crate::dataupdate::DataUpdate;
use datafusion::{error::DataFusionError, execution::context::SessionContext, sql::sqlparser};
use snafu::prelude::*;
use spicepod::component::dataset::acceleration::{Engine, Mode};
use std::{future::Future, pin::Pin, sync::Arc};

use self::memtable::MemTableBackend;

pub mod duckdb;
pub mod memtable;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to add data"))]
    UnableToAddData { source: DataFusionError },

    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Invalid configuration: {msg}"))]
    InvalidConfiguration { msg: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub trait DataBackend: Send + Sync {
    fn add_data(
        &self,
        data_update: DataUpdate,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

impl dyn DataBackend {
    pub fn new(
        ctx: &Arc<SessionContext>,
        name: &str,
        engine: &Engine,
        mode: &Mode,
    ) -> Result<Box<Self>> {
        match engine {
            Engine::Arrow => match mode {
                Mode::Memory => Ok(Box::new(MemTableBackend::new(Arc::clone(ctx), name))),
                Mode::File => InvalidConfigurationSnafu {
                    msg: "File mode not supported for Arrow engine".to_string(),
                }
                .fail()?,
            },
            Engine::DuckDB => {
                todo!("DuckDB backend not implemented yet");
            }
        }
    }
}
