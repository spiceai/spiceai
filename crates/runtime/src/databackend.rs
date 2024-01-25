use crate::dataupdate::DataUpdate;
use datafusion::{error::DataFusionError, sql::sqlparser};
use snafu::prelude::*;
use std::{future::Future, pin::Pin};

pub mod duckdb;
pub mod memtable;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to add data"))]
    UnableToAddData { source: DataFusionError },

    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub enum DataBackendType {
    #[default]
    Memtable,
    DuckDB,
}

pub trait DataBackend: Send {
    fn add_data(
        &mut self,
        data_update: DataUpdate,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}
