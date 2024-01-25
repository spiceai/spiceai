use crate::dataupdate::DataUpdate;
use datafusion::{error::DataFusionError, execution::context::SessionContext, sql::sqlparser};
use snafu::prelude::*;
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
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub enum DataBackendType {
    #[default]
    Memtable,
    DuckDB,
}

pub trait DataBackend: Send + Sync {
    fn add_data(
        &self,
        data_update: DataUpdate,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}
impl dyn DataBackend {
    #[must_use]
    pub fn get(
        ctx: &Arc<SessionContext>,
        dataset: &str,
        backend_type: &DataBackendType,
    ) -> Box<Self> {
        match backend_type {
            DataBackendType::Memtable => Box::new(MemTableBackend::new(Arc::clone(ctx), dataset)),
            DataBackendType::DuckDB => {
                todo!("DuckDB backend not implemented yet");
            }
        }
    }
}
