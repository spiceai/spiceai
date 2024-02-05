use crate::dataupdate::DataUpdate;
use datafusion::{error::DataFusionError, execution::context::SessionContext, sql::sqlparser};
use snafu::prelude::*;
use std::{future::Future, pin::Pin, sync::Arc};

use self::{memtable::MemTableBackend, viewtable::ViewTableBackend};

pub mod duckdb;
pub mod memtable;
pub mod viewtable;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to add data"))]
    UnableToAddData { source: DataFusionError },

    UnableToParseSql {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("Unable to create table: {}", reason))]
    UnableToCreateTable { reason: String },

    #[snafu(display("Unable to create table"))]
    UnableToCreateTableDataFusion { source: DataFusionError },

    #[snafu(display("Unsupported operation {} for backend {:?}", operation, backend))]
    UnsupportedOperation {
        operation: String,
        backend: DataBackendType,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default)]
pub enum DataBackendType {
    #[default]
    Memtable,
    DuckDB,
    ViewTable,
}

pub trait DataBackend: Send + Sync {
    fn add_data(
        &self,
        data_update: DataUpdate,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

impl dyn DataBackend {
    pub async fn new(
        ctx: &Arc<SessionContext>,
        name: &str,
        backend_type: &DataBackendType,
        sql: Option<&str>,
    ) -> Result<Box<Self>> {
        match backend_type {
            DataBackendType::Memtable => Ok(Box::new(MemTableBackend::new(Arc::clone(ctx), name))),
            DataBackendType::ViewTable => Ok(Box::new(
                ViewTableBackend::new(Arc::clone(ctx), name, sql.unwrap_or_default()).await?,
            )),
            DataBackendType::DuckDB => {
                todo!("DuckDB backend not implemented yet");
            }
        }
    }
}
