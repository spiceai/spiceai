use std::{collections::HashMap, sync::Arc};

use snafu::prelude::*;
use spicepod::component::dataset::acceleration;

use crate::dbconnection::DbConnection;

pub mod duckdbpool;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub enum Mode {
    Memory,
    File,
}

impl From<acceleration::Mode> for Mode {
    fn from(m: acceleration::Mode) -> Self {
        match m {
            acceleration::Mode::File => Mode::File,
            acceleration::Mode::Memory => Mode::Memory,
        }
    }
}

pub trait DbConnectionPool<P: r2d2::ManageConnection, C> {
    fn new(name: &str, mode: Mode, params: Arc<Option<HashMap<String, String>>>) -> Result<Self>
    where
        Self: Sized;
    fn connect(&self) -> Result<Box<dyn DbConnection<P>>>;
    fn connect_downcast(&self) -> Result<C>;
}
