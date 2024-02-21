use std::{collections::HashMap, sync::Arc};

use spicepod::component::dataset::acceleration;

use crate::dbconnection::DbConnection;

pub mod duckdbpool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
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

pub trait DbConnectionPool<T: r2d2::ManageConnection, P: 'static> {
    fn new(name: &str, mode: Mode, params: Arc<Option<HashMap<String, String>>>) -> Result<Self>
    where
        Self: Sized;
    fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;
}
