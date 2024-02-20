use std::{collections::HashMap, sync::Arc};

use spicepod::component::dataset::acceleration;

use crate::dbconnection::DbConnection;

pub mod duckdbpool;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

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

pub trait DbConnectionPool<T: r2d2::ManageConnection, C, P: 'static> {
    fn new(name: &str, mode: Mode, params: Arc<Option<HashMap<String, String>>>) -> Result<Self>
    where
        Self: Sized;
    fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;
    fn connect_downcast(&self) -> Result<C>;
}
