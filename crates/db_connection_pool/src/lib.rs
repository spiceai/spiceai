use crate::dbconnection::DbConnection;
use async_trait::async_trait;
use spicepod::component::dataset::acceleration;

pub mod dbconnection;
pub mod duckdbpool;
pub mod postgrespool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait DbConnectionPool<T, P: 'static> {
    async fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;
}

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
