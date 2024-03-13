use std::{collections::HashMap, sync::Arc};

use crate::dbconnection::DbConnection;
use async_trait::async_trait;

pub mod dbconnection;
pub mod duckdbpool;
pub mod postgrespool;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T, E = Error> = std::result::Result<T, E>;

pub enum Mode {
    Memory,
    File,
}

#[async_trait]
pub trait DbConnectionPool<T, P: 'static> {
    async fn new(
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self>
    where
        Self: Sized;
    async fn connect(&self) -> Result<Box<dyn DbConnection<T, P>>>;
}
