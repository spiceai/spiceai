use std::{collections::HashMap, sync::Arc};

use duckdb::{vtab::arrow::ArrowVTab, DuckdbConnectionManager, ToSql};
use flight_client::FlightClient;
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{flight::FlightConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    FlightError {},

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError {
        source: r2d2::Error,
    },
}

pub struct FlightConnectionPool {
    _pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    client: FlightClient,
}

impl FlightConnectionPool {
    pub fn new_with_client(client: FlightClient) -> Self {
        let manager = DuckdbConnectionManager::memory().unwrap();

        let pool = Arc::new(r2d2::Pool::new(manager).unwrap());
        Self { _pool: pool, client }
    }
}

impl DbConnectionPool<DuckdbConnectionManager, &'static dyn ToSql> for FlightConnectionPool {
    fn new(_: &str, _: Mode, _: Arc<Option<HashMap<String, String>>>) -> Result<Self> {
        todo!()
    }

    fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<DuckdbConnectionManager, &'static dyn ToSql>>> {
        Ok(Box::new(FlightConnection::new_with_client(
            self.client.clone(),
        )))
    }
}
