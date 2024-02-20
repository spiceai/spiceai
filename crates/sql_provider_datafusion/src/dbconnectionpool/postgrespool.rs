use std::{collections::HashMap, sync::Arc};

use postgres::types::ToSql;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use snafu::ResultExt;

use super::{ConnectionPoolSnafu, DbConnectionPool, Mode, PostgresSnafu, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, DbConnection};

pub struct PostgresConnectionPool {
    pool: Arc<r2d2::Pool<PostgresConnectionManager<NoTls>>>,
}

impl DbConnectionPool<PostgresConnectionManager<NoTls>, PostgresConnection, &'static dyn ToSql>
    for PostgresConnectionPool
{
    fn new(_name: &str, _mode: Mode, _params: Arc<Option<HashMap<String, String>>>) -> Result<Self> {
        let parsed_config = match "host=localhost user=postgres".parse() {
            Ok(parsed_config) => parsed_config,
            Err(e) => return Err(e).context(PostgresSnafu),
        };
        let manager = PostgresConnectionManager::new(parsed_config, NoTls);
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);
        Ok(PostgresConnectionPool { pool })
    }

    fn connect(&self) -> Result<Box<dyn DbConnection<PostgresConnectionManager<NoTls>, &'static dyn ToSql>>> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(PostgresConnection::new(conn)))
    }

    fn connect_downcast(&self) -> Result<PostgresConnection> {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get().context(ConnectionPoolSnafu)?;
        Ok(PostgresConnection::new(conn))
    }
}
