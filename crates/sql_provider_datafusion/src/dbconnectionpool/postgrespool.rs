use std::{collections::HashMap, sync::Arc};

use postgres::types::ToSql;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{postgresconn::PostgresConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("PostgresError: {source}"))]
    PostgresError { source: postgres::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },
}

pub struct PostgresConnectionPool {
    pool: Arc<r2d2::Pool<PostgresConnectionManager<NoTls>>>,
}

impl DbConnectionPool<PostgresConnectionManager<NoTls>, &'static (dyn ToSql + Sync)>
    for PostgresConnectionPool
{
    fn new(
        _name: &str,
        _mode: Mode,
        _params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let parsed_config = "host=localhost user=postgres"
            .parse()
            .context(PostgresSnafu)?;
        let manager = PostgresConnectionManager::new(parsed_config, NoTls);
        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);
        Ok(PostgresConnectionPool { pool })
    }

    fn connect(
        &self,
    ) -> Result<Box<dyn DbConnection<PostgresConnectionManager<NoTls>, &'static (dyn ToSql + Sync)>>>
    {
        let pool = Arc::clone(&self.pool);
        let conn = pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(PostgresConnection::new(conn)))
    }
}
