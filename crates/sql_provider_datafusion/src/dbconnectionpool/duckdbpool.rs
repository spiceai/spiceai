use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use duckdb::{vtab::arrow::ArrowVTab, DuckdbConnectionManager, ToSql};
use snafu::{prelude::*, ResultExt};

use super::{DbConnectionPool, Mode, Result};
use crate::dbconnection::{duckdbconn::DuckDbConnection, DbConnection};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },
}

pub struct DuckDbConnectionPool {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
}

#[async_trait]
impl DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>
    for DuckDbConnectionPool
{
    async fn new(
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let manager = match mode {
            Mode::Memory => DuckdbConnectionManager::memory().context(DuckDBSnafu)?,
            Mode::File => DuckdbConnectionManager::file(get_duckdb_file(name, &params))
                .context(DuckDBSnafu)?,
        };

        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        Ok(DuckDbConnectionPool { pool })
    }

    fn connect(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, &'static dyn ToSql>>,
    > {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(DuckDbConnection::new(conn)))
    }
}

fn get_duckdb_file(name: &str, params: &Arc<Option<HashMap<String, String>>>) -> String {
    params
        .as_ref()
        .as_ref()
        .and_then(|params| params.get("duckdb_file").cloned())
        .unwrap_or(format!("{name}.db"))
}
