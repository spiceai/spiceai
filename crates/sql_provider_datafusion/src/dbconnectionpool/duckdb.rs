use std::{collections::HashMap, sync::Arc};

use duckdb_rs::{vtab::arrow::ArrowVTab, DuckdbConnectionManager};
use snafu::ResultExt;

use super::{ConnectionPoolSnafu, DbConnectionPool, DuckDBSnafu, Mode, Result};
use crate::dbconnection::{duckdb::DuckDbConnection, DbConnection};

pub struct DuckDbConnectionPool {
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
}

impl DbConnectionPool<DuckdbConnectionManager, DuckDbConnection> for DuckDbConnectionPool {
    fn new(name: &str, mode: Mode, params: Arc<Option<HashMap<String, String>>>) -> Result<Self>
    where
        Self: Sized,
    {
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

    fn connect(&self) -> Result<Box<dyn DbConnection<DuckdbConnectionManager>>> {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(Box::new(DuckDbConnection::new(conn)))
    }

    fn connect_downcast(&self) -> Result<DuckDbConnection> {
        let pool = Arc::clone(&self.pool);
        let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
            pool.get().context(ConnectionPoolSnafu)?;
        Ok(DuckDbConnection::new(conn))
    }
}

fn get_duckdb_file(name: &str, params: &Arc<Option<HashMap<String, String>>>) -> String {
    params
        .as_ref()
        .as_ref()
        .and_then(|params| params.get("duckdb_file").cloned())
        .unwrap_or(format!("{name}.db"))
}
