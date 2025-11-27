use std::{sync::Arc, time::Duration};

use datafusion_table_providers::{
    sql::db_connection_pool::{
        sqlitepool::{SqliteConnectionPool, SqliteConnectionPoolFactory},
        DbConnectionPool,
    },
    sqlite::SqliteTableFactory,
};
use pyo3::prelude::*;

use crate::{
    utils::{to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.sqlite")]
struct RawSqliteTableFactory {
    pool: Arc<SqliteConnectionPool>,
    factory: SqliteTableFactory,
}

#[pymethods]
impl RawSqliteTableFactory {
    #[new]
    #[pyo3(signature = (path, mode, busy_timeout_s, attach_databases = None))]
    pub fn new(
        py: Python,
        path: &str,
        mode: String,
        busy_timeout_s: f64,
        attach_databases: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let mode = mode.as_str().into();
        let busy_timeout = Duration::from_secs_f64(busy_timeout_s);
        let attach_databases = attach_databases.map(|d| d.into_iter().map(Arc::from).collect());
        let factory = SqliteConnectionPoolFactory::new(path, mode, busy_timeout)
            .with_databases(attach_databases);
        let pool = Arc::new(wait_for_future(py, factory.build()).map_err(to_pyerr)?);

        Ok(Self {
            factory: SqliteTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    pub fn tables(&self, py: Python) -> PyResult<Vec<String>> {
        wait_for_future(py, async {
            let conn = self.pool.connect().await.map_err(to_pyerr)?;
            let conn_async = conn.as_async().ok_or(to_pyerr(
                "Unable to create connection to sqlite db".to_string(),
            ))?;
            let schemas = conn_async.schemas().await.map_err(to_pyerr)?;

            let mut tables = Vec::default();
            for schema in schemas {
                let schema_tables = conn_async.tables(&schema).await.map_err(to_pyerr)?;
                tables.extend(schema_tables);
            }

            Ok(tables)
        })
    }

    pub fn get_table(&self, py: Python, table_reference: &str) -> PyResult<RawTableProvider> {
        let table = wait_for_future(py, self.factory.table_provider(table_reference.into()))
            .map_err(to_pyerr)?;

        Ok(RawTableProvider {
            table,
            supports_pushdown_filters: true,
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawSqliteTableFactory>()?;

    Ok(())
}
