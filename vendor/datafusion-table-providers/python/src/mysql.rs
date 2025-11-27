use std::sync::Arc;

use datafusion_table_providers::{
    mysql::MySQLTableFactory,
    sql::db_connection_pool::{mysqlpool::MySQLConnectionPool, DbConnectionPool},
    util::secrets::to_secret_map,
};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    utils::{pydict_to_hashmap, to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.mysql")]
struct RawMySQLTableFactory {
    pool: Arc<MySQLConnectionPool>,
    factory: MySQLTableFactory,
}

#[pymethods]
impl RawMySQLTableFactory {
    #[new]
    #[pyo3(signature = (params))]
    pub fn new(py: Python, params: &Bound<'_, PyDict>) -> PyResult<Self> {
        let params = to_secret_map(pydict_to_hashmap(params)?);
        let pool =
            Arc::new(wait_for_future(py, MySQLConnectionPool::new(params)).map_err(to_pyerr)?);

        Ok(Self {
            factory: MySQLTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    pub fn tables(&self, py: Python) -> PyResult<Vec<String>> {
        wait_for_future(py, async {
            let conn = self.pool.connect().await.map_err(to_pyerr)?;
            let conn_async = conn.as_async().ok_or(to_pyerr(
                "Unable to create connection to Mysql db".to_string(),
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
    m.add_class::<RawMySQLTableFactory>()?;

    Ok(())
}
