use std::{str::FromStr, sync::Arc};

use datafusion_table_providers::{
    duckdb::DuckDBTableFactory,
    sql::db_connection_pool::{duckdbpool::DuckDbConnectionPool, DbConnectionPool},
};
use duckdb::AccessMode;
use pyo3::prelude::*;

use crate::{
    utils::{to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.duckdb")]
struct RawDuckDBTableFactory {
    pool: Arc<DuckDbConnectionPool>,
    factory: DuckDBTableFactory,
}

#[pymethods]
impl RawDuckDBTableFactory {
    #[staticmethod]
    #[pyo3(signature = ())]
    pub fn new_memory() -> PyResult<Self> {
        let pool = Arc::new(DuckDbConnectionPool::new_memory().map_err(to_pyerr)?);

        Ok(Self {
            factory: DuckDBTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (path, access_mode))]
    pub fn new_file(path: &str, access_mode: &str) -> PyResult<Self> {
        let access_mode = AccessMode::from_str(access_mode).map_err(to_pyerr)?;
        let pool = Arc::new(DuckDbConnectionPool::new_file(path, &access_mode).map_err(to_pyerr)?);

        Ok(Self {
            factory: DuckDBTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    pub fn tables(&self, py: Python) -> PyResult<Vec<String>> {
        wait_for_future(py, async {
            let conn = self.pool.connect().await.map_err(to_pyerr)?;

            let conn_sync = conn
                .as_sync()
                .ok_or(to_pyerr("Unable to create synchronous DuckDB connection"))?;
            let schemas = conn_sync.schemas().map_err(to_pyerr)?;

            let mut tables = Vec::default();
            for schema in schemas {
                let schema_tables = conn_sync.tables(&schema).map_err(to_pyerr)?;
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
    m.add_class::<RawDuckDBTableFactory>()?;

    Ok(())
}
