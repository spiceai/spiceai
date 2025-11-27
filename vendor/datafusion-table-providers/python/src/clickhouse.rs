use std::sync::Arc;

use datafusion_table_providers::{
    clickhouse::{Arg, ClickHouseTableFactory},
    sql::db_connection_pool::{clickhousepool::ClickHouseConnectionPool, DbConnectionPool},
    util::secrets::to_secret_map,
};
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    types::{PyDict, PyList},
};

use crate::{
    utils::{pydict_to_hashmap, to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.clickhouse")]
struct RawClickHouseTableFactory {
    pool: Arc<ClickHouseConnectionPool>,
    factory: ClickHouseTableFactory,
}

#[pymethods]
impl RawClickHouseTableFactory {
    #[new]
    #[pyo3(signature = (params))]
    pub fn new(py: Python, params: &Bound<'_, PyDict>) -> PyResult<Self> {
        let params = to_secret_map(pydict_to_hashmap(params)?);
        let pool =
            Arc::new(wait_for_future(py, ClickHouseConnectionPool::new(params)).map_err(to_pyerr)?);

        Ok(Self {
            factory: ClickHouseTableFactory::new(Arc::clone(&pool)),
            pool,
        })
    }

    pub fn tables(&self, py: Python) -> PyResult<Vec<String>> {
        wait_for_future(py, async {
            let conn = self.pool.connect().await.map_err(to_pyerr)?;
            let conn_async = conn.as_async().ok_or(to_pyerr(
                "Unable to create connection to Postgres db".to_string(),
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

    #[pyo3(signature = (table_reference, args=None))]
    pub fn get_table(
        &self,
        py: Python,
        table_reference: &str,
        args: Option<Py<PyAny>>,
    ) -> PyResult<RawTableProvider> {
        let args_vec = if let Some(args) = args {
            let seq = args.downcast_bound::<PyList>(py).map_err(|_| {
                PyTypeError::new_err("Argument must be list of int (signed/unsigned) or string")
            })?;

            let arr: Result<Vec<_>, _> = seq
                .iter()
                .map(|val| {
                    val.extract::<(String, u64)>()
                        .map(|x| (x.0, Arg::Unsigned(x.1)))
                        .or_else(|_| {
                            val.extract::<(String, i64)>()
                                .map(|x| (x.0, Arg::Signed(x.1)))
                        })
                        .or_else(|_| {
                            val.extract::<(String, String)>()
                                .map(|x| (x.0, Arg::String(x.1)))
                        })
                })
                .collect();

            let arr = arr.map_err(|_| {
                PyTypeError::new_err("Argument must be list of int (signed/unsigned) or string")
            })?;

            Some(arr)
        } else {
            None
        };

        let table = wait_for_future(
            py,
            self.factory
                .table_provider(table_reference.into(), args_vec),
        )
        .map_err(to_pyerr)?;

        Ok(RawTableProvider {
            table,
            supports_pushdown_filters: true,
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawClickHouseTableFactory>()?;

    Ok(())
}
