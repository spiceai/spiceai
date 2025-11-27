use pyo3::{exceptions::PyException, prelude::*};
use std::future::Future;

use datafusion_table_providers::sql::db_connection_pool::runtime::execute_in_tokio;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Utility to collect rust futures with GIL released
pub fn wait_for_future<F>(py: Python, f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    py.allow_threads(|| execute_in_tokio(|| f))
}

pub fn to_pyerr<T: ToString>(err: T) -> PyErr {
    PyException::new_err(err.to_string())
}

pub fn pydict_to_hashmap(pydict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (key, value) in pydict.iter() {
        let key_str: String = key.extract()?;
        let value_str: String = value.extract()?;
        map.insert(key_str, value_str);
    }
    Ok(map)
}
