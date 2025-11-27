use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion_table_providers::flight::{sql::FlightSqlDriver, FlightDriver, FlightTableFactory};
use pyo3::{prelude::*, types::PyDict};

use crate::{
    utils::{pydict_to_hashmap, to_pyerr, wait_for_future},
    RawTableProvider,
};

#[pyclass(module = "datafusion_table_providers._internal.Flight")]
struct RawFlightTableFactory {
    factory: FlightTableFactory,
}

#[pymethods]
impl RawFlightTableFactory {
    #[new]
    #[pyo3(signature = ())]
    pub fn new() -> PyResult<Self> {
        let driver: Arc<dyn FlightDriver> = Arc::new(FlightSqlDriver::new());

        Ok(Self {
            factory: FlightTableFactory::new(Arc::clone(&driver)),
        })
    }

    pub fn get_table(
        &self,
        py: Python,
        entry_point: &str,
        options: &Bound<'_, PyDict>,
    ) -> PyResult<RawTableProvider> {
        let options = pydict_to_hashmap(options)?;
        let table: Arc<dyn TableProvider> = Arc::new(
            wait_for_future(py, self.factory.open_table(entry_point, options)).map_err(to_pyerr)?,
        );

        Ok(RawTableProvider {
            table,
            supports_pushdown_filters: true,
        })
    }
}

pub(crate) fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RawFlightTableFactory>()?;

    Ok(())
}
