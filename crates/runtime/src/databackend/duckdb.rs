use std::{collections::HashMap, ops::Deref, sync::Arc};

use datafusion::execution::context::SessionContext;
use duckdb::DuckdbConnectionManager;
use duckdb_datafusion::DuckDBTable;
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::acceleration;
use std::sync::Once;
use tokio::sync::RwLock;

use crate::dataupdate::DataUpdate;

use super::{AddDataResult, DataBackend};

#[derive(Debug, Snafu)]
pub enum Error {
    DuckDBError {
        source: duckdb::Error,
    },

    ConnectionPoolError {
        source: r2d2::Error,
    },

    DuckDBDataFusion {
        source: duckdb_datafusion::Error,
    },

    DataFusion {
        source: datafusion::error::DataFusionError,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    df_initialized: Once,
    df_initialization_result: RwLock<Result<()>>,
}

pub enum Mode {
    Memory,
    File,
}

impl DataBackend for DuckDBBackend {
    fn add_data(&self, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        Box::pin(async move {
            let conn = pool.get().context(ConnectionPoolSnafu)?;
            //conn.execute(&data_update.sql).context(DuckDBError)?;

            self.initialize_datafusion();
            let r = self.df_initialization_result.read().await;
            let result = r.as_ref().map_err(|e| e.clone())?;
            Ok(())
        })
    }
}

impl DuckDBBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
    ) -> Result<Self> {
        let pool = match mode {
            Mode::Memory => {
                let manager = DuckdbConnectionManager::memory().context(DuckDBSnafu)?;
                Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?)
            }
            Mode::File => {
                let manager = DuckdbConnectionManager::file(get_duckdb_file(name, &params))
                    .context(DuckDBSnafu)?;
                Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?)
            }
        };

        let name = name.to_string();

        Ok(DuckDBBackend {
            ctx,
            name,
            pool,
            df_initialized: Once::new(),
            df_initialization_result: RwLock::new(Ok(())),
        })
    }

    fn initialize_datafusion(&self) {
        if self.df_initialized.is_completed() {
            return;
        }

        self.df_initialized.call_once(|| {
            let table = match DuckDBTable::new(&self.pool, self.name.clone())
                .context(DuckDBDataFusionSnafu)
            {
                Ok(table) => table,
                Err(e) => {
                    tracing::error!("Failed to create DuckDBTable: {}", e);
                    let mut r = self.df_initialization_result.blocking_write();
                    *r = Err(e);
                    return;
                }
            };

            if let Err(e) = self.ctx.register_table(&self.name, Arc::new(table)) {
                tracing::error!(
                    r#"Failed to register DuckDBTable "{name}" in DataFusion: {e}"#,
                    name = self.name
                );
                let mut r = self.df_initialization_result.blocking_write();
                *r = Err(Error::DataFusion { source: e });
            };
        });
    }
}

fn get_duckdb_file(name: &str, params: &Arc<Option<HashMap<String, String>>>) -> String {
    params
        .as_ref()
        .as_ref()
        .and_then(|params| params.get("duckdb_file").cloned())
        .unwrap_or(format!("{name}.db"))
}

impl From<acceleration::Mode> for Mode {
    fn from(m: acceleration::Mode) -> Self {
        match m {
            acceleration::Mode::File => Mode::File,
            acceleration::Mode::Memory => Mode::Memory,
        }
    }
}
