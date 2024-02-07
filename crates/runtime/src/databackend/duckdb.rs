use std::{collections::HashMap, mem, sync::Arc};

use arrow::record_batch::RecordBatch;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use duckdb::{
    vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab},
    DuckdbConnectionManager,
};
use duckdb_datafusion::DuckDBTable;
use r2d2::ManageConnection;
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::acceleration;

use crate::dataupdate::{DataUpdate, UpdateType};

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
}

pub enum Mode {
    Memory,
    File,
}

impl DataBackend for DuckDBBackend {
    fn add_data(&self, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();
        Box::pin(async move {
            let conn: r2d2::PooledConnection<DuckdbConnectionManager> =
                pool.get().context(ConnectionPoolSnafu)?;

            let mut duckdb_update = DuckDBUpdate {
                log_sequence_number: data_update.log_sequence_number.unwrap_or_default(),
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                duckdb_conn: conn,
            };

            duckdb_update.update()?;

            self.initialize_datafusion()?;
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
                let conn = manager.connect().context(DuckDBSnafu)?;

                conn.register_table_function::<ArrowVTab>("arrow")
                    .context(DuckDBSnafu)?;

                Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?)
            }
        };

        let name = name.to_string();

        Ok(DuckDBBackend { ctx, name, pool })
    }

    fn initialize_datafusion(&self) -> Result<()> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .context(DataFusionSnafu)?;
        if table_exists {
            return Ok(());
        }

        let table = match DuckDBTable::new(&self.pool, TableReference::bare(self.name.clone()))
            .context(DuckDBDataFusionSnafu)
        {
            Ok(table) => table,
            Err(e) => return Err(e),
        };

        self.ctx
            .register_table(&self.name, Arc::new(table))
            .context(DataFusionSnafu)?;

        Ok(())
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

struct DuckDBUpdate {
    log_sequence_number: u64,
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    duckdb_conn: r2d2::PooledConnection<DuckdbConnectionManager>,
}

impl DuckDBUpdate {
    fn update(&mut self) -> Result<()> {
        match self.update_type {
            UpdateType::Overwrite => self.create_table()?,
            UpdateType::Append => {
                if !self.table_exists()? {
                    self.create_table()?;
                }
            }
        };

        let data = mem::take(&mut self.data);
        for batch in data {
            self.insert_batch(batch)?;
        }

        tracing::trace!(
            "Processed update to DuckDB table {name} for log sequence number {lsn:?}",
            name = self.name,
            lsn = self.log_sequence_number
        );

        Ok(())
    }

    fn insert_batch(&self, batch: RecordBatch) -> Result<()> {
        let params = arrow_recordbatch_to_query_params(batch);
        let sql = format!(
            r#"INSERT INTO "{name}" SELECT * FROM arrow(?, ?)"#,
            name = self.name
        );

        self.duckdb_conn
            .execute(&sql, params)
            .context(DuckDBSnafu)?;

        Ok(())
    }

    fn create_table(&mut self) -> Result<()> {
        let table_exists = self.table_exists()?;
        if table_exists {
            let sql = format!(r#"DROP TABLE "{name}""#, name = self.name);
            self.duckdb_conn.execute(&sql, []).context(DuckDBSnafu)?;
        }

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let arrow_params = arrow_recordbatch_to_query_params(batch);
        let sql = format!(
            r#"CREATE TABLE "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.name
        );

        self.duckdb_conn
            .execute(&sql, arrow_params)
            .context(DuckDBSnafu)?;

        Ok(())
    }

    fn table_exists(&self) -> Result<bool> {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables 
              WHERE table_name = "{name}"
            )"#,
            name = self.name
        );

        self.duckdb_conn
            .query_row(&sql, [], |row| row.get(0))
            .context(DuckDBSnafu)
    }
}
