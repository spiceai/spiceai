use std::{
    collections::HashMap,
    fmt, mem,
    sync::{Arc, PoisonError},
};

use arrow::record_batch::RecordBatch;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use duckdb::{
    vtab::arrow::{arrow_recordbatch_to_query_params, ArrowVTab},
    DuckdbConnectionManager,
};
use duckdb_datafusion::DuckDBTable;
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::acceleration;

use crate::dataupdate::{DataUpdate, UpdateType};

use super::{AddDataResult, DataBackend};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DuckDBError: {source}"))]
    DuckDBError { source: duckdb::Error },

    #[snafu(display("ConnectionPoolError: {source}"))]
    ConnectionPoolError { source: r2d2::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion { source: duckdb_datafusion::Error },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct DuckDBBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<r2d2::Pool<DuckdbConnectionManager>>,
    create_mutex: std::sync::Mutex<()>,
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
                create_mutex: &self.create_mutex,
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
        let manager = match mode {
            Mode::Memory => DuckdbConnectionManager::memory().context(DuckDBSnafu)?,
            Mode::File => DuckdbConnectionManager::file(get_duckdb_file(name, &params))
                .context(DuckDBSnafu)?,
        };

        let pool = Arc::new(r2d2::Pool::new(manager).context(ConnectionPoolSnafu)?);

        let conn = pool.get().context(ConnectionPoolSnafu)?;
        conn.register_table_function::<ArrowVTab>("arrow")
            .context(DuckDBSnafu)?;

        let name = name.to_string();

        Ok(DuckDBBackend {
            ctx,
            name,
            pool,
            create_mutex: std::sync::Mutex::new(()),
        })
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

struct DuckDBUpdate<'a> {
    log_sequence_number: u64,
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    duckdb_conn: r2d2::PooledConnection<DuckdbConnectionManager>,
    create_mutex: &'a std::sync::Mutex<()>,
}

impl<'a> DuckDBUpdate<'a> {
    fn update(&mut self) -> Result<()> {
        match self.update_type {
            UpdateType::Overwrite => self.create_table(true)?,
            UpdateType::Append => {
                if !self.table_exists() {
                    self.create_table(false)?;
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
        tracing::trace!("{sql}");

        self.duckdb_conn
            .execute(&sql, params)
            .context(DuckDBSnafu)?;

        Ok(())
    }

    fn create_table(&mut self, drop_if_exists: bool) -> Result<()> {
        let _lock = self.create_mutex.lock().map_err(handle_poison)?;

        if self.table_exists() {
            if drop_if_exists {
                let sql = format!(r#"DROP TABLE "{}""#, self.name);
                tracing::trace!("{sql}");
                self.duckdb_conn.execute(&sql, []).context(DuckDBSnafu)?;
            } else {
                return Ok(());
            }
        }

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let arrow_params = arrow_recordbatch_to_query_params(batch);
        let sql = format!(
            r#"CREATE TABLE "{name}" AS SELECT * FROM arrow(?, ?)"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        self.duckdb_conn
            .execute(&sql, arrow_params)
            .context(DuckDBSnafu)?;

        Ok(())
    }

    fn table_exists(&self) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables 
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        self.duckdb_conn
            .query_row(&sql, [], |row| row.get::<usize, bool>(0))
            .unwrap_or(false)
    }
}

#[allow(clippy::needless_pass_by_value)]
fn handle_poison<T: fmt::Debug>(e: PoisonError<T>) -> Error {
    Error::LockPoisoned {
        message: format!("{e:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_add_data() {
        let ctx = Arc::new(SessionContext::new());
        let name = "test_add_data";
        let backend = DuckDBBackend::new(Arc::clone(&ctx), name, Mode::Memory, Arc::new(None))
            .expect("Unable to create DuckDBBackend");

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let data = if let Ok(batch) = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
            ],
        ) {
            vec![batch]
        } else {
            panic!("Unable to create record batch");
        };
        let data_update = DataUpdate {
            log_sequence_number: Some(1),
            data,
            update_type: UpdateType::Overwrite,
        };

        backend
            .add_data(data_update)
            .await
            .expect("Unable to add data");

        let df = ctx
            .sql("SELECT * FROM test_add_data")
            .await
            .expect("Unable to execute query");
        let _ = df.show().await;
    }
}
