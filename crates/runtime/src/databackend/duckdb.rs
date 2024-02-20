use std::{
    collections::HashMap,
    fmt, mem,
    sync::{Arc, PoisonError},
};

use arrow::record_batch::RecordBatch;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use duckdb::{vtab::arrow::arrow_recordbatch_to_query_params, DuckdbConnectionManager};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::{
    dbconnection::{self, duckdb::DuckDbConnection, DbConnection},
    dbconnectionpool::{duckdb::DuckDbConnectionPool, DbConnectionPool, Mode},
    SqlTable,
};

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: sql_provider_datafusion::dbconnection::Error,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    ConnectionPool {
        source: sql_provider_datafusion::dbconnectionpool::Error,
    },

    #[snafu(display("DuckDBError: {source}"))]
    DuckDB { source: duckdb::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

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
    pool: Arc<dyn DbConnectionPool<DuckdbConnectionManager, DuckDbConnection> + Send + Sync>,
    create_mutex: std::sync::Mutex<()>,
}

impl DataPublisher for DuckDBBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();
        Box::pin(async move {
            let conn = pool.connect_downcast().context(ConnectionPoolSnafu)?;

            let mut duckdb_update = DuckDBUpdate {
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

    fn name(&self) -> &str {
        "DuckDB"
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
        let pool = DuckDbConnectionPool::new(name, mode, params).context(ConnectionPoolSnafu)?;
        Ok(DuckDBBackend {
            ctx,
            name: name.to_string(),
            pool: Arc::new(pool),
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

        let table = match SqlTable::new(&self.pool, TableReference::bare(self.name.clone()))
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

struct DuckDBUpdate<'a> {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    duckdb_conn: dbconnection::duckdb::DuckDbConnection,
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

        tracing::trace!("Processed update to DuckDB table {name}", name = self.name,);

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
            .conn
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
                self.duckdb_conn.execute(&sql).context(DbConnectionSnafu)?;
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
            .conn
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
            .conn
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
            data,
            update_type: UpdateType::Overwrite,
        };

        let dataset = Arc::new(Dataset::new("test".to_string(), "test".to_string()));

        backend
            .add_data(dataset, data_update)
            .await
            .expect("Unable to add data");

        let df = ctx
            .sql("SELECT * FROM test_add_data")
            .await
            .expect("Unable to execute query");
        let _ = df.show().await;
    }
}
