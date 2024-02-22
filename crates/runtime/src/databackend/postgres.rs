use std::{
    collections::HashMap,
    fmt, mem,
    sync::{Arc, PoisonError},
};

use arrow::record_batch::RecordBatch;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use postgres::types::ToSql;
use r2d2_postgres::{postgres::NoTls, PostgresConnectionManager};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::{
    dbconnection::{self, postgresconn::PostgresConnection, DbConnection},
    dbconnectionpool::{postgrespool::PostgresConnectionPool, DbConnectionPool, Mode},
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
    DbConnectionPoolError {
        source: sql_provider_datafusion::dbconnectionpool::Error,
    },

    #[snafu(display("PostgresError: {source}"))]
    Postgres { source: postgres::Error },

    #[snafu(display("PostgresDataFusionError: {source}"))]
    PostgresDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
    UnableToDowncastDbConnection {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::module_name_repetitions)]
pub struct PostgresBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<
        dyn DbConnectionPool<PostgresConnectionManager<NoTls>, &'static (dyn ToSql + Sync)>
            + Send
            + Sync,
    >,
    create_mutex: std::sync::Mutex<()>,
    _primary_keys: Option<Vec<String>>,
}

impl DataPublisher for PostgresBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();
        Box::pin(async move {
            let mut conn = pool.connect().context(DbConnectionPoolSnafu)?;
            let Some(conn) = conn.as_any_mut().downcast_mut::<PostgresConnection>() else {
                return Err(
                    Box::new(Error::UnableToDowncastDbConnection {}) as Box<dyn std::error::Error>
                );
            };

            let mut postgres_update = PostgresUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                postgres_conn: conn,
                create_mutex: &self.create_mutex,
            };

            postgres_update.update()?;

            self.initialize_datafusion().await?;
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "Postgres"
    }
}

impl PostgresBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        mode: Mode,
        params: Arc<Option<HashMap<String, String>>>,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self> {
        let pool = PostgresConnectionPool::new(name, mode, params)
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(PostgresBackend {
            ctx,
            name: name.to_string(),
            pool: Arc::new(pool),
            create_mutex: std::sync::Mutex::new(()),
            _primary_keys: primary_keys,
        })
    }

    async fn initialize_datafusion(&self) -> Result<()> {
        let table_exists = self
            .ctx
            .table_exist(TableReference::bare(self.name.clone()))
            .context(DataFusionSnafu)?;
        if table_exists {
            return Ok(());
        }

        let table = match SqlTable::new(&self.pool, TableReference::bare(self.name.clone()))
            .await
            .context(PostgresDataFusionSnafu)
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

struct PostgresUpdate<'a> {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    postgres_conn: &'a mut dbconnection::postgresconn::PostgresConnection,
    create_mutex: &'a std::sync::Mutex<()>,
}

impl<'a> PostgresUpdate<'a> {
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
            "Processed update to Postgres table {name}",
            name = self.name,
        );

        Ok(())
    }

    fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // TODO: Write RecordBatch
        let sql = format!(
            r#"INSERT INTO {name} (id, description) VALUES
            (1, 'First'),
            (2, 'Second'),
            (3, 'Third');"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        Ok(())
    }

    fn create_table(&mut self, drop_if_exists: bool) -> Result<()> {
        let _lock = self.create_mutex.lock().map_err(handle_poison)?;

        if self.table_exists() {
            if drop_if_exists {
                let sql = format!(r#"DROP TABLE "{}""#, self.name);
                tracing::trace!("{sql}");
                self.postgres_conn
                    .execute(&sql, &[])
                    .context(DbConnectionSnafu)?;
            } else {
                return Ok(());
            }
        }

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        // TODO: Write RecordBatch
        let sql = format!(
            r#"CREATE TABLE "{name}" AS
                SELECT *
                FROM (VALUES
                    (1, 'First'),
                    (2, 'Second'),
                    (3, 'Third')
                ) AS t(id, description);"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        Ok(())
    }

    fn table_exists(&mut self) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        let Ok(row) = self.postgres_conn.conn.query_one(&sql, &[]) else {
            return false;
        };

        let exists: bool = row.get(0);
        exists
    }
}

#[allow(clippy::needless_pass_by_value)]
fn handle_poison<T: fmt::Debug>(e: PoisonError<T>) -> Error {
    Error::LockPoisoned {
        message: format!("{e:?}"),
    }
}
