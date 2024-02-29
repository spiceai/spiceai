use std::{collections::HashMap, sync::Arc};

use arrow::record_batch::RecordBatch;
use bb8_postgres::{
    tokio_postgres::{types::ToSql, NoTls},
    PostgresConnectionManager,
};
use datafusion::{execution::context::SessionContext, sql::TableReference};
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

pub struct PostgresBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<
        dyn DbConnectionPool<
                bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
                &'static (dyn ToSql + Sync),
            > + Send
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
            let mut conn = pool.connect().await.context(DbConnectionPoolSnafu)?;
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
        todo!()
    }

    fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        todo!()
    }

    fn create_table(&mut self, drop_if_exists: bool) -> Result<()> {
        todo!()
    }

    fn table_exists(&self) -> bool {
        todo!()
    }
}