use std::{collections::HashMap, mem, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use bb8_postgres::{
    tokio_postgres::{types::ToSql, NoTls, Transaction},
    PostgresConnectionManager,
};
use datafusion::{execution::context::SessionContext, sql::TableReference};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::{
    dbconnection::postgresconn::PostgresConnection,
    dbconnectionpool::{postgrespool::PostgresConnectionPool, DbConnectionPool},
    SqlTable,
};
use tokio::sync::Mutex;

use crate::{
    datafusion::read_pg_config,
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
    secrets::Secret,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: sql_provider_datafusion::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError {
        source: sql_provider_datafusion::dbconnectionpool::Error,
    },

    #[snafu(display("Error executing transaction: {source}"))]
    TransactionError {
        source: bb8_postgres::tokio_postgres::Error,
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

#[allow(clippy::module_name_repetitions)]
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
    create_mutex: Mutex<()>,
    _primary_keys: Option<Vec<String>>,
}

impl DataPublisher for PostgresBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let name = self.name.clone();
        Box::pin(async move {
            let mut postgres_update = PostgresUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                pool: Arc::clone(&self.pool),
                create_mutex: &self.create_mutex,
            };

            postgres_update.update().await?;

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
        params: Arc<Option<HashMap<String, String>>>,
        primary_keys: Option<Vec<String>>,
        secret: Option<Secret>,
    ) -> Result<Self> {
        let pool = PostgresConnectionPool::new(read_pg_config(params, secret))
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(PostgresBackend {
            ctx,
            name: name.to_string(),
            pool: Arc::new(pool),
            create_mutex: Mutex::new(()),
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
    pool: Arc<
        dyn DbConnectionPool<
                bb8::PooledConnection<'static, PostgresConnectionManager<NoTls>>,
                &'static (dyn ToSql + Sync),
            > + Send
            + Sync,
    >,
    create_mutex: &'a Mutex<()>,
}

impl<'a> PostgresUpdate<'a> {
    async fn update(&mut self) -> Result<()> {
        let mut transaction_conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(transaction_conn) = transaction_conn
            .as_any_mut()
            .downcast_mut::<PostgresConnection>()
        else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        let transaction = transaction_conn
            .conn
            .transaction()
            .await
            .context(TransactionSnafu)?;

        let conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(conn) = conn.as_any().downcast_ref::<PostgresConnection>() else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        if !self.table_exists(conn).await {
            self.create_table(&transaction).await?;
        } else if self.update_type == UpdateType::Overwrite {
            transaction
                .execute(format!(r#"DELETE FROM "{}""#, self.name).as_str(), &[])
                .await
                .context(TransactionSnafu)?;
        };

        let data = mem::take(&mut self.data);
        for batch in data {
            self.insert_batch(&transaction, batch).await?;
        }

        transaction.commit().await.context(TransactionSnafu)?;

        tracing::trace!(
            "Processed update to Postgres table {name}",
            name = self.name,
        );

        Ok(())
    }

    async fn insert_batch(
        &mut self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
    ) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.name, vec![batch]);
        let sql = insert_table_builder.build();

        transaction
            .execute(&sql, &[])
            .await
            .context(TransactionSnafu)?;

        Ok(())
    }

    async fn create_table(&mut self, transaction: &Transaction<'_>) -> Result<()> {
        let _lock = self.create_mutex.lock();

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let create_table_statement = CreateTableBuilder::new(batch.schema(), &self.name);
        let sql = create_table_statement.build();

        transaction
            .execute(&sql, &[])
            .await
            .context(TransactionSnafu)?;

        self.insert_batch(transaction, batch).await?;

        Ok(())
    }

    async fn table_exists(&mut self, postgres_conn: &PostgresConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables 
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        let Ok(row) = postgres_conn.conn.query_one(&sql, &[]).await else {
            return false;
        };

        row.get(0)
    }
}
