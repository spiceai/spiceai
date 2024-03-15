use std::{collections::HashMap, mem, sync::Arc};

use arrow::record_batch::RecordBatch;
use arrow_sql_gen::statement::{CreateTableBuilder, InsertBuilder};
use bb8_sqlite::RusqliteConnectionManager;
use datafusion::{execution::context::SessionContext, sql::TableReference};
use db_connection_pool::{
    dbconnection::sqliteconn::SqliteConnection, sqlitepool::SqliteConnectionPool, DbConnectionPool,
};
use rusqlite::{ToSql, Transaction};
use snafu::{prelude::*, ResultExt};
use spicepod::component::dataset::Dataset;
use sql_provider_datafusion::SqlTable;
use tokio::sync::Mutex;

use crate::{
    datapublisher::{AddDataResult, DataPublisher},
    dataupdate::{DataUpdate, UpdateType},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("Error executing transaction: {source}"))]
    TransactionError { source: rusqlite::Error },

    #[snafu(display("SqliteDataFusionError: {source}"))]
    SqliteDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("DataFusionError: {source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Lock is poisoned: {message}"))]
    LockPoisoned { message: String },

    #[snafu(display("Unable to downcast DbConnection to SqliteConnection"))]
    UnableToDowncastDbConnection {},
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[allow(clippy::module_name_repetitions)]
pub struct SqliteBackend {
    ctx: Arc<SessionContext>,
    name: String,
    pool: Arc<
        dyn DbConnectionPool<
                bb8::PooledConnection<'static, RusqliteConnectionManager>,
                &'static dyn ToSql,
            > + Send
            + Sync,
    >,
    create_mutex: Mutex<()>,
    _primary_keys: Option<Vec<String>>,
}

impl DataPublisher for SqliteBackend {
    fn add_data(&self, _dataset: Arc<Dataset>, data_update: DataUpdate) -> AddDataResult {
        let pool = Arc::clone(&self.pool);
        let name = self.name.clone();
        Box::pin(async move {
            let mut sqlite_update = SqliteUpdate {
                name,
                data: data_update.data,
                update_type: data_update.update_type,
                pool,
                create_mutex: &self.create_mutex,
            };

            sqlite_update.update().await?;

            self.initialize_datafusion().await?;
            Ok(())
        })
    }

    fn name(&self) -> &str {
        "Sqlite"
    }
}

impl SqliteBackend {
    #[allow(clippy::needless_pass_by_value)]
    pub async fn new(
        ctx: Arc<SessionContext>,
        name: &str,
        params: Arc<Option<HashMap<String, String>>>,
        primary_keys: Option<Vec<String>>,
    ) -> Result<Self> {
        let pool = SqliteConnectionPool::new(params)
            .await
            .context(DbConnectionPoolSnafu)?;
        Ok(SqliteBackend {
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
            .context(SqliteDataFusionSnafu)
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

struct SqliteUpdate<'a> {
    name: String,
    data: Vec<RecordBatch>,
    update_type: UpdateType,
    pool: Arc<
        dyn DbConnectionPool<
                bb8::PooledConnection<'static, RusqliteConnectionManager>,
                &'static dyn ToSql,
            > + Send
            + Sync,
    >,
    create_mutex: &'a Mutex<()>,
}

impl<'a> SqliteUpdate<'a> {
    async fn update(&mut self) -> Result<()> {
        let mut transaction_conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(transaction_conn) = transaction_conn
            .as_any_mut()
            .downcast_mut::<SqliteConnection>()
        else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        let transaction = transaction_conn
            .conn
            .transaction()
            .context(TransactionSnafu)?;

        let conn = self.pool.connect().await.context(DbConnectionPoolSnafu)?;
        let Some(conn) = conn.as_any().downcast_ref::<SqliteConnection>() else {
            return UnableToDowncastDbConnectionSnafu {}.fail();
        };

        if !self.table_exists(conn) {
            self.create_table(&transaction)?;
        } else if self.update_type == UpdateType::Overwrite {
            transaction
                .execute(format!(r#"DELETE FROM "{}""#, self.name).as_str(), [])
                .context(TransactionSnafu)?;
        };

        let data = mem::take(&mut self.data);
        for batch in data {
            self.insert_batch(&transaction, batch)?;
        }

        transaction.commit().context(TransactionSnafu)?;

        tracing::trace!("Processed update to Sqlite table {name}", name = self.name,);

        Ok(())
    }

    fn insert_batch(&mut self, transaction: &Transaction<'_>, batch: RecordBatch) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.name, vec![batch]);
        let sql = insert_table_builder.build();

        transaction.execute(&sql, []).context(TransactionSnafu)?;

        Ok(())
    }

    fn create_table(&mut self, transaction: &Transaction<'_>) -> Result<()> {
        let _lock = self.create_mutex.lock();

        let Some(batch) = self.data.pop() else {
            return Ok(());
        };

        let create_table_statement = CreateTableBuilder::new(batch.schema(), &self.name);
        let sql = create_table_statement.build();

        transaction.execute(&sql, []).context(TransactionSnafu)?;

        self.insert_batch(transaction, batch)?;

        Ok(())
    }

    fn table_exists(&mut self, sqlite_conn: &SqliteConnection) -> bool {
        let sql = format!(
            r#"SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables 
              WHERE table_name = '{name}'
            )"#,
            name = self.name
        );
        tracing::trace!("{sql}");

        let Ok(exists) = sqlite_conn.conn.query_row(&sql, [], |row| row.get(0)) else {
            return false;
        };

        exists
    }
}
