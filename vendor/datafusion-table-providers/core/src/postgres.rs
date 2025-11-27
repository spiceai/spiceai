use crate::sql::arrow_sql_gen::statement::{
    CreateTableBuilder, Error as SqlGenError, IndexBuilder, InsertBuilder,
};
use crate::sql::db_connection_pool::{
    self,
    dbconnection::{postgresconn::PostgresConnection, DbConnection},
    postgrespool::{self, PostgresConnectionPool},
    DbConnectionPool,
};
use crate::sql::sql_provider_datafusion::{expr::Engine, SqlTable};
use crate::util::schema::SchemaValidator;
use crate::util::supported_functions::FunctionSupport;
use crate::UnsupportedTypeAction;
use arrow::{
    array::RecordBatch,
    datatypes::{Schema, SchemaRef},
};
use async_trait::async_trait;
use bb8_postgres::{
    tokio_postgres::{types::ToSql, Transaction},
    PostgresConnectionManager,
};
use datafusion::catalog::Session;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion::{
    catalog::TableProviderFactory,
    common::Constraints,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use postgres_native_tls::MakeTlsConnector;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};

use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints::{self, get_primary_keys_from_constraints},
    indexes::IndexType,
    on_conflict::{self, OnConflict},
    secrets::to_secret_map,
    to_datafusion_error,
};

use self::write::PostgresTableWriter;

pub mod write;

pub type DynPostgresConnectionPool = dyn DbConnectionPool<
        bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
        &'static (dyn ToSql + Sync),
    > + Send
    + Sync;
pub type DynPostgresConnection = dyn DbConnection<
    bb8::PooledConnection<'static, PostgresConnectionManager<MakeTlsConnector>>,
    &'static (dyn ToSql + Sync),
>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: postgrespool::Error },

    #[snafu(display("Unable to downcast DbConnection to PostgresConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to begin Postgres transaction: {source}"))]
    UnableToBeginTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create the Postgres table: {source}"))]
    UnableToCreatePostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create an index for the Postgres table: {source}"))]
    UnableToCreateIndexForPostgresTable {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to commit the Postgres transaction: {source}"))]
    UnableToCommitPostgresTransaction {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to generate SQL: {source}"))]
    UnableToGenerateSQL { source: DataFusionError },

    #[snafu(display("Unable to delete all data from the Postgres table: {source}"))]
    UnableToDeleteAllTableData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to delete data from the Postgres table: {source}"))]
    UnableToDeleteData {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to insert Arrow batch to Postgres table: {source}"))]
    UnableToInsertArrowBatch {
        source: tokio_postgres::error::Error,
    },

    #[snafu(display("Unable to create insertion statement for Postgres table: {source}"))]
    UnableToCreateInsertStatement { source: SqlGenError },

    #[snafu(display("The table '{table_name}' doesn't exist in the Postgres server"))]
    TableDoesntExist { table_name: String },

    #[snafu(display("Constraint Violation: {source}"))]
    ConstraintViolation { source: constraints::Error },

    #[snafu(display("Error parsing column reference: {source}"))]
    UnableToParseColumnReference { source: column_reference::Error },

    #[snafu(display("Error parsing on_conflict: {source}"))]
    UnableToParseOnConflict { source: on_conflict::Error },

    #[snafu(display(
        "Failed to create '{table_name}': creating a table with a schema is not supported"
    ))]
    TableWithSchemaCreationNotSupported { table_name: String },

    #[snafu(display("Schema validation error: the provided data schema does not match the expected table schema: '{table_name}'"))]
    SchemaValidationError { table_name: String },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct PostgresTableFactory {
    pool: Arc<PostgresConnectionPool>,
}

impl PostgresTableFactory {
    #[must_use]
    pub fn new(pool: Arc<PostgresConnectionPool>) -> Self {
        Self { pool }
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;

        let table_provider = Arc::new(
            SqlTable::new(
                "postgres",
                &dyn_pool,
                table_reference,
                Some(Engine::Postgres),
            )
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
            .with_dialect(Arc::new(PostgreSqlDialect {})),
        );

        #[cfg(feature = "postgres-federation")]
        let table_provider = Arc::new(
            table_provider
                .create_federated_table_provider()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?,
        );

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone()).await?;
        let schema = read_provider.schema();

        let postgres = Postgres::new(
            table_reference,
            Arc::clone(&self.pool),
            schema,
            Constraints::default(),
        );

        Ok(PostgresTableWriter::create(read_provider, postgres, None))
    }
}

#[derive(Debug)]
pub struct PostgresTableProviderFactory {
    function_support: Option<FunctionSupport>,
}

impl PostgresTableProviderFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {
            function_support: None,
        }
    }
    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }
}

impl Default for PostgresTableProviderFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for PostgresTableProviderFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        if cmd.name.schema().is_some() {
            TableWithSchemaCreationNotSupportedSnafu {
                table_name: cmd.name.to_string(),
            }
            .fail()
            .map_err(to_datafusion_error)?;
        }

        let name = cmd.name.clone();
        let mut options = cmd.options.clone();
        let schema: Schema = cmd.schema.as_ref().into();

        let indexes_option_str = options.remove("indexes");
        let unparsed_indexes: HashMap<String, IndexType> = match indexes_option_str {
            Some(indexes_str) => util::hashmap_from_option_string(&indexes_str),
            None => HashMap::new(),
        };

        let unparsed_indexes = unparsed_indexes
            .into_iter()
            .map(|(key, value)| {
                let columns = ColumnReference::try_from(key.as_str())
                    .context(UnableToParseColumnReferenceSnafu)
                    .map_err(to_datafusion_error);
                (columns, value)
            })
            .collect::<Vec<(Result<ColumnReference, DataFusionError>, IndexType)>>();

        let mut indexes: Vec<(ColumnReference, IndexType)> = Vec::new();
        for (columns, index_type) in unparsed_indexes {
            let columns = columns?;
            indexes.push((columns, index_type));
        }

        let mut on_conflict: Option<OnConflict> = None;
        if let Some(on_conflict_str) = options.remove("on_conflict") {
            on_conflict = Some(
                OnConflict::try_from(on_conflict_str.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(to_datafusion_error)?,
            );
        }

        let params = to_secret_map(options);

        let pool = Arc::new(
            PostgresConnectionPool::new(params)
                .await
                .context(UnableToCreatePostgresConnectionPoolSnafu)
                .map_err(to_datafusion_error)?,
        );

        let schema: SchemaRef = Arc::new(schema);
        PostgresConnection::handle_unsupported_schema(&schema, UnsupportedTypeAction::default())
            .map_err(|e| DataFusionError::External(e.into()))?;

        let postgres = Postgres::new(
            name.clone(),
            Arc::clone(&pool),
            Arc::clone(&schema),
            cmd.constraints.clone(),
        );

        let mut db_conn = pool
            .connect()
            .await
            .context(DbConnectionSnafu)
            .map_err(to_datafusion_error)?;
        let postgres_conn = Postgres::postgres_conn(&mut db_conn).map_err(to_datafusion_error)?;

        let tx = postgres_conn
            .conn
            .transaction()
            .await
            .context(UnableToBeginTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let primary_keys = get_primary_keys_from_constraints(&cmd.constraints, &schema);

        postgres
            .create_table(Arc::clone(&schema), &tx, primary_keys)
            .await
            .map_err(to_datafusion_error)?;

        for index in indexes {
            postgres
                .create_index(&tx, index.0.iter().collect(), index.1 == IndexType::Unique)
                .await
                .map_err(to_datafusion_error)?;
        }

        tx.commit()
            .await
            .context(UnableToCommitPostgresTransactionSnafu)
            .map_err(to_datafusion_error)?;

        let dyn_pool: Arc<DynPostgresConnectionPool> = pool;

        let read_provider = Arc::new(
            SqlTable::new_with_schema(
                "postgres",
                &dyn_pool,
                Arc::clone(&schema),
                name,
                Some(Engine::Postgres),
            )
            .with_dialect(Arc::new(PostgreSqlDialect {}))
            .with_constraints(cmd.constraints.clone())
            .with_function_support(self.function_support.clone()),
        );

        #[cfg(feature = "postgres-federation")]
        let read_provider = Arc::new(read_provider.create_federated_table_provider()?);

        Ok(PostgresTableWriter::create(
            read_provider,
            postgres,
            on_conflict,
        ))
    }
}

#[derive(Clone)]
pub struct Postgres {
    table: TableReference,
    pool: Arc<PostgresConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres")
            .field("table_name", &self.table)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl Postgres {
    #[must_use]
    pub fn new(
        table: TableReference,
        pool: Arc<PostgresConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table,
            pool,
            schema,
            constraints,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table.table()
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub async fn connect(&self) -> Result<Box<DynPostgresConnection>> {
        let mut conn = self.pool.connect().await.context(DbConnectionSnafu)?;

        let pg_conn = Self::postgres_conn(&mut conn)?;

        if !self.table_exists(pg_conn).await {
            TableDoesntExistSnafu {
                table_name: self.table.to_string(),
            }
            .fail()?;
        }

        Ok(conn)
    }

    pub fn postgres_conn(
        db_connection: &mut Box<DynPostgresConnection>,
    ) -> Result<&mut PostgresConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<PostgresConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }

    async fn table_exists(&self, postgres_conn: &PostgresConnection) -> bool {
        let sql = match self.table.schema() {
            Some(schema) => format!(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{name}' AND table_schema = '{schema}')",
                name = self.table.table(),
                schema = schema
            ),
            None => format!(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{name}')",
                name = self.table.table()
            ),
        };

        tracing::trace!("{sql}");

        let Ok(row) = postgres_conn.conn.query_one(&sql, &[]).await else {
            return false;
        };

        row.get(0)
    }

    async fn insert_batch(
        &self,
        transaction: &Transaction<'_>,
        batch: RecordBatch,
        on_conflict: Option<OnConflict>,
    ) -> Result<()> {
        let insert_table_builder = InsertBuilder::new(&self.table, vec![batch]);

        let sea_query_on_conflict =
            on_conflict.map(|oc| oc.build_sea_query_on_conflict(&self.schema));

        let sql = insert_table_builder
            .build_postgres(sea_query_on_conflict)
            .context(UnableToCreateInsertStatementSnafu)?;

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToInsertArrowBatchSnafu)?;

        Ok(())
    }

    async fn delete_all_table_data(&self, transaction: &Transaction<'_>) -> Result<()> {
        transaction
            .execute(
                format!("DELETE FROM {}", self.table.to_quoted_string()).as_str(),
                &[],
            )
            .await
            .context(UnableToDeleteAllTableDataSnafu)?;

        Ok(())
    }

    async fn create_table(
        &self,
        schema: SchemaRef,
        transaction: &Transaction<'_>,
        primary_keys: Vec<String>,
    ) -> Result<()> {
        let create_table_statement =
            CreateTableBuilder::new(schema, self.table.table()).primary_keys(primary_keys);
        let create_stmts = create_table_statement.build_postgres();

        for create_stmt in create_stmts {
            transaction
                .execute(&create_stmt, &[])
                .await
                .context(UnableToCreatePostgresTableSnafu)?;
        }

        Ok(())
    }

    async fn create_index(
        &self,
        transaction: &Transaction<'_>,
        columns: Vec<&str>,
        unique: bool,
    ) -> Result<()> {
        let mut index_builder = IndexBuilder::new(self.table.table(), columns);
        if unique {
            index_builder = index_builder.unique();
        }
        let sql = index_builder.build_postgres();

        transaction
            .execute(&sql, &[])
            .await
            .context(UnableToCreateIndexForPostgresTableSnafu)?;

        Ok(())
    }
}
