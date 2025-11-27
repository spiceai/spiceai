use crate::duckdb::write_settings::DuckDBWriteSettings;
use crate::sql::sql_provider_datafusion;
use crate::util::supported_functions::FunctionSupport;
use crate::util::{
    self,
    column_reference::{self, ColumnReference},
    constraints,
    indexes::IndexType,
    on_conflict::{self, OnConflict},
};
use crate::{
    sql::db_connection_pool::{
        self,
        dbconnection::{
            duckdbconn::{
                flatten_table_function_name, is_table_function, DuckDBParameter, DuckDbConnection,
            },
            get_schema, DbConnection,
        },
        duckdbpool::{DuckDbConnectionPool, DuckDbConnectionPoolBuilder},
        DbConnectionPool, DbInstanceKey, Mode,
    },
    UnsupportedTypeAction,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect};
use datafusion::{
    catalog::{Session, TableProviderFactory},
    common::Constraints,
    datasource::TableProvider,
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::CreateExternalTable,
    sql::TableReference,
};
use duckdb::{AccessMode, DuckdbConnectionManager};
use itertools::Itertools;
use snafu::prelude::*;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use write::DuckDBTableWriterBuilder;

pub use self::settings::{
    DuckDBSetting, DuckDBSettingScope, DuckDBSettingsRegistry, MemoryLimitSetting,
    PreserveInsertionOrderSetting, TempDirectorySetting,
};
use self::sql_table::DuckDBTable;

#[cfg(feature = "duckdb-federation")]
mod federation;

mod creator;
mod settings;
pub mod sql_table;
pub mod write;
pub mod write_settings;
pub use creator::{RelationName, TableDefinition, TableManager, ViewCreator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DbConnectionError: {source}"))]
    DbConnectionError {
        source: db_connection_pool::dbconnection::GenericError,
    },

    #[snafu(display("DbConnectionPoolError: {source}"))]
    DbConnectionPoolError { source: db_connection_pool::Error },

    #[snafu(display("DuckDBDataFusionError: {source}"))]
    DuckDBDataFusion {
        source: sql_provider_datafusion::Error,
    },

    #[snafu(display("Unable to downcast DbConnection to DuckDbConnection"))]
    UnableToDowncastDbConnection {},

    #[snafu(display("Unable to drop duckdb table: {source}"))]
    UnableToDropDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create duckdb table: {source}"))]
    UnableToCreateDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to create index on duckdb table: {source}"))]
    UnableToCreateIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to retrieve existing primary keys from DuckDB table: {source}"))]
    UnableToGetPrimaryKeysOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to drop index on duckdb table: {source}"))]
    UnableToDropIndexOnDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to rename duckdb table: {source}"))]
    UnableToRenameDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to insert into duckdb table: {source}"))]
    UnableToInsertToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to get appender to duckdb table: {source}"))]
    UnableToGetAppenderToDuckDBTable { source: duckdb::Error },

    #[snafu(display("Unable to delete data from the duckdb table: {source}"))]
    UnableToDeleteDuckdbData { source: duckdb::Error },

    #[snafu(display("Unable to query data from the duckdb table: {source}"))]
    UnableToQueryData { source: duckdb::Error },

    #[snafu(display("Unable to commit transaction: {source}"))]
    UnableToCommitTransaction { source: duckdb::Error },

    #[snafu(display("Unable to begin duckdb transaction: {source}"))]
    UnableToBeginTransaction { source: duckdb::Error },

    #[snafu(display("Unable to rollback transaction: {source}"))]
    UnableToRollbackTransaction { source: duckdb::Error },

    #[snafu(display("Unable to delete all data from the DuckDB table: {source}"))]
    UnableToDeleteAllTableData { source: duckdb::Error },

    #[snafu(display("Unable to insert data into the DuckDB table: {source}"))]
    UnableToInsertIntoTableAsync { source: duckdb::Error },

    #[snafu(display("The table '{table_name}' doesn't exist in the DuckDB server"))]
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

    #[snafu(display("Failed to parse memory_limit value '{value}': {source}\nProvide a valid value, e.g. '2GB', '512MiB' (expected: KB, MB, GB, TB for 1000^i units or KiB, MiB, GiB, TiB for 1024^i units)"))]
    UnableToParseMemoryLimit {
        value: String,
        source: byte_unit::ParseError,
    },

    #[snafu(display("Unable to add primary key to table: {source}"))]
    UnableToAddPrimaryKey { source: duckdb::Error },

    #[snafu(display("Failed to get system time since epoch: {source}"))]
    UnableToGetSystemTime { source: std::time::SystemTimeError },

    #[snafu(display("Failed to parse the system time: {source}"))]
    UnableToParseSystemTime { source: std::num::ParseIntError },

    #[snafu(display("Failed to parse 'connection_pool_size' value '{value}': {source}. Provide a valid positive integer value, e.g. '10', '20' and try again."))]
    UnableToParseConnectionPoolSize {
        value: String,
        source: std::num::ParseIntError,
    },

    #[snafu(display("A read provider is required to create a DuckDBTableWriter"))]
    MissingReadProvider,

    #[snafu(display("A pool is required to create a DuckDBTableWriter"))]
    MissingPool,

    #[snafu(display("A table definition is required to create a DuckDBTableWriter"))]
    MissingTableDefinition,

    #[snafu(display("Failed to register Arrow scan view for DuckDB ingestion: {source}"))]
    UnableToRegisterArrowScanView { source: duckdb::Error },

    #[snafu(display(
        "Failed to register Arrow scan view to build table creation statement: {source}"
    ))]
    UnableToRegisterArrowScanViewForTableCreation { source: duckdb::Error },

    #[snafu(display("Failed to drop Arrow scan view for DuckDB ingestion: {source}"))]
    UnableToDropArrowScanView { source: duckdb::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

const DUCKDB_DB_PATH_PARAM: &str = "open";
const DUCKDB_DB_BASE_FOLDER_PARAM: &str = "data_directory";
const DUCKDB_ATTACH_DATABASES_PARAM: &str = "attach_databases";

pub struct DuckDBTableProviderFactory {
    access_mode: AccessMode,
    instances: Arc<Mutex<HashMap<DbInstanceKey, DuckDbConnectionPool>>>,
    unsupported_type_action: UnsupportedTypeAction,
    dialect: Arc<dyn Dialect>,
    settings_registry: DuckDBSettingsRegistry,
    function_support: Option<FunctionSupport>,
}

// Dialect trait does not implement Debug so we implement Debug manually
impl std::fmt::Debug for DuckDBTableProviderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBTableProviderFactory")
            .field("access_mode", &self.access_mode)
            .field("instances", &self.instances)
            .field("unsupported_type_action", &self.unsupported_type_action)
            .field("settings_registry", &self.settings_registry)
            .finish()
    }
}

impl DuckDBTableProviderFactory {
    #[must_use]
    pub fn new(access_mode: AccessMode) -> Self {
        Self {
            access_mode,
            instances: Arc::new(Mutex::new(HashMap::new())),
            unsupported_type_action: UnsupportedTypeAction::Error,
            dialect: Arc::new(DuckDBDialect::new()),
            settings_registry: DuckDBSettingsRegistry::new(),
            function_support: None,
        }
    }

    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    #[must_use]
    pub fn with_unsupported_type_action(
        mut self,
        unsupported_type_action: UnsupportedTypeAction,
    ) -> Self {
        self.unsupported_type_action = unsupported_type_action;
        self
    }

    #[must_use]
    pub fn with_dialect(mut self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        self.dialect = dialect;
        self
    }

    #[must_use]
    pub fn with_settings_registry(mut self, settings_registry: DuckDBSettingsRegistry) -> Self {
        self.settings_registry = settings_registry;
        self
    }

    #[must_use]
    pub fn settings_registry(&self) -> &DuckDBSettingsRegistry {
        &self.settings_registry
    }

    #[must_use]
    pub fn settings_registry_mut(&mut self) -> &mut DuckDBSettingsRegistry {
        &mut self.settings_registry
    }

    #[must_use]
    pub fn attach_databases(&self, options: &HashMap<String, String>) -> Vec<Arc<str>> {
        options
            .get(DUCKDB_ATTACH_DATABASES_PARAM)
            .map(|attach_databases| {
                attach_databases
                    .split(';')
                    .map(Arc::from)
                    .collect::<Vec<Arc<str>>>()
            })
            .unwrap_or_default()
    }

    /// Get the path to the DuckDB file database.
    ///
    /// ## Errors
    ///
    /// - If the path includes absolute sequences to escape the current directory, like `./`, `../`, or `/`.
    pub fn duckdb_file_path(
        &self,
        name: &str,
        options: &mut HashMap<String, String>,
    ) -> Result<String, Error> {
        let options = util::remove_prefix_from_hashmap_keys(options.clone(), "duckdb_");

        let db_base_folder = options
            .get(DUCKDB_DB_BASE_FOLDER_PARAM)
            .cloned()
            .unwrap_or(".".to_string()); // default to the current directory
        let default_filepath = &format!("{db_base_folder}/{name}.db");

        let filepath = options
            .get(DUCKDB_DB_PATH_PARAM)
            .unwrap_or(default_filepath);

        Ok(filepath.to_string())
    }

    pub async fn get_or_init_memory_instance(
        &self,
        options: &HashMap<String, String>,
    ) -> Result<DuckDbConnectionPool> {
        let mut pool_builder = DuckDbConnectionPoolBuilder::memory();

        if let Some(max_size) = extract_connection_pool_size(options)? {
            pool_builder = pool_builder.with_max_size(Some(max_size));
        }

        self.get_or_init_instance_with_builder(pool_builder).await
    }

    pub async fn get_or_init_file_instance(
        &self,
        db_path: impl Into<Arc<str>>,
        options: &HashMap<String, String>,
    ) -> Result<DuckDbConnectionPool> {
        let db_path: Arc<str> = db_path.into();
        let mut pool_builder = DuckDbConnectionPoolBuilder::file(&db_path);

        if let Some(max_size) = extract_connection_pool_size(options)? {
            pool_builder = pool_builder.with_max_size(Some(max_size));
        }

        self.get_or_init_instance_with_builder(pool_builder).await
    }

    pub async fn get_or_init_instance_with_builder(
        &self,
        pool_builder: DuckDbConnectionPoolBuilder,
    ) -> Result<DuckDbConnectionPool> {
        let mode = pool_builder.get_mode();
        let key = match mode {
            Mode::File => {
                let path = pool_builder.get_path();
                DbInstanceKey::file(path.into())
            }
            Mode::Memory => DbInstanceKey::memory(),
        };

        let access_mode = match &self.access_mode {
            AccessMode::ReadOnly => AccessMode::ReadOnly,
            AccessMode::ReadWrite => AccessMode::ReadWrite,
            AccessMode::Automatic => AccessMode::Automatic,
        };
        let pool_builder = pool_builder.with_access_mode(access_mode);

        let mut instances = self.instances.lock().await;

        if let Some(instance) = instances.get(&key) {
            return Ok(instance.clone());
        }

        let pool = pool_builder
            .build()
            .context(DbConnectionPoolSnafu)?
            .with_unsupported_type_action(self.unsupported_type_action);

        instances.insert(key, pool.clone());

        Ok(pool)
    }
}

type DynDuckDbConnectionPool = dyn DbConnectionPool<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>
    + Send
    + Sync;

#[async_trait]
impl TableProviderFactory for DuckDBTableProviderFactory {
    #[allow(clippy::too_many_lines)]
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

        let name = cmd.name.to_string();
        let mut options = cmd.options.clone();
        let mode = remove_option(&mut options, "mode").unwrap_or_default();
        let mode: Mode = mode.as_str().into();

        let indexes_option_str = remove_option(&mut options, "indexes");
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
        if let Some(on_conflict_str) = remove_option(&mut options, "on_conflict") {
            on_conflict = Some(
                OnConflict::try_from(on_conflict_str.as_str())
                    .context(UnableToParseOnConflictSnafu)
                    .map_err(to_datafusion_error)?,
            );
        }

        let pool: DuckDbConnectionPool = match &mode {
            Mode::File => {
                // open duckdb at given path or create a new one
                let db_path = self
                    .duckdb_file_path(&name, &mut options)
                    .map_err(to_datafusion_error)?;

                self.get_or_init_file_instance(db_path, &options)
                    .await
                    .map_err(to_datafusion_error)?
            }
            Mode::Memory => self
                .get_or_init_memory_instance(&options)
                .await
                .map_err(to_datafusion_error)?,
        };

        let read_pool = match &mode {
            Mode::File => {
                let read_pool = pool.clone();

                read_pool.set_attached_databases(&self.attach_databases(&options))
            }
            Mode::Memory => pool.clone(),
        };

        // Get local DuckDB SET statements to use as setup queries on the pool
        let local_settings = self
            .settings_registry
            .get_setting_statements(&options, DuckDBSettingScope::Local);

        let read_pool = read_pool.with_connection_setup_queries(local_settings);

        let schema: SchemaRef = Arc::new(cmd.schema.as_ref().into());

        let table_definition =
            TableDefinition::new(RelationName::new(name.clone()), Arc::clone(&schema))
                .with_constraints(cmd.constraints.clone())
                .with_indexes(indexes.clone());

        let pool = Arc::new(pool);
        make_initial_table(Arc::new(table_definition.clone()), &pool)?;

        let write_settings = DuckDBWriteSettings::from_params(&options);

        let table_writer_builder = DuckDBTableWriterBuilder::new()
            .with_table_definition(table_definition)
            .with_pool(pool)
            .set_on_conflict(on_conflict)
            .with_write_settings(write_settings);

        let dyn_pool: Arc<DynDuckDbConnectionPool> = Arc::new(read_pool);

        let db_conn = dyn_pool.connect().await?;
        let Some(conn) = db_conn.as_sync() else {
            return Err(DataFusionError::External(Box::new(
                Error::DbConnectionError {
                    source: "Failed to get sync DuckDbConnection using DbConnection".into(),
                },
            )));
        };

        // Apply DuckDB global settings
        self.settings_registry
            .apply_settings(conn, &options, DuckDBSettingScope::Global)?;

        let read_provider = Arc::new(DuckDBTable::new_with_schema(
            &dyn_pool,
            Arc::clone(&schema),
            TableReference::bare(name.clone()),
            None,
            Some(self.dialect.clone()),
            Some(cmd.constraints.clone()),
            self.function_support.clone(),
            indexes,
        ));

        #[cfg(feature = "duckdb-federation")]
        let read_provider: Arc<dyn TableProvider> =
            Arc::new(read_provider.create_federated_table_provider()?);

        Ok(Arc::new(
            table_writer_builder
                .with_read_provider(read_provider)
                .build()
                .map_err(to_datafusion_error)?,
        ))
    }
}

fn to_datafusion_error(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

pub struct DuckDB {
    table_name: String,
    pool: Arc<DuckDbConnectionPool>,
    schema: SchemaRef,
    constraints: Constraints,
}

impl std::fmt::Debug for DuckDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDB")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("constraints", &self.constraints)
            .finish()
    }
}

impl DuckDB {
    #[must_use]
    pub fn existing_table(
        table_name: String,
        pool: Arc<DuckDbConnectionPool>,
        schema: SchemaRef,
        constraints: Constraints,
    ) -> Self {
        Self {
            table_name,
            pool,
            schema,
            constraints,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    #[must_use]
    pub fn constraints(&self) -> &Constraints {
        &self.constraints
    }

    pub fn connect_sync(
        &self,
    ) -> Result<
        Box<dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>>,
    > {
        Arc::clone(&self.pool)
            .connect_sync()
            .context(DbConnectionSnafu)
    }

    pub fn duckdb_conn(
        db_connection: &mut Box<
            dyn DbConnection<r2d2::PooledConnection<DuckdbConnectionManager>, DuckDBParameter>,
        >,
    ) -> Result<&mut DuckDbConnection> {
        db_connection
            .as_any_mut()
            .downcast_mut::<DuckDbConnection>()
            .context(UnableToDowncastDbConnectionSnafu)
    }
}

fn remove_option(options: &mut HashMap<String, String>, key: &str) -> Option<String> {
    options
        .remove(key)
        .or_else(|| options.remove(&format!("duckdb.{key}")))
}

fn extract_connection_pool_size(options: &HashMap<String, String>) -> Result<Option<u32>> {
    if let Some(pool_size_str) = options.get("connection_pool_size") {
        pool_size_str
            .parse()
            .context(UnableToParseConnectionPoolSizeSnafu {
                value: pool_size_str.clone(),
            })
            .map(Some)
    } else {
        Ok(None)
    }
}

pub struct DuckDBTableFactory {
    pool: Arc<DuckDbConnectionPool>,
    dialect: Arc<dyn Dialect>,
    schema: Option<SchemaRef>,
    function_support: Option<FunctionSupport>,
    indexes: Vec<(ColumnReference, IndexType)>,
}

impl DuckDBTableFactory {
    #[must_use]
    pub fn new(pool: Arc<DuckDbConnectionPool>) -> Self {
        Self {
            pool,
            dialect: Arc::new(DuckDBDialect::new()),
            schema: None,
            function_support: None,
            indexes: vec![],
        }
    }

    #[must_use]
    pub fn with_function_support(mut self, function_support: FunctionSupport) -> Self {
        self.function_support = Some(function_support);
        self
    }

    #[must_use]
    pub fn with_dialect(mut self, dialect: Arc<dyn Dialect + Send + Sync>) -> Self {
        self.dialect = dialect;
        self
    }

    #[must_use]
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    #[must_use]
    pub fn with_indexes(mut self, indexes: Vec<(ColumnReference, IndexType)>) -> Self {
        self.indexes = indexes;
        self
    }

    pub async fn table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let pool = Arc::clone(&self.pool);
        let conn = Arc::clone(&pool).connect().await?;
        let dyn_pool: Arc<DynDuckDbConnectionPool> = pool;

        let schema = match self.schema.as_ref() {
            Some(schema) => Arc::clone(schema),
            None => get_schema(conn, &table_reference).await?,
        };
        let (tbl_ref, cte) = if is_table_function(&table_reference) {
            let tbl_ref_view = create_table_function_view_name(&table_reference);
            (
                tbl_ref_view.clone(),
                Some(HashMap::from_iter(vec![(
                    tbl_ref_view.to_string(),
                    table_reference.table().to_string(),
                )])),
            )
        } else {
            (table_reference.clone(), None)
        };

        let table_provider = Arc::new(DuckDBTable::new_with_schema(
            &dyn_pool,
            schema,
            tbl_ref,
            cte,
            Some(self.dialect.clone()),
            None,
            self.function_support.clone(),
            self.indexes.clone(),
        ));

        #[cfg(feature = "duckdb-federation")]
        let table_provider: Arc<dyn TableProvider> =
            Arc::new(table_provider.create_federated_table_provider()?);

        Ok(table_provider)
    }

    pub async fn read_write_table_provider(
        &self,
        table_reference: TableReference,
    ) -> Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>> {
        let read_provider = Self::table_provider(self, table_reference.clone()).await?;
        let schema = read_provider.schema();

        let table_name = RelationName::from(table_reference);
        let table_definition = TableDefinition::new(table_name, Arc::clone(&schema));
        let table_writer_builder = DuckDBTableWriterBuilder::new()
            .with_read_provider(read_provider)
            .with_pool(Arc::clone(&self.pool))
            .with_table_definition(table_definition);

        Ok(Arc::new(table_writer_builder.build()?))
    }
}

/// For a [`TableReference`] that is a table function, create a name for a view on the original [`TableReference`]
///
/// ### Example
///
/// ```rust,ignore
/// use datafusion_table_providers::duckdb::create_table_function_view_name;
/// use datafusion::common::TableReference;
///
/// let table_reference = TableReference::from("catalog.schema.read_parquet('cleaned_sales_data.parquet')");
/// let view_name = create_table_function_view_name(&table_reference);
/// assert_eq!(view_name.to_string(), "catalog.schema.read_parquet_cleaned_sales_dataparquet__view");
/// ```
fn create_table_function_view_name(table_reference: &TableReference) -> TableReference {
    let tbl_ref_view = [
        table_reference.catalog(),
        table_reference.schema(),
        Some(&flatten_table_function_name(table_reference)),
    ]
    .iter()
    .flatten()
    .join(".");
    TableReference::from(&tbl_ref_view)
}

pub(crate) fn make_initial_table(
    table_definition: Arc<TableDefinition>,
    pool: &Arc<DuckDbConnectionPool>,
) -> DataFusionResult<()> {
    let cloned_pool = Arc::clone(pool);
    let mut db_conn = Arc::clone(&cloned_pool)
        .connect_sync()
        .context(DbConnectionPoolSnafu)
        .map_err(to_datafusion_error)?;

    let duckdb_conn = DuckDB::duckdb_conn(&mut db_conn).map_err(to_datafusion_error)?;

    let tx = duckdb_conn
        .conn
        .transaction()
        .context(UnableToBeginTransactionSnafu)
        .map_err(to_datafusion_error)?;

    let has_table = table_definition
        .has_table(&tx)
        .map_err(to_datafusion_error)?;
    let internal_tables = table_definition
        .list_internal_tables(&tx)
        .map_err(to_datafusion_error)?;

    if has_table || !internal_tables.is_empty() {
        return Ok(());
    }

    let table_manager = TableManager::new(table_definition);

    table_manager
        .create_table(cloned_pool, &tx)
        .map_err(to_datafusion_error)?;

    tx.commit()
        .context(UnableToCommitTransactionSnafu)
        .map_err(to_datafusion_error)?;

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::duckdb::write::DuckDBTableWriter;

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Constraints, ToDFSchema};
    use datafusion::logical_expr::CreateExternalTable;
    use datafusion::prelude::SessionContext;
    use datafusion::sql::TableReference;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_create_with_memory_limit() {
        let table_name = TableReference::bare("test_table");
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int32, false)]);

        let mut options = HashMap::new();
        options.insert("mode".to_string(), "memory".to_string());
        options.insert("memory_limit".to_string(), "123MiB".to_string());

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(schema.to_dfschema().expect("to df schema")),
            name: table_name,
            location: "".to_string(),
            file_type: "".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };

        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("table provider created");

        let writer = table_provider
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .expect("cast to DuckDBTableWriter");

        let mut conn_box = writer.pool().connect_sync().expect("to get connection");
        let conn = DuckDB::duckdb_conn(&mut conn_box).expect("to get DuckDB connection");

        let mut stmt = conn
            .conn
            .prepare("SELECT value FROM duckdb_settings() WHERE name = 'memory_limit'")
            .expect("to prepare statement");

        let memory_limit = stmt
            .query_row([], |row| row.get::<usize, String>(0))
            .expect("to query memory limit");

        println!("Memory limit: {memory_limit}");

        assert_eq!(
            memory_limit, "123.0 MiB",
            "Memory limit must be set to 123.0 MiB"
        );
    }

    #[tokio::test]
    async fn test_create_with_temp_directory() {
        let table_name = TableReference::bare("test_table_temp_dir");
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int32, false)]);

        let test_temp_directory = "/tmp/duckdb_test_temp";
        let mut options = HashMap::new();
        options.insert("mode".to_string(), "memory".to_string());
        options.insert(
            "temp_directory".to_string(),
            test_temp_directory.to_string(),
        );

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(schema.to_dfschema().expect("to df schema")),
            name: table_name,
            location: "".to_string(),
            file_type: "".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };

        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("table provider created");

        let writer = table_provider
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .expect("cast to DuckDBTableWriter");

        let mut conn_box = writer.pool().connect_sync().expect("to get connection");
        let conn = DuckDB::duckdb_conn(&mut conn_box).expect("to get DuckDB connection");

        let mut stmt = conn
            .conn
            .prepare("SELECT value FROM duckdb_settings() WHERE name = 'temp_directory'")
            .expect("to prepare statement");

        let temp_directory = stmt
            .query_row([], |row| row.get::<usize, String>(0))
            .expect("to query temp directory");

        println!("Temp directory: {temp_directory}");

        assert_eq!(
            temp_directory, test_temp_directory,
            "Temp directory must be set to {test_temp_directory}"
        );
    }

    #[tokio::test]
    async fn test_create_with_preserve_insertion_order_true() {
        let table_name = TableReference::bare("test_table_preserve_order_true");
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int32, false)]);

        let mut options = HashMap::new();
        options.insert("mode".to_string(), "memory".to_string());
        options.insert("preserve_insertion_order".to_string(), "true".to_string());

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(schema.to_dfschema().expect("to df schema")),
            name: table_name,
            location: "".to_string(),
            file_type: "".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };

        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("table provider created");

        let writer = table_provider
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .expect("cast to DuckDBTableWriter");

        let mut conn_box = writer.pool().connect_sync().expect("to get connection");
        let conn = DuckDB::duckdb_conn(&mut conn_box).expect("to get DuckDB connection");

        let mut stmt = conn
            .conn
            .prepare("SELECT value FROM duckdb_settings() WHERE name = 'preserve_insertion_order'")
            .expect("to prepare statement");

        let preserve_order = stmt
            .query_row([], |row| row.get::<usize, String>(0))
            .expect("to query preserve_insertion_order");

        assert_eq!(
            preserve_order, "true",
            "preserve_insertion_order must be set to true"
        );
    }

    #[tokio::test]
    async fn test_create_with_preserve_insertion_order_false() {
        let table_name = TableReference::bare("test_table_preserve_order_false");
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int32, false)]);

        let mut options = HashMap::new();
        options.insert("mode".to_string(), "memory".to_string());
        options.insert("preserve_insertion_order".to_string(), "false".to_string());

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(schema.to_dfschema().expect("to df schema")),
            name: table_name,
            location: "".to_string(),
            file_type: "".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };

        let table_provider = factory
            .create(&ctx.state(), &cmd)
            .await
            .expect("table provider created");

        let writer = table_provider
            .as_any()
            .downcast_ref::<DuckDBTableWriter>()
            .expect("cast to DuckDBTableWriter");

        let mut conn_box = writer.pool().connect_sync().expect("to get connection");
        let conn = DuckDB::duckdb_conn(&mut conn_box).expect("to get DuckDB connection");

        let mut stmt = conn
            .conn
            .prepare("SELECT value FROM duckdb_settings() WHERE name = 'preserve_insertion_order'")
            .expect("to prepare statement");

        let preserve_order = stmt
            .query_row([], |row| row.get::<usize, String>(0))
            .expect("to query preserve_insertion_order");

        assert_eq!(
            preserve_order, "false",
            "preserve_insertion_order must be set to false"
        );
    }

    #[tokio::test]
    async fn test_create_with_invalid_preserve_insertion_order() {
        let table_name = TableReference::bare("test_table_preserve_order_invalid");
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int32, false)]);

        let mut options = HashMap::new();
        options.insert("mode".to_string(), "memory".to_string());
        options.insert(
            "preserve_insertion_order".to_string(),
            "invalid".to_string(),
        );

        let factory = DuckDBTableProviderFactory::new(duckdb::AccessMode::ReadWrite);
        let ctx = SessionContext::new();
        let cmd = CreateExternalTable {
            schema: Arc::new(schema.to_dfschema().expect("to df schema")),
            name: table_name,
            location: "".to_string(),
            file_type: "".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            temporary: false,
        };

        let result = factory.create(&ctx.state(), &cmd).await;
        assert!(
            result.is_err(),
            "Should fail with invalid preserve_insertion_order value"
        );
        if let Err(e) = result {
            assert_eq!(e.to_string(), "External error: Query execution failed.\nInvalid Input Error: Failed to cast value: Could not convert string 'invalid' to BOOL\nFor details, refer to the DuckDB manual: https://duckdb.org/docs/");
        }
    }
}
