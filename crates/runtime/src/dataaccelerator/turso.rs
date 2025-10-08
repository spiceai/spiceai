/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use data_components::delete::{DeletionExec, DeletionSink, DeletionTableProvider};
use data_components::poly::PolyTableProvider;
use datafusion::{
    catalog::Session,
    datasource::{
        TableProvider,
        sink::{DataSink, DataSinkExec},
    },
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{CreateExternalTable, Expr, TableType, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
    sql::unparser::{Unparser, dialect::SqliteDialect},
};
use futures::stream::{self, StreamExt, TryStreamExt};
use runtime_table_partition::expression::PartitionBy;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, fmt, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;
use turso::{Builder, Connection, Database, Value as TursoValue};

use crate::{
    component::dataset::acceleration::Engine,
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    make_spice_data_directory,
    parameters::ParameterSpec,
    spice_data_base_path,
};

use super::{AccelerationSource, DataAccelerator};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create table: {source}"))]
    UnableToCreateTable {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Acceleration creation failed: {source}"))]
    AccelerationCreationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Acceleration initialization failed: {source}"))]
    AccelerationInitializationFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "The \"turso_file\" acceleration parameter has an invalid extension. Expected one of \"{valid_extensions}\" but got \"{extension}\"."
    ))]
    InvalidFileExtension {
        valid_extensions: String,
        extension: String,
    },

    #[snafu(display("The \"turso_file\" acceleration parameter value is a directory."))]
    InvalidFileIsDirectory,

    #[snafu(display("Acceleration not enabled for dataset: {dataset}"))]
    AccelerationNotEnabled { dataset: Arc<str> },

    #[snafu(display("Invalid Turso acceleration configuration: {detail}"))]
    InvalidConfiguration { detail: Arc<str> },

    #[snafu(display("Turso database error: {source}"))]
    TursoDatabaseError { source: turso::Error },

    #[snafu(display(
        "Turso only supports file mode acceleration. Memory mode is not supported. Please set mode: file in your acceleration configuration."
    ))]
    MemoryModeNotSupported,

    #[snafu(display(
        "Remote Turso databases are not supported when using Turso as a file accelerator. Remote database support (turso_url, turso_auth_token) will be available when Turso is used as a data connector."
    ))]
    RemoteDatabaseNotSupported,
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Connection pool for Turso databases
#[derive(Debug)]
pub struct TursoConnectionPool {
    database: Arc<Database>,
}

impl TursoConnectionPool {
    pub async fn new(path: &str) -> Result<Self> {
        // Turso does not support in-memory mode due to threading requirements
        if path == ":memory:" {
            return Err(Error::MemoryModeNotSupported);
        }

        let database = Builder::new_local(path)
            .build()
            .await
            .context(TursoDatabaseSnafu)?;

        Ok(Self {
            database: Arc::new(database),
        })
    }

    pub async fn connect(&self) -> Result<Connection> {
        self.database.connect().context(TursoDatabaseSnafu)
    }
}

/// Turso Table Provider for reading data
#[derive(Debug)]
pub struct TursoTableProvider {
    schema: SchemaRef,
    table_name: String,
    pool: Arc<TursoConnectionPool>,
}

impl TursoTableProvider {
    pub fn new(schema: SchemaRef, table_name: String, pool: Arc<TursoConnectionPool>) -> Self {
        Self {
            schema,
            table_name,
            pool,
        }
    }

    fn values_to_record_batch(
        rows: &[Vec<TursoValue>],
        schema: &SchemaRef,
    ) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        use arrow::array::*;

        let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column: Arc<dyn arrow::array::Array> = match field.data_type() {
                DataType::Int64 => {
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Int64Array::from(values))
                }
                DataType::Int32 => {
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => i32::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arrow::array::Int32Array::from(values))
                }
                DataType::UInt64 => {
                    let values: Vec<Option<u64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => u64::try_from(*i).ok(),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arrow::array::UInt64Array::from(values))
                }
                DataType::Float64 => {
                    let values: Vec<Option<f64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => Some(*f),
                            TursoValue::Integer(i) => Some(*i as f64),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(Float64Array::from(values))
                }
                DataType::Float32 => {
                    let values: Vec<Option<f32>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Real(f) => Some(*f as f32),
                            TursoValue::Integer(i) => Some(*i as f32),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arrow::array::Float32Array::from(values))
                }
                DataType::Utf8 => {
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => Some(s.clone()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
                DataType::Boolean => {
                    let values: Vec<Option<bool>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i != 0),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(BooleanArray::from(values))
                }
                DataType::Binary | DataType::LargeBinary => {
                    let values: Vec<Option<&[u8]>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Blob(b) => Some(b.as_slice()),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(BinaryArray::from(values))
                }
                DataType::Timestamp(_unit, _tz) => {
                    // Timestamps stored as INTEGER (milliseconds since epoch)
                    let values: Vec<Option<i64>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Integer(i) => Some(*i),
                            TursoValue::Null => None,
                            _ => None,
                        })
                        .collect();
                    Arc::new(
                        arrow::array::TimestampMillisecondArray::from(values)
                            .with_timezone_opt(None::<String>),
                    )
                }
                _ => {
                    // Default to string representation for unsupported types
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| match &row[col_idx] {
                            TursoValue::Text(s) => Some(s.clone()),
                            TursoValue::Integer(i) => Some(i.to_string()),
                            TursoValue::Real(f) => Some(f.to_string()),
                            TursoValue::Null => None,
                            TursoValue::Blob(_) => Some("[BLOB]".to_string()),
                        })
                        .collect();
                    Arc::new(StringArray::from(values))
                }
            };
            columns.push(column);
        }

        Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
    }
}

#[async_trait]
impl TableProvider for TursoTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };

        Ok(Arc::new(TursoExec::new(
            Arc::clone(&projected_schema),
            self.table_name.clone(),
            Arc::clone(&self.pool),
            projection.cloned(),
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Turso does not support UPSERT/ON CONFLICT operations yet
        // Warn if overwrite mode is requested
        if !matches!(overwrite, InsertOp::Append) {
            tracing::warn!(
                "Turso accelerator does not support UPSERT/ON CONFLICT operations. InsertOp::{:?} will be treated as Append.",
                overwrite
            );
        }

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(TursoDataSink::new(
                Arc::clone(&self.pool),
                self.table_name.clone(),
                Arc::clone(&self.schema),
            )),
            None,
        )) as _)
    }
}

#[async_trait]
impl DeletionTableProvider for TursoTableProvider {
    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: &[Expr],
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeletionExec::new(
            Arc::new(TursoDeletionSink::new(
                Arc::clone(&self.pool),
                self.table_name.clone(),
                filters,
            )),
            &self.schema(),
        )))
    }
}

/// Execution plan for Turso queries
#[derive(Debug)]
pub struct TursoExec {
    schema: SchemaRef,
    table_name: String,
    pool: Arc<TursoConnectionPool>,
    #[allow(dead_code)] // Stored for future optimization of column selection
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl TursoExec {
    pub fn new(
        schema: SchemaRef,
        table_name: String,
        pool: Arc<TursoConnectionPool>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            table_name,
            pool,
            projection,
            properties,
        }
    }
}

impl DisplayAs for TursoExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TursoExec: table={}", self.table_name)
    }
}

impl ExecutionPlan for TursoExec {
    fn name(&self) -> &str {
        "TursoExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let pool = Arc::clone(&self.pool);
        let table_name = self.table_name.clone();
        let schema = Arc::clone(&self.schema);

        let stream = async move {
            let conn = pool
                .connect()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let query = format!("SELECT * FROM {}", table_name);
            let mut stmt = conn
                .prepare(&query)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut rows = stmt
                .query(())
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut all_rows = Vec::new();
            while let Some(row) = rows
                .next()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
            {
                let mut values = Vec::new();
                for i in 0..schema.fields().len() {
                    let value = row
                        .get_value(i)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    values.push(value);
                }
                all_rows.push(values);
            }

            if all_rows.is_empty() {
                return Ok::<_, datafusion::error::DataFusionError>(stream::empty().boxed());
            }

            let batch = TursoTableProvider::values_to_record_batch(&all_rows, &schema)
                .map_err(datafusion::error::DataFusionError::External)?;

            Ok::<_, datafusion::error::DataFusionError>(
                stream::once(async move { Ok(batch) }).boxed(),
            )
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream::once(stream).try_flatten(),
        )))
    }
}

/// Data sink for INSERT operations
#[derive(Debug)]
struct TursoDataSink {
    pool: Arc<TursoConnectionPool>,
    table_name: String,
    schema: SchemaRef,
}

impl DisplayAs for TursoDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TursoDataSink(table={})", self.table_name)
    }
}

impl TursoDataSink {
    fn new(pool: Arc<TursoConnectionPool>, table_name: String, schema: SchemaRef) -> Self {
        Self {
            pool,
            table_name,
            schema,
        }
    }

    async fn insert_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.connect().await?;

        // Build column list for INSERT statement
        let columns: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Build a batch SQL statement with all INSERT statements
        let mut batch_sql = String::new();
        batch_sql.push_str("BEGIN;\n");

        // Build INSERT statements for each row
        for row_idx in 0..batch.num_rows() {
            let mut values_str = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = ScalarValue::try_from_array(column, row_idx)?;

                // Convert DataFusion ScalarValue to SQL literal string
                let sql_value = match value {
                    ScalarValue::Int64(Some(v)) => v.to_string(),
                    ScalarValue::Int32(Some(v)) => v.to_string(),
                    ScalarValue::Int16(Some(v)) => v.to_string(),
                    ScalarValue::Int8(Some(v)) => v.to_string(),
                    ScalarValue::UInt64(Some(v)) => v.to_string(),
                    ScalarValue::UInt32(Some(v)) => v.to_string(),
                    ScalarValue::UInt16(Some(v)) => v.to_string(),
                    ScalarValue::UInt8(Some(v)) => v.to_string(),
                    ScalarValue::Float64(Some(v)) => v.to_string(),
                    ScalarValue::Float32(Some(v)) => v.to_string(),
                    ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                        format!("'{}'", v.replace('\'', "''"))
                    }
                    ScalarValue::Boolean(Some(v)) => {
                        if v {
                            "1".to_string()
                        } else {
                            "0".to_string()
                        }
                    }
                    ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => {
                        format!("X'{}'", hex::encode(v))
                    }
                    ScalarValue::TimestampMillisecond(Some(v), _) => v.to_string(),
                    ScalarValue::TimestampMicrosecond(Some(v), _) => (v / 1000).to_string(),
                    ScalarValue::TimestampNanosecond(Some(v), _) => (v / 1_000_000).to_string(),
                    ScalarValue::TimestampSecond(Some(v), _) => (v * 1000).to_string(),
                    ScalarValue::Date32(Some(v)) => v.to_string(),
                    ScalarValue::Date64(Some(v)) => v.to_string(),
                    _ => "NULL".to_string(),
                };
                values_str.push(sql_value);
            }

            batch_sql.push_str(&format!(
                "INSERT INTO {} ({}) VALUES ({});\n",
                self.table_name,
                columns.join(", "),
                values_str.join(", ")
            ));
        }

        batch_sql.push_str("COMMIT;");

        // Execute the entire batch in one call
        conn.execute_batch(&batch_sql).await?;

        Ok(())
    }
}

#[async_trait]
impl DataSink for TursoDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let mut total_rows = 0u64;

        while let Some(batch) = data.next().await {
            let batch = batch?;
            total_rows += batch.num_rows() as u64;
            self.insert_batch(&batch)
                .await
                .map_err(datafusion::error::DataFusionError::External)?;
        }

        Ok(total_rows)
    }
}

/// Deletion sink for DELETE operations
struct TursoDeletionSink {
    pool: Arc<TursoConnectionPool>,
    table_name: String,
    filters: Vec<Expr>,
}

impl TursoDeletionSink {
    fn new(pool: Arc<TursoConnectionPool>, table_name: String, filters: &[Expr]) -> Self {
        Self {
            pool,
            table_name,
            filters: filters.to_vec(),
        }
    }
}

#[async_trait]
impl DeletionSink for TursoDeletionSink {
    async fn delete_from(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        // Build WHERE clause using SQLite dialect unparser (before async)
        let where_clause = if self.filters.is_empty() {
            String::new()
        } else {
            let dialect = SqliteDialect {};
            let unparser = Unparser::new(&dialect);
            let filter_sqls: Vec<String> = self
                .filters
                .iter()
                .map(|f| unparser.expr_to_sql(f).map(|ast| format!("{ast}")))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            format!(" WHERE {}", filter_sqls.join(" AND "))
        };

        let delete_sql = format!("DELETE FROM {}{}", self.table_name, where_clause);

        let conn = self.pool.connect().await?;
        let rows_affected = conn
            .execute(&delete_sql, ())
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(rows_affected)
    }
}

pub struct TursoAccelerator {
    // Store connection pools for file-based databases
    pools: Arc<Mutex<std::collections::HashMap<String, Arc<TursoConnectionPool>>>>,
}

impl Default for TursoAccelerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TursoAccelerator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Returns the `Turso` file path that would be used for a file-based `Turso` accelerator from this dataset
    pub fn turso_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        if !source.is_file_accelerated() {
            Err(Error::InvalidConfiguration {
                detail: Arc::from("Dataset is not file accelerated"),
            })
        } else if let Some(acceleration) = source.acceleration() {
            let acceleration_params = &acceleration.params;

            // Check for remote database parameters (not supported as accelerator)
            if acceleration_params.contains_key("turso_url")
                || acceleration_params.contains_key("turso_auth_token")
            {
                return Err(Error::RemoteDatabaseNotSupported);
            }

            // Check if user specified a custom file path
            if let Some(turso_file) = acceleration_params.get("turso_file") {
                return Ok(turso_file.clone());
            }

            // Use default path based on dataset name
            let data_directory = spice_data_base_path();
            let name_str = source.name().to_string().replace('/', "_");
            let file_name = format!("{}.turso", name_str);
            let path = PathBuf::from(data_directory).join(file_name);

            Ok(path.to_string_lossy().to_string())
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }

    /// Returns an existing `Turso` connection for the given dataset, or creates a new one if it doesn't exist.
    pub async fn get_connection(&self, source: &dyn AccelerationSource) -> Result<Connection> {
        let turso_file = self.turso_file_path(source)?;

        let db = if source.is_file_accelerated() {
            Builder::new_local(&turso_file)
                .build()
                .await
                .context(TursoDatabaseSnafu)?
        } else {
            Builder::new_local(":memory:")
                .build()
                .await
                .context(TursoDatabaseSnafu)?
        };

        db.connect().context(TursoDatabaseSnafu)
    }

    /// Returns the shared connection pool for a `Turso` database
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<TursoConnectionPool>> {
        let turso_file = self.turso_file_path(source)?;

        let mut pools = self.pools.lock().await;
        if let Some(pool) = pools.get(&turso_file) {
            Ok(Arc::clone(pool))
        } else {
            let pool = Arc::new(TursoConnectionPool::new(&turso_file).await?);
            pools.insert(turso_file, Arc::clone(&pool));
            Ok(pool)
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("turso_file"),
    // Note: turso_url and turso_auth_token are not supported as accelerator parameters
    // They will be supported when Turso is implemented as a data connector
];

#[async_trait]
impl DataAccelerator for TursoAccelerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "turso"
    }

    fn valid_file_extensions(&self) -> Vec<&'static str> {
        vec!["turso", "db", "sqlite", "sqlite3"]
    }

    fn file_path(&self, source: &dyn AccelerationSource) -> Result<String, FilePathError> {
        self.turso_file_path(source)
            .map_err(|err| FilePathError::External {
                engine: Engine::Turso,
                source: err.into(),
            })
    }

    fn is_initialized(&self, source: &dyn AccelerationSource) -> bool {
        if !source.is_file_accelerated() {
            return false; // Turso requires file mode
        }

        // Check if the file exists
        self.has_existing_file(source)
    }

    /// Initializes a `Turso` database for the dataset
    /// Turso only supports file mode
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !source.is_file_accelerated() {
            return Err(Error::MemoryModeNotSupported.into());
        }

        // Check for remote database parameters early
        if let Some(acceleration) = source.acceleration() {
            if acceleration.params.contains_key("turso_url")
                || acceleration.params.contains_key("turso_auth_token")
            {
                return Err(Error::RemoteDatabaseNotSupported.into());
            }
        }

        let path = self.file_path(source)?;

        if let Some(acceleration) = source.acceleration() {
            if !acceleration.params.contains_key("turso_file") {
                make_spice_data_directory()
                    .map_err(|err| Error::AccelerationCreationFailed { source: err.into() })?;
            } else if !self.is_valid_file(source) {
                if std::path::Path::new(&path).is_dir() {
                    return Err(Error::InvalidFileIsDirectory.into());
                }

                let extension = std::path::Path::new(&path)
                    .extension()
                    .and_then(OsStr::to_str)
                    .unwrap_or("");

                return Err(Error::InvalidFileExtension {
                    valid_extensions: self.valid_file_extensions().join(","),
                    extension: extension.to_string(),
                }
                .into());
            }

            download_snapshot_if_needed(acceleration, source, PathBuf::from(path)).await;

            // Initialize the database file
            self.get_connection(source).await?;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Option<PartitionBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_none(),
            super::InvalidConfigurationSnafu {
                msg: "Turso data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        // Turso only supports file mode
        if let Some(source) = source {
            if !source.is_file_accelerated() {
                return Err(Error::MemoryModeNotSupported.into());
            }
        }

        // Determine the database path
        let db_path = if let Some(source) = source {
            self.turso_file_path(source)?
        } else if let Some(file) = cmd.options.get("file") {
            file.clone()
        } else {
            return Err(Error::MemoryModeNotSupported.into());
        };

        // Get or create connection pool
        let pool = {
            let mut pools = self.pools.lock().await;
            if let Some(pool) = pools.get(&db_path) {
                Arc::clone(pool)
            } else {
                let new_pool = Arc::new(TursoConnectionPool::new(&db_path).await?);
                pools.insert(db_path.clone(), Arc::clone(&new_pool));
                new_pool
            }
        };

        // Create the table if it doesn't exist
        let conn = pool.connect().await?;
        let table_name = cmd.name.table().to_string();

        // Build CREATE TABLE statement from schema
        let mut columns = Vec::new();
        for field in cmd.schema.fields() {
            let col_type = match field.data_type() {
                // Integer types map to SQLite INTEGER
                DataType::Int64
                | DataType::Int32
                | DataType::Int16
                | DataType::Int8
                | DataType::UInt64
                | DataType::UInt32
                | DataType::UInt16
                | DataType::UInt8 => "INTEGER",
                // Floating point types map to REAL
                DataType::Float64 | DataType::Float32 => "REAL",
                // String types map to TEXT
                DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
                // Binary types map to BLOB
                DataType::Binary | DataType::LargeBinary => "BLOB",
                // Boolean maps to INTEGER (0/1)
                DataType::Boolean => "INTEGER",
                // Timestamp types map to INTEGER (Unix timestamp in milliseconds)
                DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => "INTEGER",
                // Decimal types map to REAL
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "REAL",
                // Default to TEXT for unsupported types (serialized as JSON or string)
                _ => "TEXT",
            };
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            columns.push(format!("{} {}{}", field.name(), col_type, nullable));
        }

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name,
            columns.join(", ")
        );

        conn.execute(&create_sql, ())
            .await
            .map_err(|e| Error::AccelerationCreationFailed {
                source: Box::new(e),
            })?;

        // Create the table provider
        let schema = Arc::new(Schema::new(
            cmd.schema
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
                .collect::<Vec<_>>(),
        ));

        let turso_provider = Arc::new(TursoTableProvider::new(schema, table_name, pool));

        // Wrap in PolyTableProvider for proper read/write separation
        // This allows the table to support both reading and writing operations
        let write_provider = Arc::clone(&turso_provider);
        let delete_provider = Arc::clone(&turso_provider);
        let read_provider = turso_provider as Arc<dyn TableProvider>;

        let table_provider = Arc::new(PolyTableProvider::new(
            write_provider,
            delete_provider,
            read_provider,
        ));

        Ok(table_provider)
    }

    fn prefix(&self) -> &'static str {
        "turso"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::acceleration::Acceleration;
    use crate::component::dataset::builder::DatasetBuilder;
    use crate::{Runtime, app};
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{cast, col, dml::InsertOp, lit},
        physical_plan::collect,
        scalar::ScalarValue,
    };
    use datafusion_table_providers::util::test::MockExec;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_turso_file_initialization() {
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "turso_file_accelerator_init".to_string(),
            "turso_file_accelerator_init",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            ..Default::default()
        });

        let accelerator = TursoAccelerator::new();
        assert!(!accelerator.is_initialized(&dataset));

        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        assert!(accelerator.is_initialized(&dataset));
        assert!(accelerator.file_path(&dataset).is_ok());

        let path = accelerator.file_path(&dataset).expect("path should exist");
        assert!(std::path::Path::new(&path).exists());

        // cleanup
        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn test_remote_params_rejected() {
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        // Test with turso_url
        let mut dataset =
            DatasetBuilder::try_new("turso_remote_test_url".to_string(), "turso_remote_test_url")
                .expect("Failed to create builder")
                .with_app(Arc::new(app.clone()))
                .with_runtime(Arc::new(rt.clone()))
                .build()
                .expect("Failed to build dataset");

        let mut params = HashMap::new();
        params.insert(
            "turso_url".to_string(),
            "libsql://test.turso.io".to_string(),
        );

        dataset.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            params,
            ..Default::default()
        });

        let accelerator = TursoAccelerator::new();
        let result = accelerator.init(&dataset).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Remote Turso databases are not supported")
        );

        // Test with turso_auth_token
        let mut dataset2 = DatasetBuilder::try_new(
            "turso_remote_test_token".to_string(),
            "turso_remote_test_token",
        )
        .expect("Failed to create builder")
        .with_app(Arc::new(app))
        .with_runtime(Arc::new(rt))
        .build()
        .expect("Failed to build dataset");

        let mut params2 = HashMap::new();
        params2.insert("turso_auth_token".to_string(), "secret_token".to_string());

        dataset2.acceleration = Some(Acceleration {
            engine: Engine::Turso,
            mode: Mode::File,
            params: params2,
            ..Default::default()
        });

        let result2 = accelerator.init(&dataset2).await;
        assert!(result2.is_err());
        assert!(
            result2
                .unwrap_err()
                .to_string()
                .contains("Remote Turso databases are not supported")
        );
    }

    #[tokio::test]
    #[allow(clippy::unreadable_literal)]
    async fn test_round_trip_turso() {
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("time_in_string", DataType::Utf8, false),
            arrow::datatypes::Field::new("time_int", DataType::Int64, false),
        ]));
        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_turso_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };
        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, None)
            .await
            .expect("table should be created");

        let arr1 = StringArray::from(vec![
            "1970-01-01",
            "2012-12-01T11:11:11Z",
            "2012-12-01T11:11:12Z",
        ]);
        let arr3 = Int64Array::from(vec![0, 1354360271, 1354360272]);
        let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr1), Arc::new(arr3)])
            .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        let table =
            get_deletion_provider(table).expect("table should be returned as deletion provider");

        let filter = cast(
            col("time_in_string"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        )
        .lt(lit(ScalarValue::TimestampMillisecond(
            Some(1354360272000),
            None,
        )));
        let plan = table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![1]);
        assert_eq!(actual, &expected);

        let filter = col("time_int").lt(lit(1354360273));
        let plan = table
            .delete_from(&ctx.state(), &[filter])
            .await
            .expect("deletion should be successful");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("deletion successful");
        let actual = result
            .first()
            .expect("result should have at least one batch")
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("result should be UInt64Array");
        let expected = UInt64Array::from(vec![2]);
        assert_eq!(actual, &expected);
    }
}
