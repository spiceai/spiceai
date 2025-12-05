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

//! Turso data accelerator for high-performance local caching.
//!
//! This module provides acceleration capabilities using Turso (libSQL) as a local database
//! for caching and accelerating query performance. It supports both in-memory and file-based
//! acceleration modes.
//!
//! # Supported Features
//!
//! - **Memory mode**: Fast in-memory database for temporary caching
//! - **File mode**: Persistent file-based database for durable acceleration
//! - **MVCC support**: Multi-Version Concurrency Control for concurrent transactions
//! - **Connection pooling**: Efficient connection management via shared pools
//!
//! # Important Limitation: Accelerator Use Case Only
//!
//! This accelerator implementation **only supports local Turso databases** (file-based or
//! in-memory). Remote Turso databases using `turso_url` and `turso_auth_token` are **not
//! supported** in this accelerator context.
//!
//! **This is not a general Turso limitation** - it's specific to the accelerator use case,
//! where local database access is required for optimal performance and to support
//! acceleration-specific operations like local caching and fast query execution.
//!
//! Remote Turso database support will be available when Turso is implemented as a **data
//! connector** (for source datasets), where remote access patterns are the primary use case
//! and local acceleration is not the goal.

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use data_components::poly::PolyTableProvider;
use data_components::turso::TursoTableProvider;
use datafusion::{
    common::utils::quote_identifier, datasource::TableProvider, logical_expr::CreateExternalTable,
};
use runtime_table_partition::expression::PartitionedBy;
use snafu::prelude::*;
use std::{any::Any, ffi::OsStr, path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

use crate::{
    component::dataset::acceleration::{Engine, Mode},
    dataaccelerator::{FilePathError, snapshots::download_snapshot_if_needed},
    datafusion::udf::deny_spice_specific_functions,
    make_spice_data_directory,
    parameters::ParameterSpec,
    register_data_accelerator, spice_data_base_path,
};

use super::{AccelerationSource, DataAccelerator, upsert_dedup};

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
        "Remote Turso databases (turso_url, turso_auth_token) are not supported when using Turso as an accelerator. \
        This limitation is specific to the accelerator use case, which requires local database access for optimal performance. \
        Remote Turso database support will be available when Turso is implemented as a data connector for source datasets."
    ))]
    RemoteDatabaseNotSupported,
}

type Result<T, E = Error> = std::result::Result<T, E>;

// All Turso data components (TursoConnectionPool, TursoTableProvider, TursoExec,
// TursoDataSink, TursoDeletionSink) are now imported from data_components::turso

fn sanitize_identifier(identifier: &str, context: &str) -> Result<String> {
    let trimmed = identifier.trim();
    ensure!(
        !trimmed.is_empty(),
        InvalidConfigurationSnafu {
            detail: Arc::from(format!("{context} identifier cannot be empty"))
        }
    );

    Ok(quote_identifier(trimmed).into_owned())
}

fn sanitize_column_reference(column_ref: &str) -> Result<Vec<String>> {
    let trimmed = column_ref.trim();
    ensure!(
        !trimmed.is_empty(),
        InvalidConfigurationSnafu {
            detail: Arc::from("Index column reference cannot be empty")
        }
    );

    let inner = if trimmed.starts_with('(') {
        ensure!(
            trimmed.ends_with(')'),
            InvalidConfigurationSnafu {
                detail: Arc::from(format!(
                    "Compound index reference '{trimmed}' must end with ')'"
                ))
            }
        );
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    let mut sanitized_columns = Vec::new();
    for column in inner
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let mut parts = column.split_whitespace();
        let name = parts.next().unwrap_or_default();
        ensure!(
            !name.is_empty(),
            InvalidConfigurationSnafu {
                detail: Arc::from(format!("Invalid index column reference '{column}'"))
            }
        );
        let mut sanitized = sanitize_identifier(name, "Column")?;

        if let Some(order) = parts.next() {
            let upper = order.to_ascii_uppercase();
            ensure!(
                upper == "ASC" || upper == "DESC",
                InvalidConfigurationSnafu {
                    detail: Arc::from(format!(
                        "Unsupported index column ordering '{order}' in reference '{column}'"
                    ))
                }
            );
            sanitized.push(' ');
            sanitized.push_str(&upper);
        }

        ensure!(
            parts.next().is_none(),
            InvalidConfigurationSnafu {
                detail: Arc::from(format!(
                    "Unexpected tokens in index column reference '{column}'"
                ))
            }
        );

        sanitized_columns.push(sanitized);
    }

    ensure!(
        !sanitized_columns.is_empty(),
        InvalidConfigurationSnafu {
            detail: Arc::from(format!(
                "Index column reference '{trimmed}' did not contain any columns"
            ))
        }
    );

    Ok(sanitized_columns)
}
// Re-export for use within the runtime crate
pub use data_components::turso::TursoConnectionPool;

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

    /// Parses the `turso_mvcc` parameter from the acceleration configuration
    /// Returns true if MVCC should be enabled, false otherwise (default: disabled)
    fn parse_mvcc_enabled(source: &dyn AccelerationSource) -> Result<bool> {
        if let Some(acceleration) = source.acceleration() {
            if let Some(mvcc_value) = acceleration.params.get("turso_mvcc") {
                match mvcc_value.as_str() {
                    "enabled" => Ok(true),
                    "disabled" => Ok(false),
                    _ => Err(Error::InvalidConfiguration {
                        detail: Arc::from(format!(
                            "Invalid 'turso_mvcc' value: '{mvcc_value}'. Expected 'enabled' or 'disabled'."
                        )),
                    }),
                }
            } else {
                // Default to disabled
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    /// Parses the `internal_timestamp_format` parameter from the acceleration configuration
    /// Returns the timestamp format (default: Rfc3339)
    fn parse_timestamp_format(
        source: &dyn AccelerationSource,
    ) -> Result<data_components::turso::TimestampFormat> {
        if let Some(acceleration) = source.acceleration() {
            if let Some(format_value) = acceleration.params.get("internal_timestamp_format") {
                match format_value.as_str() {
                    "rfc3339" => Ok(data_components::turso::TimestampFormat::Rfc3339),
                    "integer_millis" => Ok(data_components::turso::TimestampFormat::IntegerMillis),
                    _ => Err(Error::InvalidConfiguration {
                        detail: Arc::from(format!(
                            "Invalid 'internal_timestamp_format' value: '{format_value}'. Expected 'rfc3339' or 'integer_millis'."
                        )),
                    }),
                }
            } else {
                // Default to RFC3339
                Ok(data_components::turso::TimestampFormat::Rfc3339)
            }
        } else {
            Ok(data_components::turso::TimestampFormat::Rfc3339)
        }
    }

    /// Returns the database path for a Turso accelerator.
    ///
    /// This function determines the appropriate database path based on the acceleration mode:
    /// - **Memory mode** (`!is_file_accelerated()`): Returns `":memory:"` for in-memory database
    /// - **File mode** (`is_file_accelerated()`): Returns a file path, which can be:
    ///   - User-specified via `turso_file` parameter, or
    ///   - Auto-generated default path: `{spice_data_dir}/{dataset_name}.turso`
    ///
    /// # Accelerator-Specific Limitation
    ///
    /// Remote Turso databases (using `turso_url` and `turso_auth_token`) are **not supported**
    /// when using Turso as a **file accelerator**. This is because accelerators require local
    /// file access for optimal performance and to support acceleration-specific operations.
    ///
    /// Remote Turso database support will be available when Turso is implemented as a data
    /// connector (for source datasets), where remote access patterns are more appropriate.
    ///
    /// # Returns
    ///
    /// - `Ok(String)` - The database path (":memory:" or file path)
    /// - `Err(Error::RemoteDatabaseNotSupported)` - If remote parameters are provided
    ///
    /// # Note
    ///
    /// This function will never return `":memory:"` when called with file mode.
    pub fn turso_file_path(&self, source: &dyn AccelerationSource) -> Result<String> {
        // Check acceleration mode first
        if !source.is_file_accelerated() {
            // Memory mode: always use in-memory database
            return Ok(":memory:".to_string());
        }

        // File mode: determine the file path to use
        if let Some(acceleration) = source.acceleration() {
            let acceleration_params = &acceleration.params;

            // Remote databases are not supported as accelerators
            if acceleration_params.contains_key("turso_url")
                || acceleration_params.contains_key("turso_auth_token")
            {
                return Err(Error::RemoteDatabaseNotSupported);
            }

            // Use custom file path if specified
            if let Some(turso_file) = acceleration_params.get("turso_file") {
                return Ok(turso_file.clone());
            }

            // Generate default file path based on dataset name
            let data_directory = spice_data_base_path();
            let name_str = source.name().to_string().replace('/', "_");
            let file_name = format!("{name_str}.turso");
            let path = PathBuf::from(data_directory).join(file_name);

            Ok(path.to_string_lossy().to_string())
        } else {
            unreachable!("Expected dataset to have acceleration parameters, but none were found")
        }
    }

    /// Returns the shared connection pool for a `Turso` database
    pub async fn get_shared_pool(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<TursoConnectionPool>> {
        let turso_file = self.turso_file_path(source)?;
        let mvcc_enabled = Self::parse_mvcc_enabled(source)?;
        let timestamp_format = Self::parse_timestamp_format(source)?;

        let mut pools = self.pools.lock().await;
        if let Some(pool) = pools.get(&turso_file) {
            Ok(Arc::clone(pool))
        } else {
            let pool = Arc::new(
                TursoConnectionPool::new_with_timestamp_format(
                    &turso_file,
                    mvcc_enabled,
                    timestamp_format,
                )
                .await
                .map_err(|e| match e {
                    data_components::turso::Error::TursoDatabaseError { source } => {
                        Error::TursoDatabaseError { source }
                    }
                    _ => Error::AccelerationCreationFailed {
                        source: Box::new(e),
                    },
                })?,
            );
            pools.insert(turso_file, Arc::clone(&pool));
            Ok(pool)
        }
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("turso_file")
        .description("Path to the Turso database file. If not specified, defaults to {spice_data_dir}/{dataset_name}.turso"),
    ParameterSpec::component("turso_mvcc")
        .description("Enable Multi-Version Concurrency Control (MVCC) for Turso database")
        .default("disabled")
        .one_of(&["enabled", "disabled"]),
    ParameterSpec::component("internal_timestamp_format")
        .description("Internal timestamp storage format: 'rfc3339' (default, preserves precision/timezone) or 'integer_millis' (performance, millisecond precision only)")
        .default("rfc3339")
        .one_of(&["rfc3339", "integer_millis"]),
    // Note: Remote Turso parameters (turso_url, turso_auth_token) are NOT supported when using
    // Turso as an accelerator. This limitation is specific to the accelerator use case.
    // Remote database support will be available when Turso is implemented as a data connector,
    // where remote access patterns are the primary use case and locally-cached acceleration
    // is not required.
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
            // Memory mode is never pre-initialized (always starts fresh)
            return false;
        }

        // Check if the file exists for file mode
        self.has_existing_file(source)
    }

    /// Initializes a Turso database for the dataset.
    ///
    /// Supports two acceleration modes:
    /// - **Memory mode**: Creates an in-memory database (path = ":memory:")
    /// - **File mode**: Creates a file-based database at the specified or default path
    ///
    /// # Accelerator-Specific Limitation
    ///
    /// This method will reject configurations with remote Turso parameters (`turso_url` or
    /// `turso_auth_token`). This limitation is specific to using Turso as an **accelerator**
    /// and does not apply to general Turso usage. Accelerators require local database access
    /// for optimal performance.
    ///
    /// Remote Turso databases will be supported when Turso is implemented as a data connector,
    /// where remote access is the primary use case.
    ///
    /// # Errors
    ///
    /// Returns `Error::RemoteDatabaseNotSupported` if `turso_url` or `turso_auth_token`
    /// parameters are provided in the acceleration configuration.
    async fn init(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Reject remote database configurations (not supported as accelerators)
        // Note: This is an accelerator-specific limitation. Remote databases will be
        // supported when Turso is used as a data connector.
        if let Some(acceleration) = source.acceleration()
            && (acceleration.params.contains_key("turso_url")
                || acceleration.params.contains_key("turso_auth_token"))
        {
            return Err(Error::RemoteDatabaseNotSupported.into());
        }

        let path = self.file_path(source)?;

        // Handle memory mode: no file operations needed
        if path == ":memory:" {
            // Initialize the shared pool to verify connectivity
            let pool = self.get_shared_pool(source).await?;
            pool.connect().await?;
            return Ok(());
        }

        // Handle file mode: validate path and setup file-based database
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

            // If mode is FileCreate, delete the existing file to start fresh
            if acceleration.mode == Mode::FileCreate {
                let file_path = std::path::Path::new(&path);
                if file_path.exists() {
                    tracing::warn!(
                        "Turso acceleration mode is 'file_create', removing existing file: {}",
                        path
                    );
                    std::fs::remove_file(file_path).map_err(|err| {
                        Error::AccelerationInitializationFailed { source: err.into() }
                    })?;
                }
            }

            download_snapshot_if_needed(acceleration, source, PathBuf::from(path)).await;

            // Initialize the database file using the shared pool
            let pool = self.get_shared_pool(source).await?;
            pool.connect().await?;
        }

        Ok(())
    }

    /// Creates a new table in the accelerator engine, returning a `TableProvider` that supports reading and writing.
    #[expect(clippy::too_many_lines)]
    async fn create_external_table(
        &self,
        cmd: CreateExternalTable,
        source: Option<&dyn AccelerationSource>,
        partition_by: Vec<PartitionedBy>,
    ) -> Result<Arc<dyn TableProvider>, Box<dyn std::error::Error + Send + Sync>> {
        ensure!(
            partition_by.is_empty(),
            super::InvalidConfigurationSnafu {
                msg: "Turso data accelerator does not support the `partition_by` parameter but it was provided".to_string()
            }
        );

        // Determine the database path
        // When called with a source (from DataAccelerator trait), turso_file_path returns:
        //   - ":memory:" for memory mode (!is_file_accelerated())
        //   - A file path for file mode (is_file_accelerated())
        // When called without a source (standalone external table), use provided file or memory mode
        let db_path = if let Some(source) = source {
            self.turso_file_path(source)?
        } else if let Some(file) = cmd.options.get("file") {
            file.clone()
        } else {
            ":memory:".to_string()
        };

        // Get MVCC setting
        let mvcc_enabled = if let Some(source) = source {
            Self::parse_mvcc_enabled(source)?
        } else {
            false // Default to disabled for external tables without source
        };

        // Get or create connection pool
        let pool = {
            let mut pools = self.pools.lock().await;
            if let Some(pool) = pools.get(&db_path) {
                Arc::clone(pool)
            } else {
                let new_pool = Arc::new(TursoConnectionPool::new(&db_path, mvcc_enabled).await?);
                pools.insert(db_path.clone(), Arc::clone(&new_pool));
                new_pool
            }
        };

        // Create the table if it doesn't exist
        let conn = pool.connect().await?;
        let table_name = cmd.name.table().to_string();
        let quoted_table_name = sanitize_identifier(&table_name, "Table")?;

        // Build CREATE TABLE statement from schema
        let mut columns = Vec::new();
        for field in cmd.schema.fields() {
            #[expect(clippy::match_same_arms)]
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
                // Temporal types map to INTEGER
                DataType::Timestamp(_, _)
                | DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_) => "INTEGER",
                // Decimal types map to REAL
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "REAL",
                // Complex types (List, Struct, etc.) map to TEXT (JSON serialized)
                DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
                    "TEXT"
                }
                // Default to TEXT for unsupported types (serialized as JSON or string)
                _ => "TEXT",
            };
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            let column_name = sanitize_identifier(field.name(), "Column")?;
            columns.push(format!("{column_name} {col_type}{nullable}"));
        }

        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quoted_table_name,
            columns.join(", ")
        );

        conn.execute(&create_sql, ())
            .await
            .map_err(|e| Error::AccelerationCreationFailed {
                source: Box::new(e),
            })?;

        // Handle indexes if specified
        if let Some(indexes_str) = cmd.options.get("indexes") {
            if mvcc_enabled {
                // Indexes are not yet supported in MVCC mode
                tracing::warn!(
                    "Indexes are not yet supported in MVCC mode for Turso. Skipping index creation for table '{}'",
                    table_name
                );
            } else {
                // Parse the indexes option string
                use datafusion_table_providers::util::hashmap_from_option_string;
                let indexes = hashmap_from_option_string::<String, String>(indexes_str);

                // Create indexes
                for (column_ref_str, index_type_str) in indexes {
                    let index_type = crate::component::dataset::acceleration::IndexType::from(
                        index_type_str.as_str(),
                    );
                    let index_name = format!(
                        "idx_{}_{}",
                        table_name,
                        column_ref_str.replace(['(', ')', ' ', ','], "_")
                    );
                    let quoted_index_name = sanitize_identifier(&index_name, "Index")?;
                    let unique_clause = match &index_type {
                        crate::component::dataset::acceleration::IndexType::Unique => "UNIQUE ",
                        crate::component::dataset::acceleration::IndexType::Enabled => "",
                    };

                    let sanitized_columns = sanitize_column_reference(&column_ref_str)?;
                    let column_list = format!("({})", sanitized_columns.join(", "));

                    let create_index_sql = format!(
                        "CREATE {unique_clause}INDEX IF NOT EXISTS {quoted_index_name} ON {quoted_table_name} {column_list}"
                    );

                    conn.execute(&create_index_sql, ()).await.map_err(|e| {
                        Error::AccelerationCreationFailed {
                            source: Box::new(e),
                        }
                    })?;

                    tracing::debug!(
                        "Created {}index '{}' on table '{}' for columns: {}",
                        if unique_clause.is_empty() {
                            ""
                        } else {
                            "unique "
                        },
                        index_name,
                        table_name,
                        column_ref_str
                    );
                }
            }
        }

        // Create the table provider
        let schema = Arc::new(Schema::new(
            cmd.schema
                .fields()
                .iter()
                .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
                .collect::<Vec<_>>(),
        ));

        let turso_provider = Arc::new(
            TursoTableProvider::new(schema, table_name, pool)
                .with_function_support(deny_spice_specific_functions()),
        );

        // Wrap in PolyTableProvider for proper read/write separation
        // This allows the table to support both reading and writing operations
        let fed_provider = Arc::new(
            Arc::clone(&turso_provider)
                .create_federated_table_provider()
                .boxed()?,
        ) as Arc<dyn TableProvider>;

        // Wrap with upsert deduplication if needed
        let (write_provider, delete_provider) =
            upsert_dedup::wrap_with_upsert_dedup_if_needed(turso_provider, &cmd.options);

        let table_provider = Arc::new(PolyTableProvider::new(
            write_provider,
            delete_provider,
            fed_provider,
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

register_data_accelerator!(Engine::Turso, TursoAccelerator);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Runtime;
    use crate::component::dataset::acceleration::{Acceleration, Mode};
    use crate::component::dataset::builder::DatasetBuilder;
    use arrow::{
        array::{Int64Array, RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Schema},
    };
    use data_components::delete::get_deletion_provider;
    use datafusion::{
        common::{Constraints, TableReference, ToDFSchema},
        execution::context::SessionContext,
        logical_expr::{CreateExternalTable, cast, col, dml::InsertOp, lit},
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
        let error = result.expect_err("Expected error for remote Turso database");
        assert!(
            error
                .to_string()
                .contains("Remote Turso databases (turso_url, turso_auth_token) are not supported")
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
        let error2 = result2.expect_err("Expected error for remote Turso database with auth token");
        assert!(
            error2
                .to_string()
                .contains("Remote Turso databases (turso_url, turso_auth_token) are not supported")
        );
    }

    #[tokio::test]
    #[expect(clippy::unreadable_literal)]
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
            .create_external_table(external_table, None, vec![])
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

    #[tokio::test]
    async fn test_projection_filter_limit_pushdown() {
        // Create a schema with multiple columns
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, false),
            arrow::datatypes::Field::new("value", DataType::Int64, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_pushdown_table"),
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
            .create_external_table(external_table, None, vec![])
            .await
            .expect("table should be created");

        // Insert test data
        let id_arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let name_arr = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let value_arr = Int64Array::from(vec![100, 200, 300, 400, 500]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(name_arr), Arc::new(value_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);
        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Test 1: Projection pushdown - select only specific columns
        let projection = Some(vec![0_usize, 2_usize]); // id and value columns
        let scan_plan = table
            .scan(&ctx.state(), projection.as_ref(), &[], None)
            .await
            .expect("scan should be successful");

        // Verify the projected schema only contains the selected columns
        let projected_schema = scan_plan.schema();
        assert_eq!(projected_schema.fields().len(), 2);
        assert_eq!(projected_schema.field(0).name(), "id");
        assert_eq!(projected_schema.field(1).name(), "value");

        // Test 2: Filter pushdown - add WHERE clause
        let filter = col("value").gt(lit(200_i64));
        let scan_with_filter = table
            .scan(&ctx.state(), None, &[filter], None)
            .await
            .expect("scan with filter should be successful");

        let result = collect(scan_with_filter, ctx.task_ctx())
            .await
            .expect("query with filter successful");

        // Should return 3 rows (value > 200: 300, 400, 500)
        assert_eq!(result[0].num_rows(), 3);

        // Test 3: Limit pushdown
        let scan_with_limit = table
            .scan(&ctx.state(), None, &[], Some(2))
            .await
            .expect("scan with limit should be successful");

        let result_with_limit = collect(scan_with_limit, ctx.task_ctx())
            .await
            .expect("query with limit successful");

        // Should return at most 2 rows
        let total_rows: usize = result_with_limit.iter().map(RecordBatch::num_rows).sum();
        assert!(total_rows <= 2);

        // Test 4: Combined projection, filter, and limit
        let projection = Some(vec![1_usize]); // name column only
        let filter = col("id").gt(lit(2_i64));
        let limit = Some(2);

        let scan_combined = table
            .scan(&ctx.state(), projection.as_ref(), &[filter], limit)
            .await
            .expect("combined scan should be successful");

        // Verify schema has only the projected column
        let combined_schema = scan_combined.schema();
        assert_eq!(combined_schema.fields().len(), 1);
        assert_eq!(combined_schema.field(0).name(), "name");

        let result_combined = collect(scan_combined, ctx.task_ctx())
            .await
            .expect("combined query successful");

        // Should return at most 2 rows with id > 2 (Charlie, David)
        let total_rows: usize = result_combined.iter().map(RecordBatch::num_rows).sum();
        assert!(total_rows <= 2);
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_file_mode_turso_creation() {
        // Test that file mode creates a Turso database at a specified path
        let test_path = "/tmp/test_turso_file_mode.db";

        // Clean up if file exists from previous test
        let _ = std::fs::remove_file(test_path);
        let _ = std::fs::remove_file(format!("{test_path}-wal"));
        let _ = std::fs::remove_file(format!("{test_path}-shm"));
        let _ = std::fs::remove_file(format!("{test_path}-log"));

        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("name", DataType::Utf8, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");

        let mut options = HashMap::new();
        options.insert("file".to_string(), test_path.to_string());

        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_file_mode_table"),
            location: String::new(),
            file_type: String::new(),
            table_partition_cols: vec![],
            if_not_exists: true,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options,
            constraints: Constraints::new_unverified(vec![]),
            column_defaults: HashMap::default(),
            temporary: false,
        };

        let ctx = SessionContext::new();
        let table = TursoAccelerator::new()
            .create_external_table(external_table, None, vec![])
            .await
            .expect("table should be created");

        // Verify the file was created
        assert!(
            std::path::Path::new(test_path).exists(),
            "Turso database file should be created at specified path"
        );

        // Test that we can insert and query data
        let id_arr = Int64Array::from(vec![1, 2, 3]);
        let name_arr = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(name_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Query back the data
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should be successful");

        let results = collect(scan, ctx.task_ctx())
            .await
            .expect("scan successful");

        assert_eq!(results.len(), 1, "should have 1 batch");
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 3, "should have 3 rows");

        // Verify data
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id should be Int64Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be StringArray");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");

        // Clean up - drop the table first to close connections
        drop(table);
        drop(ctx);

        // Give a moment for connections to close
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Clean up all database files
        let _ = std::fs::remove_file(test_path);
        let _ = std::fs::remove_file(format!("{test_path}-wal"));
        let _ = std::fs::remove_file(format!("{test_path}-shm"));
        let _ = std::fs::remove_file(format!("{test_path}-log"));
    }

    #[tokio::test]
    async fn test_file_mode_turso_creation_default_path() {
        // Test that file mode creates a Turso database using default path when not specified
        let app = app::AppBuilder::new("test").build();
        let rt = Runtime::builder().build().await;

        let mut dataset = DatasetBuilder::try_new(
            "turso_default_path_test".to_string(),
            "turso_default_path_test",
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

        // Initialize the accelerator
        accelerator
            .init(&dataset)
            .await
            .expect("initialization should be successful");

        // Verify initialization
        assert!(
            accelerator.is_initialized(&dataset),
            "accelerator should be initialized"
        );

        // Get the file path
        let file_path = accelerator
            .file_path(&dataset)
            .expect("should have file path");

        // Verify the file was created at the default location
        assert!(
            std::path::Path::new(&file_path).exists(),
            "Turso database file should be created at default path"
        );

        // Verify the path includes the dataset name
        assert!(
            file_path.contains("turso_default_path_test"),
            "File path should contain dataset name"
        );

        // Now test that we can create a table and use it
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, false),
            arrow::datatypes::Field::new("value", DataType::Utf8, false),
        ]));

        let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
        let external_table = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_default_path_table"),
            location: file_path.clone(),
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
            .create_external_table(external_table, None, vec![])
            .await
            .expect("table should be created");

        // Insert test data
        let id_arr = Int64Array::from(vec![10, 20, 30]);
        let value_arr = StringArray::from(vec!["A", "B", "C"]);
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(id_arr), Arc::new(value_arr)],
        )
        .expect("data should be created");

        let exec = MockExec::new(vec![Ok(data)], schema);

        let insertion = table
            .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
            .await
            .expect("insertion should be successful");

        collect(insertion, ctx.task_ctx())
            .await
            .expect("insert successful");

        // Query back the data to verify it works
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan should be successful");

        let results = collect(scan, ctx.task_ctx())
            .await
            .expect("scan successful");

        assert_eq!(results.len(), 1, "should have 1 batch");
        assert_eq!(results[0].num_rows(), 3, "should have 3 rows");

        // Clean up
        std::fs::remove_file(&file_path).ok();
    }

    #[tokio::test]
    #[expect(clippy::too_many_lines)]
    async fn test_timestamp_unit_conversion() {
        // Test that timestamps are correctly converted between different units
        // All timestamps are stored as milliseconds in Turso, but should be
        // correctly scaled when reading back based on the schema's unit

        use arrow::array::{
            TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
            TimestampSecondArray,
        };
        use arrow::datatypes::TimeUnit;

        // Test value: 2024-01-01 00:00:00 UTC
        // In different units:
        const TEST_TIMESTAMP_SECONDS: i64 = 1_704_067_200;
        const TEST_TIMESTAMP_MILLIS: i64 = 1_704_067_200_000;
        const TEST_TIMESTAMP_MICROS: i64 = 1_704_067_200_000_000;
        const TEST_TIMESTAMP_NANOS: i64 = 1_704_067_200_000_000_000;

        let ctx = SessionContext::new();

        // Test 1: TimestampSecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_seconds"),
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

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, vec![])
                .await
                .expect("table should be created");

            // Insert timestamp in seconds
            let ts_arr = TimestampSecondArray::from(vec![TEST_TIMESTAMP_SECONDS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("should be TimestampSecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_SECONDS,
                "TimestampSecond should round-trip correctly"
            );
        }

        // Test 2: TimestampMillisecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_millis"),
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

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, vec![])
                .await
                .expect("table should be created");

            // Insert timestamp in milliseconds
            let ts_arr = TimestampMillisecondArray::from(vec![TEST_TIMESTAMP_MILLIS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("should be TimestampMillisecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_MILLIS,
                "TimestampMillisecond should round-trip correctly"
            );
        }

        // Test 3: TimestampMicrosecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_micros"),
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

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, vec![])
                .await
                .expect("table should be created");

            // Insert timestamp in microseconds
            let ts_arr = TimestampMicrosecondArray::from(vec![TEST_TIMESTAMP_MICROS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("should be TimestampMicrosecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_MICROS,
                "TimestampMicrosecond should round-trip correctly"
            );
        }

        // Test 4: TimestampNanosecond
        {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            )]));

            let df_schema = ToDFSchema::to_dfschema_ref(Arc::clone(&schema)).expect("df schema");
            let external_table = CreateExternalTable {
                schema: df_schema,
                name: TableReference::bare("test_ts_nanos"),
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

            let table = TursoAccelerator::new()
                .create_external_table(external_table, None, vec![])
                .await
                .expect("table should be created");

            // Insert timestamp in nanoseconds
            let ts_arr = TimestampNanosecondArray::from(vec![TEST_TIMESTAMP_NANOS]);
            let data = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ts_arr)])
                .expect("data should be created");

            let exec = MockExec::new(vec![Ok(data)], Arc::clone(&schema));
            let insertion = table
                .insert_into(&ctx.state(), Arc::new(exec), InsertOp::Append)
                .await
                .expect("insertion should be successful");

            collect(insertion, ctx.task_ctx())
                .await
                .expect("insert successful");

            // Read back and verify
            let scan = table
                .scan(&ctx.state(), None, &[], None)
                .await
                .expect("scan");
            let results = collect(scan, ctx.task_ctx()).await.expect("collect");

            let ts_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("should be TimestampNanosecondArray");

            assert_eq!(
                ts_col.value(0),
                TEST_TIMESTAMP_NANOS,
                "TimestampNanosecond should round-trip correctly"
            );
        }
    }
}
