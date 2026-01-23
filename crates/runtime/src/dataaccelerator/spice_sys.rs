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

//! Durable storage for Spice operational data related to acceleration.

#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "turso",
    feature = "postgres-accel"
))]
use std::path::Path;
#[cfg(any(feature = "duckdb", feature = "turso"))]
use std::sync::Arc;

use super::AccelerationSource;
#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "turso",
    feature = "postgres-accel"
))]
use snafu::ResultExt;
use snafu::{OptionExt, Snafu};

#[cfg(feature = "postgres-accel")]
use {
    datafusion_table_providers::sql::db_connection_pool::postgrespool::{
        self, PostgresConnectionPool,
    },
    datafusion_table_providers::util::secrets::to_secret_map,
};

#[cfg(all(not(windows), feature = "sqlite"))]
use super::DataAccelerator;
#[cfg(all(not(windows), feature = "sqlite"))]
use super::cayenne::{CayenneAccelerator, Error as CayenneError};
#[cfg(feature = "turso")]
use super::turso::{Error as TursoError, TursoAccelerator};
#[cfg(feature = "duckdb")]
use {
    super::duckdb::{DuckDBAccelerator, Error as DuckDbError},
    super::partitioned_duckdb::{Error as PartitionedDuckDbError, PartitionedDuckDBAccelerator},
    datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
#[cfg(feature = "sqlite")]
use {
    super::sqlite::{Error as SqliteError, SqliteAccelerator},
    datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
};

use crate::component::dataset::acceleration::Engine;
#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "turso",
    feature = "postgres-accel"
))]
use crate::dataaccelerator::get_registered_accelerator;

pub mod dataset_checkpoint;
#[cfg(feature = "debezium")]
pub mod debezium_kafka;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;

pub mod caching_engine;

enum AccelerationConnection {
    #[cfg(feature = "duckdb")]
    DuckDB(Arc<DuckDbConnectionPool>),
    #[cfg(feature = "postgres-accel")]
    Postgres(PostgresConnectionPool),
    #[cfg(feature = "sqlite")]
    SQLite(SqliteConnectionPool),
    #[cfg(feature = "turso")]
    Turso(Arc<super::turso::TursoConnectionPool>),
    #[cfg(all(not(windows), feature = "sqlite"))]
    Cayenne(SqliteConnectionPool),
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Acceleration is not enabled"))]
    AccelerationNotEnabled,

    #[snafu(display("{engine:?} accelerator engine not available"))]
    AcceleratorEngineUnavailable { engine: Engine },

    #[cfg(feature = "duckdb")]
    #[snafu(display("Failed to resolve DuckDB file path: {source}"))]
    DuckDbFilePath { source: DuckDbError },

    #[cfg(feature = "duckdb")]
    #[snafu(display("DuckDB file does not exist at {path}"))]
    DuckDbFileMissing { path: String },

    #[cfg(feature = "duckdb")]
    #[snafu(display("Unable to create DuckDB connection pool: {source}"))]
    DuckDbPool { source: DuckDbError },

    #[cfg(feature = "duckdb")]
    #[snafu(display("Unable to create Partitioned DuckDB connection pool: {source}"))]
    PartitionedDuckDbPool { source: PartitionedDuckDbError },

    #[cfg(feature = "sqlite")]
    #[snafu(display("Failed to resolve SQLite file path: {source}"))]
    SqliteFilePath { source: SqliteError },

    #[cfg(feature = "sqlite")]
    #[snafu(display("SQLite file does not exist at {path}"))]
    SqliteFileMissing { path: String },

    #[cfg(feature = "sqlite")]
    #[snafu(display("Unable to create SQLite connection pool: {source}"))]
    SqlitePool { source: SqliteError },

    #[cfg(feature = "postgres-accel")]
    #[snafu(display("Unable to create PostgreSQL connection pool: {source}"))]
    PostgresPool { source: postgrespool::Error },

    #[cfg(not(feature = "duckdb"))]
    #[snafu(display("Spice wasn't built with DuckDB support enabled"))]
    DuckDbFeatureNotEnabled,

    #[cfg(not(feature = "sqlite"))]
    #[snafu(display("Spice wasn't built with SQLite support enabled"))]
    SqliteFeatureNotEnabled,

    #[cfg(not(feature = "postgres-accel"))]
    #[snafu(display("Spice wasn't built with PostgreSQL acceleration support enabled"))]
    PostgresFeatureNotEnabled,

    #[cfg(feature = "turso")]
    #[snafu(display("Failed to resolve Turso file path: {source}"))]
    TursoFilePath { source: TursoError },

    #[cfg(feature = "turso")]
    #[snafu(display("Turso file does not exist at {path}"))]
    TursoFileMissing { path: String },

    #[cfg(feature = "turso")]
    #[snafu(display("Unable to create Turso connection: {source}"))]
    TursoConnection { source: TursoError },

    #[cfg(not(feature = "turso"))]
    #[snafu(display("Spice wasn't built with Turso support enabled"))]
    TursoFeatureNotEnabled,

    #[cfg(all(not(windows), feature = "sqlite"))]
    #[snafu(display("Failed to resolve Cayenne file path: {source}"))]
    CayenneFilePath { source: CayenneError },

    #[cfg(all(not(windows), feature = "sqlite"))]
    #[snafu(display("Cayenne metadata directory does not exist at {path}"))]
    CayenneMetadataMissing { path: String },

    #[cfg(all(not(windows), feature = "sqlite"))]
    #[snafu(display("Unable to create Cayenne connection pool: {source}"))]
    CayennePool {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("{engine} acceleration not supported"))]
    UnsupportedEngine { engine: Engine },

    #[snafu(display("No acceleration connection available"))]
    NoAccelerationConnection,

    #[snafu(display("Failed to downcast to {target}"))]
    DowncastFailed { target: &'static str },

    #[snafu(display("{source}"))]
    External {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl Error {
    #[cfg(any(
        feature = "sqlite",
        feature = "duckdb",
        feature = "postgres",
        feature = "turso",
        feature = "kafka"
    ))]
    fn external(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::External { source: err.into() }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum OpenOption {
    CreateIfNotExists,
    OpenExisting,
}

async fn acceleration_connection(
    source: &dyn AccelerationSource,
    open_option: OpenOption,
) -> Result<AccelerationConnection> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;
    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::DuckDB,
                })?;

            let duckdb_accelerator = accelerator
                .as_any()
                .downcast_ref::<DuckDBAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "DuckDBAccelerator",
                })?;

            let duckdb_file = duckdb_accelerator
                .duckdb_file_path(source)
                .context(DuckDbFilePathSnafu)?;
            if open_option == OpenOption::OpenExisting && !Path::new(&duckdb_file).exists() {
                return DuckDbFileMissingSnafu { path: duckdb_file }.fail();
            }

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .context(DuckDbPoolSnafu)?;

            Ok(AccelerationConnection::DuckDB(Arc::new(pool)))
        }
        #[cfg(feature = "duckdb")]
        Engine::PartitionedDuckDB => {
            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::PartitionedDuckDB,
                })?;
            let duckdb_accelerator = accelerator
                .as_any()
                .downcast_ref::<PartitionedDuckDBAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "PartitionedDuckDBAccelerator",
                })?;

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .context(PartitionedDuckDbPoolSnafu)?;

            Ok(AccelerationConnection::DuckDB(pool))
        }
        #[cfg(feature = "duckdb")]
        Engine::TableModePartitionedDuckDB => {
            use crate::dataaccelerator::partitioned_duckdb::tables_mode::TablesModePartitionedDuckDBAccelerator;

            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::TableModePartitionedDuckDB,
                })?;
            let duckdb_accelerator = accelerator
                .as_any()
                .downcast_ref::<TablesModePartitionedDuckDBAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "TableModePartitionedDuckDBAccelerator",
                })?;

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .context(PartitionedDuckDbPoolSnafu)?;

            Ok(AccelerationConnection::DuckDB(pool))
        }
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB | Engine::PartitionedDuckDB | Engine::TableModePartitionedDuckDB => {
            DuckDbFeatureNotEnabledSnafu.fail()
        }
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::Sqlite,
                })?;
            let sqlite_accelerator = accelerator
                .as_any()
                .downcast_ref::<SqliteAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "SqliteAccelerator",
                })?;

            let sqlite_file = sqlite_accelerator
                .sqlite_file_path(source)
                .context(SqliteFilePathSnafu)?;
            if open_option == OpenOption::OpenExisting && !Path::new(&sqlite_file).exists() {
                return SqliteFileMissingSnafu { path: sqlite_file }.fail();
            }

            let conn = sqlite_accelerator
                .get_shared_pool(source)
                .await
                .context(SqlitePoolSnafu)?;

            Ok(AccelerationConnection::SQLite(conn))
        }
        #[cfg(not(feature = "sqlite"))]
        Engine::Sqlite => SqliteFeatureNotEnabledSnafu.fail(),
        #[cfg(feature = "postgres-accel")]
        Engine::PostgreSQL => {
            let secret_map = to_secret_map(acceleration_settings.params.clone());

            let pool = PostgresConnectionPool::new(secret_map)
                .await
                .context(PostgresPoolSnafu)?;

            Ok(AccelerationConnection::Postgres(pool))
        }
        #[cfg(not(feature = "postgres-accel"))]
        Engine::PostgreSQL => PostgresFeatureNotEnabledSnafu.fail(),

        #[cfg(feature = "turso")]
        Engine::Turso => {
            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::Turso,
                })?;
            let turso_accelerator = accelerator
                .as_any()
                .downcast_ref::<TursoAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "TursoAccelerator",
                })?;

            let turso_file = turso_accelerator
                .turso_file_path(source)
                .context(TursoFilePathSnafu)?;
            if open_option == OpenOption::OpenExisting && !Path::new(&turso_file).exists() {
                return TursoFileMissingSnafu { path: turso_file }.fail();
            }

            let pool = turso_accelerator
                .get_shared_pool(source)
                .await
                .context(TursoConnectionSnafu)?;

            Ok(AccelerationConnection::Turso(pool))
        }
        #[cfg(not(feature = "turso"))]
        Engine::Turso => TursoFeatureNotEnabledSnafu.fail(),
        #[cfg(all(not(windows), feature = "sqlite"))]
        Engine::Cayenne => {
            use datafusion_table_providers::sqlite::SqliteTableProviderFactory;
            use std::sync::Arc;

            let accelerator = get_registered_accelerator(source, acceleration_settings.engine)
                .await
                .context(AcceleratorEngineUnavailableSnafu {
                    engine: Engine::Cayenne,
                })?;
            let cayenne_accelerator = accelerator
                .as_any()
                .downcast_ref::<CayenneAccelerator>()
                .context(DowncastFailedSnafu {
                    target: "CayenneAccelerator",
                })?;

            // Validate that we can resolve the file path (used for file existence check validation)
            let _ = cayenne_accelerator
                .file_path(source)
                .map_err(|e| Error::CayenneFilePath {
                    source: super::cayenne::Error::InvalidConfiguration {
                        detail: std::sync::Arc::from(format!("{e}")),
                    },
                })?;

            // Derive metadata directory using shared resolution logic
            let metadata_dir = CayenneAccelerator::resolve_metadata_dir(source.acceleration());

            let metadata_db_path = format!("{metadata_dir}/cayenne.db");

            if open_option == OpenOption::OpenExisting && !Path::new(&metadata_db_path).exists() {
                return CayenneMetadataMissingSnafu {
                    path: metadata_db_path,
                }
                .fail();
            }

            // Ensure metadata directory exists
            if let Some(parent) = Path::new(&metadata_db_path).parent() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|e| Error::CayennePool {
                        source: Box::new(e),
                    })?;
            }

            // Create SQLite connection pool for cayenne metadata using the factory
            let sqlite_factory = SqliteTableProviderFactory::new();
            let pool = sqlite_factory
                .get_or_init_instance(
                    Arc::from(metadata_db_path.as_str()),
                    datafusion_table_providers::sql::db_connection_pool::Mode::File,
                    std::time::Duration::from_millis(5000),
                )
                .await
                .map_err(|e| Error::CayennePool {
                    source: Box::new(e),
                })?;

            Ok(AccelerationConnection::Cayenne(pool))
        }
        #[cfg(any(windows, not(feature = "sqlite")))]
        Engine::Cayenne => UnsupportedEngineSnafu {
            engine: Engine::Cayenne,
        }
        .fail(),
        Engine::Arrow => UnsupportedEngineSnafu {
            engine: acceleration_settings.engine,
        }
        .fail(),
    }
}
