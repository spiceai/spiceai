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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use super::{AccelerationSource, DataAccelerator};
use snafu::{OptionExt, ResultExt, Snafu};

#[cfg(feature = "postgres")]
use {
    datafusion_table_providers::sql::db_connection_pool::postgrespool::{
        self, PostgresConnectionPool,
    },
    datafusion_table_providers::util::secrets::to_secret_map,
};

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

pub mod dataset_checkpoint;
#[cfg(feature = "debezium")]
pub mod debezium_kafka;

#[cfg(feature = "kafka")]
pub mod kafka;

enum AccelerationConnection {
    #[cfg(feature = "duckdb")]
    DuckDB(Arc<DuckDbConnectionPool>),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnectionPool),
    #[cfg(feature = "sqlite")]
    SQLite(SqliteConnectionPool),
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Acceleration is not enabled"))]
    AccelerationNotEnabled,

    #[snafu(display("{engine:?} accelerator engine not available"))]
    AcceleratorEngineUnavailable { engine: Engine },

    #[snafu(display("Accelerator is not a {expected}"))]
    InvalidAcceleratorType { expected: &'static str },

    #[cfg(feature = "duckdb")]
    #[snafu(display("Failed to resolve DuckDB file path: {source}"))]
    DuckDbFilePath { source: DuckDbError },

    #[cfg(feature = "duckdb")]
    #[snafu(display("DuckDB file does not exist at {path}"))]
    DuckDbFileMissing { path: String },

    #[cfg(feature = "duckdb")]
    #[snafu(display("DuckDB pool acquisition failed: {source}"))]
    DuckDbPool { source: DuckDbError },

    #[cfg(feature = "duckdb")]
    #[snafu(display("Partitioned DuckDB pool acquisition failed: {source}"))]
    PartitionedDuckDbPool { source: PartitionedDuckDbError },

    #[cfg(feature = "sqlite")]
    #[snafu(display("Failed to resolve SQLite file path: {source}"))]
    SqliteFilePath { source: SqliteError },

    #[cfg(feature = "sqlite")]
    #[snafu(display("SQLite file does not exist at {path}"))]
    SqliteFileMissing { path: String },

    #[cfg(feature = "sqlite")]
    #[snafu(display("SQLite pool acquisition failed: {source}"))]
    SqlitePool { source: SqliteError },

    #[cfg(feature = "postgres")]
    #[snafu(display("PostgreSQL pool creation failed: {source}"))]
    PostgresPool { source: postgrespool::Error },

    #[cfg(not(feature = "duckdb"))]
    #[snafu(display("Spice wasn't built with DuckDB support enabled"))]
    DuckDbFeatureNotEnabled,

    #[cfg(not(feature = "sqlite"))]
    #[snafu(display("Spice wasn't built with SQLite support enabled"))]
    SqliteFeatureNotEnabled,

    #[cfg(not(feature = "postgres"))]
    #[snafu(display("Spice wasn't built with PostgreSQL support enabled"))]
    PostgresFeatureNotEnabled,

    #[snafu(display("{engine:?} acceleration not supported for metadata"))]
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
    fn external<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::External {
            source: Box::new(err),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum OpenOption {
    CreateIfNotExists,
    OpenExisting,
}

pub async fn acceleration_file_path(source: &dyn AccelerationSource) -> Result<Option<PathBuf>> {
    let Some(acceleration_settings) = source.acceleration() else {
        return Ok(None);
    };

    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator =
                get_registered_accelerator(source, acceleration_settings.engine).await?;

            let duckdb_accelerator =
                downcast_accelerator::<DuckDBAccelerator>(&accelerator, "DuckDBAccelerator")?;

            let duckdb_file = duckdb_accelerator
                .duckdb_file_path(source)
                .context(DuckDbFilePathSnafu)?;
            Ok(Some(PathBuf::from(duckdb_file)))
        }
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator =
                get_registered_accelerator(source, acceleration_settings.engine).await?;

            let sqlite_accelerator =
                downcast_accelerator::<SqliteAccelerator>(&accelerator, "SqliteAccelerator")?;

            let sqlite_file = sqlite_accelerator
                .sqlite_file_path(source)
                .context(SqliteFilePathSnafu)?;
            Ok(Some(PathBuf::from(sqlite_file)))
        }
        _ => Ok(None),
    }
}

async fn acceleration_connection(
    source: &dyn AccelerationSource,
    create_table_if_not_exists: bool,
) -> Result<AccelerationConnection> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;
    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator =
                get_registered_accelerator(source, acceleration_settings.engine).await?;

            let duckdb_accelerator =
                downcast_accelerator::<DuckDBAccelerator>(&accelerator, "DuckDBAccelerator")?;

            let duckdb_file = duckdb_accelerator
                .duckdb_file_path(source)
                .context(DuckDbFilePathSnafu)?;
            if !create_table_if_not_exists && !Path::new(&duckdb_file).exists() {
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
            let accelerator =
                get_registered_accelerator(source, acceleration_settings.engine).await?;
            let duckdb_accelerator = downcast_accelerator::<PartitionedDuckDBAccelerator>(
                &accelerator,
                "PartitionedDuckDBAccelerator",
            )?;

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .context(PartitionedDuckDbPoolSnafu)?;

            Ok(AccelerationConnection::DuckDB(pool))
        }
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB | Engine::PartitionedDuckDB => DuckDbFeatureNotEnabledSnafu.fail(),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator =
                get_registered_accelerator(source, acceleration_settings.engine).await?;
            let sqlite_accelerator =
                downcast_accelerator::<SqliteAccelerator>(&accelerator, "SqliteAccelerator")?;

            let sqlite_file = sqlite_accelerator
                .sqlite_file_path(source)
                .context(SqliteFilePathSnafu)?;
            if !create_table_if_not_exists && !Path::new(&sqlite_file).exists() {
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
        #[cfg(feature = "postgres")]
        Engine::PostgreSQL => {
            let secret_map = to_secret_map(acceleration_settings.params.clone());

            let pool = PostgresConnectionPool::new(secret_map)
                .await
                .context(PostgresPoolSnafu)?;

            Ok(AccelerationConnection::Postgres(pool))
        }
        #[cfg(not(feature = "postgres"))]
        Engine::PostgreSQL => PostgresFeatureNotEnabledSnafu.fail(),
        Engine::Arrow => UnsupportedEngineSnafu {
            engine: acceleration_settings.engine,
        }
        .fail(),
    }
}

async fn get_registered_accelerator(
    source: &dyn AccelerationSource,
    engine: Engine,
) -> Result<Arc<dyn DataAccelerator>> {
    source
        .runtime()
        .accelerator_engine_registry()
        .get_accelerator_engine(engine)
        .await
        .context(AcceleratorEngineUnavailableSnafu { engine })
}

fn downcast_accelerator<'a, T: 'static>(
    accelerator: &'a Arc<dyn DataAccelerator>,
    expected: &'static str,
) -> Result<&'a T> {
    accelerator
        .as_any()
        .downcast_ref::<T>()
        .context(InvalidAcceleratorTypeSnafu { expected })
}
