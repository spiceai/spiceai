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

#[cfg(feature = "postgres")]
use {
    datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    datafusion_table_providers::util::secrets::to_secret_map,
};

#[cfg(feature = "duckdb")]
use {
    super::duckdb::DuckDBAccelerator, super::partitioned_duckdb::PartitionedDuckDBAccelerator,
    datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
#[cfg(feature = "sqlite")]
use {
    super::sqlite::SqliteAccelerator,
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

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum OpenOption {
    CreateIfNotExists,
    OpenExisting,
}

pub async fn acceleration_file_path(source: &dyn AccelerationSource) -> Option<PathBuf> {
    let acceleration_settings = source.acceleration()?;

    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator = get_registered_accelerator(
                source,
                acceleration_settings.engine,
                "DuckDB accelerator engine not available",
            )
            .await
            .ok()?;

            let duckdb_accelerator = downcast_accelerator::<DuckDBAccelerator>(
                &accelerator,
                "Accelerator is not a DuckDBAccelerator",
            )
            .ok()?;

            let duckdb_file = duckdb_accelerator.duckdb_file_path(source).ok()?;
            Some(PathBuf::from(duckdb_file))
        }
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator = get_registered_accelerator(
                source,
                acceleration_settings.engine,
                "Sqlite accelerator engine not available",
            )
            .await
            .ok()?;

            let sqlite_accelerator = downcast_accelerator::<SqliteAccelerator>(
                &accelerator,
                "Accelerator is not a SqliteAccelerator",
            )
            .ok()?;

            let sqlite_file = sqlite_accelerator.sqlite_file_path(source).ok()?;
            Some(PathBuf::from(sqlite_file))
        }
        _ => None,
    }
}

async fn acceleration_connection(
    source: &dyn AccelerationSource,
    create_table_if_not_exists: bool,
) -> Result<AccelerationConnection> {
    let acceleration_settings = source.acceleration().ok_or("Acceleration is not enabled")?;
    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator = get_registered_accelerator(
                source,
                acceleration_settings.engine,
                "DuckDB accelerator engine not available",
            )
            .await?;

            let duckdb_accelerator = downcast_accelerator::<DuckDBAccelerator>(
                &accelerator,
                "Accelerator is not a DuckDBAccelerator",
            )?;

            let duckdb_file = duckdb_accelerator.duckdb_file_path(source)?;
            if !create_table_if_not_exists && !Path::new(&duckdb_file).exists() {
                return Err("DuckDB file does not exist.".into());
            }

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .map_err(|e| e.to_string())?;

            Ok(AccelerationConnection::DuckDB(Arc::new(pool)))
        }
        #[cfg(feature = "duckdb")]
        Engine::PartitionedDuckDB => {
            let accelerator = get_registered_accelerator(
                source,
                acceleration_settings.engine,
                "DuckDB accelerator engine not available",
            )
            .await?;
            let duckdb_accelerator = downcast_accelerator::<PartitionedDuckDBAccelerator>(
                &accelerator,
                "Accelerator is not a PartitionedDuckDBAccelerator",
            )?;

            let pool = duckdb_accelerator
                .get_shared_pool(source)
                .await
                .map_err(|e| e.to_string())?;

            Ok(AccelerationConnection::DuckDB(pool))
        }
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB | Engine::PartitionedDuckDB => {
            Err("Spice wasn't built with DuckDB support enabled".into())
        }
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator = get_registered_accelerator(
                source,
                acceleration_settings.engine,
                "Sqlite accelerator engine not available",
            )
            .await?;
            let sqlite_accelerator = downcast_accelerator::<SqliteAccelerator>(
                &accelerator,
                "Accelerator is not a SqliteAccelerator",
            )?;

            let sqlite_file = sqlite_accelerator.sqlite_file_path(source)?;
            if !create_table_if_not_exists && !Path::new(&sqlite_file).exists() {
                return Err("Sqlite file does not exist.".into());
            }

            let conn = sqlite_accelerator.get_shared_pool(source).await?;

            Ok(AccelerationConnection::SQLite(conn))
        }
        #[cfg(not(feature = "sqlite"))]
        Engine::Sqlite => Err("Spice wasn't built with Sqlite support enabled".into()),
        #[cfg(feature = "postgres")]
        Engine::PostgreSQL => {
            let secret_map = to_secret_map(acceleration_settings.params.clone());

            let pool = PostgresConnectionPool::new(secret_map)
                .await
                .map_err(|e| e.to_string())?;

            Ok(AccelerationConnection::Postgres(pool))
        }
        #[cfg(not(feature = "postgres"))]
        Engine::PostgreSQL => Err("Spice wasn't built with PostgreSQL support enabled".into()),
        Engine::Arrow => Err("Arrow acceleration not supported for metadata".into()),
    }
}

async fn get_registered_accelerator(
    source: &dyn AccelerationSource,
    engine: Engine,
    missing_err: &'static str,
) -> std::result::Result<Arc<dyn DataAccelerator>, &'static str> {
    source
        .runtime()
        .accelerator_engine_registry()
        .get_accelerator_engine(engine)
        .await
        .ok_or(missing_err)
}

fn downcast_accelerator<T: 'static>(
    accelerator: &Arc<dyn DataAccelerator>,
    wrong_type_err: &'static str,
) -> std::result::Result<&T, &'static str> {
    accelerator
        .as_any()
        .downcast_ref::<T>()
        .ok_or(wrong_type_err)
}
