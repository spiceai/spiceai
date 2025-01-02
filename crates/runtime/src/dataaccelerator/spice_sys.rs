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

use std::{path::Path, sync::Arc};

#[cfg(feature = "postgres")]
use {
    datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool,
    datafusion_table_providers::util::secrets::to_secret_map,
};

#[cfg(feature = "duckdb")]
use {
    super::duckdb::DuckDBAccelerator,
    datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
#[cfg(feature = "sqlite")]
use {
    super::sqlite::SqliteAccelerator,
    datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
};

use super::get_accelerator_engine;
use crate::component::dataset::{acceleration::Engine, Dataset};

pub mod dataset_checkpoint;
#[cfg(feature = "debezium")]
pub mod debezium_kafka;

enum AccelerationConnection {
    #[cfg(feature = "duckdb")]
    DuckDB(Arc<DuckDbConnectionPool>),
    #[cfg(feature = "postgres")]
    Postgres(PostgresConnectionPool),
    #[cfg(feature = "sqlite")]
    SQLite(SqliteConnectionPool),
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn acceleration_connection(
    dataset: &Dataset,
    create_table_if_not_exists: bool,
) -> Result<AccelerationConnection> {
    let acceleration = dataset
        .acceleration
        .as_ref()
        .ok_or("Dataset acceleration not enabled")?;
    match acceleration.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator = get_accelerator_engine(Engine::DuckDB)
                .await
                .ok_or("DuckDB accelerator engine not available")?;
            let duckdb_accelerator = accelerator
                .as_any()
                .downcast_ref::<DuckDBAccelerator>()
                .ok_or("Accelerator is not a DuckDBAccelerator")?;

            let duckdb_file = duckdb_accelerator.duckdb_file_path(dataset)?;
            if !create_table_if_not_exists && !Path::new(&duckdb_file).exists() {
                return Err("DuckDB file does not exist.".into());
            }

            let pool = duckdb_accelerator
                .get_shared_pool(dataset)
                .await
                .map_err(|e| e.to_string())?;

            Ok(AccelerationConnection::DuckDB(Arc::new(pool)))
        }
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB => Err("Spice wasn't built with DuckDB support enabled".into()),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator = get_accelerator_engine(Engine::Sqlite)
                .await
                .ok_or("Sqlite accelerator engine not available")?;
            let sqlite_accelerator = accelerator
                .as_any()
                .downcast_ref::<SqliteAccelerator>()
                .ok_or("Accelerator is not a SqliteAccelerator")?;

            let sqlite_file = sqlite_accelerator.sqlite_file_path(dataset)?;
            if !create_table_if_not_exists && !Path::new(&sqlite_file).exists() {
                return Err("Sqlite file does not exist.".into());
            }

            let conn = sqlite_accelerator.get_shared_pool(dataset).await?;

            Ok(AccelerationConnection::SQLite(conn))
        }
        #[cfg(not(feature = "sqlite"))]
        Engine::Sqlite => Err("Spice wasn't built with Sqlite support enabled".into()),
        #[cfg(feature = "postgres")]
        Engine::PostgreSQL => {
            let Some(acceleration) = &dataset.acceleration else {
                return Err("Dataset is not accelerated.".into());
            };

            let secret_map = to_secret_map(acceleration.params.clone());

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
