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
    datafusion_table_providers::sql::db_connection_pool::duckdbpool::DuckDbConnectionPool,
};
#[cfg(feature = "sqlite")]
use {
    super::sqlite::{Error as SqliteError, SqliteAccelerator},
    datafusion_table_providers::sql::db_connection_pool::sqlitepool::SqliteConnectionPool,
};

use crate::component::dataset::acceleration::Engine;
use crate::dataaccelerator::AcceleratorEngineRegistry;
use runtime_checkpoint_api::BlobCheckpointStore;

pub mod dataset_checkpoint;
#[cfg(feature = "debezium")]
pub mod debezium_kafka;

#[cfg(feature = "kafka")]
pub mod kafka;

// Driver-free sidecar (SQL + JSON only, like `dataset_checkpoint`/`caching_engine`),
// so it is always compiled: the `connector-dynamodb` crate calls
// `dynamodb::init_checkpoint_store` regardless of which accelerator backend is enabled.
pub mod dynamodb;

#[cfg(feature = "mongodb")]
pub mod mongodb;

#[cfg(feature = "mysql")]
pub mod mysql_binlog;

#[cfg(any(feature = "kafka", feature = "debezium"))]
mod offsets;

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

    #[snafu(display(
        "Failed to process accelerated data: internal type conversion error for '{target}'"
    ))]
    DowncastFailed { target: &'static str },

    #[snafu(display("Acceleration error: {source}"))]
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
        feature = "kafka",
        feature = "mongodb",
        feature = "mysql"
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

/// Construct the per-dataset **blob** checkpoint store backed by the dataset's own
/// accelerator, writing into the sidecar `table_name`. Returns `None` when the dataset
/// has no usable accelerator connection (acceleration disabled, or the engine isn't
/// compiled in), so a CDC connector degrades to re-bootstrapping from scratch rather
/// than failing.
///
/// This is the seam that lets CDC connectors persist their stream position without
/// naming any runtime-internal accelerator type: they receive an
/// `Arc<dyn BlobCheckpointStore>` and never see the engine. Resolving the connection
/// needs the accelerator engine registry, which is why this factory lives in `runtime`.
#[cfg(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "postgres-accel",
    feature = "turso"
))]
pub async fn checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
    table_name: &'static str,
) -> Option<Arc<dyn BlobCheckpointStore>> {
    let registry = dataset.runtime.accelerator_engine_registry();
    let connection = match acceleration_connection(dataset, registry, OpenOption::CreateIfNotExists)
        .await
    {
        Ok(connection) => connection,
        Err(e) => {
            // Surface *why* checkpointing is unavailable (missing engine feature,
            // missing file, pool-init failure, …) instead of a silent `None`.
            tracing::warn!(
                dataset = %dataset.name,
                error = %e,
                "Could not resolve the dataset's accelerator connection for checkpoint storage; the connector will run without a persisted checkpoint"
            );
            return None;
        }
    };
    let dataset_name = dataset.name.to_string();

    // Exhaustive over the compiled `AccelerationConnection` variants — no wildcard, so
    // adding an accelerator variant forces a matching arm here.
    let store: Arc<dyn BlobCheckpointStore> = match connection {
        #[cfg(feature = "duckdb")]
        AccelerationConnection::DuckDB(pool) => {
            Arc::new(runtime_checkpoint_duckdb::DuckDbBlobCheckpointStore::new(
                pool,
                dataset_name,
                table_name,
            ))
        }
        #[cfg(feature = "postgres-accel")]
        AccelerationConnection::Postgres(pool) => Arc::new(
            runtime_checkpoint_postgres::PostgresBlobCheckpointStore::new(
                pool,
                dataset_name,
                table_name,
            ),
        ),
        #[cfg(feature = "sqlite")]
        AccelerationConnection::SQLite(pool) => {
            Arc::new(runtime_checkpoint_sqlite::SqliteBlobCheckpointStore::new(
                pool,
                dataset_name,
                table_name,
            ))
        }
        #[cfg(feature = "turso")]
        AccelerationConnection::Turso(pool) => Arc::new(
            runtime_checkpoint_turso::TursoBlobCheckpointStore::new(pool, dataset_name, table_name),
        ),
        #[cfg(all(not(windows), feature = "sqlite"))]
        AccelerationConnection::Cayenne(pool) => {
            Arc::new(runtime_checkpoint_sqlite::SqliteBlobCheckpointStore::new(
                pool,
                dataset_name,
                table_name,
            ))
        }
    };
    Some(store)
}

/// No accelerator backend is compiled in, so nothing can persist a checkpoint: the
/// connector runs stateless (ephemeral, re-bootstrapping on restart). Signature parity
/// with the accelerator-backed variant above (see it for the full contract).
#[cfg(not(any(
    feature = "duckdb",
    feature = "sqlite",
    feature = "postgres-accel",
    feature = "turso"
)))]
#[expect(
    clippy::unused_async,
    reason = "signature parity with the accelerator-backed build; no async work when no backend is compiled in"
)]
pub async fn checkpoint_store(
    dataset: &crate::component::dataset::Dataset,
    table_name: &'static str,
) -> Option<Arc<dyn BlobCheckpointStore>> {
    let _ = (dataset, table_name);
    None
}

async fn acceleration_connection(
    source: &dyn AccelerationSource,
    #[cfg_attr(
        not(any(feature = "duckdb", feature = "sqlite", feature = "turso")),
        expect(unused_variables)
    )]
    registry: Arc<AcceleratorEngineRegistry>,
    #[cfg_attr(
        not(any(feature = "duckdb", feature = "sqlite", feature = "turso")),
        expect(unused_variables)
    )]
    open_option: OpenOption,
) -> Result<AccelerationConnection> {
    let acceleration_settings = source.acceleration().context(AccelerationNotEnabledSnafu)?;
    match acceleration_settings.engine {
        #[cfg(feature = "duckdb")]
        Engine::DuckDB => {
            let accelerator = registry
                .get_accelerator_engine(acceleration_settings.engine)
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
        #[cfg(not(feature = "duckdb"))]
        Engine::DuckDB => DuckDbFeatureNotEnabledSnafu.fail(),
        #[cfg(feature = "sqlite")]
        Engine::Sqlite => {
            let accelerator = registry
                .get_accelerator_engine(acceleration_settings.engine)
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
            let accelerator = registry
                .get_accelerator_engine(acceleration_settings.engine)
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

            let accelerator = registry
                .get_accelerator_engine(acceleration_settings.engine)
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

            // When the Cayenne metastore backend is Turso, it owns `cayenne.db` as a
            // libSQL/MVCC database. Route the checkpoint through a Turso connection
            // on that same file rather than opening it as a raw SQLite pool, which
            // would re-stamp it SQLite-WAL and conflict with the metastore.
            #[cfg(feature = "turso")]
            {
                let metastore_type = source
                    .acceleration()
                    .and_then(|a| a.params.get("cayenne_metastore"))
                    .map_or("sqlite", String::as_str);
                if metastore_type == "turso" {
                    let pool = super::turso::TursoConnectionPool::new(&metadata_db_path)
                        .await
                        .map_err(|e| Error::CayennePool {
                            source: Box::new(e),
                        })?;
                    return Ok(AccelerationConnection::Turso(Arc::new(pool)));
                }
            }

            // Create SQLite connection pool for cayenne metadata using the factory
            let sqlite_factory = SqliteTableProviderFactory::new();
            let pool = sqlite_factory
                .get_or_init_instance(
                    Arc::from(metadata_db_path.as_str()),
                    datafusion_table_providers::sql::db_connection_pool::Mode::File,
                    std::time::Duration::from_secs(5),
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
        Engine::Arrow | Engine::PartitionedArrow => UnsupportedEngineSnafu {
            engine: acceleration_settings.engine,
        }
        .fail(),
    }
}
