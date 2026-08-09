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

/// Runs a synchronous `DuckDB` sidecar operation on the blocking pool.
///
/// Every `spice_sys` `DuckDB` helper takes the connection pool's write gate, and a
/// `duckdb_on_full_refresh: replace_file` refresh holds that gate **exclusively**
/// while it copies every co-resident table into the staging file and checkpoints it.
/// That window scales with the size of the *other* datasets sharing the file, so
/// waiting on the gate from an async worker parks the whole worker — including
/// `/health`, which Kubernetes uses to decide the pod is dead — for seconds to
/// minutes.
///
/// The helpers themselves are also plain blocking `DuckDB` I/O, so they belong here
/// regardless of the gate; `DuckDbBlobCheckpointStore` and the accelerator's
/// `drop_table`/`evolve_table_schema` already do exactly this.
#[cfg(feature = "duckdb")]
async fn spawn_duckdb_blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(Error::external)?
}

/// [`spawn_duckdb_blocking`] for the read paths that report a failure as `None`
/// rather than as an error.
///
/// A panic is re-raised on this task rather than reported as `None`: these reads
/// answer "where did this dataset get to", and a `None` the caller believes means
/// the dataset re-bootstraps from the beginning of the change stream. Running the
/// read on the blocking pool must not turn a bug into that answer.
#[cfg(any(feature = "mongodb", feature = "mysql"))]
async fn spawn_duckdb_blocking_opt<T, F>(f: F) -> Option<T>
where
    F: FnOnce() -> Option<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(value) => value,
        Err(join_error) if join_error.is_panic() => {
            std::panic::resume_unwind(join_error.into_panic())
        }
        // Cancellation, i.e. the runtime is shutting down under the read (see
        // `is_shutdown_cancellation`). Below the default level, but not silent: it
        // must not pass unremarked for "no checkpoint".
        Err(join_error) => {
            tracing::debug!(
                "Did not read the sidecar checkpoint: the runtime is shutting down ({join_error})"
            );
            None
        }
    }
}

/// True when a `spice_sys` failure is the runtime shutting down under a task on the
/// blocking pool, rather than the operation itself failing.
///
/// A sidecar helper that runs on the blocking pool surfaces a shutdown as a
/// cancelled [`tokio::task::JoinError`] wrapped in [`Error::External`], which
/// callers see only as an opaque `"Acceleration error: task ... was cancelled"` —
/// hence classifying by type rather than by message. The task never started, so
/// there is nothing to retry and nothing an operator can act on; a caller that
/// reports its failures at `warn` should report this one below the default level.
///
/// Prefer this over the `RuntimeStatus::is_shutdown()` guard the refresh task uses
/// for the same purpose: `is_shutdown()` is only *coincidental* — every failure that
/// races a shutdown gets quietened, including real ones — whereas the `JoinError`
/// is a *causal* statement that this specific work did not run.
///
/// The condition it reads is "the task was cancelled", and the shutdown reading
/// holds because a `spawn_blocking` task is cancelled only when the runtime is
/// dropped with the task still queued; nothing here calls `JoinHandle::abort`. A
/// caller that starts aborting sidecar tasks (a per-operation timeout, say) has to
/// revisit that.
///
/// The whole source chain is walked, so it holds however deeply the caller has
/// boxed or wrapped the error. A *panicked* task is deliberately not matched: that
/// is a bug and must stay loud.
pub(crate) fn is_shutdown_cancellation(error: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(error), |error| std::error::Error::source(*error)).any(|error| {
        error
            .downcast_ref::<tokio::task::JoinError>()
            .is_some_and(tokio::task::JoinError::is_cancelled)
    })
}

/// Retries for a sidecar write contending with another writer, on top of the
/// initial attempt. Bounded and short: paired with [`UPSERT_MAX_RETRY_DELAY`] the
/// worst-case added latency stays well under one checkpoint/commit interval, and a
/// persistent conflict just retries on the next interval anyway.
#[cfg(any(feature = "kafka", feature = "debezium", feature = "mysql"))]
pub(crate) const UPSERT_MAX_RETRIES: usize = 4;

/// Per-attempt cap on the `FibonacciBackoffBuilder` delay for sidecar upsert
/// retries. The shared Fibonacci schedule starts at 1s, far longer than a
/// transient writer hand-off needs, so clamp each delay to keep the whole retry
/// budget (~4 x 100ms) short relative to the commit interval.
#[cfg(any(feature = "kafka", feature = "debezium", feature = "mysql"))]
pub(crate) const UPSERT_MAX_RETRY_DELAY: std::time::Duration =
    std::time::Duration::from_millis(100);

/// Whether a sidecar write failure is a transient lock/contention error worth
/// retrying rather than surfacing.
///
/// Deliberately a string heuristic over the boxed engine error (rusqlite, Turso,
/// `DuckDB`, and tokio-postgres all report contention differently), mirroring the
/// reconnect classifier in `data_components::mysql_replication::resilience`. Slight
/// over-matching is harmless: retries are bounded, so a misclassified non-lock error
/// only costs a few short sleeps before it is returned unchanged.
///
/// The `DuckDB` markers matter because its transaction manager is optimistic — it
/// reports a write-write conflict instead of blocking, so two sidecar writers
/// touching the same row surface `TransactionContext Error: Conflict on update!`
/// rather than serializing. Sidecar writers take the pool's write gate with `read()`
/// and so do not exclude each other; only a file swap takes it exclusively.
#[cfg(any(feature = "kafka", feature = "debezium", feature = "mysql"))]
pub(crate) fn is_retryable_lock_error(err: &Error) -> bool {
    const MARKERS: &[&str] = &[
        "database is locked",
        "database table is locked",
        "sqlite_busy",
        "sqlite_locked",
        "deadlock",
        // DuckDB's optimistic concurrency control.
        "conflict on update",
        "transactioncontext error",
        "write-write conflict",
    ];
    let msg = err.to_string().to_ascii_lowercase();
    MARKERS.iter().any(|marker| msg.contains(marker))
}

/// Runs a sidecar write, retrying a transient write conflict a bounded number of
/// times.
///
/// `DuckDB`'s transaction manager is optimistic: two sidecar writers touching the
/// same row get `Conflict on update!` rather than being serialized, because they
/// hold the pool's write gate with `read()` and so do not exclude each other. The
/// attempt is re-run rather than surfaced, matching how a contended write is handled
/// for the binlog checkpoint.
#[cfg(any(feature = "kafka", feature = "debezium"))]
pub(crate) async fn retry_on_write_conflict<F, Fut>(dataset_name: &str, attempt: F) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

    let backoff = FibonacciBackoffBuilder::new()
        .max_retries(Some(UPSERT_MAX_RETRIES))
        .max_duration(Some(UPSERT_MAX_RETRY_DELAY))
        .build();

    retry(backoff, || async {
        attempt().await.map_err(|e| {
            if is_retryable_lock_error(&e) {
                tracing::debug!(
                    dataset = %dataset_name,
                    error = %e,
                    "sidecar offset upsert hit a transient accelerator write conflict"
                );
                RetryError::transient(e)
            } else {
                RetryError::permanent(e)
            }
        })
    })
    .await
}

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
                    // Take the pool from the registered accelerator's path-keyed cache
                    // rather than constructing one. `cayenne.db` is opened by every
                    // sidecar of every Cayenne dataset in the pod, and the lock that
                    // serializes their DDL against each other's `BEGIN CONCURRENT`
                    // writes lives on the pool instance — a pool of our own would hold
                    // a lock no other sidecar observes.
                    let turso_engine = registry
                        .get_accelerator_engine(Engine::Turso)
                        .await
                        .context(AcceleratorEngineUnavailableSnafu {
                            engine: Engine::Turso,
                        })?;
                    let turso_accelerator = turso_engine
                        .as_any()
                        .downcast_ref::<TursoAccelerator>()
                        .context(DowncastFailedSnafu {
                            target: "TursoAccelerator",
                        })?;
                    let pool = turso_accelerator
                        .get_shared_pool_for_path(&metadata_db_path)
                        .await
                        .context(TursoConnectionSnafu)?;
                    return Ok(AccelerationConnection::Turso(pool));
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

#[cfg(test)]
mod tests {
    use super::{Error, is_shutdown_cancellation};

    #[cfg(any(feature = "mongodb", feature = "mysql"))]
    #[tokio::test]
    #[should_panic(expected = "sidecar read panicked")]
    async fn spawn_duckdb_blocking_opt_does_not_report_a_panic_as_no_checkpoint() {
        // `None` from a checkpoint read means "this dataset has no recorded position",
        // which sends the connector back to the start of the change stream. A panic in
        // the read is a bug and must not be answered that way.
        let _: Option<()> =
            super::spawn_duckdb_blocking_opt(|| panic!("sidecar read panicked")).await;
    }

    #[cfg(any(feature = "mongodb", feature = "mysql"))]
    #[tokio::test]
    async fn spawn_duckdb_blocking_opt_passes_through_both_outcomes() {
        assert_eq!(super::spawn_duckdb_blocking_opt(|| Some(7)).await, Some(7));
        assert_eq!(
            super::spawn_duckdb_blocking_opt(|| Option::<u8>::None).await,
            None
        );
    }

    /// A task the runtime dropped before it ran, i.e. what a sidecar write on the
    /// blocking pool sees when the process is shutting down under it.
    async fn cancelled_join_error() -> tokio::task::JoinError {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        let join_error = handle
            .await
            .expect_err("an aborted task must not complete successfully");
        assert!(join_error.is_cancelled());
        join_error
    }

    /// The chain a checkpoint caller actually sees: `DatasetCheckpointer::checkpoint`
    /// boxes the `spice_sys` error, whose `External` variant carries the `JoinError`.
    #[tokio::test]
    async fn a_cancelled_sidecar_task_is_recognized_through_the_boxed_chain() {
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(Error::External {
            source: Box::new(cancelled_join_error().await),
        });
        assert!(is_shutdown_cancellation(boxed.as_ref()));
    }

    #[tokio::test]
    async fn a_bare_cancellation_is_recognized_without_any_wrapping() {
        let join_error = cancelled_join_error().await;
        assert!(is_shutdown_cancellation(&join_error));
    }

    /// A panicking task is a bug, not a shutdown, and has to keep its `warn`.
    #[tokio::test]
    async fn a_panicked_task_is_not_a_shutdown_cancellation() {
        let handle = tokio::spawn(async { panic!("sidecar write panicked") });
        let join_error = handle
            .await
            .expect_err("a panicking task must not complete successfully");
        assert!(join_error.is_panic());

        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(Error::External {
            source: Box::new(join_error),
        });
        assert!(!is_shutdown_cancellation(boxed.as_ref()));
    }

    /// An ordinary sidecar failure — the case that must keep reporting at `warn`.
    #[test]
    fn an_ordinary_sidecar_failure_is_not_a_shutdown_cancellation() {
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(Error::External {
            source: "TransactionContext Error: Conflict on update!".into(),
        });
        assert!(!is_shutdown_cancellation(boxed.as_ref()));

        assert!(!is_shutdown_cancellation(&Error::NoAccelerationConnection));
    }

    #[cfg(all(not(windows), feature = "sqlite", feature = "turso"))]
    mod cayenne_turso_metastore {
        use super::super::{
            AccelerationConnection, AccelerationSource, AcceleratorEngineRegistry, OpenOption,
            acceleration_connection,
        };
        use crate::component::dataset::acceleration::{Acceleration, Engine, Mode};
        use datafusion::sql::TableReference;
        use std::sync::Arc;

        struct MockSource {
            name: TableReference,
            acceleration: Option<Acceleration>,
        }

        impl MockSource {
            fn cayenne_with_turso_metastore(name: &str, metadata_dir: &str) -> Self {
                let mut acceleration = Acceleration {
                    engine: Engine::Cayenne,
                    mode: Mode::File,
                    ..Default::default()
                };
                acceleration
                    .params
                    .insert("cayenne_metadata_dir".to_string(), metadata_dir.to_string());
                acceleration
                    .params
                    .insert("cayenne_metastore".to_string(), "turso".to_string());

                Self {
                    name: TableReference::bare(name.to_string()),
                    acceleration: Some(acceleration),
                }
            }
        }

        impl AccelerationSource for MockSource {
            fn clone_arc(&self) -> Arc<dyn AccelerationSource> {
                Arc::new(Self {
                    name: self.name.clone(),
                    acceleration: self.acceleration.clone(),
                })
            }

            fn is_file_accelerated(&self) -> bool {
                true
            }

            fn app(&self) -> Arc<app::App> {
                unimplemented!("acceleration_connection does not consult the app")
            }

            fn secrets(&self) -> Arc<tokio::sync::RwLock<crate::secrets::Secrets>> {
                unimplemented!("acceleration_connection does not consult secrets")
            }

            fn acceleration(&self) -> Option<&Acceleration> {
                self.acceleration.as_ref()
            }

            fn name(&self) -> &TableReference {
                &self.name
            }

            fn connector_name(&self) -> Option<&str> {
                None
            }

            fn time_column(&self) -> Option<&str> {
                None
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

        /// Both `spice_sys` sidecars of one Cayenne dataset — the dataset checkpoint
        /// and the checkpoint store — open the same `cayenne.db`, and both do DDL and
        /// DML on it. The lock that stops one sidecar's `CREATE TABLE` from landing
        /// inside the other's open `BEGIN CONCURRENT` write lives on the pool
        /// instance, so serialization only happens if they are handed the same pool.
        ///
        /// Before #12727 this branch constructed a pool per call, so each sidecar took
        /// a lock the others could not observe and the gate excluded nothing.
        #[tokio::test]
        async fn two_connections_for_one_dataset_share_a_pool() {
            let metadata_dir = std::env::temp_dir().join("spice_cayenne_turso_metastore_12727");
            let _ = std::fs::remove_dir_all(&metadata_dir);
            std::fs::create_dir_all(&metadata_dir).expect("metadata directory should be creatable");
            let metadata_dir = metadata_dir.to_string_lossy().to_string();

            let source = MockSource::cayenne_with_turso_metastore("orders", &metadata_dir);

            let registry = Arc::new(AcceleratorEngineRegistry::new());
            registry.register_all().await;

            let first = acceleration_connection(
                &source,
                Arc::clone(&registry),
                OpenOption::CreateIfNotExists,
            )
            .await
            .expect("the first sidecar should open the Turso metastore");
            let second = acceleration_connection(
                &source,
                Arc::clone(&registry),
                OpenOption::CreateIfNotExists,
            )
            .await
            .expect("the second sidecar should open the Turso metastore");

            let AccelerationConnection::Turso(first) = &first else {
                panic!("`cayenne_metastore: turso` must connect through Turso");
            };
            let AccelerationConnection::Turso(second) = &second else {
                panic!("`cayenne_metastore: turso` must connect through Turso");
            };

            assert!(
                Arc::ptr_eq(first, second),
                "both sidecars must share one pool over `cayenne.db`, or the schema lock serializes nothing"
            );

            let _ = std::fs::remove_dir_all(&metadata_dir);
        }
    }
}
