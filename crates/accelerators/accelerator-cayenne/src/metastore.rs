/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Decides and opens a dataset's Cayenne metastore.

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use cayenne::metastore::object_native::ObjectNativeCatalog;
use data_accelerator_api::{AccelerationSource, DataAccelerator};
use runtime_acceleration::acceleration::Mode;
use runtime_acceleration::sidecar::{AcceleratorSidecar, OpenOption};
use runtime_checkpoint_api::CheckpointError;
use runtime_checkpoint_sqlite::SqliteSidecar;
use snafu::prelude::*;
use tokio::sync::OnceCell;

use super::{
    AccelerationInitializationFailedSnafu, CAYENNE_MIGRATION_DOCS, CayenneAccelerator, Error,
    InvalidConfigurationSnafu, Result, TursoMetastoreOnDiskSnafu, native_sidecar,
    overlapping_metastore_dir, path_exists,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Metastore {
    Sqlite,
    ObjectNative,
}

/// The shared catalog and the native handle used by checkpoint sidecars.
pub(super) struct OpenedCatalog {
    pub(super) catalog: Arc<dyn cayenne::MetadataCatalog>,
    native: Option<Arc<ObjectNativeCatalog>>,
}

const OBJECT_NATIVE_DIR: &str = "object_native";

/// `runtime.params` key that puts new file-mode Cayenne datasets on the
/// object-native metastore: `enabled` or `disabled` (the default).
const CAYENNE_VNEXT_PARAM: &str = "cayenne_vnext";

/// The `SQLite` database a Cayenne metastore lives in, inside its metadata
/// directory. Every metastore connection string in this file ends in this name.
pub(super) const METASTORE_DB_FILE: &str = "cayenne.db";

/// The `cayenne_metastore` value that selected the retired Turso metastore.
const TURSO_METASTORE: &str = "turso";

/// The warning for a dataset that still sets `cayenne_metastore: turso` over a metastore
/// Turso did not write. The param has no effect, so the dataset loads and the warning says
/// to remove it.
fn turso_metastore_param_warning(dataset: &str) -> String {
    format!(
        "Dataset '{dataset}' sets `cayenne_metastore: turso`, which is no longer supported and has no effect: \
        the dataset uses the built-in metastore. Remove `cayenne_metastore` from this dataset. \
        See: {CAYENNE_MIGRATION_DOCS}"
    )
}

/// Whether the `cayenne_metastore: turso` warning for `dataset` has yet to be emitted: once
/// per dataset, however often its load is retried or the Spicepod reloaded.
fn turso_metastore_param_unreported(dataset: &str) -> bool {
    static REPORTED: LazyLock<std::sync::Mutex<std::collections::HashSet<String>>> =
        LazyLock::new(|| std::sync::Mutex::new(std::collections::HashSet::new()));

    REPORTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(dataset.to_string())
}

/// Whether the metastore at `db_path` was written by the retired Turso metastore.
///
/// Turso's MVCC journal mode stamps `0xFF` into both file-format version bytes of the
/// database header (offsets 18 and 19), where `SQLite` writes 1 (rollback journal) or
/// 2 (WAL), and keeps its commit log beside the database in `cayenne.db-log`. Either
/// one marks the catalog as Turso's.
///
/// `SQLite` refuses to open such a file ("file is not a database"), so this check is
/// not what keeps the catalog from being misread. It turns that refusal into an error
/// that says what to do, and it runs before anything opens or creates the database, so
/// the Turso files are left exactly as they were.
///
/// A header that cannot be read is reported as not Turso: opening the database
/// reports that failure itself.
async fn is_turso_metastore(db_path: &Path) -> bool {
    use tokio::io::AsyncReadExt;

    const FILE_FORMAT_VERSION_OFFSET: usize = 18;
    const MVCC_FILE_FORMAT_VERSION: [u8; 2] = [0xFF, 0xFF];

    let mut log_path = db_path.as_os_str().to_owned();
    log_path.push("-log");
    if tokio::fs::try_exists(&log_path).await.unwrap_or(false) {
        return true;
    }

    let Ok(mut file) = tokio::fs::File::open(db_path).await else {
        return false;
    };
    let mut header = [0_u8; FILE_FORMAT_VERSION_OFFSET + MVCC_FILE_FORMAT_VERSION.len()];
    if file.read_exact(&mut header).await.is_err() {
        return false;
    }
    header[FILE_FORMAT_VERSION_OFFSET..] == MVCC_FILE_FORMAT_VERSION
}

impl CayenneAccelerator {
    /// Get or create the shared in-memory (`memdb`) catalog for `mode: memory`
    /// tables. The DSN uses `SQLite`'s `memdb` VFS keyed by this accelerator's
    /// instance id, so the metastore lives entirely in RAM (nothing on disk) and
    /// distinct accelerator instances stay isolated.
    pub(super) async fn get_or_create_memory_catalog(
        &self,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        let connection_string =
            format!("sqlite://file:/cayenne-mem-{}?vfs=memdb", self.instance_id);
        Self::init_cayenne_catalog(&self.memory_catalog, connection_string).await
    }

    /// Whether the local SQLite metastore database exists.
    pub(super) fn metadata_db_exists(source: &dyn AccelerationSource) -> bool {
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        let metadata_db_path = format!("{metadata_dir}/{METASTORE_DB_FILE}");
        PathBuf::from(metadata_db_path).exists()
    }

    /// Open a concrete catalog for transactions spanning a dataset's partitions.
    pub(super) async fn open_partition_catalog(
        metadata_dir: &str,
    ) -> Result<Arc<cayenne::CayenneCatalog>> {
        // Relies on metastore selection keeping `partition_by` on SQLite.
        let catalog_connection_string = format!("sqlite://{metadata_dir}/{METASTORE_DB_FILE}");
        let catalog_concrete: Arc<cayenne::CayenneCatalog> = Arc::new(
            cayenne::CayenneCatalog::new(catalog_connection_string)
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?,
        );
        // Initialize the catalog (creates tables if needed)
        cayenne::MetadataCatalog::init(catalog_concrete.as_ref())
            .await
            .boxed()
            .context(AccelerationInitializationFailedSnafu)?;

        Ok(catalog_concrete)
    }

    /// Lazily initialize a Cayenne catalog into `cell` from `connection_string`,
    /// sharing the init/`OnceCell` machinery between the file-mode and memory-mode
    /// catalog getters.
    pub(super) async fn init_cayenne_catalog(
        cell: &OnceCell<Arc<dyn cayenne::MetadataCatalog>>,
        connection_string: String,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        cell.get_or_try_init(move || {
            let connection_string = connection_string;
            async move {
                let catalog = Arc::new(
                    cayenne::CayenneCatalog::new(connection_string)
                        .boxed()
                        .context(AccelerationInitializationFailedSnafu)?,
                ) as Arc<dyn cayenne::MetadataCatalog>;

                catalog
                    .init()
                    .await
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?;

                Ok::<Arc<dyn cayenne::MetadataCatalog>, Error>(catalog)
            }
        })
        .await
        .map(Arc::clone)
    }

    /// Refuse a configuration whose recreate would delete the shared metastore — see
    /// [`overlapping_metastore_dir`] for the shape and how the defaults reach it.
    ///
    /// Called at open time, where the operator can still fix the spicepod, and again
    /// immediately before each recursive delete: at open time neither directory need
    /// exist yet, so the resolution falls back to a lexical one, and an overlap that
    /// only a symlink reveals appears once the directories are real.
    ///
    /// A path that cannot be placed refuses the recreate. The guard's whole job is to
    /// prove the delete cannot reach the metastore, and it cannot prove that about a
    /// path it failed to resolve.
    pub(super) async fn ensure_metastore_outside_data_dir(
        source: &dyn AccelerationSource,
        data_dir: &str,
    ) -> Result<()> {
        // With `cayenne_vnext` enabled, the object-native metastore lives under a local
        // `runtime.state.location` rather than the metadata directory when one is set.
        // Disabled, the state location holds no metastore of this instance's making;
        // one left there by an earlier enabled run is found on disk before a delete by
        // `ensure_no_catalog_under_data_dir`.
        let app = source.app();
        let state_dir = Self::vnext_enabled(source)?
            .then_some(app.runtime.state.as_ref())
            .flatten()
            .and_then(|state| url::Url::parse(&state.location).ok())
            .filter(|url| url.scheme() == "file")
            .and_then(|url| url.to_file_path().ok())
            .map(|path| path.to_string_lossy().into_owned());
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        let candidates = std::iter::once((metadata_dir, false))
            .chain(state_dir.map(|state_dir| (state_dir, true)));
        for (metastore_dir, is_state_location) in candidates {
            let overlap = overlapping_metastore_dir(data_dir, &metastore_dir)
                .await
                .map_err(|source_error| Error::CayenneDirsUnresolvable {
                    table_name: source.name().to_string(),
                    data_dir: data_dir.to_string(),
                    metadata_dir: metastore_dir.clone(),
                    source: source_error,
                })?;
            if let Some((data, metastore)) = overlap {
                let table_name = source.name().to_string();
                let data_dir = data.to_string_lossy().into_owned();
                let metastore = metastore.to_string_lossy().into_owned();
                return Err(if is_state_location {
                    Error::StateLocationInsideDataDir {
                        table_name,
                        data_dir,
                        state_dir: metastore,
                    }
                } else {
                    Error::MetastoreInsideDataDir {
                        table_name,
                        data_dir,
                        metadata_dir: metastore,
                    }
                });
            }
        }
        Ok(())
    }

    /// Whether `runtime.params.cayenne_vnext` is `enabled`: new file-mode datasets
    /// then use the object-native metastore.
    fn vnext_enabled(source: &dyn AccelerationSource) -> Result<bool> {
        match source
            .app()
            .runtime
            .params
            .get(CAYENNE_VNEXT_PARAM)
            .map(String::as_str)
        {
            None | Some("disabled") => Ok(false),
            Some("enabled") => Ok(true),
            Some(other) => InvalidConfigurationSnafu {
                detail: Arc::from(format!(
                    "`runtime.params.{CAYENNE_VNEXT_PARAM}` is '{other}'. Set it to 'enabled' or 'disabled'."
                )),
            }
            .fail(),
        }
    }

    /// The metastore holding `source`'s file-mode acceleration. A new dataset goes to the metastore
    /// `cayenne_vnext` names: object-native when enabled, SQLite otherwise. An
    /// existing dataset is never moved: one whose metastore differs from the flag
    /// is an error naming the dataset and the fix, except under `mode: file_create`,
    /// which recreates it from its source in the metastore the flag names. With the
    /// flag disabled, an instance that never used the object-native metastore checks
    /// only its local metadata directory.
    pub(super) async fn metastore_type_for(
        &self,
        source: &dyn AccelerationSource,
        metadata_dir: &str,
    ) -> Result<Metastore> {
        // A `mode: memory` table keeps its metastore in RAM.
        if !source.is_file_accelerated() {
            return Ok(Metastore::Sqlite);
        }
        let requested = Self::vnext_enabled(source)?;
        // A cluster scheduler or executor keeps SQLite under the flag. The
        // executors of a cluster share `runtime.state.location`, while the
        // object-native metastore allows one writer per dataset log, so each
        // executor would fence the others' commits.
        let clustered = self.cluster_role.is_some();
        let vnext = requested && !clustered;
        let recreated = source
            .acceleration()
            .is_some_and(|acceleration| acceleration.mode == Mode::FileCreate);
        let table_name = source.name().to_string();
        // The local `object_native` directory holds the catalog, or records that this
        // instance uses one in `runtime.state.location`.
        let in_native = (vnext || path_exists(format!("{metadata_dir}/{OBJECT_NATIVE_DIR}")).await)
            && self.native_catalog_exists(metadata_dir, source).await?
            && self
                .catalog_holds(metadata_dir, Metastore::ObjectNative, source, &table_name)
                .await?;
        // Without the flag, an instance that never used the object-native metastore
        // keeps SQLite after one local check.
        if !vnext && !in_native {
            if requested {
                self.vnext_notice(&table_name, "uses the SQLite Cayenne metastore although `runtime.params.cayenne_vnext` is enabled, because the new metastore does not support cluster mode yet.");
            }
            return Ok(Metastore::Sqlite);
        }
        let partitioned = source
            .acceleration()
            .is_some_and(|acceleration| !acceleration.partition_by.is_empty());
        let in_sqlite = path_exists(format!("{metadata_dir}/{METASTORE_DB_FILE}")).await
            && self
                .catalog_holds(metadata_dir, Metastore::Sqlite, source, &table_name)
                .await?;
        if in_sqlite {
            // A partitioned dataset stays on SQLite under the flag (below), so
            // finding it there is where it belongs, not an acceleration to move.
            if partitioned && vnext {
                self.vnext_notice(&table_name, "uses the SQLite Cayenne metastore although `runtime.params.cayenne_vnext` is enabled, because the new metastore does not support `partition_by` yet.");
            }
            ensure!(
                !vnext || recreated || partitioned,
                InvalidConfigurationSnafu {
                    detail: Arc::from(format!(
                        "Dataset '{table_name}' is accelerated in the SQLite Cayenne metastore, but `runtime.params.{CAYENNE_VNEXT_PARAM}` is enabled, and an existing acceleration is not moved to the object-native metastore. Set `runtime.params.{CAYENNE_VNEXT_PARAM}: disabled` to keep serving it, or start it once with `acceleration.mode: file_create` to reload it from its source into the object-native metastore."
                    )),
                }
            );
            // `mode: file_create` drops this copy and recreates the table, which then
            // lands in the object-native metastore.
            return Ok(Metastore::Sqlite);
        }
        if in_native {
            ensure!(
                !clustered || recreated,
                InvalidConfigurationSnafu {
                    detail: Arc::from(format!(
                        "Dataset '{table_name}' is accelerated in the object-native Cayenne metastore, which does not support cluster mode yet. Run it outside a cluster to keep serving it, or start it once with `acceleration.mode: file_create` to reload it from its source into the SQLite metastore."
                    )),
                }
            );
            ensure!(
                vnext || recreated,
                InvalidConfigurationSnafu {
                    detail: Arc::from(format!(
                        "Dataset '{table_name}' is accelerated in the object-native Cayenne metastore, but `runtime.params.{CAYENNE_VNEXT_PARAM}` is not enabled, and an existing acceleration is not moved to the SQLite metastore. Set `runtime.params.{CAYENNE_VNEXT_PARAM}: enabled` to keep serving it, or start it once with `acceleration.mode: file_create` to reload it from its source into the SQLite metastore."
                    )),
                }
            );
        }
        if partitioned {
            // The object-native metastore does not hold partitioned tables yet.
            // One already in it is refused rather than moved to a new, empty
            // SQLite table that would strand its rows.
            ensure!(
                !in_native,
                InvalidConfigurationSnafu {
                    detail: Arc::from(format!(
                        "Dataset '{table_name}' is in the object-native Cayenne metastore, which does not support `partition_by` yet. Remove `partition_by` to keep serving it, or delete its acceleration so it rebuilds on SQLite."
                    )),
                }
            );
            self.vnext_notice(&table_name, "uses the SQLite Cayenne metastore although `runtime.params.cayenne_vnext` is enabled, because the new metastore does not support `partition_by` yet.");
            return Ok(Metastore::Sqlite);
        }
        // `mode: file_create` with the flag disabled drops the object-native copy
        // and recreates the table, which then lands in SQLite.
        Ok(Metastore::ObjectNative)
    }

    fn vnext_notice(&self, dataset: &str, outcome: &str) {
        if self.vnext_notices.lock().insert(dataset.to_string()) {
            // The accelerator crate's own target is below the default log level;
            // this line is for the operator, like the auto-tuned config line.
            tracing::info!(target: "spiced::acceleration::cayenne", "Dataset '{dataset}' {outcome}");
        }
    }

    async fn catalog_holds(
        &self,
        metadata_dir: &str,
        metastore_type: Metastore,
        source: &dyn AccelerationSource,
        table_name: &str,
    ) -> Result<bool> {
        let catalog = self
            .get_or_create_catalog(metadata_dir, metastore_type, source)
            .await?;
        Ok(catalog
            .catalog
            .list_table_names()
            .await
            .boxed()
            .context(AccelerationInitializationFailedSnafu)?
            .iter()
            .any(|name| name == table_name))
    }

    /// Whether an object-native catalog was created for this metadata directory
    /// (or `runtime.state.location`), read without creating one.
    async fn native_catalog_exists(
        &self,
        metadata_dir: &str,
        source: &dyn AccelerationSource,
    ) -> Result<bool> {
        if self.native_catalog.get().is_some() {
            return Ok(true);
        }
        let Some(state) = source.app().runtime.state.clone() else {
            return Ok(
                path_exists(format!("{metadata_dir}/{OBJECT_NATIVE_DIR}/catalog/head")).await,
            );
        };
        // One request per process: only this accelerator creates the catalog,
        // and a created one answers from the cell above. An error is not kept.
        self.native_presence
            .get_or_try_init(|| Self::state_location_has_native_catalog(source, state))
            .await
            .copied()
    }

    async fn state_location_has_native_catalog(
        source: &dyn AccelerationSource,
        state: spicepod::component::runtime::RuntimeState,
    ) -> Result<bool> {
        let location = spicepod::component::runtime::StateLocation {
            location: state.location.clone(),
            params: state.params.clone(),
            source: spicepod::component::runtime::StateSource::RuntimeState,
        };
        let (store, prefix) = runtime_state_location::build_object_store(
            source.secrets(),
            tokio::runtime::Handle::current(),
            &location,
            runtime_state_location::StateFeature::CayenneMetastore,
        )
        .await
        .boxed()
        .context(AccelerationInitializationFailedSnafu)?;
        let head = object_store::path::Path::from(prefix)
            .join("v1")
            .join("catalog")
            .join("head");
        match object_store::ObjectStoreExt::head(store.as_ref(), &head).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error)
                .boxed()
                .context(AccelerationInitializationFailedSnafu),
        }
    }

    pub(super) async fn get_or_create_catalog(
        &self,
        metadata_dir: &str,
        metastore_type: Metastore,
        source: &dyn AccelerationSource,
    ) -> Result<OpenedCatalog> {
        match metastore_type {
            Metastore::ObjectNative => {
                let native =
                    Self::init_object_native_catalog(&self.native_catalog, metadata_dir, source)
                        .await?;
                Ok(OpenedCatalog {
                    catalog: Arc::clone(&native) as Arc<dyn cayenne::MetadataCatalog>,
                    native: Some(native),
                })
            }
            Metastore::Sqlite => {
                let connection_string = format!("sqlite://{metadata_dir}/{METASTORE_DB_FILE}");
                let catalog = Self::init_cayenne_catalog(&self.catalog, connection_string).await?;
                Ok(OpenedCatalog {
                    catalog,
                    native: None,
                })
            }
        }
    }

    /// The shared file-mode catalog for `source`, once [`Self::ensure_supported_metastore`]
    /// has ruled out a retired Turso metastore, in the metastore
    /// [`Self::metastore_type_for`] chooses.
    pub(super) async fn catalog_for_source(
        &self,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<dyn cayenne::MetadataCatalog>> {
        Self::ensure_supported_metastore(source).await?;
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        let metastore_type = self.metastore_type_for(source, &metadata_dir).await?;
        self.get_or_create_catalog(&metadata_dir, metastore_type, source)
            .await
            .map(|opened| opened.catalog)
    }

    /// Refuses a dataset whose metastore on disk the retired Turso metastore wrote, and
    /// warns, once per dataset, about one that still sets `cayenne_metastore: turso`.
    ///
    /// A Turso catalog has to be deleted and every dataset in it reloaded, so it fails
    /// the dataset before anything opens, creates, or changes a file. Otherwise the param
    /// has no effect: the dataset runs on the `SQLite` metastore and loads, including in
    /// `mode: memory`, which never had a Turso metastore to select.
    pub(super) async fn ensure_supported_metastore(source: &dyn AccelerationSource) -> Result<()> {
        let acceleration = source.acceleration();
        if source.is_file_accelerated() {
            let metadata_dir = Self::resolve_metadata_dir(acceleration);
            let metastore_path = Path::new(&metadata_dir).join(METASTORE_DB_FILE);
            if is_turso_metastore(&metastore_path).await {
                return TursoMetastoreOnDiskSnafu {
                    dataset: Arc::from(source.name().to_string()),
                    metastore_path: metastore_path.to_string_lossy().into_owned(),
                    metadata_dir,
                }
                .fail();
            }
        }

        let configured_turso = acceleration
            .and_then(|acceleration| acceleration.params.get("cayenne_metastore"))
            .is_some_and(|metastore| metastore.trim().eq_ignore_ascii_case(TURSO_METASTORE));
        let dataset = source.name().to_string();
        if configured_turso && turso_metastore_param_unreported(&dataset) {
            tracing::warn!(
                target: "spiced::acceleration::cayenne",
                "{}",
                turso_metastore_param_warning(&dataset)
            );
        }
        Ok(())
    }

    /// Get or create the object-native catalog, with no SQL metastore. With
    /// `runtime.state.location` set, its head log is kept in the location's
    /// `cayenne/v1/` folder; otherwise in `{metadata_dir}/object_native`.
    async fn init_object_native_catalog(
        cell: &OnceCell<Arc<cayenne::metastore::object_native::ObjectNativeCatalog>>,
        metadata_dir: &str,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<cayenne::metastore::object_native::ObjectNativeCatalog>> {
        let directory = PathBuf::from(metadata_dir).join(OBJECT_NATIVE_DIR);
        let state = source.app().runtime.state.clone();
        let secrets = source.secrets();
        cell.get_or_try_init(move || async move {
            let native = match state {
                Some(state) => {
                    let location = spicepod::component::runtime::StateLocation {
                        location: state.location.clone(),
                        params: state.params.clone(),
                        source: spicepod::component::runtime::StateSource::RuntimeState,
                    };
                    let (store, prefix) = runtime_state_location::build_object_store(
                        secrets,
                        tokio::runtime::Handle::current(),
                        &location,
                        runtime_state_location::StateFeature::CayenneMetastore,
                    )
                    .await
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?;
                    let native =
                        cayenne::metastore::object_native::ObjectNativeCatalog::open_or_create_at(
                            store,
                            object_store::path::Path::from(prefix)
                                .join("v1")
                                .join("catalog"),
                            &format!("{}/cayenne/v1", state.location.trim_end_matches('/')),
                        )
                        .await;
                    if native.is_ok() {
                        // A local record that this instance uses the catalog in the state
                        // location, so a start with `cayenne_vnext` disabled finds it on
                        // local disk and asks the state location only then.
                        tokio::fs::create_dir_all(&directory)
                            .await
                            .boxed()
                            .context(AccelerationInitializationFailedSnafu)?;
                        tokio::fs::write(directory.join("state_location"), &state.location)
                            .await
                            .boxed()
                            .context(AccelerationInitializationFailedSnafu)?;
                    }
                    native
                }
                None => {
                    cayenne::metastore::object_native::ObjectNativeCatalog::open_or_create_local(
                        &directory,
                    )
                    .await
                }
            };
            let catalog = Arc::new(
                native
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?,
            );
            cayenne::MetadataCatalog::init(catalog.as_ref())
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;
            Ok::<_, Error>(catalog)
        })
        .await
        .map(Arc::clone)
    }

    pub(super) async fn open_checkpoint_sidecar(
        &self,
        source: &dyn AccelerationSource,
        open_option: OpenOption,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
        {
            use datafusion_table_providers::sqlite::SqliteTableProviderFactory;

            // Resolving the data directory validates the acceleration configuration; the
            // metastore path below is derived independently of it.
            self.file_path(source)
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;

            // The sidecar opens `cayenne.db` itself, so it refuses a Turso metastore
            // here as well as through the catalog.
            Self::ensure_supported_metastore(source)
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;

            let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
            let metadata_db_path = format!("{metadata_dir}/{METASTORE_DB_FILE}");

            let metastore_type = self
                .metastore_type_for(source, &metadata_dir)
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
            if metastore_type == Metastore::ObjectNative {
                let head = format!("{metadata_dir}/{OBJECT_NATIVE_DIR}/catalog/head");
                // A catalog in `runtime.state.location` is checked when it is opened.
                if open_option == OpenOption::OpenExisting
                    && source.app().runtime.state.is_none()
                    && !path_exists(&head).await
                {
                    return Err(CheckpointError::Store {
                        source: format!("Cayenne object-native catalog does not exist at {head}")
                            .into(),
                    });
                }
                let catalog = self
                    .get_or_create_catalog(&metadata_dir, metastore_type, source)
                    .await
                    .map_err(|source| CheckpointError::Store {
                        source: Box::new(source),
                    })?;
                // Opening the object-native metastore always yields its typed catalog; a
                // missing one is refused rather than served from the SQLite sidecar, which
                // would split the dataset's positions from its data.
                let Some(native) = catalog.native else {
                    return Err(CheckpointError::Store {
                        source: format!(
                            "Cayenne object-native catalog for dataset '{}' did not open",
                            source.name()
                        )
                        .into(),
                    });
                };
                return Ok(Arc::new(native_sidecar::NativeSidecar::new(
                    native,
                    source.name().to_string(),
                )));
            }

            if open_option == OpenOption::OpenExisting && !path_exists(&metadata_db_path).await {
                return Err(CheckpointError::Store {
                    source: format!(
                        "Cayenne metadata directory does not exist at {metadata_db_path}"
                    )
                    .into(),
                });
            }

            if let Some(parent) = std::path::Path::new(&metadata_db_path).parent() {
                tokio::fs::create_dir_all(parent).await.map_err(|source| {
                    CheckpointError::Store {
                        source: Box::new(source),
                    }
                })?;
            }

            let sqlite_factory = SqliteTableProviderFactory::new();
            let pool = sqlite_factory
                .get_or_init_instance(
                    Arc::from(metadata_db_path.as_str()),
                    datafusion_table_providers::sql::db_connection_pool::Mode::File,
                    std::time::Duration::from_secs(5),
                )
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;

            Ok(Arc::new(SqliteSidecar::new(
                Arc::new(pool),
                source.name().to_string(),
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turso_metastore_messages_name_the_dataset_the_fix_and_the_docs() {
        assert_eq!(
            turso_metastore_param_warning("orders"),
            "Dataset 'orders' sets `cayenne_metastore: turso`, which is no longer supported and has no effect: \
             the dataset uses the built-in metastore. Remove `cayenne_metastore` from this dataset. \
             See: https://spiceai.org/docs/components/data-accelerators/cayenne/migration"
        );
        // Once per dataset: an init retry or a reload that keeps the param stays quiet.
        assert!(turso_metastore_param_unreported("turso_param_dedupe"));
        assert!(!turso_metastore_param_unreported("turso_param_dedupe"));
        assert!(turso_metastore_param_unreported("turso_param_dedupe_other"));

        let on_disk = Error::TursoMetastoreOnDisk {
            dataset: Arc::from("orders"),
            metastore_path: "/data/metadata/cayenne.db".to_string(),
            metadata_dir: "/data/metadata".to_string(),
        }
        .to_string();
        assert_eq!(
            on_disk,
            "Failed to load dataset 'orders' (cayenne): the Cayenne metastore '/data/metadata/cayenne.db' was written by the Turso metastore, which is no longer supported, so it cannot be opened. \
             Stop Spice, remove `cayenne_metastore` from every dataset that sets it, delete the Cayenne metastore directory '/data/metadata', then start Spice to reload the datasets from their sources. \
             Every Cayenne dataset that uses this metastore also reloads from its source. \
             See: https://spiceai.org/docs/components/data-accelerators/cayenne/migration"
        );
    }

    /// The detection reads only the header and the `-log` sidecar. A real Turso
    /// catalog is exercised end to end; these pin the byte-level rule against a
    /// database `SQLite` itself wrote, which must never read as Turso.
    #[tokio::test]
    async fn turso_metastore_detection_reads_the_header_and_the_mvcc_log() {
        let dir = tempfile::tempdir().expect("temp dir");

        let missing = dir.path().join("missing").join(METASTORE_DB_FILE);
        assert!(
            !is_turso_metastore(&missing).await,
            "no metastore is not Turso"
        );

        let sqlite = dir.path().join("sqlite").join(METASTORE_DB_FILE);
        std::fs::create_dir_all(sqlite.parent().expect("parent")).expect("sqlite dir");
        {
            let conn = rusqlite::Connection::open(&sqlite).expect("open sqlite");
            conn.pragma_update(None, "journal_mode", "WAL")
                .expect("wal mode");
            conn.execute("CREATE TABLE t (a INTEGER)", [])
                .expect("create table");
        }
        assert!(
            !is_turso_metastore(&sqlite).await,
            "a WAL-mode SQLite metastore is not Turso"
        );

        let mvcc = dir.path().join("mvcc").join(METASTORE_DB_FILE);
        std::fs::create_dir_all(mvcc.parent().expect("parent")).expect("mvcc dir");
        let mut header = std::fs::read(&sqlite).expect("read sqlite header");
        header[18] = 0xFF;
        header[19] = 0xFF;
        std::fs::write(&mvcc, &header).expect("write mvcc header");
        assert!(
            is_turso_metastore(&mvcc).await,
            "0xFF file-format versions are Turso's MVCC journal mode"
        );

        let log_only = dir.path().join("log").join(METASTORE_DB_FILE);
        std::fs::create_dir_all(log_only.parent().expect("parent")).expect("log dir");
        std::fs::write(log_only.with_file_name("cayenne.db-log"), b"").expect("write log");
        assert!(
            is_turso_metastore(&log_only).await,
            "a Turso MVCC log beside the database marks the catalog as Turso's"
        );

        let short = dir.path().join("short").join(METASTORE_DB_FILE);
        std::fs::create_dir_all(short.parent().expect("parent")).expect("short dir");
        std::fs::write(&short, b"SQLite").expect("write short file");
        assert!(
            !is_turso_metastore(&short).await,
            "a truncated header is left for SQLite to report"
        );
    }
}
