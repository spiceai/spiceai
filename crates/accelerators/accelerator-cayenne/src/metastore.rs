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

use super::{
    AccelerationCreationFailedSnafu, RefreshMode, data_in_state_location,
    placed_acceleration_dropped_message, placed_dataset_folder, resolved_refresh_mode,
    spice_data_base_path,
};
use cayenne::metastore::object_native::ObjectNativeCatalog;
use data_accelerator_api::{AccelerationSource, DataAccelerator};
use runtime_acceleration::acceleration::Mode;
use runtime_acceleration::sidecar::{AcceleratorSidecar, OpenOption};
use runtime_checkpoint_api::CheckpointError;
use runtime_checkpoint_sqlite::SqliteSidecar;
use snafu::prelude::*;
use spicepod::component::runtime::{StateLocation, StateSource};
use tokio::sync::OnceCell;

use super::{
    AccelerationInitializationFailedSnafu, CAYENNE_MIGRATION_DOCS, CayenneAccelerator, Error,
    InvalidConfigurationSnafu, Result, TursoMetastoreOnDiskSnafu, UnsupportedSetting,
    ensure_setting_supported, native_sidecar, overlapping_metastore_dir, path_exists,
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

impl OpenedCatalog {
    /// The object-native catalog, when the dataset is in the state location.
    pub(super) fn native(&self) -> Option<&Arc<ObjectNativeCatalog>> {
        self.native.as_ref()
    }
}

const OBJECT_NATIVE_DIR: &str = "object_native";

/// `runtime.params` key that puts new file-mode Cayenne datasets on the
/// object-native metastore: `enabled` or `disabled` (the default).
pub(super) const CAYENNE_VNEXT_PARAM: &str = "cayenne_vnext";

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
        // With `cayenne_vnext` enabled, the object-native metastore lives in the state
        // location, which is `.spice/data` when `runtime.state.location` is unset.
        // Disabled, Cayenne reads no metastore there, so the state location is not
        // checked.
        let state_dir = if Self::vnext_enabled(source)? {
            Some(effective_state_location(&source.app())?)
        } else {
            None
        };
        let state_dir = state_dir
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
    pub(super) fn vnext_enabled(source: &dyn AccelerationSource) -> Result<bool> {
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

    /// The metastore holding `source`'s file-mode acceleration.
    ///
    /// With `runtime.params.cayenne_vnext` on, every dataset goes to the
    /// object-native metastore: one an earlier release kept on SQLite loads from its
    /// source there, as a new one does, and its SQLite copy is not opened.
    ///
    /// With the flag off it is SQLite: a dataset SQLite holds is served from it, a
    /// new one is created there, and one only the object-native metastore holds is
    /// refused unless `mode: file_create` reloads it into SQLite.
    ///
    /// Every dataset of a cluster scheduler or executor stays on SQLite under the
    /// flag; outside a cluster a partitioned dataset is refused under it.
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
        self.ensure_partition_by_supported(source)?;
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
        let partitioned = source
            .acceleration()
            .is_some_and(|acceleration| !acceleration.partition_by.is_empty());
        if vnext {
            ensure_setting_supported(&table_name, object_native_data_unsupported(source))?;
            self.ignored_file_path_notice(source);
            let location_supported = url::Url::parse(
                &effective_state_location(&source.app())?.location,
            )
            .is_ok_and(|location| {
                location_datasets_url(&location, &object_store::path::Path::default()).is_some()
            });
            ensure!(
                location_supported,
                InvalidConfigurationSnafu {
                    detail: Arc::from(cayenne::metastore_layout::object_native_data_path_refusal(
                        &table_name
                    )),
                }
            );
        }
        // With the flag on, a copy an earlier release kept in the SQLite metastore is
        // neither read nor written, not even opened (opening it writes its files):
        // the dataset loads from its source into the object-native metastore, and that
        // copy stays as the release left it, for a rollback. With the flag off, a
        // dataset SQLite holds is served from it, as the release would serve it, even
        // when the object-native metastore holds a later copy; deciding that needs
        // nothing from the state location, which may be unreachable.
        if !vnext
            && path_exists(format!("{metadata_dir}/cayenne.db")).await
            && self
                .catalog_holds(metadata_dir, Metastore::Sqlite, source, &table_name)
                .await?
        {
            if requested {
                self.vnext_notice(&table_name, "uses the SQLite Cayenne metastore although `runtime.params.cayenne_vnext` is enabled, because the new metastore does not support cluster mode yet.");
            }
            return Ok(Metastore::Sqlite);
        }
        // A table only the object-native catalog holds stays there: with the flag off
        // it is refused rather than created empty on SQLite. With the flag off, the
        // state location is read only on an instance that has used the catalog.
        let in_native = self
            .existing_native_catalog_holds(metadata_dir, source, &table_name)
            .await?;
        // Without the flag, an instance that never used the object-native metastore
        // keeps SQLite after one local check.
        if !vnext && !in_native {
            if requested {
                self.vnext_notice(&table_name, "uses the SQLite Cayenne metastore although `runtime.params.cayenne_vnext` is enabled, because the new metastore does not support cluster mode yet.");
            }
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
        // The object-native metastore does not hold partitioned tables yet. One that
        // gained `partition_by` while in it (outside a cluster it is refused above
        // with the flag on) is refused rather than moved to a new, empty SQLite table
        // that would strand its rows.
        ensure!(
            !partitioned,
            InvalidConfigurationSnafu {
                detail: Arc::from(format!(
                    "Dataset '{table_name}' is in the object-native Cayenne metastore, which does not support `partition_by` yet. Remove `partition_by` to keep serving it, or delete its acceleration so it rebuilds on SQLite."
                )),
            }
        );
        // `mode: file_create` with the flag disabled drops the object-native copy
        // and recreates the table, which then lands in SQLite.
        Ok(Metastore::ObjectNative)
    }

    /// Warns, once per dataset, that `cayenne_file_path` no longer places the
    /// acceleration: under `runtime.params.cayenne_vnext` its data is in the state
    /// location, and a copy an earlier release kept at that path is left there.
    fn ignored_file_path_notice(&self, source: &dyn AccelerationSource) {
        let set = source.acceleration().is_some_and(|acceleration| {
            acceleration
                .params
                .get("cayenne_file_path")
                .is_some_and(|value| !value.trim().is_empty())
        });
        let dataset = source.name().to_string();
        if set
            && self
                .vnext_notices
                .lock()
                .insert(format!("{dataset}\0cayenne_file_path"))
        {
            let location = effective_state_location(&source.app())
                .map_or_else(|_| String::new(), |location| location.location);
            // The accelerator crate's own target is below the default log level;
            // this line is for the operator.
            tracing::warn!(
                target: "spiced::acceleration::cayenne",
                "{}",
                ignored_file_path_message(&dataset, &location)
            );
        }
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

    pub(super) async fn get_or_create_catalog(
        &self,
        metadata_dir: &str,
        metastore_type: Metastore,
        source: &dyn AccelerationSource,
    ) -> Result<OpenedCatalog> {
        match metastore_type {
            Metastore::ObjectNative => {
                let native = Self::init_object_native_catalog(&self.native_catalog, source).await?;
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

    /// Get or create the object-native catalog, with no SQL metastore. Its head log
    /// is kept in the `cayenne/v1/` folder of the state location
    /// ([`effective_state_location`]): `runtime.state.location`, or `.spice/data`
    /// when that is unset.
    async fn init_object_native_catalog(
        cell: &OnceCell<Arc<cayenne::metastore::object_native::ObjectNativeCatalog>>,
        source: &dyn AccelerationSource,
    ) -> Result<Arc<cayenne::metastore::object_native::ObjectNativeCatalog>> {
        let location = effective_state_location(&source.app())?;
        let location_url = location.location.clone();
        let secrets = source.secrets();
        let catalog = cell
            .get_or_try_init(move || async move {
                // One client for the location; the logs' requests and the data files'
                // are measured apart.
                let (store, prefix) = runtime_state_location::build_unmetered_object_store(
                    Arc::clone(&secrets),
                    tokio::runtime::Handle::current(),
                    &location,
                    runtime_state_location::StateFeature::CayenneMetastore,
                )
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;
                let prefix = object_store::path::Path::parse(&prefix)
                    .boxed()
                    .context(AccelerationInitializationFailedSnafu)?;
                let data_store = runtime_state_location::metrics::metered(
                    Arc::clone(&store),
                    runtime_state_location::StateFeature::CayenneData,
                );
                // Instances on one location share its accelerations: a writer
                // lease on each dataset decides which instance writes it, and
                // the others serve it as replicas until it lapses.
                let config = cayenne::metastore::object_native::NativeCatalogConfig {
                    access: cayenne::metastore::object_native::Access::Leased(
                        cayenne::metastore::object_native::LeaseSettings::default(),
                    ),
                    ..cayenne::metastore::object_native::NativeCatalogConfig::default()
                };
                let native = cayenne::metastore::object_native::ObjectNativeCatalog::open_or_create_at_with_config(
                        runtime_state_location::metrics::metered(
                            store,
                            runtime_state_location::StateFeature::CayenneMetastore,
                        ),
                        prefix.join("v1").join("catalog"),
                        &format!("{}/cayenne/v1", location.location.trim_end_matches('/')),
                        config,
                    )
                    .await
                    .map(|native| native.with_data_store(data_store));
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
            .map(Arc::clone)?;
        // A local record that this instance uses the catalog in the state location, so
        // a start with `cayenne_vnext` disabled finds it on local disk and reads the
        // state location only then.
        let marker = native_marker_dir(source);
        if !path_exists(marker.join("state_location")).await {
            tokio::fs::create_dir_all(&marker)
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;
            tokio::fs::write(marker.join("state_location"), &location_url)
                .await
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?;
        }
        Ok(catalog)
    }

    /// Initializes a `Cayenne` database for the dataset
    /// If the dataset is not file-accelerated, this is a no-op
    /// Creates the data directory if it doesn't exist
    /// Cayenne keeps its sidecar tables in the metastore database beside the dataset's
    /// Cayenne directory, not in the dataset's own store.
    pub(super) async fn open_checkpoint_sidecar(
        &self,
        source: &dyn AccelerationSource,
        open_option: OpenOption,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
        {
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
            let metastore_type = self
                .metastore_type_for(source, &metadata_dir)
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
            self.sidecar_in(source, &metadata_dir, metastore_type, open_option)
                .await
        }
    }
}

/// Async equivalent of [`Path::exists`]: `true` only when the path is present.
/// I/O errors (including permission denied) are `false`, matching `Path::exists`,
/// so converting the accelerator's existence gates does not change who proceeds.
/// The local directory recording that this instance has used the object-native
/// catalog in the state location: in `source`'s `cayenne_metadata_dir`, or the
/// default metadata directory. Never under `cayenne_file_path`, which the
/// object-native metastore ignores, and where an earlier release's copy may be.
fn native_marker_dir(source: &dyn AccelerationSource) -> PathBuf {
    let metadata_dir = util::spice_paths::cayenne_metastore_dir(
        None,
        source
            .acceleration()
            .and_then(|acceleration| acceleration.params.get("cayenne_metadata_dir"))
            .map(String::as_str),
    );
    Path::new(&metadata_dir).join(OBJECT_NATIVE_DIR)
}

impl CayenneAccelerator {
    /// `source`'s sidecar in the `metastore_type` metastore under `metadata_dir`, which
    /// is where its dataset checkpoint, append cursor and connector offsets are kept.
    async fn sidecar_in(
        &self,
        source: &dyn AccelerationSource,
        metadata_dir: &str,
        metastore_type: Metastore,
        open_option: OpenOption,
    ) -> Result<Arc<dyn AcceleratorSidecar>, CheckpointError> {
        use datafusion_table_providers::sqlite::SqliteTableProviderFactory;

        if metastore_type == Metastore::ObjectNative {
            // The catalog in the state location is checked when it is opened.
            let opened = self
                .get_or_create_catalog(metadata_dir, metastore_type, source)
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
            let Some(native) = opened.native else {
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

        let metadata_db_path = format!("{metadata_dir}/{METASTORE_DB_FILE}");
        if open_option == OpenOption::OpenExisting && !path_exists(&metadata_db_path).await {
            return Err(CheckpointError::Store {
                source: format!("Cayenne metadata directory does not exist at {metadata_db_path}")
                    .into(),
            });
        }

        if let Some(parent) = std::path::Path::new(&metadata_db_path).parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|source| CheckpointError::Store {
                    source: Box::new(source),
                })?;
        }

        let sqlite_factory = SqliteTableProviderFactory::new();
        // As long as the metastore waits: a restore writes here while the restores of
        // other datasets in the same database hold its write lock.
        let pool = sqlite_factory
            .get_or_init_instance(
                Arc::from(metadata_db_path.as_str()),
                datafusion_table_providers::sql::db_connection_pool::Mode::File,
                std::time::Duration::from_millis(
                    cayenne::sqlite_metastore_config().busy_timeout_ms,
                ),
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
    /// Clears the dataset checkpoint, append cursor and source positions `source` has
    /// in the `metastore_type` metastore, whose catalog does not hold its table: the
    /// table is about to be created there empty. A checkpoint or cursor left from a
    /// table that metastore held before (a `mode: file_create` drop, a dataset moved to
    /// the other metastore and back) would mark the empty table loaded and skip its
    /// initial load, and a source position would resume a connector that reads it after
    /// this point past every row the table lost. Kafka and Debezium read theirs when
    /// they are built, before the table opens. Without such a load record the positions
    /// stay: they are this start's own, as a connector records its consumer group
    /// before the table opens.
    pub(super) async fn clear_load_state_of_new_table(
        &self,
        source: &dyn AccelerationSource,
        metadata_dir: &str,
        metastore_type: Metastore,
    ) -> Result<()> {
        let table_name = source.name().to_string();
        // The SQLite sidecar is the dataset's own `cayenne.db` under its metadata
        // directory, which is not the shared catalog's when datasets set different
        // `cayenne_metadata_dir`s. With no such file there is no load record to clear.
        if metastore_type != Metastore::ObjectNative
            && !path_exists(&format!("{metadata_dir}/{METASTORE_DB_FILE}")).await
        {
            return Ok(());
        }
        let sidecar = self
            .sidecar_in(
                source,
                metadata_dir,
                metastore_type,
                OpenOption::OpenExisting,
            )
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;
        let checkpoint = sidecar
            .dataset_checkpointer(runtime_acceleration::snapshot::SnapshotBehavior::Disabled)
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;
        let cursor = sidecar
            .blob_checkpoint_store(runtime_checkpoint_api::APPEND_CURSOR_TABLE)
            .boxed()
            .context(AccelerationCreationFailedSnafu)?;
        let had_checkpoint = checkpoint.exists().await;
        let had_cursor = cursor
            .get()
            .await
            .boxed()
            .context(AccelerationCreationFailedSnafu)?
            .is_some();
        if had_checkpoint {
            checkpoint
                .delete()
                .await
                .context(AccelerationCreationFailedSnafu)?;
        }
        if had_cursor {
            cursor
                .delete()
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
        }
        if had_checkpoint || had_cursor {
            sidecar
                .clear_source_positions()
                .await
                .boxed()
                .context(AccelerationCreationFailedSnafu)?;
            tracing::info!(
                target: "spiced::acceleration::cayenne",
                "Dataset '{table_name}' (cayenne) has no table in its metastore, so it is created empty and loaded from its source; the load record left from an earlier table of the same name is cleared."
            );
        }
        Ok(())
    }
    /// Drops `table_name`'s copy in the object-native metastore, when one exists, for
    /// a `mode: file_create` start on `SQLite`, so the table it creates is new and the
    /// next start does not find the dataset in both metastores. A copy whose data is
    /// in the state location has its folder deleted by the catalog, as for any placed
    /// dataset that drops its acceleration; one at the data path shares the local
    /// directory the recreate deletes.
    pub(super) async fn drop_older_object_native_copy(
        &self,
        source: &dyn AccelerationSource,
        table_name: &str,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metadata_dir = Self::resolve_metadata_dir(source.acceleration());
        if !self
            .existing_native_catalog_holds(&metadata_dir, source, table_name)
            .await?
        {
            return Ok(());
        }
        let native = self
            .get_or_create_catalog(&metadata_dir, Metastore::ObjectNative, source)
            .await?
            .catalog;
        let placed = match native.get_table(table_name).await {
            Ok(stored) if data_in_state_location(native.as_ref(), &stored.path) => {
                Some(stored.path)
            }
            Ok(_) | Err(_) => None,
        };
        if native
            .drop_table(table_name)
            .await
            .boxed()
            .context(AccelerationInitializationFailedSnafu)?
        {
            tracing::info!(
                "Dropped the older object-native Cayenne table metadata for '{table_name}' (file_create mode)"
            );
        }
        if let Some(data_path) = placed.as_deref() {
            tracing::info!(
                target: "spiced::acceleration::cayenne",
                "{}",
                placed_acceleration_dropped_message(
                    table_name,
                    placed_dataset_folder(data_path),
                    true
                )
            );
        }
        Ok(())
    }

    /// Whether an object-native catalog already exists for `source` and holds
    /// `table_name`. It is opened only when it exists: deciding this never
    /// creates one.
    async fn existing_native_catalog_holds(
        &self,
        metadata_dir: &str,
        source: &dyn AccelerationSource,
        table_name: &str,
    ) -> Result<bool> {
        if self.native_catalog.get().is_none() {
            // With the flag off, only an instance that has opened the object-native
            // catalog (its local marker) asks the state location; any other flag-off
            // dataset is decided by one local check and never reads the location.
            let vnext = Self::vnext_enabled(source)? && self.cluster_role.is_none();
            if !vnext && !path_exists(native_marker_dir(source)).await {
                return Ok(false);
            }
            let location = effective_state_location(&source.app())?;
            let (store, prefix) = runtime_state_location::build_unmetered_object_store(
                source.secrets(),
                tokio::runtime::Handle::current(),
                &location,
                runtime_state_location::StateFeature::CayenneMetastore,
            )
            .await
            .boxed()
            .context(AccelerationInitializationFailedSnafu)?;
            let head = object_store::path::Path::parse(&prefix)
                .boxed()
                .context(AccelerationInitializationFailedSnafu)?
                .join("v1")
                .join("catalog")
                .join("head");
            match object_store::ObjectStoreExt::head(store.as_ref(), &head).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => return Ok(false),
                Err(error) => {
                    return Err(error)
                        .boxed()
                        .context(AccelerationInitializationFailedSnafu);
                }
            }
        }
        self.catalog_holds(metadata_dir, Metastore::ObjectNative, source, table_name)
            .await
    }
}

/// A dataset's data files in its dataset folder in the object-native location.
/// The URL of the location's dataset folders, `datasets_prefix` in the
/// location's store, when the location holds data files: an `s3://` bucket,
/// whose store is keyed from the bucket's root, or a local directory, whose
/// store is keyed by absolute paths (`runtime_state_location`), so the
/// `file://` URL of the same path names the same objects.
/// The state location the object-native metastore keeps its logs and its
/// datasets' data files in: `runtime.state` when it is set, otherwise the local
/// `.spice/data` directory as a `file://` location. Both take the same
/// object-store path, so a local install needs no object store or credentials.
pub(super) fn effective_state_location(app: &app::App) -> Result<StateLocation> {
    if let Some(state) = app.runtime.state.as_ref() {
        return Ok(StateLocation {
            location: state.location.clone(),
            params: state.params.clone(),
            source: StateSource::RuntimeState,
        });
    }
    let directory = spice_data_base_path();
    let location = url::Url::from_directory_path(&directory).map_err(|()| {
        Error::InvalidConfiguration {
            detail: Arc::from(format!(
                "The default state location '{directory}' is not an absolute directory. Set `runtime.state.location` to a local directory or an S3 location."
            )),
        }
    })?;
    Ok(StateLocation {
        location: location.into(),
        params: None,
        source: StateSource::RuntimeState,
    })
}

pub(super) fn location_datasets_url(
    location: &url::Url,
    datasets_prefix: &object_store::path::Path,
) -> Option<String> {
    let root = match (location.scheme(), location.host_str()) {
        ("s3", Some(bucket)) => url::Url::parse(&format!("s3://{bucket}/")).ok()?,
        ("file", None | Some("" | "localhost")) => url::Url::parse("file:///").ok()?,
        _ => return None,
    };
    cayenne::dataset_storage::url_of_object_key(&root, datasets_prefix).map(Into::into)
}

/// What `source` needs that data in the object-native location does not support yet;
/// `None` when its data can go there. Covered:
/// `refresh_mode: full`, `append` and `changes`, with or without a primary key.
/// Upserts and deletes write deletion vectors through the dataset's store, and a
/// change stream's acknowledged position commits with the data it covers, so a
/// dataset reopened from the location alone resumes from it.
pub(super) fn object_native_data_unsupported(
    source: &dyn AccelerationSource,
) -> Option<UnsupportedSetting> {
    let acceleration = source.acceleration()?;
    match resolved_refresh_mode(source, acceleration) {
        RefreshMode::Full | RefreshMode::Append | RefreshMode::Changes => {}
        RefreshMode::Caching => return Some(UnsupportedSetting::RefreshMode("caching")),
        RefreshMode::Snapshot => return Some(UnsupportedSetting::RefreshMode("snapshot")),
        RefreshMode::Disabled => return Some(UnsupportedSetting::RefreshMode("disabled")),
    }
    // A local `cayenne_file_path` is ignored, with a warning: it names where an
    // earlier release kept the acceleration, which is left there. One in an object
    // store (S3 Express) is not supported yet, as `cayenne_s3_zone_ids` is not.
    if acceleration
        .params
        .get("cayenne_file_path")
        .is_some_and(|path| cayenne::is_object_store_path(path.trim()))
    {
        return Some(UnsupportedSetting::Param("cayenne_file_path"));
    }
    ["cayenne_s3_zone_ids", "cayenne_datalake_location"]
        .into_iter()
        .find(|param| {
            acceleration
                .params
                .get(*param)
                .is_some_and(|value| !value.trim().is_empty())
        })
        .map(UnsupportedSetting::Param)
}

/// The warning for a dataset that sets `cayenne_file_path` under
/// `runtime.params.cayenne_vnext`.
fn ignored_file_path_message(dataset: &str, location: &str) -> String {
    format!(
        "Dataset '{dataset}' (cayenne) ignores `cayenne_file_path`, because its acceleration is now in '{location}'. To keep acceleration data on that disk, set `runtime.state.location` to it. Remove `cayenne_file_path` from this dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ignored_file_path_warning_names_the_dataset_the_location_and_the_fix() {
        assert_eq!(
            ignored_file_path_message("orders", "s3://bucket/state/"),
            "Dataset 'orders' (cayenne) ignores `cayenne_file_path`, because its acceleration is now in 's3://bucket/state/'. To keep acceleration data on that disk, set `runtime.state.location` to it. Remove `cayenne_file_path` from this dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
    }

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
