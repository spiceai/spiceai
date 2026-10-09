/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Datasets that read acceleration snapshots: `from: s3://…` with `file_format: snapshot`.
//!
//! Such a dataset is shorthand for a read-only snapshot reader: an acceleration in
//! `refresh_mode: snapshot` that restores, and then polls, the snapshots a writer (a
//! dataset with `acceleration.snapshots` enabled) publishes under `from`. The
//! acceleration engine is the one that created the snapshots, and only their
//! `metadata.json` records it, so the dataset cannot be built as an accelerated
//! dataset until that file has been read. Until then the dataset is *pending*: it is
//! built without an acceleration, and loading it resolves it (see
//! `Runtime::resolve_snapshot_source`), recording the engine in the
//! [`SnapshotSourceRegistry`] that every later build reads.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::common::TableReference;
use parking_lot::{Mutex, RwLock};
use runtime_acceleration::Engine;
use runtime_acceleration::snapshot::CurrentSnapshotError;
use snafu::prelude::*;
use spicepod::acceleration::{
    self as spicepod_acceleration, Mode, RefreshMode, SnapshotBehavior, ZeroResultsAction,
};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::Dataset as SpicepodDataset;
use spicepod::component::snapshot::{BootstrapOnFailureBehavior, Snapshots};
use spicepod::param::{ParamValue, Params};
use url::Url;

use super::{InvalidConfigurationSnafu, Result};

/// The `file_format` that makes an `s3` dataset read acceleration snapshots.
pub(crate) const SNAPSHOT_FILE_FORMAT: &str = "snapshot";

/// Where reading snapshots from a dataset's `from` location is documented.
pub(crate) const SNAPSHOT_SOURCE_DOCS: &str =
    "https://spiceai.org/docs/features/data-acceleration/snapshots";

/// Where the S3 params a snapshot dataset reads through are documented.
const S3_CONNECTOR_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/s3#auth";

/// The dataset params a snapshot location honors: the format itself, and the S3
/// connection params the snapshot store reads, which `snapshots.params` accepts too.
const SUPPORTED_PARAMS: &[&str] = &[
    "file_format",
    "s3_region",
    "s3_endpoint",
    "s3_auth",
    "s3_key",
    "s3_secret",
    "s3_session_token",
    "client_timeout",
    "allow_http",
];

/// Whether `params` select `file_format: snapshot`.
pub(crate) fn is_snapshot_format(params: &HashMap<String, String>) -> bool {
    params
        .get("file_format")
        .is_some_and(|format| format.trim().eq_ignore_ascii_case(SNAPSHOT_FILE_FORMAT))
}

/// A dataset that reads acceleration snapshots, as its Spicepod declares it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SnapshotSource {
    /// The `from` location: the prefix that holds the snapshots' `metadata.json`.
    location: String,
    /// The dataset's `acceleration` block as written, completed by [`Self::acceleration`]
    /// once the snapshots' engine is known.
    acceleration: Option<spicepod_acceleration::Acceleration>,
}

impl SnapshotSource {
    /// The snapshot source `dataset` declares, or `None` when it does not set
    /// `file_format: snapshot`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the fix when the dataset reads snapshots from anything
    /// but an S3 location, or configures something a read-only snapshot dataset cannot
    /// honor: a param the snapshot store does not read, write access, or embeddings and
    /// search, which are computed from a source this dataset does not have.
    pub(crate) fn from_spicepod(dataset: &SpicepodDataset) -> Result<Option<Self>> {
        let params = dataset
            .params
            .as_ref()
            .map(Params::as_string_map)
            .unwrap_or_default();
        if !is_snapshot_format(&params) {
            return Ok(None);
        }
        let name = dataset.name.as_str();

        let connector = runtime_component::find_first_delimiter(&dataset.from)
            .map_or(dataset.from.as_str(), |(position, _)| {
                &dataset.from[..position]
            });
        ensure!(
            connector == "s3",
            InvalidConfigurationSnafu {
                config_key: "from",
                message: format!(
                    "Dataset '{name}' sets `file_format: snapshot`, which reads snapshots from S3, but `from` uses the '{connector}' connector. Set `from` to the S3 location the snapshots are published to, such as 's3://my-bucket/snapshots/'. See: {SNAPSHOT_SOURCE_DOCS}"
                ),
            }
        );
        ensure!(
            Url::parse(&dataset.from)
                .is_ok_and(|url| url.host_str().is_some_and(|bucket| !bucket.is_empty())),
            InvalidConfigurationSnafu {
                config_key: "from",
                message: format!(
                    "Dataset '{name}' reads snapshots from '{}', which is not an S3 location. Set `from` to the location the snapshots are published to, such as 's3://my-bucket/snapshots/'. See: {SNAPSHOT_SOURCE_DOCS}",
                    dataset.from
                ),
            }
        );

        let mut unsupported: Vec<&str> = params
            .keys()
            .map(String::as_str)
            .filter(|key| !SUPPORTED_PARAMS.contains(key))
            .collect();
        unsupported.sort_unstable();
        ensure!(
            unsupported.is_empty(),
            InvalidConfigurationSnafu {
                config_key: "params",
                message: format!(
                    "Dataset '{name}' reads snapshots (`file_format: snapshot`), which do not use {}. Remove {}; a snapshot dataset accepts {}. See: {SNAPSHOT_SOURCE_DOCS}",
                    backticked(&unsupported),
                    if unsupported.len() == 1 { "it" } else { "them" },
                    backticked(SUPPORTED_PARAMS),
                ),
            }
        );
        if let Some(auth) = params.get("s3_auth") {
            ensure!(
                matches!(auth.as_str(), "iam_role" | "key"),
                InvalidConfigurationSnafu {
                    config_key: "params.s3_auth",
                    message: format!(
                        "Dataset '{name}' reads snapshots (`file_format: snapshot`) with `s3_auth: {auth}`, which snapshots do not support. Set `s3_auth` to 'iam_role' or 'key'. See: {SNAPSHOT_SOURCE_DOCS}"
                    ),
                }
            );
        }

        ensure!(
            dataset.access == AccessMode::Read,
            InvalidConfigurationSnafu {
                config_key: "access",
                message: format!(
                    "Dataset '{name}' reads snapshots (`file_format: snapshot`), so it is read-only. Remove `access: {}`. See: {SNAPSHOT_SOURCE_DOCS}",
                    access_mode_name(&dataset.access)
                ),
            }
        );
        let computes_columns = !dataset.embeddings.is_empty()
            || dataset
                .vectors
                .as_ref()
                .is_some_and(|vectors| vectors.enabled)
            || dataset
                .full_text_search
                .as_ref()
                .is_some_and(|fts| fts.enabled)
            || dataset.columns.iter().any(|column| {
                !column.embeddings.is_empty()
                    || column
                        .full_text_search
                        .as_ref()
                        .is_some_and(|fts| fts.enabled)
            });
        ensure!(
            !computes_columns,
            InvalidConfigurationSnafu {
                config_key: "embeddings",
                message: format!(
                    "Dataset '{name}' reads snapshots (`file_format: snapshot`), which serve the snapshot's data as it was published, so they cannot compute embeddings or search indexes. Configure `embeddings`, `vectors` and `full_text_search` on the dataset that publishes the snapshots, and remove them here. See: {SNAPSHOT_SOURCE_DOCS}"
                ),
            }
        );

        let source = Self {
            location: dataset.from.clone(),
            acceleration: dataset.acceleration.clone(),
        };
        source.validate(name)?;
        Ok(Some(source))
    }

    /// The `from` location the snapshots are read from.
    pub(crate) fn location(&self) -> &str {
        &self.location
    }

    /// How often the dataset checks for a newer snapshot, when its `acceleration` block
    /// says: waiting for the first snapshot uses the same cadence.
    pub(crate) fn refresh_check_interval(&self) -> Option<Duration> {
        self.acceleration
            .as_ref()?
            .refresh_check_interval
            .as_deref()
            .and_then(|interval| fundu::parse_duration(interval).ok())
    }

    /// The snapshot configuration the dataset reads through: its `from` location with its
    /// S3 params, in place of the Spicepod's top-level `snapshots` section, which
    /// configures where snapshotting datasets publish.
    pub(crate) fn snapshots(&self, dataset_params: &HashMap<String, String>) -> Snapshots {
        let s3_params: HashMap<String, String> = dataset_params
            .iter()
            .filter(|(key, _)| key.as_str() != "file_format")
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Snapshots {
            enabled: true,
            location: Some(self.location.clone()),
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::default(),
            params: (!s3_params.is_empty()).then(|| Params::from_string_map(s3_params)),
        }
    }

    /// The acceleration the dataset runs with once its snapshots are known to have been
    /// created with `engine`: the user's block, completed as a read-only snapshot reader.
    ///
    /// # Errors
    ///
    /// Returns an error when the block contradicts reading the snapshots — see
    /// [`Self::validate`] — or names an engine other than `engine`.
    pub(crate) fn acceleration(
        &self,
        dataset: &TableReference,
        engine: Engine,
    ) -> Result<spicepod_acceleration::Acceleration> {
        let name = dataset.to_string();
        self.validate(&name)?;

        let mut acceleration = self.acceleration.clone().unwrap_or_default();
        if let Some(configured) = acceleration.engine.as_deref() {
            ensure!(
                Engine::try_from(configured).is_ok_and(|configured| configured == engine),
                InvalidConfigurationSnafu {
                    config_key: "acceleration.engine",
                    message: engine_mismatch_message(&name, &self.location, engine, configured),
                }
            );
        }

        if engine != Engine::Cayenne
            && let Some(param) = acceleration.params.as_ref().and_then(|params| {
                CAYENNE_PATH_PARAMS
                    .iter()
                    .find(|param| params.data.contains_key(**param))
            })
        {
            return InvalidConfigurationSnafu {
                config_key: "acceleration.params",
                message: format!(
                    "Dataset '{name}' reads snapshots from '{}' that were created with the '{engine}' engine, so `acceleration.params.{param}`, which sets where a Cayenne copy is kept, does not apply. Remove `acceleration.params.{param}`. See: {SNAPSHOT_SOURCE_DOCS}",
                    self.location
                ),
            }
            .fail();
        }

        acceleration.enabled = true;
        acceleration.engine = Some(engine.to_string());
        acceleration.mode = Mode::File;
        acceleration.refresh_mode = Some(RefreshMode::Snapshot);
        acceleration.snapshots = SnapshotBehavior::BootstrapOnly;

        let params = acceleration.params.get_or_insert_with(Params::default);
        for (param, path) in local_copy_params(dataset, &self.location, engine) {
            params
                .data
                .insert(param.to_string(), ParamValue::String(path));
        }

        Ok(acceleration)
    }

    /// Rejects an `acceleration` block that contradicts reading snapshots. Everything
    /// checked here is independent of the snapshots' engine, so it fails before the
    /// snapshot metadata is read.
    fn validate(&self, dataset: &str) -> Result<()> {
        let Some(acceleration) = &self.acceleration else {
            return Ok(());
        };

        let conflict = |config_key: &str, reason: &str, fix: String| {
            InvalidConfigurationSnafu {
                config_key: format!("acceleration.{config_key}"),
                message: format!(
                    "Dataset '{dataset}' reads snapshots (`file_format: snapshot`), {reason}. {fix}. See: {SNAPSHOT_SOURCE_DOCS}"
                ),
            }
            .fail()
        };

        if !acceleration.enabled {
            return conflict(
                "enabled",
                "so it is always served from an acceleration of the current snapshot",
                "Remove `acceleration.enabled: false`".to_string(),
            );
        }
        if let Some(refresh_mode) = acceleration
            .refresh_mode
            .as_ref()
            .filter(|mode| **mode != RefreshMode::Snapshot)
        {
            return conflict(
                "refresh_mode",
                "so it refreshes only by loading newer snapshots",
                format!(
                    "Remove `acceleration.refresh_mode: {}`",
                    refresh_mode_name(refresh_mode)
                ),
            );
        }
        if matches!(acceleration.mode, Mode::FileCreate | Mode::FileUpdate) {
            return conflict(
                "mode",
                "so its acceleration is a local copy of the current snapshot",
                format!("Remove `acceleration.mode: {}`", acceleration.mode),
            );
        }
        if matches!(
            acceleration.snapshots,
            SnapshotBehavior::Enabled | SnapshotBehavior::CreateOnly
        ) {
            return conflict(
                "snapshots",
                "so it only reads snapshots and never creates them",
                format!(
                    "Remove `acceleration.snapshots: {}`; the dataset that publishes the snapshots creates them",
                    snapshot_behavior_name(acceleration.snapshots)
                ),
            );
        }
        if acceleration.refresh_sql.is_some() {
            return conflict(
                "refresh_sql",
                "so its data is exactly the snapshot's",
                "Remove `acceleration.refresh_sql`, or filter the data in the dataset that publishes the snapshots".to_string(),
            );
        }
        if acceleration.retention_period.is_some()
            || acceleration.retention_sql.is_some()
            || acceleration.retention_check_enabled
        {
            return conflict(
                "retention_period",
                "so its data is exactly the snapshot's",
                "Remove the `acceleration.retention_*` settings, or apply retention in the dataset that publishes the snapshots".to_string(),
            );
        }
        if acceleration.on_zero_results == ZeroResultsAction::UseSource {
            return conflict(
                "on_zero_results",
                "so it has no source to fall back to",
                "Remove `acceleration.on_zero_results: use_source`".to_string(),
            );
        }
        if let Some(param) = acceleration.params.as_ref().and_then(|params| {
            LOCAL_COPY_PARAMS
                .iter()
                .find(|param| params.data.contains_key(**param))
        }) {
            return conflict(
                "params",
                "so Spice chooses where it keeps its local copy of the snapshot, under `.spice/data`",
                format!("Remove `acceleration.params.{param}`"),
            );
        }

        Ok(())
    }
}

/// The accelerator params that choose where a dataset's data lives on disk. Where a
/// snapshot dataset keeps its local copy is Spice's choice; see [`local_copy_params`].
/// [`CAYENNE_PATH_PARAMS`] are allowed: a Cayenne copy is per dataset, not per
/// location, wherever it lives.
const LOCAL_COPY_PARAMS: &[&str] = &[
    "duckdb_file",
    "duckdb_data_dir",
    "sqlite_file",
    "turso_file",
    "cayenne_s3_zone_ids",
];

/// Where a Cayenne snapshot dataset keeps its copy, when it chooses. Rejected once the
/// snapshots turn out to be another engine's.
const CAYENNE_PATH_PARAMS: &[&str] = &["cayenne_file_path", "cayenne_metadata_dir"];

/// Where a snapshot dataset keeps its local copy, for the engines that keep one file
/// per dataset: under `.spice/data`, in a file named for the dataset and the location it
/// reads.
///
/// Scoping the file to the location keeps a dataset pointed at another location from
/// reopening the previous location's copy, which `DuckDB` would otherwise serve from the
/// database it caches, by path, for the life of the process. It also keeps `DuckDB` and
/// `SQLite` from putting every dataset into their shared default file, where restoring
/// one dataset's snapshot would replace the others'.
///
/// Cayenne keeps its own layout: one catalog per process, in the shared metadata
/// directory, and a data directory per dataset, both under `.spice/data` unless the
/// dataset sets them. A snapshot dataset never serves a copy it did not restore in this
/// process, whatever the engine; see `DataFusion::create_accelerated_table`.
fn local_copy_params(
    dataset: &TableReference,
    location: &str,
    engine: Engine,
) -> Vec<(&'static str, String)> {
    let path = |suffix: &str| {
        std::path::Path::new(&data_accelerator_api::spice_data_base_path())
            .join(format!(
                "{}-{}.{suffix}",
                file_stem(dataset),
                &location_hash(location)[..12]
            ))
            .to_string_lossy()
            .into_owned()
    };
    match engine {
        Engine::DuckDB => vec![("duckdb_file", path("duckdb"))],
        Engine::Sqlite => vec![("sqlite_file", path("sqlite"))],
        Engine::Turso => vec![("turso_file", path("turso"))],
        _ => Vec::new(),
    }
}

/// FNV-1a of `location` as the registry keys it: a hash that does not change between
/// builds of Spice, so a local copy keeps its name across upgrades.
fn location_hash(location: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in registry_location(location).bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// The engines this process has found snapshot sources were created with, keyed by
/// dataset and location, so building a dataset — which cannot wait on the network —
/// can complete its acceleration once its snapshots have been described.
#[derive(Debug, Default)]
pub(crate) struct SnapshotSourceRegistry {
    engines: RwLock<HashMap<(TableReference, String), Engine>>,
    resolutions: Mutex<HashMap<TableReference, u64>>,
}

impl SnapshotSourceRegistry {
    /// The engine `dataset`'s snapshots at `location` were created with, once known.
    pub(crate) fn engine(&self, dataset: &TableReference, location: &str) -> Option<Engine> {
        self.engines
            .read()
            .get(&(dataset.clone(), registry_location(location)))
            .copied()
    }

    /// Records that `dataset`'s snapshots at `location` were created with `engine`.
    pub(crate) fn record(&self, dataset: &TableReference, location: &str, engine: Engine) {
        self.engines
            .write()
            .insert((dataset.clone(), registry_location(location)), engine);
    }

    /// Forgets `dataset`: the engines recorded for it, and any resolution in flight, which
    /// stops before it restores or loads anything. For a dataset that is removed, so one
    /// added again later reads its snapshots' metadata afresh.
    pub(crate) fn forget(&self, dataset: &TableReference) {
        self.engines
            .write()
            .retain(|(recorded, _), _| recorded != dataset);
        if let Some(generation) = self.resolutions.lock().get_mut(dataset) {
            *generation += 1;
        }
    }

    /// Starts resolving `dataset`, superseding any resolution already running for it: a
    /// reconfigured dataset starts a new load, and the old one must not also load it.
    pub(crate) fn begin_resolution(self: &Arc<Self>, dataset: &TableReference) -> Resolution {
        let mut resolutions = self.resolutions.lock();
        let generation = resolutions.entry(dataset.clone()).or_default();
        *generation += 1;
        Resolution {
            registry: Arc::clone(self),
            dataset: dataset.clone(),
            generation: *generation,
        }
    }
}

/// One attempt to resolve a snapshot source, from [`SnapshotSourceRegistry::begin_resolution`].
pub(crate) struct Resolution {
    registry: Arc<SnapshotSourceRegistry>,
    dataset: TableReference,
    generation: u64,
}

impl Resolution {
    /// Whether a later resolution of the same dataset has superseded this one.
    pub(crate) fn is_superseded(&self) -> bool {
        self.registry.resolutions.lock().get(&self.dataset) != Some(&self.generation)
    }
}

/// `location` as the registry keys it: a trailing `/` names the same prefix.
fn registry_location(location: &str) -> String {
    location.trim_end_matches('/').to_string()
}

/// A file name for `dataset`'s local copy of its snapshot.
fn file_stem(dataset: &TableReference) -> String {
    dataset
        .to_string()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn backticked(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("`{value}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn access_mode_name(access: &AccessMode) -> &'static str {
    match access {
        AccessMode::Read => "read",
        AccessMode::ReadWrite => "read_write",
        AccessMode::ReadWriteCreate => "read_write_create",
    }
}

fn refresh_mode_name(refresh_mode: &RefreshMode) -> &'static str {
    match refresh_mode {
        RefreshMode::Full => "full",
        RefreshMode::Append => "append",
        RefreshMode::Changes => "changes",
        RefreshMode::Caching => "caching",
        RefreshMode::Snapshot => "snapshot",
    }
}

fn snapshot_behavior_name(behavior: SnapshotBehavior) -> &'static str {
    match behavior {
        SnapshotBehavior::Disabled => "disabled",
        SnapshotBehavior::Enabled => "enabled",
        SnapshotBehavior::BootstrapOnly => "bootstrap_only",
        SnapshotBehavior::CreateOnly => "create_only",
    }
}

/// The configuration error for an `acceleration.engine` that differs from the engine
/// that created the snapshots.
pub(crate) fn engine_mismatch_message(
    dataset: &str,
    location: &str,
    snapshot_engine: Engine,
    configured: &str,
) -> String {
    format!(
        "Dataset '{dataset}' reads snapshots from '{location}' that were created with the '{snapshot_engine}' engine, but `acceleration.engine` is '{configured}'. Remove `acceleration.engine`, which a snapshot dataset takes from its snapshots. See: {SNAPSHOT_SOURCE_DOCS}"
    )
}

/// The warning for a snapshot dataset that cannot load yet and is retried: no snapshot
/// has been published for it, or its metadata could not be read.
pub(crate) fn waiting_for_snapshot_message(
    dataset: &TableReference,
    error: &CurrentSnapshotError,
) -> String {
    match error {
        CurrentSnapshotError::MetadataNotFound { .. }
        | CurrentSnapshotError::DatasetNotFound { .. }
        | CurrentSnapshotError::NoCurrentSnapshot { .. } => format!(
            "Dataset '{dataset}' has no snapshot to load yet, so it cannot be queried until one is published: {error}. Spice keeps checking for it. See: {SNAPSHOT_SOURCE_DOCS}"
        ),
        CurrentSnapshotError::ReadMetadata { .. } => format!(
            "Failed to read the snapshot list of dataset '{dataset}', so it cannot be queried until a read succeeds. Check that the dataset's `s3_*` params reach the bucket and that its credentials can read the object; Spice keeps retrying. Cause: {error}. See: {S3_CONNECTOR_DOCS}"
        ),
        _ => format!(
            "Failed to read the snapshot list of dataset '{dataset}', so it cannot be queried until a valid one is published; Spice keeps checking. Cause: {error}. See: {SNAPSHOT_SOURCE_DOCS}"
        ),
    }
}

/// The error for a snapshot dataset that no retry can load.
pub(crate) fn cannot_load_snapshot_message(dataset: &TableReference, cause: &str) -> String {
    format!(
        "Failed to load dataset '{dataset}' from its snapshots, so it cannot be queried: {cause}. See: {SNAPSHOT_SOURCE_DOCS}"
    )
}

/// The cause, for [`cannot_load_snapshot_message`], when the snapshots were created with
/// an engine this build cannot load them into.
pub(crate) fn unavailable_engine_cause(location: &str, engine: &str) -> String {
    format!(
        "the snapshots at '{location}' were created with the '{engine}' engine, which this build of Spice does not include. Run a build that includes '{engine}'"
    )
}

/// The line reporting which engine a snapshot dataset loads its snapshots into.
pub(crate) fn resolved_message(dataset: &TableReference, location: &str, engine: Engine) -> String {
    format!(
        "Dataset '{dataset}' loads the {engine} snapshots published at '{location}'; it is read-only and checks for newer snapshots on its refresh interval"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use object_store::Error as ObjectStoreError;
    use spicepod::component::dataset::Dataset as SpicepodDataset;

    fn dataset(from: &str, params: &[(&str, &str)]) -> SpicepodDataset {
        let mut dataset = SpicepodDataset::new(from.to_string(), "modules".to_string());
        dataset.params = Some(Params::from_string_map(
            params
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        ));
        dataset
    }

    fn snapshot_dataset() -> SpicepodDataset {
        dataset_at("s3://bucket-b/snapshots/")
    }

    fn dataset_at(from: &str) -> SpicepodDataset {
        dataset(
            from,
            &[("file_format", "snapshot"), ("s3_region", "us-west-2")],
        )
    }

    fn source(dataset: &SpicepodDataset) -> SnapshotSource {
        SnapshotSource::from_spicepod(dataset)
            .expect("the dataset is a valid snapshot source")
            .expect("the dataset reads snapshots")
    }

    fn config_error(dataset: &SpicepodDataset) -> String {
        SnapshotSource::from_spicepod(dataset)
            .expect_err("the dataset is not a valid snapshot source")
            .to_string()
    }

    #[test]
    fn only_file_format_snapshot_reads_snapshots() {
        assert!(
            SnapshotSource::from_spicepod(&dataset(
                "s3://bucket/data/",
                &[("file_format", "parquet")]
            ))
            .expect("a parquet dataset is valid")
            .is_none()
        );
        assert!(
            SnapshotSource::from_spicepod(&dataset("s3://bucket/data/", &[]))
                .expect("a dataset without params is valid")
                .is_none()
        );
        assert!(is_snapshot_format(&HashMap::from([(
            "file_format".to_string(),
            " Snapshot ".to_string()
        )])));

        assert_eq!(
            source(&snapshot_dataset()).location(),
            "s3://bucket-b/snapshots/"
        );
    }

    #[test]
    fn snapshots_are_read_only_from_s3() {
        let message = config_error(&dataset(
            "file:///tmp/snapshots/",
            &[("file_format", "snapshot")],
        ));
        assert_eq!(
            message,
            format!(
                "Invalid configuration for 'from': Dataset 'modules' sets `file_format: snapshot`, which reads snapshots from S3, but `from` uses the 'file' connector. Set `from` to the S3 location the snapshots are published to, such as 's3://my-bucket/snapshots/'. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );

        let message = config_error(&dataset("s3:snapshots", &[("file_format", "snapshot")]));
        assert!(
            message.contains("reads snapshots from 's3:snapshots', which is not an S3 location"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn params_the_snapshot_store_does_not_read_are_rejected() {
        let message = config_error(&dataset(
            "s3://bucket/snapshots/",
            &[
                ("file_format", "snapshot"),
                ("hive_partitioning_enabled", "true"),
                ("csv_has_header", "true"),
            ],
        ));
        assert!(
            message.contains(
                "which do not use `csv_has_header`, `hive_partitioning_enabled`. Remove them;"
            ),
            "the message names every unsupported param: {message}"
        );
        assert!(
            message.contains("`s3_region`"),
            "the message lists the supported params: {message}"
        );

        let message = config_error(&dataset(
            "s3://bucket/snapshots/",
            &[("file_format", "snapshot"), ("s3_auth", "public")],
        ));
        assert!(
            message.contains("with `s3_auth: public`, which snapshots do not support"),
            "unexpected message: {message}"
        );
    }

    #[test]
    fn a_snapshot_dataset_is_read_only_and_computes_nothing() {
        let mut writable = snapshot_dataset();
        writable.access = AccessMode::ReadWrite;
        assert!(
            config_error(&writable).contains("so it is read-only. Remove `access: read_write`"),
            "writes are rejected up front"
        );

        let mut searchable = snapshot_dataset();
        searchable.full_text_search = Some(spicepod::fts::FtsStore {
            enabled: true,
            ..Default::default()
        });
        assert!(
            config_error(&searchable).contains("cannot compute embeddings or search indexes"),
            "search indexes are rejected up front"
        );
    }

    #[test]
    fn the_acceleration_is_a_read_only_snapshot_reader_in_the_recorded_engine() {
        let mut declared = snapshot_dataset();
        declared.acceleration = Some(spicepod_acceleration::Acceleration {
            refresh_check_interval: Some("10s".to_string()),
            ..Default::default()
        });
        let source = source(&declared);
        assert_eq!(
            source.refresh_check_interval(),
            Some(Duration::from_secs(10))
        );

        let acceleration = source
            .acceleration(&TableReference::bare("modules"), Engine::Cayenne)
            .expect("the acceleration is completed");

        assert!(acceleration.enabled);
        assert_eq!(acceleration.engine.as_deref(), Some("cayenne"));
        assert_eq!(acceleration.mode, Mode::File);
        assert_eq!(acceleration.refresh_mode, Some(RefreshMode::Snapshot));
        assert_eq!(acceleration.snapshots, SnapshotBehavior::BootstrapOnly);
        assert_eq!(
            acceleration.refresh_check_interval.as_deref(),
            Some("10s"),
            "the user's settings are kept"
        );
        assert!(
            acceleration
                .params
                .as_ref()
                .is_none_or(|params| params.data.is_empty()),
            "Cayenne keeps its own layout: one catalog per process, a data directory per dataset"
        );
    }

    #[test]
    fn the_local_copy_is_named_for_the_dataset_and_the_location_it_reads() {
        let dataset = TableReference::partial("sales", "modules");
        let source = source(&snapshot_dataset());
        let path_of = |source: &SnapshotSource, engine: Engine, param: &str| {
            source
                .acceleration(&dataset, engine)
                .expect("the acceleration is completed")
                .params
                .as_ref()
                .and_then(|params| params.data.get(param))
                .map(ParamValue::as_string)
                .expect("Spice sets where the local copy goes")
        };

        let stem = format!(
            "sales.modules-{}",
            &location_hash("s3://bucket-b/snapshots/")[..12]
        );
        for (engine, param, suffix) in [
            (Engine::DuckDB, "duckdb_file", "duckdb"),
            (Engine::Sqlite, "sqlite_file", "sqlite"),
            (Engine::Turso, "turso_file", "turso"),
        ] {
            let path = path_of(&source, engine, param);
            assert!(
                path.ends_with(&format!("{stem}.{suffix}")),
                "unexpected {param} for {engine}: {path}"
            );
            assert!(path.contains(".spice"), "{path}");
        }

        let elsewhere = self::source(&dataset_at("s3://bucket-a/snapshots/"));
        assert_ne!(
            path_of(&source, Engine::DuckDB, "duckdb_file"),
            path_of(&elsewhere, Engine::DuckDB, "duckdb_file"),
            "another location never reuses this location's copy"
        );
        assert_eq!(
            path_of(&source, Engine::DuckDB, "duckdb_file"),
            path_of(
                &self::source(&dataset_at("s3://bucket-b/snapshots")),
                Engine::DuckDB,
                "duckdb_file"
            ),
            "a trailing slash names the same location, and so the same copy"
        );
        assert_eq!(
            location_hash("s3://bucket-b/snapshots/"),
            "f2f9bec74bce392a",
            "the hash must not change between builds, or every copy is downloaded again"
        );
    }

    #[test]
    fn a_path_for_the_local_copy_cannot_be_set() {
        for param in LOCAL_COPY_PARAMS {
            let mut declared = snapshot_dataset();
            declared.acceleration = Some(spicepod_acceleration::Acceleration {
                params: Some(Params::from_string_map(HashMap::from([(
                    (*param).to_string(),
                    "/data/modules".to_string(),
                )]))),
                ..Default::default()
            });
            let message = config_error(&declared);
            assert!(
                message.contains(&format!("Remove `acceleration.params.{param}`")),
                "unexpected message for {param}: {message}"
            );
        }

        let mut tuned = snapshot_dataset();
        tuned.acceleration = Some(spicepod_acceleration::Acceleration {
            params: Some(Params::from_string_map(HashMap::from([(
                "duckdb_memory_limit".to_string(),
                "2GB".to_string(),
            )]))),
            ..Default::default()
        });
        let acceleration = source(&tuned)
            .acceleration(&TableReference::bare("modules"), Engine::DuckDB)
            .expect("engine tuning params are kept");
        assert_eq!(
            acceleration
                .params
                .as_ref()
                .and_then(|params| params.data.get("duckdb_memory_limit"))
                .map(ParamValue::as_string)
                .as_deref(),
            Some("2GB")
        );
    }

    #[test]
    fn cayenne_paths_for_the_local_copy_can_be_set() {
        let mut declared = snapshot_dataset();
        declared.acceleration = Some(spicepod_acceleration::Acceleration {
            params: Some(Params::from_string_map(HashMap::from([
                (
                    "cayenne_file_path".to_string(),
                    "/data/modules/".to_string(),
                ),
                (
                    "cayenne_metadata_dir".to_string(),
                    "/data/metadata/".to_string(),
                ),
            ]))),
            ..Default::default()
        });
        let acceleration = source(&declared)
            .acceleration(&TableReference::bare("modules"), Engine::Cayenne)
            .expect("Cayenne paths are accepted");
        let param = |name: &str| {
            acceleration
                .params
                .as_ref()
                .and_then(|params| params.data.get(name))
                .map(ParamValue::as_string)
        };
        assert_eq!(
            param("cayenne_file_path").as_deref(),
            Some("/data/modules/")
        );
        assert_eq!(
            param("cayenne_metadata_dir").as_deref(),
            Some("/data/metadata/")
        );

        let message = source(&declared)
            .acceleration(&TableReference::bare("modules"), Engine::DuckDB)
            .expect_err("Cayenne paths do not apply to DuckDB snapshots")
            .to_string();
        assert!(
            message.contains("Remove `acceleration.params.cayenne_"),
            "{message}"
        );
    }

    #[test]
    fn an_engine_the_snapshots_were_not_created_with_is_rejected() {
        let mut declared = snapshot_dataset();
        declared.acceleration = Some(spicepod_acceleration::Acceleration {
            engine: Some("duckdb".to_string()),
            ..Default::default()
        });
        let source = source(&declared);

        let message = source
            .acceleration(&TableReference::bare("modules"), Engine::Cayenne)
            .expect_err("the configured engine differs")
            .to_string();
        assert_eq!(
            message,
            format!(
                "Invalid configuration for 'acceleration.engine': Dataset 'modules' reads snapshots from 's3://bucket-b/snapshots/' that were created with the 'cayenne' engine, but `acceleration.engine` is 'duckdb'. Remove `acceleration.engine`, which a snapshot dataset takes from its snapshots. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );

        assert!(
            source
                .acceleration(&TableReference::bare("modules"), Engine::DuckDB)
                .is_ok(),
            "the engine the snapshots were created with is accepted"
        );
    }

    #[test]
    fn acceleration_settings_that_contradict_reading_snapshots_are_rejected() {
        let with = |acceleration: spicepod_acceleration::Acceleration| {
            let mut declared = snapshot_dataset();
            declared.acceleration = Some(acceleration);
            config_error(&declared)
        };

        let cases = [
            (
                spicepod_acceleration::Acceleration {
                    enabled: false,
                    ..Default::default()
                },
                "Remove `acceleration.enabled: false`",
            ),
            (
                spicepod_acceleration::Acceleration {
                    refresh_mode: Some(RefreshMode::Full),
                    ..Default::default()
                },
                "Remove `acceleration.refresh_mode: full`",
            ),
            (
                spicepod_acceleration::Acceleration {
                    mode: Mode::FileCreate,
                    ..Default::default()
                },
                "Remove `acceleration.mode: file_create`",
            ),
            (
                spicepod_acceleration::Acceleration {
                    snapshots: SnapshotBehavior::Enabled,
                    ..Default::default()
                },
                "Remove `acceleration.snapshots: enabled`",
            ),
            (
                spicepod_acceleration::Acceleration {
                    refresh_sql: Some("SELECT * FROM modules".to_string()),
                    ..Default::default()
                },
                "Remove `acceleration.refresh_sql`",
            ),
            (
                spicepod_acceleration::Acceleration {
                    retention_period: Some("1d".to_string()),
                    ..Default::default()
                },
                "Remove the `acceleration.retention_*` settings",
            ),
            (
                spicepod_acceleration::Acceleration {
                    on_zero_results: ZeroResultsAction::UseSource,
                    ..Default::default()
                },
                "Remove `acceleration.on_zero_results: use_source`",
            ),
        ];
        for (acceleration, fix) in cases {
            let message = with(acceleration);
            assert!(message.contains(fix), "expected '{fix}' in: {message}");
            assert!(
                message.contains("Dataset 'modules' reads snapshots (`file_format: snapshot`)"),
                "the message names the dataset and why: {message}"
            );
            assert!(message.ends_with(SNAPSHOT_SOURCE_DOCS), "{message}");
        }

        let mut consistent = snapshot_dataset();
        consistent.acceleration = Some(spicepod_acceleration::Acceleration {
            refresh_mode: Some(RefreshMode::Snapshot),
            mode: Mode::File,
            snapshots: SnapshotBehavior::BootstrapOnly,
            ..Default::default()
        });
        assert!(
            SnapshotSource::from_spicepod(&consistent).is_ok(),
            "settings a snapshot reader already has are accepted"
        );
    }

    #[test]
    fn the_dataset_reads_through_its_own_location_and_params() {
        let source = source(&snapshot_dataset());
        let snapshots = source.snapshots(&HashMap::from([
            ("file_format".to_string(), "snapshot".to_string()),
            ("s3_region".to_string(), "us-west-2".to_string()),
        ]));

        assert_eq!(
            snapshots.location.as_deref(),
            Some("s3://bucket-b/snapshots/")
        );
        assert_eq!(
            snapshots.params.as_ref().map(Params::as_string_map),
            Some(HashMap::from([(
                "s3_region".to_string(),
                "us-west-2".to_string()
            )]))
        );
        assert!(snapshots.enabled);
    }

    #[test]
    fn the_registry_keys_by_dataset_and_location() {
        let registry = Arc::new(SnapshotSourceRegistry::default());
        let modules = TableReference::bare("modules");

        assert_eq!(registry.engine(&modules, "s3://bucket/snapshots/"), None);
        registry.record(&modules, "s3://bucket/snapshots/", Engine::Cayenne);
        assert_eq!(
            registry.engine(&modules, "s3://bucket/snapshots"),
            Some(Engine::Cayenne),
            "a trailing slash names the same location"
        );
        assert_eq!(
            registry.engine(&modules, "s3://other/snapshots/"),
            None,
            "another location has not been resolved"
        );
        assert_eq!(
            registry.engine(&TableReference::bare("orders"), "s3://bucket/snapshots/"),
            None
        );
    }

    #[test]
    fn forgetting_a_dataset_drops_its_engines_and_stops_its_resolution() {
        let registry = Arc::new(SnapshotSourceRegistry::default());
        let modules = TableReference::bare("modules");
        let orders = TableReference::bare("orders");
        registry.record(&modules, "s3://bucket/a/", Engine::DuckDB);
        registry.record(&modules, "s3://bucket/b/", Engine::Cayenne);
        registry.record(&orders, "s3://bucket/a/", Engine::Sqlite);
        let resolution = registry.begin_resolution(&modules);

        registry.forget(&modules);

        assert_eq!(registry.engine(&modules, "s3://bucket/a/"), None);
        assert_eq!(registry.engine(&modules, "s3://bucket/b/"), None);
        assert!(resolution.is_superseded(), "an in-flight resolution stops");
        assert_eq!(
            registry.engine(&orders, "s3://bucket/a/"),
            Some(Engine::Sqlite),
            "other datasets are kept"
        );
    }

    #[test]
    fn a_later_resolution_supersedes_an_earlier_one() {
        let registry = Arc::new(SnapshotSourceRegistry::default());
        let modules = TableReference::bare("modules");

        let first = registry.begin_resolution(&modules);
        assert!(!first.is_superseded());

        let second = registry.begin_resolution(&modules);
        assert!(first.is_superseded());
        assert!(!second.is_superseded());
        assert!(
            !registry
                .begin_resolution(&TableReference::bare("orders"))
                .is_superseded()
        );
        assert!(!second.is_superseded(), "other datasets are independent");
    }

    #[test]
    fn waiting_messages_name_the_dataset_the_cause_and_the_docs() {
        let dataset = TableReference::bare("modules");

        let not_published = CurrentSnapshotError::MetadataNotFound {
            metadata: "s3://bucket-b/snapshots/metadata.json".to_string(),
        };
        assert_eq!(
            waiting_for_snapshot_message(&dataset, &not_published),
            format!(
                "Dataset 'modules' has no snapshot to load yet, so it cannot be queried until one is published: 's3://bucket-b/snapshots/metadata.json' does not exist. Spice keeps checking for it. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );

        let unreadable = CurrentSnapshotError::ReadMetadata {
            metadata: "s3://bucket-b/snapshots/metadata.json".to_string(),
            source: ObjectStoreError::Generic {
                store: "S3",
                source: "403 Forbidden".into(),
            },
        };
        let message = waiting_for_snapshot_message(&dataset, &unreadable);
        assert!(
            message.starts_with(
                "Failed to read the snapshot list of dataset 'modules', so it cannot be queried until a read succeeds. Check that the dataset's `s3_*` params reach the bucket and that its credentials can read the object; Spice keeps retrying. Cause: reading 's3://bucket-b/snapshots/metadata.json' failed:"
            ),
            "unexpected message: {message}"
        );
        assert!(message.contains("403 Forbidden"), "{message}");
        assert!(message.ends_with(S3_CONNECTOR_DOCS), "{message}");

        assert_eq!(
            cannot_load_snapshot_message(
                &dataset,
                &unavailable_engine_cause("s3://bucket-b/snapshots/", "turso")
            ),
            format!(
                "Failed to load dataset 'modules' from its snapshots, so it cannot be queried: the snapshots at 's3://bucket-b/snapshots/' were created with the 'turso' engine, which this build of Spice does not include. Run a build that includes 'turso'. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );
    }
}
