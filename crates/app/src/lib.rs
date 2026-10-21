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

#![allow(clippy::missing_errors_doc)]

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};
use snafu::prelude::*;
pub use spicepod;
use spicepod::{
    Spicepod,
    acceleration::{Acceleration, Mode as AccelerationMode, RefreshMode, SnapshotBehavior},
    component::{
        caching::{CacheConfig, SQLResultsCacheConfig},
        catalog::Catalog,
        dataset::Dataset,
        embeddings::Embeddings,
        function::Function,
        management::Management,
        model::Model,
        rerankers::Reranker,
        runtime::{CorsConfig, Runtime, TlsConfig, runtime_state_folder},
        secret::Secret,
        snapshot::{SNAPSHOTS_QUEUE_URL_PARAM, SNAPSHOTS_STATE_FOLDER, Snapshots},
        tool::Tool,
        view::View,
        worker::Worker,
    },
    extension::Extension,
    param::Params,
};
use util::in_tracing_context;
use util::spice_paths::{absolute_dir, cayenne_metastore_dir, fs_probe_path};

pub mod runtime;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct App {
    pub name: String,

    pub secrets: Vec<Secret>,

    pub extensions: HashMap<String, Extension>,

    pub catalogs: Vec<Catalog>,

    pub datasets: Vec<Dataset>,

    pub views: Vec<View>,

    pub models: Vec<Model>,

    pub embeddings: Vec<Embeddings>,

    pub rerankers: Vec<Reranker>,

    pub tools: Vec<Tool>,

    pub workers: Vec<Worker>,

    pub functions: Vec<Function>,

    pub spicepods: Vec<Spicepod>,

    pub runtime: Runtime,

    pub management: Option<Management>,

    /// The deprecated top-level `snapshots` section as written. Use
    /// [`App::dataset_snapshots`], which resolves a dataset's snapshot configuration.
    pub snapshots: Option<Arc<Snapshots>>,
}

/// The setting whose params a dataset's snapshots use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotParamsSetting {
    /// The dataset's `acceleration.snapshots_params`.
    Dataset,
    /// The top-level `snapshots.params`.
    Section,
    /// `runtime.state.params`.
    RuntimeState,
}

impl App {
    /// The snapshot configuration of a dataset with `acceleration`. Read the
    /// snapshot configuration through this, not the `snapshots` field.
    ///
    /// The location and the bootstrap failure behavior each come from the
    /// acceleration's `snapshots_*` field, else the top-level `snapshots` section,
    /// else `runtime.state` (its `snapshots/` folder) or the default. The params
    /// are the acceleration's `snapshots_params` when set, else the params beside
    /// the location.
    #[must_use]
    pub fn dataset_snapshots(&self, acceleration: &Acceleration) -> Arc<Snapshots> {
        let section = self.snapshots.as_deref();
        let state = self.runtime.state.as_ref();
        let location = acceleration
            .snapshots_location
            .clone()
            .or_else(|| section.and_then(|section| section.location.clone()))
            .or_else(|| {
                state.map(|state| runtime_state_folder(&state.location, SNAPSHOTS_STATE_FOLDER))
            });
        let params = match self.snapshot_params_setting(acceleration) {
            Some(SnapshotParamsSetting::Dataset) => acceleration.snapshots_params.clone(),
            Some(SnapshotParamsSetting::Section) => {
                section.and_then(|section| section.params.clone())
            }
            Some(SnapshotParamsSetting::RuntimeState) => {
                state.and_then(|state| state.params.clone())
            }
            None => None,
        };
        Arc::new(Snapshots {
            enabled: section.is_none_or(|section| section.enabled),
            location,
            bootstrap_on_failure_behavior: acceleration
                .snapshots_bootstrap_on_failure_behavior
                .or_else(|| section.map(|section| section.bootstrap_on_failure_behavior))
                .unwrap_or_default(),
            params,
        })
    }

    /// The setting that holds a dataset's snapshot params, or `None` when none
    /// apply. The acceleration's `snapshots_params` always apply when set.
    /// Otherwise the params sit beside the location: a `snapshots_location` has
    /// none, the top-level `snapshots.location` has the section's `params` as
    /// written, and the `runtime.state` location has the section's `params`, else
    /// `runtime.state.params`.
    fn snapshot_params_setting(
        &self,
        acceleration: &Acceleration,
    ) -> Option<SnapshotParamsSetting> {
        let section = self.snapshots.as_deref();
        if acceleration.snapshots_params.is_some() {
            return Some(SnapshotParamsSetting::Dataset);
        }
        if acceleration.snapshots_location.is_some() {
            return None;
        }
        if section.is_some_and(|section| section.params.is_some()) {
            return Some(SnapshotParamsSetting::Section);
        }
        if section.is_some_and(|section| section.location.is_some()) {
            return None;
        }
        self.runtime
            .state
            .as_ref()
            .and_then(|state| state.params.as_ref())
            .map(|_| SnapshotParamsSetting::RuntimeState)
    }

    /// The settings that set `s3_queue_url` for snapshots that no dataset with
    /// `refresh_mode: snapshot` and `snapshots: enabled` or `bootstrap_only` reads, named as a user writes them, for example
    /// `` `runtime.state.params.s3_queue_url` ``. Empty when the top-level
    /// `snapshots` section turns snapshots off.
    #[must_use]
    pub fn unread_snapshot_queues(&self) -> Vec<String> {
        if self
            .snapshots
            .as_ref()
            .is_some_and(|section| !section.enabled)
        {
            return Vec::new();
        }
        let sets_queue = |params: Option<&Params>| {
            params.is_some_and(|params| params.data.contains_key(SNAPSHOTS_QUEUE_URL_PARAM))
        };
        let read: Vec<(SnapshotParamsSetting, &str)> = self
            .datasets
            .iter()
            .filter_map(|dataset| {
                let acceleration = dataset.acceleration.as_ref().filter(|acceleration| {
                    acceleration.enabled
                        && acceleration.refresh_mode == Some(RefreshMode::Snapshot)
                        && matches!(
                            acceleration.snapshots,
                            SnapshotBehavior::Enabled | SnapshotBehavior::BootstrapOnly
                        )
                })?;
                Some((
                    self.snapshot_params_setting(acceleration)?,
                    dataset.name.as_str(),
                ))
            })
            .collect();
        let is_read = |setting| read.iter().any(|(read, _)| *read == setting);

        let mut unread = Vec::new();
        if sets_queue(
            self.runtime
                .state
                .as_ref()
                .and_then(|state| state.params.as_ref()),
        ) && !is_read(SnapshotParamsSetting::RuntimeState)
        {
            unread.push(format!(
                "`runtime.state.params.{SNAPSHOTS_QUEUE_URL_PARAM}`"
            ));
        }
        if sets_queue(
            self.snapshots
                .as_ref()
                .and_then(|section| section.params.as_ref()),
        ) && !is_read(SnapshotParamsSetting::Section)
        {
            unread.push(format!("`snapshots.params.{SNAPSHOTS_QUEUE_URL_PARAM}`"));
        }
        for dataset in &self.datasets {
            let Some(acceleration) = dataset.acceleration.as_ref() else {
                continue;
            };
            if sets_queue(acceleration.snapshots_params.as_ref())
                && !read.contains(&(SnapshotParamsSetting::Dataset, dataset.name.as_str()))
            {
                unread.push(format!(
                    "`acceleration.snapshots_params.{SNAPSHOTS_QUEUE_URL_PARAM}` of dataset '{}'",
                    dataset.name
                ));
            }
        }
        unread
    }

    /// Retrieve all dataset names that are of a specific connector type.
    #[must_use]
    pub fn datasets_of_connector_type(&self, prefix: &str) -> Vec<String> {
        self.datasets
            .iter()
            .filter(|d| d.from.starts_with(format!("{prefix}:").as_str()))
            .map(|d| d.name.clone())
            .collect()
    }
}

impl Default for App {
    fn default() -> Self {
        App {
            name: "DEFAULT".to_string(),
            secrets: vec![],
            extensions: HashMap::default(),
            catalogs: vec![],
            datasets: vec![],
            views: vec![],
            models: vec![],
            embeddings: vec![],
            rerankers: vec![],
            tools: vec![],
            workers: vec![],
            functions: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
            management: None,
            snapshots: None,
        }
    }
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to load spicepod {}: {source}", path.display()))]
    UnableToLoadSpicepod {
        #[snafu(source(from(spicepod::Error, Box::new)))]
        source: Box<spicepod::Error>,
        path: PathBuf,
    },

    #[snafu(display(
        "Invalid Cayenne configuration: datasets resolve to different Cayenne metastore directories, but one `spiced` process keeps a single Cayenne catalog: {datasets}. Set the same `cayenne_metadata_dir` on every Cayenne dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne#metastore-location"
    ))]
    InvalidCayenneConfiguration { datasets: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Whether the load failed because the Spicepod file is absent, rather than
    /// present but unloadable. See [`spicepod::Error::is_spicepod_missing`].
    #[must_use]
    pub fn is_spicepod_missing(&self) -> bool {
        match self {
            Self::UnableToLoadSpicepod { source, .. } => source.is_spicepod_missing(),
            Self::InvalidCayenneConfiguration { .. } => false,
        }
    }
}

/// Whether `dataset` reads published snapshots (`file_format: snapshot`).
fn reads_snapshots(dataset: &Dataset) -> bool {
    dataset
        .params
        .as_ref()
        .and_then(|params| params.data.get("file_format"))
        .is_some_and(|format| format.as_string().trim().eq_ignore_ascii_case("snapshot"))
}

/// The file-mode Cayenne datasets of a Spicepod with the metastore directory each
/// resolves to, when they resolve to more than one; `None` when they share one.
///
/// One `spiced` process keeps a single Cayenne catalog, so two datasets whose
/// metastores differ cannot both load. The directory follows from
/// `cayenne_file_path` and `cayenne_metadata_dir` through
/// [`cayenne_metastore_dir`], the rule the accelerator itself applies, including
/// the defaults a dataset gets when it sets neither (#14715). Directories are
/// compared through [`comparable_metastore_dir`], so a dataset that spells the
/// default directory out does not read as a conflict with one that leaves it
/// unset; the message shows each directory as it resolves.
fn cayenne_metastore_dir_conflict(datasets: &[Dataset]) -> Option<String> {
    let cayenne_datasets: Vec<(String, String)> = datasets
        .iter()
        .filter_map(|dataset| {
            let acceleration = dataset.acceleration.as_ref()?;
            let engine = acceleration.engine_name();
            let declared_cayenne_file = (engine.eq_ignore_ascii_case("cayenne")
                || engine.eq_ignore_ascii_case("vortex"))
                && matches!(
                    acceleration.mode,
                    AccelerationMode::File
                        | AccelerationMode::FileCreate
                        | AccelerationMode::FileUpdate
                );
            if !acceleration.enabled || !(declared_cayenne_file || reads_snapshots(dataset)) {
                return None;
            }

            let params = acceleration.params.as_ref().map(|params| &params.data);
            let param = |key: &str| {
                params
                    .and_then(|params| params.get(key))
                    .map(spicepod::param::ParamValue::as_string)
            };
            let file_path = param("cayenne_file_path");
            let metadata_dir = param("cayenne_metadata_dir");
            // A snapshot dataset names neither engine nor mode: its engine comes from
            // the snapshots and its mode is always file, so a Cayenne path parameter on
            // it is a Cayenne path. Without one it may not be a Cayenne dataset at all.
            if !declared_cayenne_file && file_path.is_none() && metadata_dir.is_none() {
                return None;
            }

            let metastore_dir =
                cayenne_metastore_dir(file_path.as_deref(), metadata_dir.as_deref());
            Some((dataset.name.clone(), metastore_dir))
        })
        .collect();

    let mut comparable = cayenne_datasets
        .iter()
        .map(|(_, dir)| comparable_metastore_dir(dir));
    let first = comparable.next()?;
    if comparable.all(|dir| dir == first) {
        return None;
    }

    Some(
        cayenne_datasets
            .iter()
            .map(|(name, dir)| format!("'{}' ('{}')", name.escape_debug(), dir.escape_debug()))
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// A metastore directory in the form two datasets' directories are compared in: the
/// catalog file the accelerator opens in it, `{dir}/cayenne.db` built the way the
/// accelerator builds it (so an empty directory is `/cayenne.db`, not the working
/// directory), with a `file:` scheme stripped the way the accelerator strips it
/// ([`fs_probe_path`]) and placed against the working directory the way the
/// accelerator places it ([`absolute_dir`]), compared by component, so `./.spice/data/metadata`,
/// `file:///<cwd>/.spice/data/metadata` and the default `<cwd>/.spice/data/metadata`
/// compare equal, a trailing separator or a `.` component does not make two
/// directories differ, and the root `/` stays the root. Nothing is read from disk,
/// so a `..` component is kept as written rather than folded: `<base>/link/../meta`
/// names a different directory from `<base>/meta` when `link` is a symlink, and a
/// check that folded it would pass two metastores off as one. Two spellings that
/// meet only through `..` or a symlink therefore read as two directories; spell the
/// shared directory the same way on every dataset. The metastore is always opened on
/// the local filesystem, so a directory that reads like a URL (`meta://shared`) is a
/// local path too. SQLite opens the catalog with `SQLITE_OPEN_URI`, so a `file:` value
/// is read the way SQLite reads a URI: its filename ends at a `?` query or `#`
/// fragment and its percent escapes are decoded, while a plain path is read as
/// written. `file:meta%2Fone` is therefore `meta/one/cayenne.db`, not the literal
/// `meta%2Fone/cayenne.db`, and a decoded `%00` ends the filename (see
/// [`decode_uri_part`]). A URI's query options are part of what it names, since
/// they can choose another database for the same file (`mode=memory` opens no file
/// at all), so two URIs name one metastore only when their options match too. A URI
/// whose escapes do not decode to UTF-8 is compared by its decoded bytes, which no
/// plain path equals. A URI that opens no file (see [`file_uri_opens_no_file`]) is
/// named by its spelling, not by a path, so it is compared as written.
fn comparable_metastore_dir(dir: &str) -> ComparableMetastore {
    let place = |path: &str| absolute_dir(path).unwrap_or_else(|_| PathBuf::from(path));
    let catalog = format!("{dir}/cayenne.db");
    let path = fs_probe_path(&catalog);
    if !catalog.starts_with("file:") {
        return ComparableMetastore::File {
            file: Ok(place(path)),
            options: None,
        };
    }

    let without_fragment = path.split('#').next().unwrap_or(path);
    let (filename, options) = match without_fragment.split_once('?') {
        Some((filename, options)) => (filename, Some(options.to_string())),
        None => (without_fragment, None),
    };
    let decoded = decode_uri_part(filename);
    if file_uri_opens_no_file(&decoded, options.as_deref()) {
        return ComparableMetastore::AsWritten(catalog);
    }
    let file = match String::from_utf8(decoded) {
        Ok(filename) => Ok(place(&filename)),
        Err(undecodable) => Err(undecodable.into_bytes()),
    };
    ComparableMetastore::File { file, options }
}

/// Whether SQLite opens a `file:` URI with this decoded filename and these query
/// options as something other than that file: a private in-memory database
/// (`:memory:`), a private temporary database (an empty filename), a named
/// in-memory database (`mode=memory`), or a database behind another VFS
/// (`vfs=memdb`), which names it by its filename string rather than by a path.
fn file_uri_opens_no_file(filename: &[u8], options: Option<&str>) -> bool {
    if filename.is_empty() || filename == b":memory:" {
        return true;
    }
    options
        .into_iter()
        .flat_map(|options| options.split('&'))
        .any(|option| {
            let (key, value) = option.split_once('=').unwrap_or((option, ""));
            let key = decode_uri_part(key);
            key == b"vfs" || (key == b"mode" && decode_uri_part(value) == b"memory")
        })
}

/// A `file:` URI's filename, option key or option value with its percent escapes
/// decoded the way SQLite decodes them: a decoded `%00` ends it, and the rest of it
/// is ignored, so `file::memory:%00tail` opens `:memory:`.
fn decode_uri_part(part: &str) -> Vec<u8> {
    percent_encoding::percent_decode_str(part)
        .take_while(|&byte| byte != 0)
        .collect()
}

/// A metastore directory as [`comparable_metastore_dir`] compares it.
#[derive(Debug, PartialEq, Eq)]
enum ComparableMetastore {
    /// A catalog file.
    File {
        /// The catalog file's absolute path, or a `file:` URI's decoded filename
        /// bytes when they are not UTF-8.
        file: Result<PathBuf, Vec<u8>>,
        /// A `file:` URI's query options.
        options: Option<String>,
    },
    /// A `file:` catalog URI that opens no file, as written.
    AsWritten(String),
}

pub struct AppBuilder {
    name: String,
    secrets: Vec<Secret>,
    extensions: HashMap<String, Extension>,
    catalogs: Vec<Catalog>,
    datasets: Vec<Dataset>,
    views: Vec<View>,
    models: Vec<Model>,
    embeddings: Vec<Embeddings>,
    rerankers: Vec<Reranker>,
    tools: Vec<Tool>,
    workers: Vec<Worker>,
    functions: Vec<Function>,
    spicepods: Vec<Spicepod>,
    runtime: Runtime,
    management: Option<Management>,
    snapshots: Option<Snapshots>,
}

impl AppBuilder {
    pub fn new(name: impl Into<String>) -> AppBuilder {
        AppBuilder {
            name: name.into(),
            secrets: vec![],
            extensions: HashMap::new(),
            catalogs: vec![],
            datasets: vec![],
            views: vec![],
            models: vec![],
            embeddings: vec![],
            rerankers: vec![],
            tools: vec![],
            workers: vec![],
            functions: vec![],
            spicepods: vec![],
            runtime: Runtime::default(),
            management: None,
            snapshots: None,
        }
    }
    #[must_use]
    pub fn with_spicepod(mut self, spicepod: Spicepod) -> AppBuilder {
        self.runtime = spicepod.runtime.clone();
        self.secrets.extend(spicepod.secrets.clone());
        self.extensions.extend(spicepod.extensions.clone());
        if let Some(ref management) = spicepod.management {
            self.management = Some(management.clone());
        }
        if let Some(ref snapshot) = spicepod.snapshots {
            self.snapshots = Some(snapshot.clone());
        }
        self.catalogs.extend(spicepod.catalogs.clone());
        self.datasets.extend(spicepod.datasets.clone());
        self.views.extend(spicepod.views.clone());
        self.models.extend(spicepod.models.clone());
        self.embeddings.extend(spicepod.embeddings.clone());
        self.rerankers.extend(spicepod.rerankers.clone());
        self.tools.extend(spicepod.tools.clone());
        self.workers.extend(spicepod.workers.clone());
        self.functions.extend(spicepod.functions.clone());
        self.spicepods.push(spicepod);
        self
    }

    /// Load a spicepod dependency into the app builder.
    ///
    /// As a dependency, `.runtime`, `.management`, and `.snapshots` configurations will be ignored.
    #[must_use]
    pub fn with_spicepod_dependency(mut self, mut spicepod: Spicepod) -> AppBuilder {
        if spicepod.runtime != Runtime::default() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'runtime' field(s) defined. Runtime configuration must be set in primary spicepod. runtime configuration from dependency will be ignored."
                );
            });
        }
        spicepod.runtime = self.runtime.clone();

        if spicepod.management.is_some() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'management' field(s) defined. Management configuration must be set in primary spicepod. management configuration from dependency will be ignored."
                );
            });
        }
        spicepod.management = None;
        if spicepod.snapshots.is_some() {
            in_tracing_context(|| {
                tracing::warn!(
                    "Spicepod dependency has 'snapshots' field(s) defined. Snapshot configuration must be set in primary spicepod. snapshots configuration from dependency will be ignored."
                );
            });
        }
        spicepod.snapshots = None;
        self = self.with_spicepod(spicepod);
        self
    }

    #[must_use]
    pub fn with_extension(mut self, name: String, extension: Extension) -> AppBuilder {
        self.extensions.insert(name, extension);
        self
    }

    #[must_use]
    pub fn with_secret(mut self, secret: Secret) -> AppBuilder {
        self.secrets.push(secret);
        self
    }

    #[must_use]
    pub fn with_catalog(mut self, catalog: Catalog) -> AppBuilder {
        self.catalogs.push(catalog);
        self
    }

    #[must_use]
    pub fn with_dataset(mut self, dataset: Dataset) -> AppBuilder {
        self.datasets.push(dataset);
        self
    }

    #[must_use]
    pub fn with_view(mut self, view: View) -> AppBuilder {
        self.views.push(view);
        self
    }

    #[must_use]
    pub fn with_model(mut self, model: Model) -> AppBuilder {
        self.models.push(model);
        self
    }

    #[must_use]
    pub fn with_embedding(mut self, embedding: Embeddings) -> AppBuilder {
        self.embeddings.push(embedding);
        self
    }

    #[must_use]
    pub fn with_tool(mut self, tool: Tool) -> AppBuilder {
        self.tools.push(tool);
        self
    }

    #[must_use]
    pub fn with_worker(mut self, worker: Worker) -> AppBuilder {
        self.workers.push(worker);
        self
    }

    #[must_use]
    pub fn with_function(mut self, function: Function) -> AppBuilder {
        self.functions.push(function);
        self
    }

    #[must_use]
    pub fn with_sql_cache(mut self, sql_results: SQLResultsCacheConfig) -> AppBuilder {
        self.runtime.caching.sql_results = Some(sql_results);
        self
    }

    #[must_use]
    pub fn with_search_cache(mut self, search_cache: CacheConfig) -> AppBuilder {
        self.runtime.caching.search_results = Some(search_cache);
        self
    }

    #[must_use]
    pub fn with_embeddings_cache(mut self, embeddings_cache: CacheConfig) -> AppBuilder {
        self.runtime.caching.embeddings = Some(embeddings_cache);
        self
    }

    #[must_use]
    pub fn with_tls_config(mut self, tls_config: TlsConfig) -> AppBuilder {
        self.runtime.tls = Some(tls_config);
        self
    }

    #[must_use]
    pub fn with_runtime_params(mut self, params: HashMap<String, String>) -> AppBuilder {
        self.runtime.params = params;
        self
    }

    #[must_use]
    pub fn with_cors_config(mut self, cors_config: CorsConfig) -> AppBuilder {
        self.runtime.cors = cors_config;
        self
    }

    #[must_use]
    pub fn with_runtime(mut self, runtime: Runtime) -> AppBuilder {
        self.runtime = runtime;
        self
    }

    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: impl Into<String>) -> AppBuilder {
        self.runtime.shutdown_timeout = Some(timeout.into());
        self
    }

    #[must_use]
    pub fn with_management(mut self, management: Management) -> AppBuilder {
        self.management = Some(management);
        self
    }

    #[must_use]
    pub fn with_snapshots(mut self, snapshots: Snapshots) -> AppBuilder {
        self.snapshots = Some(snapshots);
        self
    }

    #[must_use]
    pub fn build(self) -> App {
        App {
            name: self.name,
            secrets: self.secrets,
            extensions: self.extensions,
            catalogs: self.catalogs,
            datasets: self.datasets,
            views: self.views,
            models: self.models,
            embeddings: self.embeddings,
            rerankers: self.rerankers,
            tools: self.tools,
            workers: self.workers,
            functions: self.functions,
            spicepods: self.spicepods,
            runtime: self.runtime,
            management: self.management,
            snapshots: self.snapshots.map(Arc::new),
        }
    }

    pub async fn build_from_path(path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let spicepod_root = Spicepod::load(&path)
            .await
            .context(UnableToLoadSpicepodSnafu { path: path.clone() })?;
        Self::build_from_spicepod(spicepod_root, Spicepod::base_path(&path)).await
    }

    pub async fn build_from_spicepod(spicepod: Spicepod, path: impl Into<PathBuf>) -> Result<App> {
        let path = path.into();
        let secrets = spicepod.secrets.clone();
        let runtime = spicepod.runtime.clone();
        let extensions = spicepod.extensions.clone();
        let management = spicepod.management.clone();
        let snapshots = spicepod.snapshots.clone();
        let mut catalogs: Vec<Catalog> = vec![];
        let mut datasets: Vec<Dataset> = vec![];
        let mut views: Vec<View> = vec![];
        let mut models: Vec<Model> = vec![];
        let mut embeddings: Vec<Embeddings> = vec![];
        let mut rerankers: Vec<Reranker> = vec![];
        let mut tools: Vec<Tool> = vec![];
        let mut workers: Vec<Worker> = vec![];
        let mut functions: Vec<Function> = vec![];

        for catalog in &spicepod.catalogs {
            catalogs.push(catalog.clone());
        }

        for dataset in &spicepod.datasets {
            datasets.push(dataset.clone());
        }

        for view in &spicepod.views {
            views.push(view.clone());
        }

        for model in &spicepod.models {
            models.push(model.clone());
        }

        for embedding in &spicepod.embeddings {
            embeddings.push(embedding.clone());
        }

        for reranker in &spicepod.rerankers {
            rerankers.push(reranker.clone());
        }

        for tool in &spicepod.tools {
            tools.push(tool.clone());
        }

        for worker in &spicepod.workers {
            workers.push(worker.clone());
        }

        for function in &spicepod.functions {
            functions.push(function.clone());
        }

        let root_spicepod_name = spicepod.name.clone();
        let mut spicepods: Vec<Spicepod> = vec![];

        for dependency in &spicepod.dependencies {
            let dependency_path = path.join("spicepods").join(dependency);
            let dependent_spicepod =
                Spicepod::load(&dependency_path)
                    .await
                    .context(UnableToLoadSpicepodSnafu {
                        path: &dependency_path,
                    })?;
            for catalog in &dependent_spicepod.catalogs {
                catalogs.push(catalog.clone());
            }
            for dataset in &dependent_spicepod.datasets {
                datasets.push(dataset.clone());
            }
            for view in &dependent_spicepod.views {
                views.push(view.clone());
            }
            for model in &dependent_spicepod.models {
                models.push(model.clone());
            }
            for embedding in &dependent_spicepod.embeddings {
                embeddings.push(embedding.clone());
            }

            for reranker in &dependent_spicepod.rerankers {
                rerankers.push(reranker.clone());
            }

            for tool in &dependent_spicepod.tools {
                tools.push(tool.clone());
            }

            for worker in &dependent_spicepod.workers {
                workers.push(worker.clone());
            }

            for function in &dependent_spicepod.functions {
                functions.push(function.clone());
            }

            if dependent_spicepod.runtime != Runtime::default() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'runtime' field(s) defined. Runtime configuration must be set in primary spicepod. '{dependency}' runtime configuration will be ignored."
                    );
                });
            }

            if dependent_spicepod.management.is_some() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'management' field(s) defined. Management configuration must be set in primary spicepod. '{dependency}' management configuration will be ignored."
                    );
                });
            }

            if dependent_spicepod.snapshots.is_some() {
                in_tracing_context(|| {
                    tracing::warn!(
                        "Spicepod dependency '{dependency}' has 'snapshots' field(s) defined. Snapshot configuration must be set in primary spicepod. '{dependency}' snapshots configuration will be ignored."
                    );
                });
            }

            spicepods.push(dependent_spicepod);
        }

        spicepods.push(spicepod);
        if let Some(datasets) = cayenne_metastore_dir_conflict(&datasets) {
            return InvalidCayenneConfigurationSnafu { datasets }.fail();
        }

        Ok(App {
            name: root_spicepod_name,
            secrets,
            extensions,
            catalogs,
            datasets,
            views,
            models,
            embeddings,
            rerankers,
            tools,
            workers,
            functions,
            spicepods,
            runtime,
            management,
            snapshots: snapshots.map(Arc::new),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AccelerationMode, Error, cayenne_metastore_dir_conflict};
    use spicepod::{acceleration::Acceleration, component::dataset::Dataset, param::Params};
    use std::collections::HashMap;
    use util::spice_paths::spice_data_base_path;

    fn cayenne_dataset(name: &str, file_path: Option<&str>, metadata_dir: Option<&str>) -> Dataset {
        let mut params = HashMap::new();
        if let Some(file_path) = file_path {
            params.insert("cayenne_file_path".to_string(), file_path.to_string());
        }
        if let Some(metadata_dir) = metadata_dir {
            params.insert("cayenne_metadata_dir".to_string(), metadata_dir.to_string());
        }

        let mut dataset = Dataset::new("file:data.parquet", name);
        dataset.acceleration = Some(Acceleration {
            engine: Some("cayenne".to_string()),
            mode: AccelerationMode::File,
            params: (!params.is_empty()).then(|| Params::from_string_map(params)),
            ..Default::default()
        });
        dataset
    }

    #[test]
    fn cayenne_rejects_multiple_data_roots_without_shared_metadata() {
        let datasets = vec![
            cayenne_dataset("orders", Some("/mnt/a/cayenne"), None),
            cayenne_dataset("customers", Some("/mnt/b/cayenne"), None),
        ];

        let datasets = cayenne_metastore_dir_conflict(&datasets).expect("paths must conflict");
        let error = Error::InvalidCayenneConfiguration { datasets };
        assert_eq!(
            error.to_string(),
            "Invalid Cayenne configuration: datasets resolve to different Cayenne metastore directories, but one `spiced` process keeps a single Cayenne catalog: 'orders' ('/mnt/a/cayenne/metadata'), 'customers' ('/mnt/b/cayenne/metadata'). Set the same `cayenne_metadata_dir` on every Cayenne dataset. See: https://spiceai.org/docs/components/data-accelerators/cayenne#metastore-location"
        );
    }

    /// A dataset that leaves `cayenne_file_path` unset still has a metastore, at the
    /// default `.spice/data/metadata`, so it conflicts with one that sets a path
    /// (#14715).
    #[test]
    fn cayenne_rejects_a_default_data_root_beside_an_explicit_one() {
        let datasets = vec![
            cayenne_dataset("explicit_path", Some("/abs/custom/"), None),
            cayenne_dataset("default_path", None, None),
        ];

        assert_eq!(
            cayenne_metastore_dir_conflict(&datasets),
            Some(format!(
                "'explicit_path' ('/abs/custom/metadata'), 'default_path' ('{}/metadata')",
                spice_data_base_path().escape_debug()
            ))
        );
    }

    /// Two datasets that set only `cayenne_metadata_dir`, to different directories,
    /// resolve to two metastores (#14715, second shape).
    #[test]
    fn cayenne_rejects_two_metadata_dirs_without_file_paths() {
        let datasets = vec![
            cayenne_dataset("meta_a", None, Some("/abs/meta-a/")),
            cayenne_dataset("meta_b", None, Some("/abs/meta-b/")),
        ];

        assert_eq!(
            cayenne_metastore_dir_conflict(&datasets).as_deref(),
            Some("'meta_a' ('/abs/meta-a/'), 'meta_b' ('/abs/meta-b/')")
        );
    }

    #[test]
    fn cayenne_accepts_multiple_data_roots_with_shared_metadata() {
        let datasets = vec![
            cayenne_dataset(
                "orders",
                Some("/mnt/a/cayenne"),
                Some("/data/cayenne/metadata"),
            ),
            cayenne_dataset(
                "customers",
                Some("/mnt/b/cayenne"),
                Some("/data/cayenne/metadata/"),
            ),
            cayenne_dataset("default_root", None, Some("/data/cayenne/metadata")),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    #[test]
    fn cayenne_accepts_one_shared_data_root_without_explicit_metadata() {
        let datasets = vec![
            cayenne_dataset("orders", Some("/data/cayenne"), None),
            cayenne_dataset("customers", Some("/data/cayenne/"), None),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    #[test]
    fn cayenne_accepts_every_dataset_on_the_defaults() {
        let datasets = vec![
            cayenne_dataset("orders", None, None),
            cayenne_dataset("customers", None, None),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    /// The default directory spelled out relative to the working directory is the
    /// default directory, not a second metastore.
    #[test]
    fn cayenne_accepts_the_default_directory_spelled_relative() {
        let datasets = vec![
            cayenne_dataset("spelled_out", Some("./.spice/data"), None),
            cayenne_dataset("default_path", None, None),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    /// A `file:` URI names the same directory as the plain path, as it does for the
    /// accelerator, so the two spellings are one metastore.
    #[test]
    fn cayenne_accepts_a_file_uri_spelling_of_the_same_directory() {
        let datasets = vec![
            cayenne_dataset("as_uri", Some("file:///mnt/a/cayenne"), None),
            cayenne_dataset("as_path", Some("/mnt/a/cayenne/"), None),
            cayenne_dataset(
                "explicit",
                None,
                Some("file://localhost/mnt/a/cayenne/metadata"),
            ),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    /// A `.` component and a trailing separator are spellings of one directory. A
    /// `..` component is not folded: `<base>/link/../meta` names `<base>/target/meta`
    /// when `link` is a symlink to `<base>/target/child`, so folding it lexically
    /// would pass two metastores off as one, and the check reads nothing from disk.
    /// Such a spelling is reported as a different directory, and the message asks
    /// for one spelling on every dataset.
    #[test]
    fn cayenne_folds_self_references_but_not_parent_references() {
        let datasets = vec![
            cayenne_dataset("plain", None, Some("/data/meta")),
            cayenne_dataset("through_self", None, Some("/data/./meta/")),
        ];
        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());

        let datasets = vec![
            cayenne_dataset("plain", None, Some("/data/meta")),
            cayenne_dataset("through_parent", None, Some("/data/meta/../meta")),
        ];
        assert_eq!(
            cayenne_metastore_dir_conflict(&datasets).as_deref(),
            Some("'plain' ('/data/meta'), 'through_parent' ('/data/meta/../meta')")
        );
    }

    /// A `..` after a symlink names a directory under the link's target, not under the
    /// link's parent: `<base>/link/../meta` is `<base>/target/meta` when `link` points
    /// to `<base>/target/child`, so it must not pass as `<base>/meta`.
    #[test]
    #[cfg(unix)]
    fn cayenne_keeps_a_parent_reference_through_a_symlink_distinct() {
        let base = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(base.path().join("target/child")).expect("target dir");
        std::os::unix::fs::symlink(base.path().join("target/child"), base.path().join("link"))
            .expect("symlink");
        let through_link = base.path().join("link/../meta");
        let plain = base.path().join("meta");
        // The operating system places them in different directories.
        std::fs::create_dir_all(&through_link).expect("dir through the link");
        assert!(base.path().join("target/meta").is_dir());
        assert!(!plain.exists());

        let datasets = vec![
            cayenne_dataset("plain", None, Some(&plain.to_string_lossy())),
            cayenne_dataset("through_link", None, Some(&through_link.to_string_lossy())),
        ];
        assert!(cayenne_metastore_dir_conflict(&datasets).is_some());
    }

    /// The filesystem root is a directory of its own, not the working directory a
    /// trailing separator trimmed to nothing would place it at.
    #[test]
    fn cayenne_keeps_the_filesystem_root_distinct_from_the_working_directory() {
        let cwd = std::env::current_dir().expect("the working directory is readable");
        let datasets = vec![
            cayenne_dataset("at_root", None, Some("/")),
            cayenne_dataset("at_cwd", None, Some(&cwd.to_string_lossy())),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_some());
    }

    /// A relative `cayenne_metadata_dir` is placed against the working directory, so
    /// it is the same metastore as its absolute spelling.
    #[test]
    fn cayenne_accepts_a_relative_metadata_dir_beside_its_absolute_spelling() {
        let cwd = std::env::current_dir().expect("the working directory is readable");
        let absolute = cwd.join("meta").to_string_lossy().into_owned();
        let datasets = vec![
            cayenne_dataset("relative", None, Some("./meta")),
            cayenne_dataset("absolute", None, Some(&absolute)),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    /// An empty `cayenne_metadata_dir` puts the catalog at `/cayenne.db`, not in the
    /// working directory, so it is a different metastore from the working directory.
    #[test]
    fn cayenne_keeps_an_empty_metadata_dir_distinct_from_the_working_directory() {
        let cwd = std::env::current_dir().expect("the working directory is readable");
        let datasets = vec![
            cayenne_dataset("empty", Some("/mnt/a/cayenne"), Some("")),
            cayenne_dataset(
                "at_cwd",
                Some("/mnt/b/cayenne"),
                Some(&cwd.to_string_lossy()),
            ),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_some());
    }

    /// The metastore is always opened on the local filesystem, so a metadata
    /// directory that reads like a URL is a relative local path, the same metastore
    /// as its absolute spelling.
    #[test]
    fn cayenne_places_a_url_like_metadata_dir_against_the_working_directory() {
        let cwd = std::env::current_dir().expect("the working directory is readable");
        let absolute = cwd.join("meta://shared").to_string_lossy().into_owned();
        let datasets = vec![
            cayenne_dataset("relative", None, Some("meta://shared")),
            cayenne_dataset("absolute", None, Some(&absolute)),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    /// SQLite opens a `file:` catalog as a URI, decoding its percent escapes and
    /// ending its filename at a query, but opens a plain path as written, so the two
    /// spellings name the same metastore only when they name the same file.
    #[test]
    fn cayenne_reads_a_file_uri_the_way_sqlite_opens_it() {
        let conflict = |uri: &str, plain: &str| {
            cayenne_metastore_dir_conflict(&[
                cayenne_dataset("uri", None, Some(uri)),
                cayenne_dataset("plain", None, Some(plain)),
            ])
        };

        assert!(conflict("file:meta%2Fone", "meta%2Fone").is_some());
        assert!(conflict("file:meta%2Fone", "meta/one").is_none());
        assert!(conflict("file:shared?label=x", "shared").is_some());
    }

    /// A URI's query options can choose a different database for the same filename
    /// (`mode=memory` opens no file at all), so two URIs that differ only in their
    /// options are two metastores; the same URI spelled twice is one.
    #[test]
    fn cayenne_compares_a_file_uri_with_its_options() {
        let conflict = |a: &str, b: &str| {
            cayenne_metastore_dir_conflict(&[
                cayenne_dataset("a", None, Some(a)),
                cayenne_dataset("b", None, Some(b)),
            ])
        };

        assert!(
            conflict(
                "file:meta?mode=memory&unused=",
                "file:meta?mode=rwc&unused="
            )
            .is_some()
        );
        assert!(conflict("file:meta?mode=rwc&unused=", "file:meta?mode=rwc&unused=").is_none());
    }

    /// A `file:` URI that opens no file (an in-memory database, a non-default VFS)
    /// is named by its URI as written, not by a filesystem path, so two spellings that
    /// would name one file still name two metastores.
    #[test]
    fn cayenne_compares_a_file_uri_that_opens_no_file_as_written() {
        let conflict = |a: &str, b: &str| {
            cayenne_metastore_dir_conflict(&[
                cayenne_dataset("a", None, Some(a)),
                cayenne_dataset("b", None, Some(b)),
            ])
        };
        let cwd = std::env::current_dir().expect("current dir");
        let cwd = cwd.to_string_lossy();

        assert!(
            conflict(
                "file:meta?mode=memory&cache=shared&unused=",
                &format!("file:{cwd}/meta?mode=memory&cache=shared&unused="),
            )
            .is_some()
        );
        assert!(
            conflict(
                "file:/meta/./shared?vfs=memdb&unused=",
                "file:/meta/shared?vfs=memdb&unused=",
            )
            .is_some()
        );
        assert!(conflict("file::memory:#", &format!("file:{cwd}/:memory:#")).is_some());
        // SQLite ends a filename, option key or option value at a decoded `%00`.
        assert!(
            conflict(
                "file::memory:%00tail",
                &format!("file:{cwd}/:memory:%00tail")
            )
            .is_some()
        );
        assert!(
            conflict(
                "file:meta/./shared?mode=memory%00suffix&cache=shared&unused=",
                "file:meta/shared?mode=memory%00suffix&cache=shared&unused=",
            )
            .is_some()
        );
        assert!(
            conflict(
                "file:/meta/shared?vfs=memdb&unused=",
                "file:/meta/shared?vfs=memdb&unused=",
            )
            .is_none()
        );
    }

    /// SQLite decodes a URI's escapes to bytes whether or not they form UTF-8, so
    /// `file:meta%FF` opens a different file from the literal `meta%FF`.
    #[test]
    fn cayenne_compares_an_undecodable_file_uri_by_its_decoded_bytes() {
        let conflict = |a: &str, b: &str| {
            cayenne_metastore_dir_conflict(&[
                cayenne_dataset("a", None, Some(a)),
                cayenne_dataset("b", None, Some(b)),
            ])
        };

        assert!(conflict("file:meta%FF", "meta%FF").is_some());
        assert!(conflict("file:meta%FF", "file:meta%ff").is_none());
    }

    /// An object-store data path keeps its metastore at the default local
    /// directory, like a dataset that sets no path at all.
    #[test]
    fn cayenne_accepts_an_object_store_data_root_beside_the_default() {
        let datasets = vec![
            cayenne_dataset("in_s3", Some("s3://bucket/prefix"), None),
            cayenne_dataset("default_path", None, None),
        ];

        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    #[test]
    fn cayenne_rejects_conflicts_for_runtime_engine_spellings() {
        let mut datasets = vec![
            cayenne_dataset("orders", Some("/mnt/a/cayenne"), None),
            cayenne_dataset("customers", Some("/mnt/b/cayenne"), None),
        ];
        datasets[0]
            .acceleration
            .as_mut()
            .expect("acceleration")
            .engine = Some("Cayenne".into());
        datasets[1]
            .acceleration
            .as_mut()
            .expect("acceleration")
            .engine = Some("vortex".into());

        // Both spellings must be recognized as Cayenne, so the error names both datasets.
        assert_eq!(
            cayenne_metastore_dir_conflict(&datasets).as_deref(),
            Some("'orders' ('/mnt/a/cayenne/metadata'), 'customers' ('/mnt/b/cayenne/metadata')")
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn cayenne_rejects_conflicts_for_datasets_that_name_no_engine() {
        let mut datasets = vec![
            cayenne_dataset("orders", Some("/mnt/a/cayenne"), None),
            cayenne_dataset("customers", Some("/mnt/b/cayenne"), None),
        ];
        for dataset in &mut datasets {
            dataset.acceleration.as_mut().expect("acceleration").engine = None;
        }

        assert!(cayenne_metastore_dir_conflict(&datasets).is_some());
    }

    #[test]
    fn cayenne_rejects_conflicts_for_snapshot_datasets() {
        let snapshot_dataset = |name: &str, file_path: Option<&str>, metadata_dir: Option<&str>| {
            let mut dataset = cayenne_dataset(name, file_path, metadata_dir);
            dataset.from = "s3://bucket/snapshots/".to_string();
            dataset.params = Some(Params::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "snapshot".to_string(),
            )])));
            let acceleration = dataset.acceleration.as_mut().expect("acceleration");
            acceleration.engine = None;
            acceleration.mode = AccelerationMode::default();
            dataset
        };

        let datasets = vec![
            snapshot_dataset("orders", Some("/data/orders"), None),
            snapshot_dataset("customers", Some("/data/customers"), None),
        ];
        assert!(cayenne_metastore_dir_conflict(&datasets).is_some());

        let datasets = vec![
            snapshot_dataset("orders", Some("/data/orders"), Some("/data/metadata")),
            snapshot_dataset("customers", Some("/data/customers"), Some("/data/metadata")),
        ];
        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());

        // A snapshot dataset that names no Cayenne path may not be a Cayenne dataset
        // at all (its engine comes from the snapshots), so it is not held to the
        // default metastore directory.
        let datasets = vec![
            snapshot_dataset("orders", None, None),
            snapshot_dataset("customers", Some("/data/customers"), None),
        ];
        assert!(cayenne_metastore_dir_conflict(&datasets).is_none());
    }

    mod dataset_snapshots {
        use crate::{App, AppBuilder};
        use spicepod::acceleration::{Acceleration, RefreshMode, SnapshotBehavior};
        use spicepod::component::dataset::Dataset;
        use spicepod::component::runtime::{Runtime, RuntimeState};
        use spicepod::component::snapshot::{BootstrapOnFailureBehavior, Snapshots};
        use spicepod::param::Params;

        const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/snapshots";

        fn params(entries: &[(&str, &str)]) -> Params {
            Params::from_string_map(
                entries
                    .iter()
                    .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                    .collect(),
            )
        }

        fn app(snapshots: Option<Snapshots>, state: Option<RuntimeState>) -> App {
            app_with(snapshots, state, Vec::new())
        }

        fn app_with(
            snapshots: Option<Snapshots>,
            state: Option<RuntimeState>,
            datasets: Vec<Dataset>,
        ) -> App {
            let mut builder = AppBuilder::new("snapshots").with_runtime(Runtime {
                state,
                ..Default::default()
            });
            if let Some(snapshots) = snapshots {
                builder = builder.with_snapshots(snapshots);
            }
            for dataset in datasets {
                builder = builder.with_dataset(dataset);
            }
            builder.build()
        }

        fn state() -> RuntimeState {
            RuntimeState {
                location: "s3://state-bucket/spice/".to_string(),
                params: Some(params(&[("s3_region", "us-east-1")])),
            }
        }

        fn section(location: Option<&str>, section_params: Option<Params>) -> Snapshots {
            Snapshots {
                location: location.map(ToString::to_string),
                params: section_params,
                ..Default::default()
            }
        }

        fn acceleration(location: Option<&str>, dataset_params: Option<Params>) -> Acceleration {
            Acceleration {
                snapshots_location: location.map(ToString::to_string),
                snapshots_params: dataset_params,
                ..Default::default()
            }
        }

        fn resolved(app: &App, acceleration: &Acceleration) -> (Option<String>, Option<Params>) {
            let snapshots = app.dataset_snapshots(acceleration);
            (snapshots.location.clone(), snapshots.params.clone())
        }

        fn state_folder() -> String {
            "s3://state-bucket/spice/snapshots/".to_string()
        }

        fn section_params() -> Params {
            params(&[("s3_region", "eu-west-1")])
        }

        fn dataset_params() -> Params {
            params(&[("s3_region", "ap-south-1")])
        }

        #[test]
        fn without_any_setting_there_is_no_location() {
            assert_eq!(
                *app(None, None).dataset_snapshots(&Acceleration::default()),
                Snapshots::default()
            );
        }

        #[test]
        fn runtime_state_gives_its_snapshots_folder_and_params() {
            assert_eq!(
                resolved(&app(None, Some(state())), &Acceleration::default()),
                (Some(state_folder()), state().params)
            );
        }

        #[test]
        fn section_location_wins_over_runtime_state_and_keeps_its_params_as_written() {
            let without_params = app(
                Some(section(Some("s3://snapshot-bucket/snapshots/"), None)),
                Some(state()),
            );
            assert_eq!(
                resolved(&without_params, &Acceleration::default()),
                (Some("s3://snapshot-bucket/snapshots/".to_string()), None),
                "runtime.state.params are not for the section's location"
            );

            let with_params = app(
                Some(section(
                    Some("s3://snapshot-bucket/snapshots/"),
                    Some(section_params()),
                )),
                Some(state()),
            );
            assert_eq!(
                resolved(&with_params, &Acceleration::default()),
                (
                    Some("s3://snapshot-bucket/snapshots/".to_string()),
                    Some(section_params())
                )
            );
        }

        /// The section's `params` replace `runtime.state.params` as a whole for the
        /// `runtime.state` location, so `s3_region` from `runtime.state` is gone.
        #[test]
        fn section_params_replace_runtime_state_params() {
            let queue = params(&[("s3_queue_url", QUEUE_URL)]);
            assert_eq!(
                resolved(
                    &app(Some(section(None, Some(queue.clone()))), Some(state())),
                    &Acceleration::default()
                ),
                (Some(state_folder()), Some(queue))
            );
        }

        #[test]
        fn dataset_location_wins_and_takes_no_inherited_params() {
            let location = Some("s3://archive-bucket/snapshots/");
            for app in [
                app(None, Some(state())),
                app(
                    Some(section(
                        Some("s3://snapshot-bucket/"),
                        Some(section_params()),
                    )),
                    Some(state()),
                ),
                app(Some(section(None, Some(section_params()))), None),
            ] {
                assert_eq!(
                    resolved(&app, &acceleration(location, None)),
                    (location.map(ToString::to_string), None)
                );
            }
        }

        #[test]
        fn dataset_params_apply_whatever_gives_the_location() {
            let dataset_location = "s3://archive-bucket/snapshots/";
            for (app, acceleration, location) in [
                (
                    app(None, Some(state())),
                    acceleration(None, Some(dataset_params())),
                    Some(state_folder()),
                ),
                (
                    app(
                        Some(section(
                            Some("s3://snapshot-bucket/"),
                            Some(section_params()),
                        )),
                        Some(state()),
                    ),
                    acceleration(None, Some(dataset_params())),
                    Some("s3://snapshot-bucket/".to_string()),
                ),
                (
                    app(Some(section(None, Some(section_params()))), Some(state())),
                    acceleration(Some(dataset_location), Some(dataset_params())),
                    Some(dataset_location.to_string()),
                ),
            ] {
                assert_eq!(
                    resolved(&app, &acceleration),
                    (location, Some(dataset_params()))
                );
            }
        }

        #[test]
        fn bootstrap_on_failure_behavior_falls_back_on_its_own() {
            let section = Snapshots {
                bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Retry,
                ..Default::default()
            };
            let behavior = |app: &App, acceleration: &Acceleration| {
                app.dataset_snapshots(acceleration)
                    .bootstrap_on_failure_behavior
            };
            let dataset = Acceleration {
                snapshots_bootstrap_on_failure_behavior: Some(BootstrapOnFailureBehavior::Fallback),
                snapshots_location: Some("s3://archive-bucket/".to_string()),
                ..Default::default()
            };

            let with_section = app(Some(section), Some(state()));
            assert_eq!(
                behavior(&with_section, &dataset),
                BootstrapOnFailureBehavior::Fallback
            );
            assert_eq!(
                behavior(
                    &with_section,
                    &acceleration(Some("s3://archive-bucket/"), None)
                ),
                BootstrapOnFailureBehavior::Retry,
                "falls back to the section even when the dataset sets the location"
            );
            assert_eq!(
                behavior(&app(None, Some(state())), &Acceleration::default()),
                BootstrapOnFailureBehavior::Warn
            );
        }

        #[test]
        fn section_enabled_false_turns_snapshots_off() {
            let disabled = Snapshots {
                enabled: false,
                ..Default::default()
            };
            assert!(
                !app(Some(disabled), Some(state()))
                    .dataset_snapshots(&acceleration(Some("s3://archive-bucket/"), None))
                    .enabled
            );
        }

        fn snapshot_reader(name: &str, acceleration: Acceleration) -> Dataset {
            let mut dataset = Dataset::new("spice.ai:orders", name);
            dataset.acceleration = Some(Acceleration {
                refresh_mode: Some(RefreshMode::Snapshot),
                snapshots: SnapshotBehavior::BootstrapOnly,
                ..acceleration
            });
            dataset
        }

        fn snapshot_writer(name: &str, acceleration: Acceleration) -> Dataset {
            let mut dataset = Dataset::new("spice.ai:orders", name);
            dataset.acceleration = Some(acceleration);
            dataset
        }

        fn state_with_queue() -> RuntimeState {
            RuntimeState {
                params: Some(params(&[("s3_queue_url", QUEUE_URL)])),
                ..state()
            }
        }

        #[test]
        fn a_queue_a_snapshot_reader_uses_is_read() {
            let queue = Some(params(&[("s3_queue_url", QUEUE_URL)]));
            for app in [
                app_with(
                    None,
                    Some(state_with_queue()),
                    vec![snapshot_reader("orders", Acceleration::default())],
                ),
                app_with(
                    Some(section(None, queue.clone())),
                    Some(state()),
                    vec![snapshot_reader("orders", Acceleration::default())],
                ),
                app_with(
                    None,
                    None,
                    vec![snapshot_reader("orders", acceleration(None, queue))],
                ),
            ] {
                assert_eq!(app.unread_snapshot_queues(), Vec::<String>::new());
            }
        }

        #[test]
        fn each_unread_queue_is_named_by_its_setting() {
            let queue = Some(params(&[("s3_queue_url", QUEUE_URL)]));
            let only_writers = app_with(
                Some(section(None, queue.clone())),
                Some(state_with_queue()),
                vec![
                    snapshot_writer("orders", acceleration(None, queue)),
                    snapshot_reader(
                        "events",
                        acceleration(Some("s3://archive-bucket/snapshots/"), None),
                    ),
                ],
            );
            assert_eq!(
                only_writers.unread_snapshot_queues(),
                vec![
                    "`runtime.state.params.s3_queue_url`".to_string(),
                    "`snapshots.params.s3_queue_url`".to_string(),
                    "`acceleration.snapshots_params.s3_queue_url` of dataset 'orders'".to_string(),
                ]
            );

            // A reader with its own params does not read `runtime.state.params`.
            let reader_with_own_params = app_with(
                None,
                Some(state_with_queue()),
                vec![snapshot_reader(
                    "orders",
                    acceleration(None, Some(params(&[("s3_region", "us-east-1")]))),
                )],
            );
            assert_eq!(
                reader_with_own_params.unread_snapshot_queues(),
                vec!["`runtime.state.params.s3_queue_url`".to_string()]
            );

            // Only `enabled` and `bootstrap_only` snapshots read the queue.
            for snapshots in [SnapshotBehavior::Disabled, SnapshotBehavior::CreateOnly] {
                let mut dataset = snapshot_reader("orders", Acceleration::default());
                if let Some(acceleration) = dataset.acceleration.as_mut() {
                    acceleration.snapshots = snapshots;
                }
                let not_loading = app_with(None, Some(state_with_queue()), vec![dataset]);
                assert_eq!(
                    not_loading.unread_snapshot_queues(),
                    vec!["`runtime.state.params.s3_queue_url`".to_string()],
                    "{snapshots:?}"
                );
            }

            let disabled = app_with(
                Some(Snapshots {
                    enabled: false,
                    ..Default::default()
                }),
                Some(state_with_queue()),
                Vec::new(),
            );
            assert_eq!(disabled.unread_snapshot_queues(), Vec::<String>::new());
        }
    }
}
