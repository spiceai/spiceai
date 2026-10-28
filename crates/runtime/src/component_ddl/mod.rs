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

//! Datasets and catalogs defined with SQL.
//!
//! `CREATE TABLE ... WITH (from = ...)`, `ALTER TABLE`, `DROP TABLE`, and the
//! `CATALOG` statements define the components a Spicepod declares, in the
//! Spicepod's own terms (see [`datafusion_ddl::component_options`]). The
//! runtime keeps what SQL defined in one [`SqlDefinitions`] document in the
//! `sql-definitions/` folder of its state location, adds it to the [`App`] it serves, and
//! applies a statement with the diff that applies a Spicepod edit, so a
//! SQL-defined component loads, reloads, and unloads as a declared one does.
//!
//! A component belongs to where it is defined: SQL changes only what SQL
//! defined (see [`plan`]).

mod plan;
mod store;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;

use app::{App, AppBuilder};
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion_ddl::component_ddl::OptionsChange;
use datafusion_ddl::component_ddl::{ComponentDdlHandler, ComponentDdlOp};
use datafusion_ddl::component_options::{
    ComponentKind, ConnectorParameters, OptionValue, create_statement,
};
use runtime_acceleration::Engine;
use runtime_auth::AuthRequestContext;
use runtime_parameters::{ParameterKey, ParameterSpec, Parameters};
use runtime_request_context::{AsyncMarker, RequestContext};
use spicepod::component::catalog::Catalog;
use spicepod::component::dataset::Dataset;
use tokio::sync::{OnceCell, RwLock};

pub(crate) use plan::resolved_dataset_name;
pub(crate) use store::{DefinitionsStore, SqlDefinitions};

use crate::Runtime;
use crate::component::catalog::CatalogBuilder;
use crate::component::dataset::builder::DatasetBuilder;
use crate::dataconnector::get_connector_factory;
use plan::{Component, Planned, display_dataset_name};

/// How long a statement waits for the dataset it created or changed to
/// register before it returns. A dataset that is still loading after this keeps
/// loading, and the statement says so.
const REGISTRATION_WAIT: Duration = Duration::from_secs(30);

/// How long validating a new definition may take, which includes reaching its
/// source.
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the SQL definitions are read again: while they could not be read,
/// and, from a store other instances share, to serve the changes they make.
const SQL_DEFINITIONS_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// The task that reads the SQL definitions again.
const SQL_DEFINITIONS_TASK: &str = "sql_definitions";

const RUNTIME_STATE_DOCS: &str = "https://spiceai.org/docs/reference/spicepod/runtime#runtimestate";

/// The runtime's SQL-defined components.
pub(crate) struct SqlComponents {
    /// The SQL-defined components the runtime serves. `None` until they are
    /// loaded from the store, and no statement applies before then.
    defined: RwLock<Option<SqlDefinitions>>,
    /// The app the Spicepods declare, before the SQL-defined components are
    /// added: what decides which components SQL may change.
    spicepod: RwLock<Option<Arc<App>>>,
    store: OnceCell<Arc<DefinitionsStore>>,
    /// The instance directory, where the definitions are kept when
    /// `runtime.state.location` is unset.
    instance_dir: PathBuf,
}

impl std::fmt::Debug for SqlComponents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlComponents")
            .field("instance_dir", &self.instance_dir)
            .finish_non_exhaustive()
    }
}

impl SqlComponents {
    pub(crate) fn new(instance_dir: PathBuf) -> Self {
        Self {
            defined: RwLock::new(None),
            spicepod: RwLock::new(None),
            store: OnceCell::new(),
            instance_dir,
        }
    }

    /// The instance directory spiced claims: `$SPICE_CONFIG_DIR`, or `.spice`
    /// in the working directory.
    pub(crate) fn default_instance_dir() -> PathBuf {
        std::env::var_os("SPICE_CONFIG_DIR")
            .filter(|dir| !dir.is_empty())
            .map_or_else(|| PathBuf::from(".spice"), PathBuf::from)
    }

    /// The SQL-defined components, once they are loaded.
    pub(crate) async fn defined(&self) -> Option<SqlDefinitions> {
        self.defined.read().await.clone()
    }
}

/// Applies component statements against a runtime.
pub(crate) struct RuntimeComponentDdl {
    runtime: Weak<Runtime>,
    /// The addresses the runtime serves SQL on.
    listeners: Vec<SocketAddr>,
}

impl std::fmt::Debug for RuntimeComponentDdl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeComponentDdl")
            .field("listeners", &self.listeners)
            .finish_non_exhaustive()
    }
}

impl RuntimeComponentDdl {
    fn runtime(&self) -> DFResult<Arc<Runtime>> {
        self.runtime.upgrade().ok_or_else(|| {
            DataFusionError::Execution(
                "Spice is shutting down; the statement was not applied.".to_string(),
            )
        })
    }
}

#[async_trait]
impl ComponentDdlHandler for RuntimeComponentDdl {
    async fn check_access(&self, op: &ComponentDdlOp) -> DFResult<()> {
        let context = RequestContext::current(AsyncMarker::new().await);
        let noun = op.kind().noun();
        if let Some(principal) = context.auth_principal() {
            if principal.groups().contains(&"admin") {
                return Ok(());
            }
            return Err(DataFusionError::Plan(format!(
                "Defining a {noun} with SQL requires an API key ending in ':admin'; the key used can only read or write data. Add one to `runtime.auth.api-key.keys`. See https://spiceai.org/docs/api/auth"
            )));
        }
        // Without authentication, only a caller on this machine reaches a
        // runtime that listens on loopback alone, and that caller can edit the
        // Spicepod too -- unless a web page it opened sends the request, which a
        // browser does to localhost without asking.
        if !self.listeners.iter().all(|addr| addr.ip().is_loopback()) {
            return Err(DataFusionError::Plan(format!(
                "Defining a {noun} with SQL requires an `:admin` API key when Spice listens beyond localhost. Configure `runtime.auth.api-key` with an `:admin` key, or bind --http and --flight to 127.0.0.1. See https://spiceai.org/docs/api/auth"
            )));
        }
        if let Some(site) = context.extension::<HttpRequestSite>() {
            if let Some(origin) = &site.foreign_origin {
                return Err(DataFusionError::Plan(format!(
                    "Defining a {noun} with SQL from a web page requires an `:admin` API key, and this request came from the page '{origin}'. Send it from a client that is not a browser, or configure `runtime.auth.api-key`. See https://spiceai.org/docs/api/auth"
                )));
            }
            if let Some(host) = &site.foreign_host {
                return Err(DataFusionError::Plan(format!(
                    "Defining a {noun} with SQL requires an `:admin` API key when the request names the host '{host}' rather than localhost. Connect through localhost, or configure `runtime.auth.api-key`. See https://spiceai.org/docs/api/auth"
                )));
            }
        }
        Ok(())
    }

    async fn apply(&self, op: ComponentDdlOp) -> DFResult<String> {
        self.runtime()?.apply_component_ddl(op).await
    }

    async fn create_statement(&self, kind: ComponentKind, name: &str) -> DFResult<Option<String>> {
        let runtime = self.runtime()?;
        let Some(app) = runtime.read_app().await else {
            return Ok(None);
        };
        match kind {
            ComponentKind::Dataset => {
                let wanted = resolved_dataset_name(name);
                let Some(dataset) = app
                    .datasets
                    .iter()
                    .find(|ds| resolved_dataset_name(&ds.name) == wanted)
                else {
                    return Ok(None);
                };
                let shown = ShownParameters::of_dataset(&runtime, dataset).await;
                create_statement(dataset, &|path| shown.shows(path)).map(Some)
            }
            ComponentKind::Catalog => {
                let Some(catalog) = app.catalogs.iter().find(|c| c.name == name) else {
                    return Ok(None);
                };
                let shown = ShownParameters::of_catalog(&runtime, catalog).await;
                create_statement(catalog, &|path| shown.shows(path)).map(Some)
            }
        }
    }
}

/// Where an HTTP request came from, as far as its headers say: a web page on
/// another site (`Origin`), or a client that named a host other than localhost
/// (`Host`), as a page does after rebinding its own name to 127.0.0.1.
/// Recorded for every HTTP request, so a statement that defines a component
/// without an API key refuses one a browser was made to send.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HttpRequestSite {
    /// The request's `Origin`, when it is not a loopback origin.
    pub foreign_origin: Option<String>,
    /// The request's `Host`, when it is not a loopback host.
    pub foreign_host: Option<String>,
}

impl HttpRequestSite {
    pub(crate) fn from_headers(headers: &axum::http::HeaderMap) -> Self {
        use axum::http;
        let header = |name: http::header::HeaderName| {
            headers
                .get(name)
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        };
        let foreign_origin = header(http::header::ORIGIN).filter(|origin| {
            url::Url::parse(origin)
                .ok()
                .and_then(|url| url.host_str().map(is_loopback_host))
                != Some(true)
        });
        let foreign_host = header(http::header::HOST).filter(|host| {
            let name = host
                .rsplit_once(':')
                .filter(|(name, port)| !name.is_empty() && port.chars().all(|c| c.is_ascii_digit()))
                .map_or(host.as_str(), |(name, _)| name);
            !is_loopback_host(name)
        });
        Self {
            foreign_origin,
            foreign_host,
        }
    }
}

impl runtime_request_context::Extension for HttpRequestSite {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Whether `host`, a URL or `Host` header host, names this machine.
fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.eq_ignore_ascii_case("localhost.")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// The parameters a connector reads, from its parameter specs.
pub(crate) struct SpecParameters {
    connector: String,
    prefix: &'static str,
    specs: &'static [ParameterSpec],
}

impl SpecParameters {
    /// The parameters of the connector a component of `kind` reading `from`
    /// uses, when one is registered.
    pub(crate) async fn for_component(kind: ComponentKind, from: &str) -> Option<Self> {
        match kind {
            ComponentKind::Dataset => {
                let source = runtime_component::dataset::DatasetSpec::source_of(from);
                let factory = get_connector_factory(source).await?;
                Some(Self {
                    connector: source.to_string(),
                    prefix: factory.prefix(),
                    specs: factory.parameters(),
                })
            }
            ComponentKind::Catalog => {
                let provider = runtime_component::catalog::CatalogSpec::provider(from);
                let (prefix, specs) = crate::catalogconnector::parameter_specs(provider).await?;
                Some(Self {
                    connector: provider.to_string(),
                    prefix,
                    specs,
                })
            }
        }
    }
}

impl ConnectorParameters for SpecParameters {
    fn connector(&self) -> &str {
        &self.connector
    }

    fn reads(&self, key: &str) -> bool {
        matches!(
            Parameters::classify_key(self.specs, self.prefix, key),
            ParameterKey::Accepted(_) | ParameterKey::Retired
        )
    }
}

/// The names a set of parameter specs shows the values of: each name a spec
/// declares non-secret and none declares secret.
fn shown_names<'a>(
    specs: impl IntoIterator<Item = (&'a str, &'a [ParameterSpec])>,
) -> HashSet<String> {
    let mut shown: HashMap<String, bool> = HashMap::new();
    for (prefix, specs) in specs {
        for spec in specs {
            *shown.entry(spec.display_name(prefix)).or_insert(true) &= !spec.secret;
        }
    }
    shown
        .into_iter()
        .filter_map(|(name, shown)| shown.then_some(name))
        .collect()
}

/// The parameters of the vector engines that declare which are secret, which
/// read `vectors.params` and a column's `embeddings[].params`. The parameters
/// of the other vector engines, and of the full-text engines, are always
/// redacted.
fn vector_engine_specs() -> [(&'static str, &'static [ParameterSpec]); 1] {
    [("s3_vectors", crate::embeddings::index::s3::PARAMETERS)]
}

/// The prefix and parameters of the accelerator `engine`, when it is registered.
async fn accelerator_specs(
    runtime: &Runtime,
    engine: Engine,
) -> Option<(&'static str, &'static [ParameterSpec])> {
    let accelerator = runtime
        .df
        .accelerator_engine_registry()
        .get_accelerator_engine(engine)
        .await?;
    Some((accelerator.prefix(), accelerator.parameters()))
}

/// What `SHOW CREATE` prints of one component's parameters maps: per map, the
/// value of an entry whose name the specs of the map's reader declare
/// non-secret. Any other entry, an unknown one or one of a map no spec
/// describes, is printed as `'<redacted>'`.
#[derive(Debug, Default)]
struct ShownParameters {
    /// `params`: the data or catalog connector's.
    connector: HashSet<String>,
    /// A catalog's `dataset_params`: what every data connector shows.
    dataset_params: HashSet<String>,
    /// `acceleration.params`: the accelerator's.
    accelerator: HashSet<String>,
    /// `acceleration.snapshots_params`: a snapshot location's.
    location: HashSet<String>,
    /// `vectors.params` and a column's `embeddings[].params`.
    vectors: HashSet<String>,
}

impl ShownParameters {
    async fn of_dataset(runtime: &Runtime, dataset: &Dataset) -> Self {
        let source = runtime_component::dataset::DatasetSpec::source_of(&dataset.from);
        let connector = match get_connector_factory(source).await {
            Some(factory) => shown_names([(factory.prefix(), factory.parameters())]),
            None => HashSet::new(),
        };
        let engine = dataset
            .acceleration
            .as_ref()
            .and_then(|acceleration| Engine::try_from(acceleration.engine_name()).ok());
        let accelerator = match engine {
            Some(engine) => shown_names(accelerator_specs(runtime, engine).await),
            None => HashSet::new(),
        };
        Self {
            connector,
            accelerator,
            location: shown_names(runtime_state_location::location_parameter_specs()),
            vectors: shown_names(vector_engine_specs()),
            ..Self::default()
        }
    }

    async fn of_catalog(runtime: &Runtime, catalog: &Catalog) -> Self {
        let provider = runtime_component::catalog::CatalogSpec::provider(&catalog.from);
        let connector = shown_names(crate::catalogconnector::parameter_specs(provider).await);
        let accelerator = shown_names(accelerator_specs(runtime, Engine::Cayenne).await);
        Self {
            connector,
            dataset_params: shown_names(crate::dataconnector::all_parameter_specs().await),
            accelerator,
            ..Self::default()
        }
    }

    /// Whether `SHOW CREATE` prints the value at `path`, an entry of one of the
    /// component's parameters maps.
    fn shows(&self, path: &[String]) -> bool {
        let [map @ .., name] = path else {
            return false;
        };
        let map: Vec<&str> = map.iter().map(String::as_str).collect();
        let shown = match map.as_slice() {
            ["params"] => &self.connector,
            ["dataset_params"] => &self.dataset_params,
            ["acceleration", "params"] => &self.accelerator,
            ["acceleration", "snapshots_params"] => &self.location,
            ["vectors", "params"] | ["columns", _, "embeddings", _, "params"] => &self.vectors,
            _ => return false,
        };
        shown.contains(name)
    }
}

/// The names of the parameters task history and the list of running queries
/// show the values of: each name that some component's specs declare
/// non-secret and none declares secret. Any other parameter is recorded as
/// `'<redacted>'`.
async fn recorded_parameter_names(runtime: &Runtime) -> HashSet<String> {
    let mut specs: Vec<(&'static str, &'static [ParameterSpec])> =
        crate::dataconnector::all_parameter_specs().await;
    specs.extend(crate::catalogconnector::all_parameter_specs().await);
    specs.extend(runtime_state_location::location_parameter_specs());
    let accelerators: Vec<Arc<dyn crate::dataaccelerator::DataAccelerator>> = runtime
        .df
        .accelerator_engine_registry()
        .accelerator_engine_registry
        .read()
        .await
        .values()
        .cloned()
        .collect();
    specs.extend(
        accelerators
            .iter()
            .map(|accelerator| (accelerator.prefix(), accelerator.parameters())),
    );
    specs.extend(vector_engine_specs());
    shown_names(specs)
}

impl Runtime {
    /// Install the handler that applies component statements, once.
    pub(crate) async fn install_component_ddl_handler(self: &Arc<Self>) {
        self.df
            .set_recorded_parameter_names(recorded_parameter_names(self).await);
        let listeners = vec![
            self.config.http_bind_address,
            self.config.flight_bind_address,
        ];
        self.df
            .set_component_ddl_handler(Arc::new(RuntimeComponentDdl {
                runtime: Arc::downgrade(self),
                listeners,
            }));
    }

    async fn sql_definitions_store(&self) -> Result<Arc<DefinitionsStore>, store::Error> {
        self.sql_components
            .store
            .get_or_try_init(|| async {
                let state = self
                    .read_app()
                    .await
                    .and_then(|app| app.runtime.state.clone());
                DefinitionsStore::open(
                    state.as_ref(),
                    &self.sql_components.instance_dir,
                    self.state_location_check.as_ref(),
                    self.secrets(),
                    self.tokio_io_runtime(),
                )
                .await
                .map(Arc::new)
            })
            .await
            .cloned()
    }

    /// Load the SQL-defined components into the app, before the components
    /// load. When they cannot be read, none is served and the statements that
    /// define components are refused, so none is applied over definitions this
    /// runtime did not see, until a retry every
    /// [`SQL_DEFINITIONS_CHECK_INTERVAL`] reads them.
    pub(crate) async fn load_sql_definitions(self: &Arc<Self>) {
        let (store, reported) = match self.read_sql_definitions(false).await {
            Ok(store) if !store.is_shared() => return,
            Ok(store) => (Some(store), None),
            Err(cause) => {
                tracing::error!("{}", self.unread_definitions_message(&cause).await);
                (None, Some(cause))
            }
        };
        let runtime = Arc::downgrade(self);
        // The task is cancelled at shutdown; its future is not awaited.
        drop(
            self.start_runtime_task(SQL_DEFINITIONS_TASK, None, async move {
                Self::keep_sql_definitions(runtime, store, reported).await;
                Ok(())
            })
            .await,
        );
    }

    /// Read the SQL definitions and serve them: before the components load
    /// (`late` false), as part of the app the load reads; after, applied as a
    /// Spicepod edit is. Returns the store they were read from.
    async fn read_sql_definitions(
        self: &Arc<Self>,
        late: bool,
    ) -> Result<Arc<DefinitionsStore>, String> {
        let store = self
            .sql_definitions_store()
            .await
            .map_err(|e| e.to_string())?;
        let defined = store.load().await.map_err(|e| e.to_string())?;
        // Serialized with every other change to the served app.
        let serialized = self.apply_app_lock.lock().await;
        let served = self.read_app().await;
        let spicepod = served
            .clone()
            .unwrap_or_else(|| Arc::new(AppBuilder::new("spice").build()));
        if !defined.is_empty() {
            tracing::info!(
                "Loading {} defined with SQL from {}",
                plan::component_counts(&defined),
                store.location()
            );
            plan::warn_shadowed(&spicepod, &defined);
            let next = Arc::new(plan::serve((*spicepod).clone(), &defined));
            if late {
                Arc::clone(self).apply_app_diff(served.as_ref(), next).await;
            } else {
                *self.app.write().await = Some(next);
            }
        }
        *self.sql_components.spicepod.write().await = Some(spicepod);
        *self.sql_components.defined.write().await = Some(defined);
        drop(serialized);
        Ok(store)
    }

    /// The error a failed read of the SQL definitions logs.
    async fn unread_definitions_message(&self, cause: &str) -> String {
        let shared = self
            .read_app()
            .await
            .is_some_and(|app| app.runtime.state.is_some());
        let check = if shared {
            "Check `runtime.state.location` and its credentials."
        } else {
            "Check that Spice can read its instance directory."
        };
        format!(
            "Failed to read the datasets and catalogs defined with SQL, so none is served and defining one with SQL is refused; Spice retries every {} seconds. {check} Cause: {cause} See {RUNTIME_STATE_DOCS}",
            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs()
        )
    }

    /// Every [`SQL_DEFINITIONS_CHECK_INTERVAL`], until the runtime is gone:
    /// read the SQL definitions while they are not read (`store` is `None`),
    /// and then, from a store other instances share, serve the changes they
    /// make. `reported` is the failure already logged.
    async fn keep_sql_definitions(
        runtime: Weak<Self>,
        mut store: Option<Arc<DefinitionsStore>>,
        mut reported: Option<String>,
    ) {
        let mut interval = tokio::time::interval(SQL_DEFINITIONS_CHECK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes at once, and the definitions were just read.
        interval.tick().await;
        loop {
            interval.tick().await;
            let Some(runtime) = runtime.upgrade() else {
                return;
            };
            let Some(shared) = store.clone() else {
                match runtime.read_sql_definitions(true).await {
                    Ok(read) => {
                        tracing::info!(
                            "Read the datasets and catalogs defined with SQL from {}, and serving them.",
                            read.location()
                        );
                        if !read.is_shared() {
                            return;
                        }
                        store = Some(read);
                        reported = None;
                    }
                    Err(cause) => {
                        // Reported once until it changes.
                        if reported.as_deref() != Some(cause.as_str()) {
                            tracing::error!("{}", runtime.unread_definitions_message(&cause).await);
                        }
                        reported = Some(cause);
                    }
                }
                continue;
            };
            // Compared without the lock first: a change is rare, and a statement
            // or a Spicepod reload should not wait on every check. A failed read
            // takes the locked path, which reports it.
            let unchanged = match (shared.load().await, runtime.sql_components.defined().await) {
                (_, None) => return,
                (Ok(stored), Some(known)) => stored == known,
                (Err(_), Some(_)) => false,
            };
            if unchanged {
                reported = None;
                continue;
            }
            let _serialize = runtime.apply_app_lock.lock().await;
            let Some(known) = runtime.sql_components.defined().await else {
                return;
            };
            match runtime.serve_stored_definitions(&shared, known).await {
                Ok(_) => reported = None,
                Err(cause) => {
                    // Reported once until it changes or the check succeeds.
                    if reported.as_deref() != Some(cause.as_str()) {
                        tracing::warn!(
                            "Failed to apply a change another instance made to the datasets and catalogs defined with SQL in {}, so this instance keeps serving its earlier definitions and retries every {} seconds. Cause: {cause}",
                            shared.location(),
                            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs()
                        );
                    }
                    reported = Some(cause);
                }
            }
        }
    }

    /// Serve the definitions `store` holds when they differ from `known`, the
    /// ones this instance serves: another instance that shares the state
    /// location changed them. A dataset they drop or point at another source
    /// stops serving here and loses its acceleration data, as on the instance
    /// that changed it. The caller holds `apply_app_lock`. Returns the
    /// definitions now served.
    async fn serve_stored_definitions(
        self: &Arc<Self>,
        store: &DefinitionsStore,
        known: SqlDefinitions,
    ) -> Result<SqlDefinitions, String> {
        let stored = store.load().await.map_err(|e| e.to_string())?;
        if stored == known {
            return Ok(known);
        }
        let served = self
            .read_app()
            .await
            .unwrap_or_else(|| Arc::new(AppBuilder::new("spice").build()));
        let spicepod = self
            .sql_components
            .spicepod
            .read()
            .await
            .clone()
            .unwrap_or_else(|| Arc::new(plan::spicepod_part((*served).clone(), &known)));
        let retired = plan::retired_datasets(&spicepod, &known, &stored, None);
        let applied = self.retire(&spicepod, &served, &stored, &retired).await?;
        plan::warn_shadowed(&spicepod, &stored);
        let current = Arc::new(plan::serve((*spicepod).clone(), &stored));
        *self.sql_components.defined.write().await = Some(stored.clone());
        Arc::clone(self)
            .apply_app_diff(Some(&applied), current)
            .await;
        tracing::info!(
            "Applied the change another instance made to the datasets and catalogs defined with SQL in {}: {}.",
            store.location(),
            plan::changed_components(&known, &stored).join(", ")
        );
        Ok(stored)
    }

    /// Stop serving `retired`, the datasets a change to `definitions` retires,
    /// and delete their acceleration data, so no later definition of their
    /// names loads it. Returns the app then served: `served` without them.
    /// When a deletion fails, `served` is served again and the error names the
    /// dataset.
    async fn retire(
        self: &Arc<Self>,
        spicepod: &Arc<App>,
        served: &Arc<App>,
        definitions: &SqlDefinitions,
        retired: &[Dataset],
    ) -> Result<Arc<App>, String> {
        if retired.is_empty() {
            return Ok(Arc::clone(served));
        }
        let mut without = definitions.clone();
        without.datasets.retain(|ds| {
            !retired
                .iter()
                .any(|old| resolved_dataset_name(&old.name) == resolved_dataset_name(&ds.name))
        });
        let without_app = Arc::new(plan::serve((**spicepod).clone(), &without));
        Arc::clone(self)
            .apply_app_diff(Some(served), Arc::clone(&without_app))
            .await;
        for dataset in retired {
            if let Err(e) = Arc::clone(self)
                .delete_acceleration_data(dataset, served)
                .await
            {
                Arc::clone(self)
                    .apply_app_diff(Some(&without_app), Arc::clone(served))
                    .await;
                return Err(format!(
                    "the acceleration data of dataset {} could not be deleted. Cause: {e}",
                    display_dataset_name(&dataset.name)
                ));
            }
        }
        Ok(without_app)
    }

    /// The app the Spicepods declare, without the components defined with SQL
    /// that the runtime serves beside them.
    pub async fn read_spicepod_app(&self) -> Option<Arc<App>> {
        match self.sql_components.spicepod.read().await.clone() {
            Some(spicepod) => Some(spicepod),
            None => self.read_app().await,
        }
    }

    /// The app to serve for `app`, an app the Spicepods declare: it with the
    /// SQL-defined components added.
    pub(crate) async fn with_sql_definitions(&self, app: Arc<App>) -> Arc<App> {
        let Some(defined) = self.sql_components.defined().await else {
            return app;
        };
        let spicepod = Arc::new(plan::spicepod_part((*app).clone(), &defined));
        *self.sql_components.spicepod.write().await = Some(Arc::clone(&spicepod));
        if defined.is_empty() {
            return spicepod;
        }
        plan::warn_shadowed(&spicepod, &defined);
        Arc::new(plan::serve((*spicepod).clone(), &defined))
    }

    /// Apply a component statement: validate the definition it makes, store
    /// it, and apply it as a Spicepod edit is applied.
    pub(crate) async fn apply_component_ddl(
        self: Arc<Self>,
        op: ComponentDdlOp,
    ) -> DFResult<String> {
        if self.distributed.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "{} is not supported in a cluster yet. Define the {} in the Spicepod.",
                op.label(),
                op.kind().noun()
            )));
        }
        let store = self
            .sql_definitions_store()
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let serialized = self.apply_app_lock.lock().await;
        let Some(known) = self.sql_components.defined().await else {
            return Err(DataFusionError::Execution(format!(
                "{} was not applied: the datasets and catalogs defined with SQL could not be read from {}. Spice retries every {} seconds, and its log reports why it fails; run the statement again once they are read.",
                op.label(),
                store.location(),
                SQL_DEFINITIONS_CHECK_INTERVAL.as_secs()
            )));
        };
        // Another instance that shares the state location may have changed
        // the definitions since this one read them: the statement is planned
        // against what is stored, served first.
        let defined = self
            .serve_stored_definitions(&store, known)
            .await
            .map_err(|cause| {
                DataFusionError::Execution(format!("{} was not applied: {cause}", op.label()))
            })?;
        let served = self
            .read_app()
            .await
            .unwrap_or_else(|| Arc::new(AppBuilder::new("spice").build()));
        let spicepod = self
            .sql_components
            .spicepod
            .read()
            .await
            .clone()
            .unwrap_or_else(|| Arc::new(plan::spicepod_part((*served).clone(), &defined)));
        let parameters = Self::altered_connector_parameters(&op, &defined).await;
        let Planned {
            definitions,
            message,
            validate,
        } = plan::plan(
            &spicepod,
            &defined,
            &op,
            parameters.as_ref().map(|p| p as &dyn ConnectorParameters),
        )?;
        let definitions = definitions.as_stored().map_err(|e| {
            DataFusionError::Execution(format!(
                "{} was not applied: its definition could not be stored. Cause: {e}",
                op.label()
            ))
        })?;
        if definitions == defined {
            return Ok(message);
        }

        let next = Arc::new(plan::serve((*spicepod).clone(), &definitions));
        if let Some(component) = &validate {
            tokio::time::timeout(
                VALIDATION_TIMEOUT,
                Arc::clone(&self).validate_component(component, &next),
            )
            .await
            .map_err(|_| {
                DataFusionError::Execution(format!(
                    "{} was not applied: validating it did not finish within {} seconds. Check that its source is reachable, then run it again.",
                    op.label(),
                    VALIDATION_TIMEOUT.as_secs()
                ))
            })??;
        }

        // A dataset this statement drops, replaces, or points at another source
        // stops serving and loses its acceleration data before the change is
        // stored, so no definition ever loads data another one put there.
        let retired = plan::retired_datasets(&spicepod, &defined, &definitions, Some(&op));
        let applied = self
            .retire(&spicepod, &served, &definitions, &retired)
            .await
            .map_err(|cause| {
                DataFusionError::Execution(format!("{} was not applied: {cause}", op.label()))
            })?;

        if let Err(e) = store.replace(&defined, &definitions).await {
            if !Arc::ptr_eq(&applied, &served) {
                Arc::clone(&self)
                    .apply_app_diff(Some(&applied), Arc::clone(&served))
                    .await;
            }
            return Err(DataFusionError::Execution(e.to_string()));
        }
        *self.sql_components.defined.write().await = Some(definitions);
        Arc::clone(&self).apply_app_diff(Some(&applied), next).await;
        drop(serialized);

        let message = match op {
            ComponentDdlOp::CreateDataset { .. } | ComponentDdlOp::CreateCatalog { .. } => {
                match self.ephemeral_store_note(&store, &op).await {
                    Some(note) => format!("{message} {note}"),
                    None => message,
                }
            }
            _ => message,
        };
        match validate {
            Some(Component::Dataset(dataset)) => {
                Ok(self.await_registration(&dataset, message).await)
            }
            Some(Component::Catalog(catalog)) => Ok(self.catalog_outcome(&catalog.name, message)),
            None => Ok(message),
        }
    }

    /// The parameters of the connector an `ALTER` sets options for: the one
    /// a new `from` names, or the component's own.
    async fn altered_connector_parameters(
        op: &ComponentDdlOp,
        defined: &SqlDefinitions,
    ) -> Option<SpecParameters> {
        let new_from = |change: &OptionsChange| match change {
            OptionsChange::Set(options) => {
                options
                    .iter()
                    .find_map(|option| match (&*option.key, &option.value) {
                        ("from", OptionValue::String(from)) => Some(from.clone()),
                        _ => None,
                    })
            }
            OptionsChange::Reset(_) => None,
        };
        let (kind, from) = match op {
            ComponentDdlOp::AlterDataset { name, change } => {
                let wanted = resolved_dataset_name(name);
                let from = new_from(change).or_else(|| {
                    defined
                        .datasets
                        .iter()
                        .find(|ds| resolved_dataset_name(&ds.name) == wanted)
                        .map(|ds| ds.from.clone())
                })?;
                (ComponentKind::Dataset, from)
            }
            ComponentDdlOp::AlterCatalog { name, change } => {
                let from = new_from(change).or_else(|| {
                    defined
                        .catalogs
                        .iter()
                        .find(|c| c.name == *name)
                        .map(|c| c.from.clone())
                })?;
                (ComponentKind::Catalog, from)
            }
            _ => return None,
        };
        SpecParameters::for_component(kind, &from).await
    }

    /// What `op`, a statement that defines a component, says when the
    /// definition is kept where a Kubernetes container loses it on restart: on
    /// the container's own filesystem, without `runtime.state.location`.
    async fn ephemeral_store_note(
        &self,
        store: &DefinitionsStore,
        op: &ComponentDdlOp,
    ) -> Option<String> {
        let shared = self
            .read_app()
            .await
            .is_some_and(|app| app.runtime.state.is_some());
        if shared || std::env::var_os("KUBERNETES_SERVICE_HOST").is_none() {
            return None;
        }
        let location = store.location();
        let component = match op.kind() {
            ComponentKind::Dataset => {
                format!("Dataset {}", display_dataset_name(op.component_name()))
            }
            ComponentKind::Catalog => format!("Catalog '{}'", op.component_name()),
        };
        tracing::warn!(
            "{component} was defined with SQL in {location}, on the container's own filesystem, so it is lost when the container restarts unless that path is on a volume. Set `runtime.state.location` to keep it. See {RUNTIME_STATE_DOCS}"
        );
        Some(format!(
            "Its definition is kept in {location}, which Kubernetes discards when the container restarts unless that path is on a volume. Set `runtime.state.location` to keep it. See {RUNTIME_STATE_DOCS}"
        ))
    }

    /// Check a definition the way loading it would, before it is stored: the
    /// Spicepod's validation, the connector's parameters, and, unless the
    /// dataset is registered without its source, the source's schema.
    async fn validate_component(
        self: Arc<Self>,
        component: &Component,
        app: &Arc<App>,
    ) -> DFResult<()> {
        match component {
            Component::Dataset(dataset) => self.validate_dataset(dataset, app).await,
            Component::Catalog(catalog) => self.validate_catalog(catalog, app).await,
        }
    }

    async fn validate_dataset(self: Arc<Self>, dataset: &Dataset, app: &Arc<App>) -> DFResult<()> {
        let display = display_dataset_name(&dataset.name);
        let invalid = |e: &dyn std::fmt::Display| {
            DataFusionError::Plan(format!("Dataset {display} is not valid: {e}"))
        };
        let built = DatasetBuilder::try_from(dataset.clone())
            .map_err(|e| invalid(&e))?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(&self))
            .build()
            .map_err(|e| invalid(&e))?;
        let built = Arc::new(built);

        let source = built.source();
        let Some(factory) = get_connector_factory(source).await else {
            let suggestion = crate::dataconnector::suggest_connector(source)
                .await
                .map(|s| format!(" Did you mean '{s}'?"))
                .unwrap_or_default();
            return Err(DataFusionError::Plan(format!(
                "Dataset {display} reads from '{source}', which is not a data connector.{suggestion} See https://spiceai.org/docs/components/data-connectors"
            )));
        };
        if let Some(params) = &dataset.params {
            refuse_ignored_parameters(
                &format!("Dataset {display}"),
                factory.parameters(),
                factory.prefix(),
                params.data.keys(),
            )?;
        }

        let connector = self
            .get_dataconnector_from_dataset(Arc::clone(&built))
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!("Dataset {display} cannot be loaded: {e}"))
            })?;
        let context =
            crate::dataconnector::parameters::RuntimeConnectorContext::for_dataset(&built);
        if built.ready_state != crate::component::dataset::ReadyState::OnRegistration {
            connector
                .read_provider(&context, &built)
                .await
                .map_err(|e| {
                    DataFusionError::Plan(format!("Dataset {display} cannot read its source: {e}"))
                })?;
        }
        // A writable dataset writes to its source unless its writes stay in the
        // acceleration, and registering it needs a source that takes them.
        let writes_reach_source = built.access().allows_write()
            && built.acceleration.as_ref().is_none_or(|acceleration| {
                acceleration.write_mode != spicepod::acceleration::WriteMode::Acceleration
            });
        if writes_reach_source {
            match connector.read_write_provider(&context, &built).await {
                None => {
                    return Err(DataFusionError::Plan(format!(
                        "Dataset {display} is writable, but its source '{source}' cannot be written to. Remove `access`, or keep the writes in the acceleration with acceleration.write_mode = 'acceleration'. See https://spiceai.org/docs/reference/sql/dml"
                    )));
                }
                Some(Err(e)) => {
                    return Err(DataFusionError::Plan(format!(
                        "Dataset {display} is writable, but its source cannot be written to: {e}"
                    )));
                }
                Some(Ok(_)) => {}
            }
        }
        Ok(())
    }

    async fn validate_catalog(self: Arc<Self>, catalog: &Catalog, app: &Arc<App>) -> DFResult<()> {
        let name = &catalog.name;
        let invalid = |e: &dyn std::fmt::Display| {
            DataFusionError::Plan(format!("Catalog '{name}' is not valid: {e}"))
        };
        let built = CatalogBuilder::try_from(catalog.clone())
            .map_err(|e| invalid(&e))?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(&self))
            .build()
            .map_err(|e| invalid(&e))?;
        let provider = built.provider.clone();
        let Some((prefix, specs)) = crate::catalogconnector::parameter_specs(&provider).await
        else {
            let suggestion = crate::catalogconnector::suggest_catalog_connector(&provider)
                .await
                .map(|s| format!(" Did you mean '{s}'?"))
                .unwrap_or_default();
            return Err(DataFusionError::Plan(format!(
                "Catalog '{name}' reads from '{provider}', which is not a catalog connector.{suggestion} See https://spiceai.org/docs/components/catalogs"
            )));
        };
        if let Some(params) = &catalog.params {
            refuse_ignored_parameters(
                &format!("Catalog '{name}'"),
                specs,
                prefix,
                params.data.keys(),
            )?;
        }
        // What loading it does first, short of registering it: the connector,
        // with every required parameter, and a provider from its source. An
        // accelerated catalog's provider loads a dataset per table, so it is
        // checked by loading.
        let connector = self.load_catalog_connector(&built).await.map_err(|e| {
            DataFusionError::Plan(format!("Catalog '{name}' cannot be loaded: {e}"))
        })?;
        if built.acceleration.is_none() {
            crate::catalogconnector::get_catalog_provider(
                connector,
                Arc::clone(&self),
                &built,
                None,
            )
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!("Catalog '{name}' cannot read its source: {e}"))
            })?;
        }
        Ok(())
    }

    /// Say how the statement left `catalog`, which its change loaded or left
    /// loading in the background.
    fn catalog_outcome(&self, catalog: &str, message: String) -> String {
        match self.status.get_catalog_statuses().get(catalog) {
            Some(crate::status::ComponentStatus::Error(_)) => {
                format!("{message} It failed to load and is retrying; the Spice log reports why.")
            }
            Some(crate::status::ComponentStatus::Initializing) => {
                format!("{message} It is still loading.")
            }
            _ => message,
        }
    }

    /// Delete the acceleration data of `dataset`, a definition `app` served and
    /// that no longer serves.
    async fn delete_acceleration_data(
        self: Arc<Self>,
        dataset: &Dataset,
        app: &Arc<App>,
    ) -> DFResult<()> {
        if !dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
            return Ok(());
        }
        let built = DatasetBuilder::try_from(dataset.clone())
            .map_err(|e| DataFusionError::Execution(e.to_string()))?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(&self))
            .build()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        self.df
            .delete_acceleration_data(&built)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    }

    /// Wait, within [`REGISTRATION_WAIT`], for `dataset` to register, and say
    /// how the statement left it.
    async fn await_registration(&self, dataset: &Dataset, message: String) -> String {
        // The name the runtime registers the dataset, and its status, under.
        let reference = datafusion::common::TableReference::parse_str(&dataset.name);
        let registered = tokio::time::timeout(
            REGISTRATION_WAIT,
            self.status.wait_for_dataset_registered(&reference),
        )
        .await;
        match (registered, self.status.get_dataset_status(&reference)) {
            (Ok(_), Some(crate::status::ComponentStatus::Error(_))) => format!(
                "{message} It failed to load and is retrying; GET /v1/datasets?status=true reports why."
            ),
            (Ok(_), _) => message,
            (Err(_), _) => format!(
                "{message} It is still loading; GET /v1/datasets?status=true reports its progress."
            ),
        }
    }
}

/// Refuse the parameters a component would ignore: a typo would otherwise
/// leave the component without the setting it was given.
fn refuse_ignored_parameters<'a>(
    component: &str,
    specs: &[ParameterSpec],
    prefix: &str,
    keys: impl Iterator<Item = &'a String>,
) -> DFResult<()> {
    for key in keys {
        let reason = match Parameters::classify_key(specs, prefix, key) {
            ParameterKey::Accepted(_) | ParameterKey::Retired => continue,
            ParameterKey::Unsupported {
                suggestion: Some(suggestion),
            } => format!("Did you mean '{suggestion}'?"),
            ParameterKey::Unsupported { suggestion: None } => {
                "Check the connector's parameters.".to_string()
            }
            ParameterKey::MustBePrefixed { full_prefix } => {
                format!("Write it with the '{full_prefix}' prefix.")
            }
            ParameterKey::MustNotBePrefixed { full_prefix } => {
                format!("Write it without the '{full_prefix}' prefix.")
            }
        };
        return Err(DataFusionError::Plan(format!(
            "{component} has the parameter '{key}', which its connector does not read. {reason}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, header};

    use super::*;

    fn site(origin: Option<&str>, host: Option<&str>) -> HttpRequestSite {
        let mut headers = HeaderMap::new();
        if let Some(origin) = origin {
            headers.insert(
                header::ORIGIN,
                HeaderValue::from_str(origin).expect("origin"),
            );
        }
        if let Some(host) = host {
            headers.insert(header::HOST, HeaderValue::from_str(host).expect("host"));
        }
        HttpRequestSite::from_headers(&headers)
    }

    /// A browser sends `Origin` with a cross-site request and the page's own
    /// host name in `Host` after rebinding it to 127.0.0.1; a client that is
    /// not a browser sends neither.
    #[test]
    fn a_request_from_another_site_or_through_another_host_is_marked() {
        let local = HttpRequestSite::default();
        assert_eq!(site(None, None), local);
        for host in [
            "127.0.0.1:8090",
            "localhost:8090",
            "LOCALHOST",
            "[::1]:8090",
            "127.1.2.3",
        ] {
            assert_eq!(site(None, Some(host)), local, "{host}");
        }
        for origin in [
            "http://localhost:3000",
            "https://127.0.0.1",
            "http://[::1]:8080",
        ] {
            assert_eq!(
                site(Some(origin), Some("127.0.0.1:8090")),
                local,
                "{origin}"
            );
        }

        assert_eq!(
            site(Some("http://evil.example"), Some("127.0.0.1:8090")),
            HttpRequestSite {
                foreign_origin: Some("http://evil.example".to_string()),
                foreign_host: None,
            }
        );
        // A sandboxed frame or a `file:` page sends the opaque origin `null`.
        assert_eq!(
            site(Some("null"), Some("127.0.0.1:8090"))
                .foreign_origin
                .as_deref(),
            Some("null")
        );
        assert_eq!(
            site(None, Some("rebound.example:8090")),
            HttpRequestSite {
                foreign_origin: None,
                foreign_host: Some("rebound.example:8090".to_string()),
            }
        );
        assert_eq!(
            site(None, Some("localhost.evil.example"))
                .foreign_host
                .as_deref(),
            Some("localhost.evil.example")
        );
    }
}
