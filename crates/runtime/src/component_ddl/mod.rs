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
mod served;
mod store;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::time::Duration;

use app::{App, AppBuilder};
use arrow::array::{Array, AsArray, RecordBatch};
use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion_ddl::component_ddl::OptionsChange;
use datafusion_ddl::component_ddl::{ComponentDdlHandler, ComponentDdlNode, ComponentDdlOp};
use datafusion_ddl::component_options::{
    ComponentKind, ConnectorParameters, OptionValue, create_statement,
};
use runtime_acceleration::Engine;
use runtime_acceleration::layout::AccelerationLayout;
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
use crate::dataaccelerator::AccelerationSource;
use crate::dataconnector::get_connector_factory;
use crate::status::ComponentStatus;
use plan::{Component, Planned, display_dataset_name};

/// How long a statement waits for the dataset it created or changed to
/// register before it returns. A dataset that is still loading after this keeps
/// loading, and the statement says so.
const REGISTRATION_WAIT: Duration = Duration::from_secs(30);

/// How often a statement checks that the dataset it is waiting for is in the
/// catalog, once its load has started registering it.
const REGISTRATION_POLL: Duration = Duration::from_millis(5);

/// How many times a statement is planned when another instance keeps changing
/// the definitions under it: a change to other components does not stop the
/// statement, which is planned again against it, as an Iceberg commit retries.
const CONFLICT_ATTEMPTS: usize = 4;

/// How long validating a new definition may take, which includes reaching its
/// source.
const VALIDATION_TIMEOUT: Duration = Duration::from_secs(60);

/// The status that holds the runtime not ready, as `ready_state: on_load`
/// reads readiness, until the SQL definitions are read: until then, none of
/// the datasets and catalogs they define is served.
const SQL_DEFINITIONS_STATUS: &str = "sql_definitions:definitions";

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
    /// Where the definitions are kept and why they could not be read, while
    /// they could not be: what a statement they refuse says.
    unread: parking_lot::Mutex<Option<(String, String)>>,
    /// The instance directory, where the definitions are kept when
    /// `runtime.state.location` is unset.
    instance_dir: PathBuf,
    /// The earlier definitions of datasets whose acceleration data this
    /// instance could not delete when a change dropped them or gave their
    /// names another source: no definition of their names is served until a
    /// retry deletes it, so none answers from the data they left. Recorded
    /// with what this instance serves (see [`served`]), so a restart keeps
    /// them withheld.
    withheld: parking_lot::Mutex<Vec<Dataset>>,
    /// The warnings logged for the SQL-defined components a Spicepod declares
    /// too, so each is logged once while the Spicepod keeps declaring it.
    shadowed: parking_lot::Mutex<Vec<String>>,
    /// The cause a Spicepod reload or a restart logged withholding datasets
    /// for, so the first retry of the same failure does not log it again.
    withheld_logged: parking_lot::Mutex<Option<String>>,
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
            unread: parking_lot::Mutex::new(None),
            instance_dir,
            withheld: parking_lot::Mutex::new(Vec::new()),
            shadowed: parking_lot::Mutex::new(Vec::new()),
            withheld_logged: parking_lot::Mutex::new(None),
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

    /// The earlier definitions of the datasets not served until their
    /// acceleration data is deleted.
    fn withheld(&self) -> Vec<Dataset> {
        self.withheld.lock().clone()
    }

    /// Log each of [`plan::shadowed_warnings`] not logged since the Spicepod
    /// started declaring its component: a reload that keeps a name declared,
    /// or one edit applied more than once, logs nothing again.
    fn warn_shadowed(&self, spicepod: &App, definitions: &SqlDefinitions) {
        let warnings = plan::shadowed_warnings(spicepod, definitions);
        let mut logged = self.shadowed.lock();
        for warning in &warnings {
            if !logged.contains(warning) {
                tracing::warn!("{warning}");
            }
        }
        *logged = warnings;
    }

    /// `spicepod` with `definitions` added, as [`plan::serve`] serves them,
    /// without the datasets withheld until their acceleration data is deleted,
    /// whoever defines them now.
    fn serve(&self, spicepod: &App, definitions: &SqlDefinitions) -> App {
        let mut app = plan::serve(spicepod.clone(), definitions);
        let withheld = self.withheld();
        if !withheld.is_empty() {
            app.datasets.retain(|dataset| {
                !withheld
                    .iter()
                    .any(|old| same_dataset(&old.name, &dataset.name))
            });
        }
        app
    }
}

fn same_dataset(a: &str, b: &str) -> bool {
    resolved_dataset_name(a) == resolved_dataset_name(b)
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
        // A change takes several steps that must all run: the query runs the
        // statement to its end once it starts (see [`RunningStatement`]), so a
        // client that leaves does not stop it partway.
        let runtime = self.runtime()?;
        runtime.apply_component_ddl(op).await
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

/// A statement that defines, changes, or removes a dataset or catalog, as its
/// query runs it: to its end once it starts, whether or not its client stays,
/// since a change cannot stop partway.
pub(crate) struct RunningStatement {
    /// The statement as logs name it: `CREATE TABLE orders`.
    statement: String,
    /// What a log line leaves out of the statement's error: see
    /// [`statement_values`].
    values: Vec<String>,
}

impl RunningStatement {
    /// The statement `plan` runs that defines, changes, or removes a dataset
    /// or catalog, when it runs one: `EXPLAIN` shows one without running it.
    pub(crate) fn of(plan: &LogicalPlan) -> Option<Self> {
        if matches!(plan, LogicalPlan::Explain(_)) {
            return None;
        }
        let mut found = None;
        plan.apply(|node| {
            if let LogicalPlan::Extension(extension) = node
                && let Some(ddl) = extension.node.as_any().downcast_ref::<ComponentDdlNode>()
            {
                found = Some(Self {
                    statement: format!("{} {}", ddl.op.label(), ddl.op.component_name()),
                    values: statement_values(&ddl.op),
                });
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .ok()?;
        found
    }

    /// Logs how the statement ended, `result`, for a client that left before
    /// it did and so was not told.
    pub(crate) fn log_unread_outcome(&self, result: &DFResult<Vec<RecordBatch>>) {
        let statement = &self.statement;
        match result {
            Ok(batches) => {
                let message = batches
                    .first()
                    .and_then(|batch| batch.columns().first())
                    .and_then(|column| column.as_string_opt::<i32>())
                    .filter(|messages| !messages.is_empty())
                    .map_or("", |messages| messages.value(0));
                tracing::info!("`{statement}` finished after its client disconnected: {message}");
            }
            Err(e) => {
                // A log is no place for a credential: the cause is logged
                // without the values of the statement it may repeat.
                let cause = e.to_string();
                let cause = datafusion_ddl::component_options::redact_values(&cause, &self.values);
                let cause = util::single_line(&cause);
                tracing::warn!(
                    "`{statement}` failed after its client disconnected, so it was not applied. Cause: {cause}"
                );
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
        self.status
            .update_component_status(SQL_DEFINITIONS_STATUS, ComponentStatus::Initializing);
        let (store, reported) = match self.read_sql_definitions(false).await {
            Ok(store) => {
                self.status
                    .update_component_status(SQL_DEFINITIONS_STATUS, ComponentStatus::Ready);
                (Some(store), None)
            }
            Err(e) => {
                tracing::error!("{}", self.unread_definitions_message(&e).await);
                self.status.update_component_status(
                    SQL_DEFINITIONS_STATUS,
                    ComponentStatus::Error(Some(e.to_string())),
                );
                (None, Some(e.to_string()))
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
    ) -> Result<Arc<DefinitionsStore>, store::Error> {
        let read = async {
            let store = self.sql_definitions_store().await?;
            let defined = store.load().await?;
            Ok::<_, store::Error>((store, defined))
        };
        let (store, defined) = match read.await {
            Ok(read) => {
                *self.sql_components.unread.lock() = None;
                read
            }
            Err(e) => {
                *self.sql_components.unread.lock() = Some((e.location().to_string(), e.cause()));
                return Err(e);
            }
        };
        // Serialized with every other change to the served app.
        let serialized = self.apply_app_lock.lock().await;
        let served = self.read_app().await;
        let spicepod = served
            .clone()
            .unwrap_or_else(|| Arc::new(AppBuilder::new("spice").build()));
        self.retire_while_stopped(&spicepod, &defined).await;
        if !defined.is_empty() {
            tracing::info!(
                "Loading {} defined with SQL from {}",
                plan::component_counts(&defined),
                store.location()
            );
            self.sql_components.warn_shadowed(&spicepod, &defined);
            let next = Arc::new(self.sql_components.serve(&spicepod, &defined));
            if late {
                Arc::clone(self).apply_app_diff(served.as_ref(), next).await;
            } else {
                *self.app.write().await = Some(next);
            }
        }
        *self.sql_components.spicepod.write().await = Some(spicepod);
        *self.sql_components.defined.write().await = Some(defined);
        self.record_served().await;
        drop(serialized);
        Ok(store)
    }

    /// While this instance is stopped, another that shares the definitions
    /// can drop a dataset or point it at another source, and a Spicepod edit
    /// can start or stop declaring a name SQL defines. Before anything loads,
    /// delete the acceleration data of each dataset this instance recorded it
    /// served under such a name that `defined` and `spicepod` no longer serve
    /// from the same source (`from` and connector parameters), of each it
    /// withheld, and what the store each dataset served now starts to use
    /// holds under its name; a dataset whose data cannot be deleted is
    /// withheld until a retry deletes it.
    async fn retire_while_stopped(self: &Arc<Self>, spicepod: &Arc<App>, defined: &SqlDefinitions) {
        let Some(recorded) = served::read(&self.sql_components.instance_dir).await else {
            return;
        };
        let next = plan::serve((**spicepod).clone(), defined);
        let started = self
            .started_datasets(&recorded.served, &next.datasets, defined, spicepod)
            .await;
        let mut retired: Vec<Dataset> = recorded
            .served
            .into_iter()
            .filter(|before| {
                next.datasets
                    .iter()
                    .find(|now| same_dataset(&now.name, &before.name))
                    .is_none_or(|now| now.from != before.from || now.params != before.params)
            })
            .collect();
        for dataset in recorded.withheld.into_iter().chain(started) {
            if !retired.contains(&dataset) {
                retired.push(dataset);
            }
        }
        let mut deleted = Vec::new();
        let mut withheld = Vec::new();
        let mut cause = None;
        for dataset in retired {
            match Arc::clone(self)
                .delete_acceleration_data(&dataset, spicepod)
                .await
            {
                Ok(true) => {
                    if !deleted
                        .iter()
                        .any(|done: &Dataset| same_dataset(&done.name, &dataset.name))
                    {
                        deleted.push(dataset);
                    }
                }
                Ok(false) => {}
                Err(e) => {
                    cause.get_or_insert_with(|| deletion_failed(&dataset.name, &e));
                    withheld.push(dataset);
                }
            }
        }
        if !deleted.is_empty() {
            tracing::info!("{}", withheld_deleted(&deleted));
        }
        if let Some(cause) = cause {
            let unserved = withheld
                .iter()
                .map(|dataset| dataset.name.clone())
                .collect();
            *self.sql_components.withheld_logged.lock() = Some(cause.clone());
            tracing::warn!("{}", withheld_change(&ServeFailure { cause, unserved }));
        }
        *self.sql_components.withheld.lock() = withheld;
    }

    /// Record, in the instance directory, the definitions defined with SQL,
    /// what this instance serves under each of their names, and what it
    /// withholds, so that once restarted it can tell which acceleration data
    /// a change made while it was stopped left ([`Self::retire_while_stopped`]).
    pub(crate) async fn record_served(&self) {
        let Some(definitions) = self.sql_components.defined().await else {
            return;
        };
        let served = self.read_app().await.map_or_else(Vec::new, |app| {
            app.datasets
                .iter()
                .filter(|dataset| {
                    definitions
                        .datasets
                        .iter()
                        .any(|sql| same_dataset(&sql.name, &dataset.name))
                })
                .cloned()
                .collect()
        });
        let record = served::Record {
            definitions,
            served,
            withheld: self.sql_components.withheld(),
        };
        if let Err(e) = served::write(&self.sql_components.instance_dir, &record).await {
            tracing::warn!(
                "{}",
                unrecorded(
                    &served::path(&self.sql_components.instance_dir),
                    &util::single_line(&e.to_string())
                )
            );
        }
    }

    /// Before a Spicepod reload serves `next` in place of `current`: unload
    /// each dataset whose name SQL defines and whose source the reload
    /// changes, as when the Spicepod starts or stops declaring the name, and
    /// delete its acceleration data, so the definition served next does not
    /// answer from it; and delete what the store a definition served next
    /// starts to use holds under its name. One whose data cannot be deleted is
    /// withheld until a retry deletes it. Returns the app then served, and
    /// `next` without what is withheld. The caller holds `apply_app_lock`.
    pub(crate) async fn retire_switched(
        self: &Arc<Self>,
        current: Arc<App>,
        next: Arc<App>,
    ) -> (Arc<App>, Arc<App>) {
        let Some(defined) = self.sql_components.defined().await else {
            return (current, next);
        };
        let mut switched: Vec<Dataset> = current
            .datasets
            .iter()
            .filter(|before| {
                defined
                    .datasets
                    .iter()
                    .any(|sql| same_dataset(&sql.name, &before.name))
                    && next
                        .datasets
                        .iter()
                        .find(|after| same_dataset(&after.name, &before.name))
                        .is_none_or(|after| {
                            after.from != before.from || after.params != before.params
                        })
            })
            .cloned()
            .collect();
        for started in self
            .started_datasets(&current.datasets, &next.datasets, &defined, &next)
            .await
        {
            if !switched.contains(&started) {
                switched.push(started);
            }
        }
        if switched.is_empty() {
            return (current, next);
        }
        let without = |app: &App, retired: &[Dataset]| {
            let mut app = app.clone();
            app.datasets.retain(|dataset| {
                !retired
                    .iter()
                    .any(|old| same_dataset(&old.name, &dataset.name))
            });
            Arc::new(app)
        };
        let unloaded = without(&current, &switched);
        Arc::clone(self)
            .apply_app_diff(Some(&current), Arc::clone(&unloaded))
            .await;
        let mut withheld = Vec::new();
        let mut cause = None;
        for dataset in switched {
            let deleted = if self
                .df
                .table_exists(&datafusion::common::TableReference::parse_str(
                    &dataset.name,
                )) {
                Err("it could not be unloaded; the Spice log says why".to_string())
            } else {
                Arc::clone(self)
                    .delete_acceleration_data(&dataset, &current)
                    .await
            };
            if let Err(e) = deleted {
                cause.get_or_insert_with(|| deletion_failed(&dataset.name, &e));
                withheld.push(dataset);
            }
        }
        let Some(cause) = cause else {
            return (unloaded, next);
        };
        let unserved = withheld
            .iter()
            .map(|dataset| dataset.name.clone())
            .collect();
        *self.sql_components.withheld_logged.lock() = Some(cause.clone());
        tracing::warn!("{}", withheld_change(&ServeFailure { cause, unserved }));
        let next = without(&next, &withheld);
        self.sql_components.withheld.lock().extend(withheld);
        (unloaded, next)
    }

    /// The error a failed read of the SQL definitions logs.
    async fn unread_definitions_message(&self, e: &store::Error) -> String {
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
            "Failed to read the datasets and catalogs defined with SQL from '{}', so none is served and defining one with SQL is refused; Spice retries every {} seconds. {check} See {RUNTIME_STATE_DOCS} Cause: {}",
            e.location(),
            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs(),
            e.cause()
        )
    }

    /// Every [`SQL_DEFINITIONS_CHECK_INTERVAL`], until the runtime is gone:
    /// read the SQL definitions while they are not read (`store` is `None`),
    /// and then serve the changes other instances that share the store make,
    /// and retry deleting the acceleration data of the datasets withheld until
    /// it is. `reported` is the failure already logged.
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
                        runtime.status.update_component_status(
                            SQL_DEFINITIONS_STATUS,
                            ComponentStatus::Ready,
                        );
                        store = Some(read);
                        reported = None;
                    }
                    Err(e) => {
                        // Reported once until it changes.
                        let cause = e.to_string();
                        if reported.as_deref() != Some(cause.as_str()) {
                            tracing::error!("{}", runtime.unread_definitions_message(&e).await);
                            runtime.status.update_component_status(
                                SQL_DEFINITIONS_STATUS,
                                ComponentStatus::Error(Some(cause.clone())),
                            );
                        }
                        reported = Some(cause);
                    }
                }
                continue;
            };
            // Compared without the lock first: a change is rare, and a statement
            // or a Spicepod reload should not wait on every check.
            let stored = match shared.load().await {
                Ok(stored) => stored,
                Err(e) => {
                    // Reported once until it changes or a read succeeds.
                    let cause = e.to_string();
                    if reported.as_deref() != Some(cause.as_str()) {
                        tracing::warn!(
                            "Failed to read the datasets and catalogs defined with SQL from '{}', so this instance keeps serving the ones it read before; Spice retries every {} seconds. Cause: {}",
                            e.location(),
                            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs(),
                            e.cause()
                        );
                    }
                    reported = Some(cause);
                    continue;
                }
            };
            let Some(known) = runtime.sql_components.defined().await else {
                return;
            };
            // Datasets withheld until their acceleration data is deleted are
            // retried even when the definitions did not change.
            if stored == known && runtime.sql_components.withheld.lock().is_empty() {
                reported = None;
                continue;
            }
            let _serialize = runtime.apply_app_lock.lock().await;
            let Some(known) = runtime.sql_components.defined().await else {
                return;
            };
            // Unchanged definitions: only the datasets withheld are retried,
            // whatever change withheld them.
            let retried_only = stored == known;
            match runtime.serve_stored_definitions(&shared, known).await {
                Ok(_) => reported = None,
                Err(failure) => {
                    // Reported once until it changes or the check succeeds.
                    if reported.as_deref() != Some(failure.cause.as_str()) {
                        if retried_only && !failure.unserved.is_empty() {
                            let logged = runtime.sql_components.withheld_logged.lock().take();
                            if logged.as_deref() != Some(failure.cause.as_str()) {
                                tracing::warn!("{}", withheld_change(&failure));
                            }
                        } else {
                            tracing::warn!("{}", unapplied_change(shared.location(), &failure));
                        }
                    }
                    reported = Some(failure.cause);
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
    ) -> Result<SqlDefinitions, ServeFailure> {
        let stored = store.load().await.map_err(|e| ServeFailure {
            cause: e.to_string(),
            unserved: Vec::new(),
        })?;
        self.serve_definitions(store, known, stored).await
    }

    /// Serve the change another instance made to the definitions a statement
    /// raced with: the statement's error names it, and `SHOW CREATE` shows it
    /// from then on. When it cannot be served, the statement's error says so,
    /// and the check that runs every 10 seconds, which runs once the caller
    /// releases `apply_app_lock`, logs why and serves it.
    async fn serve_change_by_another(
        self: &Arc<Self>,
        store: &DefinitionsStore,
        known: SqlDefinitions,
        stored: SqlDefinitions,
    ) -> Option<ServeFailure> {
        self.serve_definitions(store, known, stored).await.err()
    }

    /// Serve `stored`, the definitions `store` holds, when they differ from
    /// `known`, the ones this instance serves, or while datasets are withheld.
    /// The caller holds `apply_app_lock`. Returns the definitions now served.
    /// A dataset that `stored` drops or points at another source, and whose
    /// acceleration data cannot be deleted, is withheld until a retry deletes
    /// it, and the rest of the change is served: its earlier definition would
    /// answer for a dataset that no longer reads that data, and a later one
    /// could load it.
    async fn serve_definitions(
        self: &Arc<Self>,
        store: &DefinitionsStore,
        known: SqlDefinitions,
        stored: SqlDefinitions,
    ) -> Result<SqlDefinitions, ServeFailure> {
        let withheld = self.sql_components.withheld();
        if stored == known && withheld.is_empty() {
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
            // Unset only before the SQL definitions are read, when the app
            // served holds none of them.
            .unwrap_or_else(|| Arc::clone(&served));
        // What the change retires, and the datasets still withheld: each of
        // their earlier definitions, even of a name the change retires again,
        // since any of them may have left data under it. And the stores the
        // definitions served next start to use, which may hold data an
        // earlier definition left under their names.
        let mut retired = plan::retired_datasets(&spicepod, &known, &stored, None);
        let next = plan::serve((*spicepod).clone(), &stored);
        let started = self
            .started_datasets(&served.datasets, &next.datasets, &stored, &served)
            .await;
        for dataset in withheld.iter().cloned().chain(started) {
            if !retired.contains(&dataset) {
                retired.push(dataset);
            }
        }
        let applied = match self
            .retire(
                &spicepod,
                &served,
                &stored,
                &retired,
                OnUndeleted::KeepUnserved,
            )
            .await
        {
            Ok(applied) => applied,
            Err(failure) => {
                if !failure.unserved.is_empty() {
                    // The rest of the change is served.
                    *self.sql_components.withheld.lock() = retired
                        .into_iter()
                        .filter(|old| {
                            failure
                                .unserved
                                .iter()
                                .any(|name| same_dataset(name, &old.name))
                        })
                        .collect();
                    *self.sql_components.defined.write().await = Some(stored);
                    self.record_served().await;
                }
                return Err(failure);
            }
        };
        self.sql_components.withheld.lock().clear();
        self.sql_components.warn_shadowed(&spicepod, &stored);
        let current = Arc::new(next);
        *self.sql_components.defined.write().await = Some(stored.clone());
        Arc::clone(self)
            .apply_app_diff(Some(&applied), current)
            .await;
        self.record_served().await;
        if stored != known {
            tracing::info!(
                "Applied the change another instance made to the datasets and catalogs defined with SQL in {}: {}.",
                store.location(),
                plan::changed_components(&known, &stored).join(", ")
            );
        }
        if !withheld.is_empty() {
            tracing::info!("{}", withheld_deleted(&withheld));
        }
        Ok(stored)
    }

    /// Stop serving `retired`, the datasets a change to `definitions` retires,
    /// and delete their acceleration data, so no later definition of their
    /// names loads it. Returns the app then served: `served` without them.
    /// When a dataset cannot be unloaded, `served` is served again; when its
    /// acceleration data cannot be deleted, `on_undeleted` says what is
    /// served. The error names the dataset.
    async fn retire(
        self: &Arc<Self>,
        spicepod: &Arc<App>,
        served: &Arc<App>,
        definitions: &SqlDefinitions,
        retired: &[Dataset],
        on_undeleted: OnUndeleted,
    ) -> Result<Arc<App>, ServeFailure> {
        if retired.is_empty() {
            return Ok(Arc::clone(served));
        }
        let mut without = definitions.clone();
        without.datasets.retain(|ds| {
            !retired
                .iter()
                .any(|old| resolved_dataset_name(&old.name) == resolved_dataset_name(&ds.name))
        });
        let without_app = Arc::new(self.sql_components.serve(spicepod, &without));
        Arc::clone(self)
            .apply_app_diff(Some(served), Arc::clone(&without_app))
            .await;
        // A dataset whose unload failed, as when its pending changes could not
        // be drained, is still served from its acceleration data.
        if let Some(dataset) = retired.iter().find(|dataset| {
            self.df
                .table_exists(&datafusion::common::TableReference::parse_str(
                    &dataset.name,
                ))
        }) {
            Arc::clone(self)
                .apply_app_diff(Some(&without_app), Arc::clone(served))
                .await;
            return Err(ServeFailure {
                cause: format!(
                    "dataset {} could not be unloaded, so it is still served, with its acceleration data. Run the statement again; the Spice log says why the unload failed.",
                    display_dataset_name(&dataset.name)
                ),
                unserved: Vec::new(),
            });
        }
        let mut undeleted: Vec<String> = Vec::new();
        let mut cause = None;
        for dataset in retired {
            if let Err(e) = Arc::clone(self)
                .delete_acceleration_data(dataset, served)
                .await
            {
                if matches!(on_undeleted, OnUndeleted::ServeEarlier) {
                    Arc::clone(self)
                        .apply_app_diff(Some(&without_app), Arc::clone(served))
                        .await;
                    let left = if self
                        .sql_components
                        .withheld()
                        .iter()
                        .any(|old| same_dataset(&old.name, &dataset.name))
                    {
                        Undeleted::Withheld
                    } else if served
                        .datasets
                        .iter()
                        .any(|now| same_dataset(&now.name, &dataset.name))
                    {
                        Undeleted::Served
                    } else {
                        Undeleted::New
                    };
                    return Err(ServeFailure {
                        cause: undeleted_by_statement(&dataset.name, left, &e),
                        unserved: Vec::new(),
                    });
                }
                cause.get_or_insert_with(|| deletion_failed(&dataset.name, &e));
                if !undeleted
                    .iter()
                    .any(|name| same_dataset(name, &dataset.name))
                {
                    undeleted.push(dataset.name.clone());
                }
            }
        }
        let Some(cause) = cause else {
            return Ok(without_app);
        };
        // A dataset whose data was deleted is served from its new definition;
        // one whose data stays is not served, whoever defines it, since a
        // Spicepod's definition of its name would load that data too.
        let mut serving = plan::serve((**spicepod).clone(), definitions);
        serving.datasets.retain(|dataset| {
            !undeleted
                .iter()
                .any(|name| same_dataset(name, &dataset.name))
        });
        Arc::clone(self)
            .apply_app_diff(Some(&without_app), Arc::new(serving))
            .await;
        Err(ServeFailure {
            cause,
            unserved: undeleted,
        })
    }

    /// The app the Spicepods declare, without the components defined with SQL
    /// that the runtime serves beside them, or `None` while the runtime serves
    /// no app: reading the definitions with none served records an empty one.
    pub async fn read_spicepod_app(&self) -> Option<Arc<App>> {
        let served = self.read_app().await?;
        Some(
            self.sql_components
                .spicepod
                .read()
                .await
                .clone()
                .unwrap_or(served),
        )
    }

    /// The app to serve for `app`, an app the Spicepods declare: it with the
    /// SQL-defined components added. A component `app` declares is the
    /// Spicepod's, even one identical to a SQL definition.
    pub(crate) async fn with_sql_definitions(&self, app: Arc<App>) -> Arc<App> {
        let Some(defined) = self.sql_components.defined().await else {
            return app;
        };
        *self.sql_components.spicepod.write().await = Some(Arc::clone(&app));
        if defined.is_empty() {
            return app;
        }
        self.sql_components.warn_shadowed(&app, &defined);
        Arc::new(self.sql_components.serve(&app, &defined))
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
        let not_applied = |cause: &dyn std::fmt::Display| {
            DataFusionError::Execution(format!(
                "{} {} was not applied: {cause}",
                op.label(),
                op.component_name()
            ))
        };
        let store = self
            .sql_definitions_store()
            .await
            .map_err(|e| not_applied(&e))?;

        let serialized = self.apply_app_lock.lock().await;
        let mut attempt = 0;
        let (message, validate) = loop {
            attempt += 1;
            let Some(known) = self.sql_components.defined().await else {
                let unread = self.sql_components.unread.lock().clone();
                let (location, cause) =
                    unread.unwrap_or_else(|| (store.location().to_string(), "unknown".to_string()));
                return Err(not_applied(&unreadable(&location, &cause)));
            };
            // Another instance that shares the state location may have changed
            // the definitions since this one read them: the statement is
            // planned against what is stored, served first.
            let stored = store.load().await.map_err(|e| {
                if e.is_unreadable() {
                    not_applied(&unreadable(e.location(), &e.cause()))
                } else {
                    not_applied(&e)
                }
            })?;
            let defined = match self.serve_definitions(&store, known, stored).await {
                Ok(defined) => defined,
                Err(failure) if defines(&op) && withholds(&op, &failure) => {
                    return Err(not_applied(&withheld_dataset(
                        op.component_name(),
                        &failure.cause,
                    )));
                }
                // The rest of the change is served, and this statement names
                // no dataset withheld.
                Err(failure) if !failure.unserved.is_empty() => self
                    .sql_components
                    .defined()
                    .await
                    .ok_or_else(|| not_applied(&failure.cause))?,
                Err(failure) => return Err(not_applied(&failure.cause)),
            };
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
                // Unset only before the SQL definitions are read, when the app
                // served holds none of them.
                .unwrap_or_else(|| Arc::clone(&served));
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
                not_applied(&format!("its definition could not be stored. Cause: {e}"))
            })?;
            if definitions == defined {
                return Ok(message);
            }

            let next = Arc::new(self.sql_components.serve(&spicepod, &definitions));
            // Refused before anything is validated or deleted: clearing the
            // store under this name would delete the other dataset's data.
            if defines(&op)
                && let Some(dataset) = next
                    .datasets
                    .iter()
                    .find(|dataset| same_dataset(&dataset.name, op.component_name()))
                && let Some(shared) = self.component_sharing_table(dataset, &next).await
            {
                return Err(not_applied(&shares_table_with(&shared)));
            }
            if let Some(component) = &validate {
                tokio::time::timeout(
                    VALIDATION_TIMEOUT,
                    Arc::clone(&self).validate_component(component, &next),
                )
                .await
                .map_err(|_| {
                    not_applied(&format!(
                        "validating it did not finish within {} seconds. Check that its source is reachable, then run it again.",
                        VALIDATION_TIMEOUT.as_secs()
                    ))
                })??;
            }

            // A dataset this statement drops, replaces, or points at another
            // source stops serving and loses its acceleration data before the
            // change is stored, so no definition ever loads data another one put
            // there. So does what the store a definition starts to use holds
            // under its name.
            let retired = plan::retired_datasets(&spicepod, &defined, &definitions, Some(&op));
            let started: Vec<Dataset> = self
                .started_datasets(&served.datasets, &next.datasets, &definitions, &served)
                .await
                .into_iter()
                .filter(|started| !retired.contains(started))
                .collect();
            if !retired.is_empty() || !started.is_empty() {
                // Checked again just before data is deleted: a change another
                // instance stored since is found here, before anything is
                // deleted, rather than by the write below, after.
                let stored = store.load().await.map_err(|e| not_applied(&e))?;
                if stored != defined {
                    if plan::changes_component(&op, &defined, &stored) {
                        let failure = self.serve_change_by_another(&store, defined, stored).await;
                        return Err(not_applied(&changed_by_another(&op, failure.as_ref())));
                    }
                    if attempt < CONFLICT_ATTEMPTS {
                        continue;
                    }
                    return Err(not_applied(&store::Error::Conflict {
                        location: store.location().to_string(),
                    }));
                }
            }
            // A started store is another than the one the dataset served under
            // the name keeps its data in, so it is cleared while that dataset
            // serves: a failure leaves it as it was.
            for dataset in &started {
                if let Err(e) = Arc::clone(&self)
                    .delete_acceleration_data(dataset, &served)
                    .await
                {
                    let left = if self
                        .sql_components
                        .withheld()
                        .iter()
                        .any(|old| same_dataset(&old.name, &dataset.name))
                    {
                        Undeleted::Withheld
                    } else if served
                        .datasets
                        .iter()
                        .any(|now| same_dataset(&now.name, &dataset.name))
                    {
                        Undeleted::Started
                    } else {
                        Undeleted::New
                    };
                    return Err(not_applied(&undeleted_by_statement(
                        &dataset.name,
                        left,
                        &e,
                    )));
                }
            }
            let applied = self
                .retire(
                    &spicepod,
                    &served,
                    &definitions,
                    &retired,
                    OnUndeleted::ServeEarlier,
                )
                .await
                .map_err(|failure| not_applied(&failure.cause))?;

            match store.replace(&defined, &definitions).await {
                Ok(()) => {
                    *self.sql_components.defined.write().await = Some(definitions);
                    Arc::clone(&self).apply_app_diff(Some(&applied), next).await;
                    self.record_served().await;
                    break (message, validate);
                }
                Err(e) => {
                    // What this statement retired is served again, from its source.
                    if !retired.is_empty() {
                        Arc::clone(&self)
                            .apply_app_diff(Some(&applied), Arc::clone(&served))
                            .await;
                    }
                    // A change another instance made to other components does
                    // not stop this one: it is planned again against it. One
                    // to this statement's own component does.
                    let mut cause = e.to_string();
                    if matches!(e, store::Error::Conflict { .. })
                        && let Ok(stored) = store.load().await
                    {
                        if plan::changes_component(&op, &defined, &stored) {
                            let failure =
                                self.serve_change_by_another(&store, defined, stored).await;
                            cause = changed_by_another(&op, failure.as_ref());
                        } else if attempt < CONFLICT_ATTEMPTS {
                            continue;
                        }
                    }
                    if retired.is_empty() {
                        return Err(not_applied(&cause));
                    }
                    let names: Vec<String> = retired
                        .iter()
                        .map(|dataset| display_dataset_name(&dataset.name))
                        .collect();
                    return Err(not_applied(&format!(
                        "{}. The acceleration data of {} {} was deleted before the change could be stored, so it is loaded again from the source.",
                        cause.trim_end_matches('.'),
                        if names.len() == 1 {
                            "dataset"
                        } else {
                            "datasets"
                        },
                        names.join(", ")
                    )));
                }
            }
        };
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
            Some(ComponentStatus::Error(_)) => {
                format!("{message} It failed to load and is retrying; the Spice log reports why.")
            }
            Some(ComponentStatus::Initializing) => {
                format!("{message} It is still loading.")
            }
            _ => message,
        }
    }

    /// The store `dataset`'s acceleration keeps its data in beyond its table,
    /// or `None` when there is none: no acceleration, or a memory one that
    /// ends with its table. A store is where the accelerator keeps the data,
    /// for the engine it resolves to, so a change that leaves the data where it
    /// is, such as a tuning parameter, naming the default engine, or another
    /// spelling of the same file, leaves the store.
    async fn acceleration_store(
        self: &Arc<Self>,
        dataset: &Dataset,
        app: &Arc<App>,
    ) -> Option<Store> {
        if !dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
            return None;
        }
        let built = DatasetBuilder::try_from(dataset.clone())
            .ok()?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(self))
            .build()
            .ok()?;
        self.source_store(&built).await
    }

    /// The store `view`'s acceleration keeps its data in, as
    /// [`Self::acceleration_store`] gives a dataset's.
    async fn view_acceleration_store(
        self: &Arc<Self>,
        view: &spicepod::component::view::View,
        app: &Arc<App>,
    ) -> Option<Store> {
        if !view.acceleration.as_ref().is_some_and(|a| a.enabled) {
            return None;
        }
        let spec = crate::component::view::ViewBuilder::try_from(view.clone())
            .ok()?
            .build();
        let built = crate::component::view::View::new(spec, Arc::clone(self), Arc::clone(app));
        self.source_store(&built).await
    }

    /// The store `source`'s acceleration keeps its data in, as
    /// [`Self::acceleration_store`] describes it.
    async fn source_store(&self, source: &dyn AccelerationSource) -> Option<Store> {
        let acceleration = source.acceleration()?;
        let engine = acceleration.engine;
        if !source.is_file_accelerated() {
            return engine
                .shares_memory_instance()
                .then_some(Store::SharedMemory(engine));
        }
        let accelerator = self
            .df
            .accelerator_engine_registry()
            .get_accelerator_engine(engine)
            .await?;
        let location = match accelerator.acceleration_layout(source) {
            AccelerationLayout::None => {
                if let Ok(path) = accelerator.file_path(source) {
                    place(std::path::Path::new(&path))
                        .await
                        .display()
                        .to_string()
                } else {
                    // An engine with no file of its own, PostgreSQL, keeps the
                    // data where its parameters point.
                    let mut params: Vec<_> = acceleration.params.iter().collect();
                    params.sort();
                    format!("{params:?}")
                }
            }
            AccelerationLayout::File { path } => format!(
                "{:?}",
                AccelerationLayout::File {
                    path: place(&path).await
                }
            ),
            AccelerationLayout::Directories { dirs } => {
                let mut placed = Vec::with_capacity(dirs.len());
                for (dir, prefix) in dirs {
                    placed.push((place(&dir).await, prefix));
                }
                format!("{:?}", AccelerationLayout::Directories { dirs: placed })
            }
        };
        Some(Store::Located { engine, location })
    }

    /// The datasets of `after` whose names `defined` defines and whose
    /// acceleration starts a store: one the dataset of the name in `before`,
    /// if any, did not keep its data in, so what it holds under the name an
    /// earlier definition left. Only a definition that changed starts one.
    async fn started_datasets(
        self: &Arc<Self>,
        before: &[Dataset],
        after: &[Dataset],
        defined: &SqlDefinitions,
        app: &Arc<App>,
    ) -> Vec<Dataset> {
        let mut started = Vec::new();
        for now in after {
            if !defined
                .datasets
                .iter()
                .any(|sql| same_dataset(&sql.name, &now.name))
            {
                continue;
            }
            let earlier = before
                .iter()
                .find(|earlier| same_dataset(&earlier.name, &now.name));
            if earlier == Some(now) {
                continue;
            }
            let Some(store) = self.acceleration_store(now, app).await else {
                continue;
            };
            let earlier_store = match earlier {
                Some(earlier) => self.acceleration_store(earlier, app).await,
                None => None,
            };
            if earlier_store.as_ref() != Some(&store) {
                started.push(now.clone());
            }
        }
        started
    }

    /// Another dataset or view of `app` whose acceleration keeps its data in
    /// the table `dataset`'s would. An engine names a component's table after
    /// the name the component prints as, so two names that print alike, such
    /// as `"s1.t1"` and `s1.t1`, share one table in a store of any engine, and
    /// Cayenne finds a table by that name in a metastore other datasets may
    /// share; `DuckDB`, `SQLite`, and Turso also read names that differ only in
    /// case as one table.
    async fn component_sharing_table(
        self: &Arc<Self>,
        dataset: &Dataset,
        app: &Arc<App>,
    ) -> Option<SharedTable> {
        let store = self.acceleration_store(dataset, app).await?;
        let table = |name: &str| datafusion::common::TableReference::parse_str(name).to_string();
        let own = table(&dataset.name);
        let folds_case = store.engine().reads_case_variant_names_as_one_table();
        // Whether `other`, the name another component prints as, names the
        // same table, and whether it prints alike rather than in another case.
        let names_own = |other: &str| {
            if other == own {
                Some(true)
            } else {
                (folds_case && other.eq_ignore_ascii_case(&own)).then_some(false)
            }
        };
        let others = app
            .datasets
            .iter()
            .filter(|other| !same_dataset(&other.name, &dataset.name))
            .map(|other| ("dataset", &other.name, None, Some(other)))
            .chain(
                app.views
                    .iter()
                    .map(|view| ("view", &view.name, Some(view), None)),
            );
        for (kind, name, view, other) in others {
            let Some(alike) = names_own(&table(name)) else {
                continue;
            };
            let other_store = match (view, other) {
                (Some(view), _) => self.view_acceleration_store(view, app).await,
                (None, Some(other)) => self.acceleration_store(other, app).await,
                (None, None) => None,
            };
            let Some(other_store) = other_store else {
                continue;
            };
            let shares = other_store == store
                || (alike
                    && store.engine() == Engine::Cayenne
                    && other_store.engine() == Engine::Cayenne);
            if shares {
                return Some(SharedTable {
                    kind,
                    name: name.clone(),
                    table: alike.then(|| own.clone()),
                    store,
                });
            }
        }
        None
    }

    /// Delete the acceleration data of `dataset`, a definition `app` served and
    /// that no longer serves. A failure is its whole chain on one line: its
    /// innermost error can be a bare code, such as `DuckDB`'s, whose layer
    /// above holds the text that names the file.
    async fn delete_acceleration_data(
        self: Arc<Self>,
        dataset: &Dataset,
        app: &Arc<App>,
    ) -> Result<bool, String> {
        let one_line = |e: &dyn std::fmt::Display| util::single_line(&e.to_string()).into_owned();
        if !dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
            return Ok(false);
        }
        let built = DatasetBuilder::try_from(dataset.clone())
            .map_err(|e| one_line(&e))?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(&self))
            .build()
            .map_err(|e| one_line(&e))?;
        self.df
            .delete_acceleration_data(&built)
            .await
            .map_err(|e| one_line(&e))
    }

    /// Wait, within [`REGISTRATION_WAIT`], for `dataset` to register, and say
    /// how the statement left it.
    async fn await_registration(&self, dataset: &Dataset, message: String) -> String {
        // The name the runtime registers the dataset, and its status, under.
        let reference = datafusion::common::TableReference::parse_str(&dataset.name);
        let failed = || {
            matches!(
                self.status.get_dataset_status(&reference),
                Some(ComponentStatus::Error(_))
            )
        };
        let registered = tokio::time::timeout(REGISTRATION_WAIT, async {
            if matches!(
                self.status.wait_for_dataset_registered(&reference).await,
                crate::status::WaitOutcome::ShuttingDown
            ) {
                return;
            }
            // The status leaves `Initializing` as the load starts to register
            // the dataset, before its table is in the catalog: an accelerated
            // table is opened first, so a query in between would not find it.
            while !self.df.table_exists(&reference) && !failed() {
                tokio::time::sleep(REGISTRATION_POLL).await;
            }
        })
        .await;
        match (registered, self.status.get_dataset_status(&reference)) {
            (Ok(()), Some(ComponentStatus::Error(_))) => format!(
                "{message} It failed to load and is retrying; GET /v1/datasets?status=true reports why."
            ),
            (Ok(()), _) => message,
            (Err(_), _) => format!(
                "{message} It is still loading; GET /v1/datasets?status=true reports its progress."
            ),
        }
    }
}

/// Why a statement is refused while the definitions at `location` cannot be
/// read.
fn unreadable(location: &str, cause: &str) -> String {
    format!(
        "the datasets and catalogs defined with SQL could not be read from '{location}'; Spice retries every {} seconds, so run the statement again once they are read. Cause: {cause}",
        SQL_DEFINITIONS_CHECK_INTERVAL.as_secs()
    )
}

/// A change to the definitions that this instance could not serve, and what
/// it serves meanwhile.
#[derive(Debug)]
struct ServeFailure {
    /// What failed, worded as the cause a message goes on to give.
    cause: String,
    /// The datasets the change drops or points at another source that this
    /// instance stopped serving and could not delete the acceleration data of.
    /// It serves the rest of the change. Empty when it serves its earlier
    /// definitions instead.
    unserved: Vec<String>,
}

/// The place `path` names, so that every spelling of one file or folder
/// gives the same path: relative to the directory Spice runs in, as an
/// engine opens it, with `.` and `..` resolved, no trailing separator, and
/// links resolved for the part of it that exists.
async fn place(path: &std::path::Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let components: Vec<_> = absolute.components().collect();
    // The longest part that exists resolves as the file system resolves it,
    // each link and each `..` in the order they are written, so `link/..` is
    // the folder above where the link points. Only the rest, which does not
    // exist yet, is resolved as written.
    let mut end = components.len();
    let mut resolved = loop {
        if end == 0 {
            break PathBuf::new();
        }
        let existing: PathBuf = components[..end].iter().collect();
        if let Ok(existing) = tokio::fs::canonicalize(&existing).await {
            break existing;
        }
        end -= 1;
    };
    for component in &components[end..] {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            other => resolved.push(other),
        }
    }
    resolved
}

/// Where an acceleration keeps a dataset's data beyond the dataset's table,
/// so that a later definition of the dataset's name finds it there.
#[derive(Debug, PartialEq, Eq)]
enum Store {
    /// A file, folders, or another database of the engine's, where the
    /// accelerator resolves them for the dataset.
    Located { engine: Engine, location: String },
    /// The one in-process instance the engine keeps every memory-mode dataset
    /// in ([`Engine::shares_memory_instance`]).
    SharedMemory(Engine),
}

impl Store {
    /// The engine that keeps the data.
    fn engine(&self) -> Engine {
        match self {
            Store::Located { engine, .. } | Store::SharedMemory(engine) => *engine,
        }
    }
}

/// A dataset or view whose acceleration keeps its data in the table that a
/// statement's dataset would use.
#[derive(Debug)]
struct SharedTable {
    /// `dataset` or `view`.
    kind: &'static str,
    /// Its name, as declared.
    name: String,
    /// The table both would use, when the two names print alike; `None` when
    /// they differ only in case.
    table: Option<String>,
    /// The store the statement's dataset would keep its data in.
    store: Store,
}

/// Why a statement that defines a dataset is refused when `shared` keeps its
/// acceleration data in the table the dataset would use.
fn shares_table_with(shared: &SharedTable) -> String {
    let SharedTable {
        kind,
        name,
        table,
        store,
    } = shared;
    let shown = display_dataset_name(name);
    let engine = store.engine();
    let engine_name = match engine {
        Engine::DuckDB => "DuckDB".to_string(),
        Engine::Sqlite => "SQLite".to_string(),
        Engine::Turso => "Turso".to_string(),
        Engine::Cayenne => "Cayenne".to_string(),
        other => format!("`{other}`"),
    };
    if engine == Engine::Cayenne {
        let table = table.as_deref().unwrap_or_default();
        return format!(
            "{kind} {shown} keeps its acceleration data in the Cayenne table `{table}`, which this dataset would use too. Give this dataset another name."
        );
    }
    let (place, apart) = match store {
        Store::Located { .. } => (
            format!("the same {engine_name} file"),
            format!("set its own `acceleration.params.{engine}_file`"),
        ),
        Store::SharedMemory(_) => (
            format!("the same {engine_name} memory instance"),
            "accelerate it in a file (`acceleration.mode = 'file'`)".to_string(),
        ),
    };
    let default = Engine::default();
    match table {
        Some(table) => format!(
            "{kind} {shown} keeps its acceleration data in {place}, in the table `{table}`, which this dataset would use too. Give this dataset another name, or keep its data apart: {apart}, or use the `{default}` engine."
        ),
        None => format!(
            "{kind} {shown} keeps its acceleration data in {place}, and {engine_name} reads table names that differ only in case as one table, so the two cannot keep their data apart there. Give this dataset a name that differs from {shown} by more than case, or keep its data apart: {apart}, or use the `{default}` engine."
        ),
    }
}

/// What [`Runtime::retire`] serves when it cannot delete a retired dataset's
/// acceleration data.
#[derive(Clone, Copy, Debug)]
enum OnUndeleted {
    /// The app served before: a statement that fails changes nothing.
    ServeEarlier,
    /// The app without the retired datasets: the change is stored already, so
    /// their earlier definitions would answer for a dataset that is dropped
    /// or reads another source.
    KeepUnserved,
}

/// Why a statement was not applied when another instance changed its own
/// dataset or catalog while it ran, and how to see that change. `failure` is
/// why this instance could not serve the change yet, when it could not.
fn changed_by_another(op: &ComponentDdlOp, failure: Option<&ServeFailure>) -> String {
    let name = op.component_name();
    let (noun, object) = match op.kind() {
        ComponentKind::Dataset => ("dataset", "TABLE"),
        ComponentKind::Catalog => ("catalog", "CATALOG"),
    };
    match failure {
        Some(failure) if withholds(op, failure) => format!(
            "another instance changed {noun} '{name}' while this statement ran, and this instance failed to apply that change, so it does not serve {noun} '{name}' until a retry applies it; Spice retries every {} seconds. Then run the statement again if it still applies. Cause: {}",
            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs(),
            failure.cause
        ),
        Some(failure) if failure.unserved.is_empty() => format!(
            "another instance changed {noun} '{name}' while this statement ran, and this instance failed to apply that change, so `SHOW CREATE {object} {name}` shows the earlier definition until a retry applies it; Spice retries every {} seconds. Then run the statement again if it still applies. Cause: {}",
            SQL_DEFINITIONS_CHECK_INTERVAL.as_secs(),
            failure.cause
        ),
        // Served, or served without only other datasets, whose failure is
        // logged.
        _ => format!(
            "another instance changed {noun} '{name}' while this statement ran. Run `SHOW CREATE {object} {name}` to see the change, then run the statement again if it still applies."
        ),
    }
}

/// Whether `op` defines or changes its dataset: what a withheld dataset
/// refuses, since its new definition could load the data the earlier one
/// left. A `DROP` goes on.
fn defines(op: &ComponentDdlOp) -> bool {
    matches!(
        op,
        ComponentDdlOp::CreateDataset { .. } | ComponentDdlOp::AlterDataset { .. }
    )
}

/// Whether `failure` withholds the dataset `op` names.
fn withholds(op: &ComponentDdlOp, failure: &ServeFailure) -> bool {
    matches!(op.kind(), ComponentKind::Dataset)
        && failure
            .unserved
            .iter()
            .any(|dataset| same_dataset(dataset, op.component_name()))
}

/// Why a statement that names `dataset`, which this instance withholds until
/// it deletes the acceleration data an earlier definition left, was not
/// applied.
fn withheld_dataset(dataset: &str, cause: &str) -> String {
    format!(
        "this instance does not serve dataset {} until it deletes the acceleration data stored under its name; Spice retries every {} seconds, so run the statement again once it has. Cause: {cause}",
        display_dataset_name(dataset),
        SQL_DEFINITIONS_CHECK_INTERVAL.as_secs()
    )
}

/// The warning when this instance cannot record, at `path`, what it serves
/// under the names SQL defines, because of `cause`.
fn unrecorded(path: &std::path::Path, cause: &str) -> String {
    format!(
        "Failed to record which datasets and catalogs defined with SQL this instance serves in '{}', so if a dataset changes while this instance is stopped, by another instance or in the Spicepod, this one may serve the acceleration data its earlier definition loaded once restarted. Cause: {cause}",
        path.display()
    )
}

/// That the acceleration data of `dataset` could not be deleted, because of
/// `error`.
fn deletion_failed(dataset: &str, error: impl std::fmt::Display) -> String {
    format!(
        "the acceleration data of dataset {} could not be deleted. Cause: {error}",
        display_dataset_name(dataset)
    )
}

/// What a statement that could not delete a dataset's acceleration data
/// left the dataset as.
#[derive(Debug, Clone, Copy)]
enum Undeleted {
    /// Served before the statement, and still.
    Served,
    /// Served before the statement from its own store, and still: what the
    /// store the statement starts holds under its name stays.
    Started,
    /// Not served: the statement defines the name anew.
    New,
    /// Withheld until a retry deletes its data.
    Withheld,
}

/// Why a statement that retires `dataset`, or starts its store, was not
/// applied when the acceleration data under its name could not be deleted,
/// because of `error`, saying what the dataset is `left` as.
fn undeleted_by_statement(dataset: &str, left: Undeleted, error: impl std::fmt::Display) -> String {
    let shown = display_dataset_name(dataset);
    match left {
        Undeleted::Served => format!(
            "the acceleration data of dataset {shown} could not be deleted, so the dataset is left as it was; run the statement again once the cause is fixed. Cause: {error}"
        ),
        Undeleted::Started => format!(
            "what the acceleration store of dataset {shown} holds under its name could not be deleted, so the dataset is left as it was; run the statement again once the cause is fixed. Cause: {error}"
        ),
        Undeleted::New => format!(
            "what the acceleration store of dataset {shown} holds under its name could not be deleted, so the dataset was not created; run the statement again once the cause is fixed. Cause: {error}"
        ),
        Undeleted::Withheld => withheld_dataset(dataset, &deletion_failed(dataset, error)),
    }
}

/// `names`, each once, as a message shows a dataset's name.
fn shown_datasets<'a>(names: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut shown: Vec<String> = Vec::new();
    for name in names {
        let name = display_dataset_name(name);
        if !shown.contains(&name) {
            shown.push(name);
        }
    }
    shown
}

/// `dataset 'a'`, or `datasets 'a', 'b'`.
fn dataset_phrase(shown: &[String]) -> String {
    match shown {
        [name] => format!("dataset {name}"),
        names => format!("datasets {}", names.join(", ")),
    }
}

/// The log line of a change another instance made to the definitions in
/// `location` that this instance could not serve, saying what it serves.
fn unapplied_change(location: &str, failure: &ServeFailure) -> String {
    let seconds = SQL_DEFINITIONS_CHECK_INTERVAL.as_secs();
    let serving = if failure.unserved.is_empty() {
        format!("keeps serving its earlier definitions and retries every {seconds} seconds")
    } else {
        format!(
            "serves it without {} until a retry, every {seconds} seconds, applies it",
            dataset_phrase(&shown_datasets(failure.unserved.iter().map(String::as_str)))
        )
    };
    format!(
        "Failed to apply a change another instance made to the datasets and catalogs defined with SQL in {location}, so this instance {serving}. Cause: {}",
        failure.cause
    )
}

/// The log line of datasets withheld when a restart or a Spicepod reload
/// would serve them from another source and the acceleration data their
/// earlier definitions loaded cannot be deleted.
fn withheld_change(failure: &ServeFailure) -> String {
    let shown = shown_datasets(failure.unserved.iter().map(String::as_str));
    format!(
        "Failed to apply a change to {}, so this instance does not serve {} until a retry, every {} seconds, applies it. Cause: {}",
        dataset_phrase(&shown),
        if shown.len() == 1 { "it" } else { "them" },
        SQL_DEFINITIONS_CHECK_INTERVAL.as_secs(),
        failure.cause
    )
}

/// The log line of datasets withheld until the acceleration data their
/// earlier definitions left was deleted, once it is: the change another
/// instance made to them, a drop or a new definition, is then applied.
fn withheld_deleted(withheld: &[Dataset]) -> String {
    let shown = shown_datasets(withheld.iter().map(|dataset| dataset.name.as_str()));
    format!(
        "Deleted the earlier acceleration data of {} and applied the change to {}.",
        dataset_phrase(&shown),
        if shown.len() == 1 { "it" } else { "them" }
    )
}

/// The values of `op`'s definition that a log line leaves out of the
/// statement's error: every parameter's, and every value an `ALTER` sets.
fn statement_values(op: &ComponentDdlOp) -> Vec<String> {
    match op {
        ComponentDdlOp::CreateDataset { dataset, .. } => {
            datafusion_ddl::component_options::parameter_values(dataset.as_ref())
        }
        ComponentDdlOp::CreateCatalog { catalog, .. } => {
            datafusion_ddl::component_options::parameter_values(catalog.as_ref())
        }
        ComponentDdlOp::AlterDataset {
            change: OptionsChange::Set(options),
            ..
        }
        | ComponentDdlOp::AlterCatalog {
            change: OptionsChange::Set(options),
            ..
        } => options
            .iter()
            .filter_map(|option| match &option.value {
                OptionValue::String(value) if value.chars().count() >= 4 => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
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

    /// `SHOW CREATE` and the recorded text print a value unredacted only when
    /// it is a reference and nothing else, so the check must agree with the
    /// lexer the secret resolver reads references with: a value it would not
    /// resolve is used as written, and may be the secret.
    #[test]
    fn a_reference_is_what_the_secret_resolver_reads_as_one() {
        for value in [
            "${secrets:pg_pass}",
            "${ secrets:pg_pass }",
            "${ secrets : pg_pass }",
            "${\tenv:PG-PASS\n}",
            "${env:pg_pass_2}",
            "${hunter2}",
            "${secrets:pg:pass}",
            "${secrets:}",
            "${:pg_pass}",
            "${1secrets:pg_pass}",
            "${secrets:pg_pass}}",
            "${secrets:pg_pass} and more",
            "x${secrets:pg_pass}",
            "${secrets:a}${secrets:b}",
            "${secrets.pg_pass}",
            "${secrets:pg pass}",
            "${secrets:_pass}",
            "{secrets:pg_pass}",
            "",
        ] {
            let references: Vec<_> = runtime_secrets::iter_secret_references(value).collect();
            // A reference and nothing else: the value, without the whitespace
            // the lexer skips, is exactly the one reference it reads.
            let resolved_whole = match references.as_slice() {
                [reference] => {
                    let compact: String = value
                        .chars()
                        .filter(|c| !matches!(c, ' ' | '\t' | '\n' | '\x0C'))
                        .collect();
                    compact == format!("${{{}:{}}}", reference.store, reference.key)
                }
                _ => false,
            };
            assert_eq!(
                datafusion_ddl::component_options::is_secret_reference(value),
                resolved_whole,
                "{value:?}"
            );
        }
    }

    /// A statement that loses a race to a change to its own component sends
    /// the user to `SHOW CREATE`, and says what this instance serves when it
    /// could not apply the change: its earlier definitions, or, when the
    /// change retires the statement's dataset and its acceleration data could
    /// not be deleted, no definition of it. A failure that leaves only other
    /// datasets unserved still served this one's change.
    #[test]
    fn a_lost_race_says_what_this_instance_serves() {
        let op = ComponentDdlOp::DropDataset {
            name: "orders".to_string(),
            if_exists: false,
        };
        let not_deleted = |dataset: &str| ServeFailure {
            cause: format!(
                "the acceleration data of dataset '{dataset}' could not be deleted. Cause: Permission denied (os error 13)"
            ),
            unserved: vec![dataset.to_string()],
        };
        let served = "another instance changed dataset 'orders' while this statement ran. Run `SHOW CREATE TABLE orders` to see the change, then run the statement again if it still applies.";
        assert_eq!(changed_by_another(&op, None), served);
        assert_eq!(
            changed_by_another(&op, Some(&not_deleted("customers"))),
            served
        );
        assert_eq!(
            changed_by_another(
                &op,
                Some(&ServeFailure {
                    cause: "dataset 'orders' could not be unloaded".to_string(),
                    unserved: Vec::new(),
                })
            ),
            "another instance changed dataset 'orders' while this statement ran, and this instance failed to apply that change, so `SHOW CREATE TABLE orders` shows the earlier definition until a retry applies it; Spice retries every 10 seconds. Then run the statement again if it still applies. Cause: dataset 'orders' could not be unloaded"
        );
        assert_eq!(
            changed_by_another(&op, Some(&not_deleted("orders"))),
            "another instance changed dataset 'orders' while this statement ran, and this instance failed to apply that change, so it does not serve dataset 'orders' until a retry applies it; Spice retries every 10 seconds. Then run the statement again if it still applies. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
    }

    /// The log line of a change another instance made that this instance
    /// could not serve says what it serves meanwhile, naming each dataset it
    /// does not serve.
    #[test]
    fn an_unapplied_change_says_what_this_instance_serves() {
        let location = "s3://bucket/sql-definitions/definitions.json";
        let failure = |unserved: &[&str]| {
            ServeFailure {
            cause: "the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)".to_string(),
            unserved: unserved.iter().map(ToString::to_string).collect(),
        }
        };
        assert_eq!(
            unapplied_change(location, &failure(&[])),
            "Failed to apply a change another instance made to the datasets and catalogs defined with SQL in s3://bucket/sql-definitions/definitions.json, so this instance keeps serving its earlier definitions and retries every 10 seconds. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            unapplied_change(location, &failure(&["orders"])),
            "Failed to apply a change another instance made to the datasets and catalogs defined with SQL in s3://bucket/sql-definitions/definitions.json, so this instance serves it without dataset 'orders' until a retry, every 10 seconds, applies it. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            unapplied_change(location, &failure(&["orders", "sales.customers", "orders"])),
            "Failed to apply a change another instance made to the datasets and catalogs defined with SQL in s3://bucket/sql-definitions/definitions.json, so this instance serves it without datasets 'orders', 'sales.customers' until a retry, every 10 seconds, applies it. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
    }

    /// The log line of datasets a restart or a Spicepod reload withholds
    /// names each of them, and when they are served.
    #[test]
    fn a_withheld_change_names_its_datasets() {
        let failure = |unserved: &[&str]| {
            ServeFailure {
            cause: "the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)".to_string(),
            unserved: unserved.iter().map(ToString::to_string).collect(),
        }
        };
        assert_eq!(
            withheld_change(&failure(&["orders"])),
            "Failed to apply a change to dataset 'orders', so this instance does not serve it until a retry, every 10 seconds, applies it. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            withheld_change(&failure(&["orders", "sales.customers"])),
            "Failed to apply a change to datasets 'orders', 'sales.customers', so this instance does not serve them until a retry, every 10 seconds, applies it. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
    }

    /// A statement that names a withheld dataset says why it is refused and
    /// when to run it again; the log says when the data is deleted and the
    /// change, a drop or a new definition, applied.
    #[test]
    fn a_withheld_dataset_is_named_in_its_refusal_and_once_deleted() {
        assert_eq!(
            withheld_dataset(
                "orders",
                &deletion_failed("orders", "Permission denied (os error 13)")
            ),
            "this instance does not serve dataset 'orders' until it deletes the acceleration data stored under its name; Spice retries every 10 seconds, so run the statement again once it has. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
        let orders = Dataset::new("file:orders.csv", "orders");
        assert_eq!(
            withheld_deleted(&[orders.clone(), orders.clone()]),
            "Deleted the earlier acceleration data of dataset 'orders' and applied the change to it."
        );
        assert_eq!(
            withheld_deleted(&[orders, Dataset::new("file:c.csv", "sales.customers")]),
            "Deleted the earlier acceleration data of datasets 'orders', 'sales.customers' and applied the change to them."
        );
    }

    /// The warning when what an instance serves cannot be recorded names the
    /// file, both changes it cannot then catch up with, and the cause.
    #[test]
    fn an_unrecorded_record_names_what_a_restart_may_serve() {
        assert_eq!(
            unrecorded(
                std::path::Path::new(".spice/sql-definitions/served.json"),
                "File exists (os error 17)"
            ),
            "Failed to record which datasets and catalogs defined with SQL this instance serves in '.spice/sql-definitions/served.json', so if a dataset changes while this instance is stopped, by another instance or in the Spicepod, this one may serve the acceleration data its earlier definition loaded once restarted. Cause: File exists (os error 17)"
        );
    }

    /// A statement whose dataset's acceleration data cannot be deleted says
    /// that the dataset is left as it was, that it was not created, or,
    /// withheld, that it is still not served, and when to run the statement
    /// again.
    #[test]
    fn a_statement_that_cannot_delete_the_data_says_what_is_served() {
        assert_eq!(
            undeleted_by_statement(
                "orders",
                Undeleted::Served,
                "Permission denied (os error 13)"
            ),
            "the acceleration data of dataset 'orders' could not be deleted, so the dataset is left as it was; run the statement again once the cause is fixed. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            undeleted_by_statement(
                "orders",
                Undeleted::Started,
                "Permission denied (os error 13)"
            ),
            "what the acceleration store of dataset 'orders' holds under its name could not be deleted, so the dataset is left as it was; run the statement again once the cause is fixed. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            undeleted_by_statement("orders", Undeleted::New, "Permission denied (os error 13)"),
            "what the acceleration store of dataset 'orders' holds under its name could not be deleted, so the dataset was not created; run the statement again once the cause is fixed. Cause: Permission denied (os error 13)"
        );
        assert_eq!(
            undeleted_by_statement(
                "orders",
                Undeleted::Withheld,
                "Permission denied (os error 13)"
            ),
            "this instance does not serve dataset 'orders' until it deletes the acceleration data stored under its name; Spice retries every 10 seconds, so run the statement again once it has. Cause: the acceleration data of dataset 'orders' could not be deleted. Cause: Permission denied (os error 13)"
        );
    }

    /// A statement that would put a dataset's acceleration in the table of
    /// another dataset or view, one whose name differs only in case or prints
    /// alike, names it, the engine, and how to keep the two apart: another
    /// name, a file of its own, or the default engine, which is `cayenne`
    /// where it is built.
    #[cfg(not(windows))]
    #[test]
    fn a_dataset_that_would_share_a_table_names_the_other() {
        let shared = |kind, name: &str, table: Option<&str>, store| SharedTable {
            kind,
            name: name.to_string(),
            table: table.map(str::to_string),
            store,
        };
        assert_eq!(
            shares_table_with(&shared(
                "dataset",
                "\"Orders\"",
                None,
                Store::Located {
                    engine: Engine::Sqlite,
                    location: "/data/accelerated_sqlite.db".to_string(),
                }
            )),
            "dataset 'Orders' keeps its acceleration data in the same SQLite file, and SQLite reads table names that differ only in case as one table, so the two cannot keep their data apart there. Give this dataset a name that differs from 'Orders' by more than case, or keep its data apart: set its own `acceleration.params.sqlite_file`, or use the `cayenne` engine."
        );
        assert_eq!(
            shares_table_with(&shared(
                "view",
                "\"Orders\"",
                None,
                Store::SharedMemory(Engine::DuckDB)
            )),
            "view 'Orders' keeps its acceleration data in the same DuckDB memory instance, and DuckDB reads table names that differ only in case as one table, so the two cannot keep their data apart there. Give this dataset a name that differs from 'Orders' by more than case, or keep its data apart: accelerate it in a file (`acceleration.mode = 'file'`), or use the `cayenne` engine."
        );
        // Two names that print alike: `s1.t1`, a table in schema `s1`, and
        // `"s1.t1"`, one name, which prints quoted.
        assert_eq!(
            shares_table_with(&shared(
                "dataset",
                "s1.t1",
                Some("s1.t1"),
                Store::Located {
                    engine: Engine::DuckDB,
                    location: "/data/accelerated_duckdb.db".to_string(),
                }
            )),
            "dataset 's1.t1' keeps its acceleration data in the same DuckDB file, in the table `s1.t1`, which this dataset would use too. Give this dataset another name, or keep its data apart: set its own `acceleration.params.duckdb_file`, or use the `cayenne` engine."
        );
        assert_eq!(
            shares_table_with(&shared(
                "dataset",
                "\"s1.t1\"",
                Some("s1.t1"),
                Store::Located {
                    engine: Engine::Cayenne,
                    location: "/data/s1_t1".to_string(),
                }
            )),
            "dataset '\"s1.t1\"' keeps its acceleration data in the Cayenne table `s1.t1`, which this dataset would use too. Give this dataset another name."
        );
    }

    /// Every spelling of one file or folder names one place: relative to the
    /// directory Spice runs in, with `.` and `..`, a trailing separator, or
    /// through a link, also one followed by `..`, whether it exists yet or not.
    #[cfg(unix)]
    #[tokio::test]
    async fn every_spelling_of_a_file_names_one_place() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = dir.path().join("data");
        tokio::fs::create_dir(&data).await.expect("data dir");
        tokio::fs::write(data.join("a.db"), b"")
            .await
            .expect("file");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&data, &link).expect("symlink");
        // `uplink/..` is `data`, the folder above where the link points.
        tokio::fs::create_dir(data.join("sub"))
            .await
            .expect("sub dir");
        let uplink = dir.path().join("uplink");
        std::os::unix::fs::symlink(data.join("sub"), &uplink).expect("symlink");
        let cwd = std::env::current_dir().expect("cwd");
        let relative = pathdiff(&data.join("a.db"), &cwd);
        let file = place(&data.join("a.db")).await;
        for spelling in [
            data.join("./a.db"),
            data.join("../data/a.db"),
            link.join("a.db"),
            uplink.join("../a.db"),
            relative,
        ] {
            assert_eq!(place(&spelling).await, file, "{}", spelling.display());
        }
        let folder = place(&data).await;
        assert_eq!(place(&dir.path().join("data/")).await, folder);
        assert_eq!(place(&link).await, folder);
        // Not created yet: the part that exists resolves, the rest is kept.
        let missing = place(&data.join("new/b.db")).await;
        assert_eq!(missing, folder.join("new").join("b.db"));
        assert_eq!(place(&link.join("./new/../new/b.db")).await, missing);
        assert_ne!(place(&data.join("b.db")).await, file);
    }

    /// `path` relative to `base`, both absolute.
    #[cfg(unix)]
    fn pathdiff(path: &std::path::Path, base: &std::path::Path) -> PathBuf {
        let path: Vec<_> = path.components().collect();
        let base: Vec<_> = base.components().collect();
        let common = path.iter().zip(&base).take_while(|(a, b)| a == b).count();
        let mut relative: PathBuf = base[common..].iter().map(|_| "..").collect();
        relative.extend(&path[common..]);
        relative
    }
}
