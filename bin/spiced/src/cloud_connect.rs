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

//! Bootstrap glue for Spice Cloud Connect inside `spiced`.
//!
//! Wires the spice-cloud-connect client into the runtime so that a
//! standalone `spiced` can be discovered, adopted, and managed by a
//! Spice Cloud control plane.
//!
//! ## Opt-in semantics
//!
//! `CloudConnect` is **disabled by default**. It activates only if one of
//! the following is true at boot:
//!
//! 1. The `--cloud-connect` flag was passed.
//! 2. `$SPICE_CONFIG_DIR/identity.json` exists.
//! 3. `SPICE_CONNECT_ADOPT_CODE` env var is set.
//! 4. `$SPICE_CONFIG_DIR/pending-adopt-code` file exists.
//!
//! If none of the above is true, this module never opens a connection.
//! `--cloud-connect` forces the client on but cannot conjure a credential:
//! with no identity and no code it logs an actionable warning and the
//! runtime continues unmanaged. Conversely its absence keeps the
//! signal-based activation, so instances enrolled before the flag existed
//! keep connecting after an upgrade.
//!
//! ## How a deployment applies
//!
//! `apply_spicepod` validates the incoming spicepod and persists it as
//! `spicepod-cloud-managed.yml`, and how it becomes live depends on what it
//! changes. That is decided — see [`classify_deployment`] — before the spicepod
//! is written, its secrets are installed, or any component is reconciled, so an
//! apply cannot start down one path and discover it needed the other.
//!
//! A change confined to the sections `Runtime::apply_app` reconciles
//! ([`HOT_SECTIONS`]) is applied to this process: the instance keeps serving and
//! the result says the deployment is live. Anything else is made live by
//! exiting 0 — the supervisor relaunches `spiced`, [`cloud_managed_spicepod`]
//! hands the persisted file to the app builder, and the instance comes up
//! serving it.
//!
//! Two consequences of the restart path the caller has to hold:
//!
//! - **The command result may never arrive.** The process exits mid-command, so
//!   the stream drops before the result is guaranteed to land — a caller cannot
//!   treat its absence as a failed deployment.
//! - **Downtime is proportional to the app's size, not the change's.** A
//!   one-line edit reloads every dataset and rebuilds every acceleration.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use app::{App, AppBuilder};
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use futures::StreamExt;
use parking_lot::RwLock;
use runtime::Runtime;
use runtime::datafusion::query::Error as QueryError;
use runtime::metrics_reader::MetricsReader;
use runtime::status::ComponentStatus;
use runtime_cloud_connect::config::{
    CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig, IDENTITY_FILE, PENDING_ADOPT_CODE_FILE,
};
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, MAX_QUERY_RESULT_BYTES, QueryOutcome, RuntimeHandle,
    RuntimePhase, SpicepodDeployment, StatusReport, effective_max_rows,
};
use runtime_cloud_connect::supervisor::Supervisor;
use runtime_cloud_connect::{
    CloudConnect,
    identity::{AppAttachment, AttachmentState, IdentityStore},
};
// Reached through the `runtime` re-export rather than a direct dependency, the
// same way `runtime::status` is.
use runtime::secrets::stores::cloud_delivered::{CLOUD_DELIVERED_STORE, CloudDeliveredSecretStore};
use runtime_cloud_connect::identity::CacheKey;

use crate::log_capture::LogRingBuffer;

/// Default number of log lines returned by `GetLogs` when the command leaves
/// `tail_lines` unset. Bounded by the ring buffer's capacity.
const DEFAULT_LOG_TAIL_LINES: usize = 500;

/// How long a deployment's shutdown may drain before the process exits anyway.
///
/// A deployment restart is a planned shutdown, so it takes the same drain path
/// a `SIGTERM` would — an accelerator with local state has to flush it. The
/// bound is what keeps one stuck connection from turning a deployment into an
/// instance that never comes back: `Runtime::shutdown` has its own
/// (`runtime.shutdown_timeout`, 30s by default) budget, and this is the outer
/// one that holds even if that budget is raised or a phase hangs.
const DEPLOYMENT_DRAIN_BUDGET: Duration = Duration::from_mins(2);

/// Cheap probe for whether Spice Cloud Connect is configured for this
/// instance, using the same signals as [`maybe_start`]: the explicit
/// `--cloud-connect` flag, an on-disk identity, a staged pending adoption
/// code, or the `SPICE_CONNECT_ADOPT_CODE` env var.
///
/// Called from `init_tracing` (before [`maybe_start`]) to decide whether to
/// install the log-capture layer. It runs in the same process — hence the
/// same working directory — as [`maybe_start`], so both resolve the config
/// directory identically. This is a lightweight existence check; it does not
/// read or validate the files (that happens in `maybe_start`).
pub(crate) fn is_configured(cloud_connect_flag: bool) -> bool {
    if cloud_connect_flag {
        return true;
    }
    let config_dir = CloudConnectConfig::default_config_dir();
    config_dir.join(IDENTITY_FILE).exists()
        || config_dir.join(PENDING_ADOPT_CODE_FILE).exists()
        || std::env::var_os(runtime_cloud_connect::config::ADOPT_CODE_ENV)
            .is_some_and(|v| !v.is_empty())
}

/// Read the optional `cloud-endpoint` override file written by
/// `spice connect <code> --endpoint <url>`. This overrides the cloud
/// **enroll** endpoint (state plane); the gateway (stream) address comes
/// from the enroll response.
fn read_endpoint_override(config_dir: &Path) -> Option<String> {
    let path = config_dir.join("cloud-endpoint");
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Build a [`CloudConnectConfig`] from env + on-disk state.
fn build_config(runtime_version: &str) -> CloudConnectConfig {
    let mut config = CloudConnectConfig::from_env(runtime_version);
    if std::env::var_os("SPICE_CLOUD_ENDPOINT").is_none()
        && let Some(override_endpoint) = read_endpoint_override(&config.config_dir)
    {
        config.enroll_endpoint = override_endpoint;
    }
    config
}

/// The spicepod a deployment persisted, and the deployment it belongs to.
///
/// Every deployment persists this file, so on every start it — not the instance
/// directory's `spicepod.yaml` — is the configuration a cloud-managed instance
/// serves.
#[derive(Debug)]
pub struct CloudManagedSpicepod {
    pub path: PathBuf,
    /// The persisted YAML, kept so an `ApplySpicepod` can tell a redelivery of
    /// the running deployment from a new one without re-reading the file.
    pub spicepod_yaml: String,
}

#[derive(Debug)]
pub struct CloudManagedSpicepodReadError {
    pub path: PathBuf,
    pub source: std::io::Error,
}

/// The cloud-managed spicepod this instance starts on, or `None` when Cloud
/// Connect is not configured or no deployment has ever landed here.
///
/// Reads files only — no control-plane round trip — so an instance whose
/// gateway is unreachable still comes up on its deployed configuration.
pub async fn cloud_managed_spicepod(
    cloud_connect_flag: bool,
) -> std::result::Result<Option<CloudManagedSpicepod>, CloudManagedSpicepodReadError> {
    if !is_configured(cloud_connect_flag) {
        return Ok(None);
    }
    let config_dir = CloudConnectConfig::default_config_dir();
    let path = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    read_cloud_managed_spicepod(path).await
}

async fn read_cloud_managed_spicepod(
    path: PathBuf,
) -> std::result::Result<Option<CloudManagedSpicepod>, CloudManagedSpicepodReadError> {
    match tokio::fs::read_to_string(&path).await {
        Ok(spicepod_yaml) => Ok(Some(CloudManagedSpicepod {
            path,
            spicepod_yaml,
        })),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(CloudManagedSpicepodReadError { path, source }),
    }
}

/// The delivered-secrets store and its cache key, restored from local state.
///
/// Built and installed **before** the runtime loads its components, because
/// component initialization is what resolves `${ secrets:… }`: a store
/// registered afterwards would arrive after every referencing component had
/// already failed. The Cloud Connect *client* still starts after loading (see
/// [`maybe_start`]) — only the local restore has to be early.
pub struct DeliveredSecretsState {
    store: Arc<CloudDeliveredSecretStore>,
}

/// Register the delivered-secrets store on `runtime` and restore the last
/// delivered set from the local cache.
///
/// Call this before `load_components()`. Returns `None` when Cloud Connect is
/// not configured for this instance, in which case nothing is registered — a
/// vanilla OSS install gains no store and reads no files.
///
/// Deliberately local-only: the key lives in `identity.json` and the payload in
/// the config dir, so a restart restores its secrets with the gateway
/// unreachable. That is the property the local cache key buys.
pub async fn restore_delivered_secrets(
    runtime_version: &str,
    runtime: &Arc<Runtime>,
    cloud_connect_flag: bool,
) -> Option<DeliveredSecretsState> {
    if !is_configured(cloud_connect_flag) {
        return None;
    }
    let config = build_config(runtime_version);

    // Registered as a built-in so `${ secrets:NAME }` reaches it with nothing
    // declared in the spicepod, it sits below every user-declared store, and a
    // spicepod reload cannot clear it.
    let store = Arc::new(CloudDeliveredSecretStore::new());
    runtime.secrets().write().await.register_builtin_store(
        CLOUD_DELIVERED_STORE,
        Arc::clone(&store) as Arc<dyn runtime::secrets::SecretStore>,
    );

    load_cached_secrets(&config, &store);
    Some(DeliveredSecretsState { store })
}

/// Start the Cloud Connect client if any of the opt-in conditions are
/// met. The returned `Option<CloudConnect>` is `None` when `CloudConnect`
/// is disabled — which is the default for vanilla OSS installs.
///
/// `cloud_connect_flag` is the explicit `--cloud-connect` opt-in: it forces
/// the client on, but with no identity and no adoption code there is
/// nothing to connect with — that case logs an actionable warning (instead
/// of the silent debug skip) and the runtime continues unmanaged.
///
/// `running_deployment` is the cloud-managed spicepod the runtime actually
/// loaded, or `None` when it is serving something else (a local spicepod, or a
/// deployed one that failed to build). It is what a redelivered `ApplySpicepod`
/// is compared against, so passing a configuration that is not live would let a
/// redelivery be answered as already applied when it is not.
///
/// `runtime_overrides` are the process's `--set-runtime` values, which a
/// deployment applied in place has to carry the same way a restart onto it
/// would.
pub async fn maybe_start(
    runtime_version: &str,
    runtime: Arc<Runtime>,
    cloud_connect_flag: bool,
    delivered_secrets: Option<DeliveredSecretsState>,
    running_deployment: Option<CloudManagedSpicepod>,
    metrics: Option<MetricsReader>,
    runtime_overrides: Vec<(String, String)>,
) -> Option<CloudConnect> {
    let config = build_config(runtime_version);

    // Quick sanity probe — if no identity AND no adoption code, skip.
    // Surface a load/parse error (corrupt or unreadable identity.json)
    // rather than silently treating it as "not adopted", so a broken
    // identity file is visible to the operator instead of quietly
    // disabling Cloud Connect.
    let mut persisted_app_id = None;
    let has_identity = match IdentityStore::load_optional(&config.identity_path) {
        Ok(opt) => {
            // Restores the metrics attribution across a restart. Without it the
            // instance exports nothing until its next deploy, which may be days.
            persisted_app_id = opt.as_ref().and_then(|i| i.app_id.clone());
            opt.is_some()
        }
        Err(err) => {
            tracing::warn!(
                "Spice Cloud Connect: could not read identity at {}: {err}; \
                 treating as not-adopted — fix or remove the file to re-adopt",
                config.identity_path.display()
            );
            false
        }
    };
    if !has_identity && config.adoption_code.is_none() {
        if cloud_connect_flag {
            tracing::warn!(
                "Spice Cloud Connect: --cloud-connect was passed but no identity exists at {} and no adoption code is available; \
                 the runtime is NOT connected to Spice Cloud. Run `spice connect <code>` with a code from the Spice Cloud portal, \
                 or set {} and restart. See: https://spiceai.org/docs",
                config.identity_path.display(),
                runtime_cloud_connect::config::ADOPT_CODE_ENV
            );
        } else {
            tracing::debug!(
                "Spice Cloud Connect: disabled (no identity at {} and no adoption code)",
                config.identity_path.display()
            );
        }
        return None;
    }

    tracing::info!(
        "Spice Cloud Connect: enabled, enroll_endpoint={} mode={}",
        config.enroll_endpoint,
        if has_identity { "identity" } else { "adopt" }
    );

    // Hand the runtime handle the log-capture ring buffer (installed by
    // `init_tracing` when Cloud Connect is configured) so it can answer
    // `GetLogs`. `None` if capture wasn't installed — the handler then
    // reports logs as unavailable rather than returning an empty blob.
    let logs = crate::log_capture::handle();

    if let Some(app_id) = &persisted_app_id {
        tracing::debug!(
            app_id,
            "Spice Cloud Connect: metrics attribution restored from the stored identity"
        );
    } else {
        tracing::debug!(
            "Spice Cloud Connect: the stored identity names no app; metrics are withheld until a deploy names one"
        );
    }

    // Installed before `load_components()` by `restore_delivered_secrets`, so
    // the cached secrets were already in place when components resolved their
    // references. `None` only if that call was skipped, in which case a
    // deployment can still deliver secrets to this process — they just will not
    // have been available at startup.
    let delivered_store = if let Some(state) = delivered_secrets {
        state.store
    } else {
        tracing::debug!(
            "Spice Cloud Connect: no delivered-secrets store was installed before component load; registering one now"
        );
        let store = Arc::new(CloudDeliveredSecretStore::new());
        runtime.secrets().write().await.register_builtin_store(
            CLOUD_DELIVERED_STORE,
            Arc::clone(&store) as Arc<dyn runtime::secrets::SecretStore>,
        );
        load_cached_secrets(&config, &store);
        store
    };

    // Detected once: nothing about the process's supervisor changes while it
    // runs, and every deployment depends on the answer.
    let supervisor = Supervisor::detect();
    if let Some(caveat) = supervisor.caveat() {
        tracing::warn!("Spice Cloud Connect: {caveat}");
    } else {
        tracing::info!(
            "Spice Cloud Connect: process supervisor detected ({}); a deployment that cannot be applied in place relaunches spiced",
            supervisor.as_str()
        );
    }

    // The cache key is read from the identity on each write, not captured here:
    // an instance enrolling in this very process has no identity yet.
    let identity_path = config.identity_path.clone();
    let handle: Arc<dyn RuntimeHandle> =
        Arc::new(SpicedRuntimeHandle::new(SpicedRuntimeHandleParts {
            runtime,
            logs,
            delivered_secrets: delivered_store,
            identity_path,
            running_deployment,
            supervisor,
            metrics,
            app_id: persisted_app_id,
            runtime_overrides,
        }));

    match CloudConnect::start(config, handle).await {
        Ok(Some(client)) => Some(client),
        Ok(None) => None,
        Err(err) => {
            tracing::warn!(
                "Spice Cloud Connect: failed to start (continuing without cloud management): {err}"
            );
            None
        }
    }
}

/// Load the delivered-secrets cache into `store`.
///
/// The key lives in `identity.json`, so both halves come from local state and
/// this never contacts the control plane — which is the property that lets a
/// restart succeed while the gateway is down.
///
/// Every failure is a degradation, never fatal: an unreadable, corrupt,
/// wrong-key, or unknown-version cache is discarded with an actionable warning
/// and the instance comes up with no delivered secrets, which one deployment
/// restores. Crashing here would make a corrupt file unbootable.
fn load_cached_secrets(config: &CloudConnectConfig, store: &CloudDeliveredSecretStore) {
    let Some(key) = IdentityStore::load_optional(&config.identity_path)
        .ok()
        .flatten()
        .and_then(|identity| identity.cache_key())
    else {
        // No identity yet (a first boot that will enroll below), or one that
        // predates the cache key. Either way there is nothing to restore.
        return;
    };

    let path = config
        .config_dir
        .join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
    match runtime_cloud_connect::secret_cache::read(&path, &key) {
        Ok(Some(cached)) => {
            // Names only.
            tracing::info!(
                "Spice Cloud Connect: restored {} delivered secret(s) from the local cache: {}",
                cached.names().len(),
                cached.names().join(", ")
            );
            store.replace(cached.into_values());
        }
        Ok(None) => {
            tracing::debug!(
                "Spice Cloud Connect: no delivered-secrets cache at {}; components referencing \
                 delivered secrets wait for the first deployment",
                path.display()
            );
        }
        Err(err) => {
            tracing::warn!("Spice Cloud Connect: {err}");
        }
    }
}

/// What an incoming `ApplySpicepod` turns out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Disposition {
    /// The runtime is already serving exactly this deployment. Restarting for
    /// it would turn a redelivery into a restart loop.
    AlreadyLive,
    /// This process already persisted exactly this deployment and is on its way
    /// out. Re-issuing the exit is safe and covers an exit that did not happen.
    AlreadyPersisted,
    /// Persist it, then restart onto it.
    Apply,
}

/// Classify an incoming spicepod against what this process is serving (`live`)
/// and what it has already persisted but not yet restarted onto (`persisted`).
///
/// The spicepod itself is the identity of a deployment here: with nothing else to
/// compare, byte-identical YAML is the same configuration and anything else is a
/// new one — unless it delivers secret values this instance does not hold, which
/// is a change the YAML does not show and which nothing else would apply.
fn disposition(
    live: &str,
    persisted: Option<&str>,
    incoming: &str,
    secrets_changed: bool,
) -> Disposition {
    if secrets_changed {
        return Disposition::Apply;
    }
    if persisted.is_some_and(|persisted| persisted == incoming) {
        return Disposition::AlreadyPersisted;
    }
    // Only meaningful while nothing has been persisted: once it has, the live
    // configuration is the one being replaced.
    if persisted.is_none() && live == incoming {
        return Disposition::AlreadyLive;
    }
    Disposition::Apply
}

/// The spicepod sections [`Runtime::apply_app`] reconciles in a running
/// process.
///
/// Every other section — `runtime`, `secrets`, `extensions`, `embeddings`,
/// `tools`, and the rest — is read while the process starts, so a change to one
/// takes effect by starting again. `workers` reconciles only in a build without
/// the `models` feature, so it is left out: the cost of omitting a section is a
/// restart that was not needed, while listing one the runtime does not
/// reconcile in every build drops the change silently.
const HOT_SECTIONS: [&str; 5] = ["catalogs", "datasets", "functions", "models", "views"];

/// How an incoming spicepod takes effect.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ApplyMode {
    /// Reconcile it into this process.
    Hot,
    /// Persist it and restart onto it.
    Cold(ColdReason),
}

/// What makes a deployment reach the instance by restarting it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ColdReason {
    /// Sections the deployment changes that are read at start. Never empty.
    Sections(Vec<String>),
    /// The deployment carries secret values this instance does not hold.
    /// A component resolves `${ secrets:… }` as it loads, so a new value
    /// reaches the components already loaded by loading them again.
    DeliveredSecrets,
    /// This process serves no deployment, so there is nothing to diff an
    /// incoming one against.
    NoLiveDeployment,
    /// This process has already persisted another deployment and is on its way
    /// out for it. It cannot be the process that makes this one live: applying
    /// it in place would reconcile components into a runtime that is draining,
    /// and report as live a configuration this process will not serve. The
    /// newest deployment is still the one persisted, so the restart comes up on
    /// it rather than on the one it superseded.
    RestartPending,
    /// The initial component load has not finished. It is still registering the
    /// components of the app being replaced, and reconciling only what the diff
    /// shows as changed would leave the two writing the same tables — the load
    /// installing what this deployment replaced, after it replaced it.
    InitialLoadUnfinished,
}

impl ColdReason {
    /// The spicepod sections that forced the restart, for the command result.
    ///
    /// Empty when what forced it is the state of this process rather than
    /// something the deployment changed.
    fn sections(&self) -> Vec<String> {
        match self {
            Self::Sections(sections) => sections.clone(),
            Self::DeliveredSecrets => vec!["secrets".to_string()],
            Self::NoLiveDeployment | Self::RestartPending | Self::InitialLoadUnfinished => {
                Vec::new()
            }
        }
    }

    /// The clause naming what forced the restart, phrased to follow "because".
    fn summary(&self) -> String {
        match self {
            Self::Sections(sections) => format!(
                "it changes configuration this instance reads when it starts: {}",
                sections.join(", ")
            ),
            Self::DeliveredSecrets => {
                "it delivers secret values this instance does not hold, and a component resolves its secrets as it loads".to_string()
            }
            Self::NoLiveDeployment => {
                "this instance is not serving a deployment to apply the change onto".to_string()
            }
            Self::RestartPending => {
                "this instance is already restarting onto a deployment it persisted".to_string()
            }
            Self::InitialLoadUnfinished => {
                "this instance has not finished loading its components".to_string()
            }
        }
    }

    fn message(&self) -> String {
        format!(
            "The spicepod was validated and persisted. This instance is restarting to serve it because {}. The restart reloads every dataset, so the app is unavailable until it finishes.",
            self.summary()
        )
    }
}

/// What this process knows about an incoming deployment when it decides how to
/// apply it.
struct ApplyFacts<'a> {
    /// The spicepod this process is serving, empty when it serves none.
    live: &'a str,
    /// The spicepod being deployed.
    incoming: &'a str,
    /// Whether the deployment carries secret values other than the ones
    /// installed.
    secrets_changed: bool,
    /// Whether this process has already persisted a deployment and is
    /// restarting onto it.
    restart_pending: bool,
    /// Whether the initial component load is still running.
    initial_load_unfinished: bool,
}

/// Decide whether a deployment can be applied to this process.
///
/// Called before the spicepod is persisted, before its secrets are installed,
/// and before any component is reconciled, so the decision holds for the whole
/// apply: a hot apply can never discover halfway through that it needed the
/// restart.
/// Any one of these answers is enough on its own, so what the deployment
/// carries is reported ahead of what the process happens to be doing: it is the
/// half the caller can change.
fn classify_deployment(facts: &ApplyFacts) -> ApplyMode {
    if facts.live.is_empty() {
        return ApplyMode::Cold(ColdReason::NoLiveDeployment);
    }
    let mut changed = start_time_changes(facts.live, facts.incoming);
    if !changed.is_empty() {
        if facts.secrets_changed {
            // Delivered values are not in the document, but they are the app's
            // secrets: an operator who reverted only the sections named here
            // would be restarted again by the rotation.
            changed.push("secrets".to_string());
            changed.sort();
            changed.dedup();
        }
        return ApplyMode::Cold(ColdReason::Sections(changed));
    }
    if facts.secrets_changed {
        return ApplyMode::Cold(ColdReason::DeliveredSecrets);
    }
    if facts.restart_pending {
        return ApplyMode::Cold(ColdReason::RestartPending);
    }
    if facts.initial_load_unfinished {
        return ApplyMode::Cold(ColdReason::InitialLoadUnfinished);
    }
    ApplyMode::Hot
}

/// The sections in which `incoming` differs from `live`, excluding the ones a
/// running process reconciles.
///
/// A document that is not a mapping — malformed YAML, or a scalar — reads as
/// having no sections at all, so the `name`, `version` and `kind` every valid
/// spicepod carries count as changed and the deployment takes the restart path.
/// Validation rejects it before any of that is applied.
fn start_time_changes(live: &str, incoming: &str) -> Vec<String> {
    let live = sections(live);
    let incoming = sections(incoming);
    let mut changed: Vec<String> = live
        .keys()
        .chain(incoming.keys())
        .filter(|section| !HOT_SECTIONS.contains(&section.as_str()))
        .filter(|section| live.get(*section) != incoming.get(*section))
        .cloned()
        .collect();
    changed.sort();
    changed.dedup();
    changed
}

/// The top-level sections of a spicepod document.
///
/// A section that is null or empty is dropped, so a `secrets: []` and no
/// `secrets:` at all — which build the same app — do not read as a change.
fn sections(spicepod_yaml: &str) -> BTreeMap<String, yaml::Value> {
    let Ok(yaml::Value::Mapping(document)) = yaml::from_str::<yaml::Value>(spicepod_yaml) else {
        return BTreeMap::new();
    };
    document
        .into_iter()
        .filter_map(|(name, section)| match name {
            yaml::Value::String(name) if !is_empty_section(&section) => Some((name, section)),
            _ => None,
        })
        .collect()
}

fn is_empty_section(section: &yaml::Value) -> bool {
    match section {
        yaml::Value::Null => true,
        yaml::Value::Sequence(items) => items.is_empty(),
        yaml::Value::Mapping(entries) => entries.is_empty(),
        _ => false,
    }
}

/// The components an applied deployment reports.
struct ComponentCounts {
    datasets: usize,
    models: usize,
    catalogs: usize,
    views: usize,
}

impl ComponentCounts {
    fn of(app: &App) -> Self {
        Self {
            datasets: app.datasets.len(),
            models: app.models.len(),
            catalogs: app.catalogs.len(),
            views: app.views.len(),
        }
    }
}

impl std::fmt::Display for ComponentCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} datasets, {} models, {} catalogs, {} views",
            self.datasets, self.models, self.catalogs, self.views
        )
    }
}

/// Thin adapter so the cloud-connect client can call into the runtime
/// without taking a hard dep on the `runtime` crate.
struct SpicedRuntimeHandle {
    runtime: Arc<Runtime>,
    /// Recent log lines for `GetLogs`. `None` when the capture layer was
    /// not installed (Cloud Connect not configured at tracing-init time).
    logs: Option<LogRingBuffer>,
    /// Secrets the control plane delivered with a deployment. Registered as a
    /// built-in store so `${ secrets:NAME }` resolves them with nothing declared
    /// in the spicepod, and so a spicepod reload cannot drop them.
    delivered_secrets: Arc<CloudDeliveredSecretStore>,
    /// Where the identity — and with it the local cache key — lives.
    ///
    /// The key is read from here on each write rather than captured at startup,
    /// because an instance enrolling *in this process* has no identity yet when
    /// the handle is built: capturing then would leave the cache permanently
    /// unwritable on a first boot, which is precisely the case the cache exists
    /// for.
    identity_path: std::path::PathBuf,
    /// The deployment this process is serving, replaced by a hot apply so the
    /// next one is diffed against what the process serves now rather than what
    /// it booted on.
    ///
    /// A runtime serving a local spicepod holds an empty string, which matches
    /// no incoming deployment.
    ///
    /// Guarded by a `parking_lot` lock held for the read/write only, never
    /// across an `.await`.
    live: RwLock<String>,
    /// A deployment this process validated and persisted, waiting on the exit
    /// that makes it live. Read on every status report, written by an apply
    /// that needs a restart — a `parking_lot` lock held for the read/write
    /// only, never across an `.await`.
    persisted: RwLock<Option<String>>,
    /// The `--set-runtime` overrides this process was started with, applied to a
    /// deployment the same way the start path applies them so what a hot apply
    /// installs is the app a restart onto the same file would have produced.
    runtime_overrides: Vec<(String, String)>,
    /// Serializes an apply end to end — the decision, the persistence it commits
    /// to, and the live-deployment update that follows — so two dispatches
    /// cannot interleave one's decision with the other's state change.
    applying: tokio::sync::Mutex<()>,
    /// What will relaunch this process after a deployment exits it.
    supervisor: Supervisor,
    /// Reader for the metrics pushed to the control plane. `None` when no
    /// reader was attached to the meter provider, in which case this instance
    /// reports nothing rather than an empty payload.
    metrics: Option<MetricsReader>,
    /// The app this instance's metrics are attributed to, learned from
    /// `ApplySpicepod` and mirrored into the identity file so it survives a
    /// restart. `None` until the control plane names one, and metrics are
    /// withheld for as long as it is: a series with no `scp_app_id` is ingested
    /// but matches no app dashboard, so exporting one spends the backend's quota
    /// to produce something nothing can read.
    ///
    /// Guarded by a `parking_lot` lock held only for the brief read/write, never
    /// across an `.await`.
    app_id: RwLock<Option<String>>,
}

/// What a [`SpicedRuntimeHandle`] is assembled from, before the deployment is
/// reduced to the spicepod that is live and the mutable fields are wrapped.
struct SpicedRuntimeHandleParts {
    runtime: Arc<Runtime>,
    logs: Option<LogRingBuffer>,
    delivered_secrets: Arc<CloudDeliveredSecretStore>,
    identity_path: std::path::PathBuf,
    running_deployment: Option<CloudManagedSpicepod>,
    supervisor: Supervisor,
    metrics: Option<MetricsReader>,
    app_id: Option<String>,
    runtime_overrides: Vec<(String, String)>,
}

impl SpicedRuntimeHandle {
    fn new(parts: SpicedRuntimeHandleParts) -> Self {
        let SpicedRuntimeHandleParts {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            running_deployment,
            supervisor,
            metrics,
            app_id,
            runtime_overrides,
        } = parts;
        let live = running_deployment.map_or_else(String::new, |running| running.spicepod_yaml);
        Self {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            live: RwLock::new(live),
            persisted: RwLock::new(None),
            runtime_overrides,
            applying: tokio::sync::Mutex::new(()),
            supervisor,
            metrics,
            app_id: RwLock::new(app_id),
        }
    }

    /// Record the app id alongside the credential so a restart keeps exporting.
    ///
    /// Runs on the blocking pool: the identity store is synchronous `std::fs`,
    /// and this is called from the command dispatch loop.
    ///
    /// A failure is logged, not returned. The in-memory value is already set, so
    /// metrics flow either way — all that is lost is durability across a
    /// restart, and failing the deploy over that would be the wrong trade.
    async fn persist_app_id(&self, app_id: &str) {
        let path = self.identity_path.clone();
        let app_id = app_id.to_string();
        let result =
            tokio::task::spawn_blocking(move || IdentityStore::store_app_id(&path, &app_id)).await;
        let error = match result {
            Ok(Ok(())) => return,
            Ok(Err(err)) => err.to_string(),
            Err(join) => format!("identity persistence task panicked: {join}"),
        };
        tracing::warn!(
            "Spice Cloud Connect could not save the cloud app ID to {}. Metrics for this instance will not appear in Spice Cloud if the process restarts. Does the runtime have permission to write to that path? {error}",
            self.identity_path.display()
        );
    }

    /// Persist the attachment tuple and return the attachment state now on
    /// disk — which is what the command result reports, since absence
    /// preserves (a detach keeps the org) and echoing the command instead
    /// would misreport the instance.
    async fn persist_attachment(
        &self,
        attachment: Option<&AppAttachment>,
    ) -> Result<AttachmentState, CommandError> {
        let path = self.identity_path.clone();
        let attachment = attachment.cloned();
        let persisted = tokio::task::spawn_blocking(move || {
            IdentityStore::set_attachment(&path, attachment.as_ref())
        })
        .await
        .map_err(|error| {
            CommandError::failed(format!("Failed to save the cloud app attachment: {error}"))
        })?
        .map_err(|error| {
            CommandError::failed(format!(
                "Failed to save the cloud app attachment to {}: {error}. Check that the runtime can write this path and retry.",
                self.identity_path.display()
            ))
        })?;
        persisted.ok_or_else(|| {
            CommandError::failed(
                "Failed to save the cloud app attachment because the Cloud Connect identity is missing. Reconnect the instance and retry.",
            )
        })
    }

    /// The local delivered-secrets cache key, read fresh from `identity.json`.
    ///
    /// `None` when there is no identity yet, or it predates the cache key — in
    /// both cases the cache is unavailable, which costs a redeploy after a
    /// restart rather than the deployment.
    fn cache_key(&self) -> Option<CacheKey> {
        IdentityStore::load_optional(&self.identity_path)
            .ok()
            .flatten()
            .and_then(|identity| identity.cache_key())
    }

    /// Persist the delivered secrets so the restart a deployment takes comes
    /// back up with them, without a control-plane round trip.
    ///
    /// Best-effort by design: the secrets are already applied to this running
    /// instance, so a cache failure costs a redeploy after the next restart
    /// rather than the deployment. It is reported in the command result so the
    /// operator is not left to discover it at restart time.
    fn cache_delivered_secrets(
        &self,
        config_dir: &Path,
        secrets: &runtime_cloud_connect::sealed_secrets::DeliveredSecrets,
    ) -> Option<String> {
        let key = self.cache_key()?;
        let path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        // No deployment version to record: the dispatch does not carry one.
        match runtime_cloud_connect::secret_cache::write(&path, &key, "", secrets) {
            Ok(()) => None,
            Err(err) => {
                tracing::warn!("Spice Cloud Connect: could not cache delivered secrets: {err}");
                Some(err.to_string())
            }
        }
    }

    /// Write the delivered secrets to the cache for a deployment that has
    /// nothing else left to do.
    ///
    /// The values are the ones this instance already holds — that is what makes
    /// the deployment a redelivery — but the cache is what the next start
    /// reads, and a write that failed on an earlier deployment would otherwise
    /// never be retried: a redelivery is the last chance to repair it before a
    /// restart needs it.
    fn refresh_secret_cache(
        &self,
        config_dir: &Path,
        delivered_secrets: Option<&runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
    ) -> Option<String> {
        self.cache_delivered_secrets(config_dir, delivered_secrets?)
    }

    /// Persist the deployment and reconcile it into this running process.
    ///
    /// Components converge one at a time: one that fails to build lands in an
    /// error state, stays visible through `GetStatus`, and is retried by the
    /// next deployment — the same place a component that fails at boot lands.
    /// Nothing is rolled back, and the result does not claim otherwise.
    ///
    /// The deployment is recorded as live only once the app is installed, so a
    /// redelivery arriving before that is answered as the deployment still
    /// being applied rather than as one already serving. Live names the
    /// deployment this process serves, not a component-by-component state:
    /// which components are ready is what `GetStatus` answers.
    async fn hot_apply(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
        delivered_secrets: Option<runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
    ) -> Result<ApplyOutcome, CommandError> {
        let staged =
            stage_cloud_managed_spicepod(config_dir, spicepod_yaml, &self.runtime_overrides)
                .await?;

        // The instance already holds these values — that is what makes this a
        // hot apply — but a cache write that failed on an earlier deployment
        // would otherwise never be retried, and the restart that eventually
        // comes would have no secrets to resolve.
        let mut cache_error = None;
        let delivered_names = delivered_secrets.map(|secrets| {
            cache_error = self.cache_delivered_secrets(config_dir, &secrets);
            secrets.keys().cloned().collect::<Vec<String>>()
        });

        let (new_app, path) = staged.promote().await?;
        let counts = ComponentCounts::of(&new_app);

        Arc::clone(&self.runtime).apply_app(Arc::new(new_app)).await;
        *self.live.write() = spicepod_yaml.to_string();

        tracing::info!(
            "Spice Cloud Connect: the deployed spicepod was validated, persisted to {} ({counts}), and applied without restarting",
            path.display(),
        );

        Ok(ApplyOutcome::settled(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": true,
            "restart": "not_required",
            "message": "The spicepod was validated, persisted, and applied to this running instance, which is serving it. Its components reconcile one at a time, so some may still be loading: GetStatus reports which are ready, and one that fails to load stays there and is retried by the next deployment.",
            "datasets": counts.datasets,
            "models": counts.models,
            "catalogs": counts.catalogs,
            "views": counts.views,
            // Names only — a delivered value never leaves this process.
            "delivered_secrets": delivered_names,
            "secrets_cache_error": cache_error,
        })))
    }

    /// Persist the deployment for the restart that makes it live.
    ///
    /// Ordered so that a deployment that does not land leaves the running
    /// instance as it was, and one interrupted part-way leaves it able to
    /// start.
    ///
    /// The spicepod is validated before the delivered secrets are touched at
    /// all: building the [`App`] parses the document rather than resolving
    /// `${ secrets:… }` — a component does that as it loads — so a deployment
    /// that does not build changes nothing.
    ///
    /// They are cached before the spicepod is promoted, because the restart is
    /// what makes it live and the components that resolve `${ secrets:… }` run
    /// during that start: an instance interrupted in between comes back on the
    /// previous spicepod with the current credentials cached for it, rather
    /// than on a spicepod whose secrets it cannot resolve.
    ///
    /// What this process resolves changes last, so a promotion that fails
    /// leaves the instance serving the configuration and resolving the
    /// credentials it already had.
    async fn cold_apply(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
        delivered_secrets: Option<runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
        reason: &ColdReason,
    ) -> Result<ApplyOutcome, CommandError> {
        let staged =
            stage_cloud_managed_spicepod(config_dir, spicepod_yaml, &self.runtime_overrides)
                .await?;

        let mut cache_error = None;
        if let Some(secrets) = &delivered_secrets {
            cache_error = self.cache_delivered_secrets(config_dir, secrets);
        }

        let (new_app, path) = staged.promote().await?;

        let delivered_names = delivered_secrets.map(|secrets| {
            let names: Vec<String> = secrets.keys().cloned().collect();
            // Replaces the whole set: an app whose secrets were removed must
            // stop resolving them.
            self.delivered_secrets.replace(secrets);
            names
        });
        let counts = ComponentCounts::of(&new_app);

        *self.persisted.write() = Some(spicepod_yaml.to_string());

        tracing::info!(
            "Spice Cloud Connect: the deployed spicepod was validated and persisted to {} ({counts}); restarting to apply it because {}",
            path.display(),
            reason.summary(),
        );

        Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": false,
            "restart": "required",
            // The sections that forced the restart, so a caller can tell an
            // operator what to change to keep the next deployment in place.
            "restart_sections": reason.sections(),
            "supervised": self.supervisor.is_supervised(),
            "supervisor": self.supervisor.as_str(),
            "message": reason.message(),
            "datasets": counts.datasets,
            "models": counts.models,
            "catalogs": counts.catalogs,
            "views": counts.views,
            // Names only — a delivered value never leaves this process.
            "delivered_secrets": delivered_names,
            "secrets_cache_error": cache_error,
        })))
    }
}

#[async_trait]
impl RuntimeHandle for SpicedRuntimeHandle {
    /// What a standalone `spiced` can answer. `Restart` is excluded
    /// deliberately: a restart is how a *deployment* applies, driven by the
    /// instance once it has a validated spicepod to come back on — a restart on
    /// demand, with nothing to change, is a control the portal should not offer
    /// for a process that may have no supervisor to come back under.
    fn supports(&self, capability: Capability) -> bool {
        match capability {
            // The handle holds the runtime, so it can always plan and execute
            // a query against whatever the instance currently serves — an
            // empty catalog answers with an error, not an inability.
            Capability::ApplySpicepod
            | Capability::AttachApp
            | Capability::GetStatus
            | Capability::ExecuteQuery => true,
            // Only when the log-capture layer was installed at startup;
            // otherwise there is no buffer to read from.
            Capability::GetLogs => self.logs.is_some(),
            Capability::Restart | Capability::UpgradeRuntime => false,
        }
    }

    fn unsupported_reason(&self, capability: Capability) -> String {
        match capability {
            Capability::Restart => "Restart is unsupported on standalone spiced: it is not a control the runtime offers on demand. A deployment that needs one already restarts this instance onto the spicepod it validated; to restart it without deploying, use your process manager (systemd/Docker/Kubernetes). See: https://spiceai.org/docs".to_string(),
            Capability::UpgradeRuntime => "UpgradeRuntime is unsupported on standalone spiced: it cannot replace its own binary. Upgrade it the way you installed it (`spice upgrade`, your container image, or your package manager). See: https://spiceai.org/docs".to_string(),
            Capability::GetLogs => "Log capture is not enabled for this runtime: Spice Cloud Connect must be configured before startup for spiced to install the log-capture layer. See: https://spiceai.org/docs".to_string(),
            Capability::ApplySpicepod
            | Capability::AttachApp
            | Capability::GetStatus
            | Capability::ExecuteQuery => format!(
                "{} is not supported by this instance",
                capability.wire_name()
            ),
        }
    }

    async fn active_datasets(&self) -> u32 {
        match self.runtime.read_app().await {
            Some(app) => u32::try_from(app.datasets.len()).unwrap_or(u32::MAX),
            None => 0,
        }
    }

    async fn active_models(&self) -> u32 {
        match self.runtime.read_app().await {
            Some(app) => u32::try_from(app.models.len()).unwrap_or(u32::MAX),
            None => 0,
        }
    }

    async fn runtime_info_json(&self) -> serde_json::Value {
        let app = self.runtime.read_app().await;
        let (name, datasets, models, catalogs, views) = match &app {
            Some(app) => (
                Some(app.name.clone()),
                app.datasets.len(),
                app.models.len(),
                app.catalogs.len(),
                app.views.len(),
            ),
            None => (None, 0, 0, 0, 0),
        };
        serde_json::json!({
            "name": name,
            "datasets": datasets,
            "models": models,
            "catalogs": catalogs,
            "views": views,
        })
    }

    /// Apply a cloud-managed spicepod, in this process where it can be and by
    /// restarting where it cannot.
    ///
    /// 1. A redelivery of the spicepod this process is already serving — the
    ///    same YAML, byte for byte, delivering the secret values it already
    ///    holds — is answered as applied without reconciling or restarting
    ///    anything; restarting for it would make a redelivery a restart loop.
    ///    It does write the delivered secrets to the cache the next start reads
    ///    ([`SpicedRuntimeHandle::refresh_secret_cache`]), which is the one
    ///    thing a redelivery can still repair.
    /// 2. [`classify_deployment`] decides how the deployment applies, before the
    ///    spicepod is written, its secrets are installed, or any component is
    ///    reconciled.
    /// 3. Either way the YAML is validated by building an [`App`] from it on a
    ///    sibling temp file, so a malformed push is rejected with a clear error
    ///    and the previous good `spicepod-cloud-managed.yml` is left untouched
    ///    and still running, and the validated file is then promoted to the
    ///    canonical path so the next start comes up on this configuration.
    /// 4. A hot apply reconciles the new app into this process
    ///    ([`SpicedRuntimeHandle::hot_apply`]) and records it as what is live. A
    ///    cold one installs the delivered secrets, records the spicepod as
    ///    persisted, and returns [`ApplyOutcome::exit_to_apply`] — the client
    ///    sends the result and then calls [`RuntimeHandle::exit_to_apply`],
    ///    which drains and exits 0 for the supervisor to relaunch.
    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<ApplyOutcome, CommandError> {
        let SpicepodDeployment {
            config_dir,
            spicepod_yaml,
            delivered_secrets,
            app_id,
        } = deployment;

        // One apply at a time. The decision below is only immutable if nothing
        // else can persist a spicepod, install secrets, or change what is live
        // between taking it and completing the apply it commits to. It does not
        // extend to the exit a cold apply asks the client for, which happens
        // after this returns — `persisted` is what keeps a deployment arriving
        // in that window off the hot path.
        let _applying = self.applying.lock().await;

        // Recorded before staging, and independently of whether staging succeeds:
        // which app this instance belongs to is a fact about the deploy's target,
        // not about the spicepod being valid. A rejected spicepod would otherwise
        // withhold metrics for a reason that has nothing to do with them.
        //
        // Leave any id already recorded in place when the deployment names none:
        // the instance's app has not changed, and clearing would silence metrics
        // that are correctly attributed.
        //
        // Cloned out of the lock rather than read in the scrutinee: a scrutinee
        // temporary lives until the match ends, so the read guard would still be
        // held when an arm takes the write lock.
        let held = self.app_id.read().clone();
        match (app_id, held) {
            (None, Some(held)) => tracing::debug!(
                app_id = %held,
                "Spice Cloud Connect: the Spicepod deployment named no cloud app; keeping the one already recorded"
            ),
            (None, None) => tracing::warn!(
                "Spice Cloud Connect provided no app ID on Spicepod deployment. Metrics for this instance will not appear in Spice Cloud. Is this instance attached to an app?"
            ),
            (Some(app_id), held) => {
                match held.as_deref() {
                    Some(held) if held == app_id => tracing::debug!(
                        app_id,
                        "Spice Cloud Connect: the Spicepod deployment re-confirmed the cloud app metrics are attributed to"
                    ),
                    Some(previous) => tracing::debug!(
                        app_id,
                        previous,
                        "Spice Cloud Connect: the Spicepod deployment moved this instance to a different cloud app; metrics follow it from the next export"
                    ),
                    None => tracing::debug!(
                        app_id,
                        "Spice Cloud Connect: metrics will be attributed to this cloud app from the next export"
                    ),
                }
                *self.app_id.write() = Some(app_id.to_string());
                self.persist_app_id(app_id).await;
            }
        }

        // A delivered set this instance does not hold is a change the spicepod
        // text does not show, so it is decided before anything reads the text.
        let secrets_changed = delivered_secrets
            .as_ref()
            .is_some_and(|secrets| !self.delivered_secrets.holds(secrets));

        // Cloned out of the locks rather than compared under them: the guards
        // must not be held across the awaits below.
        let live = self.live.read().clone();
        let persisted = self.persisted.read().clone();
        match disposition(&live, persisted.as_deref(), spicepod_yaml, secrets_changed) {
            Disposition::AlreadyLive => {
                tracing::info!(
                    "Spice Cloud Connect: the deployed spicepod is already applied; nothing to reconcile"
                );
                return Ok(ApplyOutcome::settled(serde_json::json!({
                    "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                    "applied": true,
                    "live": true,
                    "restart": "not_required",
                    "message": "This spicepod is already applied and live; this instance is serving it.",
                    "secrets_cache_error": self.refresh_secret_cache(config_dir, delivered_secrets.as_ref()),
                })));
            }
            Disposition::AlreadyPersisted => {
                tracing::info!(
                    "Spice Cloud Connect: the deployed spicepod is already persisted; restarting onto it"
                );
                return Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
                    "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                    "applied": true,
                    "live": false,
                    "restart": "in_progress",
                    "message": "This spicepod is persisted; this instance is restarting onto it.",
                    "secrets_cache_error": self.refresh_secret_cache(config_dir, delivered_secrets.as_ref()),
                })));
            }
            Disposition::Apply => {}
        }

        let mode = classify_deployment(&ApplyFacts {
            live: &live,
            incoming: spicepod_yaml,
            secrets_changed,
            restart_pending: persisted.is_some(),
            initial_load_unfinished: self.runtime.initial_load_in_flight(),
        });

        match mode {
            ApplyMode::Hot => {
                self.hot_apply(config_dir, spicepod_yaml, delivered_secrets)
                    .await
            }
            ApplyMode::Cold(reason) => {
                self.cold_apply(config_dir, spicepod_yaml, delivered_secrets, &reason)
                    .await
            }
        }
    }

    /// Drain and exit 0 so the supervisor relaunches spiced on the spicepod
    /// [`SpicedRuntimeHandle::apply_spicepod`] persisted.
    ///
    /// Takes the same drain path a `SIGTERM` would rather than exiting outright:
    /// a deployment restart is a planned shutdown, and an accelerator with local
    /// state has to flush it before the process goes. The drain is bounded by
    /// [`DEPLOYMENT_DRAIN_BUDGET`] — a deployment that cannot finish draining
    /// must still restart the instance, not strand it.
    ///
    /// Does not return.
    async fn exit_to_apply(&self) {
        // Abandon an unfinished initial load first. It retries a failing
        // component for as long as the runtime is up, so left running it would
        // keep registering datasets from the app being replaced for the whole
        // drain — work thrown away at exit, against a configuration that is no
        // longer canonical.
        if self.runtime.supersede_initial_load() {
            tracing::debug!(
                "Spice Cloud Connect: abandoned the in-flight component load to restart for a deployment"
            );
        }

        if tokio::time::timeout(DEPLOYMENT_DRAIN_BUDGET, self.runtime.shutdown())
            .await
            .is_err()
        {
            tracing::warn!(
                "Spice Cloud Connect: the runtime did not finish draining within {DEPLOYMENT_DRAIN_BUDGET:?}; exiting anyway to apply the deployment"
            );
        }

        if !self.supervisor.is_supervised() {
            tracing::error!(
                "Spice Cloud Connect: exiting to apply a deployment with no process supervisor detected — nothing will restart this instance. Install the service with `sudo spice connect --install`, or run spiced under a supervisor. See: https://spiceai.org/docs"
            );
        }
        tracing::info!("Spice Cloud Connect: exiting to apply the deployment");
        std::process::exit(0);
    }

    /// Return recent captured log lines for a `GetLogs` command.
    ///
    /// A standalone `spiced` has no pod / kube API, so it serves its own
    /// recently-captured log output (see [`crate::log_capture`]) instead. The
    /// text is returned verbatim to the caller, which sends it as the `text`
    /// arm of the result payload.
    ///
    /// An absent `tail_lines` returns the last [`DEFAULT_LOG_TAIL_LINES`]
    /// lines; a value returns that many (capped by the ring buffer). Returns
    /// an error — not an empty string — when capture is unavailable, so the
    /// control plane can tell "no logs captured" from "logging off".
    async fn get_logs(&self, tail_lines: Option<u32>) -> Result<String, CommandError> {
        let Some(ring) = self.logs.as_ref() else {
            return Err(CommandError::unsupported(
                self.unsupported_reason(Capability::GetLogs),
            ));
        };
        let n = tail_lines.map_or(DEFAULT_LOG_TAIL_LINES, |lines| {
            usize::try_from(lines).unwrap_or(DEFAULT_LOG_TAIL_LINES)
        });
        Ok(ring.tail(n))
    }

    /// Report standalone runtime readiness — for `GetStatus`, and for the
    /// phase stamped on every heartbeat.
    ///
    /// - [`RuntimePhase::Failed`] — the runtime is shutting down.
    /// - [`RuntimePhase::Ready`] — all registered components have reached
    ///   readiness (`RuntimeStatus::is_ready`).
    /// - [`RuntimePhase::Progressing`] — otherwise (components still
    ///   initializing/erroring).
    ///
    /// A conservative `Progressing` (rather than `Failed`) is used for
    /// not-yet-ready runtimes because `is_ready` is deliberately lenient — an
    /// accelerated dataset can keep serving from its acceleration layer even
    /// while its source is in error — so a component error is not necessarily
    /// terminal. Per-component states and any error messages ride in the
    /// `components`/`errors` detail, alongside the deployment this instance is
    /// serving and whether anything supervises it — the two facts that say
    /// whether a deployment landed and whether the next one can.
    async fn status(&self) -> Result<StatusReport, CommandError> {
        let status = self.runtime.status();
        let all = status.get_all_statuses();
        let ready = status.is_ready();
        let shutting_down = status.is_shutdown();

        let total = all.len();
        let ready_count = all
            .values()
            .filter(|s| matches!(s, ComponentStatus::Ready))
            .count();

        // Collect components that are neither Ready nor Refreshing, plus any
        // error messages, so `reason` can name what's holding readiness back.
        let mut not_ready: Vec<String> = Vec::new();
        let mut errors: Vec<serde_json::Value> = Vec::new();
        for (name, st) in &all {
            match st {
                ComponentStatus::Ready | ComponentStatus::Refreshing => {}
                ComponentStatus::Error(msg) => {
                    not_ready.push(name.clone());
                    errors.push(serde_json::json!({
                        "component": name,
                        "message": msg.clone(),
                    }));
                }
                _ => not_ready.push(name.clone()),
            }
        }
        not_ready.sort();

        let (phase, reason) = if shutting_down {
            (RuntimePhase::Failed, "runtime is shutting down".to_string())
        } else if ready {
            (
                RuntimePhase::Ready,
                format!("{ready_count}/{total} components ready"),
            )
        } else if total == 0 {
            (
                RuntimePhase::Progressing,
                "no components registered yet".to_string(),
            )
        } else {
            (
                RuntimePhase::Progressing,
                format!(
                    "{ready_count}/{total} components ready; waiting on: {}",
                    not_ready.join(", ")
                ),
            )
        };

        // Per-component map: name -> status string (ComponentStatus serializes
        // as a plain string; the Error variant collapses to "Error", so error
        // messages are surfaced separately in `errors`).
        let components: serde_json::Map<String, serde_json::Value> = all
            .iter()
            .map(|(k, v)| (k.clone(), serde_json::json!(v.to_string())))
            .collect();

        // A deployment this process persisted but has not yet restarted onto.
        // The window is short — the client exits as soon as the result is
        // flushed — but a status read inside it must not report the deployment
        // as live.
        let restart_pending = self.persisted.read().is_some();

        Ok(
            StatusReport::new(phase, reason).with_detail(serde_json::json!({
                "ready": ready,
                "component_count": total,
                "ready_count": ready_count,
                "components": components,
                "errors": errors,
                "restart_pending": restart_pending,
                // Whether anything will bring this instance back when a
                // deployment exits it. `false` means a deployment stops the
                // instance instead of updating it — the operator has to know
                // before that happens, not after.
                "supervised": self.supervisor.is_supervised(),
                "supervisor": self.supervisor.as_str(),
                "supervisor_note": self.supervisor.caveat(),
                // Names only, never values — this document reaches the portal
                // and the command log. The count and the names are what let an
                // operator tell "the deployment delivered no secrets" apart from
                // "the component's reference is misspelled".
                "delivered_secrets": self.delivered_secrets.names(),
                "delivered_secrets_available": !self.delivered_secrets.is_empty(),
                // Whether a restart will still have them. Without a cache key
                // the secrets are memory-only, so the next restart needs a
                // redeploy — status says so rather than failing mutely later.
                "delivered_secrets_persisted": self.cache_key().is_some(),
            })),
        )
    }

    async fn collect_metrics(&self) -> Result<Option<Vec<u8>>, CommandError> {
        let Some(reader) = &self.metrics else {
            return Ok(None);
        };
        // Read and release: the collection below is CPU work, and the write side
        // is an inbound ApplySpicepod that must not queue behind it.
        let app_id = self.app_id.read().clone();
        let Some(app_id) = app_id else {
            tracing::debug!(
                "Spice Cloud Connect: metrics are withheld because this instance is not attached to a Spice Cloud app"
            );
            return Ok(None);
        };
        let reader = reader.clone();
        tokio::task::spawn_blocking(move || reader.collect_otlp_export(&app_id))
            .await
            .map_err(|err| {
                CommandError::internal(format!("The metrics encoder stopped unexpectedly: {err}"))
            })?
            .map_err(|err| CommandError::internal(err.to_string()))
    }

    async fn attach_app(
        &self,
        attachment: Option<&AppAttachment>,
    ) -> Result<serde_json::Value, CommandError> {
        let persisted = self.persist_attachment(attachment).await?;
        (*self.app_id.write()).clone_from(&persisted.app_id);
        // The result reports the persisted state, not the command: the two
        // differ where absence preserves (a detach keeps the stored org).
        Ok(serde_json::json!(persisted))
    }

    /// Execute an `ExecuteQuery` against the in-process runtime.
    ///
    /// The query goes through the same `DataFusion` entry point the local SQL
    /// APIs use, so there is no HTTP hop and no second set of query semantics
    /// to keep aligned.
    ///
    /// It runs **read-only**: the statement is planned and then rejected if it
    /// carries DDL, DML, `COPY`, or a write-capable extension. This command
    /// arrives from the control plane rather than from someone holding the
    /// instance's own credentials, so it reads the instance and never changes
    /// it.
    ///
    /// `max_rows` is clamped again here rather than trusted: the caller already
    /// clamps it, and a limit enforced in exactly one place is a limit one
    /// refactor away from being gone.
    async fn execute_query(&self, sql: &str, max_rows: u32) -> Result<QueryOutcome, CommandError> {
        let result = self
            .runtime
            .datafusion()
            .query_builder(sql)
            .read_only(true)
            .build()
            .run()
            .await
            .map_err(|source| query_error(&source))?;
        bounded_arrow_ipc(result.data, effective_max_rows(max_rows)).await
    }
}

/// Classify a query failure so the portal can tell a bad statement from a
/// struggling instance without reading the English.
///
/// A statement the engine could not parse, plan, or resolve is the caller's to
/// fix and fails identically on retry — that is `INVALID_ARGUMENT`. An
/// execution, resource, or I/O fault is the instance's, and may not recur, so
/// it stays retryable.
fn query_error(source: &QueryError) -> CommandError {
    let caller_fault = match source {
        QueryError::UnableToExecuteQuery { source } | QueryError::BindingParameters { source } => {
            is_caller_error(source)
        }
        QueryError::TableAccessDisallowed { .. } => true,
        _ => false,
    };
    classify(caller_fault, format!("Query failed: {source}"))
}

/// The same classification for a failure that arrives as a bare
/// `DataFusionError` — mid-stream, after planning already succeeded.
fn datafusion_error(source: &DataFusionError) -> CommandError {
    classify(is_caller_error(source), format!("Query failed: {source}"))
}

fn classify(caller_fault: bool, message: String) -> CommandError {
    if caller_fault {
        CommandError::invalid_argument(message)
    } else {
        CommandError::failed(message)
    }
}

/// Whether a `DataFusion` error blames the statement rather than the instance.
fn is_caller_error(source: &DataFusionError) -> bool {
    match source {
        DataFusionError::SQL(..)
        | DataFusionError::Plan(..)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(..) => true,
        // Wrappers that add context around the error that actually happened.
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            is_caller_error(inner)
        }
        DataFusionError::Shared(inner) => is_caller_error(inner),
        // Only the caller's fault if nothing in the collection is the
        // instance's: a set holding both blames the instance, since that is the
        // half a retry might clear.
        DataFusionError::Collection(errors) => {
            !errors.is_empty() && errors.iter().all(is_caller_error)
        }
        _ => false,
    }
}

/// An in-memory sink that refuses to grow past `limit`.
///
/// Bounding the *writer* is what makes an oversized result cheap: the encoder
/// fails on the first write that would cross the cap, so the bytes are never
/// materialized and then measured. A single row too large to fit trips it the
/// same way a million small ones do.
struct BoundedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    /// Set when a write was refused, so the caller can tell the cap apart from
    /// a genuine encoding fault after the error has been laundered through
    /// `ArrowError`.
    overflowed: Arc<AtomicBool>,
}

impl std::io::Write for BoundedBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.bytes.len().saturating_add(data.len()) > self.limit {
            self.overflowed.store(true, Ordering::Relaxed);
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "Cloud Connect query result limit exceeded",
            ));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode a result of at most `max_rows` rows as one complete Arrow IPC stream
/// of at most [`MAX_QUERY_RESULT_BYTES`] bytes.
///
/// One row beyond the cap is read so an exact-cap result can be distinguished
/// from a truncated one. Encoding runs on Tokio's blocking pool and receives
/// batches over a one-slot channel, retaining the streaming bound without
/// doing synchronous Arrow serialization on a runtime worker. An empty result
/// still produces a valid stream carrying the schema.
async fn bounded_arrow_ipc(
    mut stream: SendableRecordBatchStream,
    max_rows: u32,
) -> Result<QueryOutcome, CommandError> {
    let schema = stream.schema();
    let limit = max_rows as usize;
    let overflowed = Arc::new(AtomicBool::new(false));
    let encoder_overflowed = Arc::clone(&overflowed);
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<RecordBatch>(1);
    let encoder = tokio::task::spawn_blocking(move || {
        let sink = BoundedBuffer {
            bytes: Vec::new(),
            limit: MAX_QUERY_RESULT_BYTES,
            overflowed: Arc::clone(&encoder_overflowed),
        };
        let mut writer = StreamWriter::try_new(sink, &schema)
            .map_err(|source| encode_error(&encoder_overflowed, &source))?;
        let mut rows = 0_u64;
        while let Some(batch) = receiver.blocking_recv() {
            rows = rows.saturating_add(batch.num_rows() as u64);
            writer
                .write(&batch)
                .map_err(|source| encode_error(&encoder_overflowed, &source))?;
        }
        writer
            .finish()
            .map_err(|source| encode_error(&encoder_overflowed, &source))?;
        let sink = writer
            .into_inner()
            .map_err(|source| encode_error(&encoder_overflowed, &source))?;
        Ok(QueryOutcome {
            arrow_ipc: sink.bytes,
            row_count: rows,
        })
    });

    let stream_result = async {
        let mut rows = 0_usize;
        let mut exceeded = false;
        while let Some(batch) = stream.next().await {
            // A failure that surfaces here rather than at planning is usually
            // the instance's — a federated source going away mid-scan.
            let batch = batch.map_err(|source| datafusion_error(&source))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let remaining = limit.saturating_sub(rows);
            if batch.num_rows() > remaining {
                exceeded = true;
                break;
            }
            rows += batch.num_rows();
            if sender.send(batch).await.is_err() {
                // The encoder has already produced the more specific error;
                // stop pulling the query stream and surface it below.
                break;
            }
        }
        Ok::<bool, CommandError>(exceeded)
    }
    .await;
    drop(sender);

    // Always join the blocking task, including after a stream failure, so an
    // encoder never outlives the command that owns it.
    let outcome = encoder.await.map_err(|err| {
        CommandError::internal(format!(
            "The query result encoder stopped unexpectedly: {err}"
        ))
    })?;
    let exceeded = stream_result?;
    if exceeded {
        return Err(row_limit_error(max_rows));
    }
    outcome
}

fn row_limit_error(max_rows: u32) -> CommandError {
    CommandError::result_too_large(format!(
        "The query result exceeds the {max_rows}-row Cloud Connect limit and was not sent. Add or reduce LIMIT, narrow the projection, or aggregate the result and run it again."
    ))
}

/// Classify an encoder failure: the byte cap, or a genuine fault.
///
/// Neither message repeats the query or a value from it.
fn encode_error(overflowed: &AtomicBool, source: &ArrowError) -> CommandError {
    if overflowed.load(Ordering::Relaxed) {
        return CommandError::result_too_large(format!(
            "The query result exceeds the {} MiB Cloud Connect limit and was not sent. Return fewer rows or columns (a smaller LIMIT, a narrower projection, or an aggregate) and run it again.",
            MAX_QUERY_RESULT_BYTES / (1024 * 1024)
        ));
    }
    CommandError::internal(format!(
        "Failed to encode the query result as Arrow IPC: {source}"
    ))
}

/// A validated cloud-managed spicepod, written beside the canonical file and
/// waiting to be promoted onto it.
///
/// Validating and promoting are separate steps so a caller with state that has
/// to survive the deployment — the delivered-secrets cache, which the restart
/// reads — can write it while the instance still starts on the previous
/// spicepod. A crash in that window comes back on the previous configuration
/// with the new secrets, rather than on the new configuration with secrets it
/// cannot resolve.
#[derive(Debug)]
struct StagedSpicepod {
    /// The app the deployment builds. The caller reads the component counts it
    /// reports off it.
    app: App,
    incoming: std::path::PathBuf,
    canonical: std::path::PathBuf,
}

impl StagedSpicepod {
    /// Promote the validated file onto the canonical path, so the next start
    /// comes up on this configuration.
    async fn promote(self) -> Result<(App, std::path::PathBuf), CommandError> {
        match replace_canonical_spicepod(&self.incoming, &self.canonical).await {
            Ok(()) => Ok((self.app, self.canonical)),
            Err(err) => {
                // Best-effort cleanup; ignore failure (temp file is inert).
                let _ = tokio::fs::remove_file(&self.incoming).await;
                Err(err)
            }
        }
    }
}

/// Validate a cloud-managed spicepod against a sibling `*.incoming.yml` temp
/// file, so a bad push never clobbers the last known-good spicepod on disk.
///
/// On any failure the canonical file is left untouched — the instance keeps
/// serving it — and the temp file is cleaned up.
///
/// `runtime_overrides` are applied to the built app exactly as the start path
/// applies them, so a deployment applied to this process lands the app a restart
/// onto the same file would have produced.
///
/// Factored out of [`SpicedRuntimeHandle::apply_spicepod`] so the
/// file-staging + validation can be unit-tested without a running runtime.
/// A malformed push is the operator's mistake, not the runtime's, so it is
/// reported as [`CommandError::InvalidArgument`]; a filesystem failure is
/// something the runtime may recover from, so it is [`CommandError::Failed`].
async fn stage_cloud_managed_spicepod(
    config_dir: &Path,
    spicepod_yaml: &str,
    runtime_overrides: &[(String, String)],
) -> Result<StagedSpicepod, CommandError> {
    let canonical = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|e| CommandError::failed(format!("create config dir: {e}")))?;

    let incoming = config_dir.join("spicepod-cloud-managed.incoming.yml");
    tokio::fs::write(&incoming, spicepod_yaml)
        .await
        .map_err(|e| CommandError::failed(format!("write spicepod: {e}")))?;

    match AppBuilder::build_from_path(incoming.clone()).await {
        Ok(mut app) => {
            // Rendered before the cleanup below: the error is not `Send`, so
            // holding it across an `.await` would make this future unspawnable.
            match crate::apply_overrides(app.runtime, runtime_overrides).map_err(|e| e.to_string())
            {
                Ok(runtime) => app.runtime = runtime,
                Err(e) => {
                    let _ = tokio::fs::remove_file(&incoming).await;
                    return Err(CommandError::invalid_argument(format!(
                        "invalid spicepod: the runtime overrides this instance was started with (--set-runtime) do not apply to it: {e}"
                    )));
                }
            }
            Ok(StagedSpicepod {
                app,
                incoming,
                canonical,
            })
        }
        Err(e) => {
            // Best-effort cleanup; ignore failure (temp file is inert).
            let _ = tokio::fs::remove_file(&incoming).await;
            Err(CommandError::invalid_argument(format!(
                "invalid spicepod: {e}"
            )))
        }
    }
}

/// Promote the validated `incoming` file onto the canonical `path` without
/// ever leaving the runtime with no known-good config.
///
/// On Unix `rename` atomically replaces an existing destination, so the swap
/// is a single syscall and the canonical file is never absent. On Windows
/// `rename` fails when the destination exists, so we fall back to a
/// backup-and-rollback sequence: move the current canonical file aside to a
/// `*.bak`, move the incoming file into place, then delete the backup on
/// success. If the second move fails we restore the backup, so the previous
/// known-good config survives a mid-swap failure (permissions, a transient
/// file lock, etc.) instead of being deleted up front.
async fn replace_canonical_spicepod(incoming: &Path, path: &Path) -> Result<(), CommandError> {
    match tokio::fs::rename(incoming, path).await {
        Ok(()) => return Ok(()),
        Err(e) => {
            // A fresh install has no canonical file yet — nothing to preserve,
            // so surface the error directly.
            if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                return Err(CommandError::failed(format!("persist spicepod: {e}")));
            }
            // Destination exists (the Windows case): fall through to the
            // backup-and-rollback swap below.
        }
    }

    let backup = path.with_extension("yml.bak");
    let _ = tokio::fs::remove_file(&backup).await;
    tokio::fs::rename(path, &backup)
        .await
        .map_err(|e| CommandError::failed(format!("persist spicepod (backup current): {e}")))?;
    match tokio::fs::rename(incoming, path).await {
        Ok(()) => {
            // New config is in place; the backup is no longer needed.
            let _ = tokio::fs::remove_file(&backup).await;
            Ok(())
        }
        Err(e) => {
            // Roll the previous known-good file back into place so we never
            // lose the only good copy.
            let _ = tokio::fs::rename(&backup, path).await;
            Err(CommandError::failed(format!("persist spicepod: {e}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::ipc::reader::StreamReader;
    use datafusion::physical_plan::memory::MemoryStream;
    use futures::TryStreamExt as _;
    use runtime_cloud_connect::handlers::{MAX_QUERY_ROWS, PostApply};
    use runtime_cloud_connect::sealed_secrets::{DeliveredSecrets, Zeroizing};

    /// Minimal valid spicepod (no components — an empty app is valid).
    const VALID_SPICEPOD: &str = "version: v2\nkind: Spicepod\nname: cloud-managed-test\n";
    /// Invalid YAML (unclosed flow sequence) — guaranteed to fail parsing.
    const INVALID_SPICEPOD: &str = "name: [unclosed";

    /// Create (and clean) a unique temp dir for a test without pulling in a
    /// temp-file crate. Names are per-test + per-process so parallel tests
    /// don't collide.
    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("spice-cc-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }

    #[tokio::test]
    async fn a_missing_managed_deployment_is_an_ordinary_absence() {
        let dir = scratch_dir("managed-missing");
        let path = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);

        let deployed = read_cloud_managed_spicepod(path)
            .await
            .expect("a missing deployment is not a read failure");
        assert!(deployed.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_unreadable_managed_deployment_is_not_treated_as_missing() {
        let dir = scratch_dir("managed-unreadable");
        let path = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
        tokio::fs::write(&path, [0xff])
            .await
            .expect("write invalid UTF-8 fixture");

        let Err(err) = read_cloud_managed_spicepod(path.clone()).await else {
            panic!("invalid UTF-8 must remain a read failure");
        };
        assert_eq!(err.path, path);
        assert_eq!(err.source.kind(), std::io::ErrorKind::InvalidData);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stage_valid_spicepod_writes_canonical_and_returns_app() {
        let dir = scratch_dir("valid");
        let (app, path) = stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD, &[])
            .await
            .expect("valid spicepod stages")
            .promote()
            .await
            .expect("a validated spicepod is promoted");

        assert_eq!(app.name, "cloud-managed-test");
        assert_eq!(path, dir.join(CLOUD_MANAGED_SPICEPOD_FILE));
        // Canonical file written with the supplied content.
        let on_disk = std::fs::read_to_string(&path).expect("canonical file exists");
        assert_eq!(on_disk, VALID_SPICEPOD);
        // The temp .incoming file was renamed away, not left behind.
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stage_invalid_spicepod_preserves_previous_good_file() {
        let dir = scratch_dir("invalid-preserves");
        // Land a known-good canonical file first.
        stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD, &[])
            .await
            .expect("first valid stage")
            .promote()
            .await
            .expect("a validated spicepod is promoted");
        let canonical = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);

        // A subsequent invalid push must be rejected and must NOT clobber it.
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD, &[])
            .await
            .expect_err("invalid spicepod rejected");
        assert!(
            matches!(err, CommandError::InvalidArgument { .. }),
            "a malformed push is the caller's mistake, not a runtime failure: {err}"
        );
        assert!(
            err.to_string().contains("invalid spicepod"),
            "unexpected error: {err}"
        );

        let on_disk = std::fs::read_to_string(&canonical).expect("canonical still present");
        assert_eq!(on_disk, VALID_SPICEPOD, "previous good config preserved");
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn stage_invalid_spicepod_when_none_exists_leaves_no_canonical() {
        let dir = scratch_dir("invalid-fresh");
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD, &[])
            .await
            .expect_err("invalid spicepod rejected");
        assert!(
            matches!(err, CommandError::InvalidArgument { .. }),
            "a malformed push is the caller's mistake, not a runtime failure: {err}"
        );
        assert!(
            err.to_string().contains("invalid spicepod"),
            "unexpected error: {err}"
        );
        // Nothing should have been promoted to the canonical path.
        assert!(!dir.join(CLOUD_MANAGED_SPICEPOD_FILE).exists());
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The redelivery case #1306 turns into a crash loop if it is missed: the
    /// spicepod the instance is already serving must change nothing.
    #[test]
    fn redelivering_the_running_spicepod_is_a_no_op() {
        assert_eq!(
            disposition(VALID_SPICEPOD, None, VALID_SPICEPOD, false),
            Disposition::AlreadyLive
        );
    }

    /// Anything else is a new configuration and must be applied, or the running
    /// app stops matching what was deployed.
    #[test]
    fn a_changed_spicepod_is_applied() {
        let changed = "version: v2\nkind: Spicepod\nname: changed\n";
        assert_eq!(
            disposition(VALID_SPICEPOD, None, changed, false),
            Disposition::Apply
        );
    }

    /// Between the result being sent and the process exiting, the control plane
    /// can redeliver. The instance has already committed to that spicepod, so it
    /// re-issues the exit rather than persisting it again — and never reports it
    /// as live, which it is not.
    #[test]
    fn redelivery_while_a_restart_is_pending_re_issues_the_exit() {
        let pending = "version: v2\nkind: Spicepod\nname: next\n";
        assert_eq!(
            disposition(VALID_SPICEPOD, Some(pending), pending, false),
            Disposition::AlreadyPersisted
        );
        // And the spicepod it replaced is no longer "already live": it is on its
        // way out, so re-sending it is a new configuration to apply.
        assert_eq!(
            disposition(VALID_SPICEPOD, Some(pending), VALID_SPICEPOD, false),
            Disposition::Apply
        );
    }

    /// An instance serving a local spicepod has no deployment to match, so the
    /// first one always applies.
    #[test]
    fn a_runtime_with_no_deployment_applies_the_first_one() {
        assert_eq!(
            disposition("", None, VALID_SPICEPOD, false),
            Disposition::Apply
        );
    }

    /// A rotation can arrive with the spicepod the instance already serves, and
    /// the values are not in the text. Answering it as already applied would
    /// report success for a rotation that never reached the app.
    #[test]
    fn a_redelivery_carrying_new_secret_values_is_not_already_applied() {
        assert_eq!(
            disposition(VALID_SPICEPOD, None, VALID_SPICEPOD, true),
            Disposition::Apply
        );
        let pending = "version: v2\nkind: Spicepod\nname: next\n";
        assert_eq!(
            disposition(VALID_SPICEPOD, Some(pending), pending, true),
            Disposition::Apply,
            "a rotation arriving before the restart still has to be installed and cached for it"
        );
    }

    // ----------------------------------------------------------------------
    // ExecuteQuery: row cap, byte cap, and the Arrow IPC the caller decodes.
    // ----------------------------------------------------------------------

    /// One `Int32` column named `n`, the shape every query test below returns.
    fn int_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]))
    }

    /// A batch of `count` rows counting up from `start`.
    fn int_batch(start: i32, count: i32) -> RecordBatch {
        let values: Vec<i32> = (start..start + count).collect();
        RecordBatch::try_new(int_schema(), vec![Arc::new(Int32Array::from(values))])
            .expect("build int batch")
    }

    /// A batch of one row holding `bytes` bytes of string, for the byte-cap
    /// tests.
    fn wide_batch(bytes: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("blob", DataType::Utf8, false)]));
        let value = "x".repeat(bytes);
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec![value]))])
            .expect("build wide batch")
    }

    fn stream_of(schema: SchemaRef, batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(MemoryStream::try_new(batches, schema, None).expect("memory stream"))
    }

    /// Decode an Arrow IPC stream back into batches — what the caller does, so
    /// the assertion is on a real round trip rather than on a byte count.
    fn decode(ipc: &[u8]) -> (SchemaRef, Vec<RecordBatch>) {
        let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None)
            .expect("the payload must be a complete Arrow IPC stream");
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .expect("every batch must decode");
        (schema, batches)
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(RecordBatch::num_rows).sum()
    }

    #[tokio::test]
    async fn a_query_result_round_trips_through_arrow_ipc() {
        let outcome = bounded_arrow_ipc(
            stream_of(int_schema(), vec![int_batch(0, 3), int_batch(3, 2)]),
            MAX_QUERY_ROWS,
        )
        .await
        .expect("a small result encodes");

        assert_eq!(outcome.row_count, 5);
        let (schema, batches) = decode(&outcome.arrow_ipc);
        assert_eq!(schema.field(0).name(), "n");
        assert_eq!(total_rows(&batches), 5);
        let values: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int column")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![0, 1, 2, 3, 4], "values must survive the trip");
    }

    /// An empty result is a schema and zero rows, not an absent payload — the
    /// caller must be able to tell "no rows" from "no answer".
    #[tokio::test]
    async fn an_empty_result_still_carries_the_schema() {
        let outcome = bounded_arrow_ipc(stream_of(int_schema(), vec![]), MAX_QUERY_ROWS)
            .await
            .expect("an empty result encodes");

        assert_eq!(outcome.row_count, 0);
        assert!(
            !outcome.arrow_ipc.is_empty(),
            "an empty result must still be a valid stream"
        );
        let (schema, batches) = decode(&outcome.arrow_ipc);
        assert_eq!(schema.field(0).name(), "n");
        assert_eq!(total_rows(&batches), 0);
    }

    /// A row over the cap fails instead of returning an apparently complete
    /// result that was silently truncated inside a batch.
    #[tokio::test]
    async fn the_row_cap_rejects_a_partial_batch() {
        let err = bounded_arrow_ipc(
            stream_of(int_schema(), vec![int_batch(0, 600)]),
            MAX_QUERY_ROWS,
        )
        .await
        .expect_err("a truncated result must be refused");
        assert!(
            matches!(err, CommandError::ResultTooLarge { .. }),
            "an over-cap result must be typed result-too-large, got {err:?}"
        );
    }

    /// The cap is enforced across the whole result, not per batch.
    #[tokio::test]
    async fn the_row_cap_rejects_rows_in_a_later_batch() {
        let batches: Vec<RecordBatch> = (0..10).map(|i| int_batch(i * 100, 100)).collect();
        let err = bounded_arrow_ipc(stream_of(int_schema(), batches), 250)
            .await
            .expect_err("a truncated result must be refused");
        assert!(matches!(err, CommandError::ResultTooLarge { .. }));
    }

    #[tokio::test]
    async fn a_result_with_exactly_the_row_cap_succeeds() {
        let outcome = bounded_arrow_ipc(
            stream_of(int_schema(), vec![int_batch(0, 100), int_batch(100, 150)]),
            250,
        )
        .await
        .expect("an exact-cap result is complete");
        assert_eq!(outcome.row_count, 250);
        let (_, decoded) = decode(&outcome.arrow_ipc);
        assert_eq!(total_rows(&decoded), 250);
    }

    /// A result over the byte cap fails outright. No partial stream comes back:
    /// a truncated Arrow IPC payload would look to the caller like a complete,
    /// smaller answer.
    #[tokio::test]
    async fn an_oversized_result_fails_without_partial_data() {
        // Twenty rows of ~512 KiB each — well past 4 MiB in total, but each one
        // fits, so the cap has to be enforced across the whole stream.
        let schema = wide_batch(1).schema();
        let batches: Vec<RecordBatch> = (0..20).map(|_| wide_batch(512 * 1024)).collect();

        let err = bounded_arrow_ipc(stream_of(schema, batches), MAX_QUERY_ROWS)
            .await
            .expect_err("an oversized result must fail");
        assert!(
            matches!(err, CommandError::ResultTooLarge { .. }),
            "an oversized result must be typed result-too-large, got {err:?}"
        );
    }

    /// A single row too large to send takes the same path — the caller cannot
    /// narrow a LIMIT out of it, so it must still be a typed refusal rather
    /// than a truncated row.
    #[tokio::test]
    async fn a_single_oversized_row_fails_the_same_way() {
        let batch = wide_batch(MAX_QUERY_RESULT_BYTES + 1024);
        let schema = batch.schema();

        let err = bounded_arrow_ipc(stream_of(schema, vec![batch]), MAX_QUERY_ROWS)
            .await
            .expect_err("an oversized row must fail");
        assert!(
            matches!(err, CommandError::ResultTooLarge { .. }),
            "an oversized row must be typed result-too-large, got {err:?}"
        );
    }

    /// The refusal names the limit and the way out, and repeats neither the
    /// query nor a value from it.
    #[test]
    fn the_oversized_message_leaks_neither_sql_nor_values() {
        let overflowed = AtomicBool::new(true);
        let message = encode_error(
            &overflowed,
            &ArrowError::IoError(
                "ignored".to_string(),
                std::io::Error::other("Cloud Connect query result limit exceeded"),
            ),
        )
        .to_string();
        assert!(message.contains("4 MiB"), "must name the limit: {message}");
        assert!(
            message.contains("LIMIT"),
            "must say how to get under it: {message}"
        );
    }

    /// A statement the engine could not parse, plan, or resolve is the caller's
    /// to fix; an execution or I/O fault is the instance's and may not recur.
    /// Collapsing both into one code leaves the portal unable to tell "your SQL
    /// is wrong" from "this instance is struggling".
    #[test]
    fn query_failures_are_classified_by_who_can_fix_them() {
        let callers = [
            DataFusionError::Plan("No field named foo".to_string()),
            DataFusionError::NotImplemented("LATERAL".to_string()),
            // The engine wraps planning errors in context/diagnostic layers;
            // classification has to see through them.
            DataFusionError::Context(
                "while planning".to_string(),
                Box::new(DataFusionError::Plan("bad".to_string())),
            ),
        ];
        for source in callers {
            assert!(
                is_caller_error(&source),
                "must blame the statement: {source}"
            );
        }

        let instances = [
            DataFusionError::Execution("scan failed".to_string()),
            DataFusionError::ResourcesExhausted("out of memory".to_string()),
            DataFusionError::Internal("bug".to_string()),
            DataFusionError::Context(
                "while scanning".to_string(),
                Box::new(DataFusionError::Execution("source down".to_string())),
            ),
        ];
        for source in instances {
            assert!(
                !is_caller_error(&source),
                "must blame the instance: {source}"
            );
        }
    }

    /// The classification has to survive the `QueryError` wrapper the runtime
    /// actually returns, not just a bare `DataFusionError`.
    #[test]
    fn a_planning_failure_reaches_the_wire_as_invalid_argument() {
        let planning = QueryError::UnableToExecuteQuery {
            source: DataFusionError::Plan("No field named foo".to_string()),
        };
        assert!(
            matches!(query_error(&planning), CommandError::InvalidArgument { .. }),
            "a planning failure is the caller's mistake"
        );

        let execution = QueryError::UnableToExecuteQuery {
            source: DataFusionError::Execution("source unreachable".to_string()),
        };
        assert!(
            matches!(query_error(&execution), CommandError::Failed { .. }),
            "an execution failure is retryable, not the caller's mistake"
        );
    }

    // ----------------------------------------------------------------------
    // The gate: what a deployment changes decides how it applies.
    // ----------------------------------------------------------------------

    /// The deployment the gate tests are changes against.
    const LIVE: &str = "\
version: v2
kind: Spicepod
name: gate
datasets:
  - from: memory:a
    name: a
";

    /// `LIVE` with `extra` appended as further top-level sections.
    fn with_sections(extra: &str) -> String {
        format!("{LIVE}{extra}")
    }

    /// Facts for a deployment arriving at an instance that is serving `live`
    /// with everything else in the settled state: no secrets delivered, load
    /// finished.
    fn arriving<'a>(live: &'a str, incoming: &'a str) -> ApplyFacts<'a> {
        ApplyFacts {
            live,
            incoming,
            secrets_changed: false,
            restart_pending: false,
            initial_load_unfinished: false,
        }
    }

    /// The case the gate exists for: a change confined to the components the
    /// runtime reconciles applies without restarting.
    #[test]
    fn a_component_only_change_applies_in_place() {
        let changes = [
            "datasets:\n  - from: memory:b\n    name: b\n",
            "views:\n  - name: v\n    sql: SELECT 1\n",
            "catalogs:\n  - from: spice.ai\n    name: c\n",
            "models:\n  - from: openai\n    name: m\n",
            "functions:\n  - name: f\n    from: https://example.com\n",
        ];
        for change in changes {
            // Appending to `LIVE` would leave two `datasets:` keys, so the
            // component sections are compared against a document holding none.
            let live = "version: v2\nkind: Spicepod\nname: gate\n";
            let incoming = format!("{live}{change}");
            assert_eq!(
                classify_deployment(&arriving(live, &incoming)),
                ApplyMode::Hot,
                "a change to `{change}` alone must not restart the instance"
            );
        }
    }

    /// Every section outside the component set is read while the process
    /// starts, so a change to one has to restart it — and the result has to
    /// name which one, because that is all the operator has to go on.
    #[test]
    fn every_start_time_section_restarts_and_is_named() {
        let changes = [
            ("dependencies", "dependencies:\n  - spiceai/quickstart\n"),
            ("embeddings", "embeddings:\n  - name: e\n    from: openai\n"),
            (
                "extensions",
                "extensions:\n  spice_cloud:\n    enabled: true\n",
            ),
            ("management", "management:\n  enabled: true\n"),
            ("metadata", "metadata:\n  team: data\n"),
            ("rerankers", "rerankers:\n  - name: r\n    from: openai\n"),
            ("runtime", "runtime:\n  dataset_load_parallelism: 2\n"),
            ("secrets", "secrets:\n  - from: env\n    name: env\n"),
            ("snapshots", "snapshots:\n  enabled: true\n"),
            ("tools", "tools:\n  - name: t\n    from: builtin\n"),
            ("workers", "workers:\n  - name: w\n    from: builtin\n"),
        ];
        for (section, change) in changes {
            let incoming = with_sections(change);
            assert_eq!(
                classify_deployment(&arriving(LIVE, &incoming)),
                ApplyMode::Cold(ColdReason::Sections(vec![section.to_string()])),
                "changing `{section}` must restart the instance and name the section"
            );
        }
    }

    /// The spicepod's own identity is start-time configuration too.
    #[test]
    fn a_renamed_or_reversioned_spicepod_restarts() {
        for (section, from, to) in [
            ("name", "name: gate", "name: renamed"),
            ("version", "version: v2", "version: v1"),
        ] {
            let incoming = LIVE.replace(from, to);
            assert_eq!(
                classify_deployment(&arriving(LIVE, &incoming)),
                ApplyMode::Cold(ColdReason::Sections(vec![section.to_string()])),
                "changing `{section}` must restart the instance"
            );
        }
    }

    /// A rotation riding along with a start-time change is named too: an
    /// operator who reverted only the sections in the document would deploy
    /// again and be restarted again by the secrets.
    #[test]
    fn a_rotation_alongside_a_start_time_change_is_named_with_it() {
        let incoming = with_sections("runtime:\n  dataset_load_parallelism: 2\n");
        let mut facts = arriving(LIVE, &incoming);
        facts.secrets_changed = true;

        let ApplyMode::Cold(reason) = classify_deployment(&facts) else {
            panic!("a start-time change must restart the instance");
        };
        assert_eq!(
            reason.sections(),
            vec!["runtime".to_string(), "secrets".to_string()]
        );
    }

    /// Reporting one of the sections that forced the restart is not enough: an
    /// operator who reverts only what was named would deploy again and be
    /// restarted again by the section that was left out.
    #[test]
    fn every_section_that_forced_the_restart_is_named() {
        let incoming = with_sections(
            "runtime:\n  dataset_load_parallelism: 2\nmetadata:\n  team: data\nviews:\n  - name: v\n    sql: SELECT 1\n",
        );

        let ApplyMode::Cold(reason) = classify_deployment(&arriving(LIVE, &incoming)) else {
            panic!("a start-time change must restart the instance");
        };
        assert_eq!(
            reason.sections(),
            vec!["metadata".to_string(), "runtime".to_string()],
            "both start-time sections must be named, and the component one must not be"
        );
        let message = reason.message();
        assert!(message.contains("metadata"), "{message}");
        assert!(message.contains("runtime"), "{message}");
    }

    /// A section written out empty configures what an absent one does, so it
    /// must not cost a restart.
    #[test]
    fn an_empty_section_is_not_a_change() {
        let incoming = with_sections("secrets: []\nruntime:\nmetadata: {}\n");
        assert_eq!(
            classify_deployment(&arriving(LIVE, &incoming)),
            ApplyMode::Hot
        );
    }

    /// Sections are compared as documents, not as text: rewriting one without
    /// changing what it configures is not a change.
    #[test]
    fn a_reordered_start_time_section_is_not_a_change() {
        let live =
            with_sections("runtime:\n  dataset_load_parallelism: 2\n  ready_state: on_load\n");
        let incoming =
            with_sections("runtime:\n  ready_state: on_load\n  dataset_load_parallelism: 2\n");
        assert_eq!(
            classify_deployment(&arriving(&live, &incoming)),
            ApplyMode::Hot
        );
    }

    /// Secret propagation is restart-only: a component resolves
    /// `${ secrets:… }` as it loads, so a rotated value reaches the components
    /// already loaded by loading them again.
    #[test]
    fn a_delivered_secret_change_restarts_an_otherwise_hot_deployment() {
        let incoming = with_sections("views:\n  - name: v\n    sql: SELECT 1\n");
        let mut facts = arriving(LIVE, &incoming);
        facts.secrets_changed = true;

        let ApplyMode::Cold(reason) = classify_deployment(&facts) else {
            panic!("a delivered-secret change must restart the instance");
        };
        assert_eq!(reason, ColdReason::DeliveredSecrets);
        assert_eq!(
            reason.sections(),
            vec!["secrets".to_string()],
            "the caller has to be able to tell what forced the restart"
        );
    }

    /// A diff against an app whose components are not all registered yet would
    /// treat the ones the load has not reached as already applied and never
    /// register them.
    #[test]
    fn an_unfinished_component_load_restarts() {
        let incoming = with_sections("views:\n  - name: v\n    sql: SELECT 1\n");
        let mut facts = arriving(LIVE, &incoming);
        facts.initial_load_unfinished = true;

        assert_eq!(
            classify_deployment(&facts),
            ApplyMode::Cold(ColdReason::InitialLoadUnfinished)
        );
    }

    /// Once this process has committed to restarting onto a deployment, a later
    /// one has to restart too: applied in place it would be persisted over the
    /// deployment the restart is for, which the restart would then not serve.
    #[test]
    fn a_deployment_arriving_after_a_restart_was_committed_restarts_too() {
        let incoming = with_sections("views:\n  - name: v\n    sql: SELECT 1\n");
        let mut facts = arriving(LIVE, &incoming);
        facts.restart_pending = true;

        assert_eq!(
            classify_deployment(&facts),
            ApplyMode::Cold(ColdReason::RestartPending)
        );
    }

    /// An instance serving a local spicepod, or one whose deployment failed to
    /// build, has nothing to diff the incoming deployment against.
    #[test]
    fn an_instance_serving_no_deployment_restarts() {
        assert_eq!(
            classify_deployment(&arriving("", LIVE)),
            ApplyMode::Cold(ColdReason::NoLiveDeployment)
        );
    }

    /// Input the gate cannot read must never be applied in place. Validation
    /// rejects it, but the gate runs first — and a gate that answered "nothing
    /// start-time changed" for a document it failed to parse would answer it
    /// for every document it failed to parse.
    #[test]
    fn an_unreadable_deployment_is_never_applied_in_place() {
        for incoming in [INVALID_SPICEPOD, "", "a scalar", "- a\n- b\n", "\u{fffd}"] {
            assert!(
                matches!(
                    classify_deployment(&arriving(LIVE, incoming)),
                    ApplyMode::Cold(_)
                ),
                "a document the gate cannot read must restart the instance: {incoming:?}"
            );
        }
    }

    // ----------------------------------------------------------------------
    // Applying a deployment: what the process serves afterwards.
    // ----------------------------------------------------------------------

    /// The deployment a handle under test starts out serving.
    const SERVING: &str = "\
version: v2
kind: Spicepod
name: applied
views:
  - name: served_view
    sql: SELECT 1 AS n
";

    /// `SERVING` plus a second view — a change the runtime reconciles.
    const SERVING_PLUS_VIEW: &str = "\
version: v2
kind: Spicepod
name: applied
views:
  - name: served_view
    sql: SELECT 1 AS n
  - name: deployed_view
    sql: SELECT 2 AS n
";

    /// A handle over a runtime that has finished loading `live`, which is the
    /// state an instance is in between deployments.
    async fn handle_serving(dir: &Path, live: &str) -> Arc<SpicedRuntimeHandle> {
        let handle = handle_loading(dir, live).await;
        tokio::time::timeout(
            Duration::from_mins(2),
            Arc::clone(&handle.runtime).load_components(),
        )
        .await
        .expect("the runtime finishes loading its components");
        handle
    }

    /// A handle over a runtime that has not run its component load, which is
    /// the state an instance is in from the moment Cloud Connect connects until
    /// the load finishes.
    async fn handle_loading(dir: &Path, live: &str) -> Arc<SpicedRuntimeHandle> {
        let path = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
        std::fs::write(&path, live).expect("write the live deployment");
        let app = AppBuilder::build_from_path(path.clone())
            .await
            .expect("the live deployment builds");

        let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

        Arc::new(SpicedRuntimeHandle::new(SpicedRuntimeHandleParts {
            runtime,
            logs: None,
            delivered_secrets: Arc::new(CloudDeliveredSecretStore::new()),
            identity_path: dir.join(IDENTITY_FILE),
            running_deployment: Some(CloudManagedSpicepod {
                path,
                spicepod_yaml: live.to_string(),
            }),
            supervisor: Supervisor::detect(),
            metrics: None,
            app_id: None,
            runtime_overrides: Vec::new(),
        }))
    }

    fn deployment<'a>(config_dir: &'a Path, spicepod_yaml: &'a str) -> SpicepodDeployment<'a> {
        SpicepodDeployment {
            config_dir,
            spicepod_yaml,
            delivered_secrets: None,
            app_id: None,
        }
    }

    /// One delivered secret, for the tests that rotate one.
    fn delivered(name: &str, value: &[u8]) -> DeliveredSecrets {
        [(name.to_string(), Zeroizing::new(value.to_vec()))]
            .into_iter()
            .collect()
    }

    /// Give the instance an identity carrying a cache key, which is what makes
    /// the delivered-secrets cache writable — without one the cache is skipped
    /// and a test asserting on it would pass for the wrong reason.
    fn enrol_with_a_cache_key(identity_path: &Path) {
        let mock_pem = "-----BEGIN PRIVATE KEY-----\nMOCK\n-----END PRIVATE KEY-----\n".to_string();
        let mut identity = runtime_cloud_connect::identity::Identity {
            identifier: "inst_test".to_string(),
            identity_cert_pem: "-----BEGIN CERTIFICATE-----\nMOCK\n-----END CERTIFICATE-----\n"
                .to_string(),
            private_key_pem: mock_pem.clone(),
            public_key_pem: "-----BEGIN PUBLIC KEY-----\nMOCK\n-----END PUBLIC KEY-----\n"
                .to_string(),
            ca_bundle_pem: String::new(),
            gateway_addr: "gateway.test.spice.ai:443".to_string(),
            not_after_unix: None,
            app_id: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
            enc_private_key_pem: mock_pem,
            enc_public_key_pem: "-----BEGIN PUBLIC KEY-----\nMOCKENC\n-----END PUBLIC KEY-----\n"
                .to_string(),
            enc_previous_private_key_pem: String::new(),
            cache_key_b64: String::new(),
        };
        assert!(
            identity.ensure_cache_key(),
            "the identity mints a cache key"
        );
        IdentityStore::store(identity_path, &identity).expect("write the identity");
    }

    /// What the delivered-secrets cache holds, read back the way a restart
    /// reads it. `None` when nothing has been cached.
    fn cached_secrets(dir: &Path) -> Option<CloudDeliveredSecretStore> {
        let key = IdentityStore::load_optional(&dir.join(IDENTITY_FILE))
            .expect("read the identity")?
            .cache_key()?;
        let cached = runtime_cloud_connect::secret_cache::read(
            &dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE),
            &key,
        )
        .expect("the cache is readable")?;
        let store = CloudDeliveredSecretStore::new();
        store.replace(cached.into_values());
        Some(store)
    }

    /// How long a test waits for a component to finish converging.
    const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);

    /// Count the rows `sql` answers with, waiting for the component it reads to
    /// converge.
    ///
    /// The runtime registers a view in a task of its own, so a component the
    /// apply reconciled is not necessarily registered by the time the apply
    /// returns. Polling is what a caller does; asserting on the first attempt
    /// would make this a race rather than a test.
    async fn query_rows(runtime: &Runtime, sql: &str) -> usize {
        let start = tokio::time::Instant::now();
        loop {
            let last = match runtime.datafusion().query_builder(sql).build().run().await {
                Ok(result) => {
                    let batches: Vec<RecordBatch> =
                        result.data.try_collect().await.expect("the query streams");
                    return total_rows(&batches);
                }
                Err(err) => err.to_string(),
            };
            assert!(
                start.elapsed() < CONVERGE_TIMEOUT,
                "`{sql}` did not answer within {CONVERGE_TIMEOUT:?}: {last}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn view_names(app: &App) -> Vec<String> {
        app.views.iter().map(|view| view.name.clone()).collect()
    }

    async fn live_view_names(handle: &SpicedRuntimeHandle) -> Vec<String> {
        view_names(&handle.runtime.read_app().await.expect("an app is loaded"))
    }

    fn persisted_spicepod(dir: &Path) -> String {
        std::fs::read_to_string(dir.join(CLOUD_MANAGED_SPICEPOD_FILE))
            .expect("the canonical spicepod exists")
    }

    /// The whole point: a component-only deployment is served by this process,
    /// not by the next one. The query is what proves it — an assertion that the
    /// runtime is still up would pass for a deployment that was ignored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_component_only_deployment_is_applied_in_place_and_queryable() {
        let dir = scratch_dir("hot-apply");
        let handle = handle_serving(&dir, SERVING).await;

        let outcome = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("a component-only deployment applies");

        assert_eq!(
            outcome.post_apply,
            PostApply::Nothing,
            "a component-only deployment must not exit the process"
        );
        assert_eq!(outcome.document["live"], serde_json::json!(true));
        assert_eq!(
            outcome.document["restart"],
            serde_json::json!("not_required")
        );
        assert_eq!(outcome.document["views"], serde_json::json!(2));

        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1,
            "the deployed view must answer in this process"
        );
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view", "deployed_view"]
        );
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(*handle.live.read(), SERVING_PLUS_VIEW);
        assert!(
            handle.persisted.read().is_none(),
            "nothing is waiting on a restart"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A start-time change persists and restarts, and says which section made
    /// it restart — the instance keeps serving what it was serving until it
    /// does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_start_time_deployment_persists_and_restarts_naming_the_section() {
        let dir = scratch_dir("cold-apply");
        let handle = handle_serving(&dir, SERVING).await;
        let incoming = format!("{SERVING_PLUS_VIEW}runtime:\n  dataset_load_parallelism: 2\n");

        let outcome = handle
            .apply_spicepod(deployment(&dir, &incoming))
            .await
            .expect("a start-time deployment persists");

        assert_eq!(
            outcome.post_apply,
            PostApply::ExitToApply,
            "a start-time change takes effect by restarting"
        );
        assert_eq!(outcome.document["live"], serde_json::json!(false));
        assert_eq!(outcome.document["restart"], serde_json::json!("required"));
        assert_eq!(
            outcome.document["restart_sections"],
            serde_json::json!(["runtime"])
        );

        assert_eq!(persisted_spicepod(&dir), incoming, "it is persisted");
        assert_eq!(
            *handle.live.read(),
            SERVING,
            "and not live: this process still serves what it was serving"
        );
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view"],
            "the deployment must not have been half-applied on the way to the restart"
        );

        // A component-only deployment arriving in the window before the process
        // exits cannot be applied in place either: this process is on its way
        // out and would not be the one serving it. It is persisted, so the
        // restart comes up on the newest deployment rather than the one it
        // superseded.
        let follow_on = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("a deployment arriving before the restart is answered");
        assert_eq!(follow_on.post_apply, PostApply::ExitToApply);
        assert_eq!(live_view_names(&handle).await, vec!["served_view"]);
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(
            handle.persisted.read().as_deref(),
            Some(SERVING_PLUS_VIEW),
            "the restart is for the deployment that will be on disk when it happens"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The next deployment is diffed against what this process serves now. A
    /// gate still reading the boot-time spicepod would answer a redelivery of
    /// the deployment it just applied as a new one, and the one it replaced as
    /// already live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_next_deployment_is_diffed_against_the_one_now_serving() {
        let dir = scratch_dir("hot-apply-twice");
        let handle = handle_serving(&dir, SERVING).await;

        handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("the first deployment applies");
        // Reverting is only a test of the revert once the view it removes has
        // finished converging.
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1
        );

        let redelivered = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("a redelivery is answered");
        assert_eq!(redelivered.post_apply, PostApply::Nothing);
        assert_eq!(
            redelivered.document["message"],
            serde_json::json!(
                "This spicepod is already applied and live; this instance is serving it."
            ),
            "the deployment this process now serves is a no-op, not another apply"
        );

        // And the spicepod it replaced is a new deployment, not the live one.
        let reverted = handle
            .apply_spicepod(deployment(&dir, SERVING))
            .await
            .expect("the previous deployment applies again");
        assert_eq!(reverted.post_apply, PostApply::Nothing);
        assert_eq!(reverted.document["live"], serde_json::json!(true));
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view"],
            "reverting must actually remove the view the first deployment added"
        );
        assert!(
            handle
                .runtime
                .datafusion()
                .query_builder("SELECT n FROM deployed_view")
                .build()
                .run()
                .await
                .is_err(),
            "the removed view must stop answering"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deployment arriving before the instance has finished loading restarts
    /// it. The load is still registering the components of the app being
    /// replaced, so reconciling only what changed would leave the two of them
    /// writing the same tables.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deployment_arriving_before_the_load_finishes_restarts() {
        let dir = scratch_dir("loading-apply");
        let handle = handle_loading(&dir, SERVING).await;
        assert!(
            handle.runtime.initial_load_in_flight(),
            "the runtime under test has not loaded its components"
        );

        let outcome = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("a deployment arriving during the load is answered");

        assert_eq!(
            outcome.post_apply,
            PostApply::ExitToApply,
            "a component-only deployment still restarts an instance that is still loading"
        );
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(*handle.live.read(), SERVING);
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view"],
            "nothing was reconciled into the app the load is still installing"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deployment that does not build changes nothing: not the file this
    /// instance starts on, not what it is serving, not what it reports as live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_deployment_changes_neither_the_persisted_nor_the_live_spicepod() {
        let dir = scratch_dir("malformed-apply");
        let handle = handle_serving(&dir, SERVING).await;

        let err = handle
            .apply_spicepod(deployment(&dir, INVALID_SPICEPOD))
            .await
            .expect_err("a malformed deployment is refused");
        assert!(
            matches!(err, CommandError::InvalidArgument { .. }),
            "a malformed push is the caller's mistake: {err}"
        );

        assert_eq!(persisted_spicepod(&dir), SERVING);
        assert_eq!(*handle.live.read(), SERVING);
        assert!(handle.persisted.read().is_none());
        assert_eq!(live_view_names(&handle).await, vec!["served_view"]);
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM served_view").await,
            1,
            "the instance keeps serving what it was serving"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deployment's secrets are installed only once its spicepod validates,
    /// so a malformed push leaves the instance resolving what it was resolving
    /// and leaves nothing behind for the restart it is not taking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_deployment_leaves_the_delivered_secrets_alone() {
        let dir = scratch_dir("malformed-secrets");
        let handle = handle_serving(&dir, SERVING).await;
        handle
            .delivered_secrets
            .replace(delivered("api_key", b"value-one"));

        handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: INVALID_SPICEPOD,
                delivered_secrets: Some(delivered("api_key", b"value-two")),
                app_id: None,
            })
            .await
            .expect_err("a malformed deployment is refused");

        assert!(
            handle
                .delivered_secrets
                .holds(&delivered("api_key", b"value-one")),
            "the rejected deployment's secrets must not have stayed installed"
        );
        assert!(
            !dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE)
                .exists(),
            "nothing is cached for a restart that is not happening"
        );
        assert_eq!(persisted_spicepod(&dir), SERVING);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache is what a restart resolves its secrets from, so a deployment
    /// that does not build must leave it holding the values the instance is
    /// still serving on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_deployment_leaves_the_cached_secrets_alone() {
        let dir = scratch_dir("malformed-cache");
        let handle = handle_serving(&dir, SERVING).await;
        enrol_with_a_cache_key(&dir.join(IDENTITY_FILE));

        handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("the first deployment applies");
        assert!(
            cached_secrets(&dir)
                .expect("the applied deployment cached its secrets")
                .holds(&delivered("api_key", b"value-one"))
        );

        handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: INVALID_SPICEPOD,
                delivered_secrets: Some(delivered("api_key", b"value-two")),
                app_id: None,
            })
            .await
            .expect_err("a malformed deployment is refused");

        assert!(
            cached_secrets(&dir)
                .expect("the cache is still there")
                .holds(&delivered("api_key", b"value-one")),
            "a rejected deployment must not leave its secrets for the next start"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A cache write fails for its own reasons — a full disk, a permission —
    /// and the instance keeps running on secrets it holds in memory. The
    /// redelivery that follows is the last chance to repair it before a restart
    /// needs it, so it must not be answered as a pure no-op.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_redelivery_repairs_a_cache_an_earlier_deployment_did_not_write() {
        let dir = scratch_dir("cache-repair");
        let handle = handle_serving(&dir, SERVING).await;
        // The state a failed cache write leaves: installed in memory, absent
        // from the cache the next start reads.
        handle
            .delivered_secrets
            .replace(delivered("api_key", b"value-one"));
        enrol_with_a_cache_key(&dir.join(IDENTITY_FILE));
        assert!(cached_secrets(&dir).is_none(), "nothing is cached yet");

        let outcome = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("a redelivery of the live deployment is answered");

        assert_eq!(outcome.post_apply, PostApply::Nothing);
        assert_eq!(
            outcome.document["secrets_cache_error"],
            serde_json::Value::Null,
            "the repair is reported as having succeeded"
        );
        assert!(
            cached_secrets(&dir)
                .expect("the redelivery wrote the cache")
                .holds(&delivered("api_key", b"value-one")),
            "the restart this instance may take must find its secrets"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Delivered secrets reach a component as it loads, so a rotation restarts
    /// the instance however small the spicepod change is — and a redelivery of
    /// the values it already holds does not.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rotated_delivered_secret_restarts_and_a_redelivered_one_does_not() {
        let dir = scratch_dir("secret-rotation");
        let handle = handle_serving(&dir, SERVING).await;
        handle
            .delivered_secrets
            .replace(delivered("api_key", b"value-one"));

        let redelivered = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("a deployment redelivering the installed secrets applies");
        assert_eq!(
            redelivered.post_apply,
            PostApply::Nothing,
            "secrets this instance already holds are not a change"
        );
        assert_eq!(
            redelivered.document["delivered_secrets"],
            serde_json::json!(["api_key"]),
            "names only, and the names the deployment carried"
        );

        let rotated = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING,
                delivered_secrets: Some(delivered("api_key", b"value-two")),
                app_id: None,
            })
            .await
            .expect("a rotation applies");
        assert_eq!(
            rotated.post_apply,
            PostApply::ExitToApply,
            "a rotated value reaches loaded components by loading them again"
        );
        assert_eq!(
            rotated.document["restart_sections"],
            serde_json::json!(["secrets"])
        );
        for document in [&redelivered.document, &rotated.document] {
            let rendered = document.to_string();
            assert!(
                !rendered.contains("value-one") && !rendered.contains("value-two"),
                "a delivered value must never reach the command result: {rendered}"
            );
        }

        // A rotation carrying the spicepod this instance already serves is
        // still a rotation: answering it as already applied would report
        // success for values that never reached the app.
        let unchanged_spicepod = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING,
                delivered_secrets: Some(delivered("api_key", b"value-three")),
                app_id: None,
            })
            .await
            .expect("a rotation with an unchanged spicepod applies");
        assert_eq!(unchanged_spicepod.post_apply, PostApply::ExitToApply);
        assert_eq!(
            unchanged_spicepod.document["restart_sections"],
            serde_json::json!(["secrets"])
        );
        assert!(
            handle
                .delivered_secrets
                .holds(&delivered("api_key", b"value-three")),
            "the rotated values are installed for the restart to come up on"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The decision and the state it commits to are taken together. Two
    /// dispatches of one deployment must apply it once: a second that read the
    /// live spicepod before the first replaced it would apply it again, on top
    /// of itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_deliveries_of_one_deployment_apply_it_once() {
        let dir = scratch_dir("concurrent-same");
        let handle = handle_serving(&dir, SERVING).await;

        let dispatches = 4;
        let start = Arc::new(tokio::sync::Barrier::new(dispatches));
        let mut tasks = Vec::with_capacity(dispatches);
        for _ in 0..dispatches {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let dir = dir.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                handle
                    .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
                    .await
                    .expect("every dispatch is answered")
            }));
        }

        let mut applied = 0;
        for task in tasks {
            let outcome = task.await.expect("the dispatch finishes");
            assert_eq!(
                outcome.post_apply,
                PostApply::Nothing,
                "a component-only deployment never exits the process"
            );
            // Only an apply reports what it applied; the answer for a
            // deployment already being served carries no component counts.
            if outcome.document.get("views").is_some() {
                applied += 1;
            }
        }
        assert_eq!(applied, 1, "the deployment must be applied exactly once");

        assert_eq!(*handle.live.read(), SERVING_PLUS_VIEW);
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view", "deployed_view"]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Deployments arriving together leave one state: the file this instance
    /// starts on, the spicepod it reports as live, and the app it is serving
    /// all describe the same deployment. Interleaving them could persist one,
    /// serve another, and diff the next against a third.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_deployments_leave_one_consistent_state() {
        let dir = scratch_dir("concurrent-distinct");
        let handle = handle_serving(&dir, SERVING).await;

        let deployments: Vec<String> = ["alpha", "beta", "gamma", "delta"]
            .into_iter()
            .map(|name| format!("{SERVING}  - name: view_{name}\n    sql: SELECT 3 AS n\n"))
            .collect();
        let start = Arc::new(tokio::sync::Barrier::new(deployments.len()));
        let mut tasks = Vec::with_capacity(deployments.len());
        for spicepod in deployments {
            let handle = Arc::clone(&handle);
            let start = Arc::clone(&start);
            let dir = dir.clone();
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                handle
                    .apply_spicepod(deployment(&dir, &spicepod))
                    .await
                    .expect("every dispatch is answered");
            }));
        }
        for task in tasks {
            task.await.expect("the dispatch finishes");
        }

        let live = handle.live.read().clone();
        assert_eq!(
            persisted_spicepod(&dir),
            live,
            "the file this instance starts on is the deployment it reports as live"
        );
        let views = live_view_names(&handle).await;
        assert_eq!(
            views.len(),
            2,
            "one deployment's views, not several: {views:?}"
        );
        // Exactly the deployment that is live, and none of the three it raced.
        for name in ["view_alpha", "view_beta", "view_gamma", "view_delta"] {
            assert_eq!(
                live.contains(name),
                views[1] == name,
                "the app being served is the deployment reported as live: {views:?}"
            );
        }
        assert!(
            handle.persisted.read().is_none(),
            "no restart was committed for a component-only deployment"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A result that fits must never be misreported as oversized.
    #[tokio::test]
    async fn a_result_just_under_the_cap_still_sends() {
        let batch = wide_batch(MAX_QUERY_RESULT_BYTES / 2);
        let schema = batch.schema();

        let outcome = bounded_arrow_ipc(stream_of(schema, vec![batch]), MAX_QUERY_ROWS)
            .await
            .expect("a result under the cap must send");
        assert_eq!(outcome.row_count, 1);
        assert!(outcome.arrow_ipc.len() <= MAX_QUERY_RESULT_BYTES);
        let (_, batches) = decode(&outcome.arrow_ipc);
        assert_eq!(total_rows(&batches), 1);
    }
}
