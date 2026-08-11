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
//! standalone `spiced` can be discovered, enrolled, and managed by a
//! Spice Cloud control plane.
//!
//! ## Opt-in semantics
//!
//! `CloudConnect` is **disabled by default**. It activates only if one of
//! the following is true at boot:
//!
//! 1. `--token <enrollment-key>` was passed — [`bootstrap_enrollment`]
//!    then enrolls this instance *before the runtime is built or any
//!    listener binds*, so the durable identity exists by the time anything
//!    else here runs.
//! 2. `$SPICE_CONFIG_DIR/identity.json` exists (a previously enrolled
//!    instance) — reconnection is automatic, with no flag.
//!
//! If neither is true, this module never opens a connection. An existing
//! identity always wins over a supplied `--token`: the key is not redeemed
//! and nothing about it is persisted.
//!
//! ## How a deployment applies
//!
//! By restarting, and only by restarting. `apply_spicepod` validates the
//! incoming spicepod, persists it as `spicepod-cloud-managed.yml`, records the
//! deployment version beside it, and exits 0 — the supervisor relaunches
//! `spiced`, [`cloud_managed_spicepod`] hands the persisted file to the app
//! builder, and the instance comes up serving it and reporting its version.
//!
//! Two consequences the caller has to hold:
//!
//! - **The command result may never arrive.** The process exits mid-command, so
//!   the stream drops before the result is guaranteed to land — a caller cannot
//!   treat its absence as a failed deployment.
//! - **Downtime is proportional to the app's size, not the change's.** A
//!   one-line edit reloads every dataset and rebuilds every acceleration.
//!
//! What this buys is one behaviour instead of two: no section that reconciles
//! in-process while another waits for an operator, and no partially-applied
//! state that is neither the old configuration nor the new one.

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
    CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig, IDENTITY_FILE,
};
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, MAX_QUERY_RESULT_BYTES, QueryOutcome, RuntimeHandle,
    RuntimePhase, SpicepodDeployment, StatusReport, effective_max_rows,
};
use runtime_cloud_connect::supervisor::Supervisor;
use runtime_cloud_connect::{CloudConnect, identity::IdentityStore};
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
/// instance, using the same signals as [`maybe_start`]: a `--token`
/// enrollment bootstrap in progress, or an on-disk identity.
///
/// Called from `init_tracing` (before [`maybe_start`]) to decide whether to
/// install the log-capture layer. It runs in the same process — hence the
/// same working directory — as [`maybe_start`], so both resolve the config
/// directory identically. This is a lightweight existence check; it does not
/// read or validate the file (that happens in `maybe_start`).
pub(crate) fn is_configured(token_supplied: bool) -> bool {
    if token_supplied {
        return true;
    }
    CloudConnectConfig::default_config_dir()
        .join(IDENTITY_FILE)
        .exists()
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

/// Why a `--token` bootstrap could not produce a durable identity. Every
/// message is safe to print: none can contain the enrollment key.
#[derive(Debug, snafu::Snafu)]
pub enum BootstrapEnrollmentError {
    #[snafu(display(
        "Failed to enroll this instance with Spice Cloud: the --token value was rejected \
         before any request was made. {source}"
    ))]
    InvalidKey {
        source: runtime_cloud_connect::InvalidEnrollmentKey,
    },

    #[snafu(display(
        "Failed to enroll this instance with Spice Cloud: the --region value {region:?} is not \
         a valid region label. Expected 2-64 lowercase letters, digits, or hyphens (for example \
         'us-west-2' or 'on-prem-syd'). See: https://spiceai.org/docs"
    ))]
    InvalidRegion { region: String },

    #[snafu(display("Failed to enroll this instance with Spice Cloud ({endpoint}): {source}"))]
    Enroll {
        endpoint: String,
        source: runtime_cloud_connect::EnrollNowError,
    },
}

/// Redeem a `--token` enrollment key: the pre-runtime Cloud Connect
/// bootstrap.
///
/// Called from `main` **before the runtime is built, any listener binds, or
/// readiness can be reported** — a failed enrollment therefore leaves the
/// process with no port bound and nothing an orchestrator could route
/// traffic to. Retryable failures are retried for up to the headless
/// ten-minute budget while the process stays unready; terminal failures
/// return an error the caller exits `1` on.
///
/// No-op without `--token`. An existing valid identity wins: the key is not
/// redeemed, nothing about it is persisted, and the runtime reconnects from
/// the identity as it would have without the flag.
///
/// # Errors
///
/// Returns [`BootstrapEnrollmentError`] when the key or region is malformed
/// (checked locally, never echoed) or when [`enroll_now`] fails terminally
/// or exhausts its retry budget.
pub async fn bootstrap_enrollment(args: &mut crate::Args) -> Result<(), BootstrapEnrollmentError> {
    use runtime_cloud_connect::{
        EnrollNowOutcome, EnrollmentAuthority, EnrollmentKey, RetryPolicy,
    };

    let Some(raw_key) = args.token.take() else {
        return Ok(());
    };

    let key = EnrollmentKey::parse(raw_key.expose_secret())
        .map_err(|source| BootstrapEnrollmentError::InvalidKey { source })?;
    // The canonical wrapper owns a zeroizing copy now. Drop the raw clap
    // value immediately so it cannot live inside `Args` for the runtime's
    // entire process lifetime.
    drop(raw_key);

    // Validated before anything else so a typo fails fast — even when an
    // existing identity would win and the region would go unused.
    if let Some(region) = args.region.as_deref()
        && !runtime_cloud_connect::is_valid_instance_region(region)
    {
        return Err(BootstrapEnrollmentError::InvalidRegion {
            region: region.to_string(),
        });
    }

    let mut config = build_config(env!("CARGO_PKG_VERSION"));
    config.instance_region = args.region.clone();

    let authority = EnrollmentAuthority::Token {
        key,
        expected_org: None,
    };
    let outcome = runtime_cloud_connect::enroll_now(&config, &authority, RetryPolicy::HEADLESS)
        .await
        .map_err(|source| BootstrapEnrollmentError::Enroll {
            endpoint: config.enroll_endpoint.clone(),
            source,
        })?;

    match outcome {
        EnrollNowOutcome::AlreadyEnrolled { identity } => {
            tracing::info!(
                "Spice Cloud Connect: this instance is already enrolled as {} (identity at {}); \
                 the supplied enrollment key was NOT redeemed and can be used elsewhere or revoked",
                identity.identifier,
                config.identity_path.display()
            );
        }
        EnrollNowOutcome::Enrolled { identity, metadata } => {
            tracing::info!(
                "Spice Cloud Connect: enrolled as {} in organization {}{} (identity stored at {})",
                identity.identifier,
                metadata.organization.name,
                metadata
                    .region
                    .as_deref()
                    .map(|region| format!(", region {region}"))
                    .unwrap_or_default(),
                config.identity_path.display()
            );
            if let Some(url) = metadata.new_project_url.as_deref() {
                tracing::info!(
                    "Spice Cloud Connect: this instance is not yet attached to a project. Create one: {url}"
                );
            }
        }
    }
    Ok(())
}

/// The spicepod a deployment persisted, and the deployment it belongs to.
///
/// A deployment applies by persisting this file and restarting, so on every
/// start it — not the instance directory's `spicepod.yaml` — is the
/// configuration a cloud-managed instance serves.
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
    token_supplied: bool,
) -> std::result::Result<Option<CloudManagedSpicepod>, CloudManagedSpicepodReadError> {
    if !is_configured(token_supplied) {
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
    token_supplied: bool,
) -> Option<DeliveredSecretsState> {
    if !is_configured(token_supplied) {
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

/// Start the Cloud Connect client when this instance holds an enrolled
/// identity. The returned `Option<CloudConnect>` is `None` when
/// `CloudConnect` is disabled — which is the default for vanilla OSS
/// installs.
///
/// Activation is identity-driven and needs no flag: a `--token` bootstrap
/// persisted the identity before the runtime was built ([`bootstrap_enrollment`]),
/// and a previously enrolled instance reconnects from the identity alone.
///
/// `running_deployment` is the cloud-managed spicepod the runtime actually
/// loaded, or `None` when it is serving something else (a local spicepod, or a
/// deployed one that failed to build). It is what a redelivered `ApplySpicepod`
/// is compared against, so passing a configuration that is not live would let a
/// redelivery be answered as already applied when it is not.
pub async fn maybe_start(
    runtime_version: &str,
    runtime: Arc<Runtime>,
    delivered_secrets: Option<DeliveredSecretsState>,
    running_deployment: Option<CloudManagedSpicepod>,
    metrics: Option<MetricsReader>,
) -> Option<CloudConnect> {
    let config = build_config(runtime_version);

    // Quick sanity probe — no identity means disabled. Surface a load/parse
    // error (corrupt or unreadable identity.json) rather than silently
    // treating it as "not enrolled", so a broken identity file is visible
    // to the operator instead of quietly disabling Cloud Connect.
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
                 treating as not-enrolled — fix or remove the file to re-enroll",
                config.identity_path.display()
            );
            false
        }
    };
    if !has_identity {
        tracing::debug!(
            "Spice Cloud Connect: disabled (no identity at {})",
            config.identity_path.display()
        );
        return None;
    }

    tracing::info!(
        "Spice Cloud Connect: enabled, enroll_endpoint={}",
        config.enroll_endpoint,
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
            "Spice Cloud Connect: process supervisor detected ({}); a deployment applies by restarting spiced",
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
/// new one.
fn disposition(live: &str, persisted: Option<&str>, incoming: &str) -> Disposition {
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
    /// The deployment this process is serving. Fixed for the life of the
    /// process: a deployment takes effect by restarting, so nothing can change
    /// what is live without starting a new one.
    ///
    /// A runtime serving a local spicepod holds an empty string, which matches
    /// no incoming deployment.
    live: String,
    /// A deployment this process validated and persisted, waiting on the exit
    /// that makes it live. Read on every `Hello`, written once per apply — a
    /// `parking_lot` lock held for the read/write only, never across an
    /// `.await`.
    persisted: RwLock<Option<String>>,
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
        } = parts;
        let live = running_deployment.map_or_else(String::new, |running| running.spicepod_yaml);
        Self {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            live,
            persisted: RwLock::new(None),
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

    async fn persist_attachment(&self, app_id: Option<&str>) -> Result<(), CommandError> {
        let path = self.identity_path.clone();
        let app_id = app_id.map(str::to_string);
        let result = tokio::task::spawn_blocking(move || {
            IdentityStore::set_app_id(&path, app_id.as_deref())
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
        if !result {
            return Err(CommandError::failed(
                "Failed to save the cloud app attachment because the Cloud Connect identity is missing. Reconnect the instance and retry.",
            ));
        }
        Ok(())
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

    /// Persist the delivered secrets so the restart every deployment performs
    /// comes back up with them, without a control-plane round trip.
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
            Capability::Restart => "Restart is unsupported on standalone spiced: it is not a control the runtime offers on demand. A deployment already applies by restarting this instance onto the spicepod it validated; to restart it without deploying, use your process manager (systemd/Docker/Kubernetes). See: https://spiceai.org/docs".to_string(),
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

    /// Apply a cloud-managed spicepod by persisting it and restarting onto it.
    ///
    /// 1. A redelivery of the spicepod this process is already serving — the
    ///    same YAML, byte for byte — is answered as applied and changes nothing,
    ///    not even the delivered secrets. Restarting for it would make a
    ///    redelivery a restart loop, so a change to an app's secrets has to
    ///    arrive with a changed spicepod rather than as a re-send of the current
    ///    one.
    /// 2. Delivered secrets are installed and cached first. The restart is what
    ///    makes the spicepod live, and the components that resolve
    ///    `${ secrets:… }` run during that start, so the cache — not this
    ///    process's in-memory store — is what has to hold them by then.
    /// 3. The YAML is validated by building an [`App`] from it on a sibling
    ///    temp file, so a malformed push is rejected with a clear error and the
    ///    previous good `spicepod-cloud-managed.yml` is left untouched and
    ///    still running.
    /// 4. The validated file is promoted to the canonical path and the
    ///    deployment version recorded beside it, so the next start comes up on
    ///    this configuration and reports this version.
    /// 5. The client sends this result and then calls
    ///    [`RuntimeHandle::exit_to_apply`], which drains and exits 0 for the
    ///    supervisor to relaunch.
    ///
    /// There is deliberately no hot-reload path. Reconciling part of a spicepod
    /// in-process and leaving the rest (`runtime:`, `secrets:`, embeddings,
    /// tools) for an operator to restart for meant two behaviours to reason
    /// about and a partially-applied state that was neither. The cost is that
    /// downtime is proportional to the app's size rather than the change's —
    /// every deployment is a full start, including acceleration rebuilds.
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

        // Cloned out of the lock rather than compared under it: the guard must
        // not be held across the awaits below.
        let persisted = self.persisted.read().clone();
        match disposition(&self.live, persisted.as_deref(), spicepod_yaml) {
            Disposition::AlreadyLive => {
                tracing::info!(
                    "Spice Cloud Connect: the deployed spicepod is already applied; nothing to do"
                );
                return Ok(ApplyOutcome::settled(serde_json::json!({
                    "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                    "applied": true,
                    "live": true,
                    "restart": "not_required",
                    "message": "This spicepod is already applied and live; this instance is serving it.",
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
                })));
            }
            Disposition::Apply => {}
        }

        // Install and cache the delivered secrets BEFORE the spicepod is
        // validated: `AppBuilder::build_from_path` resolves `${ secrets:… }`
        // references, so secrets installed afterwards would arrive after
        // validation had already failed on them.
        let mut cache_error = None;
        let delivered_names = match delivered_secrets {
            None => None,
            Some(secrets) => {
                let names: Vec<String> = secrets.keys().cloned().collect();
                cache_error = self.cache_delivered_secrets(config_dir, &secrets);
                // Replaces the whole set: an app whose secrets were removed
                // must stop resolving them.
                self.delivered_secrets.replace(secrets);
                Some(names)
            }
        };

        let (new_app, path) = stage_cloud_managed_spicepod(config_dir, spicepod_yaml).await?;

        *self.persisted.write() = Some(spicepod_yaml.to_string());

        tracing::info!(
            "Spice Cloud Connect: the deployed spicepod was validated and persisted to {} ({} datasets, {} models, {} catalogs, {} views); restarting to apply it",
            path.display(),
            new_app.datasets.len(),
            new_app.models.len(),
            new_app.catalogs.len(),
            new_app.views.len(),
        );

        Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": false,
            "restart": "required",
            "supervised": self.supervisor.is_supervised(),
            "supervisor": self.supervisor.as_str(),
            "message": "The spicepod was validated and persisted. This instance is restarting to serve it; the restart reloads every dataset, so the app is unavailable until it finishes.",
            "datasets": new_app.datasets.len(),
            "models": new_app.models.len(),
            "catalogs": new_app.catalogs.len(),
            "views": new_app.views.len(),
            // Names only — a delivered value never leaves this process.
            "delivered_secrets": delivered_names,
            "secrets_cache_error": cache_error,
        })))
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

    async fn attach_app(&self, app_id: Option<&str>) -> Result<serde_json::Value, CommandError> {
        self.persist_attachment(app_id).await?;
        *self.app_id.write() = app_id.map(str::to_string);
        Ok(serde_json::json!({ "app_id": app_id }))
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

/// Validate a cloud-managed spicepod and persist it to disk.
///
/// Writes `spicepod_yaml` to a sibling `*.incoming.yml` temp file, builds an
/// [`App`] from it to validate (parse + resolve), and only on success
/// atomically promotes the temp file to the canonical
/// [`CLOUD_MANAGED_SPICEPOD_FILE`] path. On any failure the canonical file is
/// left untouched — the instance keeps serving it — and the temp file is
/// cleaned up. Returns the built `App`, which the caller reads counts off for
/// the command result, and the canonical path it was written to.
///
/// Factored out of [`SpicedRuntimeHandle::apply_spicepod`] so the
/// file-staging + validation can be unit-tested without a running runtime.
/// A malformed push is the operator's mistake, not the runtime's, so it is
/// reported as [`CommandError::InvalidArgument`]; a filesystem failure is
/// something the runtime may recover from, so it is [`CommandError::Failed`].
async fn stage_cloud_managed_spicepod(
    config_dir: &Path,
    spicepod_yaml: &str,
) -> Result<(App, std::path::PathBuf), CommandError> {
    let path = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|e| CommandError::failed(format!("create config dir: {e}")))?;

    // Validate on a temp file first so a bad push never clobbers the last
    // known-good spicepod on disk.
    let incoming = config_dir.join("spicepod-cloud-managed.incoming.yml");
    tokio::fs::write(&incoming, spicepod_yaml)
        .await
        .map_err(|e| CommandError::failed(format!("write spicepod: {e}")))?;

    match AppBuilder::build_from_path(incoming.clone()).await {
        Ok(app) => {
            replace_canonical_spicepod(&incoming, &path).await?;
            Ok((app, path))
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
    use runtime_cloud_connect::handlers::MAX_QUERY_ROWS;

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
        let (app, path) = stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD)
            .await
            .expect("valid spicepod stages");

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
        stage_cloud_managed_spicepod(&dir, VALID_SPICEPOD)
            .await
            .expect("first valid stage");
        let canonical = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);

        // A subsequent invalid push must be rejected and must NOT clobber it.
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD)
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
        let err = stage_cloud_managed_spicepod(&dir, INVALID_SPICEPOD)
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
            disposition(VALID_SPICEPOD, None, VALID_SPICEPOD),
            Disposition::AlreadyLive
        );
    }

    /// Anything else is a new configuration and must be applied, or the running
    /// app stops matching what was deployed.
    #[test]
    fn a_changed_spicepod_is_applied() {
        let changed = "version: v2\nkind: Spicepod\nname: changed\n";
        assert_eq!(
            disposition(VALID_SPICEPOD, None, changed),
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
            disposition(VALID_SPICEPOD, Some(pending), pending),
            Disposition::AlreadyPersisted
        );
        // And the spicepod it replaced is no longer "already live": it is on its
        // way out, so re-sending it is a new configuration to apply.
        assert_eq!(
            disposition(VALID_SPICEPOD, Some(pending), VALID_SPICEPOD),
            Disposition::Apply
        );
    }

    /// An instance serving a local spicepod has no deployment to match, so the
    /// first one always applies.
    #[test]
    fn a_runtime_with_no_deployment_applies_the_first_one() {
        assert_eq!(disposition("", None, VALID_SPICEPOD), Disposition::Apply);
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
