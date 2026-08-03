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
//! By restarting, and only by restarting. `apply_spicepod` validates the
//! incoming spicepod, persists it as `spicepod-cloud-managed.yml`, records the
//! deployment version beside it, and exits 0 — the supervisor relaunches
//! `spiced`, [`cloud_managed_spicepod`] hands the persisted file to the app
//! builder, and the instance comes up serving it and reporting its version.
//!
//! Two consequences the caller has to hold:
//!
//! - **The command result is not how a deployment is confirmed.** The process
//!   exits mid-command, so the stream drops; the control plane reconciles against
//!   the `DeployState` on the next connection's `Hello`. A deployment that never
//!   gets that far — one rejected at validation — is reported on the open
//!   session's next `Heartbeat` instead, since nothing restarts.
//! - **Downtime is proportional to the app's size, not the change's.** A
//!   one-line edit reloads every dataset and rebuilds every acceleration.
//!
//! What this buys is one behaviour instead of two: no section that reconciles
//! in-process while another waits for an operator, and no partially-applied
//! state that is neither the old configuration nor the new one.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app::{App, AppBuilder};
use async_trait::async_trait;
use parking_lot::RwLock;
use runtime::Runtime;
use runtime::status::ComponentStatus;
use runtime_cloud_connect::config::{
    CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig, IDENTITY_FILE, PENDING_ADOPT_CODE_FILE,
};
use runtime_cloud_connect::handlers::{
    ApplyOutcome, Capability, CommandError, DeployState, RuntimeHandle, RuntimePhase,
    SpicepodDeployment, StatusReport,
};
use runtime_cloud_connect::supervisor::Supervisor;
use runtime_cloud_connect::{CloudConnect, deployment, identity::IdentityStore};
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
/// A deployment applies by persisting this file and restarting, so on every
/// start it — not the instance directory's `spicepod.yaml` — is the
/// configuration a cloud-managed instance serves.
pub struct CloudManagedSpicepod {
    pub path: PathBuf,
    /// The persisted YAML, kept so an `ApplySpicepod` can tell a redelivery of
    /// the running deployment from a new one without re-reading the file.
    pub spicepod_yaml: String,
    /// The deployment that persisted it, from the record beside it. `None` when
    /// the dispatch assigned no version, or the record is missing or corrupt —
    /// in which case the instance reports that it cannot name a version.
    pub deployment_version: Option<u64>,
}

/// A deployed spicepod this start would not load, and the deployment it belongs
/// to.
///
/// The apply that persisted it validated it and then exited, so this process is
/// the first to discover the problem — a secret that has since been removed, a
/// file that is no longer there. It is reported on the `Hello` this start
/// connects with, which is the only frame left: the command that dispatched the
/// deployment was answered a restart ago.
pub struct RejectedDeployment {
    /// The refused deployment. `None` when the record named no version, in which
    /// case there is nothing for the control plane to attach the failure to and
    /// it is logged only.
    pub deployment_version: Option<u64>,
    /// Why it would not load, verbatim — an operator reads this in the portal.
    pub message: String,
}

/// The cloud-managed spicepod this instance starts on, or `None` when Cloud
/// Connect is not configured or no deployment has ever landed here.
///
/// Reads files only — no control-plane round trip — so an instance whose
/// gateway is unreachable still comes up on its deployed configuration.
pub async fn cloud_managed_spicepod(cloud_connect_flag: bool) -> Option<CloudManagedSpicepod> {
    if !is_configured(cloud_connect_flag) {
        return None;
    }
    let config_dir = CloudConnectConfig::default_config_dir();
    let path = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    // An unreadable file is the same as an absent one here: the caller falls
    // back to the local spicepod and reports no applied version, which leaves
    // the instance reachable for the deployment that fixes it. `is_configured`
    // ran before this, so a missing file is the ordinary not-yet-deployed case.
    let spicepod_yaml = tokio::fs::read_to_string(&path).await.ok()?;
    Some(CloudManagedSpicepod {
        deployment_version: deployment::read_version(&config_dir),
        path,
        spicepod_yaml,
    })
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
/// deployed one that failed to build). It is what the instance reports as its
/// applied deployment, so passing it for a configuration that is not live would
/// resolve a deployment that never landed.
///
/// `rejected_deployment` is the deployed spicepod this start refused, when there
/// is one. The two are mutually exclusive in practice: a start either serves the
/// deployment it found or falls back and reports it failed.
pub async fn maybe_start(
    runtime_version: &str,
    runtime: Arc<Runtime>,
    cloud_connect_flag: bool,
    delivered_secrets: Option<DeliveredSecretsState>,
    running_deployment: Option<CloudManagedSpicepod>,
    rejected_deployment: Option<RejectedDeployment>,
) -> Option<CloudConnect> {
    let config = build_config(runtime_version);

    // Quick sanity probe — if no identity AND no adoption code, skip.
    // Surface a load/parse error (corrupt or unreadable identity.json)
    // rather than silently treating it as "not adopted", so a broken
    // identity file is visible to the operator instead of quietly
    // disabling Cloud Connect.
    let has_identity = match IdentityStore::load_optional(&config.identity_path) {
        Ok(opt) => opt.is_some(),
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
    let handle: Arc<dyn RuntimeHandle> = Arc::new(SpicedRuntimeHandle::new(
        runtime,
        logs,
        delivered_store,
        identity_path,
        running_deployment,
        rejected_deployment,
        supervisor,
    ));

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

/// One deployment as this instance sees it: the version that names it and the
/// spicepod it carries. Two deployments are the same one when both halves
/// match — a version alone cannot decide it, because a control plane that
/// redelivers a version with different content is describing a different
/// configuration whatever it calls it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Deployed {
    deployment_version: Option<u64>,
    spicepod_yaml: String,
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

/// The report for a `committed` deployment version and a `refused` deployment.
///
/// The applied version is always named, and `0` when there is nothing to name:
/// no deployment since enrolment, a deployment that carried no version, or a
/// start that fell back because the deployed spicepod would not load. Absent
/// would mean "this instance cannot say", which is a different answer — the
/// control plane settles the two differently, and an instance announcing
/// `deploy.versions` never sends it.
fn deploy_state_of(committed: Option<u64>, refused: Option<RefusedDeployment>) -> DeployState {
    let state = DeployState::applied(committed.unwrap_or(0));
    match refused {
        Some(refused) => state.with_failure(refused.deployment_version, refused.message),
        None => state,
    }
}

/// Classify an incoming deployment against what this process is serving
/// (`live`) and what it has already persisted but not yet restarted onto
/// (`persisted`).
fn disposition(live: &Deployed, persisted: Option<&Deployed>, incoming: &Deployed) -> Disposition {
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
    /// A runtime serving a local spicepod has an empty one, which matches no
    /// incoming deployment and reports no version.
    live: Deployed,
    /// A deployment this process validated and persisted, waiting on the exit
    /// that makes it live. Read on every `Hello`, written once per apply — a
    /// `parking_lot` lock held for the read/write only, never across an
    /// `.await`.
    persisted: RwLock<Option<Deployed>>,
    /// The deployment this instance refused, and why: a spicepod rejected at
    /// validation, or one that validated at apply time and then would not load
    /// at startup.
    ///
    /// One slot, because the report is a snapshot the control plane replaces
    /// wholesale — the latest refusal is the only one worth naming, and an
    /// accepted deployment clears it. A stale failure left here would fail a
    /// later deployment that reused the version.
    refused: RwLock<Option<RefusedDeployment>>,
    /// What will relaunch this process after a deployment exits it.
    supervisor: Supervisor,
}

/// A deployment this instance would not serve.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RefusedDeployment {
    deployment_version: u64,
    /// The runtime's own error, verbatim — the validation failure, or the load
    /// failure behind a fallback. It reaches an operator in the portal, so it
    /// has to say what was wrong with *their* spicepod.
    message: String,
}

impl SpicedRuntimeHandle {
    fn new(
        runtime: Arc<Runtime>,
        logs: Option<LogRingBuffer>,
        delivered_secrets: Arc<CloudDeliveredSecretStore>,
        identity_path: std::path::PathBuf,
        running_deployment: Option<CloudManagedSpicepod>,
        rejected_deployment: Option<RejectedDeployment>,
        supervisor: Supervisor,
    ) -> Self {
        let live = running_deployment.map_or_else(
            || Deployed {
                deployment_version: None,
                spicepod_yaml: String::new(),
            },
            |running| Deployed {
                deployment_version: running.deployment_version,
                spicepod_yaml: running.spicepod_yaml,
            },
        );
        // A deployed spicepod that would not load at startup is a refusal this
        // process discovered on its own, and the `Hello` it is about to send is
        // the frame that reports it: the apply that persisted it exited long ago,
        // so nothing else will. A rejection with no version is dropped — it names
        // no deployment, so the control plane could only attach it to the wrong
        // one (see `DeployState::failure_message`).
        let refused = rejected_deployment.and_then(|rejected| {
            let deployment_version = rejected.deployment_version?;
            Some(RefusedDeployment {
                deployment_version,
                message: rejected.message,
            })
        });
        Self {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            live,
            persisted: RwLock::new(None),
            refused: RwLock::new(refused),
            supervisor,
        }
    }

    /// Record `version` as refused, replacing any earlier refusal.
    fn refuse(&self, version: Option<u64>, message: &str) {
        let Some(deployment_version) = version else {
            // Nothing to report it against: a failure with no version names no
            // deployment. The error still reaches the caller as the command
            // result and the log.
            return;
        };
        *self.refused.write() = Some(RefusedDeployment {
            deployment_version,
            message: message.to_string(),
        });
    }

    /// The deployment this instance has committed to: the one it persisted if a
    /// deployment is mid-apply, otherwise the one it is serving.
    ///
    /// Reported both on `Hello` and in the `get_status` document, so the two
    /// can never disagree. It is deliberately the *persisted* deployment during
    /// the window between the result being sent and the process exiting: the
    /// instance has accepted that configuration and is restarting onto it, and
    /// reporting the outgoing version there would read to the control plane as
    /// a deployment that rolled back.
    fn committed_deployment_version(&self) -> Option<u64> {
        self.persisted
            .read()
            .as_ref()
            .map_or(self.live.deployment_version, |persisted| {
                persisted.deployment_version
            })
    }

    /// What this instance reports about its deployments, for the `Hello` and for
    /// any `Heartbeat` with news.
    ///
    /// The applied version is always named — `0` when no deployment has been
    /// applied since enrolment, including when a deployment landed without one
    /// (there is no version to claim) or when the deployed spicepod would not
    /// load and this start fell back. Absent would mean "cannot say", which an
    /// instance announcing the capability never reports.
    fn deploy_state_snapshot(&self) -> DeployState {
        // Cloned out of the lock rather than held across the call below.
        let refused = self.refused.read().clone();
        deploy_state_of(self.committed_deployment_version(), refused)
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
        deployment_version: Option<u64>,
        secrets: &runtime_cloud_connect::sealed_secrets::DeliveredSecrets,
    ) -> Option<String> {
        let key = self.cache_key()?;
        let path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        let version = deployment::version_label(deployment_version);
        match runtime_cloud_connect::secret_cache::write(&path, &key, &version, secrets) {
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
            // `DeployVersions` is unconditional: every start reads the deployment
            // record beside the spicepod, so there is always a version to name
            // (`0` when none has been applied). Announcing it is what tells the
            // control plane to reconcile deployments against what this instance
            // reports instead of against a command result the restart discards.
            Capability::ApplySpicepod | Capability::GetStatus | Capability::DeployVersions => true,
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
            Capability::ApplySpicepod | Capability::GetStatus | Capability::DeployVersions => {
                format!("{} is not supported by this instance", capability.wire_name())
            }
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

    async fn deploy_state(&self) -> Option<DeployState> {
        Some(self.deploy_state_snapshot())
    }

    async fn refuse_deployment(&self, deployment_version: Option<u64>, message: &str) {
        self.refuse(deployment_version, message);
        tracing::warn!(
            "Spice Cloud Connect: {} was refused and is NOT applied; this instance keeps serving its current configuration and reports the refusal to Spice Cloud: {message}",
            deployment_version.map_or_else(
                || "a deployment".to_string(),
                |version| format!("deployment {version}")
            )
        );
    }

    /// Apply a cloud-managed spicepod by persisting it and restarting onto it.
    ///
    /// 1. A redelivery of the deployment this process is already serving — same
    ///    version, same YAML — is answered as applied and changes nothing, not
    ///    even the delivered secrets. Restarting for it would make a redelivery
    ///    a restart loop, so a change to an app's secrets has to arrive as a new
    ///    deployment rather than as a re-send of the current one.
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
            deployment_version,
            delivered_secrets,
        } = deployment;

        let incoming = Deployed {
            deployment_version,
            spicepod_yaml: spicepod_yaml.to_string(),
        };
        let version_label = deployment_version.map_or_else(
            || "this deployment".to_string(),
            |version| format!("deployment {version}"),
        );

        // Cloned out of the lock rather than compared under it: the comparison
        // is against an owned `incoming` and the guard must not be held across
        // the awaits below.
        let persisted = self.persisted.read().as_ref().cloned();
        match disposition(&self.live, persisted.as_ref(), &incoming) {
            Disposition::AlreadyLive => {
                tracing::info!(
                    "Spice Cloud Connect: {version_label} is already applied; nothing to do"
                );
                return Ok(ApplyOutcome::settled(serde_json::json!({
                    "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                    "applied": true,
                    "live": true,
                    "restart": "not_required",
                    "deployment_version": deployment_version,
                    "message": format!("{version_label} is already applied and live; this instance is serving it."),
                })));
            }
            Disposition::AlreadyPersisted => {
                tracing::info!(
                    "Spice Cloud Connect: {version_label} is already persisted; restarting onto it"
                );
                return Ok(ApplyOutcome::exit_to_apply(serde_json::json!({
                    "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                    "applied": true,
                    "live": false,
                    "restart": "in_progress",
                    "deployment_version": deployment_version,
                    "message": format!("{version_label} is persisted; this instance is restarting onto it."),
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
                cache_error =
                    self.cache_delivered_secrets(config_dir, deployment_version, &secrets);
                // Replaces the whole set: an app whose secrets were removed
                // must stop resolving them.
                self.delivered_secrets.replace(secrets);
                Some(names)
            }
        };

        let (new_app, path) = stage_cloud_managed_spicepod(config_dir, spicepod_yaml).await?;

        // Recorded after the spicepod is promoted, so a failure between the two
        // leaves the instance on a configuration it can still name. A failure
        // *here* leaves the reverse — the new spicepod canonical, the version
        // behind it — which the restart would report as the old version and the
        // control plane would read as a rollback. It cannot be swallowed: the
        // result says so, and the instance still restarts onto the persisted
        // spicepod rather than leaving the canonical file and the running
        // configuration divergent.
        let version_error = record_deployment(config_dir, deployment_version).await;

        *self.persisted.write() = Some(incoming);
        // This deployment supersedes any earlier refusal, so the report stops
        // naming it. Keeping a stale failure would fail a later deployment that
        // reused that version — the control plane replaces its whole record with
        // each state it receives.
        *self.refused.write() = None;

        tracing::info!(
            "Spice Cloud Connect: {version_label} validated and persisted to {} ({} datasets, {} models, {} catalogs, {} views); restarting to apply it",
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
            "deployment_version": deployment_version,
            "deployment_version_error": version_error,
            "supervised": self.supervisor.is_supervised(),
            "supervisor": self.supervisor.as_str(),
            "message": format!(
                "{version_label} was validated and persisted. This instance is restarting to serve it; \
                 the restart reloads every dataset, so the app is unavailable until it finishes."
            ),
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
        let refused = self.refused.read().clone();

        Ok(
            StatusReport::new(phase, reason).with_detail(serde_json::json!({
                "ready": ready,
                "component_count": total,
                "ready_count": ready_count,
                "components": components,
                "errors": errors,
                // The deployment this instance has committed to — the same value
                // it announces in the `DeployState` on its `Hello`.
                "applied_deployment_version": self.committed_deployment_version(),
                // The deployment actually being served, which differs from the
                // above only while a restart is pending.
                "live_deployment_version": self.live.deployment_version,
                // The deployment this instance refused, and why — the same pair
                // it reports in the `DeployState`, so a `get_status` read and the
                // reconciler never disagree about which deployment failed.
                "failed_deployment_version": refused.as_ref().map(|refused| refused.deployment_version),
                "deployment_failure": refused.as_ref().map(|refused| refused.message.clone()),
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
}

/// Record `deployment_version` as this instance's applied deployment, returning
/// the message to surface when it could not be written.
///
/// Runs on the blocking pool: the record is written through the same
/// synchronous atomic-rename path as the identity, and this is called from the
/// Cloud Connect task on a runtime worker.
async fn record_deployment(config_dir: &Path, deployment_version: Option<u64>) -> Option<String> {
    let dir = config_dir.to_path_buf();
    let result =
        tokio::task::spawn_blocking(move || deployment::write(&dir, deployment_version)).await;
    let error = match result {
        Ok(Ok(())) => return None,
        Ok(Err(err)) => err.to_string(),
        Err(join) => format!("the deployment record task panicked: {join}"),
    };
    tracing::error!(
        "Spice Cloud Connect: {error}. The spicepod is persisted and this instance will restart onto it, but it will report the previous deployment version — which the control plane reads as a rollback. Fix the permissions on the Spice config directory and deploy again. See: https://spiceai.org/docs"
    );
    Some(error)
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

    fn deployed(version: Option<u64>, yaml: &str) -> Deployed {
        Deployed {
            deployment_version: version,
            spicepod_yaml: yaml.to_string(),
        }
    }

    /// The redelivery case #1306 turns into a crash loop if it is missed: the
    /// deployment the instance is already serving must change nothing.
    #[test]
    fn redelivering_the_running_deployment_is_a_no_op() {
        let live = deployed(Some(7), VALID_SPICEPOD);
        assert_eq!(
            disposition(&live, None, &deployed(Some(7), VALID_SPICEPOD)),
            Disposition::AlreadyLive
        );
    }

    /// A version the instance is serving, with content it is not, is a
    /// different configuration whatever the control plane calls it — applying
    /// it is what keeps the running app equal to the deployed one.
    #[test]
    fn the_same_version_with_different_content_is_applied() {
        let live = deployed(Some(7), VALID_SPICEPOD);
        let changed = "version: v2\nkind: Spicepod\nname: changed\n";
        assert_eq!(
            disposition(&live, None, &deployed(Some(7), changed)),
            Disposition::Apply
        );
    }

    /// A version-less deployment cannot be deduplicated by version, so the
    /// spicepod itself decides. Two dispatches of the same content must not
    /// restart the instance twice.
    #[test]
    fn versionless_deployments_deduplicate_on_content() {
        let live = deployed(None, VALID_SPICEPOD);
        assert_eq!(
            disposition(&live, None, &deployed(None, VALID_SPICEPOD)),
            Disposition::AlreadyLive
        );
        assert_eq!(
            disposition(&live, None, &deployed(Some(1), VALID_SPICEPOD)),
            Disposition::Apply,
            "the same spicepod under a new deployment is a new deployment to report"
        );
    }

    /// Between the result being sent and the process exiting, the control plane
    /// can redeliver. The instance has already committed to that deployment, so
    /// it re-issues the exit rather than persisting it again — and never
    /// reports it as live, which it is not.
    #[test]
    fn redelivery_while_a_restart_is_pending_re_issues_the_exit() {
        let live = deployed(Some(7), VALID_SPICEPOD);
        let pending = deployed(Some(8), "version: v2\nkind: Spicepod\nname: next\n");
        assert_eq!(
            disposition(&live, Some(&pending), &pending),
            Disposition::AlreadyPersisted
        );
        // And the deployment it replaced is no longer "already live": it is on
        // its way out, so re-sending it is a new deployment to apply.
        assert_eq!(
            disposition(&live, Some(&pending), &live),
            Disposition::Apply
        );
    }

    /// An instance serving a local spicepod has no deployment to match, so the
    /// first one always applies.
    #[test]
    fn a_runtime_with_no_deployment_applies_the_first_one() {
        let live = deployed(None, "");
        assert_eq!(
            disposition(&live, None, &deployed(Some(1), VALID_SPICEPOD)),
            Disposition::Apply
        );
    }

    /// An instance that has applied nothing reports a zero, never an absence:
    /// "cannot say" sends the control plane down a different path than "nothing
    /// yet", and this instance can always say.
    #[test]
    fn nothing_applied_reports_a_zero() {
        let state = deploy_state_of(None, None);
        assert_eq!(state.applied_deployment_version, Some(0));
        assert_eq!(state.failed_deployment_version, None);
        assert!(state.failure_message.is_empty());
    }

    /// A refusal names the version it refused and leaves the applied version
    /// reporting what the instance is still running — the control plane reads
    /// the pair, not one or the other.
    #[test]
    fn a_refusal_is_reported_alongside_what_is_still_running() {
        let state = deploy_state_of(
            Some(7),
            Some(RefusedDeployment {
                deployment_version: 8,
                message: "invalid spicepod: bad yaml".to_string(),
            }),
        );
        assert_eq!(state.applied_deployment_version, Some(7));
        assert_eq!(state.failed_deployment_version, Some(8));
        assert_eq!(state.failure_message, "invalid spicepod: bad yaml");
    }

    /// A refusal with no version to name is dropped rather than reported: on its
    /// own a message names no deployment, so it could only be attached to the
    /// wrong one.
    #[test]
    fn a_refusal_without_a_version_is_not_reported() {
        let handle_refused: Option<RefusedDeployment> = None;
        let state = deploy_state_of(Some(4), handle_refused);
        assert_eq!(state.applied_deployment_version, Some(4));
        assert_eq!(state.failed_deployment_version, None);
        assert!(
            state.failure_message.is_empty(),
            "a message with no version must not travel on its own"
        );
    }

    #[tokio::test]
    async fn the_deployment_version_is_recorded_beside_the_spicepod() {
        let dir = scratch_dir("record");
        assert_eq!(record_deployment(&dir, Some(12)).await, None);
        assert_eq!(deployment::read_version(&dir), Some(12));

        // A deployment that carries no version clears the recorded one rather
        // than leaving the instance claiming a version it is no longer serving.
        assert_eq!(record_deployment(&dir, None).await, None);
        assert_eq!(deployment::read_version(&dir), None);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
