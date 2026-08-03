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

use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use app::{App, AppBuilder};
use async_trait::async_trait;
use runtime::Runtime;
use runtime::status::ComponentStatus;
use runtime_cloud_connect::config::{
    CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig, IDENTITY_FILE, PENDING_ADOPT_CODE_FILE,
};
use runtime_cloud_connect::handlers::{
    Capability, CommandError, RuntimeHandle, RuntimePhase, StatusReport,
};
use runtime_cloud_connect::{CloudConnect, identity::IdentityStore};
// Reached through the `runtime` re-export rather than a direct dependency, the
// same way `runtime::status` is.
use runtime::secrets::stores::cloud_delivered::{
    CLOUD_DELIVERED_STORE, CloudDeliveredSecretStore,
};
use runtime_cloud_connect::identity::CacheKey;

use crate::log_capture::LogRingBuffer;

/// Default number of log lines returned by `GetLogs` when the command leaves
/// `tail_lines` unset. Bounded by the ring buffer's capacity.
const DEFAULT_LOG_TAIL_LINES: usize = 500;

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
pub async fn maybe_start(
    runtime_version: &str,
    runtime: Arc<Runtime>,
    cloud_connect_flag: bool,
    delivered_secrets: Option<DeliveredSecretsState>,
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
    let delivered_store = match delivered_secrets {
        Some(state) => state.store,
        None => {
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
        }
    };

    // The cache key is read from the identity on each write, not captured here:
    // an instance enrolling in this very process has no identity yet.
    let identity_path = config.identity_path.clone();
    let handle: Arc<dyn RuntimeHandle> = Arc::new(SpicedRuntimeHandle::new(
        runtime,
        logs,
        delivered_store,
        identity_path,
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

/// Tracks whether a deployed spicepod carried changes that `apply_app`
/// cannot hot-reload (see [`restart_required_sections`]) and therefore need a
/// process restart to take effect. Set by `apply_spicepod` and surfaced by
/// `status` so the control plane can show a "restart required" state until
/// the operator restarts spiced (standalone spiced cannot self-restart).
#[derive(Default)]
struct RestartState {
    pending: AtomicBool,
    /// Names of the spicepod sections still awaiting a restart. Guarded by a
    /// std mutex held only for the brief read/write — never across `.await`.
    sections: Mutex<Vec<String>>,
}

/// Thin adapter so the cloud-connect client can call into the runtime
/// without taking a hard dep on the `runtime` crate.
struct SpicedRuntimeHandle {
    runtime: Arc<Runtime>,
    /// Recent log lines for `GetLogs`. `None` when the capture layer was
    /// not installed (Cloud Connect not configured at tracing-init time).
    logs: Option<LogRingBuffer>,
    /// Pending-restart state accumulated across `apply_spicepod` calls.
    restart: RestartState,
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
}

impl SpicedRuntimeHandle {
    fn new(
        runtime: Arc<Runtime>,
        logs: Option<LogRingBuffer>,
        delivered_secrets: Arc<CloudDeliveredSecretStore>,
        identity_path: std::path::PathBuf,
    ) -> Self {
        Self {
            runtime,
            logs,
            restart: RestartState::default(),
            delivered_secrets,
            identity_path,
        }
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
        // No deployment version to record yet — the dispatch does not carry one.
        match runtime_cloud_connect::secret_cache::write(&path, &key, "", secrets) {
            Ok(()) => None,
            Err(err) => {
                tracing::warn!("Spice Cloud Connect: could not cache delivered secrets: {err}");
                Some(err.to_string())
            }
        }
    }
}

/// Spicepod sections that differ between `current` and `new` and that
/// [`Runtime::apply_app`] does **not** hot-reload — a change to any of them
/// only takes effect after a process restart. Returns their names (empty means
/// the change is fully hot-reloadable).
///
/// `apply_app` reconciles only catalogs, datasets, views, models, functions,
/// and workers; every other section is read once at startup. The `runtime:`
/// block is compared as a whole — a few of its fields are re-read live (e.g.
/// `flight.batch_size`, `task_history.enabled`, `functions.enabled`), so this
/// may occasionally over-report a restart, which is the safe direction: we
/// never silently drop a configuration change (data-correctness first).
///
/// `workers` are intentionally NOT checked here: they hot-reload in builds
/// without the `models` feature and, when `models` is enabled, model reloads
/// cover the common case — treating them as restart-only would over-report on
/// the typical spiced build.
fn restart_required_sections(current: &App, new: &App) -> Vec<&'static str> {
    let mut changed = Vec::new();
    if current.runtime != new.runtime {
        changed.push("runtime");
    }
    if current.embeddings != new.embeddings {
        changed.push("embeddings");
    }
    if current.rerankers != new.rerankers {
        changed.push("rerankers");
    }
    if current.tools != new.tools {
        changed.push("tools");
    }
    if current.secrets != new.secrets {
        changed.push("secrets");
    }
    if current.extensions != new.extensions {
        changed.push("extensions");
    }
    if current.management != new.management {
        changed.push("management");
    }
    changed
}

#[async_trait]
impl RuntimeHandle for SpicedRuntimeHandle {
    /// What a standalone `spiced` can answer. `Restart` is excluded
    /// deliberately: there is no supervisor to bring the process back up, so
    /// the command is not merely unimplemented here — it cannot be
    /// implemented, and the control plane should not offer it.
    fn supports(&self, capability: Capability) -> bool {
        match capability {
            Capability::ApplySpicepod | Capability::GetStatus => true,
            // Only when the log-capture layer was installed at startup;
            // otherwise there is no buffer to read from.
            Capability::GetLogs => self.logs.is_some(),
            Capability::Restart | Capability::UpgradeRuntime => false,
        }
    }

    fn unsupported_reason(&self, capability: Capability) -> String {
        match capability {
            Capability::Restart => "Restart is unsupported on standalone spiced: it has no supervisor to bring the process back up. Restart it via your process manager (systemd/Docker/Kubernetes). Configuration changes apply in-process via ApplySpicepod and do not need a restart.".to_string(),
            Capability::UpgradeRuntime => "UpgradeRuntime is unsupported on standalone spiced: it cannot replace its own binary. Upgrade it the way you installed it (`spice upgrade`, your container image, or your package manager). See: https://spiceai.org/docs".to_string(),
            Capability::GetLogs => "Log capture is not enabled for this runtime: Spice Cloud Connect must be configured before startup for spiced to install the log-capture layer. See: https://spiceai.org/docs".to_string(),
            Capability::ApplySpicepod | Capability::GetStatus => format!(
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

    /// Apply a cloud-managed spicepod, hot-reloading what can be hot-reloaded
    /// and flagging what cannot.
    ///
    /// 1. The YAML is validated by building an [`App`] from it on a sibling
    ///    temp file, so a malformed control-plane push is rejected with a
    ///    clear error and the previous good `spicepod-cloud-managed.yml` is
    ///    left untouched.
    /// 2. The validated file is promoted to the canonical path so a later
    ///    process restart picks up the FULL configuration — including the
    ///    parts that can't hot-reload.
    /// 3. Changes to catalogs/datasets/views/models/functions/workers are
    ///    hot-applied via [`Runtime::apply_app`] and take effect immediately.
    /// 4. Changes to sections `apply_app` does not reconcile (the `runtime:`
    ///    block, embeddings, rerankers, tools, secrets, extensions,
    ///    management — see [`restart_required_sections`]) cannot take effect
    ///    until a restart. Rather than silently drop them, the result reports
    ///    `restart_required: true` and the affected sections, and the pending
    ///    state is recorded so `get_status` keeps surfacing it. Standalone
    ///    spiced cannot self-restart, so the operator must restart it via
    ///    their process manager.
    ///
    /// `success` is still `true` in the restart-required case: the spicepod
    /// was accepted, validated, persisted, and its hot-reloadable parts
    /// applied — the caller learns the caveat from `restart_required` rather
    /// than from a blanket failure.
    async fn apply_spicepod(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
        delivered_secrets: Option<runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
    ) -> Result<serde_json::Value, CommandError> {
        // Install the delivered secrets BEFORE the spicepod is validated and
        // applied: `AppBuilder::build_from_path` and `apply_app` both resolve
        // `${ secrets:… }` references, so secrets installed afterwards would
        // arrive after the components that needed them had already failed.
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

        // Determine which changes can't hot-reload BEFORE apply_app swaps the
        // current app. No current app (first load) → startup applies it all,
        // so nothing is restart-pending.
        let restart_sections: Vec<String> = match self.runtime.read_app().await {
            Some(current) => restart_required_sections(&current, &new_app)
                .into_iter()
                .map(String::from)
                .collect(),
            None => Vec::new(),
        };

        let datasets = new_app.datasets.len();
        let models = new_app.models.len();
        let catalogs = new_app.catalogs.len();
        let views = new_app.views.len();

        // A deployment supersedes an unfinished initial component load. That
        // load may be retrying forever against the very spicepod this one
        // replaces, and it holds no lock the apply below could wait on: left
        // running it would keep registering datasets from the app it started
        // against, after this app had replaced it.
        //
        // Taken only once the spicepod has been validated and staged, so a
        // rejected deployment leaves the in-flight load running rather than
        // cancelling it with nothing to put in its place.
        let superseded_initial_load = self.runtime.supersede_initial_load();

        let new_app = Arc::new(new_app);
        let changed = if superseded_initial_load {
            // The cancelled load left only some of the previous app registered,
            // so reconcile against what is actually registered rather than what
            // was declared — otherwise a dataset present in both apps but never
            // reached by the load is diffed as unchanged and never loads.
            Arc::clone(&self.runtime)
                .apply_app_after_cancelled_load(new_app)
                .await
        } else {
            Arc::clone(&self.runtime).apply_app(new_app).await
        };

        let restart_required = !restart_sections.is_empty();
        if restart_required {
            // Record (accumulate) the pending-restart sections so get_status
            // reports them until the runtime is actually restarted.
            self.restart.pending.store(true, Ordering::SeqCst);
            let mut guard = self
                .restart
                .sections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for section in &restart_sections {
                if !guard.iter().any(|s| s == section) {
                    guard.push(section.clone());
                }
            }
        }

        let message = if restart_required {
            format!(
                "Spicepod accepted and persisted. Hot-reloadable components were applied, but changes to [{}] do not take effect until spiced is restarted. Standalone spiced cannot self-restart — restart it via your process manager (systemd/Docker/Kubernetes).",
                restart_sections.join(", ")
            )
        } else {
            "Spicepod applied without a restart.".to_string()
        };

        Ok(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "spicepod_changed": changed,
            "restart_required": restart_required,
            "restart_required_sections": restart_sections,
            "message": message,
            "datasets": datasets,
            "models": models,
            "catalogs": catalogs,
            "views": views,
            // Names only — a delivered value never leaves this process.
            "delivered_secrets": delivered_names,
            "secrets_cache_error": cache_error,
        }))
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
    /// `components`/`errors` detail, and `restart_pending` surfaces a deploy
    /// that needs a restart (see `apply_spicepod`).
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

        let restart_pending = self.restart.pending.load(Ordering::SeqCst);
        let restart_pending_sections: Vec<String> = self
            .restart
            .sections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();

        Ok(
            StatusReport::new(phase, reason).with_detail(serde_json::json!({
                "ready": ready,
                "component_count": total,
                "ready_count": ready_count,
                "components": components,
                "errors": errors,
                "restart_pending": restart_pending,
                "restart_pending_sections": restart_pending_sections,
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

/// Validate a cloud-managed spicepod and persist it to disk.
///
/// Writes `spicepod_yaml` to a sibling `*.incoming.yml` temp file, builds an
/// [`App`] from it to validate (parse + resolve), and only on success
/// atomically promotes the temp file to the canonical
/// [`CLOUD_MANAGED_SPICEPOD_FILE`] path. On any failure the canonical file is
/// left untouched and the temp file is cleaned up. Returns the built `App`
/// (ready to hot-apply) and the canonical path it was written to.
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

    #[test]
    fn restart_required_sections_empty_for_identical_apps() {
        let a = App::default();
        let b = App::default();
        assert!(
            restart_required_sections(&a, &b).is_empty(),
            "identical apps need no restart"
        );
    }

    #[test]
    fn restart_required_sections_flags_runtime_change() {
        let base = App::default();
        // Change a `runtime:` field that apply_app does not hot-reload.
        let mut changed = App::default();
        changed.runtime.dataset_load_parallelism = Some(4);

        assert_eq!(
            restart_required_sections(&base, &changed),
            vec!["runtime"],
            "a runtime-config change must require a restart"
        );
        // Detection is symmetric.
        assert_eq!(restart_required_sections(&changed, &base), vec!["runtime"]);
    }
}
