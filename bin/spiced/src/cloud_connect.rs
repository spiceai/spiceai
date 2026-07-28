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
//! 3. `SPICE_CONNECT_ADOPT_CODE` env var is set (`SPICE_ADOPT_CODE` is a
//!    deprecated alias).
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
use runtime_cloud_connect::handlers::{QueryResult, RuntimeHandle};
use runtime_cloud_connect::{CloudConnect, identity::IdentityStore};

use crate::log_capture::LogRingBuffer;

/// Default number of log lines returned by `GetPodLogs` when the command
/// leaves `tail_lines` unset (`0`). Bounded by the ring buffer's capacity.
const DEFAULT_POD_LOG_TAIL_LINES: usize = 500;

/// Cheap probe for whether Spice Cloud Connect is configured for this
/// instance, using the same signals as [`maybe_start`]: the explicit
/// `--cloud-connect` flag, an on-disk identity, a staged pending adoption
/// code, or the `SPICE_CONNECT_ADOPT_CODE` env var (or its deprecated
/// `SPICE_ADOPT_CODE` alias — no deprecation warning here, since this may
/// run before tracing is initialized; `from_env` warns later).
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
        || std::env::var_os(runtime_cloud_connect::config::ADOPT_CODE_ENV_DEPRECATED)
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
    // `GetPodLogs`. `None` if capture wasn't installed — the handler then
    // reports logs as unavailable rather than returning an empty blob.
    let logs = crate::log_capture::handle();
    let handle: Arc<dyn RuntimeHandle> = Arc::new(SpicedRuntimeHandle::new(runtime, logs));

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

/// Tracks whether a deployed spicepod carried changes that `apply_app`
/// cannot hot-reload (see [`restart_required_sections`]) and therefore need a
/// process restart to take effect. Set by `apply_spicepod` and surfaced by
/// `get_status` so the control plane can show a "restart required" state until
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
    /// Recent log lines for `GetPodLogs`. `None` when the capture layer was
    /// not installed (Cloud Connect not configured at tracing-init time).
    logs: Option<LogRingBuffer>,
    /// Pending-restart state accumulated across `apply_spicepod` calls.
    restart: RestartState,
}

impl SpicedRuntimeHandle {
    fn new(runtime: Arc<Runtime>, logs: Option<LogRingBuffer>) -> Self {
        Self {
            runtime,
            logs,
            restart: RestartState::default(),
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

    /// Execute a SQL statement and return the results as a native Arrow IPC
    /// stream (schema + record batches) plus row-count / truncation metadata.
    /// The runtime never flattens rows to JSON — the cloud control plane
    /// decodes the Arrow directly and renders JSON only at its REST edge.
    ///
    /// Caps:
    /// - `max_rows == 0` → default cap of [`DEFAULT_RUN_QUERY_ROW_CAP`].
    /// - Hard ceiling of [`RUN_QUERY_HARD_ROW_CAP`] rows regardless.
    /// - Byte budget of [`RUN_QUERY_BYTE_BUDGET`] on the encoded IPC stream
    ///   — exceeding either cap sets `truncated: true`.
    async fn execute_sql(&self, sql: &str, max_rows: u32) -> Result<QueryResult, String> {
        use futures::StreamExt as _;

        let cap = resolve_run_query_cap(max_rows);

        let df = self.runtime.datafusion();
        // Cloud-originated RunQuery is a remote management surface; we
        // never want an adopted control plane to be able to mutate the
        // local runtime via DDL/DML. Force read-only at the query layer
        // regardless of principal — there is no signed-in user here.
        let query = df.query_builder(sql).read_only(true).build();
        let result = query.run().await.map_err(|e| e.to_string())?;
        let mut stream = result.data;
        // Capture the stream schema BEFORE consuming. A query that returns
        // zero batches (empty result set) still has a real schema, so
        // without this snapshot the envelope would advertise empty columns.
        let stream_schema = stream.schema();

        // Collect up to `cap` rows worth of batches. We stop streaming
        // once we have enough rows so we don't pull the rest of a huge
        // result set into memory just to throw it away.
        let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
        let mut collected_rows: usize = 0;
        let mut source_truncated = false;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| e.to_string())?;
            let take_rows = cap.saturating_sub(collected_rows);
            if take_rows == 0 {
                // We've hit the cap. Only flag truncation if this next
                // batch actually carries rows — an empty trailing batch
                // means the result ended exactly at the cap, not beyond it.
                if batch.num_rows() > 0 {
                    source_truncated = true;
                }
                break;
            }
            if batch.num_rows() > take_rows {
                source_truncated = true;
                batches.push(batch.slice(0, take_rows));
                break;
            }
            collected_rows += batch.num_rows();
            batches.push(batch);
        }

        // Encode the collected batches to an Arrow IPC stream, enforcing
        // the byte budget. If encoding would exceed the budget we stop
        // writing further batches and mark the result truncated.
        let (arrow_ipc, encoded_rows, byte_truncated) =
            encode_ipc_bounded(stream_schema.as_ref(), &batches, RUN_QUERY_BYTE_BUDGET)?;
        Ok(QueryResult {
            arrow_ipc,
            row_count: encoded_rows,
            truncated: source_truncated || byte_truncated,
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
    ) -> Result<serde_json::Value, String> {
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

        let changed = Arc::clone(&self.runtime).apply_app(Arc::new(new_app)).await;

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
        }))
    }

    /// Standalone `spiced` cannot self-restart: there is no supervisor to bring
    /// the process back up, so a self-initiated exit would just take the
    /// runtime down. Return an error (not `Ok`) so the control plane records
    /// this as a failed command rather than mistaking an unexecuted restart for
    /// success — operators restart spiced via their own process manager
    /// (systemd, Docker, Kubernetes). Configuration changes apply in-process via
    /// [`RuntimeHandle::apply_spicepod`], so a restart is rarely needed.
    async fn restart(&self, _graceful: bool) -> Result<serde_json::Value, String> {
        Err("restart is unsupported on standalone spiced: it has no supervisor to bring the process back up. Restart it via your process manager (systemd/Docker/Kubernetes). Configuration changes apply in-process via ApplySpicepod and do not need a restart.".to_string())
    }

    /// Return recent captured log lines for a `GetPodLogs` command.
    ///
    /// A standalone `spiced` has no pod / kube API, so it serves its own
    /// recently-captured log output (see [`crate::log_capture`]) instead. The
    /// text is returned verbatim to the caller, which places it in
    /// `CommandResult.payload_json` as a raw string per the gateway contract.
    ///
    /// `tail_lines <= 0` returns the last [`DEFAULT_POD_LOG_TAIL_LINES`]
    /// lines; a positive value returns that many (capped by the ring buffer).
    /// Returns an error — not an empty string — when capture is unavailable,
    /// so the control plane can tell "no logs captured" from "logging off".
    async fn get_pod_logs(&self, tail_lines: i64) -> Result<String, String> {
        let Some(ring) = self.logs.as_ref() else {
            return Err(
                "log capture is not enabled for this runtime (Spice Cloud Connect must be configured at startup)".to_string(),
            );
        };
        let n = if tail_lines <= 0 {
            DEFAULT_POD_LOG_TAIL_LINES
        } else {
            usize::try_from(tail_lines).unwrap_or(DEFAULT_POD_LOG_TAIL_LINES)
        };
        Ok(ring.tail(n))
    }

    /// Build the standalone status document for a `GetStatus` command.
    ///
    /// `phase` follows the control-plane vocabulary:
    /// - `Failed` — the runtime is shutting down.
    /// - `Ready` — all registered components have reached readiness
    ///   (`RuntimeStatus::is_ready`).
    /// - `Progressing` — otherwise (components still initializing/erroring).
    ///
    /// A conservative `Progressing` (rather than `Failed`) is used for
    /// not-yet-ready runtimes because `is_ready` is deliberately lenient — an
    /// accelerated dataset can keep serving from its acceleration layer even
    /// while its source is in error — so a component error is not necessarily
    /// terminal. Per-component states and any error messages ride in
    /// `components`/`errors` for detail, and `restart_pending` surfaces a
    /// deploy that needs a restart (see `apply_spicepod`).
    async fn get_status(&self) -> Result<serde_json::Value, String> {
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
            ("Failed", "runtime is shutting down".to_string())
        } else if ready {
            ("Ready", format!("{ready_count}/{total} components ready"))
        } else if total == 0 {
            ("Progressing", "no components registered yet".to_string())
        } else {
            (
                "Progressing",
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

        Ok(serde_json::json!({
            "phase": phase,
            "reason": reason,
            "ready": ready,
            "component_count": total,
            "ready_count": ready_count,
            "components": components,
            "errors": errors,
            "restart_pending": restart_pending,
            "restart_pending_sections": restart_pending_sections,
        }))
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
async fn stage_cloud_managed_spicepod(
    config_dir: &Path,
    spicepod_yaml: &str,
) -> Result<(App, std::path::PathBuf), String> {
    let path = config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
    tokio::fs::create_dir_all(config_dir)
        .await
        .map_err(|e| format!("create config dir: {e}"))?;

    // Validate on a temp file first so a bad push never clobbers the last
    // known-good spicepod on disk.
    let incoming = config_dir.join("spicepod-cloud-managed.incoming.yml");
    tokio::fs::write(&incoming, spicepod_yaml)
        .await
        .map_err(|e| format!("write spicepod: {e}"))?;

    match AppBuilder::build_from_path(incoming.clone()).await {
        Ok(app) => {
            replace_canonical_spicepod(&incoming, &path).await?;
            Ok((app, path))
        }
        Err(e) => {
            // Best-effort cleanup; ignore failure (temp file is inert).
            let _ = tokio::fs::remove_file(&incoming).await;
            Err(format!("invalid spicepod: {e}"))
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
async fn replace_canonical_spicepod(incoming: &Path, path: &Path) -> Result<(), String> {
    match tokio::fs::rename(incoming, path).await {
        Ok(()) => return Ok(()),
        Err(e) => {
            // A fresh install has no canonical file yet — nothing to preserve,
            // so surface the error directly.
            if !tokio::fs::try_exists(path).await.unwrap_or(false) {
                return Err(format!("persist spicepod: {e}"));
            }
            // Destination exists (the Windows case): fall through to the
            // backup-and-rollback swap below.
        }
    }

    let backup = path.with_extension("yml.bak");
    let _ = tokio::fs::remove_file(&backup).await;
    tokio::fs::rename(path, &backup)
        .await
        .map_err(|e| format!("persist spicepod (backup current): {e}"))?;
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
            Err(format!("persist spicepod: {e}"))
        }
    }
}

/// Byte budget for an encoded `RunQuery` Arrow IPC stream. A coarse secondary
/// guard on top of the row cap so a few very wide rows can't blow past the
/// gRPC message size. The budget is enforced on the *encoded* size of every
/// batch — including the first — so a single wide first batch can never produce
/// a stream above the cap; the overflowing batch and everything after it are
/// dropped and the result is flagged truncated.
pub const RUN_QUERY_BYTE_BUDGET: usize = 5 * 1024 * 1024;

/// Encode `batches` into an Arrow IPC stream (`schema` first) whose total
/// encoded size never exceeds `budget`. Each batch is appended only if, after
/// encoding, the stream still fits the budget; the first batch to overflow (and
/// all subsequent batches) is dropped and `truncated` is set. Returns the
/// bytes, the number of rows actually written, and whether the budget cut the
/// stream short.
///
/// Because the check is applied *after* encoding each batch, even a single
/// oversized first batch is rejected — the function then emits the largest
/// prefix that fits (possibly schema-only) rather than an over-budget stream.
fn encode_ipc_bounded(
    schema: &arrow::datatypes::Schema,
    batches: &[arrow::record_batch::RecordBatch],
    budget: usize,
) -> Result<(Vec<u8>, u64, bool), String> {
    // Encode the accepted-prefix once at the end, but discover how many
    // batches fit by trial-encoding the running prefix. The budget guards the
    // *finished* stream, so the prospective length must include the IPC
    // end-of-stream marker that `finish` appends.
    let mut accepted = 0usize;
    let mut rows: u64 = 0;
    for batch in batches {
        let candidate = encode_ipc(schema, &batches[..=accepted])?;
        if candidate.len() > budget {
            // This batch (and thus the rest) pushes the encoded stream past
            // the cap — stop here and emit only the prefix that fit.
            break;
        }
        accepted += 1;
        rows += batch.num_rows() as u64;
    }
    let truncated = accepted < batches.len();
    let buf = encode_ipc(schema, &batches[..accepted])?;
    Ok((buf, rows, truncated))
}

/// Encode `schema` + `batches` into a complete (finished) Arrow IPC stream.
fn encode_ipc(
    schema: &arrow::datatypes::Schema,
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<u8>, String> {
    use arrow::ipc::writer::StreamWriter;
    let mut buf: Vec<u8> = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, schema)
        .map_err(|e| format!("arrow ipc init failed: {e}"))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("arrow ipc write failed: {e}"))?;
    }
    writer
        .finish()
        .map_err(|e| format!("arrow ipc finish failed: {e}"))?;
    drop(writer);
    Ok(buf)
}

/// Default row cap when the cloud control plane sets `max_rows = 0`.
pub const DEFAULT_RUN_QUERY_ROW_CAP: usize = 1_000;
/// Hard row ceiling that the runtime always enforces, even when the
/// control plane requests more.
pub const RUN_QUERY_HARD_ROW_CAP: usize = 10_000;

/// Resolve the effective row cap for a `RunQuery`:
/// - `max_rows == 0` → [`DEFAULT_RUN_QUERY_ROW_CAP`]
/// - else → `min(max_rows, RUN_QUERY_HARD_ROW_CAP)`
#[must_use]
pub fn resolve_run_query_cap(max_rows: u32) -> usize {
    if max_rows == 0 {
        DEFAULT_RUN_QUERY_ROW_CAP
    } else {
        (max_rows as usize).min(RUN_QUERY_HARD_ROW_CAP)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_query_cap_defaults_when_unset() {
        assert_eq!(resolve_run_query_cap(0), DEFAULT_RUN_QUERY_ROW_CAP);
    }

    #[test]
    fn run_query_cap_honors_caller_below_hard_cap() {
        assert_eq!(resolve_run_query_cap(50), 50);
        assert_eq!(resolve_run_query_cap(9_999), 9_999);
        assert_eq!(resolve_run_query_cap(10_000), 10_000);
    }

    #[test]
    fn run_query_cap_clamps_to_hard_cap() {
        // The hard cap is 10_000 — even when the caller asks for 99_999
        // we never serialize more than 10_000 rows.
        assert_eq!(resolve_run_query_cap(99_999), RUN_QUERY_HARD_ROW_CAP);
        assert_eq!(resolve_run_query_cap(u32::MAX), RUN_QUERY_HARD_ROW_CAP);
    }

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
        assert!(err.contains("invalid spicepod"), "unexpected error: {err}");

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
        assert!(err.contains("invalid spicepod"), "unexpected error: {err}");
        // Nothing should have been promoted to the canonical path.
        assert!(!dir.join(CLOUD_MANAGED_SPICEPOD_FILE).exists());
        assert!(!dir.join("spicepod-cloud-managed.incoming.yml").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    /// Build a single-column single-row batch whose one string cell is
    /// `width` bytes, so its encoded IPC size is dominated by that payload.
    fn wide_row_batch(schema: &Arc<Schema>, width: usize) -> RecordBatch {
        let cell = "x".repeat(width);
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(StringArray::from(vec![cell]))],
        )
        .expect("build wide-row batch")
    }

    #[test]
    fn encode_ipc_bounded_caps_a_wide_first_batch() {
        // A single first batch larger than the budget must NOT be emitted as an
        // oversized stream — the result is truncated down to the schema-only
        // prefix and the bytes stay within budget.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let budget = 4 * 1024;
        let big = wide_row_batch(&schema, 64 * 1024);

        let (bytes, rows, truncated) =
            encode_ipc_bounded(schema.as_ref(), std::slice::from_ref(&big), budget)
                .expect("encode succeeds");

        assert!(truncated, "an over-budget first batch must flag truncation");
        assert_eq!(rows, 0, "the oversized batch is dropped, so no rows ship");
        assert!(
            bytes.len() <= budget,
            "emitted stream {} bytes must stay within budget {budget}",
            bytes.len()
        );
        // The schema-only prefix is still a valid, readable IPC stream.
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .expect("schema-only stream is valid");
        assert_eq!(reader.schema().fields().len(), 1);
    }

    #[test]
    fn encode_ipc_bounded_stops_before_overflowing_batch() {
        // Several batches that each fit individually but collectively exceed the
        // budget: only the prefix that fits is emitted and truncation is set.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let budget = 8 * 1024;
        let batches: Vec<RecordBatch> = (0..8).map(|_| wide_row_batch(&schema, 2 * 1024)).collect();

        let (bytes, rows, truncated) =
            encode_ipc_bounded(schema.as_ref(), &batches, budget).expect("encode succeeds");

        assert!(truncated, "exceeding the budget must flag truncation");
        assert!(rows >= 1, "at least the first fitting batch is written");
        assert!(
            rows < batches.len() as u64,
            "not all batches fit the budget"
        );
        assert!(
            bytes.len() <= budget,
            "emitted stream {} bytes must stay within budget {budget}",
            bytes.len()
        );
    }

    #[test]
    fn encode_ipc_bounded_keeps_all_batches_under_budget() {
        // When everything fits, all rows are written and nothing is truncated.
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let batches: Vec<RecordBatch> = (0..4).map(|_| wide_row_batch(&schema, 16)).collect();

        let (bytes, rows, truncated) =
            encode_ipc_bounded(schema.as_ref(), &batches, RUN_QUERY_BYTE_BUDGET)
                .expect("encode succeeds");

        assert!(!truncated, "well-under-budget input must not truncate");
        assert_eq!(rows, batches.len() as u64, "all rows written");
        assert!(bytes.len() <= RUN_QUERY_BYTE_BUDGET);
        // Round-trips back to the same row count.
        let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
            .expect("stream is valid");
        let total: usize = reader.map(|b| b.expect("batch").num_rows()).sum();
        assert_eq!(total, batches.len());
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
