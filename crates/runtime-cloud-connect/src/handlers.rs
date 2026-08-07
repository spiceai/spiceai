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

//! Handlers for cloud-originated `ControlMessage`s.
//!
//! These translate proto-level commands into calls on a
//! [`RuntimeHandle`] which is implemented by the spiced runtime (or, in
//! tests, by a mock). Keeping the trait small here means this crate
//! never has to compile against the runtime crate — avoiding a cycle
//! and keeping spice-cloud-connect easy to unit-test.
//!
//! Nothing in this module names a generated proto type: the trait is the
//! crate's stable surface, and the client maps it onto the wire.

use std::collections::BTreeSet;
use std::path::Path;

use async_trait::async_trait;
use snafu::Snafu;

use crate::sealed_secrets::DeliveredSecrets;

/// Why a command did not succeed.
///
/// Each variant maps onto exactly one wire `ResultCode`, so the control plane
/// can act on the *class* of failure — grey out a control for
/// [`CommandError::Unsupported`], offer a retry for [`CommandError::Failed`] —
/// instead of matching on the English in the message.
#[derive(Debug, Snafu)]
pub enum CommandError {
    /// This instance does not implement the command. Permanent at this
    /// version; not worth retrying.
    #[snafu(display("{message}"))]
    Unsupported { message: String },

    /// The command is implemented but its arguments were rejected. An
    /// identical retry fails identically.
    #[snafu(display("{message}"))]
    InvalidArgument { message: String },

    /// The command was attempted and did not succeed. May succeed on retry.
    #[snafu(display("{message}"))]
    Failed { message: String },

    /// The instance hit a fault of its own while handling the command.
    #[snafu(display("{message}"))]
    Internal { message: String },

    /// A command of this kind is already in flight and this one was refused
    /// before any work started. Retryable once the first one finishes.
    #[snafu(display("{message}"))]
    Busy { message: String },

    /// The command ran but its result exceeds what the control stream carries.
    /// Never accompanied by a partial payload.
    #[snafu(display("{message}"))]
    ResultTooLarge { message: String },
}

impl CommandError {
    /// The instance does not implement this command.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported {
            message: message.into(),
        }
    }

    /// The command's arguments were rejected.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    /// The command was attempted and did not succeed.
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed {
            message: message.into(),
        }
    }

    /// The instance faulted while handling the command.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// A command of this kind is already in flight.
    pub fn busy(message: impl Into<String>) -> Self {
        Self::Busy {
            message: message.into(),
        }
    }

    /// The result is too large to send on the control stream.
    pub fn result_too_large(message: impl Into<String>) -> Self {
        Self::ResultTooLarge {
            message: message.into(),
        }
    }
}

/// Most rows a [`RuntimeHandle::run_query`] result may carry.
pub const MAX_QUERY_ROWS: u32 = 500;

/// Most bytes the complete Arrow IPC stream of a [`RuntimeHandle::run_query`]
/// result may occupy on the control stream.
pub const MAX_QUERY_RESULT_BYTES: usize = 4 * 1024 * 1024;

/// The row cap a `RunQuery` actually gets: its own request bounded by
/// [`MAX_QUERY_ROWS`], with zero meaning the full default.
#[must_use]
pub fn effective_max_rows(requested: u32) -> u32 {
    if requested == 0 {
        MAX_QUERY_ROWS
    } else {
        requested.min(MAX_QUERY_ROWS)
    }
}

/// A bounded `RunQuery` result.
#[derive(Debug, Clone)]
pub struct QueryOutcome {
    /// A complete Arrow IPC stream — schema, record batches, end-of-stream —
    /// never a fragment and never chunked across results.
    pub arrow_ipc: Vec<u8>,
    /// How many rows `arrow_ipc` carries. Reported in telemetry; the values
    /// themselves are not.
    pub row_count: u64,
}

/// An optional command a [`RuntimeHandle`] may or may not implement.
///
/// The set an instance announces in `Hello.capabilities` is derived from
/// [`RuntimeHandle::supports`], and the client consults the same method before
/// invoking a handler — so what an instance advertises and what it actually
/// answers cannot drift apart.
///
/// Only commands this client can dispatch to a `RuntimeHandle` appear here.
/// The operator-only commands (manifests, drain, pause, sealed secrets, `PromQL`
/// proxying, HTTP proxying) have no handler to route to and are always
/// answered as unsupported. The wire field is an open list of names, so a
/// cluster instance built on a different implementation can advertise
/// capabilities this enum does not know about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    /// Apply cloud-managed Spicepod YAML.
    ApplySpicepod,
    /// Restart the runtime process.
    Restart,
    /// Upgrade the runtime in place.
    UpgradeRuntime,
    /// Return recent runtime log output.
    GetLogs,
    /// Report runtime readiness.
    GetStatus,
    /// Execute a bounded SQL query through the in-process runtime.
    RunQuery,
}

impl Capability {
    /// Every capability this client can advertise, in wire-name order.
    pub const ALL: &'static [Self] = &[
        Self::ApplySpicepod,
        Self::GetLogs,
        Self::GetStatus,
        Self::Restart,
        Self::RunQuery,
        Self::UpgradeRuntime,
    ];

    /// The name carried in `Hello.capabilities`, matching the command's
    /// `snake_case` field name in the `ControlMessage` oneof.
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::ApplySpicepod => "apply_spicepod",
            Self::Restart => "restart",
            Self::UpgradeRuntime => "upgrade_runtime",
            Self::GetLogs => "get_logs",
            Self::GetStatus => "get_status",
            Self::RunQuery => "run_query",
        }
    }
}

/// How a `Restart` should be performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestartMode {
    /// The control plane sent no mode; treat as [`RestartMode::Graceful`].
    #[default]
    Unspecified,
    /// Stop accepting new work, let in-flight requests finish, then restart.
    Graceful,
    /// Restart now, abandoning in-flight requests.
    Immediate,
    /// Drain to zero in-flight work before restarting.
    DrainThenRestart,
}

/// Coarse runtime state.
///
/// The one vocabulary for "how is this runtime doing": carried on every
/// heartbeat and repeated as the `phase` of the `GetStatus` document, so the
/// control plane never has to reconcile two spellings of the same state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimePhase {
    /// The instance did not report a phase.
    #[default]
    Unspecified,
    /// Every registered component has reached readiness.
    Ready,
    /// Components are still initializing, or erroring in a way the runtime
    /// can still serve around.
    Progressing,
    /// The runtime cannot serve — shutting down, or terminally failed.
    Failed,
}

impl RuntimePhase {
    /// The phase as it appears in the `GetStatus` JSON document.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "Unspecified",
            Self::Ready => "Ready",
            Self::Progressing => "Progressing",
            Self::Failed => "Failed",
        }
    }
}

/// One deployment, as it reaches a [`RuntimeHandle`].
///
/// A struct rather than positional arguments: `apply_spicepod(dir, yaml, None)`
/// reads as nothing at the call site, and a later field would be one more thing
/// to thread through every implementation in the right order.
pub struct SpicepodDeployment<'a> {
    /// Where the cloud-managed spicepod lives.
    pub config_dir: &'a Path,
    /// The spicepod to apply, verbatim.
    pub spicepod_yaml: &'a str,
    /// App secrets that rode the same dispatch, already opened (see
    /// [`crate::sealed_secrets`]). They arrive *with* the spicepod because
    /// applying is a restart: secrets that landed afterwards would arrive after
    /// the components that referenced them had already tried to load.
    ///
    /// `None` means the deployment carried none, which is distinct from an
    /// empty map — an app whose secrets were all removed.
    pub delivered_secrets: Option<DeliveredSecrets>,
    /// The app this instance's telemetry is attributed to. It rides the
    /// deployment because the instance cannot derive it and the control plane
    /// already knows it; a handle that exports metrics records it and stamps it
    /// as `scp_app_id`.
    ///
    /// `None` when the control plane named no app.
    pub app_id: Option<&'a str>,
}

/// What the client must do once the result of an apply has been sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostApply {
    /// Nothing. The instance is already serving this deployment, so there is
    /// nothing left to make live.
    #[default]
    Nothing,
    /// The spicepod is persisted but not live: exit the process
    /// ([`RuntimeHandle::exit_to_apply`]) so the supervisor relaunches it on
    /// the new configuration.
    ExitToApply,
}

/// What an [`ApplySpicepod`](crate::proto::ApplySpicepod) produced: the document
/// the control plane reads, and what has to happen next for it to take effect.
///
/// `Debug` carries only what already goes to the control plane — the result
/// document and the follow-up action — so there is nothing here a log line
/// could leak. A delivered secret never reaches this type.
#[derive(Debug)]
pub struct ApplyOutcome {
    /// JSON summary of what was applied, returned as the command payload.
    pub document: serde_json::Value,
    pub post_apply: PostApply,
}

impl ApplyOutcome {
    /// The deployment is already live; the result is the whole answer.
    #[must_use]
    pub fn settled(document: serde_json::Value) -> Self {
        Self {
            document,
            post_apply: PostApply::Nothing,
        }
    }

    /// The deployment is persisted and takes effect on the restart the client
    /// triggers once this result is on the wire.
    #[must_use]
    pub fn exit_to_apply(document: serde_json::Value) -> Self {
        Self {
            document,
            post_apply: PostApply::ExitToApply,
        }
    }
}

/// Runtime readiness: the reply to `GetStatus`, and the source of the phase
/// stamped on every heartbeat.
#[derive(Debug, Clone)]
pub struct StatusReport {
    /// Coarse state, reported identically on the heartbeat.
    pub phase: RuntimePhase,
    /// One line explaining why the runtime is in `phase`.
    pub reason: String,
    /// Per-component detail, merged into the `GetStatus` document alongside
    /// `phase` and `reason`. Anything but a JSON object is ignored.
    pub detail: serde_json::Value,
}

impl StatusReport {
    /// Build a report with no extra detail.
    pub fn new(phase: RuntimePhase, reason: impl Into<String>) -> Self {
        Self {
            phase,
            reason: reason.into(),
            detail: serde_json::Value::Null,
        }
    }

    /// Attach the per-component detail document.
    #[must_use]
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = detail;
        self
    }

    /// The JSON document sent as the `GetStatus` payload: `detail` with
    /// `phase` and `reason` layered on top, so the two authoritative fields
    /// always win over anything of the same name in `detail`.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        let mut document = match &self.detail {
            serde_json::Value::Object(map) => map.clone(),
            _ => serde_json::Map::new(),
        };
        document.insert("phase".to_string(), self.phase.as_str().into());
        document.insert("reason".to_string(), self.reason.clone().into());
        serde_json::Value::Object(document)
    }
}

/// Surface area the cloud-connect client needs from the runtime.
///
/// All methods are best-effort. Implementations return a JSON-serializable
/// summary of what they did, or a [`CommandError`] naming the class of
/// failure — never a panic.
#[async_trait]
pub trait RuntimeHandle: Send + Sync + 'static {
    /// Whether this runtime implements `capability`.
    ///
    /// Deliberately has no default: an implementation must state what it can
    /// do, because this is what the client both advertises in `Hello` and
    /// checks before dispatching. A default would let a handle silently
    /// inherit someone else's answer.
    fn supports(&self, capability: Capability) -> bool;

    /// The message returned when `capability` is dispatched anyway.
    ///
    /// The default merely names the command. Override it wherever the
    /// instance can say something the operator can act on — "there is no
    /// supervisor to restart this process" beats "not implemented".
    fn unsupported_reason(&self, capability: Capability) -> String {
        format!(
            "{} is not supported by this instance",
            capability.wire_name()
        )
    }

    /// Number of active datasets currently loaded.
    async fn active_datasets(&self) -> u32 {
        0
    }

    /// Number of active models currently loaded.
    async fn active_models(&self) -> u32 {
        0
    }

    /// Construct the `runtime_info` payload returned for the
    /// `GetRuntimeInfo` command. Implementations should include version,
    /// dataset / model summaries, etc.
    async fn runtime_info_json(&self) -> serde_json::Value {
        serde_json::json!({
            "datasets": self.active_datasets().await,
            "models": self.active_models().await,
        })
    }

    /// Persist a cloud-managed spicepod as the configuration this instance
    /// starts on.
    ///
    /// Applying is **not** a hot reload: the spicepod is validated, persisted,
    /// and made live by a restart ([`PostApply::ExitToApply`]), so a change to
    /// any section takes effect by one path instead of some sections reloading
    /// and the rest waiting for an operator. Redelivering a deployment the
    /// instance is already serving must return [`PostApply::Nothing`] —
    /// restarting for it would make a redelivery a restart loop.
    ///
    /// The default implementation writes the YAML to
    /// `config_dir/spicepod-cloud-managed.yml` via `tokio::fs` so the
    /// filesystem write does not block the runtime worker thread. It cannot
    /// restart the process, so it reports `applied: false` and
    /// [`PostApply::Nothing`]: the file is on disk and the next start — whenever
    /// that is — picks it up.
    ///
    /// The default **refuses** a deployment that carries secrets rather than
    /// writing the spicepod and dropping them: an adapter that cannot apply
    /// secrets would otherwise report success and then fail every referencing
    /// component with a missing-parameter error that names nothing.
    ///
    /// [`SpicepodDeployment::app_id`] is ignored by the default implementation,
    /// which has no metrics pipeline to attribute.
    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<ApplyOutcome, CommandError> {
        if deployment
            .delivered_secrets
            .is_some_and(|secrets| !secrets.is_empty())
        {
            // `Unsupported`, not `Failed`: this adapter will never be able to
            // apply secrets, so a retry cannot help and the control plane should
            // not schedule one.
            return Err(CommandError::unsupported(
                "this runtime adapter cannot apply control-plane-delivered secrets; the spicepod \
                 was NOT written. Implement RuntimeHandle::apply_spicepod to accept them.",
            ));
        }
        let path = deployment
            .config_dir
            .join(crate::config::CLOUD_MANAGED_SPICEPOD_FILE);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| CommandError::failed(format!("create config dir: {e}")))?;
        }
        tokio::fs::write(&path, deployment.spicepod_yaml)
            .await
            .map_err(|e| CommandError::failed(format!("write spicepod: {e}")))?;
        Ok(ApplyOutcome::settled(serde_json::json!({
            "path": path.display().to_string(),
            "applied": false,
            "note": "spicepod written to disk; restart spiced (or implement RuntimeHandle::apply_spicepod) to take effect",
        })))
    }

    /// Exit the process so the supervisor relaunches it on the spicepod
    /// [`RuntimeHandle::apply_spicepod`] just persisted.
    ///
    /// Called only after the command result has been flushed, and only for
    /// [`PostApply::ExitToApply`]. It is not expected to return; an
    /// implementation that returns has failed to exit, and the client says so
    /// rather than leaving the control plane to infer it from a deployment that
    /// never goes live.
    ///
    /// The default reports that this adapter cannot restart itself — which is
    /// why the default `apply_spicepod` never asks for it.
    async fn exit_to_apply(&self) {
        tracing::error!(
            "Spice Cloud Connect: this runtime adapter cannot restart itself, so the persisted \
             spicepod stays pending until the process is restarted"
        );
    }

    /// Restart the runtime with the requested `mode`.
    ///
    /// The default implementation does NOT actually restart — it reports the
    /// command as unsupported so the cloud control plane sees an explicit,
    /// classified failure rather than a false success. Real runtime adapters
    /// (e.g. spiced) override this. Tests/mocks inherit the default so they
    /// don't accidentally kill themselves.
    async fn restart(&self, _mode: RestartMode) -> Result<serde_json::Value, CommandError> {
        Err(CommandError::unsupported(
            "Restart is not implemented for this runtime handle",
        ))
    }

    /// Attempt an in-place runtime upgrade.
    ///
    /// The default reports the command as unsupported rather than returning
    /// an `Ok` payload that says "unsupported" in prose — the control plane
    /// reads the result code, not the document.
    async fn upgrade_runtime(
        &self,
        target_version: &str,
    ) -> Result<serde_json::Value, CommandError> {
        Err(CommandError::unsupported(format!(
            "UpgradeRuntime to {target_version} is not implemented for this runtime handle"
        )))
    }

    /// Return recent runtime log output for a `GetLogs` command, as a single
    /// verbatim text blob (newest lines last). `tail_lines` bounds how many
    /// trailing lines to return; `None` means the implementation's own
    /// default.
    ///
    /// The default reports the command as unsupported so out-of-the-box
    /// `CloudConnect` (and test mocks) don't claim to serve logs they never
    /// captured. Real adapters override this to drain their log buffer.
    async fn get_logs(&self, _tail_lines: Option<u32>) -> Result<String, CommandError> {
        Err(CommandError::unsupported(
            "GetLogs is not implemented in this build",
        ))
    }

    /// Report runtime readiness for a `GetStatus` command, and for the phase
    /// stamped on every heartbeat.
    ///
    /// The default reports the command as unsupported so mocks don't
    /// fabricate a status. Real adapters override this.
    async fn status(&self) -> Result<StatusReport, CommandError> {
        Err(CommandError::unsupported(
            "GetStatus is not implemented in this build",
        ))
    }

    /// The instance's current metrics, as a serialized OTLP
    /// `ExportMetricsServiceRequest`.
    ///
    /// Not a [`Capability`]: the client pushes these on its own cadence rather
    /// than answering a command, so there is nothing to advertise or dispatch.
    ///
    /// `Ok(None)` means this instance has nothing to report — either it does not
    /// export metrics at all, which is the default, it has none yet, or it has not
    /// been told which app to attribute them to (see [`Self::apply_spicepod`]). An
    /// `Err` means collection was attempted and failed; the two are distinct so a
    /// permanently broken collection cannot pass for an idle runtime.
    async fn collect_metrics(&self) -> Result<Option<Vec<u8>>, CommandError> {
        Ok(None)
    }

    /// Execute `sql` through the in-process runtime and return at most
    /// `max_rows` rows as a complete Arrow IPC stream.
    ///
    /// `max_rows` arrives already reduced by [`effective_max_rows`], so an
    /// implementation caps at the value it is handed rather than re-deriving
    /// the limit. Serialization must be bounded by
    /// [`MAX_QUERY_RESULT_BYTES`]: a result that would exceed it returns
    /// [`CommandError::ResultTooLarge`] rather than a truncated stream, and
    /// the bytes are never materialized past the cap.
    ///
    /// Run it read-only. The command arrives from the control plane rather than
    /// from someone holding the instance's own credentials, so an
    /// implementation reads the instance and never changes it.
    ///
    /// `sql` and the values it returns are confidential: keep both out of logs,
    /// traces, and metrics. The returned error message is the one exception —
    /// it reaches the caller who wrote the query and nobody else, so it carries
    /// the engine's diagnostic, which is the only thing that makes a rejected
    /// statement fixable.
    ///
    /// The default reports the command as unsupported so a handle that cannot
    /// query neither advertises `run_query` nor fabricates a result.
    async fn run_query(&self, _sql: &str, _max_rows: u32) -> Result<QueryOutcome, CommandError> {
        Err(CommandError::unsupported(
            "RunQuery is not implemented in this build",
        ))
    }
}

/// The capabilities `runtime` advertises, as the wire names carried in
/// `Hello.capabilities`.
pub(crate) fn advertised_capabilities(runtime: &dyn RuntimeHandle) -> Vec<String> {
    Capability::ALL
        .iter()
        .filter(|capability| runtime.supports(**capability))
        .map(|capability| capability.wire_name().to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Minimal no-op runtime handle, useful for unit tests and as a stand-in
/// when `CloudConnect` is exercised outside the real spiced binary.
///
/// Supports nothing: every optional command is answered as unsupported.
#[derive(Debug, Default, Clone)]
pub struct NoopRuntimeHandle;

#[async_trait]
impl RuntimeHandle for NoopRuntimeHandle {
    fn supports(&self, _capability: Capability) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_runtime_handle_defaults() {
        let h = NoopRuntimeHandle;
        assert_eq!(h.active_datasets().await, 0);
        assert_eq!(h.active_models().await, 0);
        let info = h.runtime_info_json().await;
        assert_eq!(info["datasets"], 0);
        assert_eq!(info["models"], 0);
        for capability in Capability::ALL {
            assert!(
                !h.supports(*capability),
                "the no-op handle supports nothing, but claimed {}",
                capability.wire_name()
            );
        }
        assert!(matches!(
            h.upgrade_runtime("v9.9.9").await,
            Err(CommandError::Unsupported { .. })
        ));
        assert!(matches!(
            h.restart(RestartMode::Graceful).await,
            Err(CommandError::Unsupported { .. })
        ));
        assert!(matches!(
            h.get_logs(Some(100)).await,
            Err(CommandError::Unsupported { .. })
        ));
        assert!(matches!(
            h.status().await,
            Err(CommandError::Unsupported { .. })
        ));
        assert!(matches!(
            h.run_query("SELECT 1", MAX_QUERY_ROWS).await,
            Err(CommandError::Unsupported { .. })
        ));
    }

    /// A handle that can query advertises `run_query`; one that cannot must
    /// not — the control plane rejects unsupported queries on the strength of
    /// this list without a round trip, so a false advertisement is worse than
    /// none.
    #[test]
    fn run_query_is_advertised_only_by_a_handle_that_can_query() {
        struct QueryingHandle;

        #[async_trait]
        impl RuntimeHandle for QueryingHandle {
            fn supports(&self, capability: Capability) -> bool {
                capability == Capability::RunQuery
            }
        }

        assert_eq!(advertised_capabilities(&QueryingHandle), vec!["run_query"]);
        assert!(
            !advertised_capabilities(&NoopRuntimeHandle).contains(&"run_query".to_string()),
            "a handle that cannot query must not advertise run_query"
        );
    }

    #[test]
    fn zero_requested_rows_means_the_default_cap() {
        assert_eq!(effective_max_rows(0), MAX_QUERY_ROWS);
    }

    #[test]
    fn requested_rows_are_clamped_to_the_cap() {
        assert_eq!(effective_max_rows(1), 1);
        assert_eq!(effective_max_rows(MAX_QUERY_ROWS - 1), MAX_QUERY_ROWS - 1);
        assert_eq!(effective_max_rows(MAX_QUERY_ROWS), MAX_QUERY_ROWS);
        assert_eq!(effective_max_rows(MAX_QUERY_ROWS + 1), MAX_QUERY_ROWS);
        assert_eq!(effective_max_rows(u32::MAX), MAX_QUERY_ROWS);
    }

    /// The default apply persists the spicepod but cannot make it live, so it
    /// must not ask the client to exit — nothing would bring the process back.
    #[tokio::test]
    async fn default_apply_persists_without_asking_for_a_restart() {
        let dir = std::env::temp_dir().join(format!(
            "spice-handlers-default-apply-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let outcome = NoopRuntimeHandle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: "version: v2\nkind: Spicepod\nname: default-apply\n",
                delivered_secrets: None,
                app_id: None,
            })
            .await
            .expect("the default apply writes the spicepod");

        assert_eq!(outcome.post_apply, PostApply::Nothing);
        assert_eq!(outcome.document["applied"], false);
        let written = std::fs::read_to_string(dir.join(crate::config::CLOUD_MANAGED_SPICEPOD_FILE))
            .expect("spicepod written");
        assert!(written.contains("name: default-apply"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writing the spicepod and dropping the secrets it references would report
    /// success and then fail every referencing component.
    #[tokio::test]
    async fn default_apply_refuses_delivered_secrets() {
        let dir = std::env::temp_dir().join(format!(
            "spice-handlers-refuse-secrets-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);

        let mut secrets = crate::sealed_secrets::DeliveredSecrets::new();
        secrets.insert(
            "openai_key".to_string(),
            zeroize::Zeroizing::new(b"value".to_vec()),
        );

        let err = NoopRuntimeHandle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: "version: v2\nkind: Spicepod\nname: refused\n",
                delivered_secrets: Some(secrets),
                app_id: None,
            })
            .await
            .expect_err("a handle that cannot apply secrets must refuse the deployment");
        assert!(matches!(err, CommandError::Unsupported { .. }));
        assert!(
            !dir.join(crate::config::CLOUD_MANAGED_SPICEPOD_FILE)
                .exists(),
            "the spicepod must not be written when its secrets were refused"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn noop_runtime_handle_advertises_nothing() {
        assert!(advertised_capabilities(&NoopRuntimeHandle).is_empty());
    }

    #[test]
    fn capability_all_lists_every_variant_exactly_once() {
        // The match is exhaustive over the enum, so a new variant breaks this
        // test at compile time; the expected set then fails until the variant
        // reaches `ALL`. A variant missing from `ALL` is a capability the
        // control plane would never be told about.
        for capability in Capability::ALL {
            match capability {
                Capability::ApplySpicepod
                | Capability::Restart
                | Capability::UpgradeRuntime
                | Capability::GetLogs
                | Capability::GetStatus
                | Capability::RunQuery => {}
            }
        }
        let names: BTreeSet<&str> = Capability::ALL.iter().map(|c| c.wire_name()).collect();
        assert_eq!(
            names.len(),
            Capability::ALL.len(),
            "two capabilities share a wire name"
        );
        assert_eq!(
            names,
            BTreeSet::from([
                "apply_spicepod",
                "get_logs",
                "get_status",
                "restart",
                "run_query",
                "upgrade_runtime",
            ])
        );
    }

    #[test]
    fn status_report_json_carries_phase_and_reason_over_detail() {
        let report = StatusReport::new(RuntimePhase::Ready, "3/3 components ready").with_detail(
            serde_json::json!({
                "phase": "stale-value-that-must-not-win",
                "component_count": 3,
            }),
        );
        let document = report.to_json();
        assert_eq!(document["phase"], "Ready");
        assert_eq!(document["reason"], "3/3 components ready");
        assert_eq!(document["component_count"], 3);
    }

    #[test]
    fn status_report_json_tolerates_non_object_detail() {
        let report = StatusReport::new(RuntimePhase::Failed, "shutting down")
            .with_detail(serde_json::json!("not an object"));
        let document = report.to_json();
        assert_eq!(document["phase"], "Failed");
        assert_eq!(document["reason"], "shutting down");
    }
}
