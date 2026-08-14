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
//! 2. `$SPICE_CONFIG_DIR/identity.json` contains a usable previously-enrolled
//!    identity — reconnection is automatic, with no flag.
//!
//! If neither is true, this module never opens a connection. An existing
//! identity always wins over a supplied `--token`: the key is not redeemed
//! and nothing about it is persisted.
//!
//! ## How a deployment applies
//!
//! `apply_spicepod` validates the incoming spicepod, persists it as
//! `spicepod-cloud-managed.yml`, and applies as much of it as this process can
//! put into effect while it runs. **A deployment never restarts or stops the
//! instance**: `spiced` keeps its process, its connections and its accelerations
//! across every deployment, so nothing outside the instance has to bring it
//! back.
//!
//! [`reconcile`] splits the deployment against the app the runtime is serving.
//! The component sections `Runtime::apply_app` reconciles are applied here and
//! now; every other section is configuration this process read once as it
//! started, so the deployed value is persisted for the next start and the value
//! in effect is kept. The result says which is which: `live` is true only when
//! the whole deployment is in effect, and `restart_required` names the sections
//! that are on disk but not in this process.
//!
//! Two consequences the caller has to hold:
//!
//! - **A deployment can be partly live.** Its datasets, views, catalogs, models
//!   and functions are serving while `restart_required` is non-empty; the named
//!   sections take effect the next time the instance starts, whenever that is.
//! - **The pending set is against *this process*, not against the file.**
//!   Deploying a section back to the value the process is running with clears
//!   it, whatever the file said in between.

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
#[cfg(test)]
use runtime_cloud_connect::config::IDENTITY_FILE;
use runtime_cloud_connect::config::{CLOUD_MANAGED_SPICEPOD_FILE, CloudConnectConfig};
use runtime_cloud_connect::handlers::{
    Capability, CommandError, MAX_QUERY_RESULT_BYTES, QueryOutcome, RuntimeHandle, RuntimePhase,
    SpicepodDeployment, StatusReport, effective_max_rows,
};
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

/// How long a deployment waits for the initial component load before answering
/// that it could not be applied.
///
/// An apply reconciles against the app the runtime has loaded, so it cannot run
/// while the initial load is still installing that app's components: the diff
/// would treat a component the load has not reached yet as already registered,
/// and the load would go on installing the configuration the deployment
/// replaced. The load has no deadline of its own — an unreachable source is
/// retried for as long as the instance is up — so the wait needs one, or a
/// spicepod the runtime cannot satisfy would hold the command stream instead of
/// answering.
///
/// A deployment that outlasts the wait is still validated, persisted and
/// cached; only the reconcile is skipped, so the deployment that fixes an
/// instance whose load cannot finish is on disk for the next start rather than
/// lost. The reconcile cannot simply proceed instead: abandoning the load would
/// strand every component it had not reached (a diff skips what the two apps
/// agree on, so nothing would ever register them), and loading them here
/// inherits the load's own unbounded per-dataset retry, which would hold the
/// apply — and with it every later command — indefinitely. Bounding that retry
/// is [#12862](https://github.com/spiceai/spiceai/issues/12862); once the load
/// itself is bounded, this wait always settles.
const INITIAL_LOAD_BUDGET: Duration = Duration::from_mins(2);

/// The one durable-state decision made before the runtime is built.
///
/// The one-time `--token` is consumed before this runs, so it is never an
/// activation signal. A usable identity exclusively activates credentialed
/// facilities; observing an unusable identity only preserves the instance's
/// cloud-managed configuration provenance.
pub(crate) struct StartupState {
    config_dir: PathBuf,
    identity_observed: bool,
    reconnectable_identity: Option<runtime_cloud_connect::ReconnectableIdentity>,
}

impl StartupState {
    #[must_use]
    pub(crate) fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    #[must_use]
    pub(crate) fn identity_observed(&self) -> bool {
        self.identity_observed
    }

    #[must_use]
    pub(crate) fn into_identity(self) -> Option<runtime_cloud_connect::ReconnectableIdentity> {
        self.reconnectable_identity
    }
}

pub(crate) async fn load_startup_state() -> StartupState {
    load_startup_state_from_config(&build_config(env!("CARGO_PKG_VERSION"))).await
}

/// Load the durable activation snapshot without making an optional Cloud
/// Connect configuration failure fatal to the rest of the runtime.
///
/// The error is still surfaced at ERROR through the temporary startup
/// subscriber installed by `main`; the absent activation token makes every
/// credentialed Cloud Connect facility fail closed while configuration
/// provenance remains available so the runtime can keep serving.
async fn load_startup_state_from_config(config: &CloudConnectConfig) -> StartupState {
    match runtime_cloud_connect::load_reconnectable_identity_async(config).await {
        Ok(reconnectable_identity) => StartupState {
            config_dir: config.config_dir.clone(),
            identity_observed: reconnectable_identity.is_some(),
            reconnectable_identity,
        },
        Err(error) => {
            tracing::error!(
                "Spice Cloud Connect is disabled for this start because its durable identity could not be activated: {error}"
            );
            StartupState {
                config_dir: config.config_dir.clone(),
                // Preserve deployment provenance only when identity contents
                // were actually observed. An I/O failure can instead mean the
                // whole config directory is unavailable; treating that as
                // cloud-managed would make a second read failure abort startup.
                identity_observed: identity_contents_were_observed(&error),
                reconnectable_identity: None,
            }
        }
    }
}

fn identity_contents_were_observed(error: &runtime_cloud_connect::Error) -> bool {
    matches!(
        error,
        runtime_cloud_connect::Error::IdentityUnusable { .. }
            | runtime_cloud_connect::Error::IdentityValidationTaskPanicked { .. }
            | runtime_cloud_connect::Error::IdentityLoad {
                source: runtime_cloud_connect::identity::Error::Parse { .. },
                ..
            }
    )
}

/// Apply the optional instance-local `cloud-endpoint` override. This overrides
/// the cloud **enroll** endpoint (state plane); the gateway (stream) address
/// comes from the enroll response. An unsafe or unreadable file is an error so
/// the caller cannot silently renew against another control plane.
fn apply_endpoint_override(config: &mut CloudConnectConfig) -> std::io::Result<()> {
    if let Some(override_endpoint) =
        CloudConnectConfig::read_normalized_enroll_endpoint_override(&config.config_dir)
            .map_err(std::io::Error::other)?
    {
        config.enroll_endpoint = override_endpoint;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum DiskControlPlaneEndpoint {
    Absent,
    Resolved(String),
    Invalid,
}

/// Read the control plane durably bound by enrollment. A malformed durable
/// state file is distinct from an absent binding: callers must fail closed
/// instead of silently substituting the public control plane.
fn read_bound_endpoint(config_dir: &Path) -> DiskControlPlaneEndpoint {
    let identity_path = config_dir.join(runtime_cloud_connect::config::IDENTITY_FILE);
    let identity = match runtime_cloud_connect::IdentityStore::load_optional(&identity_path) {
        Ok(identity) => identity,
        Err(error) => {
            tracing::warn!(
                "Spice Cloud Connect could not read the enrolled control-plane binding in {}: {error}",
                identity_path.display()
            );
            return DiskControlPlaneEndpoint::Invalid;
        }
    };
    if let Some(endpoint) = identity.and_then(|identity| identity.control_plane_endpoint) {
        return normalize_disk_endpoint(config_dir, &endpoint, "enrolled");
    }

    match runtime_cloud_connect::EnrollmentDraft::load_optional(config_dir) {
        Ok(Some(draft)) => normalize_disk_endpoint(config_dir, &draft.binding.endpoint, "pending"),
        Ok(None) => DiskControlPlaneEndpoint::Absent,
        Err(error) => {
            tracing::warn!(
                "Spice Cloud Connect could not read the pending control-plane binding in {}: {error}",
                config_dir.display()
            );
            DiskControlPlaneEndpoint::Invalid
        }
    }
}

fn normalize_disk_endpoint(
    config_dir: &Path,
    endpoint: &str,
    binding: &str,
) -> DiskControlPlaneEndpoint {
    match runtime_cloud_connect::config::normalize_control_plane_endpoint(endpoint) {
        Ok(endpoint) => DiskControlPlaneEndpoint::Resolved(endpoint),
        Err(error) => {
            tracing::warn!(
                "Spice Cloud Connect found an invalid {binding} control-plane binding in {}: {error}",
                config_dir.display()
            );
            DiskControlPlaneEndpoint::Invalid
        }
    }
}

/// Resolve the endpoint stored on disk. A durable identity/draft binding wins;
/// the operator-authored legacy file is consulted only before a binding exists
/// and is then promoted into the identity by enrollment or successful renewal.
fn read_disk_endpoint(config_dir: &Path) -> DiskControlPlaneEndpoint {
    match read_bound_endpoint(config_dir) {
        DiskControlPlaneEndpoint::Absent => {}
        resolved => return resolved,
    }

    match CloudConnectConfig::read_normalized_enroll_endpoint_override(config_dir) {
        Ok(Some(endpoint)) => DiskControlPlaneEndpoint::Resolved(endpoint),
        Ok(None) => DiskControlPlaneEndpoint::Absent,
        Err(error) => {
            tracing::warn!(
                "Spice Cloud Connect could not read the configured control-plane endpoint in {}: {error}",
                config_dir.display()
            );
            DiskControlPlaneEndpoint::Invalid
        }
    }
}

/// Build a [`CloudConnectConfig`] from env + on-disk state.
fn build_config(runtime_version: &str) -> CloudConnectConfig {
    let mut config = CloudConnectConfig::from_env(runtime_version);
    let Some(requested) = std::env::var_os("SPICE_CLOUD_ENDPOINT")
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
    else {
        match read_disk_endpoint(&config.config_dir) {
            DiskControlPlaneEndpoint::Resolved(endpoint) => config.enroll_endpoint = endpoint,
            DiskControlPlaneEndpoint::Absent => {}
            // Leave an invalid value in the config so enrollment/renewal client
            // construction refuses to send credentials anywhere. Falling back
            // to the public endpoint would turn a local read error into a
            // cross-control-plane request.
            DiskControlPlaneEndpoint::Invalid => config.enroll_endpoint.clear(),
        }
        return config;
    };

    match reconcile_requested_endpoint(&requested, read_bound_endpoint(&config.config_dir)) {
        Ok(endpoint) => config.enroll_endpoint = endpoint,
        Err(reason) => {
            tracing::error!("Spice Cloud Connect is disabled for this start because {reason}");
            config.enroll_endpoint.clear();
        }
    }
    config
}

/// Reconcile `SPICE_CLOUD_ENDPOINT` against the control plane bound on disk.
///
/// An environment override never silently retargets an instance that already
/// enrolled somewhere: renewal would send this instance's credential to a
/// control plane it never enrolled with. `spice connect` rejects exactly this
/// mismatch, so the runtime has to agree with it rather than quietly win.
///
/// The legacy endpoint file is deliberately not consulted — it is superseded
/// once a binding exists, and it was never an enrollment.
fn reconcile_requested_endpoint(
    requested: &str,
    bound: DiskControlPlaneEndpoint,
) -> std::result::Result<String, String> {
    let requested = runtime_cloud_connect::config::normalize_control_plane_endpoint(requested)
        .map_err(|error| {
            format!("SPICE_CLOUD_ENDPOINT is not a usable control-plane endpoint: {error}")
        })?;
    match bound {
        DiskControlPlaneEndpoint::Absent => Ok(requested),
        DiskControlPlaneEndpoint::Resolved(bound) if requested == bound => Ok(bound),
        DiskControlPlaneEndpoint::Resolved(bound) => Err(format!(
            "SPICE_CLOUD_ENDPOINT ({requested}) does not match the control plane this instance enrolled with ({bound}). Unset the variable, or release the instance with `spice connect remove` before enrolling elsewhere."
        )),
        DiskControlPlaneEndpoint::Invalid => {
            Err("the control-plane binding on disk could not be read, so the requested endpoint cannot be checked against it.".to_string())
        }
    }
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

    #[snafu(display(
        "Failed to enroll this instance with Spice Cloud: the enrollment endpoint override at {} could not be read safely: {source}",
        path.display()
    ))]
    EndpointOverride {
        path: PathBuf,
        source: std::io::Error,
    },

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
/// (checked locally, never echoed), the endpoint override cannot be read
/// safely, or [`enroll_now`] fails terminally or exhausts its retry budget.
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

    let mut config = CloudConnectConfig::from_env(env!("CARGO_PKG_VERSION"));
    if std::env::var_os("SPICE_CLOUD_ENDPOINT").is_none() {
        let path = config.config_dir.join("cloud-endpoint");
        apply_endpoint_override(&mut config)
            .map_err(|source| BootstrapEnrollmentError::EndpointOverride { path, source })?;
    }
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

/// The cloud-managed spicepod this instance starts on, or `None` when no
/// durable identity state was observed or no deployment has ever landed here.
///
/// Reads files only — no control-plane round trip — so an instance whose
/// credentials are expired, corrupt, or otherwise unusable still comes up on
/// its deployed configuration without activating Cloud Connect.
pub async fn cloud_managed_spicepod(
    config_dir: &Path,
    identity_observed: bool,
) -> std::result::Result<Option<CloudManagedSpicepod>, CloudManagedSpicepodReadError> {
    if !identity_observed {
        return Ok(None);
    }
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
    runtime: &Arc<Runtime>,
    identity: Option<&runtime_cloud_connect::ReconnectableIdentity>,
) -> Option<DeliveredSecretsState> {
    let identity = identity?;
    let config = identity.config();

    // Registered as a built-in so `${ secrets:NAME }` reaches it with nothing
    // declared in the spicepod, it sits below every user-declared store, and a
    // spicepod reload cannot clear it.
    let store = Arc::new(CloudDeliveredSecretStore::new());
    runtime.secrets().write().await.register_builtin_store(
        CLOUD_DELIVERED_STORE,
        Arc::clone(&store) as Arc<dyn runtime::secrets::SecretStore>,
    );

    load_cached_secrets(config, &store, identity);
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
/// deployed one that failed to build). It is what tells a redelivery of the
/// deployment already on disk from a new one, so passing a configuration this
/// instance did not come up on would let a redelivery be answered as already
/// applied when it is not.
///
/// `runtime_overrides` are the process's `--set-runtime` values, which a
/// deployment has to carry the same way a start onto the same file would.
pub async fn maybe_start(
    runtime: Arc<Runtime>,
    identity: Option<runtime_cloud_connect::ReconnectableIdentity>,
    delivered_secrets: Option<DeliveredSecretsState>,
    running_deployment: Option<CloudManagedSpicepod>,
    metrics: Option<MetricsReader>,
    runtime_overrides: Vec<(String, String)>,
) -> Option<CloudConnect> {
    let Some(identity) = identity else {
        tracing::debug!("Spice Cloud Connect: disabled (no usable persisted identity)");
        return None;
    };
    let config = identity.config();
    // Restores metrics attribution across a restart. Without it the instance
    // exports nothing until its next deploy, which may be days.
    let persisted_app_id = identity.app_id().map(str::to_string);

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
        load_cached_secrets(config, &store, &identity);
        store
    };

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
            metrics,
            app_id: persisted_app_id,
            runtime_overrides,
            initial_load_budget: INITIAL_LOAD_BUDGET,
        }));

    Some(CloudConnect::start_reconnectable(handle, identity))
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
fn load_cached_secrets(
    config: &CloudConnectConfig,
    store: &CloudDeliveredSecretStore,
    identity: &runtime_cloud_connect::ReconnectableIdentity,
) {
    let Some(key) = identity.cache_key() else {
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

/// A deployment split into what this process can serve and what it cannot.
struct Reconciled {
    /// The app to install: the deployment's components over the start-time
    /// configuration this process is running with.
    app: App,
    /// The sections whose deployed value is not the one in effect here, sorted.
    /// Empty when the whole document is in effect.
    restart_required: Vec<String>,
}

/// Split `deployed` against the app this process is serving.
///
/// [`Runtime::apply_app`] reconciles catalogs, datasets, views, models and
/// functions into a running process, so those are taken from the deployment.
/// Every other section configures something that was built while the process
/// started — the servers, the CPU budget, the embedding models, the secret
/// stores — and nothing re-reads it, so the deployed value is persisted for the
/// next start while the value in effect is kept and named in
/// [`Reconciled::restart_required`]. Keeping it is what makes the app this
/// process reports the app it is actually serving, which is in turn what lets
/// the next deployment be diffed against something true.
///
/// The values delivered alongside the document are classified separately, by
/// [`SpicedRuntimeHandle::install_delivered_secrets`], and reported under the
/// same `secrets` name — see [`Pending`].
///
/// The [`App`] is destructured rather than compared field by field so that a
/// new field does not compile until it has been classified as one or the other.
fn reconcile(active: &App, deployed: App) -> Reconciled {
    let App {
        name,
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
        snapshots,
    } = deployed;

    let mut restart_required = Vec::new();

    let app = App {
        // Reconciled into this process.
        catalogs,
        datasets,
        views,
        models,
        functions,
        // Read at start only to label anonymous telemetry, which is already
        // running: the label keeps the name the instance started under, and
        // nothing else in a running process is built from the app's name. Take
        // the deployed one so what this instance reports serving is named the
        // way the deployment names it.
        name,
        // The documents the deployment was built from rather than configuration
        // of their own — every section they carry is classified here — so they
        // describe the deployment that is persisted, which is what the
        // `/v1/spicepods` listing answers with.
        spicepods,
        // Read once, into something already built.
        secrets: in_effect("secrets", &active.secrets, &secrets, &mut restart_required),
        extensions: in_effect(
            "extensions",
            &active.extensions,
            &extensions,
            &mut restart_required,
        ),
        embeddings: in_effect(
            "embeddings",
            &active.embeddings,
            &embeddings,
            &mut restart_required,
        ),
        rerankers: in_effect(
            "rerankers",
            &active.rerankers,
            &rerankers,
            &mut restart_required,
        ),
        tools: in_effect("tools", &active.tools, &tools, &mut restart_required),
        // Reconciled only in a build without the `models` feature, so it counts
        // as start-time in every build: a section named once too often costs a
        // restart-required path the next start clears, while one the runtime
        // does not reconcile in every build would drop the change silently.
        workers: in_effect("workers", &active.workers, &workers, &mut restart_required),
        runtime: in_effect("runtime", &active.runtime, &runtime, &mut restart_required),
        management: in_effect(
            "management",
            &active.management,
            &management,
            &mut restart_required,
        ),
        snapshots: in_effect(
            "snapshots",
            &active.snapshots,
            &snapshots,
            &mut restart_required,
        ),
    };

    restart_required.sort();
    Reconciled {
        app,
        restart_required,
    }
}

/// What the deployment on disk changes that this process is not serving.
///
/// The document's sections and the values delivered with it are kept apart
/// because a later deployment settles them separately — reverting a section
/// says nothing about a rotation, and redelivering the values in effect says
/// nothing about the document. They are reported as one set, under one name:
/// what an operator has to change is "the app's secrets" either way.
#[derive(Default)]
struct Pending {
    /// Sections of the document, from [`Reconciled::restart_required`].
    sections: Vec<String>,
    /// Whether the delivered values are not the ones the app runs on — either
    /// because this process could not install them over values its components
    /// have already resolved, or because they could not be written to the cache
    /// the next start reads. Both mean a start would not serve what was
    /// deployed, which is what the caller has to know.
    secrets: bool,
}

impl Pending {
    /// The sorted, deduplicated set the control plane reads. Empty when the
    /// deployment on disk is the one this process is serving.
    fn restart_required(&self) -> Vec<String> {
        let mut restart_required = self.sections.clone();
        if self.secrets {
            restart_required.push("secrets".to_string());
            restart_required.sort();
            restart_required.dedup();
        }
        restart_required
    }
}

/// Keep the value this process is running with, naming `section` when the
/// deployment changes it.
fn in_effect<T: Clone + PartialEq>(
    section: &str,
    active: &T,
    deployed: &T,
    restart_required: &mut Vec<String>,
) -> T {
    if active != deployed {
        restart_required.push(section.to_string());
    }
    active.clone()
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
    /// The deployment this process has applied: the spicepod it put into effect
    /// as far as it could, and against which [`SpicedRuntimeHandle::pending`]
    /// is measured. `None` until a deployment lands on an instance that booted on
    /// something else, which matches no incoming deployment.
    ///
    /// Usually also the spicepod on disk, but not always: a deployment
    /// persisted while the initial load was unfinished is on disk without
    /// having been applied, so redelivering it re-applies it rather than being
    /// answered from state that never described it.
    ///
    /// The other half of the pair — the configuration in effect — is the app
    /// the runtime holds, so there is nothing here to keep in step with it.
    ///
    /// Guarded by a `parking_lot` lock held for the read/write only, never
    /// across an `.await`.
    desired: RwLock<Option<String>>,
    /// What [`SpicedRuntimeHandle::desired`] changes that this process is not
    /// serving, as the last apply classified it. Empty when the deployment it
    /// applied is the one being served.
    ///
    /// Reported by `GetStatus` as well as by the apply that produced it, so the
    /// control plane sees the same set whether it watched the deploy or asked
    /// afterwards. A `parking_lot` lock held for the read/write only, never
    /// across an `.await`.
    pending: RwLock<Pending>,
    /// The `--set-runtime` overrides this process was started with, applied to a
    /// deployment the same way the start path applies them so what an apply
    /// installs is the app a start onto the same file would have produced.
    runtime_overrides: Vec<(String, String)>,
    /// Serializes an apply end to end — the classification, the persistence it
    /// commits to, and the state update that follows — so two dispatches cannot
    /// interleave one's decision with the other's state change.
    applying: tokio::sync::Mutex<()>,
    /// How long an apply waits for the initial component load before persisting
    /// the deployment without reconciling it ([`INITIAL_LOAD_BUDGET`]). Held
    /// rather than read from the constant so a test can exercise the bound
    /// without waiting it out.
    initial_load_budget: Duration,
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
/// reduced to the spicepod on disk and the mutable fields are wrapped.
struct SpicedRuntimeHandleParts {
    runtime: Arc<Runtime>,
    logs: Option<LogRingBuffer>,
    delivered_secrets: Arc<CloudDeliveredSecretStore>,
    identity_path: std::path::PathBuf,
    running_deployment: Option<CloudManagedSpicepod>,
    metrics: Option<MetricsReader>,
    app_id: Option<String>,
    runtime_overrides: Vec<(String, String)>,
    initial_load_budget: Duration,
}

impl SpicedRuntimeHandle {
    fn new(parts: SpicedRuntimeHandleParts) -> Self {
        let SpicedRuntimeHandleParts {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            running_deployment,
            metrics,
            app_id,
            runtime_overrides,
            initial_load_budget,
        } = parts;
        // The deployment this process started on is both what is on disk and
        // what is in effect, so nothing is pending against it.
        let desired = running_deployment.map(|running| running.spicepod_yaml);
        Self {
            runtime,
            logs,
            delivered_secrets,
            identity_path,
            desired: RwLock::new(desired),
            pending: RwLock::new(Pending::default()),
            runtime_overrides,
            applying: tokio::sync::Mutex::new(()),
            initial_load_budget,
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

    /// Persist the delivered secrets so the next start comes back up with them,
    /// without a control-plane round trip.
    ///
    /// Best-effort by design: what this process resolves is already settled by
    /// the time this runs, so a cache failure costs a redeploy after the next
    /// start rather than the deployment. It is reported in the command result so
    /// the operator is not left to discover it at start time.
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
    /// start needs it.
    fn refresh_secret_cache(
        &self,
        config_dir: &Path,
        delivered_secrets: Option<&runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
    ) -> Option<String> {
        self.cache_delivered_secrets(config_dir, delivered_secrets?)
    }

    /// Install the delivered values this instance does not hold, and report the
    /// ones it cannot change in place.
    ///
    /// A name the store does not hold is new to this process: nothing has ever
    /// resolved it here, so installing it changes no component's view of it and
    /// the components this deployment adds can resolve it as they load. A name
    /// the store already holds is different: a component resolves
    /// `${ secrets:… }` once, while it loads, so a rotated or withdrawn value
    /// only reaches the components already holding the old one by loading them
    /// again. Those keep the value in effect and are named `secrets` in
    /// `restart_required` — installing them would leave the components loaded
    /// before the deployment authenticating with one value and the ones loaded
    /// after it with another.
    ///
    /// Returns whether anything is pending. The names are logged, never the
    /// values.
    fn install_delivered_secrets(
        &self,
        secrets: &runtime_cloud_connect::sealed_secrets::DeliveredSecrets,
    ) -> bool {
        let update = self.delivered_secrets.install_new(secrets);
        if !update.installed.is_empty() {
            tracing::info!(
                "Spice Cloud Connect: the deployment delivered {} secret(s) this instance did not hold, now resolvable: {}",
                update.installed.len(),
                update.installed.join(", ")
            );
        }
        if !update.pending.is_empty() {
            tracing::warn!(
                "Spice Cloud Connect: the deployment changes {} secret(s) this instance has already resolved ({}); the values in use stay in effect until spiced starts again. See: https://spiceai.org/docs",
                update.pending.len(),
                update.pending.join(", ")
            );
        }
        !update.pending.is_empty()
    }

    /// Persist the deployment and put as much of it into effect as this process
    /// can, without restarting.
    ///
    /// Ordered so that a deployment that does not land leaves the running
    /// instance as it was, and one interrupted part-way leaves it able to start:
    ///
    /// 1. The spicepod is validated before the delivered secrets are touched at
    ///    all. Building the [`App`] parses the document rather than resolving
    ///    `${ secrets:… }` — a component does that as it loads — so a deployment
    ///    that does not build changes nothing.
    /// 2. The secrets are cached and installed before the initial load is waited
    ///    on. The cache is what the next start reads, and the start that reads it
    ///    is the one that reads the spicepod, so an instance interrupted before
    ///    the promotion comes back on the previous spicepod with the current
    ///    credentials cached for it rather than on a spicepod whose secrets it
    ///    cannot resolve. Installing them this early is what lets a deployment
    ///    carrying a credential the running instance is missing unblock the very
    ///    load it then waits on: only values nothing has resolved yet are
    ///    installed, so nothing already loaded changes underneath.
    /// 3. What this process serves changes last, so a promotion that fails
    ///    leaves the instance serving the configuration it already had.
    ///
    /// Components converge one at a time: one that fails to build lands in an
    /// error state, stays visible through `GetStatus`, and is retried by the
    /// next deployment — the same place a component that fails at boot lands.
    /// Nothing is rolled back, and the result does not claim otherwise.
    async fn apply(
        &self,
        config_dir: &Path,
        spicepod_yaml: &str,
        delivered_secrets: Option<runtime_cloud_connect::sealed_secrets::DeliveredSecrets>,
    ) -> Result<serde_json::Value, CommandError> {
        let staged =
            stage_cloud_managed_spicepod(config_dir, spicepod_yaml, &self.runtime_overrides)
                .await?;

        let mut cache_error = None;
        if let Some(secrets) = &delivered_secrets {
            cache_error = self.cache_delivered_secrets(config_dir, secrets);
        }

        // `None` when the deployment carried no payload at all, which says
        // nothing about the values in effect — a rotation pending from an
        // earlier deployment stays pending.
        let mut secrets_pending = None;
        let delivered_names = delivered_secrets.map(|secrets| {
            secrets_pending = Some(self.install_delivered_secrets(&secrets));
            secrets.keys().cloned().collect::<Vec<String>>()
        });

        // The load installs the components this deployment is diffed against, so
        // it has to be over before anything is reconciled: a component it has
        // not reached is not registered, and a diff would treat it as though it
        // were. The wait is bounded, and a deployment that outlasts it is still
        // persisted — the instance keeps serving what it was serving, and a
        // start (an operator's, or one it takes for its own reasons) comes up on
        // the deployment rather than on the configuration it was sent to
        // replace.
        if !self
            .runtime
            .wait_for_initial_load(self.initial_load_budget)
            .await
        {
            let (_, path) = staged.promote().await?;
            tracing::warn!(
                "Spice Cloud Connect: the deployed spicepod was validated and persisted to {}, but this instance is still loading the components of the spicepod it is serving, so the deployment was not applied to the running process. Restart the instance to serve it. See: https://spiceai.org/docs",
                path.display(),
            );
            return Err(unfinished_load_error(
                &path,
                self.initial_load_budget,
                cache_error.as_deref(),
            ));
        }

        let (deployed_app, path) = staged.promote().await?;

        // The app the runtime holds is the configuration in effect here: every
        // deployment installs what this process serves, so reading it back is
        // what keeps a deployment from being diffed against a file this process
        // never applied. An instance that came up with no app at all is running
        // on defaults, which is what an empty app describes.
        let active: Arc<App> = self.runtime.read_app().await.unwrap_or_default();
        let Reconciled {
            app,
            restart_required: sections,
        } = reconcile(&active, deployed_app);
        let counts = ComponentCounts::of(&app);

        Arc::clone(&self.runtime).apply_app(Arc::new(app)).await;
        *self.desired.write() = Some(spicepod_yaml.to_string());
        let restart_required = {
            let mut pending = self.pending.write();
            pending.sections = sections;
            if let Some(secrets) = secrets_pending {
                // A delivered set the next start would not resolve is pending
                // just as much as one this process cannot install: either way
                // what is on disk is not what the app runs on.
                pending.secrets = secrets || cache_error.is_some();
            }
            pending.restart_required()
        };

        if restart_required.is_empty() {
            tracing::info!(
                "Spice Cloud Connect: the deployed spicepod was validated, persisted to {} ({counts}), and applied to this running instance",
                path.display(),
            );
        } else {
            tracing::warn!(
                "Spice Cloud Connect: the deployed spicepod was validated, persisted to {} ({counts}), and its components applied to this running instance; {} is read when spiced starts, so the value this instance is running with stays in effect until it next starts. See: https://spiceai.org/docs",
                path.display(),
                restart_required.join(", "),
            );
        }

        Ok(serde_json::json!({
            "path": path.display().to_string(),
            "applied": true,
            "live": restart_required.is_empty(),
            // The sections that are on disk but not in this process, so a
            // caller can tell an operator what a start would change and what
            // to revert to clear it.
            "restart_required": restart_required,
            "message": apply_message(&restart_required),
            "datasets": counts.datasets,
            "models": counts.models,
            "catalogs": counts.catalogs,
            "views": counts.views,
            // Names only — a delivered value never leaves this process.
            "delivered_secrets": delivered_names,
            "secrets_cache_error": cache_error,
        }))
    }
}

/// What the control plane is told about a deployment the instance persisted but
/// could not reconcile, because the components it would be diffed against are
/// still being loaded.
///
/// Precise about what did happen: the app attribution was recorded, the
/// spicepod is on disk, and the delivered secrets are installed and cached — it
/// is the running process that is unchanged, and a start is what makes the
/// deployment live.
fn unfinished_load_error(path: &Path, budget: Duration, cache_error: Option<&str>) -> CommandError {
    let cache = cache_error.map_or_else(String::new, |error| {
        format!(
            " Its secrets could not be cached for that start ({error}), so deploy them again once this instance is loaded."
        )
    });
    CommandError::failed(format!(
        "Failed to apply the deployed Spicepod to the running instance: it is still loading the components of the Spicepod it is serving and did not finish within {}s, so nothing was reconciled and the instance keeps serving its current configuration. The deployed Spicepod was validated and persisted to {}, and any secrets it delivered were installed, so restarting the instance serves it; GetStatus names the components the load is waiting on.{cache} See: https://spiceai.org/docs",
        budget.as_secs(),
        path.display(),
    ))
}

/// What the control plane is told about a deployment that has been applied.
fn apply_message(restart_required: &[String]) -> String {
    if restart_required.is_empty() {
        return "The spicepod was validated, persisted, and applied to this running instance, which is serving it. Its components reconcile one at a time, so some may still be loading: GetStatus reports which are ready, and one that fails to load stays there and is retried by the next deployment.".to_string();
    }
    format!(
        "The spicepod was validated, persisted, and its components applied to this running instance, which keeps serving. These sections are read when spiced starts, so the values this instance is running with stay in effect until it next starts: {}. Restart the instance to serve them, or deploy them back to the values it is running with.",
        restart_required.join(", ")
    )
}

#[async_trait]
impl RuntimeHandle for SpicedRuntimeHandle {
    /// What a standalone `spiced` can answer. `Restart` is excluded
    /// deliberately: nothing this instance does to itself ends its process, and
    /// stopping one that may have nothing to bring it back is not a control the
    /// portal should offer.
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
            Capability::Restart => "Restart is unsupported on standalone spiced: it is not a control the runtime offers on demand, and no deployment needs one — a deployment applies to the running instance and reports anything it could not put into effect. To restart the instance anyway, use your process manager (systemd/Docker/Kubernetes). See: https://spiceai.org/docs".to_string(),
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

    /// Apply a cloud-managed spicepod to this running instance.
    ///
    /// 1. A redelivery of the deployment this process has applied — the same
    ///    YAML, byte for byte, delivering the secret values this instance
    ///    already holds — is answered from the state the last apply left,
    ///    without rebuilding or reconciling anything. It does write the
    ///    delivered secrets to the cache the next start reads
    ///    ([`SpicedRuntimeHandle::refresh_secret_cache`]), which is the one
    ///    thing a redelivery can still repair.
    /// 2. Anything else is validated by building an [`App`] from it on a sibling
    ///    temp file, so a malformed push is rejected with a clear error and the
    ///    previous good `spicepod-cloud-managed.yml` is left untouched and still
    ///    running; the validated file is then promoted to the canonical path so
    ///    the next start comes up on this configuration.
    /// 3. [`SpicedRuntimeHandle::apply`] then puts into effect everything this
    ///    process can ([`reconcile`]) and reports the rest as
    ///    `restart_required`. Nothing here exits, signals, or drains the
    ///    process.
    async fn apply_spicepod(
        &self,
        deployment: SpicepodDeployment<'_>,
    ) -> Result<serde_json::Value, CommandError> {
        let SpicepodDeployment {
            config_dir,
            spicepod_yaml,
            delivered_secrets,
            app_id,
        } = deployment;

        // One apply at a time. What this deployment is diffed against is only
        // immutable if nothing else can persist a spicepod, install secrets, or
        // change the app being served between reading it and completing the
        // apply that follows.
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

        // A delivered set this instance already holds in full changes nothing,
        // so it does not make a redelivery into an apply. Anything else — a new
        // name, a rotated value, a withdrawn one — is a change the spicepod text
        // does not show.
        let secrets_settled = delivered_secrets
            .as_ref()
            .is_none_or(|secrets| self.delivered_secrets.holds(secrets));

        // Cloned out of the lock rather than compared under it: the guard must
        // not be held across the awaits below.
        let desired = self.desired.read().clone();
        if secrets_settled && desired.as_deref() == Some(spicepod_yaml) {
            let cache_error = self.refresh_secret_cache(config_dir, delivered_secrets.as_ref());
            let restart_required = {
                let mut pending = self.pending.write();
                // A delivery this instance holds in full settles a rotation an
                // earlier one left pending — but only once the cache holds it
                // too, because that is what the next start resolves.
                if delivered_secrets.is_some() {
                    pending.secrets = cache_error.is_some();
                }
                pending.restart_required()
            };
            tracing::info!(
                "Spice Cloud Connect: the deployed spicepod is the one this instance already applied; nothing to reconcile"
            );
            return Ok(serde_json::json!({
                "path": config_dir.join(CLOUD_MANAGED_SPICEPOD_FILE).display().to_string(),
                "applied": true,
                "live": restart_required.is_empty(),
                "restart_required": restart_required,
                "message": apply_message(&restart_required),
                "secrets_cache_error": cache_error,
            }));
        }

        self.apply(config_dir, spicepod_yaml, delivered_secrets)
            .await
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
    /// `components`/`errors` detail, alongside `restart_required` — what the
    /// deployment on disk changes that this process is not serving, which is
    /// what says whether the last deployment landed in full.
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

        // What the deployment on disk changes that this process is not serving.
        // The same set the apply that produced it returned, so the control plane
        // reads one answer whether it watched the deploy or asked afterwards.
        let restart_required = self.pending.read().restart_required();

        Ok(
            StatusReport::new(phase, reason).with_detail(serde_json::json!({
                "ready": ready,
                "component_count": total,
                "ready_count": ready_count,
                "components": components,
                "errors": errors,
                "restart_required": restart_required,
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
    use runtime_cloud_connect::handlers::MAX_QUERY_ROWS;
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

    #[test]
    fn legacy_endpoint_file_is_used_only_until_a_durable_binding_exists() {
        let dir = scratch_dir("legacy-control-plane-endpoint");
        std::fs::write(
            dir.join("cloud-endpoint"),
            "https://legacy-control.example/\n",
        )
        .expect("write legacy endpoint");
        assert_eq!(
            read_disk_endpoint(&dir),
            DiskControlPlaneEndpoint::Resolved("https://legacy-control.example".to_string())
        );

        let identity_path = dir.join(IDENTITY_FILE);
        enroll_with_a_cache_key(&identity_path);
        let mut identity = IdentityStore::load_optional(&identity_path)
            .expect("load identity")
            .expect("identity exists");
        identity.control_plane_endpoint = Some("https://bound-control.example".to_string());
        IdentityStore::store(&identity_path, &identity).expect("store durable binding");
        assert_eq!(
            read_disk_endpoint(&dir),
            DiskControlPlaneEndpoint::Resolved("https://bound-control.example".to_string())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unreadable_durable_binding_never_falls_back_to_an_endpoint_file() {
        let dir = scratch_dir("invalid-control-plane-binding");
        std::fs::write(dir.join(IDENTITY_FILE), "not identity JSON")
            .expect("write invalid identity");
        std::fs::write(
            dir.join("cloud-endpoint"),
            "https://attacker-controlled.example\n",
        )
        .expect("write fallback endpoint");

        assert_eq!(read_disk_endpoint(&dir), DiskControlPlaneEndpoint::Invalid);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An environment override must not retarget an enrolled instance: renewal
    /// would carry this instance's credential to a control plane it never
    /// enrolled with. `spice connect` refuses the same mismatch.
    #[test]
    fn an_environment_endpoint_never_overrides_a_durable_binding() {
        let bound = || DiskControlPlaneEndpoint::Resolved("https://bound.example".to_string());

        assert_eq!(
            reconcile_requested_endpoint("https://bound.example/", bound()),
            Ok("https://bound.example".to_string())
        );
        assert!(
            reconcile_requested_endpoint("https://elsewhere.example", bound()).is_err(),
            "a mismatched environment endpoint must fail closed"
        );
        assert!(
            reconcile_requested_endpoint(
                "https://elsewhere.example",
                DiskControlPlaneEndpoint::Invalid
            )
            .is_err(),
            "an unreadable binding must fail closed rather than trust the environment"
        );
        assert_eq!(
            reconcile_requested_endpoint("https://fresh.example", DiskControlPlaneEndpoint::Absent),
            Ok("https://fresh.example".to_string()),
            "without a binding the environment endpoint still selects the control plane"
        );
        assert!(
            reconcile_requested_endpoint("not a url", DiskControlPlaneEndpoint::Absent).is_err(),
            "an unusable environment endpoint must fail closed"
        );
    }

    #[tokio::test]
    async fn an_unusable_identity_disables_cloud_connect_without_aborting_startup() {
        let dir = scratch_dir("unusable-startup-identity");
        let identity_path = dir.join(IDENTITY_FILE);
        std::fs::write(&identity_path, "not valid identity JSON")
            .expect("write malformed identity");
        let mut config = CloudConnectConfig::from_env("test-runtime");
        config.config_dir.clone_from(&dir);
        config.identity_path = identity_path;

        let state = load_startup_state_from_config(&config).await;
        assert!(
            state.reconnectable_identity.is_none(),
            "an optional integration's unusable identity must fail Cloud Connect closed without failing spiced"
        );
        assert!(
            state.identity_observed,
            "invalid credentials must not erase cloud-managed configuration provenance"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn an_unsafe_endpoint_override_is_a_configuration_error() {
        use std::os::unix::fs::symlink;

        let dir = scratch_dir("unsafe-endpoint-override");
        let target = dir.join("redirected-endpoint");
        std::fs::write(&target, "https://wrong-control-plane.example")
            .expect("write target endpoint");
        symlink(&target, dir.join("cloud-endpoint")).expect("create endpoint symlink");
        let mut config = CloudConnectConfig::from_env("test-runtime");
        config.config_dir.clone_from(&dir);

        apply_endpoint_override(&mut config)
            .expect_err("an unsafe override must disable enrollment and renewal");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_unusable_identity_still_restores_its_managed_spicepod() {
        let dir = scratch_dir("unusable-identity-managed-spicepod");
        let identity_path = dir.join(IDENTITY_FILE);
        std::fs::write(&identity_path, "not valid identity JSON")
            .expect("write malformed identity");
        std::fs::write(dir.join(CLOUD_MANAGED_SPICEPOD_FILE), VALID_SPICEPOD)
            .expect("write managed spicepod");
        let mut config = CloudConnectConfig::from_env("test-runtime");
        config.config_dir.clone_from(&dir);
        config.identity_path = identity_path;

        let state = load_startup_state_from_config(&config).await;
        let deployed = cloud_managed_spicepod(state.config_dir(), state.identity_observed())
            .await
            .expect("read managed spicepod")
            .expect("managed spicepod remains selected");
        assert_eq!(deployed.spicepod_yaml, VALID_SPICEPOD);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn an_unreadable_identity_does_not_force_a_second_config_directory_read() {
        let dir = scratch_dir("unreadable-identity-does-not-force-managed-read");
        let identity_path = dir.join(IDENTITY_FILE);
        std::fs::create_dir(&identity_path).expect("make the identity path unreadable as a file");
        std::fs::write(dir.join(CLOUD_MANAGED_SPICEPOD_FILE), VALID_SPICEPOD)
            .expect("write managed spicepod");
        let mut config = CloudConnectConfig::from_env("test-runtime");
        config.config_dir.clone_from(&dir);
        config.identity_path = identity_path;

        let state = load_startup_state_from_config(&config).await;
        assert!(state.reconnectable_identity.is_none());
        assert!(
            !state.identity_observed,
            "an identity I/O error does not prove durable cloud-managed provenance"
        );
        assert!(
            cloud_managed_spicepod(state.config_dir(), state.identity_observed())
                .await
                .expect("the managed spicepod read is skipped")
                .is_none()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_removed_identity_does_not_restore_a_stale_managed_spicepod() {
        let dir = scratch_dir("removed-identity-stale-managed-spicepod");
        std::fs::write(dir.join(CLOUD_MANAGED_SPICEPOD_FILE), VALID_SPICEPOD)
            .expect("write stale managed spicepod");
        let mut config = CloudConnectConfig::from_env("test-runtime");
        config.config_dir.clone_from(&dir);
        config.identity_path = dir.join(IDENTITY_FILE);

        let state = load_startup_state_from_config(&config).await;
        assert!(!state.identity_observed());
        assert!(
            cloud_managed_spicepod(state.config_dir(), state.identity_observed())
                .await
                .expect("check managed spicepod")
                .is_none(),
            "release removes the identity marker, so stale deployment state must stay inactive"
        );

        let _ = std::fs::remove_dir_all(&dir);
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
    // The split: what a deployment changes decides what reaches this process.
    // ----------------------------------------------------------------------

    /// The deployment the split tests are changes against.
    const ACTIVE: &str = "\
version: v2
kind: Spicepod
name: split
datasets:
  - from: memory:a
    name: a
";

    /// `ACTIVE` with `extra` appended as further top-level sections.
    fn with_sections(extra: &str) -> String {
        format!("{ACTIVE}{extra}")
    }

    /// The app a spicepod document builds, which is what a deployment is
    /// compared against and merged with — never the text, so a section at its
    /// default or in a different key order is the configuration it describes.
    async fn built(dir: &Path, spicepod_yaml: &str) -> App {
        let path = dir.join("built.yml");
        tokio::fs::write(&path, spicepod_yaml)
            .await
            .expect("write the spicepod under test");
        AppBuilder::build_from_path(path)
            .await
            .expect("the spicepod under test builds")
    }

    /// Split `deployed` against `active`, both given as documents.
    async fn split(dir: &Path, active: &str, deployed: &str) -> Reconciled {
        let active = built(dir, active).await;
        let deployed = built(dir, deployed).await;
        reconcile(&active, deployed)
    }

    /// The case this exists for: a change confined to the components the
    /// runtime reconciles is served by this process, with nothing pending.
    #[tokio::test]
    async fn a_component_only_change_is_reconciled_in_place() {
        let dir = scratch_dir("split-components");
        let changes = [
            "datasets:\n  - from: memory:b\n    name: b\n",
            "views:\n  - name: v\n    sql: SELECT 1\n",
            "catalogs:\n  - from: spice.ai\n    name: c\n",
            "models:\n  - from: openai\n    name: m\n",
            "functions:\n  - name: f\n    from: https://example.com\n    signature:\n      args: []\n      returns: int64\n",
        ];
        for change in changes {
            // Appending to `ACTIVE` would leave two `datasets:` keys, so the
            // component sections are compared against a document holding none.
            let active = "version: v2\nkind: Spicepod\nname: split\n";
            let deployed = format!("{active}{change}");
            let split = split(&dir, active, &deployed).await;
            assert!(
                split.restart_required.is_empty(),
                "a change to `{change}` alone must not need a restart: {:?}",
                split.restart_required
            );
            assert_eq!(
                split.app,
                built(&dir, &deployed).await,
                "the deployment must be installed as it was deployed: `{change}`"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every other section configures something built while the process
    /// started. The deployed value is named — that is all the operator has to
    /// go on — and the value in effect is what stays installed.
    #[tokio::test]
    async fn every_start_time_section_is_named_and_left_as_it_is() {
        let dir = scratch_dir("split-start-time");
        let changes = [
            ("embeddings", "embeddings:\n  - name: e\n    from: openai\n"),
            (
                "extensions",
                "extensions:\n  spice_cloud:\n    enabled: true\n",
            ),
            (
                "management",
                "management:\n  enabled: true\n  api_key: test-api-key\n",
            ),
            ("rerankers", "rerankers:\n  - name: r\n    from: openai\n"),
            ("runtime", "runtime:\n  dataset_load_parallelism: 2\n"),
            ("secrets", "secrets:\n  - from: env\n    name: env\n"),
            ("snapshots", "snapshots:\n  enabled: true\n"),
            (
                "tools",
                "tools:\n  - name: t\n    from: builtin:list_datasets\n",
            ),
            ("workers", "workers:\n  - name: w\n    sql: SELECT 1\n"),
        ];
        for (section, change) in changes {
            let deployed = with_sections(change);
            let split = split(&dir, ACTIVE, &deployed).await;
            assert_eq!(
                split.restart_required,
                vec![section.to_string()],
                "changing `{section}` must be reported by name"
            );
            // Everything this process is running, with the deployment's own
            // documents recorded: those describe what is on disk, which is what
            // the `/v1/spicepods` listing answers with.
            let mut in_effect = built(&dir, ACTIVE).await;
            in_effect.spicepods = built(&dir, &deployed).await.spicepods;
            assert_eq!(
                split.app, in_effect,
                "`{section}` must be left as this process is running it"
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Sections are compared as the configuration they describe, not as text:
    /// a section written out empty, at its default, or in a different key order
    /// configures what the process is already running.
    #[tokio::test]
    async fn a_section_that_configures_nothing_new_is_not_a_change() {
        let dir = scratch_dir("split-defaults");
        let empty = with_sections("secrets: []\nruntime:\nmetadata: {}\n");
        assert!(
            split(&dir, ACTIVE, &empty)
                .await
                .restart_required
                .is_empty(),
            "an empty section must not need a restart"
        );

        let active =
            with_sections("runtime:\n  dataset_load_parallelism: 2\n  ready_state: on_load\n");
        let reordered =
            with_sections("runtime:\n  ready_state: on_load\n  dataset_load_parallelism: 2\n");
        assert!(
            split(&dir, &active, &reordered)
                .await
                .restart_required
                .is_empty(),
            "rewriting a section without changing what it configures must not need a restart"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The point of splitting rather than deferring the whole deployment: the
    /// components of a mixed change are served now, and only what cannot be is
    /// reported. Reporting one of the sections that is pending is not enough
    /// either — an operator who reverted only what was named would deploy again
    /// and still not be live.
    #[tokio::test]
    async fn a_mixed_deployment_serves_its_components_and_names_the_rest() {
        let dir = scratch_dir("split-mixed");
        let deployed = with_sections(
            "runtime:\n  dataset_load_parallelism: 2\ntools:\n  - name: t\n    from: builtin:list_datasets\nviews:\n  - name: v\n    sql: SELECT 1\n",
        );
        let split = split(&dir, ACTIVE, &deployed).await;

        assert_eq!(
            split.restart_required,
            vec!["runtime".to_string(), "tools".to_string()],
            "both start-time sections must be named, and the component one must not be"
        );
        assert_eq!(
            view_names(&split.app),
            vec!["v"],
            "the deployed view must be installed"
        );
        assert!(
            split.app.tools.is_empty() && split.app.runtime.dataset_load_parallelism.is_none(),
            "the start-time sections must be left as this process is running them"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Delivered values are not in the document, but they are the app's
    /// secrets: an operator who reverted only the sections of the document
    /// would deploy again and still be waiting on the rotation. Naming them
    /// twice would be no better.
    #[test]
    fn a_pending_secret_is_named_with_the_document_and_only_once() {
        let alongside = Pending {
            sections: vec!["runtime".to_string()],
            secrets: true,
        };
        assert_eq!(
            alongside.restart_required(),
            vec!["runtime".to_string(), "secrets".to_string()]
        );

        // The document declares a `secrets:` section *and* the delivered values
        // are pending: one name, one thing for the operator to look at.
        let with_section = Pending {
            sections: vec!["secrets".to_string()],
            secrets: true,
        };
        assert_eq!(with_section.restart_required(), vec!["secrets".to_string()]);

        let alone = Pending {
            sections: Vec::new(),
            secrets: true,
        };
        assert_eq!(alone.restart_required(), vec!["secrets".to_string()]);

        assert!(Pending::default().restart_required().is_empty());
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

    /// A handle over a runtime that has finished loading the deployment it
    /// booted on, which is the state an instance is in between deployments.
    async fn handle_serving(dir: &Path, active: &str) -> Arc<SpicedRuntimeHandle> {
        let handle = handle_loading(dir, active).await;
        finish_loading(&handle).await;
        handle
    }

    /// A handle over a runtime that has not run its component load, which is
    /// the state an instance is in from the moment Cloud Connect connects until
    /// the load finishes.
    async fn handle_loading(dir: &Path, active: &str) -> Arc<SpicedRuntimeHandle> {
        let path = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
        std::fs::write(&path, active).expect("write the deployment being served");
        let deployment = CloudManagedSpicepod {
            path,
            spicepod_yaml: active.to_string(),
        };
        handle_over(dir, active, Some(deployment), INITIAL_LOAD_BUDGET).await
    }

    /// A handle whose wait for the initial load runs out at once, which is what
    /// an instance whose load can never finish looks like to a deployment.
    async fn handle_load_never_finishing(dir: &Path, active: &str) -> Arc<SpicedRuntimeHandle> {
        let path = dir.join(CLOUD_MANAGED_SPICEPOD_FILE);
        std::fs::write(&path, active).expect("write the deployment being served");
        let deployment = CloudManagedSpicepod {
            path,
            spicepod_yaml: active.to_string(),
        };
        handle_over(dir, active, Some(deployment), Duration::from_millis(200)).await
    }

    /// A handle over a runtime serving a spicepod that did not come from a
    /// deployment — a locally-configured instance, which is what the first
    /// Cloud deployment arrives at.
    async fn handle_serving_locally(dir: &Path, local: &str) -> Arc<SpicedRuntimeHandle> {
        let handle = handle_over(dir, local, None, INITIAL_LOAD_BUDGET).await;
        finish_loading(&handle).await;
        handle
    }

    async fn handle_over(
        dir: &Path,
        app: &str,
        running_deployment: Option<CloudManagedSpicepod>,
        initial_load_budget: Duration,
    ) -> Arc<SpicedRuntimeHandle> {
        let path = dir.join("startup.yml");
        std::fs::write(&path, app).expect("write the app this instance starts on");
        let app = AppBuilder::build_from_path(path)
            .await
            .expect("the app this instance starts on builds");

        let runtime = Arc::new(Runtime::builder().with_app(app).build().await);

        Arc::new(SpicedRuntimeHandle::new(SpicedRuntimeHandleParts {
            runtime,
            logs: None,
            delivered_secrets: Arc::new(CloudDeliveredSecretStore::new()),
            identity_path: dir.join(IDENTITY_FILE),
            running_deployment,
            metrics: None,
            app_id: None,
            runtime_overrides: Vec::new(),
            initial_load_budget,
        }))
    }

    async fn finish_loading(handle: &SpicedRuntimeHandle) {
        tokio::time::timeout(
            Duration::from_mins(2),
            Arc::clone(&handle.runtime).load_components(),
        )
        .await
        .expect("the runtime finishes loading its components");
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
    fn enroll_with_a_cache_key(identity_path: &Path) {
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
            control_plane_endpoint: None,
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

    /// The app the runtime holds: the configuration in effect here, which is
    /// what a deployment is diffed against and what it must leave in place
    /// wherever it cannot be applied.
    async fn active_app(handle: &SpicedRuntimeHandle) -> Arc<App> {
        handle.runtime.read_app().await.expect("an app is loaded")
    }

    async fn live_view_names(handle: &SpicedRuntimeHandle) -> Vec<String> {
        view_names(&*active_app(handle).await)
    }

    fn persisted_spicepod(dir: &Path) -> String {
        std::fs::read_to_string(dir.join(CLOUD_MANAGED_SPICEPOD_FILE))
            .expect("the canonical spicepod exists")
    }

    /// The whole point: a component-only deployment is served by this process.
    /// The query is what proves it — an assertion that the runtime is still up
    /// would pass for a deployment that was ignored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_component_only_deployment_is_applied_in_place_and_queryable() {
        let dir = scratch_dir("apply-components");
        let handle = handle_serving(&dir, SERVING).await;

        let document = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("a component-only deployment applies");

        assert_eq!(document["live"], serde_json::json!(true));
        assert_eq!(
            document["restart_required"],
            serde_json::json!([]),
            "a component-only deployment leaves nothing waiting on a start"
        );
        assert_eq!(document["views"], serde_json::json!(2));

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
        assert_eq!(handle.desired.read().as_deref(), Some(SERVING_PLUS_VIEW));
        assert!(handle.pending.read().restart_required().is_empty());
        assert!(
            !handle.runtime.status().is_shutdown(),
            "no deployment may drain the runtime"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A start-time change is persisted for the next start and named, while the
    /// instance keeps serving what it is running — including the components the
    /// same deployment changed. The effective boot value is the assertion that
    /// matters: a process that installed the deployed one would be reporting a
    /// restart it did not need and behaving as if it had already happened.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_start_time_deployment_is_named_while_the_instance_keeps_serving() {
        let dir = scratch_dir("apply-start-time");
        let handle = handle_serving(&dir, SERVING).await;
        let deployed = format!("{SERVING_PLUS_VIEW}runtime:\n  dataset_load_parallelism: 2\n");

        let document = handle
            .apply_spicepod(deployment(&dir, &deployed))
            .await
            .expect("a start-time deployment is applied as far as it can be");

        assert_eq!(document["live"], serde_json::json!(false));
        assert_eq!(
            document["restart_required"],
            serde_json::json!(["runtime"]),
            "the section that is on disk but not in this process must be named"
        );

        assert_eq!(persisted_spicepod(&dir), deployed, "it is on disk");
        assert_eq!(
            active_app(&handle).await.runtime.dataset_load_parallelism,
            None,
            "and not in effect: this process keeps the value it started with"
        );
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1,
            "the components of the same deployment are served all the same"
        );
        assert!(
            !handle.runtime.status().is_shutdown(),
            "a deployment must never drain the runtime"
        );

        // A component-only deployment arriving while that is pending is applied
        // in place, and the pending section stays pending: it is still on disk
        // and still not in this process.
        let follow_on = format!(
            "{SERVING}  - name: third_view\n    sql: SELECT 3 AS n\nruntime:\n  dataset_load_parallelism: 2\n"
        );
        let document = handle
            .apply_spicepod(deployment(&dir, &follow_on))
            .await
            .expect("a component-only deployment applies while a section is pending");
        assert_eq!(document["restart_required"], serde_json::json!(["runtime"]));
        assert_eq!(document["live"], serde_json::json!(false));
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM third_view").await,
            1,
            "the deployment's components reach this process while the section waits"
        );
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view", "third_view"],
            "reconciling against the app being served must drop the view the deployment dropped"
        );

        // Deploying the section back to the value this process is running with
        // clears it: what is pending is measured against the process, not
        // against whatever the file said in between.
        let reverted = format!("{SERVING}  - name: third_view\n    sql: SELECT 3 AS n\n");
        let document = handle
            .apply_spicepod(deployment(&dir, &reverted))
            .await
            .expect("reverting the pending section applies");
        assert_eq!(document["restart_required"], serde_json::json!([]));
        assert_eq!(document["live"], serde_json::json!(true));
        assert!(handle.pending.read().restart_required().is_empty());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The first Cloud deployment lands on whatever the instance is already
    /// running, so that is what it is compared against. Treating "no previous
    /// deployment" as nothing to diff would make every first deployment a
    /// restart, and this instance does not have one to give.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_first_deployment_is_compared_against_the_app_this_instance_started_on() {
        let dir = scratch_dir("apply-first");
        let local = "\
version: v2
kind: Spicepod
name: local
views:
  - name: local_view
    sql: SELECT 1 AS n
tools:
  - name: local_tool
    from: builtin:list_datasets
";
        let handle = handle_serving_locally(&dir, local).await;

        // Same tools as the local spicepod, a different view: the view is
        // served here, and nothing is pending.
        let deployed = "\
version: v2
kind: Spicepod
name: local
views:
  - name: deployed_view
    sql: SELECT 2 AS n
tools:
  - name: local_tool
    from: builtin:list_datasets
";
        let document = handle
            .apply_spicepod(deployment(&dir, deployed))
            .await
            .expect("the first deployment applies");

        assert_eq!(
            document["restart_required"],
            serde_json::json!([]),
            "the sections the deployment leaves as they are must not be reported"
        );
        assert_eq!(document["live"], serde_json::json!(true));
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1
        );
        assert_eq!(live_view_names(&handle).await, vec!["deployed_view"]);
        assert_eq!(persisted_spicepod(&dir), deployed);

        // And a start-time section it does change is reported against that same
        // baseline rather than being lost with it.
        let changes_tools =
            deployed.replace("from: builtin:list_datasets", "from: https://example.com");
        let document = handle
            .apply_spicepod(deployment(&dir, &changes_tools))
            .await
            .expect("the second deployment applies");
        assert_eq!(document["restart_required"], serde_json::json!(["tools"]));
        assert_eq!(
            active_app(&handle).await.tools[0].from,
            "builtin:list_datasets",
            "the tool this instance started with is the one it keeps"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The next deployment is diffed against what this process serves now. A
    /// classifier still reading the boot-time spicepod would answer a
    /// redelivery of the deployment it just applied as a new one, and the one
    /// it replaced as already live.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_next_deployment_is_diffed_against_the_one_now_serving() {
        let dir = scratch_dir("apply-twice");
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
        assert_eq!(redelivered["live"], serde_json::json!(true));
        assert!(
            redelivered.get("views").is_none(),
            "a redelivery of the deployment on disk reconciles nothing: {redelivered}"
        );

        // And the spicepod it replaced is a new deployment, not the live one.
        let reverted = handle
            .apply_spicepod(deployment(&dir, SERVING))
            .await
            .expect("the previous deployment applies again");
        assert_eq!(reverted["live"], serde_json::json!(true));
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

    /// A deployment arriving before the instance has finished loading waits for
    /// it. The load is still registering the components of the app being
    /// replaced, so reconciling against it would treat a component the load has
    /// not reached as already registered — and the load would go on installing
    /// what the deployment replaced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_deployment_arriving_before_the_load_finishes_waits_for_it() {
        let dir = scratch_dir("apply-loading");
        let handle = handle_loading(&dir, SERVING).await;
        assert!(
            handle.runtime.initial_load_in_flight(),
            "the runtime under test has not loaded its components"
        );

        let applying = tokio::spawn({
            let handle = Arc::clone(&handle);
            let dir = dir.clone();
            async move {
                handle
                    .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
                    .await
            }
        });

        // Nothing is persisted or reconciled while the load runs. Polling the
        // canonical file is what a caller would see; the apply cannot have
        // finished without writing it.
        for _ in 0..10u32 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(
                persisted_spicepod(&dir),
                SERVING,
                "the deployment must not be persisted while the load is still running"
            );
            assert!(!applying.is_finished(), "the apply must wait for the load");
        }

        finish_loading(&handle).await;

        let document = tokio::time::timeout(Duration::from_mins(1), applying)
            .await
            .expect("the apply finishes once the load is over")
            .expect("the apply task finishes")
            .expect("a deployment that waited for the load applies");
        assert_eq!(document["live"], serde_json::json!(true));
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An instance whose load cannot finish must not be a dead end. The
    /// deployment that would fix it is the one that keeps timing out, so it is
    /// persisted even though it is not reconciled: a start serves it, and the
    /// failure says exactly that instead of claiming nothing happened. The
    /// running process is untouched, and the deployment is not recorded as
    /// applied, so it is applied in full the moment the load is over.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deployment_that_outlasts_the_load_is_persisted_for_the_next_start() {
        let dir = scratch_dir("apply-load-timeout");
        let handle = handle_load_never_finishing(&dir, SERVING).await;

        let err = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect_err("a deployment that outlasts the load is not applied");
        assert!(
            matches!(err, CommandError::Failed { .. }),
            "an instance that is still loading may be able to apply the next attempt: {err}"
        );
        let message = err.to_string();
        assert!(
            message.contains("persisted") && message.contains("restarting"),
            "the failure must say what did happen and how to serve it: {message}"
        );

        assert_eq!(
            persisted_spicepod(&dir),
            SERVING_PLUS_VIEW,
            "the deployment must be on disk for the start that serves it"
        );
        assert_eq!(
            handle.desired.read().as_deref(),
            Some(SERVING),
            "and must not be recorded as applied: this process never reconciled it"
        );
        assert_eq!(live_view_names(&handle).await, vec!["served_view"]);
        assert!(!handle.runtime.status().is_shutdown());

        // Once the load is over the same deployment applies in full, rather
        // than being answered from state that never described it.
        finish_loading(&handle).await;
        let document = handle
            .apply_spicepod(deployment(&dir, SERVING_PLUS_VIEW))
            .await
            .expect("the deployment applies once the load is over");
        assert_eq!(document["live"], serde_json::json!(true));
        assert_eq!(
            query_rows(&handle.runtime, "SELECT n FROM deployed_view").await,
            1
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deployment that does not build changes nothing: not the file this
    /// instance starts on, not what it is serving, not what it reports.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_deployment_changes_neither_the_persisted_nor_the_served_spicepod() {
        let dir = scratch_dir("apply-malformed");
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
        assert_eq!(handle.desired.read().as_deref(), Some(SERVING));
        assert!(handle.pending.read().restart_required().is_empty());
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
    /// and leaves nothing behind for the start it is not taking.
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
            "nothing is cached for a start that is not happening"
        );
        assert_eq!(persisted_spicepod(&dir), SERVING);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The cache is what the next start resolves its secrets from, so a
    /// deployment that does not build must leave it holding the values the
    /// instance is still serving on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_malformed_deployment_leaves_the_cached_secrets_alone() {
        let dir = scratch_dir("malformed-cache");
        let handle = handle_serving(&dir, SERVING).await;
        enroll_with_a_cache_key(&dir.join(IDENTITY_FILE));

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
    /// redelivery that follows is the last chance to repair it before a start
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
        enroll_with_a_cache_key(&dir.join(IDENTITY_FILE));
        assert!(cached_secrets(&dir).is_none(), "nothing is cached yet");

        let document = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("a redelivery of the deployment on disk is answered");

        assert_eq!(
            document["secrets_cache_error"],
            serde_json::Value::Null,
            "the repair is reported as having succeeded"
        );
        assert!(
            cached_secrets(&dir)
                .expect("the redelivery wrote the cache")
                .holds(&delivered("api_key", b"value-one")),
            "the start this instance may take must find its secrets"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A secret this instance has never resolved is installed by the
    /// deployment that brings it, so the components it deploys can use it. A
    /// value it has already resolved is the opposite: a component holds it
    /// until it loads again, so the deployed value waits for a start and is
    /// reported until then — and deploying the value in effect clears it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_new_delivered_secret_is_installed_and_a_rotated_one_waits() {
        let dir = scratch_dir("secret-rotation");
        let handle = handle_serving(&dir, SERVING).await;

        let first = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("a deployment delivering a new secret applies");
        assert_eq!(
            first["restart_required"],
            serde_json::json!([]),
            "a value nothing has resolved yet can be installed here"
        );
        assert_eq!(
            first["delivered_secrets"],
            serde_json::json!(["api_key"]),
            "names only, and the names the deployment carried"
        );
        assert!(
            handle
                .delivered_secrets
                .holds(&delivered("api_key", b"value-one")),
            "the delivered value must be resolvable in this process"
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
        assert_eq!(rotated["restart_required"], serde_json::json!(["secrets"]));
        assert_eq!(rotated["live"], serde_json::json!(false));
        assert!(
            handle
                .delivered_secrets
                .holds(&delivered("api_key", b"value-one")),
            "the value the loaded components resolved stays in effect"
        );
        for document in [&first, &rotated] {
            let rendered = document.to_string();
            assert!(
                !rendered.contains("value-one") && !rendered.contains("value-two"),
                "a delivered value must never reach the command result: {rendered}"
            );
        }

        // Deploying the value in effect clears the pending rotation, the same
        // way reverting a section of the document does.
        let reverted = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("reverting the rotation applies");
        assert_eq!(reverted["restart_required"], serde_json::json!([]));
        assert_eq!(reverted["live"], serde_json::json!(true));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A delivered set the cache could not take is not settled either: the next
    /// start would resolve something other than what was deployed, so the
    /// deployment is not live until a later one writes it. Reporting it as live
    /// off the in-memory values alone would hide the divergence until a start
    /// exposed it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_secret_the_cache_refused_stays_pending_until_it_is_written() {
        let dir = scratch_dir("secret-cache-pending");
        let handle = handle_serving(&dir, SERVING).await;
        enroll_with_a_cache_key(&dir.join(IDENTITY_FILE));
        // A directory where the cache file belongs: the write fails for a
        // reason that has nothing to do with the values themselves.
        let cache_path = dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);
        std::fs::create_dir_all(&cache_path).expect("block the cache path");

        let refused = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("a deployment whose cache write fails still applies");
        assert!(
            refused["secrets_cache_error"].is_string(),
            "the write failure must be reported: {refused}"
        );
        assert_eq!(refused["restart_required"], serde_json::json!(["secrets"]));
        assert_eq!(refused["live"], serde_json::json!(false));
        assert!(
            handle
                .delivered_secrets
                .holds(&delivered("api_key", b"value-one")),
            "the values are still installed here — it is the next start that would miss them"
        );
        let status = handle.status().await.expect("status is reported");
        assert_eq!(
            status.detail["restart_required"],
            serde_json::json!(["secrets"]),
            "status must report what the apply reported"
        );

        // Redelivering the same values once the cache can be written settles
        // it: what is on disk is what this instance resolves again.
        std::fs::remove_dir_all(&cache_path).expect("unblock the cache path");
        let repaired = handle
            .apply_spicepod(SpicepodDeployment {
                config_dir: &dir,
                spicepod_yaml: SERVING_PLUS_VIEW,
                delivered_secrets: Some(delivered("api_key", b"value-one")),
                app_id: None,
            })
            .await
            .expect("the redelivery is answered");
        assert_eq!(repaired["secrets_cache_error"], serde_json::Value::Null);
        assert_eq!(repaired["restart_required"], serde_json::json!([]));
        assert_eq!(repaired["live"], serde_json::json!(true));
        assert!(
            cached_secrets(&dir)
                .expect("the redelivery wrote the cache")
                .holds(&delivered("api_key", b"value-one"))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// What is pending is what `GetStatus` reports, so a caller that asked
    /// after the deploy reads the same answer as one that watched it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_reports_what_the_last_deployment_left_pending() {
        let dir = scratch_dir("status-pending");
        let handle = handle_serving(&dir, SERVING).await;

        let status = handle.status().await.expect("status is reported");
        assert_eq!(status.detail["restart_required"], serde_json::json!([]));

        let deployed = format!("{SERVING}runtime:\n  dataset_load_parallelism: 2\n");
        handle
            .apply_spicepod(deployment(&dir, &deployed))
            .await
            .expect("a start-time deployment is applied as far as it can be");

        let status = handle.status().await.expect("status is reported");
        assert_eq!(
            status.detail["restart_required"],
            serde_json::json!(["runtime"])
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The classification and the state it commits to are taken together. Two
    /// dispatches of one deployment must apply it once: a second that read the
    /// state before the first replaced it would apply it again, on top of
    /// itself.
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
            let document = task.await.expect("the dispatch finishes");
            assert_eq!(document["live"], serde_json::json!(true));
            // Only an apply reports what it applied; the answer for a
            // deployment already on disk carries no component counts.
            if document.get("views").is_some() {
                applied += 1;
            }
        }
        assert_eq!(applied, 1, "the deployment must be applied exactly once");

        assert_eq!(handle.desired.read().as_deref(), Some(SERVING_PLUS_VIEW));
        assert_eq!(persisted_spicepod(&dir), SERVING_PLUS_VIEW);
        assert_eq!(
            live_view_names(&handle).await,
            vec!["served_view", "deployed_view"]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Deployments arriving together leave one state: the file this instance
    /// starts on, the spicepod it reports as deployed, and the app it is
    /// serving all describe the same deployment. Interleaving them could
    /// persist one, serve another, and diff the next against a third.
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

        let desired = handle.desired.read().clone().expect("a deployment landed");
        assert_eq!(
            persisted_spicepod(&dir),
            desired,
            "the file this instance starts on is the deployment it reports as deployed"
        );
        let views = live_view_names(&handle).await;
        assert_eq!(
            views.len(),
            2,
            "one deployment's views, not several: {views:?}"
        );
        // Exactly the deployment on disk, and none of the three it raced.
        for name in ["view_alpha", "view_beta", "view_gamma", "view_delta"] {
            assert_eq!(
                desired.contains(name),
                views[1] == name,
                "the app being served is the deployment on disk: {views:?}"
            );
        }
        assert!(
            handle.pending.read().restart_required().is_empty(),
            "nothing a component-only deployment changes waits on a start"
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
