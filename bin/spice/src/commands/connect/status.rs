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

//! The one status model both status commands render.
//!
//! `spice connect status` renders all of [`ConnectStatus`].
//! `spice connect service status` renders the *same* [`ServiceStatus`] value
//! filtered out of it. Neither command queries, normalizes, labels, or caches
//! service state of its own, so the two cannot answer differently — the
//! failure that two independent status implementations produce as soon as one
//! of them learns a new state.
//!
//! One snapshot is collected per invocation and then rendered, rather than
//! probed per printed line, so the report describes a single moment.

use std::path::{Path, PathBuf};

use serde::Serialize;

use runtime_cloud_connect::config::IDENTITY_FILE;

use super::service::{self, ServiceBackend, ServiceStatus};
use crate::error::Result;
use crate::output::{OutputFormat, write_json};

/// Schema version of the status documents published on stdout.
///
/// This is a public automation surface. Bump it when a field is renamed or
/// removed, or when an enum grows a variant consumers must branch on, and
/// update the golden fixtures in the same change.
pub(crate) const STATUS_SCHEMA_VERSION: u32 = 1;

/// Whether this directory is connected to Spice Cloud.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ConnectionState {
    /// No Cloud Connect state in this directory at all.
    NotConnected,
    /// An enrollment started and did not finish; its retry-safe draft is still
    /// here.
    EnrollmentIncomplete,
    /// An enrolled identity is present.
    Enrolled,
}

/// Whether a Cloud deployment has landed in this directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DeploymentState {
    /// No app has been deployed; the instance runs its local spicepod.
    None,
    /// A cloud-managed spicepod is on disk.
    Deployed,
}

/// Whether the last deployment's secrets are readable from the local cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SecretsState {
    /// No deployment has delivered secrets yet.
    NotDelivered,
    /// The cache is present and its plaintext header could be read.
    Delivered,
    /// The cache is present and its header could not be read.
    Unreadable,
}

/// The Cloud identity and connection half of the report.
///
/// Field order is the JSON field order. Nothing here is credential material:
/// the identity's keys, the delivered secret *values*, and the cache key are
/// never read into this model, only the facts an operator diagnoses with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ConnectionStatus {
    pub(crate) state: ConnectionState,
    /// The instance directory this report is about.
    pub(crate) directory: PathBuf,
    /// Where the enrolled identity lives, whether or not one is there yet.
    pub(crate) identity_path: PathBuf,
    /// The Spice Cloud control plane this directory reaches.
    pub(crate) endpoint: String,
    /// Cloud-assigned instance identifier.
    pub(crate) identifier: Option<String>,
    pub(crate) org_name: Option<String>,
    pub(crate) app_name: Option<String>,
    /// Cloud-constructed portal monitor URL, when the instance is attached.
    pub(crate) monitor_url: Option<String>,
    /// The gateway the control stream dials.
    pub(crate) gateway_addr: Option<String>,
    /// Unix seconds after which the identity certificate stops being accepted.
    /// `null` means the cloud issued no expiry, which is not the same as an
    /// expiry at the epoch.
    pub(crate) expires_at_unix: Option<u64>,
    pub(crate) expired: bool,
    /// The unfinished enrollment's draft, when there is one.
    pub(crate) draft_path: Option<PathBuf>,
    /// A significant host-clock skew against Spice Cloud, measured only in the
    /// states a wrong clock explains. `null` when it was not measured or was
    /// insignificant.
    pub(crate) clock: Option<String>,
}

/// What has been deployed to this instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct DeploymentStatus {
    pub(crate) state: DeploymentState,
    /// The cloud-managed spicepod overlay, when one has been delivered.
    pub(crate) spicepod_path: Option<PathBuf>,
    pub(crate) secrets: SecretsState,
    /// Delivered secret *names* only — the values are sealed to the identity's
    /// key and are never read here.
    pub(crate) secret_names: Vec<String>,
    /// Settings the deployment persisted that the running process is not
    /// serving, so a supervisor-owned restart is required to apply them.
    ///
    /// `null` means the running instance was not asked, which is different from
    /// an empty list — nothing pending.
    pub(crate) restart_required: Option<Vec<String>>,
}

/// Everything `spice connect status` reports about one instance directory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ConnectStatus {
    pub(crate) schema_version: u32,
    pub(crate) connection: ConnectionStatus,
    pub(crate) service: ServiceStatus,
    pub(crate) deployment: DeploymentStatus,
}

/// The document `spice connect service status --output json` writes.
///
/// It carries the byte-identical `service` object from [`ConnectStatus`], so
/// automation never has to reconcile a full and a filtered schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ServiceStatusDocument<'a> {
    pub(crate) schema_version: u32,
    pub(crate) service: &'a ServiceStatus,
}

impl ConnectStatus {
    /// Collect one snapshot of this instance directory.
    ///
    /// Offline for the states that do not need the network: a healthy enrolled
    /// directory makes no request at all, and the clock is measured only where
    /// a wrong one is the explanation.
    pub(crate) async fn collect(
        backend: &dyn ServiceBackend,
        instance_dir: &Path,
        config_dir: &Path,
        endpoint: &str,
    ) -> Self {
        let identity_path = config_dir.join(IDENTITY_FILE);
        let draft_path = runtime_cloud_connect::EnrollmentDraft::path_in(config_dir);
        let identity =
            runtime_cloud_connect::identity::IdentityStore::load_optional(&identity_path)
                .ok()
                .flatten();

        let mut connection = ConnectionStatus {
            state: ConnectionState::NotConnected,
            directory: instance_dir.to_path_buf(),
            identity_path,
            endpoint: endpoint.to_string(),
            identifier: None,
            org_name: None,
            app_name: None,
            monitor_url: None,
            gateway_addr: None,
            expires_at_unix: None,
            expired: false,
            draft_path: draft_path.exists().then(|| draft_path.clone()),
            clock: None,
        };

        if let Some(id) = identity {
            connection.state = ConnectionState::Enrolled;
            connection.expired = id.is_expired();
            connection.expires_at_unix = id.not_after_unix;
            connection.gateway_addr =
                (!id.gateway_addr.is_empty()).then(|| id.gateway_addr.clone());
            connection.identifier = Some(id.identifier);
            connection.org_name = id.org_name;
            connection.app_name = id.app_name;
            connection.monitor_url = id.monitor_url;
        } else if connection.draft_path.is_some() {
            connection.state = ConnectionState::EnrollmentIncomplete;
        }

        // An identity that reads as expired on a host whose clock is wrong is
        // not actually expired, and a stuck enrollment is a state a wrong clock
        // explains — the enroll keeps failing on certificate validity. Measure
        // only there, so the common `status` stays offline and instant.
        if connection.expired || connection.state == ConnectionState::EnrollmentIncomplete {
            connection.clock = clock_advice(endpoint).await;
        }

        Self {
            schema_version: STATUS_SCHEMA_VERSION,
            connection,
            service: service::status(backend, instance_dir, config_dir),
            deployment: collect_deployment(config_dir),
        }
    }

    /// The service half on its own, for the filtered command.
    pub(crate) fn service_document(&self) -> ServiceStatusDocument<'_> {
        ServiceStatusDocument {
            schema_version: self.schema_version,
            service: &self.service,
        }
    }

    /// Whether this snapshot describes a state the reporting command should
    /// exit non-zero for.
    ///
    /// A service that is merely stopped, or a directory that is not connected,
    /// is a fact and exits `0`. A supervisor that could not be asked, or one
    /// reporting a failed service, is a problem and exits non-zero so
    /// automation does not read a degraded instance as healthy.
    pub(crate) fn is_degraded(&self) -> bool {
        self.service.state.is_degraded()
    }
}

/// Read what the last deployment left in the config dir.
///
/// Reads files only, so it answers on a host with no network.
fn collect_deployment(config_dir: &Path) -> DeploymentStatus {
    let spicepod = config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE);
    let cache_path = config_dir.join(runtime_cloud_connect::secret_cache::SECRET_CACHE_FILE);

    // Only the cache's plaintext header, so this works without the key and
    // cannot print a value. Names are what diagnose the common failure: a
    // component referencing a secret the last deployment did not deliver.
    let (secrets, secret_names) =
        match runtime_cloud_connect::secret_cache::read_header(&cache_path) {
            Some(header) => (SecretsState::Delivered, header.names),
            None if cache_path.exists() => (SecretsState::Unreadable, Vec::new()),
            None => (SecretsState::NotDelivered, Vec::new()),
        };

    DeploymentStatus {
        state: if spicepod.exists() {
            DeploymentState::Deployed
        } else {
            DeploymentState::None
        },
        spicepod_path: spicepod.exists().then_some(spicepod),
        secrets,
        secret_names,
        // Restart-required state is measured against the *running* process, so
        // it comes from the instance rather than from disk. Asking it is the
        // job of the command that talks to the runtime; absence here is
        // reported as "not observed" rather than as "nothing pending".
        restart_required: None,
    }
}

/// Measure the host clock against Spice Cloud and describe a significant skew.
///
/// Best-effort and silent on failure: `status` must stay usable on a host with
/// no network.
async fn clock_advice(endpoint: &str) -> Option<String> {
    let skew = runtime_cloud_connect::clock_skew::diagnose(endpoint, None).await?;
    skew.is_significant().then(|| skew.advice())
}

/// Write the full report in `format`.
///
/// JSON goes to stdout and nothing else does, so a `--output json` run is
/// parseable without filtering: every prompt, warning, and diagnosis in this
/// command travels on stderr or as the process's error.
///
/// # Errors
///
/// Returns an error when the report cannot be serialized.
pub(crate) fn render(status: &ConnectStatus, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => write_json(status),
        OutputFormat::Table => {
            render_connection(&status.connection);
            render_service_lines(&status.service);
            render_deployment(&status.deployment);
            render_next_steps(status);
            Ok(())
        }
    }
}

/// Write the service half in `format`, from the same snapshot.
///
/// # Errors
///
/// Returns an error when the report cannot be serialized.
pub(crate) fn render_service(status: &ConnectStatus, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => write_json(&status.service_document()),
        OutputFormat::Table => {
            println!(
                "Spice Cloud Connect service: {}",
                describe_service_state(&status.service)
            );
            render_service_lines(&status.service);
            Ok(())
        }
    }
}

fn render_connection(connection: &ConnectionStatus) {
    match connection.state {
        ConnectionState::Enrolled => {
            println!(
                "Spice Cloud Connect: connected{}",
                match (&connection.org_name, &connection.app_name) {
                    (Some(org), Some(app)) => format!(" — {org} / {app}"),
                    (Some(org), None) => format!(" — {org} (no app attached)"),
                    _ => String::new(),
                }
            );
            if let Some(identifier) = &connection.identifier {
                println!("  instance:    {identifier}");
            }
            println!("  identity:    {}", connection.identity_path.display());
            if let Some(gateway) = &connection.gateway_addr {
                println!("  gateway:     {gateway}");
            }
            println!(
                "  expiry:      {}",
                match connection.expires_at_unix {
                    Some(secs) => format!("unix={secs} (expired={})", connection.expired),
                    None => "unbounded".to_string(),
                }
            );
        }
        ConnectionState::EnrollmentIncomplete => {
            println!("Spice Cloud Connect: enrollment incomplete");
            if let Some(draft) = &connection.draft_path {
                println!("  draft:       {}", draft.display());
            }
            println!(
                "  a previous enrollment did not finish. Mint a new enrollment key in the Spice \
                 Cloud portal and start the runtime with it (`spiced --token <enrollment-key>`); \
                 the retried enrollment resumes the same pending operation instead of creating a \
                 duplicate instance."
            );
        }
        ConnectionState::NotConnected => {
            println!(
                "Spice Cloud Connect: not connected ({})",
                connection.directory.display()
            );
        }
    }
    if let Some(clock) = &connection.clock {
        println!("  clock:       {clock}");
    }
}

/// The service lines, in the order and wording both commands print them.
fn render_service_lines(service: &ServiceStatus) {
    println!("  service:     {}", describe_service_state(service));
    println!("  starts:      {}", service.starts.describe());
    if let Some(action) = &service.starts_action {
        println!("               run `{action}` to change that");
    }
    if let Some(owner) = &service.owner {
        println!("  owner:       {owner}");
    }
    if let Some(directory) = &service.working_dir {
        println!("  directory:   {}", directory.display());
    }
    if let Some(definition) = &service.definition_path {
        println!("  definition:  {}", definition.display());
    }
    if let Some(runtime) = &service.runtime_path {
        println!("  runtime:     {}", runtime.display());
    }
    println!(
        "  logs:        {}",
        match &service.log_source {
            Some(source) => source.describe(),
            None => "not configured by this service definition".to_string(),
        }
    );
    if let Some(diagnostic) = &service.diagnostic {
        println!("  diagnostic:  {diagnostic}");
    }
}

/// The state line: the normalized state plus, when installed, the scope and
/// supervisor that own it.
fn describe_service_state(service: &ServiceStatus) -> String {
    match (service.scope, service.supervisor) {
        (Some(scope), Some(supervisor)) => {
            format!("{} ({scope} service, {supervisor})", service.state)
        }
        _ => service.state.to_string(),
    }
}

fn render_deployment(deployment: &DeploymentStatus) {
    println!(
        "  deployment:  {}",
        match &deployment.spicepod_path {
            Some(path) => path.display().to_string(),
            None => "none yet — this instance runs its local spicepod until an app is deployed"
                .to_string(),
        }
    );
    println!(
        "  secrets:     {}",
        match deployment.secrets {
            SecretsState::Delivered if deployment.secret_names.is_empty() =>
                "none (the last deployment delivered no secrets)".to_string(),
            SecretsState::Delivered => format!(
                "{} delivered: {}",
                deployment.secret_names.len(),
                deployment.secret_names.join(", ")
            ),
            SecretsState::Unreadable =>
                "cache present but unreadable — deploy the app to re-deliver them".to_string(),
            SecretsState::NotDelivered =>
                "none delivered yet — deploy the app to deliver them".to_string(),
        }
    );
    if let Some(pending) = &deployment.restart_required
        && !pending.is_empty()
    {
        println!("  restart:     required for {}", pending.join(", "));
    }
}

/// What to do next, for the states that have an obvious next step.
fn render_next_steps(status: &ConnectStatus) {
    if status.connection.state == ConnectionState::NotConnected {
        println!(
            "Mint an enrollment key in the Spice Cloud portal and start the runtime with it: \
             `spiced --token <enrollment-key>`."
        );
        return;
    }
    if let Some(monitor) = &status.connection.monitor_url {
        println!("  monitor:     {monitor}");
    }
    if !status.service.installed {
        println!(
            "No service is installed for this directory. Run `spice connect service install` to \
             keep this instance running across reboots."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::service::backend::fake::FakeBackend;
    use super::*;

    async fn snapshot(root: &Path, instance_dir: &Path, config_dir: &Path) -> ConnectStatus {
        ConnectStatus::collect(
            &FakeBackend::new(root),
            instance_dir,
            config_dir,
            "https://api.example",
        )
        .await
    }

    #[tokio::test]
    async fn an_empty_directory_reports_not_connected_and_no_service() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");

        let status = snapshot(dir.path(), &instance_dir, &config_dir).await;
        assert_eq!(status.connection.state, ConnectionState::NotConnected);
        assert!(!status.service.installed);
        assert_eq!(status.deployment.state, DeploymentState::None);
        assert_eq!(status.deployment.secrets, SecretsState::NotDelivered);
        assert_eq!(
            status.deployment.restart_required, None,
            "an unobserved runtime must not read as nothing pending"
        );
        assert!(
            !status.is_degraded(),
            "a directory with no service is not degraded"
        );
    }

    #[tokio::test]
    async fn the_full_and_filtered_json_share_one_service_object() {
        // The acceptance criterion: automation must never have to reconcile two
        // service schemas.
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let status = snapshot(dir.path(), &instance_dir, &config_dir).await;

        let full: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&status).expect("serialize full"))
                .expect("parse full");
        let filtered: serde_json::Value = serde_json::from_str(
            &serde_json::to_string(&status.service_document()).expect("serialize filtered"),
        )
        .expect("parse filtered");

        assert_eq!(full["schema_version"], filtered["schema_version"]);
        assert_eq!(
            serde_json::to_string(&full["service"]).expect("re-serialize full service"),
            serde_json::to_string(&filtered["service"]).expect("re-serialize filtered service"),
            "the service object must be byte-identical in both documents"
        );
    }

    #[tokio::test]
    async fn an_unreadable_manifest_makes_the_report_degraded() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            super::super::service::ServiceManifest::path_in(&config_dir),
            "{",
        )
        .expect("write a broken manifest");

        let status = snapshot(dir.path(), &instance_dir, &config_dir).await;
        assert!(
            status.is_degraded(),
            "a service that cannot be resolved must not read as healthy"
        );
        assert!(status.service.diagnostic.is_some());
    }

    #[tokio::test]
    async fn a_deployed_spicepod_is_reported() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        std::fs::write(
            config_dir.join(runtime_cloud_connect::config::CLOUD_MANAGED_SPICEPOD_FILE),
            "version: v1beta1\n",
        )
        .expect("write a deployed spicepod");

        let status = snapshot(dir.path(), &instance_dir, &config_dir).await;
        assert_eq!(status.deployment.state, DeploymentState::Deployed);
        assert!(status.deployment.spicepod_path.is_some());
    }

    #[tokio::test]
    async fn the_json_report_carries_the_schema_version_and_all_three_sections() {
        let dir = tempfile::tempdir().expect("create tempdir");
        let instance_dir = dir.path().join("edge-1");
        let config_dir = instance_dir.join(".spice");
        let status = snapshot(dir.path(), &instance_dir, &config_dir).await;

        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&status).expect("serialize"))
                .expect("parse");
        assert_eq!(json["schema_version"], STATUS_SCHEMA_VERSION);
        for section in ["connection", "service", "deployment"] {
            assert!(json[section].is_object(), "missing section {section}");
        }
        // No credential material may reach the report.
        let text = serde_json::to_string(&status).expect("serialize");
        for forbidden in ["private_key", "cache_key", "PRIVATE KEY", "pop_sig"] {
            assert!(
                !text.contains(forbidden),
                "{forbidden} must not be reported"
            );
        }
    }
}
