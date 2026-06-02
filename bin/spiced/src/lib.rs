/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::SetOnce;

use app::spicepod::component::runtime::{
    ClientAuthMode as SpicepodClientAuthMode, Runtime as SpicepodRuntime, TelemetryConfig,
};
use app::{App, AppBuilder};
use clap::{ArgAction, Parser, ValueEnum};
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use otel_arrow::OtelArrowExporter;
use repl::ReplConfig;
use runtime::cluster::ResolvedClusterConfig;
use runtime::config::ClusterRole;
use runtime::config::Config as RuntimeConfig;
use runtime::datafusion::DataFusion;
use runtime::podswatcher::PodsWatcher;
use runtime::secrets::ExposeSecret;
use runtime::spice_metrics;
use runtime::{Runtime, auth::EndpointAuth, extension::ExtensionFactory};
use runtime_async::ManagedTokioRuntime;
use snafu::prelude::*;
use spice_cloud::SpiceExtensionFactory;
use spiced_tracing::LogVerbosity;
use tokio::runtime::Handle;
#[cfg(feature = "tpc-extension")]
use tpc_extension::TpcExtensionFactory;
use util::in_tracing_context;
use yaml::Value;

#[cfg(feature = "anonymous_telemetry")]
const TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE: &str = "Usage telemetry is anonymous and aggregated. In Spice.ai Open Source, setting runtime.telemetry.enabled: false in a Spicepod or passing --telemetry-enabled=false does not disable anonymous usage telemetry. To remove anonymous telemetry from an Open Source build, build from source without the anonymous_telemetry feature, or consider using Spice.ai Enterprise. Learn more at https://docs.spice.ai/docs/enterprise";

#[path = "tracing.rs"]
mod spiced_tracing;
mod tls;

/// Registers all external data connectors with the runtime.
///
/// This function must be called during runtime initialization to make the
/// extracted connector crates available. Unlike the built-in connectors in
/// the runtime crate, external connectors are not automatically registered
/// via the `linkme` distributed slice pattern.
pub async fn register_external_connectors() {
    use runtime::dataconnector::register_connector_factory;

    // Always-compiled connectors (no feature gate)
    register_connector_factory(
        connector_graphql::CONNECTOR_NAME,
        connector_graphql::factory(),
    )
    .await;

    // Feature-gated connectors
    #[cfg(feature = "clickhouse")]
    register_connector_factory(
        connector_clickhouse::CONNECTOR_NAME,
        connector_clickhouse::factory(),
    )
    .await;

    #[cfg(feature = "databricks")]
    register_connector_factory(
        connector_databricks::CONNECTOR_NAME,
        connector_databricks::factory(),
    )
    .await;

    #[cfg(feature = "delta_lake")]
    register_connector_factory(
        connector_delta_lake::CONNECTOR_NAME,
        connector_delta_lake::factory(),
    )
    .await;

    #[cfg(feature = "dremio")]
    register_connector_factory(
        connector_dremio::CONNECTOR_NAME,
        connector_dremio::factory(),
    )
    .await;

    #[cfg(feature = "duckdb")]
    register_connector_factory(
        connector_duckdb::CONNECTOR_NAME,
        connector_duckdb::factory(),
    )
    .await;

    #[cfg(feature = "elasticsearch")]
    register_connector_factory(
        connector_elasticsearch::CONNECTOR_NAME,
        connector_elasticsearch::factory(),
    )
    .await;

    #[cfg(feature = "flightsql")]
    register_connector_factory(
        connector_flightsql::CONNECTOR_NAME,
        connector_flightsql::factory(),
    )
    .await;

    #[cfg(feature = "ftp")]
    register_connector_factory(connector_ftp::CONNECTOR_NAME, connector_ftp::factory()).await;

    #[cfg(feature = "imap")]
    register_connector_factory(connector_imap::CONNECTOR_NAME, connector_imap::factory()).await;

    #[cfg(feature = "mongodb")]
    register_connector_factory(
        connector_mongodb::CONNECTOR_NAME,
        connector_mongodb::factory(),
    )
    .await;

    #[cfg(feature = "mssql")]
    register_connector_factory(connector_mssql::CONNECTOR_NAME, connector_mssql::factory()).await;

    #[cfg(feature = "mysql")]
    register_connector_factory(connector_mysql::CONNECTOR_NAME, connector_mysql::factory()).await;

    #[cfg(feature = "nfs")]
    register_connector_factory(connector_nfs::CONNECTOR_NAME, connector_nfs::factory()).await;

    #[cfg(feature = "odbc")]
    register_connector_factory(connector_odbc::CONNECTOR_NAME, connector_odbc::factory()).await;

    #[cfg(feature = "oracle")]
    register_connector_factory(
        connector_oracle::CONNECTOR_NAME,
        connector_oracle::factory(),
    )
    .await;

    #[cfg(feature = "postgres")]
    register_connector_factory(
        connector_postgres::CONNECTOR_NAME,
        connector_postgres::factory(),
    )
    .await;

    #[cfg(feature = "scylladb")]
    register_connector_factory(
        connector_scylladb::CONNECTOR_NAME,
        connector_scylladb::factory(),
    )
    .await;

    #[cfg(feature = "sftp")]
    register_connector_factory(connector_sftp::CONNECTOR_NAME, connector_sftp::factory()).await;

    #[cfg(feature = "sharepoint")]
    register_connector_factory(
        connector_sharepoint::CONNECTOR_NAME,
        connector_sharepoint::factory(),
    )
    .await;

    #[cfg(feature = "smb")]
    register_connector_factory(connector_smb::CONNECTOR_NAME, connector_smb::factory()).await;

    #[cfg(feature = "snowflake")]
    register_connector_factory(
        connector_snowflake::CONNECTOR_NAME,
        connector_snowflake::factory(),
    )
    .await;

    #[cfg(feature = "spark")]
    register_connector_factory(connector_spark::CONNECTOR_NAME, connector_spark::factory()).await;
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to start Spice runtime: {source}"))]
    UnableToConstructSpiceApp { source: Box<app::Error> },

    #[snafu(display("Unable to start Spice Runtime servers: {source}"))]
    UnableToStartServers { source: Box<runtime::Error> },

    #[snafu(display("Failed to load dataset: {source}"))]
    UnableToLoadDataset { source: Box<runtime::Error> },

    #[snafu(display(
        "A required parameter ({parameter}) is missing for data connector: {data_connector}",
    ))]
    RequiredParameterMissing {
        parameter: &'static str,
        data_connector: String,
    },

    #[snafu(display("Failed to initialize the query engine: {source}"))]
    UnableToCreateBackend {
        source: Box<runtime::datafusion::Error>,
    },

    #[snafu(display("Failed to start pods watcher: {source}"))]
    UnableToInitializePodsWatcher { source: runtime::NotifyError },

    #[snafu(display("Unable to configure TLS: {source}"))]
    UnableToInitializeTls { source: Box<dyn std::error::Error> },

    #[snafu(display("Unable to initialize tracing: {source}"))]
    UnableToInitializeTracing {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to initialize metrics: {source}"))]
    UnableToInitializeMetrics { source: Box<dyn std::error::Error> },

    #[snafu(display("Failed to initialize the query processing runtime: {source}"))]
    UnableToInitializeDatafusionTokioRuntime {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unexpected runtime error: {reason}"))]
    GenericError { reason: String },

    #[snafu(display("Invalid cluster configuration: {source}"))]
    InvalidClusterConfig { source: std::io::Error },

    #[snafu(display("Failed to apply the runtime overrides from `--set-runtime`. {reason}"))]
    FailedToApplyOverridesGeneric { reason: String },

    #[snafu(display(
        "Failed to apply the runtime override from `--set-runtime {path}={value}`. {reason}"
    ))]
    FailedToApplyOverride {
        path: String,
        value: String,
        reason: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Clap mirror of [`SpicepodClientAuthMode`]. Defined here so we can
/// derive `ValueEnum` for the `--tls-client-auth-mode` CLI flag
/// without pulling clap into the `spicepod` crate. The two enums
/// are trivially convertible via [`ClientAuthMode::into_spicepod`]
/// and [`ClientAuthMode::from_spicepod`].
#[derive(ValueEnum, Debug, Copy, Clone, PartialEq, Eq, Default)]
#[clap(rename_all = "snake_case")]
pub enum ClientAuthMode {
    /// No client-cert authentication. The server runs
    /// `with_no_client_auth()` at the rustls layer and never sends a
    /// `CertificateRequest`. This is the out-of-the-box default.
    #[default]
    None,
    /// Request but do not require a client cert. The server sends a
    /// `CertificateRequest`; presented certs must be signed by the
    /// configured client CA, but no-cert handshakes are admitted.
    /// Useful for migration windows and audit-only deployments.
    Request,
    /// Require every connection to present a valid X.509 client cert
    /// signed by the configured client CA.
    Required,
}

impl ClientAuthMode {
    #[must_use]
    pub fn into_spicepod(self) -> SpicepodClientAuthMode {
        match self {
            ClientAuthMode::None => SpicepodClientAuthMode::None,
            ClientAuthMode::Request => SpicepodClientAuthMode::Request,
            ClientAuthMode::Required => SpicepodClientAuthMode::Required,
        }
    }

    #[must_use]
    pub fn from_spicepod(value: SpicepodClientAuthMode) -> Self {
        match value {
            SpicepodClientAuthMode::None => ClientAuthMode::None,
            SpicepodClientAuthMode::Request => ClientAuthMode::Request,
            SpicepodClientAuthMode::Required => ClientAuthMode::Required,
        }
    }
}

#[derive(Parser, Debug)]
#[clap(about = "Spice.ai OSS Runtime")]
#[clap(rename_all = "kebab-case")]
#[expect(clippy::struct_excessive_bools)]
pub struct Args {
    /// Enable Prometheus metrics. (disabled by default)
    #[arg(long, value_name = "BIND_ADDRESS", help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// Deprecated OpenTelemetry bind address (no effect).
    #[arg(
        long = "open_telemetry",
        value_name = "OPEN_TELEMETRY_BIND_ADDRESS",
        default_value = "127.0.0.1:50052",
        action
    )]
    pub open_telemetry_bind_address: SocketAddr,

    /// Print the version and exit.
    #[arg(long)]
    pub version: bool,

    /// All runtime related arguments
    #[clap(flatten)]
    pub runtime: RuntimeConfig,

    /// Starts a SQL REPL to interactively query against the runtime's Flight endpoint.
    #[arg(long, help_heading = "SQL REPL")]
    pub repl: bool,

    #[clap(flatten)]
    pub repl_config: ReplConfig,

    /// Enable TLS for the runtime.
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    pub tls_enabled: bool,

    /// The TLS PEM-encoded certificate.
    #[arg(long, value_name = "-----BEGIN CERTIFICATE-----...")]
    pub tls_certificate: Option<String>,

    /// Path to the TLS PEM-encoded certificate file.
    #[arg(long, value_name = "cert.pem")]
    pub tls_certificate_file: Option<String>,

    /// The TLS PEM-encoded private key.
    #[arg(long, value_name = "-----BEGIN PRIVATE KEY-----...")]
    pub tls_key: Option<String>,

    /// Path to the TLS PEM-encoded private key file.
    #[arg(long, value_name = "key.pem")]
    pub tls_key_file: Option<String>,

    /// Path to a PEM-encoded CA bundle used to verify client certificates
    /// when `--tls-client-auth-mode request` or `required` (or the
    /// equivalent spicepod `runtime.tls.client_auth_mode`) is set.
    /// Eligible for hot-reload via the same watcher that picks up
    /// server cert / key rotations.
    #[arg(long, value_name = "client-ca.pem")]
    pub tls_client_auth_ca_file: Option<String>,

    /// Inline PEM-encoded CA bundle used to verify client certificates.
    /// Mutually exclusive with `--tls-client-auth-ca-file`. Inline material
    /// is not hot-reloaded.
    #[arg(long, value_name = "-----BEGIN CERTIFICATE-----...")]
    pub tls_client_auth_ca: Option<String>,

    /// How the runtime treats client certificates on the public TLS
    /// endpoints. `none` disables client-cert authentication (default).
    /// `request` sends `CertificateRequest` but accepts no-cert
    /// handshakes; presented certs must be signed by the client CA.
    /// `required` enables strict mTLS — the server demands a valid
    /// cert signed by `--tls-client-auth-ca` / `--tls-client-auth-ca-file`.
    #[arg(long, value_enum)]
    pub tls_client_auth_mode: Option<ClientAuthMode>,

    /// Enable anonymous telemetry collection. In Open Source builds that include anonymous telemetry,
    /// `false` is ignored; build without the `anonymous_telemetry` feature to remove anonymous usage telemetry.
    #[arg(long)]
    pub telemetry_enabled: Option<bool>,

    /// Enable pods watcher (disabled by default).
    #[arg(long, default_value_t = false, action = ArgAction::SetTrue)]
    pub pods_watcher_enabled: bool,

    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,

    /// Enable very verbose logging. In conjunction with `verbose` can be set via -vv or --very-verbose.
    #[arg(long)]
    pub very_verbose: bool,

    /// Path to the Spicepod directory or file. Supports local paths and remote URLs (i.e. `s3://my_bucket/spicepod.yaml`)
    ///
    /// When specified, the behavior to automatically reload changes to the Spicepod is disabled.
    #[arg(value_name = "PATH")]
    pub spicepod: Option<PathBuf>,

    /// Overrides for the runtime configuration (--set-runtime key1.subkey=value1)
    #[arg(long, action = ArgAction::Append, value_parser = parse_set_string)]
    pub set_runtime: Vec<(String, String)>,

    #[arg(skip)]
    pub open_telemetry_deprecated: bool,
}

/// Spawn a tokio task that listens for `SIGHUP` and asks the
/// process-wide [`runtime::tls::TlsControl`] to reload every TLS
/// material the runtime is watching. Mirrors the `nginx -s reload` /
/// `kill -HUP <pid>` convention.
///
/// The reload itself is **best-effort and asynchronous**:
/// `TlsControl::reload_all` enqueues a sentinel op on the watcher's
/// dispatcher channel and returns as soon as the op is queued. The
/// actual parse + validate + `ArcSwap::store` runs on the dispatcher
/// thread shortly after; operators that need to confirm the rotation
/// landed should watch the `tls_reload_total{result="ok"}` metric.
///
/// On Windows or other targets without SIGHUP semantics this is a
/// no-op: rotation still works via the polling filesystem watcher.
fn spawn_sighup_reload_task(control: std::sync::Arc<runtime::tls::TlsControl>) {
    #[cfg(unix)]
    {
        tokio::spawn(async move {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sighup = match signal(SignalKind::hangup()) {
                Ok(s) => s,
                Err(err) => {
                    tracing::warn!("TLS reload: failed to install SIGHUP handler: {err}");
                    return;
                }
            };
            tracing::debug!("TLS reload: SIGHUP handler installed");
            while sighup.recv().await.is_some() {
                tracing::info!("TLS reload: SIGHUP received, reloading TLS material");
                if let Err(err) = control.reload_all() {
                    tracing::warn!("TLS reload: SIGHUP reload failed: {err}");
                }
            }
        });
    }
    #[cfg(not(unix))]
    {
        // Non-Unix platforms have no SIGHUP equivalent; rotation still
        // works via the polling filesystem watcher.
        let _ = control;
    }
}

pub async fn run(args: Args) -> Result<()> {
    // Register external data connectors before runtime initialization.
    // This makes connectors from extracted crates available to the runtime.
    register_external_connectors().await;

    let prometheus_registry = args.metrics.map(|_| prometheus::Registry::new());

    let spicepod_path = args
        .spicepod
        .clone()
        .unwrap_or_else(|| env::current_dir().unwrap_or(PathBuf::from(".")));

    let (app, spicepod_load_error) = build_app(&args).await?;
    let mut extension_factories: Vec<Box<dyn ExtensionFactory>> = vec![];

    if let Some(some_app) = &app
        && let Some(manifest) = some_app.extensions.get("spice_cloud")
    {
        let spice_extension_factory = SpiceExtensionFactory::new(manifest.clone());
        extension_factories.push(Box::new(spice_extension_factory));
    }

    #[cfg(feature = "tpc-extension")]
    if let Some(some_app) = &app
        && let Some(manifest) = some_app.extensions.get("tpc")
    {
        let tpc_extension_factory = TpcExtensionFactory::new(manifest.clone());
        extension_factories.push(Box::new(tpc_extension_factory));
    }

    let runtime_config = app.as_ref().map(|app| &app.runtime);
    let app_name = app.as_ref().map(|app| app.name.clone());
    let spicepod_tls_config = runtime_config.and_then(|rt| rt.tls.clone());
    let tracing_config = runtime_config.and_then(|rt| rt.tracing.clone());

    // Anonymous telemetry is a function of two inputs: the CLI flag and the
    // spicepod `runtime.telemetry` config.  For schedulers and standalone
    // instances the config is available immediately from the local spicepod.
    // Executors don't have a spicepod — they fetch the app definition from the
    // scheduler after joining the cluster — so the config is resolved later.
    // A `SetOnce` lets `start_anonymous_telemetry` wait for the value.
    let telemetry_config: Arc<SetOnce<TelemetryConfig>> = Arc::new(SetOnce::new());

    let is_executor = matches!(args.runtime.cluster.role, Some(ClusterRole::Executor))
        || (args.runtime.cluster.role.is_none()
            && args.runtime.cluster.scheduler_address.is_some());

    if !is_executor {
        // Resolve immediately from the local spicepod (or use default).
        let config = runtime_config
            .map(|rt| rt.telemetry.clone())
            .unwrap_or_default();
        let _ = telemetry_config.set(config);
    }

    // Configure Flight `DoPut` rate limits from the local spicepod runtime.flight settings.
    // Executors inherit their effective setting from the scheduler's app definition after they join the cluster.
    let flight_config = runtime_config.and_then(|rt| rt.flight.clone());
    let rate_limits = {
        let mut limits = runtime::flight::RateLimits::default();
        if let Some(ref flight) = flight_config {
            limits = limits.with_flight_write_enabled(flight.do_put_rate_limit_enabled);
        }
        limits
    };

    // Single, process-wide TLS reload control plane. Both public TLS
    // (HTTP / Flight / Metrics) and cluster mTLS register their reload
    // callbacks here so we have one watcher, one dispatcher thread, one
    // SIGHUP target. Created lazily on success of `TlsControl::new`; if
    // the watcher fails to spawn we surface the error eagerly.
    let tls_control = std::sync::Arc::new(runtime::tls::TlsControl::new().map_err(|e| {
        Error::UnableToInitializeTls {
            source: Box::new(e),
        }
    })?);

    let resolved_cluster_config = in_tracing_context(|| {
        ResolvedClusterConfig::try_new_with_tls(
            args.runtime.cluster.clone(),
            Some(tls_control.as_ref()),
        )
    });

    let mut builder = Runtime::builder()
        .with_app_opt(app.clone())
        // User configured extensions
        .with_extensions(extension_factories)
        // Extensions that will be auto-loaded if not explicitly loaded and requested by a component
        .with_autoload_extensions(HashMap::from([(
            "spice_cloud".to_string(),
            Box::new(SpiceExtensionFactory::default()) as Box<dyn ExtensionFactory>,
        )]))
        .with_datasets_health_monitor()
        .with_metrics_server_opt(args.metrics, prometheus_registry.clone())
        .with_runtime_config(args.runtime.clone())
        .with_rate_limits(rate_limits)
        .with_io_runtime(Handle::current());

    // Check for explicit cluster role OR implicit executor role (scheduler_address set without explicit role)
    let is_cluster_mode =
        args.runtime.cluster.role.is_some() || args.runtime.cluster.scheduler_address.is_some();

    // Create MetricsReader for cluster mode to enable on-demand OTLP metrics collection
    let metrics_reader = if is_cluster_mode {
        Some(runtime::metrics_reader::MetricsReader::new())
    } else {
        None
    };

    match resolved_cluster_config {
        Ok(resolved_cluster_config) => {
            // Validate that scheduler mode has state_location configured
            if resolved_cluster_config.effective_role() == Some(ClusterRole::Scheduler) {
                let has_state_location = app
                    .as_ref()
                    .and_then(|a| a.runtime.scheduler.as_ref())
                    .is_some();
                if !has_state_location {
                    return Err(Error::InvalidClusterConfig {
                        source: std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "Scheduler mode requires `runtime.scheduler.state_location` to be configured in the spicepod. See: https://spiceai.org/docs/features/distributed-query",
                        ),
                    });
                }
            }

            builder = builder.with_resolved_cluster_config(resolved_cluster_config);
        }
        Err(e) if is_cluster_mode => {
            // If cluster mode is intended (explicit --role or implicit via --scheduler-address), surface the error
            return Err(Error::InvalidClusterConfig { source: e });
        }
        Err(_) => {
            // No cluster mode specified, silently continue in standalone mode
        }
    }

    // Add metrics reader to runtime for cluster observability
    if let Some(ref reader) = metrics_reader {
        builder = builder.with_metrics_reader(reader.clone());
    }

    if is_executor {
        builder = builder.with_telemetry_config(Arc::clone(&telemetry_config));
    }

    if args.pods_watcher_enabled && args.spicepod.is_none() {
        let pods_watcher = PodsWatcher::new(spicepod_path.clone());
        builder = builder.with_pods_watcher(pods_watcher);
    }

    let rt = builder.build().await;

    spiced_tracing::init_tracing(
        app.as_ref(),
        tracing_config.as_ref(),
        rt.datafusion(),
        LogVerbosity::from_flags_and_env_and_config(
            args.verbose == 1,                      // -v or --verbose
            args.verbose >= 2 || args.very_verbose, // -vv or --very-verbose
            "SPICED_LOG",
            app.as_ref().and_then(|a| a.runtime.output_level),
        ),
    )
    .await
    .context(UnableToInitializeTracingSnafu)?;

    if args.open_telemetry_deprecated {
        tracing::warn!(
            "`--open_telemetry` is deprecated and has no effect; it will be removed in a future version"
        );
    }

    // Log spicepod load error now that tracing is initialized
    if let Some(err) = spicepod_load_error {
        tracing::warn!(
            "Starting in pods watcher mode without a valid spicepod.yaml. The runtime will load components once a valid spicepod.yaml is provided.\n{err}"
        );
    }

    // Configure the CPU runtime for DataFusion by default. Opt-out via `runtime.params.dedicated_thread_pool=disabled`
    match App::get_runtime_param_opt::<String>(&app, "dedicated_thread_pool").as_deref() {
        Some("sql_engine") | None => {
            // This needs to be created after tracing is set up, or else task_history events aren't emitted.
            let cpu_runtime = ManagedTokioRuntime::try_new()
                .boxed()
                .context(UnableToInitializeDatafusionTokioRuntimeSnafu)?;

            rt.datafusion().set_cpu_runtime(cpu_runtime);

            // Create a dedicated refresh runtime for acceleration refresh workers and
            // stale-while-revalidate background cache refresh tasks. This isolates refresh
            // workloads from query execution to prevent large refresh operations from
            // impacting query latency.
            // Uses low thread priority to minimize impact on latency-sensitive operations.
            let refresh_runtime = ManagedTokioRuntime::builder()
                .with_low_priority()
                .with_thread_name("refresh-worker")
                .build()
                .boxed()
                .context(UnableToInitializeDatafusionTokioRuntimeSnafu)?;

            rt.datafusion().set_refresh_runtime(refresh_runtime);

            // Bring up the dedicated compaction runtime only when the DataFusion
            // builder carved a compaction memory environment — i.e. Cayenne
            // acceleration is configured on a dataset (and we're in this arm, so
            // dedicated thread pools are enabled). `set_compaction_runtime`
            // injects both the runtime handle and the carved memory environment
            // into the Cayenne crate, isolating compaction on CPU and memory.
            if rt.datafusion().compaction_runtime_env().is_some() {
                let compaction_runtime = ManagedTokioRuntime::builder()
                    .with_low_priority()
                    .with_thread_name("compaction-worker")
                    .build()
                    .boxed()
                    .context(UnableToInitializeDatafusionTokioRuntimeSnafu)?;

                rt.datafusion().set_compaction_runtime(compaction_runtime);
            }
        }
        Some("disabled") => {
            tracing::info!(
                "Dedicated SQL engine thread pool is disabled via runtime parameter `runtime.params.dedicated_thread_pool`."
            );
        }
        Some(other) => {
            tracing::warn!(
                "Invalid runtime parameter value for `runtime.params.dedicated_thread_pool`: `{other}`. Set to `disabled` or `sql_engine`. Continuing with dedicated SQL engine thread pool."
            );
        }
    }

    let otel_config = telemetry_config
        .get()
        .and_then(|c| c.otel_exporter.as_ref())
        .filter(|c| c.enabled);

    let needs_metrics =
        prometheus_registry.is_some() || otel_config.is_some() || metrics_reader.is_some();

    if needs_metrics {
        // Resolve secrets in OTEL exporter headers before initializing metrics
        let resolved_otel_headers = if let Some(config) = otel_config {
            let mut resolved = std::collections::HashMap::new();
            let secrets = rt.secrets();
            let secrets_guard = secrets.read().await;
            for (key, value) in &config.headers {
                let resolved_value = secrets_guard
                    .inject_secrets(key, runtime::secrets::ParamStr(value.as_ref()))
                    .await;
                resolved.insert(key.clone(), resolved_value.expose_secret().to_string());
            }
            drop(secrets_guard);
            resolved
        } else {
            std::collections::HashMap::new()
        };

        // Pre-build resource attributes from `runtime.telemetry.properties`.
        // In standalone and scheduler modes the SetOnce is already filled at
        // this point. In executor mode the SetOnce is resolved later, after
        // the executor fetches the app definition from the scheduler — same
        // as `otel_config` above. Executors emit metrics through the cluster
        // on-demand reader and inherit attribution from the scheduler-side
        // pipeline, so the empty-resource case here is consistent with the
        // surrounding executor config flow.
        let resource_attributes: Vec<KeyValue> = telemetry_config
            .get()
            .map(|c| {
                c.properties
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let metric_prefix = telemetry_config.get().and_then(|c| c.metric_prefix.clone());

        init_metrics(
            &rt.datafusion(),
            prometheus_registry.clone(),
            otel_config,
            resolved_otel_headers,
            metrics_reader,
            resource_attributes,
            metric_prefix,
        )
        .context(UnableToInitializeMetricsSnafu)?;

        // Metrics are now initialized (the Prometheus meter provider is installed).
        // Register the Cayenne compaction instruments so the carved pool-size gauge
        // plus the duration + exhaustion metrics appear in `/metrics` from startup.
        // The compaction runtime is set up earlier (before metrics init), so the
        // instruments must be (re)bound to the real meter here rather than at carve
        // time — otherwise they'd bind to the early noop meter and never export.
        if let Some(bytes) = rt.datafusion().compaction_memory_pool_bytes() {
            telemetry::register_cayenne_compaction_metrics(bytes);
        }
    }

    let (tls_config, client_auth_mode) = tls::load_tls_config(
        &args,
        spicepod_tls_config.as_ref(),
        rt.secrets(),
        tls_control.as_ref(),
    )
    .await
    .context(UnableToInitializeTlsSnafu)?;

    // Wire SIGHUP -> tls_control.reload_all() once. Both public and
    // cluster TLS register on the same `TlsControl`, so a single signal
    // pickup rotates everything atomically.
    spawn_sighup_reload_task(std::sync::Arc::clone(&tls_control));

    let telemetry_enabled = args.telemetry_enabled;
    let telemetry_config_clone = Arc::clone(&telemetry_config);
    let app_name_clone = app_name.clone();
    tokio::spawn(async move {
        start_anonymous_telemetry(
            telemetry_enabled,
            telemetry_config_clone,
            app_name_clone.as_ref(),
        )
        .await;
    });

    let rt = Arc::new(rt);

    if needs_metrics {
        rt.init_cache_metrics();
    }

    let cloned_rt = Arc::clone(&rt);
    let endpoint_auth = match app.as_ref() {
        Some(app) => EndpointAuth::new(rt.secrets(), app).await,
        None => EndpointAuth::no_auth(),
    };

    // Compute the process-wide IdentitySource from the combination of
    // `runtime.auth` (recorded by `EndpointAuth::new`) and the
    // resolved public TLS `client_auth_mode`:
    //
    // | runtime.auth | client_auth_mode | IdentitySource |
    // |--------------|------------------|----------------|
    // | unset        | none             | Anonymous      |
    // | unset        | request          | Channel        |
    // | unset        | required         | Channel        |
    // | set          | none             | RuntimeAuth    |
    // | set          | request          | RuntimeAuth    |
    // | set          | required         | RuntimeAuth    | ("mTLS-as-channel")
    let runtime_auth_configured = endpoint_auth.http_auth.is_some();
    let identity_source = match (runtime_auth_configured, client_auth_mode) {
        (true, _) => runtime_auth::IdentitySource::RuntimeAuth,
        (false, ClientAuthMode::Request | ClientAuthMode::Required) => {
            runtime_auth::IdentitySource::Channel
        }
        (false, ClientAuthMode::None) => runtime_auth::IdentitySource::Anonymous,
    };
    if matches!(
        (runtime_auth_configured, client_auth_mode),
        (true, ClientAuthMode::Required)
    ) {
        tracing::info!(
            "mTLS-as-channel mode active: client cert AND `runtime.auth` credentials \
             are both required"
        );
    }
    let endpoint_auth = endpoint_auth.with_identity_source(identity_source);

    let server_thread = tokio::spawn(async move {
        Box::pin(cloned_rt.start_servers(args.runtime, tls_config, endpoint_auth)).await
    });

    tokio::select! {
        () = Arc::clone(&rt).load_components() => {},
        () = runtime::shutdown_signal() => {
            tracing::debug!("Cancelling runtime initializing!");
        },
    }

    let result = match server_thread.await {
        // Don't treat force terminated as an error
        Ok(Err(runtime::Error::ForceTerminated)) => Ok(()),
        Ok(ok) => ok.map_err(|e| Error::UnableToStartServers {
            source: Box::new(e),
        }),
        Err(_) => Err(Error::GenericError {
            reason: "Unable to start spiced".into(),
        }),
    };

    rt.shutdown().await;

    result
}

async fn build_app(args: &Args) -> Result<(Option<Arc<App>>, Option<app::Error>)> {
    // Check for explicit executor role OR implicit executor role (scheduler_address set without explicit role)
    let is_executor = matches!(args.runtime.cluster.role, Some(ClusterRole::Executor))
        || (args.runtime.cluster.role.is_none()
            && args.runtime.cluster.scheduler_address.is_some());

    if is_executor {
        // If a spicepod is explicitly provided, load just the runtime config (e.g. flight rate
        // limits, telemetry) while using a default App for datasets/catalogs (those come from
        // the scheduler via the cluster protocol).
        if let Some(ref path) = args.spicepod
            && let Ok(built_app) = AppBuilder::build_from_path(path.clone()).await
        {
            let mut app = App::default();
            // Copy only runtime flight and telemetry config from the spicepod.
            app.runtime.flight = built_app.runtime.flight;
            app.runtime.telemetry = built_app.runtime.telemetry;
            app.runtime = apply_overrides(app.runtime, &args.set_runtime)?;
            tracing::info!("Starting as a cluster executor with runtime config from spicepod.");
            return Ok((Some(Arc::new(app)), None));
        }
        tracing::info!(
            "Starting as a cluster executor, without a Spicepod. The runtime will initialize its components upon joining the cluster."
        );
        return Ok((Some(Arc::new(App::default())), None));
    }

    let spicepod_path = args
        .spicepod
        .clone()
        .unwrap_or_else(|| env::current_dir().unwrap_or(PathBuf::from(".")));

    let mut spicepod_load_error: Option<app::Error> = None;

    let app: Option<Arc<App>> = match AppBuilder::build_from_path(spicepod_path.clone()).await {
        Ok(mut app) => {
            app.runtime = apply_overrides(app.runtime, &args.set_runtime)?;
            Some(Arc::new(app))
        }
        Err(e) => {
            // In pods watcher mode, allow runtime to start without a valid spicepod
            // It will load the spicepod when it becomes available
            if args.pods_watcher_enabled && args.spicepod.is_none() {
                spicepod_load_error = Some(e);
                None
            } else {
                // In normal mode, fail immediately if spicepod cannot be loaded
                return Err(Error::UnableToConstructSpiceApp {
                    source: Box::new(e),
                });
            }
        }
    };

    Ok((app, spicepod_load_error))
}

/// Initializes the global [`SdkMeterProvider`] with whichever metric sinks the
/// caller has configured. Each reader is attached independently; any
/// combination is valid as long as at least one source is present.
///
/// Sinks and how they are turned on:
/// - **Prometheus scrape** (`registry` is `Some`): enabled by passing
///   `--metrics <addr>` on the command line. Also attaches the `spice_metrics`
///   periodic reader that writes runtime metrics into `DataFusion` for the local
///   task-history / observability tables.
/// - **Cluster on-demand OTLP** (`metrics_reader` is `Some`): enabled when
///   spiced runs as a cluster executor. The reader lets a scheduler pull
///   metrics over the control stream even when neither `--metrics` nor
///   `otel_exporter` is configured — this subsumes the former
///   `init_cluster_metrics_only` path.
/// - **OTEL push exporter** (`otel_config` is `Some` and enabled): enabled
///   purely by `runtime.telemetry.otel_exporter` in `spicepod.yaml`. No
///   command-line flag is required; works standalone or alongside the other
///   sinks. `resolved_otel_headers` must already have secret templates
///   resolved by the caller.
///
/// Caller is expected to short-circuit (not invoke this fn) when none of the
/// three sources is configured — otherwise an empty `MeterProvider` would be
/// installed.
fn init_metrics(
    df: &Arc<DataFusion>,
    registry: Option<prometheus::Registry>,
    otel_config: Option<&app::spicepod::component::runtime::OtelExporterConfig>,
    resolved_otel_headers: std::collections::HashMap<String, String>,
    metrics_reader: Option<runtime::metrics_reader::MetricsReader>,
    resource_attributes: Vec<KeyValue>,
    metric_prefix: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Apply user-configured `runtime.telemetry.properties` as OpenTelemetry
    // resource attributes so they appear as dimensions/tags on every metric
    // exported by any of the readers attached below (Prometheus scrape,
    // cluster on-demand OTLP, OTEL push). Standard env vars such as
    // `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` are still merged in
    // by `Resource::builder()`; explicit attributes here take precedence over
    // env-derived ones with the same key.
    let mut resource_builder = Resource::builder();
    if !resource_attributes.is_empty() {
        resource_builder = resource_builder.with_attributes(resource_attributes);
    }
    let resource = resource_builder.build();

    let mut provider_builder = SdkMeterProvider::builder().with_resource(resource);

    // Optional metric name prefix (e.g. "spiceai.") configured under
    // `runtime.telemetry.metric_prefix`. Applied via an OTel View on the
    // MeterProvider, so the rename happens once at the SDK layer and is
    // observed by every reader attached below (Prometheus scrape, cluster
    // on-demand OTLP, OTEL push). The prefix is intentionally placed at the
    // telemetry level rather than under any single exporter because
    // OpenTelemetry 0.31's SDK does not support per-reader name transforms.
    if let Some(prefix) = metric_prefix.filter(|p| !p.is_empty()) {
        tracing::info!(prefix = %prefix, "OTEL metrics name prefix enabled");
        provider_builder = provider_builder.with_view(
            move |instrument: &opentelemetry_sdk::metrics::Instrument| {
                let new_name = format!("{prefix}{}", instrument.name());
                match opentelemetry_sdk::metrics::Stream::builder()
                    .with_name(new_name.clone())
                    .build()
                {
                    Ok(stream) => Some(stream),
                    Err(e) => {
                        tracing::warn!(
                            instrument = %instrument.name(),
                            new_name = %new_name,
                            error = %e,
                            "Failed to apply OTEL metric prefix; instrument will keep its original name"
                        );
                        None
                    }
                }
            },
        );
    }

    // Case 1: Prometheus scrape
    if let Some(registry) = registry {
        let prometheus_exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry)
            .without_scope_info()
            .without_units()
            .without_counter_suffixes()
            .without_target_info()
            .build()?;
        provider_builder = provider_builder.with_reader(prometheus_exporter);

        let spice_metrics_exporter =
            OtelArrowExporter::new(spice_metrics::SpiceMetricsExporter::new(df));
        let spice_metrics_reader =
            PeriodicReader::builder(spice_metrics_exporter, opentelemetry_sdk::runtime::Tokio)
                .with_interval(Duration::from_secs(30))
                .build();
        provider_builder = provider_builder.with_reader(spice_metrics_reader);
    }

    // Case 2: Cluster on-demand OTLP
    if let Some(reader) = metrics_reader {
        provider_builder = provider_builder.with_reader(reader);
        tracing::debug!("Cluster metrics reader enabled for on-demand OTLP collection");
    }

    // Case 3: OTEL push exporter
    if let Some(config) = otel_config {
        match create_otel_reader(config, resolved_otel_headers) {
            Ok(otel_reader) => {
                provider_builder = provider_builder.with_reader(otel_reader);
                let protocol = if config.is_http() { "http" } else { "grpc" };
                tracing::info!(
                    endpoint = %config.endpoint,
                    protocol = protocol,
                    push_interval = %config.push_interval,
                    temporality = ?config.temporality,
                    "OTEL metrics exporter enabled"
                );
            }
            Err(e) => {
                tracing::error!("Failed to initialize OTEL metrics exporter: {e}");
            }
        }
    }

    let provider = provider_builder.build();
    global::set_meter_provider(provider);

    Ok(())
}

/// Creates an OTEL periodic reader from the spicepod config
fn create_otel_reader(
    config: &app::spicepod::component::runtime::OtelExporterConfig,
    resolved_headers: std::collections::HashMap<String, String>,
) -> Result<runtime::otel_push_exporter::OtelPeriodicReader, runtime::otel_push_exporter::Error> {
    runtime::otel_push_exporter::create_otel_periodic_reader(config, resolved_headers)
}

async fn start_anonymous_telemetry(
    telemetry_enabled: Option<bool>,
    telemetry_config: Arc<SetOnce<TelemetryConfig>>,
    #[cfg(feature = "anonymous_telemetry")] spicepod_name: Option<&String>,
    #[cfg(not(feature = "anonymous_telemetry"))] _spicepod_name: Option<&String>,
) {
    // Always log hardware info at debug level regardless of telemetry settings
    // Use async version to avoid blocking the async runtime
    let hardware_info = telemetry::hardware::HardwareInfo::detect_async()
        .await
        .unwrap_or_else(|_| telemetry::hardware::HardwareInfo::detect());
    hardware_info.log_debug();

    #[cfg(not(feature = "anonymous_telemetry"))]
    {
        let _ = (telemetry_enabled, telemetry_config);
    }

    #[cfg(feature = "anonymous_telemetry")]
    {
        // Wait for the spicepod telemetry config to be resolved.  For schedulers
        // and standalone instances this is already set; for executors it will be
        // set once the app definition is fetched from the scheduler.
        let config = telemetry_config.wait().await;

        if should_warn_telemetry_disabled_setting_ignored(telemetry_enabled, config) {
            tracing::warn!("{TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE}");
        }

        let telemetry_properties: Vec<KeyValue> = config
            .properties
            .iter()
            .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
            .collect();

        telemetry::anonymous::start(
            spicepod_name.map_or_else(|| "unknown", String::as_str),
            telemetry_properties,
        )
        .await;
    }
}

#[cfg(any(test, feature = "anonymous_telemetry"))]
fn should_warn_telemetry_disabled_setting_ignored(
    telemetry_enabled: Option<bool>,
    config: &TelemetryConfig,
) -> bool {
    #[cfg(feature = "anonymous_telemetry")]
    {
        telemetry_enabled == Some(false) || (telemetry_enabled.is_none() && !config.enabled)
    }

    #[cfg(not(feature = "anonymous_telemetry"))]
    {
        let _ = (telemetry_enabled, config);
        false
    }
}

fn parse_set_string(s: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = s.split('=').collect();
    if parts.len() != 2 {
        return Err("Invalid set format. Use key=value".into());
    }

    Ok((parts[0].to_string(), parts[1].to_string()))
}

fn apply_overrides(
    runtime_config: SpicepodRuntime,
    overrides: &Vec<(String, String)>,
) -> Result<SpicepodRuntime> {
    if overrides.is_empty() {
        return Ok(runtime_config);
    }

    let mut yaml = match yaml::to_value(&runtime_config) {
        Ok(yaml) => yaml,
        Err(e) => {
            return FailedToApplyOverridesGenericSnafu {
                reason: format!("Runtime configuration is invalid YAML. {e}"),
            }
            .fail();
        }
    };

    for (path, value) in overrides {
        let yaml_value = yaml::from_str(value).unwrap_or_else(|_| Value::String(value.clone()));
        match apply_override(&mut yaml, path, yaml_value) {
            Ok(()) => (),
            Err(e) => {
                return FailedToApplyOverrideSnafu {
                    path: path.clone(),
                    value: value.clone(),
                    reason: format!("{e}"),
                }
                .fail();
            }
        }
    }

    match yaml::from_value(yaml) {
        Ok(runtime) => Ok(runtime),
        Err(e) => {
            FailedToApplyOverridesGenericSnafu {
                reason: format!(
                    "The runtime configuration after applying the overrides from `--set-runtime` is invalid. {e}"
                ),
            }
            .fail()
        }
    }
}

fn apply_override(
    yaml: &mut Value,
    path: &str,
    value: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = yaml;

    let parts_len = parts.len();
    for (i, part) in parts.into_iter().enumerate() {
        if i == parts_len - 1 {
            match current {
                Value::Mapping(map) => {
                    map.insert(Value::String(part.to_string()), value);
                    return Ok(());
                }
                Value::Null => {
                    let mut new_map = yaml::Mapping::new();
                    new_map.insert(Value::String(part.to_string()), value);
                    *current = Value::Mapping(new_map);
                    return Ok(());
                }
                _ => {
                    return Err(format!(
                        "Unable to apply override for {path}. Validate the override is correct and try again.",
                    )
                    .into())
                }
            }
        }

        match current {
            Value::Mapping(map) => {
                if !map.contains_key(&Value::String(part.to_string())) {
                    map.insert(
                        Value::String(part.to_string()),
                        Value::Mapping(yaml::Mapping::new()),
                    );
                }
                let key = Value::String(part.to_string());
                let Some(new_current) = map.get_mut(&key) else {
                    unreachable!("The key was inserted above if it was missing");
                };
                current = new_current;
            }
            _ => return Err(format!("Unable to apply override for {path}. Validate the override is correct and try again.").into()),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "anonymous_telemetry")]
    #[test]
    fn warns_when_spicepod_disables_telemetry_without_cli_override() {
        let config = TelemetryConfig {
            enabled: false,
            ..Default::default()
        };

        assert!(should_warn_telemetry_disabled_setting_ignored(
            None, &config
        ));
    }

    #[cfg(feature = "anonymous_telemetry")]
    #[test]
    fn warns_when_cli_disables_telemetry() {
        let config = TelemetryConfig::default();

        assert!(should_warn_telemetry_disabled_setting_ignored(
            Some(false),
            &config
        ));
    }

    #[cfg(not(feature = "anonymous_telemetry"))]
    #[test]
    fn does_not_warn_when_anonymous_telemetry_is_not_compiled() {
        let config = TelemetryConfig {
            enabled: false,
            ..Default::default()
        };

        assert!(!should_warn_telemetry_disabled_setting_ignored(
            None, &config
        ));

        assert!(!should_warn_telemetry_disabled_setting_ignored(
            Some(false),
            &config
        ));
    }

    #[cfg(not(feature = "anonymous_telemetry"))]
    #[tokio::test]
    async fn returns_without_telemetry_config_when_anonymous_telemetry_is_not_compiled() {
        let telemetry_config = Arc::new(SetOnce::new());

        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            start_anonymous_telemetry(None, telemetry_config, None),
        )
        .await
        .expect("anonymous telemetry should return without waiting for telemetry config when the feature is disabled");
    }

    #[test]
    fn does_not_warn_when_spicepod_enables_telemetry() {
        let config = TelemetryConfig::default();

        assert!(!should_warn_telemetry_disabled_setting_ignored(
            None, &config
        ));
    }

    #[test]
    fn does_not_warn_when_cli_enables_telemetry() {
        let config = TelemetryConfig {
            enabled: false,
            ..Default::default()
        };

        assert!(!should_warn_telemetry_disabled_setting_ignored(
            Some(true),
            &config
        ));
    }

    #[cfg(feature = "anonymous_telemetry")]
    #[test]
    fn telemetry_disabled_message_mentions_supported_disable_paths() {
        assert!(TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE.contains("anonymous and aggregated"));
        assert!(TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE.contains(
            "runtime.telemetry.enabled: false in a Spicepod or passing --telemetry-enabled=false does not disable anonymous usage telemetry"
        ));
        assert!(TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE.contains("--telemetry-enabled=false"));
        assert!(
            TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE
                .contains("without the anonymous_telemetry feature")
        );
        assert!(TELEMETRY_DISABLED_SETTING_IGNORED_MESSAGE.contains("Spice.ai Enterprise"));
    }
}
