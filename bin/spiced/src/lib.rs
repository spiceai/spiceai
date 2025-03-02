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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use app::spicepod::component::runtime::{Runtime as SpicepodRuntime, TelemetryConfig};
use app::{App, AppBuilder};
use clap::{ArgAction, Parser};
use flightrepl::ReplConfig;
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::Resource;
use otel_arrow::OtelArrowExporter;
use runtime::config::Config as RuntimeConfig;
use runtime::datafusion::DataFusion;
use runtime::podswatcher::PodsWatcher;
use runtime::spice_metrics;
use runtime::{auth::EndpointAuth, extension::ExtensionFactory, Runtime};
use serde_yaml::Value;
use snafu::prelude::*;
use spice_cloud::SpiceExtensionFactory;
use spiced_tracing::LogVerbosity;
use tokio::sync::broadcast;
#[cfg(feature = "tpc-extension")]
use tpc_extension::TpcExtensionFactory;
use tracing::subscriber;

#[path = "tracing.rs"]
mod spiced_tracing;
mod tls;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app: {source}"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime servers: {source}"))]
    UnableToStartServers { source: runtime::Error },

    #[snafu(display("Failed to load dataset: {source}"))]
    UnableToLoadDataset { source: runtime::Error },

    #[snafu(display(
        "A required parameter ({parameter}) is missing for data connector: {data_connector}",
    ))]
    RequiredParameterMissing {
        parameter: &'static str,
        data_connector: String,
    },

    #[snafu(display("Unable to create data backend: {source}"))]
    UnableToCreateBackend { source: runtime::datafusion::Error },

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

    #[snafu(display("Generic Error: {reason}"))]
    GenericError { reason: String },

    #[snafu(display("Failed to apply the runtime overrides from `--set-runtime`.\n{reason}"))]
    FailedToApplyOverridesGeneric { reason: String },

    #[snafu(display(
        "Failed to apply the runtime override from `--set-runtime {path}={value}`.\n{reason}"
    ))]
    FailedToApplyOverride {
        path: String,
        value: String,
        reason: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Debug)]
#[clap(about = "Spice.ai OSS Runtime")]
#[clap(rename_all = "kebab-case")]
#[allow(clippy::struct_excessive_bools)]
pub struct Args {
    /// Enable Prometheus metrics. (disabled by default)
    #[arg(long, value_name = "BIND_ADDRESS", help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

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

    /// Enable/disable anonymous telemetry collection.
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

    /// Overrides for the runtime configuration (--set-runtime key1.subkey=value1)
    #[arg(long, action = ArgAction::Append, value_parser = parse_set_string)]
    pub set_runtime: Vec<(String, String)>,
}

// Split the run function into smaller parts to address the "too many lines" warning
pub async fn run(args: Args, shutdown_rx: broadcast::Receiver<()>) -> Result<()> {
    let prometheus_registry = args.metrics.map(|_| prometheus::Registry::new());
    let current_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("."));

    // Initialize app and extension factories
    let (app, extension_factories, configs) = setup_app_and_extensions(&args, &current_dir)?;

    // Initialize runtime with configuration
    let rt = setup_runtime(
        &args,
        app.clone(),
        extension_factories,
        prometheus_registry.clone(),
        &current_dir,
    )
    .await;

    // Setup tracing and metrics
    setup_tracing_and_metrics(
        &args,
        app.as_deref(),
        configs.tracing_config.as_ref(),
        &rt,
        prometheus_registry,
    )
    .await?;

    // Setup TLS and telemetry
    let tls_config =
        tls::load_tls_config(&args, configs.spicepod_tls_config.as_ref(), rt.secrets())
            .await
            .context(UnableToInitializeTlsSnafu)?;

    start_anonymous_telemetry(
        &args,
        configs.telemetry_config.as_ref(),
        configs.app_name.as_ref(),
    )
    .await;

    // Launch server components
    let result = launch_server_components(
        &args,
        &rt,
        app.as_deref(),
        shutdown_rx,
        tls_config.as_ref().map(Arc::clone),
    )
    .await;

    // Since the close method consumes self and we're dealing with an Arc,
    // just log the cleanup message and return the result.
    // The runtime will be dropped automatically when it goes out of scope.
    tracing::debug!("Closing runtime and cleaning up resources");

    result
}

/// Type alias for the App setup result to simplify function signature
type AppSetupResult = (Option<Arc<App>>, Vec<Box<dyn ExtensionFactory>>, AppConfigs);

/// Initialize app from filesystem and setup extension factories
fn setup_app_and_extensions(args: &Args, current_dir: &Path) -> Result<AppSetupResult> {
    // Build app from filesystem
    let app: Option<Arc<App>> =
        match AppBuilder::build_from_filesystem_path(current_dir.to_path_buf()) {
            Ok(mut app) => {
                app.runtime = apply_overrides(app.runtime, &args.set_runtime)?;
                Some(Arc::new(app))
            }
            Err(e) => {
                in_tracing_context(|| {
                    tracing::warn!("{e}");
                });
                None
            }
        };

    // Set up extension factories
    let mut extension_factories: Vec<Box<dyn ExtensionFactory>> = vec![];
    if let Some(app) = &app {
        if let Some(manifest) = app.extensions.get("spice_cloud") {
            extension_factories.push(Box::new(SpiceExtensionFactory::new(manifest.clone())));
        }
        #[cfg(feature = "tpc-extension")]
        if let Some(manifest) = app.extensions.get("tpc") {
            extension_factories.push(Box::new(TpcExtensionFactory::new(manifest.clone())));
        }
    }

    // Extract various configs from app
    let runtime_config = app.as_ref().map(|app| &app.runtime);
    let app_name = app.as_ref().map(|app| app.name.clone());
    let spicepod_tls_config = runtime_config.and_then(|rt| rt.tls.clone());
    let tracing_config = runtime_config.and_then(|rt| rt.tracing.clone());
    let telemetry_config = runtime_config.map(|rt| rt.telemetry.clone());

    let configs = AppConfigs {
        app_name,
        spicepod_tls_config,
        tracing_config,
        telemetry_config,
    };

    Ok((app, extension_factories, configs))
}

/// Configuration extracted from the app
struct AppConfigs {
    app_name: Option<String>,
    spicepod_tls_config: Option<app::spicepod::component::runtime::TlsConfig>,
    tracing_config: Option<app::spicepod::component::runtime::TracingConfig>,
    telemetry_config: Option<TelemetryConfig>,
}

/// Initialize runtime with the given configuration
async fn setup_runtime(
    args: &Args,
    app: Option<Arc<App>>,
    extension_factories: Vec<Box<dyn ExtensionFactory>>,
    prometheus_registry: Option<prometheus::Registry>,
    current_dir: &Path,
) -> Arc<Runtime> {
    let mut builder = Runtime::builder()
        .with_app_opt(app)
        // User configured extensions
        .with_extensions(extension_factories)
        // Extensions that will be auto-loaded if not explicitly loaded and requested by a component
        .with_autoload_extensions(HashMap::from([(
            "spice_cloud".to_string(),
            Box::new(SpiceExtensionFactory::default()) as Box<dyn ExtensionFactory>,
        )]))
        .with_datasets_health_monitor()
        .with_metrics_server_opt(args.metrics, prometheus_registry);

    if args.pods_watcher_enabled {
        let pods_watcher = PodsWatcher::new(current_dir.to_path_buf());
        builder = builder.with_pods_watcher(pods_watcher);
    }

    builder.build().await.into()
}

/// Initialize tracing and metrics
async fn setup_tracing_and_metrics(
    args: &Args,
    app: Option<&App>,
    tracing_config: Option<&app::spicepod::component::runtime::TracingConfig>,
    rt: &Arc<Runtime>,
    prometheus_registry: Option<prometheus::Registry>,
) -> Result<()> {
    // Initialize tracing
    spiced_tracing::init_tracing(
        app.map(|app| Arc::new(app.clone())).as_ref(),
        tracing_config,
        rt.datafusion(),
        LogVerbosity::from_flags_and_env(
            args.verbose == 1,                      // -v or --verbose
            args.verbose >= 2 || args.very_verbose, // -vv or --very-verbose
            "SPICED_LOG",
        ),
    )
    .await
    .context(UnableToInitializeTracingSnafu)?;

    // Initialize metrics if enabled
    if let Some(metrics_registry) = prometheus_registry {
        init_metrics(rt.datafusion(), metrics_registry).context(UnableToInitializeMetricsSnafu)?;
    }

    Ok(())
}

/// Launch server and components, handle shutdown
async fn launch_server_components(
    args: &Args,
    rt: &Arc<Runtime>,
    app: Option<&App>,
    shutdown_rx: broadcast::Receiver<()>,
    tls_config: Option<Arc<runtime::tls::TlsConfig>>,
) -> Result<()> {
    let cloned_rt = rt.clone();

    // Set up endpoint auth
    let endpoint_auth = match app {
        Some(app) => EndpointAuth::new(rt.secrets(), app).await,
        None => EndpointAuth::no_auth(),
    };

    // Share the shutdown receiver with all components
    let components_shutdown_rx = shutdown_rx.resubscribe();

    // Register a panic hook for cleanup
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        tracing::error!("Process panicked during cleanup: {:?}", panic_info);
        default_panic(panic_info);
    }));

    // Clone runtime config to avoid reference issues
    let runtime_config = args.runtime.clone();

    // Start servers in a background task
    let server_thread = tokio::spawn(async move {
        Box::pin(
            cloned_rt
                .clone()
                .start_servers(runtime_config, tls_config, endpoint_auth),
        )
        .await
    });

    // Wait for component loading or shutdown signal
    await_components_or_shutdown(rt, components_shutdown_rx).await;

    // Wait for server thread with timeout
    handle_server_thread_completion(server_thread).await
}

/// Wait for component loading or shutdown signal
async fn await_components_or_shutdown(
    rt: &Arc<Runtime>,
    mut components_shutdown_rx: broadcast::Receiver<()>,
) {
    tokio::select! {
        completed = rt.load_components() => {
            tracing::debug!("Components loading completed: {:?}", completed);
        },
        // Internal shutdown signal from runtime
        () = runtime::shutdown_signal() => {
            tracing::debug!("Runtime shutdown signal received, cancelling initialization!");
        },
        // Listen for the external shutdown signal (Ctrl+C)
        result = components_shutdown_rx.recv() => {
            match result {
                Ok(()) => {
                    tracing::debug!("External shutdown signal received, cancelling component initialization");
                    tokio::task::yield_now().await; // Yield to allow other tasks to process the shutdown
                },
                Err(e) => {
                    tracing::debug!("Error receiving shutdown signal: {}", e);
                }
            }
        }
    }
}

/// Handle server thread completion with timeout
async fn handle_server_thread_completion(
    server_thread: tokio::task::JoinHandle<Result<(), runtime::Error>>,
) -> Result<()> {
    if let Ok(thread_result) =
        tokio::time::timeout(tokio::time::Duration::from_secs(5), server_thread).await
    {
        match thread_result {
            Ok(ok) => ok.context(UnableToStartServersSnafu),
            Err(_) => Err(Error::GenericError {
                reason: "Unable to start spiced".into(),
            }),
        }
    } else {
        // Timeout occurred, the server thread didn't complete in time
        tracing::debug!("Server thread did not complete in time, proceeding with cleanup");
        Err(Error::GenericError {
            reason: "Server shutdown timeout".into(),
        })
    }
}

fn init_metrics(
    df: Arc<DataFusion>,
    registry: prometheus::Registry,
) -> Result<(), Box<dyn std::error::Error>> {
    let resource = Resource::default();

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry)
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()?;

    let spice_metrics_exporter =
        OtelArrowExporter::new(spice_metrics::SpiceMetricsExporter::new(df));

    let periodic_reader = PeriodicReader::builder(spice_metrics_exporter, Tokio)
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(10))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(prometheus_exporter)
        .with_reader(periodic_reader)
        .build();
    global::set_meter_provider(provider);

    Ok(())
}

async fn start_anonymous_telemetry(
    args: &Args,
    spicepod_telemetry_config: Option<&TelemetryConfig>,
    spicepod_name: Option<&String>,
) {
    let explicitly_disabled = args.telemetry_enabled == Some(false)
        || spicepod_telemetry_config.is_some_and(|c| !c.enabled);

    let telemetry_properties = match spicepod_telemetry_config {
        Some(config) => config
            .properties
            .clone()
            .into_iter()
            .map(|(k, v)| KeyValue::new(k, v))
            .collect(),
        None => Vec::new(),
    };

    if !explicitly_disabled {
        #[cfg(feature = "anonymous_telemetry")]
        telemetry::anonymous::start(
            spicepod_name.map_or_else(|| "unknown", String::as_str),
            telemetry_properties,
        )
        .await;
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

    let mut yaml = match serde_yaml::to_value(runtime_config) {
        Ok(yaml) => yaml,
        Err(e) => {
            return FailedToApplyOverridesGenericSnafu {
                reason: format!("Runtime configuration is invalid YAML.\n{e}"),
            }
            .fail();
        }
    };

    for (path, value) in overrides {
        let yaml_value =
            serde_yaml::from_str(value).unwrap_or_else(|_| Value::String(value.to_string()));
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
        };
    }

    match serde_yaml::from_value(yaml) {
        Ok(runtime) => Ok(runtime),
        Err(e) => {
            FailedToApplyOverridesGenericSnafu {
                reason: format!(
                    "The runtime configuration after applying the overrides from `--set-runtime` is invalid.\n{e}"
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
                    let mut new_map = serde_yaml::Mapping::new();
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
                if !map.contains_key(Value::String(part.to_string())) {
                    map.insert(
                        Value::String(part.to_string()),
                        Value::Mapping(serde_yaml::Mapping::new()),
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

pub fn in_tracing_context<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_ansi(true)
        .finish();
    subscriber::with_default(subscriber, f)
}
