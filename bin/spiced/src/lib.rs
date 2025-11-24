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

use app::spicepod::component::runtime::{Runtime as SpicepodRuntime, TelemetryConfig};
use app::{App, AppBuilder};
use clap::{ArgAction, Parser};
use flightrepl::ReplConfig;
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::runtime::Tokio;
use otel_arrow::OtelArrowExporter;
#[cfg(feature = "cluster")]
use runtime::config::ClusterMode;
use runtime::config::Config as RuntimeConfig;
use runtime::datafusion::DataFusion;
use runtime::podswatcher::PodsWatcher;
use runtime::spice_metrics;
use runtime::{Runtime, auth::EndpointAuth, extension::ExtensionFactory};
use runtime_async::ManagedTokioRuntime;
use serde_yaml::Value;
use snafu::prelude::*;
use spice_cloud::SpiceExtensionFactory;
use spiced_tracing::LogVerbosity;
use tokio::runtime::Handle;
#[cfg(feature = "tpc-extension")]
use tpc_extension::TpcExtensionFactory;

#[path = "tracing.rs"]
mod spiced_tracing;
mod tls;

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

    #[snafu(display("Unable to create data backend: {source}"))]
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

    #[snafu(display("Unable to initialize the DataFusion Tokio runtime: {source}"))]
    UnableToInitializeDatafusionTokioRuntime {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Generic Error: {reason}"))]
    GenericError { reason: String },

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

    /// Path to the Spicepod directory or file. Supports local paths and remote URLs (i.e. `s3://my_bucket/spicepod.yaml`)
    ///
    /// When specified, the behavior to automatically reload changes to the Spicepod is disabled.
    #[arg(value_name = "PATH")]
    pub spicepod: Option<PathBuf>,

    /// Overrides for the runtime configuration (--set-runtime key1.subkey=value1)
    #[arg(long, action = ArgAction::Append, value_parser = parse_set_string)]
    pub set_runtime: Vec<(String, String)>,
}

#[allow(clippy::too_many_lines)]
pub async fn run(args: Args) -> Result<()> {
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
    let telemetry_config = runtime_config.map(|rt| rt.telemetry.clone());

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
        .with_io_runtime(Handle::current());

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
            let tokio_runtime = ManagedTokioRuntime::try_new()
                .boxed()
                .context(UnableToInitializeDatafusionTokioRuntimeSnafu)?;

            rt.datafusion().set_cpu_runtime(tokio_runtime);
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

    if let Some(ref metrics_registry) = prometheus_registry {
        init_metrics(&rt.datafusion(), metrics_registry.clone())
            .context(UnableToInitializeMetricsSnafu)?;
    }

    let tls_config = tls::load_tls_config(&args, spicepod_tls_config.as_ref(), rt.secrets())
        .await
        .context(UnableToInitializeTlsSnafu)?;

    start_anonymous_telemetry(&args, telemetry_config.as_ref(), app_name.as_ref()).await;

    let rt = Arc::new(rt);

    if prometheus_registry.is_some() {
        rt.init_cache_metrics();
    }

    let cloned_rt = Arc::clone(&rt);
    let endpoint_auth = match app.as_ref() {
        Some(app) => EndpointAuth::new(rt.secrets(), app).await,
        None => EndpointAuth::no_auth(),
    };

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
    #[cfg(feature = "cluster")]
    if matches!(args.runtime.cluster.mode, Some(ClusterMode::Executor)) {
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

fn init_metrics(
    df: &Arc<DataFusion>,
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
                reason: format!("Runtime configuration is invalid YAML. {e}"),
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
        }
    }

    match serde_yaml::from_value(yaml) {
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
