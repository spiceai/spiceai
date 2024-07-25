/*
Copyright 2024 The Spice.ai OSS Authors

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
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use app::{App, AppBuilder};
use clap::Parser;
use flightrepl::ReplConfig;
use metrics_exporter_prometheus::PrometheusHandle;
use runtime::config::Config as RuntimeConfig;

use runtime::podswatcher::PodsWatcher;
use runtime::{extension::ExtensionFactory, Runtime};
use rustls::pki_types::CertificateDer;
use rustls::pki_types::PrivateKeyDer;
use rustls::ServerConfig;
use rustls_pemfile::certs;
use rustls_pemfile::private_key;
use snafu::prelude::*;
use spice_cloud::SpiceExtensionFactory;

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

    #[snafu(display("Generic Error: {reason}"))]
    GenericError { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[clap(about = "Spice.ai OSS Runtime")]
#[clap(rename_all = "kebab-case")]
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

    /// Enable TLS for the HTTP server.
    #[arg(long)]
    pub tls: bool,

    /// Path to the TLS PEM-encoded certificate file.
    #[arg(long, value_name = "cert.pem")]
    pub tls_certificate: Option<String>,

    /// Path to the TLS PEM-encoded private key file.
    #[arg(long, value_name = "key.pem")]
    pub tls_key: Option<String>,
}

pub async fn run(args: Args, metrics_handle: Option<PrometheusHandle>) -> Result<()> {
    let current_dir = env::current_dir().unwrap_or(PathBuf::from("."));
    let pods_watcher = PodsWatcher::new(current_dir.clone());
    let app: Option<App> = match AppBuilder::build_from_filesystem_path(current_dir.clone())
        .context(UnableToConstructSpiceAppSnafu)
    {
        Ok(app) => Some(app),
        Err(e) => {
            tracing::warn!("{}", e);
            None
        }
    };

    let mut extension_factories: Vec<Box<dyn ExtensionFactory>> = vec![];

    if let Some(app) = &app {
        if let Some(manifest) = app.extensions.get("spice_cloud") {
            let spice_extension_factory = SpiceExtensionFactory::new(manifest.clone());
            extension_factories.push(Box::new(spice_extension_factory));
        }
    }

    let tls_config = load_tls_config(&args).context(UnableToInitializeTlsSnafu)?;

    let rt: Runtime = Runtime::builder()
        .with_app_opt(app)
        // User configured extensions
        .with_extensions(extension_factories)
        // Extensions that will be auto-loaded if not explicitly loaded and requested by a component
        .with_autoload_extensions(HashMap::from([(
            "spice_cloud".to_string(),
            Box::new(SpiceExtensionFactory::default()) as Box<dyn ExtensionFactory>,
        )]))
        .with_pods_watcher(pods_watcher)
        .with_datasets_health_monitor()
        .with_metrics_server_opt(args.metrics, metrics_handle)
        .with_tls_config_opt(tls_config)
        .build()
        .await;

    let cloned_rt = rt.clone();
    let server_thread = tokio::spawn(async move { cloned_rt.start_servers(args.runtime).await });

    tokio::select! {
        () = rt.load_components() => {},
        () = runtime::shutdown_signal() => {
            tracing::debug!("Cancelling runtime initializing!");
        },
    }

    match server_thread.await {
        Ok(ok) => ok.context(UnableToStartServersSnafu),
        Err(_) => Err(Error::GenericError {
            reason: "Unable to start spiced".into(),
        }),
    }
}

fn load_tls_config(
    args: &Args,
) -> std::result::Result<Option<Arc<ServerConfig>>, Box<dyn std::error::Error>> {
    if !args.tls {
        return Ok(None);
    }

    let Some(cert_path) = &args.tls_certificate else {
        return Err("TLS certificate is required (--tls-certificate)".into());
    };
    let Some(key_path) = &args.tls_key else {
        return Err("TLS key is required (--tls-key)".into());
    };

    let certs = load_certs(Path::new(cert_path))?;
    let key = load_key(Path::new(key_path))?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;

    Ok(Some(Arc::new(config)))
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    certs(&mut BufReader::new(File::open(path)?)).collect()
}

fn load_key(
    path: &Path,
) -> std::result::Result<PrivateKeyDer<'static>, Box<dyn std::error::Error>> {
    private_key(&mut BufReader::new(File::open(path)?))?
        .ok_or_else(|| format!("No private key found in {}", path.display()).into())
}
