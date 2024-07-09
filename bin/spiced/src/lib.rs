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

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use app::{App, AppBuilder};
use clap::Parser;
use flightrepl::ReplConfig;
use runtime::config::Config as RuntimeConfig;

use runtime::podswatcher::PodsWatcher;
use runtime::{extension::ExtensionFactory, Runtime};
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

    #[snafu(display("Generic Error: {reason}"))]
    GenericError { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[clap(about = "Spice.ai OSS Runtime")]
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
}

pub async fn run(args: Args) -> Result<()> {
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

    if cfg!(feature = "spice-cloud") {
        if let Some(app) = &app {
            if let Some(manifest) = app.extensions.get("spice_cloud") {
                let spice_extension_factory = SpiceExtensionFactory::new(manifest.clone());
                extension_factories.push(Box::new(spice_extension_factory));
            }
        }
    }

    let rt: Runtime = Runtime::builder()
        .with_app_opt(app)
        .with_extensions(extension_factories)
        .with_pods_watcher(pods_watcher)
        .with_datasets_health_monitor()
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
