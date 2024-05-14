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
use std::sync::Arc;

use app::App;
use clap::Parser;
use flightrepl::ReplConfig;
use runtime::config::Config as RuntimeConfig;

use runtime::podswatcher::PodsWatcher;
use runtime::Runtime;
use snafu::prelude::*;
use tokio::sync::RwLock;

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

    /// Enable Spice Cloud connection.
    #[arg(
        long,
        help_heading = "Enable metrics replication to Spice Cloud. Requires Spice Cloud API key stored in secrets."
    )]
    pub spice_cloud_connect: bool,
}

pub async fn run(args: Args) -> Result<()> {
    let current_dir = env::current_dir().unwrap_or(PathBuf::from("."));
    let df = Arc::new(RwLock::new(runtime::datafusion::DataFusion::new()));
    let pods_watcher = PodsWatcher::new(current_dir.clone());
    let app: Arc<RwLock<Option<App>>> =
        match App::new(current_dir.clone()).context(UnableToConstructSpiceAppSnafu) {
            Ok(app) => Arc::new(RwLock::new(Some(app))),
            Err(e) => {
                tracing::warn!("{}", e);
                Arc::new(RwLock::new(None))
            }
        };

    let mut rt: Runtime = Runtime::new(args.runtime, app, df, pods_watcher).await;

    if args.spice_cloud_connect {
        rt.start_metrics(args.metrics).await;
    }

    rt.load_secrets().await;

    rt.load_datasets().await;

    rt.load_models().await;

    rt.start_servers(args.metrics)
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
