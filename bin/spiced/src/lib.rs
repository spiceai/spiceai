#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use app::App;
use clap::Parser;
use flightrepl::ReplConfig;
use runtime::config::Config as RuntimeConfig;
use runtime::model::Model;

use runtime::podswatcher::PodsWatcher;
use runtime::Runtime;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime servers"))]
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

    #[snafu(display("Unable to create data backend"))]
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
}

pub async fn run(args: Args) -> Result<()> {
    let current_dir = env::current_dir().unwrap_or(PathBuf::from("."));

    let app = App::new(current_dir.clone()).context(UnableToConstructSpiceAppSnafu)?;

    let auth = load_auth_providers();

    let mut df = runtime::datafusion::DataFusion::new();

    for ds in &app.datasets {
        Runtime::load_dataset(ds, &mut df, &auth)
            .await
            .context(UnableToLoadDatasetSnafu)?;
    }

    let model_map = load_models(&app, &auth);

    let pods_watcher = PodsWatcher::new(current_dir.clone());

    let mut rt: Runtime = Runtime::new(args.runtime, app, df, model_map, pods_watcher);

    rt.start_pods_watcher()
        .await
        .context(UnableToInitializePodsWatcherSnafu)?;

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}

fn load_auth_providers() -> runtime::auth::AuthProviders {
    let mut auth = runtime::auth::AuthProviders::default();
    if let Err(e) = auth.parse_from_config() {
        tracing::warn!(
            "Unable to parse auth from config, proceeding without auth: {}",
            e
        );
    }
    auth
}

fn load_models(app: &App, auth: &runtime::auth::AuthProviders) -> HashMap<String, Model> {
    let mut model_map = HashMap::with_capacity(app.models.len());
    for m in &app.models {
        tracing::info!("Deploying model [{}] from {}...", m.name, m.from);
        match Model::load(m, auth.get(m.source().as_str())) {
            Ok(in_m) => {
                model_map.insert(m.name.clone(), in_m);
                tracing::info!("Model [{}] deployed, ready for inferencing", m.name);
            }
            Err(e) => {
                tracing::warn!(
                    "Unable to load runnable model from spicepod {}, error: {}",
                    m.name,
                    e,
                );
            }
        }
    }

    model_map
}
