#![allow(clippy::missing_errors_doc)]

use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::dataconnector::DataConnector;
use runtime::model::Model;

use runtime::podswatcher::PodsWatcher;
use runtime::{dataconnector, Runtime};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime servers"))]
    UnableToStartServers { source: runtime::Error },

    #[snafu(display("Unable to attach data connector: {data_connector}"))]
    UnableToAttachDataConnector {
        source: runtime::datafusion::Error,
        data_connector: String,
    },

    #[snafu(display("Unable to attach view"))]
    UnableToAttachView { source: runtime::datafusion::Error },

    #[snafu(display("Unable to initialize data source: {data_connector}"))]
    UnableToInitializeDataConnector {
        source: runtime::dataconnector::Error,
        data_connector: String,
    },

    #[snafu(display(
        "A required parameter ({parameter}) is missing for data source: {data_connector}",
    ))]
    RequiredParameterMissing {
        parameter: &'static str,
        data_connector: String,
    },

    #[snafu(display("Unknown data source: {data_connector}"))]
    UnknownDataConnector { data_connector: String },

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
}

pub async fn run(args: Args) -> Result<()> {
    let current_dir = env::current_dir().unwrap_or(PathBuf::from("."));

    let app = App::new(current_dir.clone()).context(UnableToConstructSpiceAppSnafu)?;

    let mut auth = runtime::auth::AuthProviders::default();
    match auth.parse_from_config() {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!(
                "Unable to parse auth from config, proceeding without auth: {}",
                e
            );
        }
    }

    let mut df = runtime::datafusion::DataFusion::new();

    for ds in &app.datasets {
        if ds.acceleration.is_none() && ds.sql.is_none() {
            tracing::warn!("No acceleration specified for dataset: {}", ds.name);
            continue;
        };

        let source = ds.source();
        let source = source.as_str();
        let params = Arc::new(ds.params.clone());
        let data_connector: Option<Box<dyn DataConnector>> = match source {
            "spice.ai" => Some(Box::new(
                dataconnector::spiceai::SpiceAI::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            )),
            "dremio" => Some(Box::new(
                dataconnector::dremio::Dremio::new(auth.get(source), params)
                    .await
                    .context(UnableToInitializeDataConnectorSnafu {
                        data_connector: source,
                    })?,
            )),
            "localhost" | "" => None,
            "debug" => Some(Box::new(dataconnector::debug::DebugSource {})),
            _ => UnknownDataConnectorSnafu {
                data_connector: source,
            }
            .fail()?,
        };

        match data_connector {
            Some(data_connector) => {
                let data_connector = Box::leak(data_connector);
                let data_backend = df.new_backend(ds).context(UnableToCreateBackendSnafu)?;

                df.attach(ds, data_connector, data_backend).context(
                    UnableToAttachDataConnectorSnafu {
                        data_connector: source,
                    },
                )?;
            }
            None => match &ds.sql {
                Some(_) => {
                    df.attach_view(ds).context(UnableToAttachViewSnafu)?;
                }
                None => {
                    let data_backend = df.new_backend(ds).context(UnableToCreateBackendSnafu)?;
                    df.attach_backend(&ds.name, data_backend).context(
                        UnableToAttachDataConnectorSnafu {
                            data_connector: source,
                        },
                    )?;
                }
            },
        }

        tracing::info!("Loaded dataset: {}", ds.name);
    }

    let mut model_map = HashMap::with_capacity(app.models.len());
    for m in &app.models {
        match Model::load(m, auth.get(m.source().as_str())) {
            Ok(in_m) => {
                model_map.insert(m.name.clone(), in_m);
                tracing::info!("Loaded model: {}", m.name);
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

    let pods_watcher = PodsWatcher::new(current_dir.clone());

    let mut rt: Runtime = Runtime::new(args.runtime, app, df, model_map, pods_watcher);

    rt.start_pods_watcher()
        .context(UnableToInitializePodsWatcherSnafu)?;

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
