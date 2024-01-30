#![allow(clippy::missing_errors_doc)]

use std::net::SocketAddr;
use std::time::Duration;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::datasource::DataSource;
use runtime::{databackend, datasource, Runtime};
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime servers"))]
    UnableToStartServers { source: runtime::Error },

    #[snafu(display("Unable to attach data source: {data_source}"))]
    UnableToAttachDataSource {
        source: runtime::datafusion::Error,
        data_source: String,
    },

    #[snafu(display("Unable to initialize data source: {data_source}"))]
    UnableToInitializeDataSource {
        source: runtime::datasource::Error,
        data_source: String,
    },

    #[snafu(display("Unknown data source: {data_source}"))]
    UnknownDataSource { data_source: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser)]
#[clap(about = "Spice.ai OSS Runtime")]
pub struct Args {
    /// Enable Prometheus metrics. (disabled by default)
    #[arg(long, value_name = "BIND_ADDRESS", help_heading = "Metrics")]
    pub metrics: Option<SocketAddr>,

    /// All runtime related arguments
    #[clap(flatten)]
    pub runtime: RuntimeConfig,
}

pub async fn run(args: Args) -> Result<()> {
    let app = App::new(".").context(UnableToConstructSpiceAppSnafu)?;

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
        match &ds.source {
            Some(source) => {
                let auth_name = match &ds.auth {
                    Some(auth_name) => auth_name,
                    None => source,
                };
                let data_source: Box<dyn DataSource> = match source.as_str() {
                    "spice.ai" => {
                        let spice_auth = auth.get(auth_name);
                        Box::new(
                            datasource::flight::Flight::new(
                                spice_auth,
                                "https://flight.spiceai.io".to_string(),
                            )
                            .await
                            .context(
                                UnableToInitializeDataSourceSnafu {
                                    data_source: source.clone(),
                                },
                            )?,
                        )
                    }
                    "dremio" => {
                        let dremio_auth = auth.get(auth_name);
                        Box::new(
                            datasource::flight::Flight::new(
                                dremio_auth,
                                "http://dremio-4mimamg7rdeve.eastus.cloudapp.azure.com:32010"
                                    .to_string(),
                            )
                            .await
                            .context(
                                UnableToInitializeDataSourceSnafu {
                                    data_source: source.clone(),
                                },
                            )?,
                        )
                    }
                    "debug" => Box::new(datasource::debug::DebugSource {
                        sleep_duration: Duration::from_secs(1),
                    }),
                    _ => UnknownDataSourceSnafu {
                        data_source: source.clone(),
                    }
                    .fail()?,
                };

                let data_source = Box::leak(data_source);

                df.attach(
                    &ds.name,
                    data_source,
                    databackend::DataBackendType::default(),
                )
                .context(UnableToAttachDataSourceSnafu {
                    data_source: source,
                })?;
            }
            None => {
                df.attach_backend(&ds.name, databackend::DataBackendType::Memtable)
                    .context(UnableToAttachDataSourceSnafu {
                        data_source: "direct",
                    })?;
            }
        }

        tracing::trace!("Loaded dataset: {}", ds.name);
    }

    let rt: Runtime = Runtime::new(args.runtime, app, df);

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
