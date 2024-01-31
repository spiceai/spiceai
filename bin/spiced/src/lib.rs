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
        let mut from_parts: Vec<&str> = ds.from.split('/').collect();
        let source = from_parts[0];
        from_parts.remove(0);
        let dataset_path = from_parts.join(".");

        let data_source: Box<dyn DataSource> = match source {
            "spice.ai" => {
                let spice_auth = auth.get(source);
                Box::new(
                    datasource::flight::Flight::new(
                        spice_auth,
                        "https://flight.spiceai.io".to_string(),
                    )
                    .await
                    .context(UnableToInitializeDataSourceSnafu {
                        data_source: source,
                    })?,
                )
            }
            "dremio" => {
                let dremio_auth = auth.get(source);
                Box::new(
                    datasource::flight::Flight::new(
                        dremio_auth,
                        "http://dremio-4mimamg7rdeve.eastus.cloudapp.azure.com:32010".to_string(),
                    )
                    .await
                    .context(UnableToInitializeDataSourceSnafu {
                        data_source: source,
                    })?,
                )
            }
            "debug" => Box::new(datasource::debug::DebugSource {
                sleep_duration: Duration::from_secs(1),
            }),
            _ => UnknownDataSourceSnafu {
                data_source: source,
            }
            .fail()?,
        };

        let data_source = Box::leak(data_source);

        let fq_dataset_name = format!("{}.{}", dataset_path, ds.name);
        df.attach(
            fq_dataset_name.as_str(),
            data_source,
            databackend::DataBackendType::default(),
        )
        .context(UnableToAttachDataSourceSnafu {
            data_source: source,
        })?;

        tracing::trace!("Loaded dataset: {}", fq_dataset_name);
    }

    let rt: Runtime = Runtime::new(args.runtime, app, df);

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
