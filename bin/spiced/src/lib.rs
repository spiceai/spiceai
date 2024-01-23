#![allow(clippy::missing_errors_doc)]

use std::net::SocketAddr;
use std::time::Duration;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::{databackend, datasource, Runtime};
use snafu::prelude::*;
// use spice_rs::Client;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp {
        source: app::Error,
    },

    #[snafu(display("Unable to start Spice Runtime servers"))]
    UnableToStartServers {
        source: runtime::Error,
    },

    UnableToAttachDataSource {
        source: runtime::datafusion::Error,
    },
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

    let mut df = runtime::datafusion::DataFusion::new();

    // for ds in rt.app.datasets.iter() {
    //     let data_source = datasource::DataSource::new(ds.clone());
    //     rt.df.attach(
    //         &ds.name,
    //         data_source,
    //         databackend::DataBackendType::default(),
    //     )
    // }

    df.attach(
        "test-stream",
        datasource::debug::DebugSource {
            sleep_duration: Duration::from_secs(1),
        },
        databackend::DataBackendType::default(),
    )
    .context(UnableToAttachDataSourceSnafu)?;

    let rt: Runtime = Runtime::new(args.runtime, app, df);

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
