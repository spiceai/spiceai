#![allow(clippy::missing_errors_doc)]

use std::net::SocketAddr;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::datasource::DataSource;
use runtime::{databackend, datasource, Runtime};
use snafu::prelude::*;

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

    let mut auth = runtime::auth::AuthProviders::default();
    match auth.parse_from_config() {
        Ok(()) => {}
        Err(e) => {
            tracing::error!(
                "Unable to parse auth from config, proceeding without auth: {}",
                e
            );
        }
    }

    let mut df = runtime::datafusion::DataFusion::new();

    df.attach_backend("test", databackend::DataBackendType::Memtable)
        .context(UnableToAttachDataSourceSnafu)?;

    for ds in &app.datasets {
        // TODO: Handle multiple data sources
        let spice_auth = auth.get("spice.ai");
        let data_source = Box::leak(Box::new(datasource::spiceai::SpiceAI::new(spice_auth)));
        df.attach(
            &ds.name,
            data_source,
            databackend::DataBackendType::default(),
        )
        .context(UnableToAttachDataSourceSnafu)?;
    }

    // let debug_source = datasource::debug::DebugSource {
    //     sleep_duration: Duration::from_secs(1),
    // };
    // // Ok to leak here since we want it to live for the lifetime of the process anyway
    // let debug_source = Box::leak(Box::new(debug_source));

    // df.attach(
    //     "test-stream",
    //     debug_source,
    //     databackend::DataBackendType::default(),
    // )
    // .context(UnableToAttachDataSourceSnafu)?;

    let rt: Runtime = Runtime::new(args.runtime, app, df);

    rt.start_servers()
        .await
        .context(UnableToStartServersSnafu)?;

    Ok(())
}
