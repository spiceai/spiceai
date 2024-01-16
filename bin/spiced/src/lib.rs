#![allow(clippy::missing_errors_doc)]

use std::net::SocketAddr;

use app::App;
use clap::Parser;
use runtime::config::Config as RuntimeConfig;
use runtime::Runtime;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to construct spice app"))]
    UnableToConstructSpiceApp { source: app::Error },

    #[snafu(display("Unable to start Spice Runtime HTTP server"))]
    UnableToStartHttpServer { source: runtime::Error },
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

    let rt: Runtime = Runtime::new(args.runtime, app);

    rt.start_server()
        .await
        .context(UnableToStartHttpServerSnafu)?;

    Ok(())
}
