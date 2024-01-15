#![allow(clippy::missing_errors_doc)]

use app::App;
use clap::Parser;
use runtime::config::Config;
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

pub async fn run() -> Result<()> {
    let app = App::new(".").context(UnableToConstructSpiceAppSnafu)?;

    let cfg = Config::parse();

    let rt: Runtime = Runtime::new(cfg, app);

    rt.start_server()
        .await
        .context(UnableToStartHttpServerSnafu)?;

    Ok(())
}
