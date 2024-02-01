use std::net::SocketAddr;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::runtime::Runtime;
use tracing::level_filters::LevelFilter;

fn main() {
    let args = spiced::Args::parse();

    if let Err(err) = init_tracing() {
        eprintln!("Unable to initialize tracing: {err:?}");
        std::process::exit(1);
    }

    if args.version {
        let version = if cfg!(feature = "release") {
            env!("CARGO_PKG_VERSION")
        } else {
            "local"
        };

        println!("{version}");
        return;
    }

    let tokio_runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::error!("Unable to start Tokio runtime: {err:?}");
            std::process::exit(1);
        }
    };

    tracing::trace!("Starting Spice Runtime!");

    if let Err(err) = tokio_runtime.block_on(start_runtime(args)) {
        tracing::error!("Spice Runtime error: {err:?}");
    }
}

async fn start_runtime(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(metrics_socket) = args.metrics {
        init_metrics(metrics_socket)?;
    }

    spiced::run(args).await?;
    Ok(())
}

fn init_tracing() -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(LevelFilter::TRACE.into())
        .with_env_var("SPICED_LOG")
        .from_env_lossy();

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

fn init_metrics(socket_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let builder = PrometheusBuilder::new().with_http_listener(socket_addr);

    // This needs to run inside a Tokio runtime.
    builder.install()?;
    tracing::trace!("Prometheus metrics server started on {socket_addr:?}");

    Ok(())
}
