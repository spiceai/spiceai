use std::net::SocketAddr;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusRecorder;
use runtime::metrics::{CompositeRecorder, LocalGaugeRecorder};
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

fn main() {
    let args = spiced::Args::parse();

    if let Err(err) = init_tracing() {
        eprintln!("Unable to initialize tracing: {err}");
        std::process::exit(1);
    }

    if args.version {
        if cfg!(feature = "release") {
            println!("v{}", env!("CARGO_PKG_VERSION"));
        } else {
            print!(
                "v{}-rc.{}",
                env!("CARGO_PKG_VERSION"),
                env!("GIT_COMMIT_HASH")
            );

            if cfg!(feature = "dev") {
                print!("-dev");
            }

            println!();
        };

        return;
    }

    let tokio_runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            tracing::error!("Unable to start Tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    if args.repl {
        if let Err(e) = tokio_runtime.block_on(flightrepl::run(args.repl_config)) {
            tracing::error!("Flight REPL error: {e}");
        };
        return;
    }

    tracing::trace!("Starting Spice Runtime!");

    if let Err(err) = tokio_runtime.block_on(start_runtime(args)) {
        tracing::error!("Spice Runtime error: {err}");
    }
}

async fn start_runtime(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(metrics_socket) = args.metrics {
        init_metrics(metrics_socket)?;
    }
    spiced::run(args).await?;
    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = if let Ok(env_log) = std::env::var("SPICED_LOG") {
        EnvFilter::new(env_log)
    } else {
        EnvFilter::new(
            "spiced=INFO,runtime=INFO,flightsql_datafusion=INFO,flight_datafusion=INFO,sql_provider_datafusion=INFO",
        )
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

fn init_metrics(socket_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = CompositeRecorder::new();
    recorder.add_recorder(LocalGaugeRecorder::new());
    recorder.add_recorder(init_prometheus(socket_addr)?);
    metrics::set_global_recorder(recorder)?;
    tracing::info!("Metrics listening on {socket_addr}");

    Ok(())
}

// This needs to run inside a Tokio runtime.
fn init_prometheus(
    socket_addr: SocketAddr,
) -> Result<PrometheusRecorder, Box<dyn std::error::Error>> {
    let (prom_recorder, prom_exporter) = PrometheusBuilder::new()
        .with_http_listener(socket_addr)
        .build()?;

    // Manually run exporter Future in current runtime.
    std::mem::drop(Handle::try_current()?.spawn(prom_exporter));
    Ok(prom_recorder)
}
