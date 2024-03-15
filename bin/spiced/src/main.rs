use std::net::SocketAddr;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

fn main() {
    let args = spiced::Args::parse();

    if let Err(err) = init_tracing() {
        eprintln!("Unable to initialize tracing: {err:?}");
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
            tracing::error!("Unable to start Tokio runtime: {err:?}");
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

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = if let Ok(env_log) = std::env::var("SPICED_LOG") {
        EnvFilter::new(env_log)
    } else {
        EnvFilter::new(
            "spiced=INFO,runtime=INFO,flight_datafusion=INFO,sql_provider_datafusion=INFO",
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
    let builder = PrometheusBuilder::new().with_http_listener(socket_addr);

    // This needs to run inside a Tokio runtime.
    builder.install()?;
    tracing::trace!("Prometheus metrics server started on {socket_addr:?}");

    Ok(())
}
