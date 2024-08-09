/*
Copyright 2024 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use clap::Parser;
use opentelemetry::global;
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::Tracer, Resource};
use rustls::crypto::{self, CryptoProvider};
use tokio::runtime::Runtime;
use tracing_subscriber::{filter, fmt, prelude::*, EnvFilter, Registry};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn main() {
    let args = spiced::Args::parse();

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
            eprintln!("Unable to start Tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    if args.repl {
        if let Err(e) = tokio_runtime.block_on(flightrepl::run(args.repl_config)) {
            eprintln!("SQL REPL Error: {e}");
        };
        return;
    }

    // Install the default AWS LC RS crypto provider for rusttls
    let _ = CryptoProvider::install_default(crypto::aws_lc_rs::default_provider());

    if let Err(err) = tokio_runtime.block_on(start_runtime(args)) {
        eprintln!("Spice Runtime error: {err}");
    }

    global::shutdown_tracer_provider();
}

async fn start_runtime(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    let prometheus_registry = match args.metrics {
        Some(_) => Some(init_metrics()?),
        None => None,
    };

    if let Err(err) = init_tracing() {
        eprintln!("Unable to initialize tracing: {err}");
        std::process::exit(1);
    }

    spiced::run(args, prometheus_registry).await?;
    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = if let Ok(env_log) = std::env::var("SPICED_LOG") {
        EnvFilter::new(env_log)
    } else {
        EnvFilter::new("task_history=INFO,spiced=INFO,runtime=INFO,secrets=INFO,data_components=INFO,cache=INFO,extensions=INFO,spice_cloud=INFO")
    };

    let registry = Registry::default();

    let otel_tracer = init_otel_tracing()?;

    let subscriber = registry
        .with(filter)
        .with(
            tracing_opentelemetry::layer()
                .with_tracer(otel_tracer)
                .with_filter(filter::filter_fn(|metadata| {
                    metadata.target() == "task_history"
                })),
        )
        .with(fmt::layer().with_ansi(true));

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

fn init_otel_tracing() -> Result<Tracer, Box<dyn std::error::Error>> {
    let tracer = opentelemetry_zipkin::new_pipeline()
        .with_http_client(reqwest::Client::new())
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    Ok(tracer)
}

fn init_metrics() -> Result<prometheus::Registry, Box<dyn std::error::Error>> {
    let registry = prometheus::Registry::new();

    let resource = Resource::default();

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()?;

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(prometheus_exporter)
        .build();
    global::set_meter_provider(provider);

    Ok(registry)
}
