/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

// Larger async fns (start_runtime) overflow the default type-layout query depth
// under the full feature set; raise the recursion limit for layout computation.
#![recursion_limit = "256"]

use std::mem::size_of;

// Spice runtime requires at least 64-bit pointer size (8 bytes).
// This compile-time assertion prevents building on 32-bit platforms.
const _: () = assert!(
    size_of::<usize>() >= 8,
    "Spice runtime requires a 64-bit platform (usize must be at least 8 bytes)"
);

use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches};
use opentelemetry::global;
use rustls::crypto::{self, CryptoProvider};
use telemetry::noop::NoopMeterProvider;
use tokio::runtime::Runtime;
use util::in_tracing_context;

#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "alloc-system")]
#[global_allocator]
static ALLOC: std::alloc::System = std::alloc::System;

// snmalloc is the default allocator if no other allocator is selected
#[cfg(not(any(
    feature = "alloc-jemalloc",
    feature = "alloc-mimalloc",
    feature = "alloc-system"
)))]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// Function to determine the allocator name at compile time
const fn get_allocator_name() -> Option<&'static str> {
    if cfg!(feature = "alloc-jemalloc") {
        Some("jemalloc")
    } else if cfg!(feature = "alloc-mimalloc") {
        Some("mimalloc")
    } else if cfg!(feature = "alloc-system") {
        Some("system")
    } else {
        None
    }
}

/// Whether `id` was given on the command line, rather than falling back to its default.
fn chosen_on_command_line(matches: &clap::ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

fn main() {
    // Before anything else, so a fault during startup is still reported. A native
    // crash is not a panic: without this the process dies silently with exit 139.
    spiced::crash_handler::install();

    let matches = spiced::Args::command().get_matches();
    let open_telemetry_deprecated =
        matches.value_source("open_telemetry_bind_address") == Some(ValueSource::CommandLine);
    // `--repl-flight-endpoint` moves only the REPL's SQL target, leaving the HTTP endpoint that
    // `nql` uses wherever it already was. Choosing one without the other leaves nothing pointing
    // the HTTP endpoint at that runtime, so `nql` says so instead of answering from whatever
    // that endpoint reaches. See #11005.
    let flight_chosen = chosen_on_command_line(&matches, "repl_flight_endpoint");
    let http_chosen = chosen_on_command_line(&matches, "http_endpoint");
    let mut args = spiced::Args::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());
    args.open_telemetry_deprecated = open_telemetry_deprecated;
    args.repl_config.http_endpoint_may_be_another_runtime =
        repl::http_endpoint_unpaired(flight_chosen, http_chosen);

    if args.version {
        println!("{}", get_version_string());
        return;
    }

    // Install the default AWS LC RS crypto provider for rusttls
    let _ = CryptoProvider::install_default(crypto::aws_lc_rs::default_provider());

    if args.repl {
        // The REPL is a Flight client, not the runtime: it sizes nothing, so it
        // keeps Tokio's own default runtime.
        let repl_runtime = match Runtime::new() {
            Ok(runtime) => runtime,
            Err(err) => {
                eprintln!("Unable to start Tokio runtime: {err}");
                std::process::exit(1);
            }
        };
        if let Err(e) = repl_runtime.block_on(repl::run(args.repl_config)) {
            eprintln!("SQL REPL Error: {e}");
        }
        return;
    }

    if let Err(err) = load_and_run(args) {
        in_tracing_context(|| {
            tracing::error!("{err}");
        });
    }

    // There is no global::shutdown_meter_provider, so we replace currently used meter provider with a noop one to clean up resources
    global::set_meter_provider(NoopMeterProvider::new());
    tracing::info!("Goodbye!");
}

/// Load the spicepod, resolve the CPU budget it configures, and only then build
/// the runtime that budget sizes.
///
/// The ordering is the point: `runtime.cpu.cores` lives in the spicepod, and the
/// budget sizes this runtime's own worker pool — so the spicepod has to be
/// parsed before the pool exists. It is loaded here on a throwaway
/// current-thread runtime and handed to `spiced::run`, so it is read exactly
/// once and all three configuration surfaces resolve through one path.
fn load_and_run(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let app_bundle = bootstrap.block_on(spiced::build_app(&args))?;
    drop(bootstrap);

    spiced::install_cpu_budget(&args, app_bundle.0.as_deref())?;

    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cpu_budget::cpu_budget().main_runtime_worker_threads())
        .enable_all()
        .build()?;
    tokio_runtime.block_on(start_runtime(args, app_bundle))
}

async fn start_runtime(
    args: spiced::Args,
    app_bundle: spiced::AppBundle,
) -> Result<(), Box<dyn std::error::Error>> {
    in_tracing_context(|| {
        if let Some(allocator_name) = get_allocator_name() {
            tracing::info!(
                "Starting runtime {version} (allocator: {allocator_name})",
                version = get_version_string(),
            );
        } else {
            tracing::info!("Starting runtime {version}", version = get_version_string());
        }
    });
    spiced::run(args, app_bundle).await?;
    Ok(())
}

fn get_version_string() -> String {
    if cfg!(feature = "release") {
        format!("v{}{}", env!("CARGO_PKG_VERSION"), build_metadata())
    } else {
        let mut version = format!(
            "v{}-build.{}",
            env!("CARGO_PKG_VERSION"),
            env!("GIT_COMMIT_HASH")
        );
        if cfg!(feature = "dev") {
            version.push_str("-dev");
        }
        version.push_str(build_metadata());
        version
    }
}

/// Build metadata conforming to <https://semver.org/#spec-item-10>
///
/// Build metadata is always known at compile time, so return a string literal.
const fn build_metadata() -> &'static str {
    match (
        cfg!(feature = "models"),
        cfg!(feature = "metal"),
        cfg!(feature = "cuda"),
    ) {
        (true, true, true) => "+models.metal.cuda",
        (true, true, false) => "+models.metal",
        (true, false, true) => "+models.cuda",
        (true, false, false) => "+models",
        _ => "",
    }
}
