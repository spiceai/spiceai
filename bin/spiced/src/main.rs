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

use clap::Parser;
use opentelemetry::global;
use rustls::crypto::{self, CryptoProvider};
use tokio::runtime::Runtime;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// Counter for tracking Ctrl+C presses
static CTRL_C_COUNT: AtomicUsize = AtomicUsize::new(0);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() {
    let args = spiced::Args::parse();

    if args.version {
        println!("{}", get_version_string());
        return;
    }

    let tokio_runtime = match Runtime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("Unable to start Tokio runtime: {err}");
            std::process::exit(1);
        }
    };

    // Install the default AWS LC RS crypto provider for rusttls
    let _ = CryptoProvider::install_default(crypto::aws_lc_rs::default_provider());

    if args.repl {
        if let Err(e) = tokio_runtime.block_on(flightrepl::run(args.repl_config)) {
            eprintln!("SQL REPL Error: {e}");
        };
        return;
    }

    // Register a global Ctrl+C handler that forcibly exits after repeated presses
    ctrlc::set_handler(move || {
        let count = CTRL_C_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
        
        // First press: request graceful shutdown
        if count == 1 {
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
            spiced::in_tracing_context(|| {
                tracing::info!("Received Ctrl+C, shutting down gracefully...");
            });
        }
        // Second press: warn user
        else if count == 2 {
            spiced::in_tracing_context(|| {
                tracing::warn!("Received Ctrl+C again, waiting for graceful shutdown to complete...");
                tracing::warn!("Press Ctrl+C once more to force exit");
            });
        }
        // Third press or more: force exit
        else {
            spiced::in_tracing_context(|| {
                tracing::error!("Received Ctrl+C multiple times, forcing immediate exit");
            });
            std::process::exit(130); // Standard exit code for Ctrl+C termination
        }
    }).expect("Error setting Ctrl+C handler");

    match tokio_runtime.block_on(start_runtime(args)) {
        Ok(_) => {
            // Successful clean shutdown
            spiced::in_tracing_context(|| {
                tracing::info!("Runtime shut down successfully");
            });
        }
        Err(err) => {
            spiced::in_tracing_context(|| {
                tracing::error!("{err}");
            });
        }
    }

    global::shutdown_tracer_provider();
}

async fn start_runtime(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    spiced::in_tracing_context(|| {
        tracing::info!("Starting runtime {version}", version = get_version_string());
    });
    
    // Create a future that completes when Ctrl+C is pressed
    let shutdown_signal = async {
        let mut grace_period = tokio::time::interval(tokio::time::Duration::from_secs(5));
        
        loop {
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(50)) => {
                    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                        // Exit the loop if shutdown was requested
                        break;
                    }
                }
                _ = grace_period.tick() => {
                    // Add a long grace period timer that will force exit if shutdown takes too long
                    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                        spiced::in_tracing_context(|| {
                            tracing::warn!("Shutdown taking too long, forcing exit...");
                        });
                        std::process::exit(1);
                    }
                }
            }
        }
    };
    
    // Race between normal runtime execution and the Ctrl+C signal
    tokio::select! {
        result = spiced::run(args) => result?,
        _ = shutdown_signal => {
            spiced::in_tracing_context(|| {
                tracing::info!("Shutdown signal received, stopping runtime");
            });
        }
    }
    
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
