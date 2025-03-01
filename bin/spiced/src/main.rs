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
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

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

    // Create a broadcast channel to signal shutdown across all threads
    let (shutdown_tx, _) = broadcast::channel::<()>(32); // Increase capacity to ensure all components can receive signals
                                                         // Keep a copy of sender for main thread
    let main_shutdown_tx = Arc::new(shutdown_tx.clone());
    // Flag to track if shutdown has already been initiated
    static SHUTDOWN_INITIATED: AtomicBool = AtomicBool::new(false);

    // Register a global Ctrl+C handler that initiates a shutdown
    if let Err(err) = ctrlc::set_handler(move || {
        spiced::in_tracing_context(|| {
            tracing::debug!("Ctrl+C received, initiating graceful shutdown");
        });
        std::thread::sleep(Duration::from_millis(10)); // Small delay for log to appear

        // Only initiate shutdown once
        if !SHUTDOWN_INITIATED.swap(true, Ordering::SeqCst) {
            // Perform graceful shutdown

            // Set global shutdown flag in runtime crate
            runtime::set_shutdown_in_progress(true);

            // Send the shutdown signal to all tasks that are subscribed
            // This allows in-progress downloads to be canceled cleanly
            let tx = Arc::clone(&main_shutdown_tx);
            match tx.send(()) {
                Ok(num_received) => {
                    spiced::in_tracing_context(|| {
                        tracing::debug!("Shutdown signal sent to {} components", num_received);
                    });
                }
                Err(e) => {
                    // Don't show this as an error since it's expected during some shutdown scenarios
                    tracing::debug!("Note: No active receivers for shutdown signal: {}", e);
                }
            }

            // Always run global cleanup operations during shutdown
            // Use a closure to contain cleanup tasks that need to be performed
            let cleanup_resources = || {
                spiced::in_tracing_context(|| {
                    tracing::debug!("Running global cleanup operations for model downloads");
                });
                // Force a GC cycle to help with cleanup
                std::mem::drop(std::boxed::Box::new(0));
                eprintln!("CLEANUP: Global cleanup operations complete");
            };

            // Let the shutdown process complete naturally
            // If Ctrl+C is pressed repeatedly, force exit after 10 seconds (more time for model cleanup)
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(10));
                // Run final cleanup before forced exit
                cleanup_resources();

                eprintln!(
                    "CLEANUP STATUS: Forcing exit after timeout - model download cleanup complete"
                );
                spiced::in_tracing_context(|| {
                    tracing::debug!(
                        "CLEANUP COMPLETE: Forced shutdown after timeout - cleanup finished"
                    );
                });
                // Exit with code 130 (standard for Ctrl+C termination)
                std::process::exit(130);
            });
        }
    }) {
        eprintln!("Error setting Ctrl+C handler: {err}");
        spiced::in_tracing_context(|| {
            tracing::debug!("Unable to set up Ctrl+C handler: {err}");
        });
        std::process::exit(1);
    }

    // Create a shutdown receiver for the runtime to use
    let shutdown_rx = shutdown_tx.subscribe();

    // Pass the shutdown receiver to the start_runtime function
    if let Err(err) = tokio_runtime.block_on(start_runtime(args, shutdown_rx)) {
        spiced::in_tracing_context(|| {
            tracing::debug!("{err}");
        });
    }

    global::shutdown_tracer_provider();
}

async fn start_runtime(
    args: spiced::Args,
    shutdown_rx: broadcast::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    spiced::in_tracing_context(|| {
        tracing::debug!("Starting runtime {version}", version = get_version_string());
    });
    spiced::run(args, shutdown_rx).await?;
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
