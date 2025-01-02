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

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

fn main() {
    let args = spiced::Args::parse();

    if args.version {
        if cfg!(feature = "release") {
            println!("v{}{}", env!("CARGO_PKG_VERSION"), build_metadata());
        } else {
            print!(
                "v{}-build.{}",
                env!("CARGO_PKG_VERSION"),
                env!("GIT_COMMIT_HASH")
            );

            if cfg!(feature = "dev") {
                print!("-dev");
            }

            print!("{}", build_metadata());

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

    // Install the default AWS LC RS crypto provider for rusttls
    let _ = CryptoProvider::install_default(crypto::aws_lc_rs::default_provider());

    if args.repl {
        if let Err(e) = tokio_runtime.block_on(flightrepl::run(args.repl_config)) {
            eprintln!("SQL REPL Error: {e}");
        };
        return;
    }

    if let Err(err) = tokio_runtime.block_on(start_runtime(args)) {
        spiced::in_tracing_context(|| {
            tracing::error!("{err}");
        });
    }

    global::shutdown_tracer_provider();
}

async fn start_runtime(args: spiced::Args) -> Result<(), Box<dyn std::error::Error>> {
    spiced::run(args).await?;
    Ok(())
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
