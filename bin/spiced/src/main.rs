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
use rustls::crypto::{self, CryptoProvider};
use tokio::runtime::Runtime;

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
    spiced::run(args).await?;
    Ok(())
}
