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

mod args;
mod cayenne_server;
mod commands;
mod local_spiced_server;
mod scenario;
mod stdio_server;

use args::{Commands, StdioArgs};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    subcommand: Commands,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    let cli = Cli::parse();

    match cli.subcommand {
        Commands::Stdio(args) => run_stdio_mode(&args).await,
        Commands::LocalSpiced(args) => run_local_spiced_mode(&args).await,
        Commands::CayenneFlightsql(args) => run_cayenne_flightsql_mode(&args).await,
    }
}

async fn run_stdio_mode(args: &StdioArgs) -> anyhow::Result<()> {
    eprintln!("Starting spidapter stdio JSON-RPC server");
    stdio_server::run_stdio_server(args).await
}

async fn run_local_spiced_mode(args: &args::LocalSpicedArgs) -> anyhow::Result<()> {
    eprintln!("Starting spidapter local-spiced stdio JSON-RPC server");
    local_spiced_server::run_local_spiced_server(args).await
}

async fn run_cayenne_flightsql_mode(args: &args::CayenneFlightsqlArgs) -> anyhow::Result<()> {
    eprintln!("Starting spidapter cayenne-flightsql stdio JSON-RPC server");
    cayenne_server::run_cayenne_flightsql_server(args).await
}
