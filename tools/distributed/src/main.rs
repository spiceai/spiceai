/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cluster;
mod commands;
mod output;

use commands::{logs, start, status, stop};

#[derive(Parser)]
#[command(name = "distributed")]
#[command(about = "Manage a distributed Spice cluster", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start cluster in background or detached mode
    Start(start::StartArgs),
    /// Stop running cluster gracefully
    Stop(stop::StopArgs),
    /// Show cluster health status
    Status(status::StatusArgs),
    /// View logs for a specific component
    Logs(logs::LogsArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Start(args) => start::execute(args).await,
        Commands::Stop(args) => stop::execute(args).await,
        Commands::Status(args) => status::execute(args).await,
        Commands::Logs(args) => logs::execute(args).await,
    }
}
