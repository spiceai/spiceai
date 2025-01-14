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
use test_framework::{anyhow, rustls};

mod commands;
mod tests;

use commands::{Commands, TestCommands};

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
        Commands::Run(TestCommands::Throughput(args)) => tests::throughput::run(&args).await?,
        Commands::Export(TestCommands::Throughput(args)) => tests::env_export(&args)?,
        Commands::Run(TestCommands::Load(args)) => tests::load::run(&args).await?,
        Commands::Export(TestCommands::Load(args)) => tests::env_export(&args)?,
        Commands::Run(TestCommands::Bench(args)) => {
            tests::bench::run(&args).await?;
        }
        Commands::Export(TestCommands::Bench(args)) => tests::env_export(&args)?,
        Commands::Run(TestCommands::DataConsistency(args)) => {
            tests::data_consistency::run(&args).await?;
        }
        Commands::Export(TestCommands::DataConsistency(args)) => {
            tests::env_export(&args.test_args)?;
        }
    }

    Ok(())
}
