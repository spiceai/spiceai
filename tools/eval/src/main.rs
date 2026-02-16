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

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

mod client;
mod eval_runner;
mod scorer;

#[derive(Parser, Debug)]
#[command(name = "spice-eval")]
#[command(about = "Standalone evaluation tool for Spice.ai models", long_about = None)]
struct Args {
    /// Spiced runtime HTTP endpoint
    #[arg(long, default_value = "http://localhost:8090", env = "SPICE_HTTP_ENDPOINT")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List available evaluations
    List,

    /// Run an evaluation
    Run {
        /// Name of the evaluation to run
        eval_name: String,

        /// Model to evaluate
        #[arg(long)]
        model: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();

    let args = Args::parse();
    let client = client::SpiceClient::new(&args.endpoint);

    match args.command {
        Commands::List => {
            let evals = client.list_evals().await?;
            if evals.is_empty() {
                println!("No evaluations configured.");
            } else {
                println!("Available evaluations:");
                for eval in evals {
                    println!("  - {}: {} (dataset: {}, scorers: {})",
                        eval.name,
                        eval.description.as_deref().unwrap_or(""),
                        eval.dataset,
                        eval.scorers.join(", ")
                    );
                }
            }
        }
        Commands::Run { eval_name, model } => {
            let eval = client.get_eval(&eval_name).await?;

            tracing::info!("Starting evaluation '{}' for model '{}'", eval_name, model);

            let run_id = eval_runner::run_eval(
                &client,
                &eval,
                &model,
            ).await?;

            tracing::info!("Evaluation completed: {}", run_id);
            println!("Evaluation run ID: {}", run_id);
        }
    }

    Ok(())
}
