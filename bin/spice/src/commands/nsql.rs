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

//! `spice nsql` command - Natural language to SQL REPL.

use crate::context::RuntimeContext;
use crate::error::{ConnectionFailedSnafu, InvalidResponseSnafu, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::io::{self, BufRead, Write};
use std::time::Instant;

/// Arguments for the `nsql` command.
#[derive(Args, Debug)]
pub struct NsqlArgs {
    /// Model to use for text-to-SQL conversion
    #[arg(long, short)]
    pub model: Option<String>,
}

/// Request body for the nsql endpoint.
#[derive(Serialize)]
struct NsqlRequest {
    query: String,
    model: String,
}

/// Model information from the models endpoint.
#[derive(Deserialize)]
struct Model {
    id: String,
}

/// Response from the models endpoint.
#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<Model>,
}

/// Execute the `nsql` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &NsqlArgs) -> Result<()> {
    println!("Welcome to the Spice.ai NSQL REPL!");

    // Get or select the model
    let model = match &args.model {
        Some(m) => m.clone(),
        None => select_model(ctx).await?,
    };

    println!("\nUsing model:\n {model}");
    println!("\nEnter a query in natural language.");

    // Run the REPL
    run_repl(ctx, &model).await
}

/// Select a model from available models.
async fn select_model(ctx: &RuntimeContext) -> Result<String> {
    let url = format!("{}/v1/models?status=true", ctx.http_endpoint());

    let mut request = ctx.http_client().get(&url);
    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    if !response.status().is_success() {
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        return Err(InvalidResponseSnafu {
            message: format!("Failed to get models: {status} - {text}"),
        }
        .build());
    }

    let models: ModelsResponse = response.json().await.map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse models response: {e}"),
        }
        .build()
    })?;

    if models.data.is_empty() {
        return Err(InvalidResponseSnafu {
            message: "No models found. Please configure a model in your Spicepod.".to_string(),
        }
        .build());
    }

    // If only one model, use it
    if models.data.len() == 1 {
        return Ok(models.data[0].id.clone());
    }

    // Let user select from multiple models
    println!("\nAvailable models:");
    for (i, model) in models.data.iter().enumerate() {
        println!("  {}: {}", i + 1, model.id);
    }

    print!("Select model (1-{}): ", models.data.len());
    let _ = io::stdout().flush();

    let stdin = io::stdin();
    let mut input = String::new();
    stdin.lock().read_line(&mut input).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to read input: {e}"),
        }
        .build()
    })?;

    let selection: usize = input.trim().parse().map_err(|_| {
        InvalidResponseSnafu {
            message: "Invalid selection".to_string(),
        }
        .build()
    })?;

    if selection == 0 || selection > models.data.len() {
        return Err(InvalidResponseSnafu {
            message: format!("Selection must be between 1 and {}", models.data.len()),
        }
        .build());
    }

    Ok(models.data[selection - 1].id.clone())
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, model: &str) -> Result<()> {
    let stdin = io::stdin();

    loop {
        print!("nsql> ");
        let _ = io::stdout().flush();

        let mut input = String::new();
        match stdin.lock().read_line(&mut input) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error reading input: {e}");
                continue;
            }
        }

        let query = input.trim();
        if query.is_empty() {
            println!("Enter a No-SQL (natural language) query.");
            continue;
        }

        // Handle exit commands
        if query == "exit" || query == "quit" || query == ".exit" || query == ".quit" {
            break;
        }

        // Execute the query
        let start = Instant::now();
        match send_nsql_request(ctx, query, model).await {
            Ok(result) => {
                let elapsed = start.elapsed().as_secs_f64();
                display_result(&result, elapsed);
            }
            Err(e) => {
                eprintln!("\x1b[31mError\x1b[0m {e}");
            }
        }
    }

    Ok(())
}

/// Send a request to the nsql endpoint.
async fn send_nsql_request(ctx: &RuntimeContext, query: &str, model: &str) -> Result<String> {
    let url = format!("{}/v1/nsql", ctx.http_endpoint());

    let body = NsqlRequest {
        query: query.to_string(),
        model: model.to_string(),
    };

    let mut request = ctx
        .http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Accept", "text/plain")
        .json(&body);

    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(InvalidResponseSnafu {
            message: format!("Query failed: {text}"),
        }
        .build());
    }

    Ok(text)
}

/// Display the query result.
fn display_result(result: &str, elapsed: f64) {
    // Empty result marker from the server
    if result == "++\n++" {
        println!("No results.");
        return;
    }

    // Count rows (subtract 3 for header lines in table format)
    let row_count = result.lines().count().saturating_sub(3);

    println!("{result}");
    println!("\nTime: {elapsed:.6} seconds. {row_count} rows.");
}
