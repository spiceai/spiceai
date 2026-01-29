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
use crate::error::{
    ConnectionFailedSnafu, InvalidResponseSnafu, ModelNotFoundSnafu, NoModelsConfiguredSnafu,
    Result,
};
use clap::Args;
use repl::util::{Spinner, create_editor_with_history, save_history};
use serde::Serialize;
use snafu::ResultExt;
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

/// Get or validate a model using the runtime context.
async fn get_or_select_model(ctx: &RuntimeContext, model: Option<&str>) -> Result<String> {
    let headers: Vec<(String, String)> = ctx.get_headers().into_iter().collect();
    repl::util::get_or_select_model(ctx.http_client(), ctx.http_endpoint(), &headers, model)
        .await
        .map_err(|e| match e {
            repl::util::UtilError::ModelNotFound { model, available } => {
                ModelNotFoundSnafu { model, available }.build()
            }
            repl::util::UtilError::NoModelsConfigured => NoModelsConfiguredSnafu.build(),
            repl::util::UtilError::ConnectionFailed { endpoint, source } => InvalidResponseSnafu {
                message: format!("Failed to connect to {endpoint}: {source}"),
            }
            .build(),
            repl::util::UtilError::InvalidResponse { message } => {
                InvalidResponseSnafu { message }.build()
            }
        })
}

/// Execute the `nsql` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &NsqlArgs) -> Result<()> {
    println!("Welcome to the Spice.ai NSQL REPL!");

    // Get or select the model
    let model = get_or_select_model(ctx, args.model.as_deref()).await?;

    println!("\nUsing model:\n {model}");
    println!("\nEnter a query in natural language.");

    // Run the REPL
    run_repl(ctx, &model).await
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, model: &str) -> Result<()> {
    let (mut rl, history_path) = create_editor_with_history("nsql_history.txt").map_err(|e| {
        InvalidResponseSnafu {
            message: e.to_string(),
        }
        .build()
    })?;

    loop {
        let readline = rl.readline("nsql> ");
        let user_input = match readline {
            Ok(line) => line,
            Err(
                rustyline::error::ReadlineError::Interrupted | rustyline::error::ReadlineError::Eof,
            ) => {
                break;
            }
            Err(e) => {
                eprintln!("Error reading input: {e}");
                continue;
            }
        };

        let query = user_input.trim();
        if query.is_empty() {
            println!("Enter a No-SQL (natural language) query.");
            continue;
        }

        // Add to history
        let _ = rl.add_history_entry(query);

        // Handle exit commands
        if query == "exit" || query == "quit" || query == ".exit" || query == ".quit" {
            break;
        }

        // Execute the query with spinner
        let start = Instant::now();
        match send_nsql_request_with_spinner(ctx, query, model).await {
            Ok(result) => {
                let elapsed = start.elapsed().as_secs_f64();
                display_result(&result, elapsed);
            }
            Err(e) => {
                eprintln!("\x1b[31mError\x1b[0m {e}");
            }
        }
    }

    // Save history
    save_history(&mut rl, history_path.as_ref());

    Ok(())
}

/// Send a request to the nsql endpoint with spinner.
async fn send_nsql_request_with_spinner(
    ctx: &RuntimeContext,
    query: &str,
    model: &str,
) -> Result<String> {
    let spinner = Spinner::start();

    let result = send_nsql_request(ctx, query, model).await;

    spinner.stop().await;

    result
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
