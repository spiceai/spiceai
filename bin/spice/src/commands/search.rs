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

//! `spice search` command - Semantic search REPL.

use crate::context::RuntimeContext;
use crate::error::{ConnectionFailedSnafu, InvalidResponseSnafu, Result};
use crate::output::TableOutput;
use clap::Args;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::{self, BufRead, Write};

/// Arguments for the `search` command.
#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Limit number of search results
    #[arg(long, short, default_value = "10")]
    pub limit: u32,

    /// Control whether the results cache is used for searches
    #[arg(long, default_value = "cache", value_parser = ["cache", "no-cache"])]
    pub cache_control: String,

    /// Model to use for search
    #[arg(long)]
    pub model: Option<String>,

    /// Remote Spice instance HTTP endpoint (e.g., `http://localhost:8090`)
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Custom HTTP headers in format 'Key:Value' (can be specified multiple times)
    #[arg(long = "headers", value_name = "KEY:VALUE")]
    pub custom_headers: Vec<String>,
}

/// Request body for the search endpoint.
#[derive(Serialize)]
struct SearchRequest {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    datasets: Option<Vec<String>>,
    limit: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    r#where: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
}

/// A single search match result.
#[derive(Deserialize)]
struct SearchMatch {
    matches: HashMap<String, StringOrSlice>,
    score: f64,
    dataset: String,
    #[serde(default)]
    primary_key: HashMap<String, serde_json::Value>,
}

/// Response from the search endpoint.
#[derive(Deserialize)]
struct SearchResponse {
    results: Vec<SearchMatch>,
    duration_ms: u64,
}

/// A string or array of strings (for flexible JSON parsing).
#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrSlice {
    Single(String),
    Multiple(Vec<String>),
}

impl StringOrSlice {
    fn as_vec(&self) -> Vec<&str> {
        match self {
            Self::Single(s) => vec![s.as_str()],
            Self::Multiple(v) => v.iter().map(String::as_str).collect(),
        }
    }
}

/// Execute the `search` command.
///
/// # Errors
///
/// Returns an error if the API requests fail or input/output fails.
pub async fn execute(ctx: &RuntimeContext, args: &SearchArgs) -> Result<()> {
    println!("Welcome to the Spice.ai search REPL! Enter your search queries.");
    println!();

    run_repl(ctx, args).await
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, args: &SearchArgs) -> Result<()> {
    let stdin = io::stdin();

    loop {
        print!("search> ");
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
            println!("Enter a search query.");
            continue;
        }

        // Handle exit commands
        if query == "exit" || query == "quit" || query == ".exit" || query == ".quit" {
            break;
        }

        // Handle clear command
        if query.to_lowercase() == ".clear" {
            print!("\x1b[H\x1b[2J");
            let _ = io::stdout().flush();
            continue;
        }

        // Execute the search
        match send_search_request(ctx, query, args).await {
            Ok(response) => {
                display_results(&response);
                #[expect(clippy::cast_precision_loss)]
                let duration_secs = response.duration_ms as f64 / 1000.0;
                println!(
                    "\nTime: {duration_secs:.3} seconds. {} results.",
                    response.results.len()
                );
                println!();
            }
            Err(e) => {
                eprintln!("\x1b[31mError\x1b[0m {e}");
            }
        }
    }

    Ok(())
}

/// Send a request to the search endpoint.
async fn send_search_request(
    ctx: &RuntimeContext,
    query: &str,
    args: &SearchArgs,
) -> Result<SearchResponse> {
    // Use endpoint override if provided, otherwise use context's endpoint
    let base_url = args
        .endpoint
        .as_deref()
        .unwrap_or_else(|| ctx.http_endpoint());
    let url = format!("{base_url}/v1/search");

    let body = SearchRequest {
        text: query.to_string(),
        datasets: None,
        limit: args.limit,
        additional_columns: None,
        r#where: None,
        model: args.model.clone(),
    };

    let mut request = ctx
        .http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Cache-Control", &args.cache_control)
        .json(&body);

    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    // Add custom headers from command line
    for header in &args.custom_headers {
        if let Some((key, value)) = header.split_once(':') {
            request = request.header(key.trim(), value.trim());
        }
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    let status = response.status();
    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(InvalidResponseSnafu {
            message: format!("Search failed: {text}"),
        }
        .build());
    }

    serde_json::from_str(&text).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })
}

/// Display search results in a table.
fn display_results(response: &SearchResponse) {
    if response.results.is_empty() {
        println!("No results.");
        return;
    }

    // Collect all primary key names
    let mut pk_names: Vec<String> = Vec::new();
    for result in &response.results {
        for key in result.primary_key.keys() {
            if !pk_names.contains(key) {
                pk_names.push(key.clone());
            }
        }
    }
    pk_names.sort();

    // Build table
    let headers = if pk_names.is_empty() {
        vec!["Rank", "Match", "Score", "Dataset"]
    } else {
        vec!["Rank", "Key", "Match", "Score", "Dataset"]
    };

    let mut table = TableOutput::new(headers);

    for (i, result) in response.results.iter().enumerate() {
        let rank = format!("{}", i + 1);
        let score = format!("{:.4}", result.score);
        let dataset = result.dataset.clone();

        // Format primary key
        let pk_value = if pk_names.is_empty() {
            String::new()
        } else {
            pk_names
                .iter()
                .filter_map(|k| result.primary_key.get(k).map(|v| format!("{v}")))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Format matches - show first 3 lines
        let match_text = format_matches(&result.matches);

        if pk_names.is_empty() {
            table.add_row(vec![rank, match_text, score, dataset]);
        } else {
            table.add_row(vec![rank, pk_value, match_text, score, dataset]);
        }
    }

    println!("{table}");
}

/// Format match text, truncating to first 3 lines.
fn format_matches(matches: &HashMap<String, StringOrSlice>) -> String {
    let mut texts: Vec<String> = Vec::new();

    for (col, values) in matches {
        for value in values.as_vec() {
            // Take first 3 lines
            let lines: Vec<&str> = value.lines().take(3).collect();
            let truncated = lines.join("\n").replace('\r', "");

            if matches.len() > 1 {
                texts.push(format!("{col}: {truncated}"));
            } else {
                texts.push(truncated);
            }
        }
    }

    texts.join("; ")
}
