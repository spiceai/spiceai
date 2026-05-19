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
use crate::output::{OutputFormat, TableOutput};
use clap::Args;
use repl::util::{Spinner, create_editor_with_history, save_history};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use std::collections::HashMap;
use std::io::{self, Write};

/// Arguments for the `search` command.
#[derive(Args, Debug)]
#[command(
    about = "Run vector / hybrid search across embedded datasets",
    long_about = r#"Run vector or hybrid search across datasets that have been
configured with embeddings.

Opens an interactive REPL by default. Within the REPL you can scope a query to
specific datasets, project additional columns, and apply a SQL `WHERE` filter.
Datasets must declare `embeddings:` in `spicepod.yaml` for search to work.

EXAMPLES
  spice search                          # Interactive REPL
  spice search --limit 25 -o json       # JSON output, larger result set
  spice search --model my_embed         # Use a specific embedding model

Docs: https://spiceai.org/docs"#
)]
pub struct SearchArgs {
    /// Maximum number of results to return per query.
    #[arg(long, short, default_value = "10")]
    pub limit: u32,

    /// Whether to use the runtime results cache (`cache` or `no-cache`).
    #[arg(long, default_value = "cache", value_parser = ["cache", "no-cache"])]
    pub cache_control: String,

    /// Embedding model id to use (defaults to the dataset's configured embedding).
    #[arg(long)]
    pub model: Option<String>,

    /// Override the runtime HTTP endpoint (e.g. `http://localhost:8090`).
    #[arg(long)]
    pub endpoint: Option<String>,

    /// Custom HTTP headers in `Key:Value` form (repeatable).
    #[arg(long = "headers", value_name = "KEY:VALUE")]
    pub custom_headers: Vec<String>,

    /// Output format.
    #[arg(long, short = 'o', default_value = "table")]
    pub output: OutputFormat,
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
#[derive(Deserialize, Serialize)]
struct SearchMatch {
    matches: HashMap<String, StringOrSlice>,
    #[serde(rename = "_score", alias = "score")]
    score: f64,
    dataset: String,
    #[serde(default)]
    primary_key: HashMap<String, serde_json::Value>,
}

/// Response from the search endpoint.
#[derive(Deserialize, Serialize)]
struct SearchResponse {
    results: Vec<SearchMatch>,
    duration_ms: u64,
}

/// Full search result with metadata.
struct SearchResult {
    response: SearchResponse,
    from_cache: bool,
}

/// A string or array of strings (for flexible JSON parsing).
#[derive(Deserialize, Serialize)]
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
    if args.output == OutputFormat::Table {
        println!("Welcome to the Spice.ai search REPL! Enter your search queries.");
        println!();
    }

    run_repl(ctx, args).await
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, args: &SearchArgs) -> Result<()> {
    let (mut rl, history_path) = create_editor_with_history("search_history.txt").map_err(|e| {
        InvalidResponseSnafu {
            message: e.to_string(),
        }
        .build()
    })?;

    loop {
        let readline = rl.readline("search> ");
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
            println!("Enter a search query.");
            continue;
        }

        // Add to history
        let _ = rl.add_history_entry(query);

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

        // Execute the search with spinner
        match send_search_request_with_spinner(ctx, query, args).await {
            Ok(result) => {
                display_results(&result.response, args.output);
                #[expect(clippy::cast_precision_loss)]
                let duration_secs = result.response.duration_ms as f64 / 1000.0;
                let cached_str = if result.from_cache { " (cached)" } else { "" };
                if args.output == OutputFormat::Table {
                    println!(
                        "\nTime: {duration_secs:.3} seconds. {} results{cached_str}.",
                        result.response.results.len()
                    );
                    println!();
                }
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

/// Send a request to the search endpoint with spinner.
async fn send_search_request_with_spinner(
    ctx: &RuntimeContext,
    query: &str,
    args: &SearchArgs,
) -> Result<SearchResult> {
    let spinner = Spinner::start();

    let result = send_search_request(ctx, query, args).await;

    spinner.stop().await;

    result
}

/// Send a request to the search endpoint.
async fn send_search_request(
    ctx: &RuntimeContext,
    query: &str,
    args: &SearchArgs,
) -> Result<SearchResult> {
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
    // Check cache status header
    let cache_status = response
        .headers()
        .get("Search-Results-Cache-Status")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let from_cache = matches!(cache_status.as_deref(), Some("HIT" | "STALE"));

    let text = response.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(InvalidResponseSnafu {
            message: format!("Search failed: {text}"),
        }
        .build());
    }

    let response: SearchResponse = serde_json::from_str(&text).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse response: {e}"),
        }
        .build()
    })?;

    Ok(SearchResult {
        response,
        from_cache,
    })
}

/// Display search results in a table.
fn display_results(response: &SearchResponse, output: OutputFormat) {
    if response.results.is_empty() {
        println!("No results.");
        return;
    }

    if output == OutputFormat::Json {
        match serde_json::to_string_pretty(response) {
            Ok(json) => println!("{json}"),
            Err(e) => eprintln!("Failed to serialize search results: {e}"),
        }
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

#[cfg(test)]
mod tests {
    use super::SearchResponse;

    #[test]
    fn deserializes_search_results_with_underscored_score() {
        let response = serde_json::from_str::<SearchResponse>(
            r#"{
                "results": [
                    {
                        "matches": {"body": "Spice runtime error"},
                        "_score": 0.98,
                        "dataset": "spice.public.issues",
                        "primary_key": {"id": 1}
                    }
                ],
                "duration_ms": 12
            }"#,
        )
        .expect("search response with _score should deserialize");

        assert_eq!(response.results.len(), 1);
        assert!((response.results[0].score - 0.98).abs() < f64::EPSILON);
    }
}
