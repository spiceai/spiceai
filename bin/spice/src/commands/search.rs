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

//! `spice search` command — one-shot, piped, or interactive vector / hybrid search.

use crate::context::RuntimeContext;
use crate::error::{
    ConnectionFailedSnafu, InvalidResponseSnafu, InvalidUsageSnafu, Result, read_response,
};
use crate::output::{OutputFormat, TableOutput, write_json};
use clap::Args;
use repl::util::{Spinner, create_editor_with_history, save_history};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};
use std::collections::HashMap;
use std::io::{self, IsTerminal, Write};

const EMPTY_QUERY_MESSAGE: &str = "A search query is required. Pass one as `spice search 'QUERY'`, or pipe queries on stdin. See: https://spiceai.org/docs";

const SEARCH_FAILED_DOCS: &str = "https://spiceai.org/docs";

/// Search datasets that have embeddings.
#[derive(Args, Debug)]
#[command(
    about = "Search datasets that have embeddings",
    disable_help_subcommand = true,
    long_about = "Search datasets that have embeddings via POST /v1/search.

Pass a query to run once and exit. With no query on a terminal, opens an \
interactive REPL. With no query and piped stdin, reads one query per line. \
JSON output of several piped queries is a JSON array.

Scoping flags are sent in the request body and filter the search.

EXAMPLES
  spice search 'dolphins'
  spice search 'dolphins' --datasets docs --where \"status = 'open'\" -o json
  echo dolphins | spice search -o json
  spice search

Docs: https://spiceai.org/docs"
)]
pub struct SearchArgs {
    /// Search query. Omit on a terminal to open the REPL; omit with piped stdin to read queries from stdin.
    #[arg(value_name = "QUERY")]
    pub query: Option<String>,

    /// Maximum number of matches to return from each dataset.
    #[arg(long, short, default_value = "10")]
    pub limit: u32,

    /// Datasets to search. Repeat or comma-separate. Default: all searchable datasets.
    #[arg(long, value_name = "DATASET", value_delimiter = ',')]
    pub datasets: Vec<String>,

    /// Extra columns to return with each match.
    #[arg(
        long = "additional-columns",
        value_name = "COLUMN",
        value_delimiter = ','
    )]
    pub additional_columns: Vec<String>,

    /// SQL filter applied before ranking, without the `WHERE` keyword.
    #[arg(long = "where", value_name = "FILTER", allow_hyphen_values = true)]
    pub where_filter: Option<String>,

    /// Keywords used to pre-filter the embedding column. Repeat or comma-separate.
    #[arg(long, value_name = "KEYWORD", value_delimiter = ',')]
    pub keywords: Vec<String>,

    /// Whether to use the runtime results cache (`cache` or `no-cache`).
    #[arg(long, default_value = "cache", value_parser = ["cache", "no-cache"])]
    pub cache_control: String,

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

/// Request body for `POST /v1/search`.
#[derive(Debug, Serialize, PartialEq, Eq)]
struct SearchRequest {
    text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    datasets: Option<Vec<String>>,
    limit: u32,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    additional_columns: Vec<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none")]
    where_filter: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    keywords: Vec<String>,
}

/// A single search match result.
#[derive(Debug, Deserialize, Serialize)]
struct SearchMatch {
    matches: HashMap<String, StringOrSlice>,
    #[serde(rename = "_score", alias = "score")]
    score: f64,
    dataset: String,
    #[serde(default)]
    primary_key: HashMap<String, serde_json::Value>,
    #[serde(default)]
    data: HashMap<String, serde_json::Value>,
}

/// Response from the search endpoint.
#[derive(Debug, Deserialize, Serialize)]
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
#[derive(Debug, Deserialize, Serialize)]
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
/// Returns an error if the query is empty in non-interactive mode, or if the
/// API request fails.
pub async fn execute(ctx: &RuntimeContext, args: &SearchArgs) -> Result<()> {
    if let Some(query) = args.query.as_deref() {
        let query = query.trim();
        ensure!(
            !query.is_empty(),
            InvalidUsageSnafu {
                message: EMPTY_QUERY_MESSAGE
            }
        );
        let queries = [query.to_string()];
        return run_queries(ctx, args, &queries).await;
    }

    if io::stdin().is_terminal() {
        return run_repl(ctx, args).await;
    }

    let queries = read_stdin_queries()?;
    ensure!(
        !queries.is_empty(),
        InvalidUsageSnafu {
            message: EMPTY_QUERY_MESSAGE
        }
    );
    run_queries(ctx, args, &queries).await
}

fn read_stdin_queries() -> Result<Vec<String>> {
    let mut queries = Vec::new();
    for line in io::stdin().lines() {
        let line = line.map_err(|source| {
            InvalidResponseSnafu {
                message: format!("Failed to read search queries from stdin: {source}"),
            }
            .build()
        })?;
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            queries.push(trimmed.to_string());
        }
    }
    Ok(queries)
}

async fn run_queries(ctx: &RuntimeContext, args: &SearchArgs, queries: &[String]) -> Result<()> {
    if args.output == OutputFormat::Json && queries.len() > 1 {
        let mut responses = Vec::with_capacity(queries.len());
        for query in queries {
            responses.push(
                send_search_request_with_spinner(ctx, query, args)
                    .await?
                    .response,
            );
        }
        return write_json(&responses);
    }

    for query in queries {
        run_one(ctx, args, query).await?;
    }
    Ok(())
}

async fn run_one(ctx: &RuntimeContext, args: &SearchArgs, query: &str) -> Result<()> {
    let result = send_search_request_with_spinner(ctx, query, args).await?;
    display_results(&result.response, args.output)?;
    if args.output == OutputFormat::Table {
        #[expect(clippy::cast_precision_loss)]
        let duration_secs = result.response.duration_ms as f64 / 1000.0;
        let cached_str = if result.from_cache { " (cached)" } else { "" };
        println!(
            "\nTime: {duration_secs:.3} seconds. {} results{cached_str}.",
            result.response.results.len()
        );
        println!();
    }
    Ok(())
}

/// Run the REPL loop.
async fn run_repl(ctx: &RuntimeContext, args: &SearchArgs) -> Result<()> {
    if args.output == OutputFormat::Table {
        println!("Welcome to the Spice.ai search REPL! Enter your search queries.");
        println!();
    }

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

        let _ = rl.add_history_entry(query);

        if query == "exit" || query == "quit" || query == ".exit" || query == ".quit" {
            break;
        }

        if query.to_lowercase() == ".clear" {
            print!("\x1b[H\x1b[2J");
            let _ = io::stdout().flush();
            continue;
        }

        match run_one(ctx, args, query).await {
            Ok(()) => {}
            Err(e) => {
                eprintln!("Error {e}");
            }
        }
    }

    save_history(&mut rl, history_path.as_ref());

    Ok(())
}

/// Send a request to the search endpoint, with a spinner when stdout is a TTY.
async fn send_search_request_with_spinner(
    ctx: &RuntimeContext,
    query: &str,
    args: &SearchArgs,
) -> Result<SearchResult> {
    let spinner = Spinner::start_if_stdout_tty();

    let result = send_search_request(ctx, query, args).await;

    if let Some(spinner) = spinner {
        spinner.stop().await;
    }

    result
}

fn search_body(query: &str, args: &SearchArgs) -> SearchRequest {
    SearchRequest {
        text: query.to_string(),
        datasets: if args.datasets.is_empty() {
            None
        } else {
            Some(args.datasets.clone())
        },
        limit: args.limit,
        additional_columns: args.additional_columns.clone(),
        where_filter: args
            .where_filter
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
        keywords: args.keywords.clone(),
    }
}

/// Send a request to the search endpoint.
async fn send_search_request(
    ctx: &RuntimeContext,
    query: &str,
    args: &SearchArgs,
) -> Result<SearchResult> {
    let base_url = args
        .endpoint
        .as_deref()
        .unwrap_or_else(|| ctx.http_endpoint());
    let url = format!("{base_url}/v1/search");

    let body = search_body(query, args);

    // A search embeds its query before it can run, so its duration is a model's.
    let mut request = ctx
        .inference_http_client()
        .post(&url)
        .header("Content-Type", "application/json")
        .header("Cache-Control", &args.cache_control)
        .json(&body);

    for (key, value) in ctx.get_headers() {
        request = request.header(&key, &value);
    }

    for header in &args.custom_headers {
        if let Some((key, value)) = header.split_once(':') {
            request = request.header(key.trim(), value.trim());
        }
    }

    let response = request
        .send()
        .await
        .context(ConnectionFailedSnafu { endpoint: &url })?;

    let cache_status = response
        .headers()
        .get("Search-Results-Cache-Status")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let from_cache = matches!(cache_status.as_deref(), Some("HIT" | "STALE"));

    let (status, text) = read_response(response, &url).await?;

    if !status.is_success() {
        return Err(InvalidResponseSnafu {
            message: format!(
                "Search failed with HTTP {status} from '{url}': {text}. Check the runtime is running and searchable datasets are ready. See: {SEARCH_FAILED_DOCS}"
            ),
        }
        .build());
    }

    let response: SearchResponse = serde_json::from_str(&text).map_err(|e| {
        InvalidResponseSnafu {
            message: format!("Failed to parse search response from '{url}': {e}"),
        }
        .build()
    })?;

    Ok(SearchResult {
        response,
        from_cache,
    })
}

/// Display search results as a table or as JSON.
fn display_results(response: &SearchResponse, output: OutputFormat) -> Result<()> {
    if output == OutputFormat::Json {
        return write_json(response);
    }

    if response.results.is_empty() {
        println!("No results.");
        return Ok(());
    }

    let mut pk_names: Vec<String> = Vec::new();
    let mut data_names: Vec<String> = Vec::new();
    for result in &response.results {
        for key in result.primary_key.keys() {
            if !pk_names.contains(key) {
                pk_names.push(key.clone());
            }
        }
        for key in result.data.keys() {
            if !data_names.contains(key) {
                data_names.push(key.clone());
            }
        }
    }
    pk_names.sort();
    data_names.sort();

    let mut headers = vec!["Rank"];
    if !pk_names.is_empty() {
        headers.push("Key");
    }
    headers.push("Match");
    if !data_names.is_empty() {
        headers.push("Data");
    }
    headers.extend(["Score", "Dataset"]);

    let mut table = TableOutput::new(headers);

    for (i, result) in response.results.iter().enumerate() {
        let rank = (i + 1).to_string();
        let score = format!("{:.4}", result.score);
        let dataset = result.dataset.clone();
        let match_text = format_matches(&result.matches);

        let mut row = vec![rank];
        if !pk_names.is_empty() {
            row.push(
                pk_names
                    .iter()
                    .filter_map(|k| result.primary_key.get(k).map(ToString::to_string))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        row.push(match_text);
        if !data_names.is_empty() {
            row.push(
                data_names
                    .iter()
                    .filter_map(|k| result.data.get(k).map(|v| format!("{k}={v}")))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        row.push(score);
        row.push(dataset);
        table.add_row(row);
    }

    println!("{table}");
    Ok(())
}

/// Format match text, truncating to first 3 lines.
fn format_matches(matches: &HashMap<String, StringOrSlice>) -> String {
    let mut texts: Vec<String> = Vec::new();

    for (col, values) in matches {
        for value in values.as_vec() {
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
    use super::*;
    use crate::context::Deadline;
    use clap::{CommandFactory, Parser};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[derive(Parser, Debug)]
    #[command(name = "search", disable_help_subcommand = true)]
    struct SearchCli {
        #[command(flatten)]
        search: SearchArgs,
    }

    fn parse_search(argv: &[&str]) -> SearchArgs {
        SearchCli::try_parse_from(argv)
            .unwrap_or_else(|e| panic!("failed to parse {argv:?}: {e}"))
            .search
    }

    fn test_args(query: &str) -> SearchArgs {
        SearchArgs {
            query: Some(query.to_string()),
            limit: 10,
            datasets: Vec::new(),
            additional_columns: Vec::new(),
            where_filter: None,
            keywords: Vec::new(),
            cache_control: "cache".to_string(),
            endpoint: None,
            custom_headers: Vec::new(),
            output: OutputFormat::Json,
        }
    }

    fn ok_template() -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                {
                    "matches": {"body": "Spice runtime error"},
                    "_score": 0.98,
                    "dataset": "spice.public.issues",
                    "primary_key": {"id": 1}
                }
            ],
            "duration_ms": 12
        }))
    }

    fn context_for(server: &MockServer) -> RuntimeContext {
        RuntimeContext::with_deadlines_for_test(
            &server.uri(),
            Deadline::Total(Duration::from_secs(30)),
            Deadline::Silence(Duration::from_secs(30)),
        )
    }

    async fn posted_body(server: &MockServer) -> serde_json::Value {
        let requests = server
            .received_requests()
            .await
            .expect("the mock server records its requests");
        assert_eq!(
            requests.len(),
            1,
            "expected one POST /v1/search, got {requests:?}"
        );
        serde_json::from_slice(&requests[0].body).expect("search body is JSON")
    }

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

    #[test]
    fn help_is_a_positional_query() {
        let args = parse_search(&["search", "help"]);
        assert_eq!(args.query.as_deref(), Some("help"));
    }

    #[test]
    fn help_flag_still_renders_help() {
        let error = SearchCli::try_parse_from(["search", "--help"])
            .expect_err("--help should render help rather than search");
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
        let help = error.to_string();
        assert!(help.contains("QUERY"), "{help}");
        assert!(!help.to_lowercase().contains("subcommand"), "{help}");
    }

    #[test]
    fn search_help_text_is_user_facing() {
        let mut command = SearchCli::command();
        let help = command.render_long_help().to_string();
        assert!(help.contains("--datasets"), "{help}");
        assert!(help.contains("--additional-columns"), "{help}");
        assert!(help.contains("--where"), "{help}");
        assert!(help.contains("--keywords"), "{help}");
        assert!(help.contains("--limit"), "{help}");
        assert!(
            !help.contains("--model"),
            "the HTTP search body has no model field, so the flag must not appear: {help}"
        );
        assert!(
            !help.contains("validate_cloud_region_usage"),
            "internal identifiers must not appear in help: {help}"
        );
        assert!(
            !help.contains("Within the REPL you can scope"),
            "help must not promise REPL scoping the command does not have: {help}"
        );
        assert!(
            !help.contains("Arguments for the"),
            "internal rustdoc must not leak into help: {help}"
        );
    }

    #[test]
    fn scoping_flags_are_serialized_into_the_request_body() {
        let mut args = test_args("dolphins");
        args.limit = 5;
        args.datasets = vec!["docs".into(), "wiki".into()];
        args.additional_columns = vec!["title".into(), "url".into()];
        args.where_filter = Some("status = 'open'".into());
        args.keywords = vec!["mammal".into()];

        let body = serde_json::to_value(search_body("dolphins", &args)).expect("serialize");
        assert_eq!(
            body,
            json!({
                "text": "dolphins",
                "datasets": ["docs", "wiki"],
                "limit": 5,
                "additional_columns": ["title", "url"],
                "where": "status = 'open'",
                "keywords": ["mammal"]
            })
        );
        assert!(body.get("model").is_none());
    }

    #[test]
    fn omitted_scoping_flags_are_left_out_of_the_body() {
        let body = serde_json::to_value(search_body("dolphins", &test_args("dolphins")))
            .expect("serialize");
        assert_eq!(
            body,
            json!({
                "text": "dolphins",
                "limit": 10
            })
        );
    }

    #[tokio::test]
    async fn each_scoping_flag_is_sent_in_the_search_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .and(header("cache-control", "no-cache"))
            .respond_with(ok_template())
            .mount(&server)
            .await;

        let mut args = test_args("dolphins");
        args.limit = 3;
        args.datasets = vec!["docs".into()];
        args.additional_columns = vec!["title".into()];
        args.where_filter = Some("user = 123".into());
        args.keywords = vec!["plane".into(), "tickets".into()];
        args.cache_control = "no-cache".into();

        execute(&context_for(&server), &args)
            .await
            .expect("search against the mock should succeed");

        let body = posted_body(&server).await;
        assert_eq!(body["text"], "dolphins");
        assert_eq!(body["datasets"], json!(["docs"]));
        assert_eq!(body["additional_columns"], json!(["title"]));
        assert_eq!(body["where"], "user = 123");
        assert_eq!(body["keywords"], json!(["plane", "tickets"]));
        assert_eq!(body["limit"], 3);
        assert!(body.get("model").is_none());
    }

    #[tokio::test]
    async fn help_is_sent_as_the_search_text() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .respond_with(ok_template())
            .mount(&server)
            .await;

        execute(&context_for(&server), &test_args("help"))
            .await
            .expect("searching for help should succeed");

        let body = posted_body(&server).await;
        assert_eq!(body["text"], "help");
    }

    #[tokio::test]
    async fn an_empty_query_is_a_usage_error() {
        let args = test_args("   ");
        let error = execute(&context_for(&MockServer::start().await), &args)
            .await
            .expect_err("whitespace-only query must fail");
        assert!(
            matches!(error, crate::error::Error::InvalidUsage { .. }),
            "{error}"
        );
        assert_eq!(
            error.to_string(),
            format!("Invalid argument: {EMPTY_QUERY_MESSAGE}")
        );
        assert_eq!(error.exit_code(), crate::error::Error::USAGE_EXIT_CODE);
    }

    #[tokio::test]
    async fn an_http_error_is_returned() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .respond_with(ResponseTemplate::new(400).set_body_string("No data sources provided"))
            .mount(&server)
            .await;

        let error = execute(&context_for(&server), &test_args("dolphins"))
            .await
            .expect_err("HTTP 400 must fail the command");
        let message = error.to_string();
        assert!(message.contains("400"), "{message}");
        assert!(message.contains("No data sources provided"), "{message}");
        assert!(message.contains("/v1/search"), "{message}");
        assert_eq!(error.exit_code(), crate::error::Error::FAILURE_EXIT_CODE);
    }

    #[tokio::test]
    async fn a_connection_failure_is_returned() {
        let ctx = RuntimeContext::with_deadlines_for_test(
            "http://127.0.0.1:1",
            Deadline::Total(Duration::from_secs(2)),
            Deadline::Silence(Duration::from_secs(2)),
        );
        let error = execute(&ctx, &test_args("dolphins"))
            .await
            .expect_err("a closed port must fail the command");
        let message = error.to_string();
        assert!(message.contains("Failed to connect"), "{message}");
        assert_eq!(error.exit_code(), crate::error::Error::FAILURE_EXIT_CODE);
    }

    #[test]
    fn empty_results_json_is_an_object_not_a_sentence() {
        let json = serde_json::to_string(&SearchResponse {
            results: Vec::new(),
            duration_ms: 4,
        })
        .expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["results"], json!([]));
        assert_eq!(value["duration_ms"], 4);
        assert!(
            !json.contains("No results."),
            "JSON output must stay machine-readable when there are no matches: {json}"
        );
    }

    #[tokio::test]
    async fn json_output_of_empty_results_is_valid_json() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [],
                "duration_ms": 4
            })))
            .mount(&server)
            .await;

        execute(&context_for(&server), &test_args("dolphins"))
            .await
            .expect("an empty result set is still a successful search");
    }
}
