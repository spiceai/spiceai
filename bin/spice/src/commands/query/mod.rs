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

//! Async query command implementation - submit and manage async queries via /v1/queries API.

use crate::context::RuntimeContext;
use crate::error::Result;
use crate::output::TableOutput;
use arrow::util::pretty::pretty_format_batches;
use clap::{Args, Subcommand};
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use spiceai::query::{QueryInfo, QueryStatus};
use spiceai::{Client, ClientBuilder};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::mpsc;

/// Default poll interval for checking query status.
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Spinner frames for the progress indicator.
const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Arguments for the query command.
#[derive(Args, Debug)]
#[command(
    about = "Submit an async query or start an interactive async query REPL",
    long_about = r#"Submit an async SQL query or start an interactive REPL for managing async queries via the /v1/queries API.

Queries are submitted asynchronously and the CLI auto-polls for completion. Press Ctrl+C to stop
waiting for a query (the query continues running in the background).

Async queries require cluster mode with scheduler.state_location configured."#
)]
pub struct QueryArgs {
    /// SQL query to submit (if not provided, starts interactive REPL)
    #[arg()]
    sql: Option<String>,

    /// Submit and return immediately without waiting for results
    #[arg(long)]
    no_wait: bool,

    /// Maximum time to wait for query completion (e.g., "30s", "5m")
    #[arg(long)]
    timeout: Option<humantime::Duration>,

    #[command(subcommand)]
    command: Option<QuerySubcommand>,
}

#[derive(Subcommand, Debug)]
pub enum QuerySubcommand {
    /// List all queries
    List {
        /// Filter by status (pending, running, succeeded, failed, cancelled)
        #[arg(long)]
        status: Option<String>,

        /// Maximum number of queries to return
        #[arg(long, default_value = "100")]
        limit: usize,
    },

    /// Check the status of a query
    Status {
        /// Query ID to check
        query_id: String,
    },

    /// Fetch and display results of a completed query
    Results {
        /// Query ID to get results for
        query_id: String,
    },

    /// Cancel a running query
    Cancel {
        /// Query ID to cancel
        query_id: String,
    },
}

/// Tracked query in the REPL session.
#[derive(Debug, Clone)]
struct TrackedQuery {
    query_id: String,
    sql: String,
    status: String,
    submitted_at: Instant,
}

/// Build the spiceai SDK client from the runtime context.
async fn build_client(ctx: &RuntimeContext) -> Result<Arc<Client>> {
    let mut builder = ClientBuilder::new().http_url(ctx.http_endpoint());

    if let Some(api_key) = ctx.api_key() {
        builder = builder.api_key(api_key);
    }

    builder = builder.user_agent(ctx.user_agent());

    let client = builder.build().await.map_err(|e| {
        let err_str = e.to_string();
        if err_str.contains("connection refused") {
            crate::error::Error::InvalidResponse {
                message: format!(
                    "Spice runtime is unavailable at {}. Is it running?",
                    ctx.http_endpoint()
                ),
            }
        } else {
            crate::error::Error::InvalidResponse { message: err_str }
        }
    })?;

    Ok(Arc::new(client))
}

/// Execute the query command.
pub async fn execute(ctx: &RuntimeContext, args: &QueryArgs) -> Result<()> {
    let client = build_client(ctx).await?;

    // Handle subcommands
    if let Some(subcmd) = &args.command {
        return execute_subcommand(&client, subcmd).await;
    }

    // If SQL argument provided, submit directly
    if let Some(sql) = &args.sql {
        return submit_and_wait(&client, sql, !args.no_wait, args.timeout.map(Into::into)).await;
    }

    // No argument, start REPL
    run_query_repl(&client).await
}

async fn execute_subcommand(client: &Arc<Client>, cmd: &QuerySubcommand) -> Result<()> {
    match cmd {
        QuerySubcommand::List { status, limit } => {
            let resp = client
                .queries(status.as_deref(), Some(*limit))
                .await
                .map_err(|e| crate::error::Error::InvalidResponse {
                    message: e.to_string(),
                })?;

            if resp.queries.is_empty() {
                println!("No queries found.");
                return Ok(());
            }

            let mut table = TableOutput::new(vec!["QUERY ID", "STATE", "CREATED", "SQL PREVIEW"]);
            for q in &resp.queries {
                let sql = if q.sql_preview.len() > 47 {
                    format!("{}...", &q.sql_preview[..47])
                } else {
                    q.sql_preview.clone()
                };
                table.add_row(vec![
                    q.query_id.clone(),
                    q.state.clone(),
                    q.created_at.clone(),
                    sql,
                ]);
            }
            table.print();
            println!("\nTotal: {} queries", resp.queries.len());
        }
        QuerySubcommand::Status { query_id } => {
            let job =
                client
                    .get_query(query_id)
                    .map_err(|e| crate::error::Error::InvalidResponse {
                        message: e.to_string(),
                    })?;
            let info = job
                .info()
                .await
                .map_err(|e| crate::error::Error::InvalidResponse {
                    message: e.to_string(),
                })?;
            display_query_info(&info);
        }
        QuerySubcommand::Results { query_id } => {
            display_results(client, query_id, Duration::ZERO).await?;
        }
        QuerySubcommand::Cancel { query_id } => {
            let info = client.cancel_query(query_id).await.map_err(|e| {
                crate::error::Error::InvalidResponse {
                    message: e.to_string(),
                }
            })?;
            println!(
                "Query {} cancelled (status: {})",
                info.query_id, info.status
            );
        }
    }
    Ok(())
}

async fn submit_and_wait(
    client: &Arc<Client>,
    sql: &str,
    wait: bool,
    timeout: Option<Duration>,
) -> Result<()> {
    let job = client
        .query(sql)
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: e.to_string(),
        })?;

    let query_id = job.id().to_string();

    // Get initial status
    let initial_status = job
        .status()
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: e.to_string(),
        })?;

    println!("Submitted query: {query_id} ({initial_status})");

    if !wait {
        println!("Check status with: spice query status {query_id}");
        println!("Get results with: spice query results {query_id}");
        return Ok(());
    }

    println!("Waiting for completion... (Ctrl+C to stop waiting)");

    let (final_status, was_cancelled, elapsed) =
        poll_for_completion(client, &query_id, timeout).await;

    if was_cancelled {
        println!("\nStopped waiting. Query ID: {query_id}");
        return Ok(());
    }

    if let Some(status) = final_status {
        if status.is_success() {
            display_results(client, &query_id, elapsed).await?;
        } else if status.is_failed() {
            let job =
                client
                    .get_query(&query_id)
                    .map_err(|e| crate::error::Error::InvalidResponse {
                        message: e.to_string(),
                    })?;
            let info = job.info().await.ok();
            if let Some(info) = info
                && let Some(err) = info.error
            {
                return Err(crate::error::Error::InvalidResponse {
                    message: format!("query failed: {}", err.message),
                });
            }
            return Err(crate::error::Error::InvalidResponse {
                message: "query failed".to_string(),
            });
        } else if status.is_cancelled() {
            println!("Query was cancelled.");
        }
    }

    Ok(())
}

async fn run_query_repl(client: &Arc<Client>) -> Result<()> {
    println!("Welcome to the Spice.ai async query REPL.");
    println!("Type SQL to submit a query, or .help for commands.");
    println!();

    let config = Config::builder().build();
    let mut rl: Editor<(), _> =
        Editor::with_config(config).map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("Failed to initialize REPL: {e}"),
        })?;

    // Load history
    let history_path = dirs::home_dir().map(|h| h.join(".spice").join("query_history.txt"));
    if let Some(ref path) = history_path {
        let _ = rl.load_history(path);
    }

    let mut tracked_queries: HashMap<String, TrackedQuery> = HashMap::new();

    loop {
        let input = match read_query_input(&mut rl) {
            Ok(Some(input)) => input,
            Ok(None) => continue,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                println!();
                break;
            }
            Err(e) => {
                println!("\x1b[31mError:\x1b[0m {e}");
                continue;
            }
        };

        let input = input.trim();
        if input.is_empty() {
            continue;
        }

        // Handle special commands
        if input.starts_with('.') {
            if !handle_special_command(client, input, &mut tracked_queries).await {
                break;
            }
            continue;
        }

        // Handle regular exit commands
        let lower_input = input.to_lowercase();
        if matches!(lower_input.as_str(), "exit" | "quit" | "q") {
            break;
        }

        if lower_input == "help" {
            print_query_help();
            continue;
        }

        // Add to history
        let _ = rl.add_history_entry(input);

        // Submit query
        let job = match client.query(input).await {
            Ok(j) => j,
            Err(e) => {
                println!("\x1b[31mError:\x1b[0m {e}");
                continue;
            }
        };

        let query_id = job.id().to_string();

        // Get initial status
        let initial_state = match job.status().await {
            Ok(s) => s.to_string(),
            Err(_) => "PENDING".to_string(),
        };

        // Track the query
        tracked_queries.insert(
            query_id.clone(),
            TrackedQuery {
                query_id: query_id.clone(),
                sql: input.to_string(),
                status: initial_state.clone(),
                submitted_at: Instant::now(),
            },
        );

        println!("Submitted query: {query_id} ({initial_state})");
        println!("Press Ctrl+C to stop waiting (query continues in background)");

        // Poll for completion
        let (final_status, was_cancelled, elapsed) =
            poll_for_completion(client, &query_id, None).await;

        if was_cancelled {
            println!("\nStopped waiting. Check status with: .status {query_id}");
            println!("Wait for completion with: .wait {query_id}");
            continue;
        }

        // Update tracked query
        if let Some(tracked) = tracked_queries.get_mut(&query_id)
            && let Some(ref status) = final_status
        {
            tracked.status = status.to_string();
        }

        // Handle final status
        if let Some(status) = final_status {
            if status.is_success() {
                if let Err(e) = display_results(client, &query_id, elapsed).await {
                    println!("\x1b[31mError displaying results:\x1b[0m {e}");
                }
            } else if status.is_failed() {
                println!("\x1b[31m✗ FAILED\x1b[0m");
                let job = client.get_query(&query_id).ok();
                if let Some(job) = job
                    && let Ok(info) = job.info().await
                    && let Some(err) = info.error
                {
                    println!("Error: {}", err.message);
                }
            } else if status.is_cancelled() {
                println!("\x1b[33m⊘ CANCELLED\x1b[0m");
            } else {
                println!("Query ended with status: {status}");
            }
        }
    }

    // Save history
    if let Some(ref path) = history_path {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = rl.save_history(path);
    }

    Ok(())
}

fn read_query_input(
    rl: &mut Editor<(), rustyline::history::DefaultHistory>,
) -> rustyline::Result<Option<String>> {
    let mut query = String::new();
    let mut first_line = true;

    loop {
        let prompt = if first_line { "query> " } else { "     > " };
        let line = rl.readline(prompt)?;

        if !query.is_empty() {
            query.push('\n');
        }
        query.push_str(&line);

        let trimmed = query.trim();
        let lower = trimmed.to_lowercase();

        // Check for special commands on first line
        if first_line
            && (lower == "help"
                || lower == "exit"
                || lower == "quit"
                || lower == "q"
                || trimmed.starts_with('.'))
        {
            break;
        }

        // Check if query ends with semicolon
        if trimmed.ends_with(';') {
            break;
        }

        first_line = false;
    }

    let result = query.trim().to_string();
    if result.is_empty() {
        Ok(None)
    } else {
        Ok(Some(result))
    }
}

async fn handle_special_command(
    client: &Arc<Client>,
    cmd: &str,
    tracked_queries: &mut HashMap<String, TrackedQuery>,
) -> bool {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return true;
    }

    let command = parts[0].to_lowercase();
    let args = &parts[1..];

    match command.as_str() {
        ".exit" | ".quit" | ".q" => return false,

        ".help" => print_query_help(),

        ".list" => {
            if tracked_queries.is_empty() {
                println!("No tracked queries. Submit a query to start tracking.");
                return true;
            }

            let mut table = TableOutput::new(vec!["QUERY ID", "STATUS", "SUBMITTED", "SQL"]);
            for q in tracked_queries.values() {
                let ago = format_duration_ago(q.submitted_at.elapsed());
                let sql = if q.sql.len() > 37 {
                    format!("{}...", &q.sql[..37])
                } else {
                    q.sql.clone()
                };
                table.add_row(vec![q.query_id.clone(), q.status.clone(), ago, sql]);
            }
            table.print();
        }

        ".status" => {
            if args.is_empty() {
                println!("Usage: .status <query_id>");
                return true;
            }
            let query_id = resolve_query_id(args[0], tracked_queries);
            if query_id.is_empty() {
                return true;
            }
            match client.get_query(&query_id) {
                Ok(job) => match job.info().await {
                    Ok(info) => display_query_info(&info),
                    Err(e) => println!("\x1b[31mError:\x1b[0m {e}"),
                },
                Err(e) => println!("\x1b[31mError:\x1b[0m {e}"),
            }
        }

        ".results" => {
            if args.is_empty() {
                println!("Usage: .results <query_id>");
                return true;
            }
            let query_id = resolve_query_id(args[0], tracked_queries);
            if query_id.is_empty() {
                return true;
            }
            if let Err(e) = display_results(client, &query_id, Duration::ZERO).await {
                println!("\x1b[31mError:\x1b[0m {e}");
            }
        }

        ".wait" => {
            if args.is_empty() {
                println!("Usage: .wait <query_id>");
                return true;
            }
            let query_id = resolve_query_id(args[0], tracked_queries);
            if query_id.is_empty() {
                return true;
            }
            wait_for_query(client, &query_id, tracked_queries).await;
        }

        ".cancel" => {
            if args.is_empty() {
                println!("Usage: .cancel <query_id>");
                return true;
            }
            let query_id = resolve_query_id(args[0], tracked_queries);
            if query_id.is_empty() {
                return true;
            }
            match client.cancel_query(&query_id).await {
                Ok(info) => {
                    println!(
                        "Query {} cancelled (status: {})",
                        info.query_id, info.status
                    );
                    if let Some(tracked) = tracked_queries.get_mut(&query_id) {
                        tracked.status = info.status.to_string();
                    }
                }
                Err(e) => println!("\x1b[31mError:\x1b[0m {e}"),
            }
        }

        ".clear" => {
            if !args.is_empty() && args[0].eq_ignore_ascii_case("history") {
                println!("Query history cleared.");
            } else {
                tracked_queries.clear();
                println!("Tracked queries cleared.");
            }
        }

        _ => println!("Unknown command: {command}. Type .help for available commands."),
    }

    true
}

fn resolve_query_id(partial: &str, tracked_queries: &HashMap<String, TrackedQuery>) -> String {
    // Try exact match
    if tracked_queries.contains_key(partial) {
        return partial.to_string();
    }

    // Try partial match
    let matches: Vec<&str> = tracked_queries
        .keys()
        .filter(|id| id.starts_with(partial) || id.contains(partial))
        .map(String::as_str)
        .collect();

    if matches.len() == 1 {
        return matches[0].to_string();
    } else if matches.len() > 1 {
        println!(
            "Multiple queries match '{}': {}. Please be more specific.",
            partial,
            matches.join(", ")
        );
        return String::new();
    }

    // No match in tracked queries, use the ID as-is
    partial.to_string()
}

async fn wait_for_query(
    client: &Arc<Client>,
    query_id: &str,
    tracked_queries: &mut HashMap<String, TrackedQuery>,
) {
    println!("Press Ctrl+C to stop waiting (query continues in background)");

    let (final_status, was_cancelled, elapsed) = poll_for_completion(client, query_id, None).await;

    if was_cancelled {
        println!("\nStopped waiting. Check status with: .status {query_id}");
        return;
    }

    // Update tracked query
    if let Some(tracked) = tracked_queries.get_mut(query_id)
        && let Some(ref status) = final_status
    {
        tracked.status = status.to_string();
    }

    if let Some(status) = final_status {
        if status.is_success() {
            if let Err(e) = display_results(client, query_id, elapsed).await {
                println!("\x1b[31mError displaying results:\x1b[0m {e}");
            }
        } else if status.is_failed() {
            println!("\x1b[31m✗ FAILED\x1b[0m");
            let job = client.get_query(query_id).ok();
            if let Some(job) = job
                && let Ok(info) = job.info().await
                && let Some(err) = info.error
            {
                println!("Error: {}", err.message);
            }
        } else if status.is_cancelled() {
            println!("\x1b[33m⊘ CANCELLED\x1b[0m");
        }
    }
}

async fn poll_for_completion(
    client: &Arc<Client>,
    query_id: &str,
    timeout: Option<Duration>,
) -> (Option<QueryStatus>, bool, Duration) {
    let start_time = Instant::now();
    let mut spinner_idx = 0;
    let mut interval = tokio::time::interval(POLL_INTERVAL);

    // Set up Ctrl+C handler
    let (cancel_tx, mut cancel_rx) = mpsc::channel::<()>(1);
    let _guard = CtrlCGuard::new(cancel_tx);

    loop {
        select! {
            _ = cancel_rx.recv() => {
                // Clear spinner line
                print!("\r\x1b[K");
                let _ = std::io::stdout().flush();
                return (None, true, start_time.elapsed());
            }
            _ = interval.tick() => {
                // Check timeout
                if let Some(t) = timeout
                    && start_time.elapsed() > t
                {
                    print!("\r\x1b[K");
                    let _ = std::io::stdout().flush();
                    println!("Timeout exceeded");
                    return (None, true, start_time.elapsed());
                }

                if let Ok(job) = client.get_query(query_id)
                    && let Ok(status) = job.status().await {
                        let elapsed = start_time.elapsed();

                        if status.is_terminal() {
                            // Clear spinner and show final status
                            print!("\r\x1b[K");
                            let _ = std::io::stdout().flush();
                            if status.is_success() {
                                println!("\x1b[32m✓ SUCCEEDED\x1b[0m ({:.1}s)", elapsed.as_secs_f64());
                            }
                            return (Some(status), false, elapsed);
                        }

                        // Update spinner
                        let frame = SPINNER_FRAMES[spinner_idx % SPINNER_FRAMES.len()];
                        spinner_idx += 1;
                        print!("\r{} {} ({:.1}s)...", frame, status, elapsed.as_secs_f64());
                        let _ = std::io::stdout().flush();
                    }
                // Continue polling on transient errors
            }
        }
    }
}

/// Guard that sends a signal when Ctrl+C is pressed.
struct CtrlCGuard {
    _handle: Option<tokio::task::JoinHandle<()>>,
}

impl CtrlCGuard {
    fn new(tx: mpsc::Sender<()>) -> Self {
        let handle = tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            let _ = tx.send(()).await;
        });
        Self {
            _handle: Some(handle),
        }
    }
}

async fn display_results(client: &Arc<Client>, query_id: &str, elapsed: Duration) -> Result<()> {
    // First get query info to check status
    let job = client
        .get_query(query_id)
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: e.to_string(),
        })?;

    let info = job
        .info()
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: e.to_string(),
        })?;

    if !info.status.is_success() {
        return Err(crate::error::Error::InvalidResponse {
            message: format!(
                "query '{}' is still {}. Use .wait {} to wait for completion",
                query_id, info.status, query_id
            ),
        });
    }

    // Fetch results as Arrow RecordBatches
    let batches = job
        .results()
        .await
        .map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("getting results: {e}"),
        })?;

    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();

    if total_rows == 0 {
        if elapsed > Duration::ZERO {
            println!("Time: {:.8} seconds. 0 rows.", elapsed.as_secs_f64());
        } else {
            println!("No results.");
        }
        return Ok(());
    }

    // Use Arrow's pretty formatting to display results
    let formatted =
        pretty_format_batches(&batches).map_err(|e| crate::error::Error::InvalidResponse {
            message: format!("formatting results: {e}"),
        })?;

    println!("{formatted}");

    // Show timing and row count
    if elapsed > Duration::ZERO {
        println!(
            "\nTime: {:.8} seconds. {} rows.",
            elapsed.as_secs_f64(),
            total_rows
        );
    } else {
        println!("\n{total_rows} row(s)");
    }

    Ok(())
}

fn display_query_info(info: &QueryInfo) {
    println!("Query ID:    {}", info.query_id);
    println!("Status:      {}", info.status);
    if let Some(ref err) = info.error {
        println!("Error:       {}", err.message);
    }
    if let Some(ref result) = info.result {
        println!("Rows:        {}", result.total_rows);
        println!("Chunks:      {}", result.total_chunks);
    }
}

fn format_duration_ago(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else {
        format!("{}h ago", secs / 3600)
    }
}

fn print_query_help() {
    println!("Available commands:");
    println!();
    println!("  SQL statements    - Submit a query (end with semicolon)");
    println!();
    println!("Special commands:");
    println!("  .list             - List all tracked queries");
    println!("  .status <id>      - Show detailed status of a specific query");
    println!("  .results <id>     - Fetch and display results of a completed query");
    println!("  .wait <id>        - Resume waiting for a query to complete");
    println!("  .cancel <id>      - Cancel a running query");
    println!("  .clear            - Clear tracked queries from local list");
    println!("  .clear history    - Clear command history");
    println!("  .help             - Show this help message");
    println!("  .exit, .quit, .q  - Exit the REPL");
    println!();
    println!("Tips:");
    println!("  - Partial query IDs work if they uniquely identify a query");
    println!("  - Press Ctrl+C while waiting to stop (query continues in background)");
    println!("  - Press Ctrl+D or type .exit to quit");
    println!();
}
