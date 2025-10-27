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

use std::borrow::Cow;
use std::error::Error;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use ansi_term::Colour;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    FlightDescriptor, decode::FlightRecordBatchStream, error::FlightError,
    flight_service_client::FlightServiceClient,
};

use crate::completer::SchemaCache;
use clap::Parser;
use config::get_user_agent;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use flight_client::{MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE, TonicStatusError};
use futures::{StreamExt, TryStreamExt};
use llms::chat::LlmRuntime;
use prost::Message;
use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::{
    CompletionType, ConditionalEventHandler, Config, Helper, Hinter, KeyEvent, Validator,
};
use rustyline::{Editor, EventHandler};
use serde_json::json;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, AsciiMetadataKey, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, IntoRequest, Status};

pub mod cache_control;
mod completer;
mod config;

#[derive(Parser, Debug)]
#[clap(about = "Spice.ai SQL REPL")]
pub struct ReplConfig {
    #[arg(
        long,
        value_name = "FLIGHT_ENDPOINT",
        default_value = "http://localhost:50051",
        help_heading = "SQL REPL"
    )]
    pub repl_flight_endpoint: String,

    #[arg(
        long,
        value_name = "HTTP_ENDPOINT",
        default_value = "http://localhost:8090",
        help_heading = "SQL REPL"
    )]
    pub http_endpoint: String,

    /// The path to the root certificate file used to verify the Spice.ai runtime server certificate
    #[arg(
        long,
        value_name = "TLS_ROOT_CERTIFICATE_FILE",
        help_heading = "SQL REPL"
    )]
    pub tls_root_certificate_file: Option<String>,

    /// The API key to use for authentication
    #[arg(long, value_name = "API_KEY", help_heading = "SQL REPL")]
    pub api_key: Option<String>,

    #[arg(long, value_name = "USER_AGENT", help_heading = "SQL REPL")]
    pub user_agent: Option<String>,

    /// Control whether the results cache is used for queries.
    #[arg(
        long,
        value_enum,
        default_value_t = cache_control::CacheControl::Cache,
        value_name = "CACHE_CONTROL",
        help_heading = "SQL REPL"
    )]
    pub cache_control: cache_control::CacheControl,
}

const NQL_LINE_PREFIX: &str = "nql ";

async fn send_nsql_request(
    client: &Client,
    base_url: String,
    query: String,
    runtime: LlmRuntime,
    user_agent: &str,
) -> Result<String, reqwest::Error> {
    client
        .post(format!("{base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .header("User-Agent", user_agent)
        .json(&json!({
            "query": query,
            "model": runtime,
        }))
        .send()
        .await?
        .text()
        .await
}

const SPECIAL_COMMANDS: [&str; 6] = [".exit", "exit", "quit", "q", ".error", "help"];
const PROMPT_COLOR: Colour = Colour::Fixed(8);

#[derive(Clone)]
struct KeyEventHandler;

impl ConditionalEventHandler for KeyEventHandler {
    fn handle(
        &self,
        evt: &rustyline::Event,
        _n: rustyline::RepeatCount,
        _positive: bool,
        ctx: &rustyline::EventContext,
    ) -> Option<rustyline::Cmd> {
        evt.get(0).and_then(|k| {
            if *k == KeyEvent::ctrl('C') {
                Some(if ctx.line().is_empty() {
                    rustyline::Cmd::EndOfFile
                } else {
                    rustyline::Cmd::Interrupt
                })
            } else {
                None
            }
        })
    }
}

#[derive(Helper, Hinter, Validator)]
struct EditorHelper {
    schema_cache: Arc<RwLock<SchemaCache>>,
    flight_client: Option<FlightServiceClient<Channel>>,
    api_key: Option<String>,
    user_agent: String,
    refresh_task_handle: Option<JoinHandle<()>>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl EditorHelper {
    pub fn new(
        flight_client: Option<FlightServiceClient<Channel>>,
        api_key: Option<String>,
        user_agent: String,
    ) -> Self {
        Self {
            schema_cache: Arc::new(RwLock::new(SchemaCache::new())),
            flight_client,
            api_key,
            user_agent,
            refresh_task_handle: None,
            shutdown_sender: None,
        }
    }
}

impl Drop for EditorHelper {
    fn drop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        if let Some(handle) = self.refresh_task_handle.take() {
            handle.abort();
        }
    }
}

impl Highlighter for EditorHelper {
    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            PROMPT_COLOR.paint(prompt).to_string().into()
        } else {
            Cow::Borrowed(prompt)
        }
    }
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::missing_errors_doc)]
pub async fn run(repl_config: ReplConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut repl_flight_endpoint = repl_config.repl_flight_endpoint;
    let mut user_agent = get_user_agent();
    if let Some(user_agent_override) = repl_config.user_agent {
        // Prepend the user agent with the Spice.ai user agent
        let mut new_agent = user_agent_override;
        new_agent.push(' ');
        new_agent.push_str(&user_agent);
        user_agent = new_agent;
    }
    let channel = if let Some(tls_root_certificate_file) = repl_config.tls_root_certificate_file {
        let tls_root_certificate = tokio::fs::read(&tls_root_certificate_file)
            .await
            .map_err(|e| {
                format!("Failed to read TLS root certificate from '{tls_root_certificate_file}': {e}. Verify the file path and permissions.")
            })?;
        let tls_root_certificate = tonic::transport::Certificate::from_pem(tls_root_certificate);
        let client_tls_config = ClientTlsConfig::new().ca_certificate(tls_root_certificate);
        if repl_flight_endpoint == "http://localhost:50051" {
            repl_flight_endpoint = "https://localhost:50051".to_string();
        }
        Channel::from_shared(repl_flight_endpoint.clone())?
            .user_agent(user_agent.clone())?
            .tls_config(client_tls_config)?
            .connect()
            .await
    } else {
        Channel::from_shared(repl_flight_endpoint.clone())?
            .user_agent(user_agent.clone())?
            .connect()
            .await
    };

    // Set up the Flight client
    let channel = channel.map_err(|e| {
        Box::<dyn Error>::from(format!(
            "Connection failed to spiced at '{repl_flight_endpoint}': {e}. Check if the Spice runtime is running, endpoint including port is correct, and TLS config (if used) is valid."
        ))
    })?;

    // The encoder/decoder size is limited to 500MB.
    let client = FlightServiceClient::new(channel)
        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

    #[cfg(target_os = "windows")]
    // Ensure ANSI support on Windows is enabled for proper color display.
    let _ = ansi_term::enable_ansi_support();

    let config = Config::builder()
        .completion_type(CompletionType::List)
        .completion_show_all_if_ambiguous(true)
        .build();

    let mut rl = Editor::with_config(config)?;

    rl.set_helper(Some(EditorHelper::new(
        Some(client.clone()),
        repl_config.api_key.clone(),
        user_agent.to_string(),
    )));
    if let Some(helper) = rl.helper_mut() {
        helper.start_refreshing(300);
    }

    let key_handler = Box::new(KeyEventHandler {});
    rl.bind_sequence(KeyEvent::ctrl('C'), EventHandler::Conditional(key_handler));
    rl.bind_sequence(KeyEvent::ctrl('D'), rustyline::Cmd::EndOfFile);

    println!("Welcome to the Spice.ai SQL REPL! Type 'help' for help.\n");
    println!("show tables; -- list available tables");

    let mut last_error: Option<Status> = None;

    'outer: loop {
        let mut first_line = true;
        // When using the Editor, prompt coloring is applied automatically by the Highlighter. Manual colorizing for
        // the prompt should not be used, as it does not work on Windows: https://github.com/kkawakam/rustyline/issues/836
        let mut prompt = "sql> ".to_string();
        let mut line = String::new();
        loop {
            let line_result = rl.readline(&prompt);
            let newline = match line_result {
                Ok(line) => line,
                Err(ReadlineError::Interrupted) => {
                    // User canceled the current query
                    continue 'outer;
                }
                Err(ReadlineError::Eof) => {
                    if line.is_empty() {
                        break 'outer;
                    }

                    continue 'outer;
                }
                Err(err) => {
                    println!("{} Input read error: {err}", Colour::Red.paint("Error:"));
                    continue 'outer;
                }
            };

            line.push_str(format!("{newline}\n").as_str());

            if SPECIAL_COMMANDS.contains(&line.to_ascii_lowercase().trim())
                || line.trim().ends_with(';')
            {
                line = line.trim().to_string();
                break;
            }

            if first_line {
                prompt = "  -> ".to_string();
                first_line = false;
            }
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let line = match line {
            ".exit" | "exit" | "quit" | "q" => break,
            ".error" => {
                match last_error {
                    Some(ref err) => {
                        let err = TonicStatusError::from(err.clone());
                        println!("{err}");
                    }
                    None => println!("No previous error recorded."),
                }
                continue;
            }
            "help" => {
                println!("Available commands:\n");
                println!(
                    "{} Exit the REPL",
                    PROMPT_COLOR.paint(".exit, exit, quit, q:")
                );
                println!(
                    "{} Show details of the last error",
                    PROMPT_COLOR.paint(".error:")
                );
                println!("{} Show this help message", PROMPT_COLOR.paint("help:"));
                println!("\nOther lines will be interpreted as SQL");
                continue;
            }
            "show tables" | "show tables;" => {
                "select table_catalog, table_schema, table_name, table_type from information_schema.tables where table_schema != 'information_schema';"
            }
            line if line.to_lowercase().starts_with(NQL_LINE_PREFIX) => {
                let _ = rl.add_history_entry(line);
                if let Err(e) = get_and_display_nql_records(
                    repl_config.http_endpoint.clone(),
                    line.strip_prefix(NQL_LINE_PREFIX)
                        .unwrap_or(line)
                        .to_string(),
                    &user_agent,
                )
                .await
                {
                    println!(
                        "{} NQL processing failed: {e}. Use '.error' if applicable.",
                        Colour::Red.paint("Error:")
                    );
                }
                continue;
            }
            _ => line,
        };

        let _ = rl.add_history_entry(line);

        let start_time = Instant::now();
        match get_records(
            client.clone(),
            line,
            repl_config.api_key.as_ref(),
            &user_agent,
            repl_config.cache_control,
        )
        .await
        {
            Ok((records, total_rows, from_cache)) => {
                display_records(&records, start_time, total_rows, from_cache)?;
            }
            Err(FlightError::Tonic(status)) => {
                display_grpc_error(&status);
                last_error = Some(*status);
            }
            Err(e) => {
                println!(
                    "{} Unexpected Flight error: {e}. Check connection or query syntax.",
                    Colour::Red.paint("Error:")
                );
            }
        }
    }

    if let Some(helper) = rl.helper_mut() {
        helper.stop_refreshing();
    }

    Ok(())
}

/// Send a SQL query to the Flight service and return the resulting record batches.
///
/// # Errors
///
/// Returns an error if the Flight service returns an error.
pub async fn get_records(
    mut client: FlightServiceClient<Channel>,
    line: &str,
    api_key: Option<&String>,
    user_agent: &str,
    cache_control: cache_control::CacheControl,
) -> Result<(Vec<RecordBatch>, usize, bool), FlightError> {
    let sql_command = CommandStatementQuery {
        query: line.to_string(),
        transaction_id: None,
    };
    let sql_command_bytes = sql_command.as_any().encode_to_vec();

    let request = FlightDescriptor::new_cmd(sql_command_bytes).into_request();
    let request = add_api_key(request, api_key)?;

    let mut flight_info = client.get_flight_info(request).await?.into_inner();
    let Some(endpoint) = flight_info.endpoint.pop() else {
        return Err(FlightError::Tonic(Box::new(Status::internal(
            "No endpoint returned from server. Verify server configuration.",
        ))));
    };
    let Some(ticket) = endpoint.ticket else {
        return Err(FlightError::Tonic(Box::new(Status::internal(
            "No ticket in endpoint. Server may be misconfigured.",
        ))));
    };
    let mut request = ticket.into_request();
    request = add_api_key(request, api_key)?;

    if cache_control == cache_control::CacheControl::NoCache {
        request
            .metadata_mut()
            .insert("cache-control", MetadataValue::from_static("no-cache"));
    }

    let user_agent_key = AsciiMetadataKey::from_str("User-Agent")
        .map_err(|e| FlightError::ExternalError(e.into()))?;
    let user_agent_value = user_agent
        .parse()
        .map_err(|e: InvalidMetadataValue| FlightError::ExternalError(e.into()))?;

    request
        .metadata_mut()
        .insert(user_agent_key, user_agent_value);

    let response = client.do_get(request).await?;
    let from_cache = response
        .metadata()
        .get("results-cache-status")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|s| s.to_lowercase().starts_with("hit"));

    let stream = response.into_inner();

    let mut stream = FlightRecordBatchStream::new_from_flight_data(
        stream.map_err(|status| FlightError::Tonic(Box::new(status))),
    );
    let mut records = vec![];
    let mut total_rows = 0_usize;
    while let Some(data) = stream.next().await {
        match data {
            Ok(data) => {
                total_rows += data.num_rows();
                records.push(data);
            }
            Err(e) => return Err(e),
        }
    }

    Ok((records, total_rows, from_cache))
}

fn add_api_key<T>(
    mut request: tonic::Request<T>,
    api_key: Option<&String>,
) -> Result<tonic::Request<T>, FlightError> {
    if let Some(api_key) = api_key {
        let val: MetadataValue<Ascii> = format!("Bearer {api_key}")
            .parse()
            .map_err(|e: InvalidMetadataValue| FlightError::ExternalError(Box::new(e)))?;
        request.metadata_mut().insert("authorization", val);
    }
    Ok(request)
}

/// Display a set of record batches to the user. This function will display the first 500 rows.
///
/// # Errors
///
/// Returns an error if the record batches cannot be loaded into Datafusion.
fn display_records(
    records: &[RecordBatch],
    start_time: Instant,
    total_rows: usize,
    from_cache: bool,
) -> Result<impl Display, Box<dyn std::error::Error>> {
    let mut limited_records = Vec::new();
    let mut rows_collected = 0;

    let elapsed = start_time.elapsed();

    for batch in records {
        if rows_collected >= 500 {
            break;
        }

        let rows_to_take = (500 - rows_collected).min(batch.num_rows());
        if rows_to_take > 0 {
            limited_records.push(batch.slice(0, rows_to_take));
            rows_collected += rows_to_take;
        }
    }

    let pretty_batches = match pretty_format_batches(&limited_records) {
        Ok(pretty) => pretty,
        Err(e) => {
            println!(
                "{} Failed to format results: {e}",
                Colour::Red.paint("Display Error:")
            );
            return Err(Box::new(e));
        }
    };

    if total_rows > 0 {
        println!("{pretty_batches}");
    } else {
        println!("No results.");
    }

    if rows_collected == total_rows {
        if total_rows == 0 {
            println!(
                "\nTime: {} seconds{}.",
                elapsed.as_secs_f64(),
                if from_cache { " (cached)" } else { "" }
            );
        } else {
            println!(
                "\nTime: {} seconds. {rows_collected} rows{}.",
                elapsed.as_secs_f64(),
                if from_cache { " (cached)" } else { "" }
            );
        }
    } else {
        println!(
            "\nTime: {} seconds. {rows_collected}/{total_rows} rows displayed{}.",
            elapsed.as_secs_f64(),
            if from_cache { " (cached)" } else { "" }
        );
    }
    Ok(pretty_batches)
}

/// Use the `POST v1/nsql` HTTP endpoint to send an NSQL query and display the resulting records.
async fn get_and_display_nql_records(
    endpoint: String,
    query: String,
    user_agent: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    let resp = send_nsql_request(
        &Client::new(),
        endpoint,
        query,
        LlmRuntime::Openai,
        user_agent,
    )
    .await
    .map_err(|e| {
        format!("Network error during NQL request: {e}. Check HTTP endpoint and network.")
    })?;

    let jsonl_resp = json_array_to_jsonl(&resp).map_err(|e| {
        format!("Failed to convert NQL response to JSONL: {e}. Response may be malformed.")
    })?;

    let (schema, _) =
        arrow_json::reader::infer_json_schema(jsonl_resp.as_bytes(), None).map_err(|e| {
            format!("Schema inference failed for NQL results: {e}. Ensure response is valid JSON.")
        })?;

    let records: Vec<RecordBatch> = arrow_json::ReaderBuilder::new(Arc::new(schema))
        .build(jsonl_resp.as_bytes())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to read NQL records into Arrow format: {e}."))?;

    let total_rows = records
        .iter()
        .map(RecordBatch::num_rows)
        .reduce(|x, y| x + y)
        .unwrap_or(0) as usize;

    display_records(&records, start_time, total_rows, false)?;

    Ok(())
}

/// Convert a JSON array string to a JSONL string.
fn json_array_to_jsonl(json_array_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    let json_array: Vec<serde_json::Value> = serde_json::from_str(json_array_str)
        .map_err(|e| format!("Invalid JSON array in response: {e}"))?;

    let jsonl_strings: Vec<String> = json_array
        .into_iter()
        .map(|item| serde_json::to_string(&item))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("Failed to serialize JSON item: {e}"))?;

    let jsonl_str = jsonl_strings.join("\n");

    Ok(jsonl_str)
}

/// Returns a boolean indicating if a message needs truncation, from a given input of lines.
fn lines_need_truncation(lines: &[&str]) -> bool {
    lines.iter().any(|line| line.len() > 120)
}

fn display_grpc_error(err: &Status) {
    let (error_type, user_err_msg) = match err.code() {
        Code::Ok => return,
        Code::Internal => (
            "Internal Error",
            "Unexpected internal error. Use '.error' for details.".to_string(),
        ),
        Code::Unknown | Code::DataLoss | Code::FailedPrecondition => (
            "Error",
            "Unexpected error. Use '.error' for details.".to_string(),
        ),
        Code::InvalidArgument | Code::AlreadyExists | Code::NotFound | Code::Unavailable => {
            let message = err.message();
            let lines = message.split('\n').collect::<Vec<_>>();
            let truncate = lines_need_truncation(&lines);

            let first_line = lines.first().unwrap_or(&message);
            let user_err_msg = match (truncate, lines.len() > 1) {
                // truncating due to length, and multiple error lines
                (true, true) => format!(
                    "{first_line}\nMessage truncated due to length. Run '.error' for full details."
                ),
                // truncating due to length, but only one line
                (true, false) => {
                    "Query failed. Message truncated; run '.error' for full details.".to_string()
                }
                _ => message.to_string(),
            };
            ("Query Error", user_err_msg)
        }
        Code::Cancelled => (
            "Operation Cancelled",
            "Request cancelled. Retry if needed.".to_string(),
        ),
        Code::Aborted => (
            "Operation Aborted",
            "Request aborted before completion. Check logs or retry.".to_string(),
        ),
        Code::DeadlineExceeded => (
            "Timeout",
            "Query exceeded time limit. Optimize query or increase timeout if configurable."
                .to_string(),
        ),
        Code::Unauthenticated => (
            "Authentication Failed",
            "Invalid credentials. Verify credentials and try again.".to_string(),
        ),
        Code::PermissionDenied => (
            "Permission Denied",
            "Insufficient permissions. Check authorization scopes or account access.".to_string(),
        ),
        Code::ResourceExhausted => (
            "Resource Exhausted",
            "Server resources exhausted. Reduce query complexity or try later.".to_string(),
        ),
        Code::Unimplemented => (
            "Unsupported Operation",
            "Feature not implemented. Check documentation for alternatives.".to_string(),
        ),
        Code::OutOfRange => (
            "Result Limit Exceeded",
            "Results too large. Consider adding a LIMIT clause to the query.".to_string(),
        ),
    };

    println!(
        "{} {user_err_msg}",
        Colour::Red.paint(format!("{error_type}:"))
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(num_rows: usize, batch_id: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(
            (0..num_rows)
                .map(|i| {
                    batch_id * 1000 + i32::try_from(i).expect("Failed to convert usize to i32")
                })
                .collect::<Vec<_>>(),
        );
        let name_array = StringArray::from(
            (0..num_rows)
                .map(|i| {
                    format!(
                        "name_{}",
                        batch_id * 1000 + i32::try_from(i).expect("Failed to convert usize to i32")
                    )
                })
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .expect("Failed to create RecordBatch")
    }

    #[test]
    fn test_display_records() {
        let test_cases = vec![
            (
                vec![
                    create_test_batch(100, 1),
                    create_test_batch(100, 2),
                    create_test_batch(100, 3),
                ],
                300,
                "multiple_batches_under_500_rows",
            ),
            (
                vec![
                    create_test_batch(200, 1),
                    create_test_batch(200, 2),
                    create_test_batch(200, 3),
                ],
                600,
                "multiple_batches_over_500_rows",
            ),
            (
                vec![create_test_batch(250, 1), create_test_batch(250, 2)],
                500,
                "multiple_batches_exactly_500_rows",
            ),
            (
                vec![create_test_batch(700, 1)],
                700,
                "single_batch_over_500_rows",
            ),
            (vec![], 0, "single_empty_batch"),
        ];

        for (records, total_rows, test_name) in test_cases {
            run_single_test_display_records(&records, total_rows, test_name);
        }
    }

    fn run_single_test_display_records(
        records: &[RecordBatch],
        total_rows: usize,
        test_name: &str,
    ) {
        let start_time = Instant::now();
        let from_cache = false;

        let result = display_records(records, start_time, total_rows, from_cache)
            .expect("Failed to display records");

        insta::assert_snapshot!(test_name, result);
    }
}
