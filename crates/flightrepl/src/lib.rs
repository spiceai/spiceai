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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use ansi_term::Colour;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    decode::FlightRecordBatchStream, error::FlightError,
    flight_service_client::FlightServiceClient, FlightDescriptor,
};

use clap::Parser;
use config::get_user_agent;
use datafusion::arrow::array::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use flight_client::{TonicStatusError, MAX_DECODING_MESSAGE_SIZE, MAX_ENCODING_MESSAGE_SIZE};
use futures::{StreamExt, TryStreamExt};
use llms::chat::LlmRuntime;
use prost::Message;
use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::history::FileHistory;
use rustyline::{Completer, ConditionalEventHandler, Helper, Hinter, KeyEvent, Validator};
use rustyline::{Editor, EventHandler, Modifiers};
use serde_json::json;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, AsciiMetadataKey, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, IntoRequest, Status};

pub mod cache_control;
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

#[derive(Completer, Helper, Hinter, Validator)]
struct EditorHelper;

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
        let tls_root_certificate = std::fs::read(tls_root_certificate_file)?;
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
    let channel = channel.map_err(|_err| {
        Box::<dyn Error>::from(format!(
            "Unable to connect to spiced at {repl_flight_endpoint}. Is it running?"
        ))
    })?;

    // The encoder/decoder size is limited to 500MB.
    let client = FlightServiceClient::new(channel)
        .max_encoding_message_size(MAX_ENCODING_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);

    #[cfg(target_os = "windows")]
    // Ensure ANSI support on Windows is enabled for proper color display.
    let _ = ansi_term::enable_ansi_support();

    let mut rl = Editor::<EditorHelper, FileHistory>::new()?;
    rl.set_helper(Some(EditorHelper));

    let key_handler = Box::new(KeyEventHandler {});
    rl.bind_sequence(KeyEvent::ctrl('C'), EventHandler::Conditional(key_handler));
    rl.bind_sequence(KeyEvent::ctrl('D'), rustyline::Cmd::EndOfFile);
    rl.bind_sequence(
        KeyEvent::new('\t', Modifiers::NONE),
        rustyline::Cmd::Insert(1, "\t".to_string()),
    );
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
                    println!("Error reading line: {err}");
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
                    },
                    None => println!("No error to display"),
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
                get_and_display_nql_records(
                    repl_config.http_endpoint.clone(),
                     line.strip_prefix(NQL_LINE_PREFIX).unwrap_or(line).to_string(),
                    &user_agent
                ).await.map_err(|e| format!("Error occured on NQL request: {e}"))?;
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
            Ok((_, 0, from_cache)) => {
                println!("No results{}.", if from_cache { " (cached)" } else { "" });
            }
            Ok((records, total_rows, from_cache)) => {
                display_records(records, start_time, total_rows, from_cache).await?;
            }
            Err(FlightError::Tonic(status)) => {
                display_grpc_error(&status);
                last_error = Some(status);
                continue;
            }
            Err(e) => {
                println!(
                    "Unexpected Flight Error {}",
                    Colour::Red.paint(e.to_string())
                );
            }
        }
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

    let request = add_api_key(
        FlightDescriptor::new_cmd(sql_command_bytes).into_request(),
        api_key,
    );

    let mut flight_info = client.get_flight_info(request).await?.into_inner();
    let Some(endpoint) = flight_info.endpoint.pop() else {
        return Err(FlightError::Tonic(Status::internal("No endpoint")));
    };
    let Some(ticket) = endpoint.ticket else {
        return Err(FlightError::Tonic(Status::internal("No ticket")));
    };
    let mut request = add_api_key(ticket.into_request(), api_key);

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

    let mut stream =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::Tonic));
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

fn add_api_key<T>(mut request: tonic::Request<T>, api_key: Option<&String>) -> tonic::Request<T> {
    if let Some(api_key) = api_key {
        let val: MetadataValue<Ascii> = match format!("Bearer {api_key}").parse() {
            Ok(val) => val,
            Err(e) => panic!("Invalid API key: {e}"),
        };
        request.metadata_mut().insert("authorization", val);
    }
    request
}

/// Display a set of record batches to the user. This function will display the first 500 rows.
///
/// # Errors
///
/// Returns an error if the record batches cannot be loaded into Datafusion.
async fn display_records(
    records: Vec<RecordBatch>,
    start_time: Instant,
    total_rows: usize,
    from_cache: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = records[0].schema();

    let ctx = SessionContext::new();
    let provider = MemTable::try_new(schema, vec![records])?;
    let df = DataFrame::new(
        ctx.state(),
        LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(provider)), None)?
            .limit(0, Some(500))?
            .build()?,
    );

    let num_rows = df.clone().count().await?;

    if let Err(e) = df.show().await {
        println!("Error displaying results: {e}");
    };
    let elapsed = start_time.elapsed();
    if num_rows == total_rows {
        println!(
            "\nTime: {} seconds. {num_rows} rows{}.",
            elapsed.as_secs_f64(),
            if from_cache { " (cached)" } else { "" }
        );
    } else {
        println!(
            "\nTime: {} seconds. {num_rows}/{total_rows} rows displayed{}.",
            elapsed.as_secs_f64(),
            if from_cache { " (cached)" } else { "" }
        );
    }
    Ok(())
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
    .await?;

    let jsonl_resp = json_array_to_jsonl(&resp)?;

    let (schema, _) = arrow_json::reader::infer_json_schema(jsonl_resp.as_bytes(), None)?;

    let records: Vec<RecordBatch> = arrow_json::ReaderBuilder::new(Arc::new(schema))
        .build(jsonl_resp.as_bytes())?
        .collect::<Result<Vec<_>, _>>()?;

    let total_rows = records
        .iter()
        .map(RecordBatch::num_rows)
        .reduce(|x, y| x + y)
        .unwrap_or(0) as usize;

    display_records(records, start_time, total_rows, false).await?;

    Ok(())
}

/// Convert a JSON array string to a JSONL string.
fn json_array_to_jsonl(json_array_str: &str) -> Result<String, Box<dyn std::error::Error>> {
    let json_array: Vec<serde_json::Value> = serde_json::from_str(json_array_str)?;

    let jsonl_strings: Vec<String> = json_array
        .into_iter()
        .map(|item| serde_json::to_string(&item))
        .collect::<Result<Vec<_>, _>>()?;

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
        Code::Unknown | Code::Internal | Code::DataLoss | Code::FailedPrecondition => (
            "Internal Error",
            "An unexpected internal error occurred. Execute '.error' for details.".to_string(),
        ),
        Code::InvalidArgument | Code::AlreadyExists | Code::NotFound | Code::Unavailable => {
            let message = err.message();
            let lines = message.split('\n').collect::<Vec<_>>();
            let truncate = lines_need_truncation(&lines);

            let first_line = lines.first().unwrap_or(&message);
            match (truncate, lines.len() > 1) {
                (true, true) => {
                    // truncating due to length, and multiple error lines
                    ("Query Error", format!("{first_line}\nThis error message has been truncated.\nFor the full error message, execute `.error`."))
                }
                (true, false) => {
                    // truncating due to length, but only one line
                    ("Query Error", "Failed to execute query.\nThis error message has been truncated.\nFor the full error message, execute `.error`.".to_string())
                }
                _ => ("Query Error", message.to_string()),
            }
        }
        Code::Cancelled => (
            "Cancelled",
            "The operation was cancelled before completion.".to_string(),
        ),
        Code::Aborted => (
            "Aborted",
            "The operation was aborted before completion.".to_string(),
        ),
        Code::DeadlineExceeded => (
            "Timeout Error",
            "The operation could not complete within the allowed time limit.".to_string(),
        ),
        Code::Unauthenticated => (
            "Authentication Error",
            "Access denied. Invalid credentials.".to_string(),
        ),
        Code::PermissionDenied => (
            "Authorization Error",
            "Access denied. Insufficient permisions to complete the request.".to_string(),
        ),
        Code::ResourceExhausted => (
            "Resource Limit Exceeded",
            "The operation could not be completed because the server resources are exhausted."
                .to_string(),
        ),
        Code::Unimplemented => (
            "Unsupported Operation",
            "The query could not be completed because the requested operation is not supported."
                .to_string(),
        ),
        Code::OutOfRange => (
            "Result Limit Exceeded",
            "The query result exceeds allowable limits. Consider using a `limit` clause."
                .to_string(),
        ),
    };

    println!(
        "{} {user_err_msg}",
        Colour::Red.paint(format!("{error_type}:"))
    );
}
