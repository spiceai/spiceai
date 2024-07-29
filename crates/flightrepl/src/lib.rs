/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use ansi_term::Colour;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    decode::FlightRecordBatchStream, error::FlightError,
    flight_service_client::FlightServiceClient, FlightDescriptor,
};

use clap::Parser;
use datafusion::arrow::array::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use futures::{StreamExt, TryStreamExt};
use llms::chat::LlmRuntime;
use prost::Message;
use reqwest::Client;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::json;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, IntoRequest, Status};

#[derive(Parser)]
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
}

const NQL_LINE_PREFIX: &str = "nql ";

async fn send_nsql_request(
    client: &Client,
    base_url: String,
    query: String,
    runtime: LlmRuntime,
) -> Result<String, reqwest::Error> {
    client
        .post(format!("{base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .json(&json!({
            "query": query,
            "use": runtime,
        }))
        .send()
        .await?
        .text()
        .await
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::missing_errors_doc)]
pub async fn run(repl_config: ReplConfig) -> Result<(), Box<dyn std::error::Error>> {
    let mut repl_flight_endpoint = repl_config.repl_flight_endpoint;
    let channel = if let Some(tls_root_certificate_file) = repl_config.tls_root_certificate_file {
        let tls_root_certificate = std::fs::read(tls_root_certificate_file)?;
        let tls_root_certificate = tonic::transport::Certificate::from_pem(tls_root_certificate);
        let client_tls_config = ClientTlsConfig::new().ca_certificate(tls_root_certificate);
        if repl_flight_endpoint == "http://localhost:50051" {
            repl_flight_endpoint = "https://localhost:50051".to_string();
        }
        Channel::from_shared(repl_flight_endpoint)?
            .tls_config(client_tls_config)?
            .connect()
            .await
    } else {
        Channel::from_shared(repl_flight_endpoint)?.connect().await
    };

    // Set up the Flight client
    let spice_endpoint = repl_config.http_endpoint.clone();
    let channel = channel.map_err(|_err| {
        Box::<dyn Error>::from(format!(
            "Unable to connect to spiced at {spice_endpoint}. Is it running?"
        ))
    })?;

    // The encoder/decoder size is limited to 500MB.
    let client = FlightServiceClient::new(channel)
        .max_decoding_message_size(500 * 1024 * 1024)
        .max_encoding_message_size(500 * 1024 * 1024);

    let mut rl = DefaultEditor::new()?;

    println!("Welcome to the Spice.ai SQL REPL! Type 'help' for help.\n");
    println!("show tables; -- list available tables");

    let mut last_error: Option<Status> = None;
    let prompt_color = Colour::Fixed(8);
    let prompt = prompt_color.paint("sql> ").to_string();

    loop {
        let line_result = rl.readline(&prompt);
        let line = match line_result {
            Ok(line) => line,
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error reading line: {err}");
                continue;
            }
        };

        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let line = match line {
            ".exit" | "exit" | "quit" | "q" => break,
            ".error" => {
                match last_error {
                    Some(ref err) => println!("{err:?}"),
                    None => println!("No error to display"),
                }
                continue;
            }
            "help" => {
                println!("Available commands:\n");
                println!(
                    "{} Exit the REPL",
                    prompt_color.paint(".exit, exit, quit, q:")
                );
                println!(
                    "{} Show details of the last error",
                    prompt_color.paint(".error:")
                );
                println!("{} Show this help message", prompt_color.paint("help:"));
                println!("\nOther lines will be interpreted as SQL");
                continue;
            }
            "show tables" | "show tables;" => {
                "select table_catalog, table_schema, table_name, table_type from information_schema.tables where table_schema != 'information_schema'"
            }
            line if line.to_lowercase().starts_with(NQL_LINE_PREFIX) => {
                let _ = rl.add_history_entry(line);
                get_and_display_nql_records(
                    repl_config.http_endpoint.clone(),
                     line.strip_prefix(NQL_LINE_PREFIX).unwrap_or(line).to_string()
                ).await.map_err(|e| format!("Error occured on NQL request: {e}"))?;
                continue;
            }
            _ => line,
        };

        let _ = rl.add_history_entry(line);

        let start_time = Instant::now();
        match get_records(client.clone(), line).await {
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
async fn get_records(
    mut client: FlightServiceClient<Channel>,
    line: &str,
) -> Result<(Vec<RecordBatch>, usize, bool), FlightError> {
    let sql_command = CommandStatementQuery {
        query: line.to_string(),
        transaction_id: None,
    };
    let sql_command_bytes = sql_command.as_any().encode_to_vec();

    let request = FlightDescriptor::new_cmd(sql_command_bytes);

    let mut flight_info = client.get_flight_info(request).await?.into_inner();
    let Some(endpoint) = flight_info.endpoint.pop() else {
        return Err(FlightError::Tonic(Status::internal("No endpoint")));
    };
    let Some(ticket) = endpoint.ticket else {
        return Err(FlightError::Tonic(Status::internal("No ticket")));
    };
    let request = ticket.into_request();

    let response = client.do_get(request).await?;
    let from_cache = response
        .metadata()
        .get("x-cache")
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
) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    let resp = send_nsql_request(&Client::new(), endpoint, query, LlmRuntime::Openai).await?;

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

fn display_grpc_error(err: &Status) {
    let (error_type, user_err_msg) = match err.code() {
        Code::Ok => return,
        Code::Unknown | Code::Internal | Code::Unauthenticated | Code::DataLoss | Code::FailedPrecondition =>{
            ("Error", "An internal error occurred. Execute '.error' to show details.")
        },
        Code::InvalidArgument | Code::AlreadyExists | Code::NotFound => ("Query Error", err.message()),
        Code::Cancelled => ("Error", "The query was cancelled before it could complete."),
        Code::Aborted => ("Error", "The query was aborted before it could complete."),
        Code::DeadlineExceeded => ("Error", "The query could not be completed because the deadline for the query was exceeded."),
        Code::PermissionDenied => ("Error", "The query could not be completed because the user does not have permission to access the requested data."),
        Code::ResourceExhausted => ("Error", "The query could not be completed because the server has run out of resources."),
        Code::Unimplemented => ("Error", "The query could not be completed because the server does not support the requested operation."),
        Code::Unavailable => ("Error", "The query could not be completed because the server is unavailable."),
        Code::OutOfRange => ("Error", "The query could not be completed because the size limit of the query result was exceeded. Retry with `limit` clause."),
    };

    println!("{} {user_err_msg}", Colour::Red.paint(error_type));
}
