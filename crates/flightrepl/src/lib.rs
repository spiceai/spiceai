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

use std::sync::Arc;
use std::time::Instant;

use ansi_term::Colour;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_flight::{
    decode::FlightRecordBatchStream, error::FlightError,
    flight_service_client::FlightServiceClient, FlightDescriptor,
};
use clap::Parser;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{provider_as_source, MemTable};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{LogicalPlanBuilder, UNNAMED_TABLE};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tonic::transport::Channel;
use tonic::{Code, IntoRequest, Status};

#[derive(Parser)]
#[clap(about = "Spice.ai Flight Query Utility")]
pub struct ReplConfig {
    #[arg(
        long,
        value_name = "FLIGHT_ENDPOINT",
        default_value = "http://localhost:50051",
        help_heading = "SQL REPL"
    )]
    pub repl_flight_endpoint: String,
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::missing_errors_doc)]
pub async fn run(repl_config: ReplConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Set up the Flight client
    let channel = Channel::from_shared(repl_config.repl_flight_endpoint)?
        .connect()
        .await?;

    // The encoder/decoder size is limited to 500MB.
    let mut client = FlightServiceClient::new(channel)
        .max_decoding_message_size(500 * 1024 * 1024)
        .max_encoding_message_size(500 * 1024 * 1024);

    let mut rl = DefaultEditor::new()?;

    println!("Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.\n");
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
                    "{} Show technical details from the last error",
                    prompt_color.paint(".error:")
                );
                println!("{} Show this help message", prompt_color.paint("help:"));
                println!("\nAny other line will be interpreted as a SQL query");
                continue;
            }
            "show tables" | "show tables;" => {
                "select table_name from information_schema.tables where table_schema = 'public'"
            }
            _ => line,
        };

        let _ = rl.add_history_entry(line);

        let sql_command = CommandStatementQuery {
            query: line.to_string(),
            transaction_id: None,
        };
        let sql_command_bytes = sql_command.as_any().encode_to_vec();

        let request = FlightDescriptor::new_cmd(sql_command_bytes);

        let start_time = Instant::now();
        let mut flight_info = match client.get_flight_info(request).await {
            Ok(flight_info) => flight_info.into_inner(),
            Err(e) => {
                display_grpc_error(&e);
                last_error = Some(e);
                continue;
            }
        };
        let Some(endpoint) = flight_info.endpoint.pop() else {
            let internal_err = Status::internal("No endpoint");
            display_grpc_error(&internal_err);
            last_error = Some(internal_err);
            continue;
        };
        let Some(ticket) = endpoint.ticket else {
            let internal_err = Status::internal("No ticket");
            display_grpc_error(&internal_err);
            last_error = Some(internal_err);
            continue;
        };
        let request = ticket.into_request();

        let stream = match client.do_get(request).await {
            Ok(stream) => stream.into_inner(),
            Err(e) => {
                display_grpc_error(&e);
                last_error = Some(e);
                continue;
            }
        };
        let mut stream =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::Tonic));
        let mut records = vec![];
        let mut total_rows = 0;
        while let Some(data) = stream.next().await {
            match data {
                Ok(data) => {
                    total_rows += data.num_rows();
                    records.push(data);
                }
                Err(e) => {
                    match e {
                        FlightError::Tonic(e) => {
                            display_grpc_error(&e);
                            last_error = Some(e);
                        }
                        _ => println!("Error receiving data: {e}"),
                    };
                    break;
                }
            }
        }

        if records.is_empty() {
            println!("No data returned for query");
            continue;
        }
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
        println!(
            "\nQuery took: {} seconds. {num_rows}/{total_rows} rows displayed.",
            elapsed.as_secs_f64()
        );
        last_error = None;
    }

    Ok(())
}

fn display_grpc_error(err: &Status) {
    let (error_type, user_err_msg) = match err.code() {
        Code::Ok => return,
        Code::Unknown | Code::Internal | Code::Unauthenticated | Code::DataLoss | Code::FailedPrecondition =>{
            ("Error", "An internal error occurred while processing the query. Show technical details with '.error'")
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
