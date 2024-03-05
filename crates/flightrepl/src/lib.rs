use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
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
use regex::Regex;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use tonic::transport::Channel;
use tonic::IntoRequest;

mod nsql;

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

    #[arg(long)]
    pub init_nsql: bool,
}

#[allow(clippy::missing_errors_doc)]
pub async fn run(repl_config: ReplConfig) -> Result<(), Box<dyn std::error::Error>> {
    // Set up the Flight client
    let channel = Channel::from_shared(repl_config.repl_flight_endpoint)?
        .connect()
        .await?;
    let client = FlightServiceClient::new(channel);

    let mut rl = DefaultEditor::new()?;

    println!("Welcome to the interactive Spice.ai SQL Query Utility! Type 'help' for help.\n");
    println!("show tables; -- list available tables");

    let mut schemas = HashMap::new();
    let model = if repl_config.init_nsql {
        nsql::load_model()?
    } else {
        None
    };

    loop {
        let line_result = rl.readline("sql> ");
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
        match line {
            ".exit" | "exit" | "quit" | "q" => break,
            "help" => {
                println!("Available commands:\n");
                println!(".exit, exit, quit, q: Exit the REPL");
                println!("help: Show this help message");
                println!("\nAny other line will be interpreted as a SQL query");
                continue;
            }
            _ => {}
        }

        // Handle NSQL, possible change user query
        let query = if repl_config.init_nsql {
            let r = Regex::new(r"NSQL ADD (?<table>[\w\.\_]+)")?;
            if let Some(cap) = r.captures(line) {
                // run `NSQL ADD <table_name>`
                let schm = get_schema(client.clone(), &cap["table"]).await?;
                schemas.insert(cap["table"].to_string(), schm);
                continue;

            } else if line.starts_with("NSQL ") {
                match model {
                    None => {
                        println!("Cannot run NSQL - no model loaded!");
                        continue;
                    }
                    Some(ref m) => {
                        nsql::run_nsql(line.to_string(), m.clone(), &schemas).await?
                    }
                }
            } else {
                line.to_string()
            }
        } else {
            line.to_string()
        };
        if repl_config.init_nsql {
            println!("Running query: '{query}'");
        }

        let _ = rl.add_history_entry(&query);
        let start_time = Instant::now();
        let records = run_sql(client.clone(), query).await?;
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
                .build()?,
        );

        if let Err(e) = df.show().await {
            println!("Error displaying results: {e}");
        };
        let elapsed = start_time.elapsed();
        println!("\nQuery took: {} seconds", elapsed.as_secs_f64());
    }

    Ok(())
}

pub async fn run_sql(mut client: FlightServiceClient<Channel>, query: String) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let request = FlightDescriptor::new_cmd(query);
    let mut flight_info = client.get_flight_info(request).await?.into_inner();
    let Some(endpoint) = flight_info.endpoint.pop() else {
        return Err("No endpoint".into())
    };
    let Some(ticket) = endpoint.ticket else {
        return Err("No ticket".into())
    };
    let request = ticket.into_request();

    let stream = match client.do_get(request).await {
        Ok(stream) => stream.into_inner(),
        Err(e) => {
            return Err(e.into());
        }
    };
    let mut stream =
        FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::Tonic));
    let mut records = vec![];
    while let Some(data) = stream.next().await {
        let data = data?;
        records.push(data);
    };
    return Ok(records)
}

pub async fn get_schema(client: FlightServiceClient<Channel>, table_ref: &str) -> Result<SchemaRef, Box<dyn std::error::Error>> {
    match run_sql(client, format!("SELECT * FROM {} limit 1", table_ref)).await?.first() {
        Some(r) => Ok(r.schema()),
        None =>  Err("Cannot infer schema from empty table".into())
    }
}