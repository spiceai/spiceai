use std::io;
use std::io::Write;
use std::sync::Arc;

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
use tonic::transport::Channel;
use tonic::IntoRequest;
use tracing_subscriber::filter::Directive;

#[derive(Parser)]
#[clap(about = "Spice.ai Flight Query Utility")]
pub struct Args {
    #[arg(
        long,
        value_name = "FLIGHT_ENDPOINT",
        default_value = "http://localhost:50051"
    )]
    pub flight_endpoint: String,
}

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = init_tracing();
    let args = Args::parse();

    // Set up the Flight client
    let channel = Channel::from_shared(args.flight_endpoint)?
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    loop {
        print!("query> ");
        let _ = io::stdout().flush();
        let mut line = String::new();
        let _ = io::stdin().read_line(&mut line);

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

        let request = FlightDescriptor::new_cmd(line.to_string());

        let mut flight_info = client.get_flight_info(request).await?.into_inner();
        let Some(endpoint) = flight_info.endpoint.pop() else {
            println!("No endpoint");
            continue;
        };
        let Some(ticket) = endpoint.ticket else {
            println!("No ticket");
            continue;
        };
        let request = ticket.into_request();

        let stream = match client.do_get(request).await {
            Ok(stream) => stream.into_inner(),
            Err(e) => {
                println!("{e}");
                continue;
            }
        };
        let mut stream =
            FlightRecordBatchStream::new_from_flight_data(stream.map_err(FlightError::Tonic));
        let mut records = vec![];
        while let Some(data) = stream.next().await {
            let data = data?;
            records.push(data);
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
                .build()?,
        );

        if let Err(e) = df.show().await {
            println!("Error displaying results: {e}");
        };
    }

    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive("flightrepl".parse::<Directive>()?)
        .with_env_var("REPL_LOG")
        .from_env_lossy();

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}
