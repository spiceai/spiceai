use std::fs::File;

use arrow::record_batch::RecordBatch;
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor, PutResult};
use clap::Parser;
use futures::stream::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tonic::transport::Channel;

use tokio::time::{self, Duration};

#[derive(Parser)]
#[clap(about = "Spice.ai Flight Publisher Utility")]
pub struct Args {
    /// Load parquet file to send to Apache Arrow Flight endpoint
    #[arg(long, value_name = "PARQUET_FILE", default_value = "test.parquet")]
    pub parquet_file: String,

    #[arg(
        long,
        value_name = "FLIGHT_ENDPOINT",
        default_value = "http://localhost:50051"
    )]
    pub flight_endpoint: String,

    #[arg(long, value_name = "DATASET_PATH", default_value = "databricks_demo_accelerated")]
    pub path: String,
}

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let file = File::open(args.parquet_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;

    let mut reader = builder.build().map_err(|e| e.to_string())?;

    let mut batches: Vec<RecordBatch> = vec![];
    while let Some(Ok(batch)) = reader.next() {
        batches.push(batch);
    }

    // Set up the Flight client
    let channel = Channel::from_shared(args.flight_endpoint)?
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);

    let path = args.path;


    loop {

        let flight_descriptor = FlightDescriptor::new_path(vec![path.clone()]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(futures::stream::iter(
            batches.clone().into_iter().map(Ok).collect::<Vec<_>>(),
        ));

        let _response: Vec<PutResult> = client
            .do_put(flight_data_stream)
            .await
            .map_err(|e| e.to_string())?
            .try_collect()
            .await
            .map_err(|e| e.to_string())?;

        println!("Data sent to Apache Arrow Flight endpoint.");
    
        time::sleep(Duration::from_secs(3)).await;
    }
    

    Ok(())
}
