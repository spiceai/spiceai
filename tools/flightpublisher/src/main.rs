use std::{clone, fs::File};

use arrow_flight::{decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder, flight_descriptor::DescriptorType, flight_service_client::FlightServiceClient, FlightClient, FlightData, FlightDescriptor, PutResult, Ticket};
use clap::Parser;
use futures::{stream::TryStreamExt, StreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tonic::{transport::Channel, IntoRequest};
use arrow::record_batch::RecordBatch;

use tokio::time::{self, error::Elapsed, Duration};

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


async fn get_test_batch(query: &str) -> RecordBatch{
    let channel = Channel::from_static("https://flight.spiceai.io").connect().await.unwrap();
    let mut client = FlightClient::new(channel);

    let _ = client.add_header("Authorization", "Bearer 313834|0666ecca421b4b33ba4d0dd2e90d6daa");

    let req = FlightDescriptor::new_cmd(query.to_string());

    let flight_info = client.get_flight_info(req).await.unwrap();

    let ticket = flight_info.endpoint[0]
        // Extract the ticket
        .ticket
        .clone()
        .expect("expected ticket");

    let mut data_stream =  client.do_get(ticket).await.expect("error fetching data");

    let res = data_stream.next().await.unwrap();

    match res {
        Ok(batch) => {          
            return batch;
        },
        Err(e) => {
            /* handle error */
            panic!("Error while processing Flight data stream: {}", e)
        },
    }

}

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Set up the Flight client
    let channel = Channel::from_shared(args.flight_endpoint)?
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);

    let path = args.path;

    loop {

        let batch = get_test_batch("SELECT number, hash, parent_hash, \"timestamp\" FROM goerli.blocks order by number desc limit 1;").await;

        let flight_descriptor = FlightDescriptor::new_path(vec![path.clone()]);
        let input_stream = futures::stream::iter(vec![Ok(batch)]);

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(input_stream);


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
