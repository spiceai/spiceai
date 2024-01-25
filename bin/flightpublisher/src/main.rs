use std::fs::File;

use arrow::record_batch::RecordBatch;
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor, PutResult};
use futures::stream::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tonic::transport::Channel;

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parquet_file = "test.parquet";

    let file = File::open(parquet_file)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| e.to_string())?;

    let mut reader = builder.build().map_err(|e| e.to_string())?;

    let mut batches: Vec<RecordBatch> = vec![];
    while let Some(Ok(batch)) = reader.next() {
        batches.push(batch);
    }

    // Set up the Flight client
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);

    let flight_descriptor = FlightDescriptor::new_path(vec!["test".to_string()]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(futures::stream::iter(
            batches.into_iter().map(Ok).collect::<Vec<_>>(),
        ));

    let _response: Vec<PutResult> = client
        .do_put(flight_data_stream)
        .await
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;

    println!("Data sent to Apache Arrow Flight endpoint.");

    Ok(())
}
