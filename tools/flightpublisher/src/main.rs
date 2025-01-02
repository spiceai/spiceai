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

use std::fs::File;

use arrow::record_batch::RecordBatch;
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor, PutResult};
use clap::Parser;
use futures::stream::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tonic::transport::{Channel, ClientTlsConfig};

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

    #[arg(long, value_name = "DATASET_PATH", default_value = "test")]
    pub path: String,

    /// Path to the root certificate file to use to verify server's TLS certificate
    #[arg(long, value_name = "TLS_ROOT_CERTIFICATE_FILE")]
    pub tls_root_certificate_file: Option<String>,
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
    let mut flight_endpoint = args.flight_endpoint;
    let channel = if let Some(tls_root_certificate_file) = args.tls_root_certificate_file {
        let tls_root_certificate = std::fs::read(tls_root_certificate_file)?;
        let tls_root_certificate = tonic::transport::Certificate::from_pem(tls_root_certificate);
        let client_tls_config = ClientTlsConfig::new().ca_certificate(tls_root_certificate);
        if flight_endpoint == "http://localhost:50051" {
            flight_endpoint = "https://localhost:50051".to_string();
        }
        Channel::from_shared(flight_endpoint)?
            .tls_config(client_tls_config)?
            .connect()
            .await
    } else {
        Channel::from_shared(flight_endpoint)?.connect().await
    }?;
    let mut client = FlightClient::new(channel);

    let flight_descriptor = FlightDescriptor::new_path(vec![args.path]);
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
