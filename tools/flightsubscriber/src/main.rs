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

use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_client::FlightServiceClient,
    FlightData, FlightDescriptor,
};
use clap::Parser;
use futures::{stream, StreamExt};
use tonic::transport::{Channel, ClientTlsConfig};
use tracing_subscriber::filter::Directive;

#[derive(Parser)]
#[clap(about = "Spice.ai Flight Subscriber Utility")]
pub struct Args {
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
    let _ = init_tracing();
    let args = Args::parse();

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
    let mut client = FlightServiceClient::new(channel);

    let flight_descriptor = FlightDescriptor::new_path(vec![args.path]);
    let subscription_request =
        stream::iter(vec![FlightData::new().with_descriptor(flight_descriptor)].into_iter());

    println!("Subscribing to Apache Arrow Flight endpoint.");
    let stream = client.do_exchange(subscription_request).await?;

    let stream = stream.into_inner();

    let mut flight_decoder = FlightDataDecoder::new(stream.map(|r| r.map_err(FlightError::Tonic)));

    loop {
        let msg = flight_decoder.next().await;
        match msg {
            Some(Ok(msg)) => match msg.payload {
                DecodedPayload::Schema(_) => {
                    tracing::trace!("SCHEMA");
                }
                DecodedPayload::RecordBatch(batch) => {
                    tracing::info!("RECORD BATCH: num_rows={}", batch.num_rows());
                }
                DecodedPayload::None => {
                    tracing::trace!("NONE");
                }
            },
            Some(Err(e)) => {
                tracing::error!("Error receiving message: {e}");
            }
            None => {
                tracing::info!("No more messages.");
                break;
            }
        }
    }

    Ok(())
}

fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive("flightsubscriber".parse::<Directive>()?)
        .with_env_var("SPICED_LOG")
        .from_env_lossy();

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}
