use arrow_flight::{
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_client::FlightServiceClient,
    FlightData, FlightDescriptor,
};
use futures::{stream, StreamExt};
use tonic::transport::Channel;
use tracing_subscriber::filter::Directive;

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = init_tracing();

    // Set up the Flight client
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    let mut client = FlightServiceClient::new(channel);

    let flight_descriptor = FlightDescriptor::new_path(vec!["test".to_string()]);
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
                tracing::error!("Error receiving message: {e:?}");
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
