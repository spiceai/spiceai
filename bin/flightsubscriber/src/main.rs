use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor};
use futures::StreamExt;
use tonic::transport::Channel;

/// Reads a Parquet file and sends it via DoPut to an Apache Arrow Flight endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the Flight client
    let channel = Channel::from_static("http://localhost:50051")
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);

    let flight_descriptor = FlightDescriptor::new_path(vec!["test".to_string()]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(futures::stream::iter(
            vec![].into_iter().map(Ok).collect::<Vec<_>>(),
        ))
        .map(Result::unwrap);

    println!("Subscribing to Apache Arrow Flight endpoint.");
    let mut stream = client.do_exchange(flight_data_stream).await?;

    loop {
        let msg = stream.next().await;
        match msg {
            Some(Ok(msg)) => {
                println!("Received message: {msg:?}");
            }
            Some(Err(e)) => {
                println!("Error receiving message: {e:?}");
            }
            None => {
                println!("No more messages.");
                break;
            }
        }
    }

    Ok(())
}
