use crate::{flight::SqlFlightClient, prices::PricesClient, tls::new_tls_flight_channel};
use arrow_flight::decode::FlightRecordBatchStream;
use std::error::Error;
use tonic::transport::Channel;

pub async fn new_spice_client(api_key: String) -> Result<SpiceClient, Box<dyn Error>> {
    return new_spice_client_with_address(
        api_key.to_string(),
        "https://data.spiceai.io".to_string(),
        "https://flight.spiceai.io".to_string(),
    )
    .await;
}

pub async fn new_spice_client_with_address(
    api_key: String,
    http_addr: String,
    flight_addr: String,
) -> Result<SpiceClient, Box<dyn Error>> {
    match new_tls_flight_channel(flight_addr).await {
        Err(e) => Err(e.into()),
        Ok(flight_chan) => Ok(SpiceClient::new(http_addr, api_key, flight_chan)),
    }
}

pub struct SpiceClient {
    flight: SqlFlightClient,
    pub prices: PricesClient,
}

impl SpiceClient {
    pub fn new(http_addr: String, api_key: String, flight: Channel) -> Self {
        Self {
            flight: SqlFlightClient::new(flight, api_key.clone()),
            prices: PricesClient::new(Some(http_addr), api_key),
        }
    }
    pub async fn query(
        &mut self,
        query: String,
        timeout: Option<u32>,
    ) -> Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.flight.query(query, timeout).await
    }
}
