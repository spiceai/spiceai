
use crate::{flight::{SqlFlightClient}, prices::PricesClient, tls::new_tls_flight_channel};
use std::error::Error;
use arrow_flight::sql::client::FlightSqlServiceClient;
use tonic::{transport::{Channel}};
use tonic::{Streaming};
use arrow_flight::{FlightData};


pub async fn new_spice_client(api_key: String, http_addr: String, flight_addr: String, firecache_addr: String) -> Result<SpiceClient, Box<dyn Error>> {
    let flight_chan = new_tls_flight_channel(flight_addr).await;
    if flight_chan.is_err() {
        return Err(flight_chan.err().expect("").into())
    }

    match new_tls_flight_channel(firecache_addr).await {
        Err(e) => Err(e.into()),
        Ok(firecache_chan) => Ok(SpiceClient::new(
            http_addr,
            api_key,
            flight_chan.expect(""),
            firecache_chan
        ))
    }
}

pub struct SpiceClient {
    flight: SqlFlightClient,
    firecache: SqlFlightClient,
    pub prices: PricesClient
}

impl SpiceClient {
    pub fn new(http_addr: String, api_key: String, flight: Channel, firecache: Channel) -> Self {
        Self {
            flight: SqlFlightClient::new(flight, api_key.clone()),
            firecache: SqlFlightClient::new(firecache, api_key.clone()),
            prices: PricesClient::new(Some(http_addr), api_key)
        }
    }
    pub async fn query(&mut self, query: String, timeout: Option<u32>) -> Result<Streaming<FlightData>, Box<dyn Error>> {
        self.flight.query(query, timeout).await
    }

    pub async fn firecache_query(&mut self, query: String, timeout: Option<u32>) -> Result<Streaming<FlightData>, Box<dyn Error>> {
        self.firecache.query(query, timeout).await
    }

    
}