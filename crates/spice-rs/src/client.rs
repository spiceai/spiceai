use crate::{flight::SqlFlightClient, prices::PricesClient, tls::new_tls_flight_channel};
use arrow_flight::decode::FlightRecordBatchStream;
use std::error::Error;
use tokio::join;
use tonic::transport::Channel;

pub struct SpiceClientConfig {
    pub https_addr: String,
    pub flight_channel: Channel,
    pub firecache_channel: Channel,
}

impl SpiceClientConfig {
    pub fn new(https_addr: String, flight_channel: Channel, firecache_channel: Channel) -> Self {
        SpiceClientConfig {
            https_addr: https_addr,
            flight_channel: flight_channel,
            firecache_channel: firecache_channel,
        }
    }

    pub async fn load_from_default() -> Result<SpiceClientConfig, Box<dyn Error>> {
        let https_addr = "https://data.spiceai.io".to_string();
        let flight_addr = "https://flight.spiceai.io".to_string();
        let firecache_addr = "https://firecache.spiceai.io".to_string();

        match join!(
            new_tls_flight_channel(flight_addr.clone()),
            new_tls_flight_channel(firecache_addr.clone())
        ) {
            (Err(e), _) => return Err(e.into()),
            (_, Err(e)) => return Err(e.into()),
            (Ok(flight_chan), Ok(firecache_chan)) => {
                return Ok(SpiceClientConfig::new(
                    https_addr,
                    flight_chan,
                    firecache_chan,
                ));
            }
        }
    }
}

pub struct SpiceClient {
    flight: SqlFlightClient,
    firecache: SqlFlightClient,
    pub prices: PricesClient,
}

impl SpiceClient {
    pub async fn new(api_key: &str) -> Self {
        let config = SpiceClientConfig::load_from_default()
            .await
            .expect("Error Loading Client Config");
        Self {
            flight: SqlFlightClient::new(config.flight_channel, api_key.to_string()),
            firecache: SqlFlightClient::new(config.firecache_channel, api_key.to_string()),
            prices: PricesClient::new(Some(config.https_addr), api_key.to_string()),
        }
    }

    pub async fn query(&mut self, query: &str) -> Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.flight.query(query).await
    }

    pub async fn fire_query(
        &mut self,
        query: &str,
    ) -> Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.firecache.query(query).await
    }
}
