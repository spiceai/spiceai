use crate::{
    config::{FIRECACHE_ADDR, FLIGHT_ADDR, HTTPS_ADDR},
    flight::SqlFlightClient,
    prices::PricesClient,
    tls::new_tls_flight_channel,
};
use arrow_flight::decode::FlightRecordBatchStream;
use futures::try_join;
use std::error::Error;
use tonic::transport::Channel;

pub struct SpiceClientConfig {
    pub https_addr: String,
    pub flight_channel: Channel,
    pub firecache_channel: Channel,
}

impl SpiceClientConfig {
    pub fn new(https_addr: String, flight_channel: Channel, firecache_channel: Channel) -> Self {
        SpiceClientConfig {
            https_addr,
            flight_channel,
            firecache_channel,
        }
    }

    pub async fn load_from_default() -> Result<SpiceClientConfig, Box<dyn Error>> {
        let (flight_chan, firecache_chan) = try_join!(
            new_tls_flight_channel(FLIGHT_ADDR),
            new_tls_flight_channel(FIRECACHE_ADDR)
        )?;

        Ok(SpiceClientConfig::new(
            HTTPS_ADDR.to_string(),
            flight_chan,
            firecache_chan,
        ))
    }
}

pub struct SpiceClient {
    flight: SqlFlightClient,
    firecache: SqlFlightClient,
    pub prices: PricesClient,
}

impl SpiceClient {
    pub async fn new(api_key: &str) -> Result<Self, Box<dyn Error>> {
        let config = SpiceClientConfig::load_from_default().await?;

        Ok(Self {
            flight: SqlFlightClient::new(config.flight_channel, api_key.to_string()),
            firecache: SqlFlightClient::new(config.firecache_channel, api_key.to_string()),
            prices: PricesClient::new(Some(config.https_addr), api_key.to_string()),
        })
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
