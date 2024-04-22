use crate::{
    config::{FIRECACHE_ADDR, FLIGHT_ADDR, HTTPS_ADDR},
    flight::SqlFlightClient,
    tls::new_tls_flight_channel,
};
use arrow_flight::decode::FlightRecordBatchStream;
use futures::try_join;
use std::error::Error;
use tonic::transport::Channel;

struct SpiceClientConfig {
    https_addr: String,
    flight_channel: Channel,
    firecache_channel: Channel,
}

impl SpiceClientConfig {
    fn new(https_addr: String, flight_channel: Channel, firecache_channel: Channel) -> Self {
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

/// The `SpiceClient` is the main entry point for interacting with the Spice API.
/// It provides methods for querying the Spice Flight and Firecache endpoints.
#[allow(clippy::module_name_repetitions)]
pub struct SpiceClient {
    flight: SqlFlightClient,
    firecache: SqlFlightClient,
}

impl SpiceClient {
    /// Creates a new `SpiceClient` with the given API key.
    /// ```
    /// use spiceai::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::new("API_KEY").await.unwrap();
    /// }
    /// ```
    pub async fn new(api_key: &str) -> Result<Self, Box<dyn Error>> {
        let config = SpiceClientConfig::load_from_default().await?;

        Ok(Self {
            flight: SqlFlightClient::new(config.flight_channel, api_key.to_string()),
            firecache: SqlFlightClient::new(config.firecache_channel, api_key.to_string()),
        })
    }

    /// Queries the Spice Flight endpoint with the given SQL query.
    /// ```
    /// # use spiceai::Client;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let mut client = Client::new("API_KEY").await.unwrap();
    /// let data = client.query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
    /// # }
    /// ````
    pub async fn query(&mut self, query: &str) -> Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.flight.query(query).await
    }

    /// Queries the Spice Firecache endpoint with the given SQL query.
    /// ```
    /// # use spiceai::Client;
    /// #
    /// #  #[tokio::main]
    /// # async fn main() {
    /// #  let mut client = Client::new("API_KEY").await.unwrap();
    /// let data = client.fire_query("SELECT * FROM eth.recent_blocks LIMIT 10;").await;
    /// # }
    /// ````
    pub async fn fire_query(
        &mut self,
        query: &str,
    ) -> Result<FlightRecordBatchStream, Box<dyn Error>> {
        self.firecache.query(query).await
    }
}
