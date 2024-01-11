use crate::{
    config::{FIRECACHE_ADDR, FLIGHT_ADDR, HTTPS_ADDR},
    flight::SqlFlightClient,
    prices::PricesClient,
    tls::new_tls_flight_channel,
    HistoricalPriceData, LatestPricesResponse,
};
use arrow_flight::decode::FlightRecordBatchStream;
use chrono::{DateTime, Utc};
use futures::try_join;
use std::{collections::HashMap, error::Error};
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

/// The SpiceClient is the main entry point for interacting with the Spice API.
/// It provides methods for querying the Spice Flight and Firecache endpoints,
/// as well as the Spice Prices endpoint.
pub struct SpiceClient {
    flight: SqlFlightClient,
    firecache: SqlFlightClient,
    prices: PricesClient,
}

impl SpiceClient {
    /// Creates a new SpiceClient with the given API key.
    /// ```
    /// use spice_rs::Client;
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
            prices: PricesClient::new(Some(config.https_addr), api_key.to_string()),
        })
    }

    /// Queries the Spice Flight endpoint with the given SQL query.
    /// ```
    /// # use spice_rs::Client;
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
    /// # use spice_rs::Client;
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

    /// Get the supported pairs:
    /// ```rust
    /// # use spice_rs::Client;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let mut client = Client::new("API_KEY").await.unwrap();
    /// let supported_pairs = client.get_supported_pairs().await;
    /// # }
    /// ```
    pub async fn get_supported_pairs(&self) -> Result<Vec<String>, Box<dyn Error>> {
        self.prices.get_supported_pairs().await
    }

    /// Get the latest price for a token pair:
    /// ```rust
    /// # use spice_rs::Client;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let mut client = Client::new("API_KEY").await.unwrap();
    /// let price_data = client.get_prices(&["BTC-USDC"]).await;
    /// # }
    /// ```
    pub async fn get_prices(&self, pairs: &[&str]) -> Result<LatestPricesResponse, Box<dyn Error>> {
        self.prices.get_prices(pairs).await
    }

    /// Get historical data:
    /// ```rust
    /// # use spice_rs::Client;
    /// # use chrono::Utc;
    /// # use chrono::Duration;
    /// # use std::ops::Sub;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let mut client = Client::new("API_KEY").await.unwrap();
    /// #  let now = Utc::now();
    /// #  let start = now.sub(Duration::seconds(3600));
    /// let historical_price_data = client
    ///     .get_historical_prices(&["BTC-USDC"], Some(start), Some(now), Option::None).await;
    /// # }
    /// ```
    pub async fn get_historical_prices(
        &self,
        pairs: &[&str],
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        granularity: Option<&str>,
    ) -> Result<HashMap<String, Vec<HistoricalPriceData>>, Box<dyn Error>> {
        self.prices
            .get_historical_prices(pairs, start, end, granularity)
            .await
    }
}
