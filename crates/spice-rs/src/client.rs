use crate::{
    config::{
        GenericError, SPICE_CLOUD_FIRECACHE_ADDR, SPICE_CLOUD_FLIGHT_ADDR, SPICE_LOCAL_FLIGHT_ADDR,
    },
    flight::SqlFlightClient,
    tls::new_tls_flight_channel,
};
use arrow_flight::decode::FlightRecordBatchStream;
use futures::try_join;
use tonic::transport::Channel;

struct SpiceClientConfig {
    flight_channel: Channel,
    firecache_channel: Channel,
}

impl SpiceClientConfig {
    fn new(flight_channel: Channel, firecache_channel: Channel) -> Self {
        SpiceClientConfig {
            flight_channel,
            firecache_channel,
        }
    }

    pub async fn load_from_default() -> Result<SpiceClientConfig, GenericError> {
        let (flight_chan, firecache_chan) = try_join!(
            new_tls_flight_channel(SPICE_CLOUD_FLIGHT_ADDR),
            new_tls_flight_channel(SPICE_CLOUD_FIRECACHE_ADDR)
        )?;

        Ok(SpiceClientConfig::new(flight_chan, firecache_chan))
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
    /// Creates a new `SpiceClient` with the given API key and default user agent.
    /// ```
    /// use spiceai::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::new("API_KEY").await.unwrap();
    /// }
    /// ```
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn new(api_key: &str) -> Result<Self, GenericError> {
        let config = SpiceClientConfig::load_from_default().await?;

        Ok(Self {
            flight: SqlFlightClient::new(config.flight_channel, Some(api_key.to_string()), None),
            firecache: SqlFlightClient::new(
                config.firecache_channel,
                Some(api_key.to_string()),
                None,
            ),
        })
    }

    #[must_use]
    pub fn builder() -> SpiceClientBuilder {
        SpiceClientBuilder::new()
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
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn query(&mut self, query: &str) -> Result<FlightRecordBatchStream, GenericError> {
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
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn fire_query(
        &mut self,
        query: &str,
    ) -> Result<FlightRecordBatchStream, GenericError> {
        self.firecache.query(query).await
    }
}

/// Builder for creating a `SpiceClient`.
///
/// By default the `SpiceClient` will use local spice runtime flight endpoint.
/// Follow [spiceai quickstart](https://github.com/spiceai/spiceai?tab=readme-ov-file#%EF%B8%8F-quickstart-local-machine) to setup local spice runtime.
/// ```
/// # use spiceai::ClientBuilder;
/// #
/// # #[tokio::main]
/// # async fn main() {
/// #    let mut client = ClientBuilder::new()
/// #      .build()
/// #      .await
/// #      .unwrap();
/// # }
/// ```
/// To use default Spice.ai Cloud endpoints, you can use the `with_spiceai_cloud()` method.
///
/// ```
/// # use spiceai::ClientBuilder;
/// #
/// # #[tokio::main]
/// # async fn main() {
/// #    let mut client = ClientBuilder::new()
/// #      .api_key("API_KEY")
/// #      .use_spiceai_cloud()
/// #      .build()
/// #      .await
/// #      .unwrap();
/// # }
/// ```
///
pub struct SpiceClientBuilder {
    api_key: Option<String>,
    user_agent: Option<String>,
    firecache_url: Option<String>,
    flight_url: Option<String>,
}

impl Default for SpiceClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SpiceClientBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            api_key: None,
            user_agent: None,
            firecache_url: None,
            flight_url: None,
        }
    }

    /// Configures the `SpiceClient` to use the given API key.
    #[must_use]
    pub fn api_key(mut self, api_key: &str) -> Self {
        self.api_key = Some(api_key.to_string());
        self
    }

    /// Configures the `SpiceClient` to use the given custom user agent.
    #[must_use]
    pub fn user_agent(mut self, user_agent: &str) -> Self {
        self.user_agent = Some(user_agent.to_string());
        self
    }

    /// Configures the `SpiceClient` to use the given Spice Firecache endpoint.
    #[must_use]
    pub fn firecache_url(mut self, firecache_url: &str) -> Self {
        self.firecache_url = Some(firecache_url.to_string());
        self
    }

    /// Configures the `SpiceClient` to use the given Spice Flight endpoint.
    #[must_use]
    pub fn flight_url(mut self, flight_url: &str) -> Self {
        self.flight_url = Some(flight_url.to_string());
        self
    }

    /// Configures the `SpiceClient` to use default Spice.ai Cloud endpoints.
    /// Equivalent to calling `.firecache_url("https://firecache.spiceai.io")` and `.flight_url("https://flight.spiceai.io")`.
    #[must_use]
    pub fn use_spiceai_cloud(mut self) -> Self {
        self.flight_url = Some(SPICE_CLOUD_FLIGHT_ADDR.to_string());
        self.firecache_url = Some(SPICE_CLOUD_FIRECACHE_ADDR.to_string());
        self
    }

    /// Builds the `SpiceClient` with the specified configuration.
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` if flight or firecache channel creation fails
    pub async fn build(self) -> Result<SpiceClient, GenericError> {
        let flight_channel = match self.flight_url {
            Some(url) => new_tls_flight_channel(&url).await?,
            None => new_tls_flight_channel(SPICE_LOCAL_FLIGHT_ADDR).await?,
        };

        let firecache_channel = match self.firecache_url {
            Some(url) => new_tls_flight_channel(&url).await?,
            None => new_tls_flight_channel(SPICE_CLOUD_FIRECACHE_ADDR).await?,
        };

        Ok(SpiceClient {
            flight: SqlFlightClient::new(
                flight_channel,
                self.api_key.clone(),
                self.user_agent.clone(),
            ),
            firecache: SqlFlightClient::new(
                firecache_channel,
                self.api_key.clone(),
                self.user_agent.clone(),
            ),
        })
    }
}
