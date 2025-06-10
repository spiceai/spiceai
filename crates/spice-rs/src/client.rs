use crate::util::{retry, FibonacciBackoffBuilder, RetryError};
use crate::{
    config::{GenericError, SPICE_CLOUD_FLIGHT_ADDR, SPICE_LOCAL_FLIGHT_ADDR},
    flight::SqlFlightClient,
    tls::{ensure_crypto_provider, new_tls_flight_channel},
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;

use tonic::transport::Channel;

const MAX_RETRIES: usize = 3;

struct SpiceClientConfig {
    flight_channel: Channel,
}

impl SpiceClientConfig {
    fn new(flight_channel: Channel) -> Self {
        SpiceClientConfig { flight_channel }
    }

    pub async fn load_from_default() -> Result<SpiceClientConfig, GenericError> {
        let flight_chan = new_tls_flight_channel(SPICE_CLOUD_FLIGHT_ADDR).await?;

        Ok(SpiceClientConfig::new(flight_chan))
    }
}

/// The `SpiceClient` is the main entry point for interacting with the Spice API.
/// It provides methods for querying the Spice Flight endpoint.
#[allow(clippy::module_name_repetitions)]
#[derive(Clone)]
pub struct SpiceClient {
    flight: SqlFlightClient,
}

impl SpiceClient {
    /// Creates a new `SpiceClient` with the given API key and default user agent.
    /// ```
    /// use spiceai::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = Client::new("API_KEY").await.unwrap();
    /// }
    /// ```
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn new(api_key: &str) -> Result<Self, GenericError> {
        ensure_crypto_provider();
        let config = SpiceClientConfig::load_from_default().await?;

        Ok(Self {
            flight: SqlFlightClient::new(
                config.flight_channel,
                Some(api_key.to_string()),
                None,
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
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let client = Client::new("API_KEY").await.unwrap();
    /// let data = client.query("SELECT * FROM taxi_trips LIMIT 10;").await;
    /// # }
    /// ````
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn query(&self, query: &str) -> Result<FlightRecordBatchStream, GenericError> {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_RETRIES))
            .build();

        retry(retry_strategy, || async {
            match self.flight.query(query).await {
                Ok(stream) => Ok(stream),
                Err(e) => Err(map_retryable_error(e)),
            }
        })
        .await
        .map_err(map_generic_error)
    }

    /// Optional parameterized query with the Spice Flight endpoint with the given SQL query.
    /// /// If `params` is `None`, it behaves like a regular query.
    /// `params` is a parameter binding `RecordBatch`.
    /// <https://docs.rs/arrow-flight/latest/arrow_flight/sql/client/struct.PreparedStatement.html#method.set_parameters>
    /// ```
    /// # use spiceai::Client;
    /// #
    /// # #[tokio::main]
    /// # async fn main() {
    /// #  let client = Client::new("API_KEY").await.unwrap();
    /// let data = client.query_with_params("SELECT * FROM taxi_trips LIMIT 10;", None).await;
    /// # }
    /// ````
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn query_with_params(
        &self,
        query: &str,
        params: Option<RecordBatch>,
    ) -> Result<FlightRecordBatchStream, GenericError> {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_RETRIES))
            .build();

        retry(retry_strategy, || async {
            match self.flight.query_with_params(query, params.clone()).await {
                Ok(stream) => Ok(stream),
                Err(e) => Err(map_retryable_error(e)),
            }
        })
        .await
        .map_err(map_generic_error)
    }
}

fn map_retryable_error(error: GenericError) -> RetryError<GenericError> {
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        if status.metadata().get("spiceai-retryable").is_some() {
            return RetryError::transient(error);
        }
    }
    RetryError::permanent(error)
}

fn map_generic_error(error: GenericError) -> GenericError {
    if let Some(status) = error.downcast_ref::<tonic::Status>() {
        return status_to_arrow_error(status).into();
    }
    error
}

#[allow(clippy::needless_pass_by_value)]
fn status_to_arrow_error(status: &tonic::Status) -> ArrowError {
    ArrowError::IpcError(format!("{status:?}"))
}

/// Builder for creating a `SpiceClient`.
///
/// By default the `SpiceClient` will use local spice runtime flight endpoint.
/// Follow [spiceai quickstart](https://github.com/spiceai/spiceai?tab=readme-ov-file#%EF%B8%8F-quickstart-local-machine) to setup local spice runtime.
/// ```
/// # use spiceai::ClientBuilder;
///
/// # #[tokio::main]
/// # async fn main() {
/// #    let client = ClientBuilder::new()
/// #      .build()
/// #      .await
/// #      .unwrap();
/// # }
/// ```
/// To use default Spice.ai Cloud endpoints, you can use the `with_spiceai_cloud()` method.
///
/// ```
/// # use spiceai::ClientBuilder;
/// # #[tokio::main]
/// # async fn main() {
/// #    let client = ClientBuilder::new()
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
    flight_url: Option<String>,
    cache_control: Option<String>,
    max_retries: usize,
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
            flight_url: None,
            cache_control: None,
            max_retries: MAX_RETRIES,
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

    /// Configures the `SpiceClient` to use the given Spice Flight endpoint.
    #[must_use]
    pub fn flight_url(mut self, flight_url: &str) -> Self {
        self.flight_url = Some(flight_url.to_string());
        self
    }

    /// Configures the `SpiceClient` to use the given maximum number of retries.
    #[must_use]
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Configures the cache control to use the given cache control policy.
    #[must_use]
    pub fn cache_control(mut self, cache_control: &str) -> Self {
        self.cache_control = Some(cache_control.to_string());
        self
    }

    /// Configures the `SpiceClient` to use default Spice.ai Cloud endpoints.
    /// Equivalent to calling `.flight_url("https://flight.spiceai.io")`.
    #[must_use]
    pub fn use_spiceai_cloud(mut self) -> Self {
        self.flight_url = Some(SPICE_CLOUD_FLIGHT_ADDR.to_string());
        self
    }

    /// Builds the `SpiceClient` with the specified configuration.
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` if flight channel creation fails
    pub async fn build(self) -> Result<SpiceClient, GenericError> {
        ensure_crypto_provider();
        let flight_channel = match self.flight_url {
            Some(url) => new_tls_flight_channel(&url).await?,
            None => new_tls_flight_channel(SPICE_LOCAL_FLIGHT_ADDR).await?,
        };

        Ok(SpiceClient {
            flight: SqlFlightClient::new(
                flight_channel,
                self.api_key.clone(),
                self.user_agent.clone(),
                self.cache_control.clone(),
            ),
        })
    }
}
