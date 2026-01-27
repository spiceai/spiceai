use crate::flight::RetryableQueryStream;
use crate::util::{FibonacciBackoffBuilder, RetryError, retry};
use crate::{
    config::{GenericError, SPICE_CLOUD_FLIGHT_ADDR, SPICE_LOCAL_FLIGHT_ADDR},
    flight::{SqlFlightClient, is_connection_reset_generic_error},
    tls::{ensure_crypto_provider, new_tls_flight_channel},
};
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use snafu::Snafu;
use std::sync::Arc;

use tonic::transport::Channel;

const MAX_RETRIES: u32 = 3;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Query execution failed: {source}"))]
    Query {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Failed to process query stream: {source}"))]
    QueryStream { source: FlightError },

    #[snafu(display("Connection reset: {message}\nPlease retry the query."))]
    ConnectionReset { message: String },
}

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
#[derive(Clone)]
pub struct SpiceClient {
    flight: Arc<SqlFlightClient>,
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
            flight: Arc::new(SqlFlightClient::new(
                config.flight_channel,
                Some(api_key.to_string()),
                None,
                None,
                MAX_RETRIES,
            )),
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
    /// #  let data = client.query("SELECT * FROM taxi_trips LIMIT 10;").await;
    /// # }
    /// ````
    ///
    /// ## Errors
    ///
    /// - `Box<dyn Error + Send + Sync>` for any query error
    pub async fn query(&self, query: &str) -> Result<RetryableQueryStream, Error> {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_RETRIES as usize))
            .build();

        retry(retry_strategy, || async {
            match self.flight.query(query).await {
                Ok(stream) => Ok(RetryableQueryStream::new(
                    Arc::clone(&self.flight),
                    query,
                    None,
                    Box::pin(stream),
                )),
                Err(e) => {
                    if is_connection_reset_generic_error(&e) {
                        return Err(RetryError::transient(e));
                    }
                    Err(RetryError::Permanent(e))
                }
            }
        })
        .await
        .map_err(|e| Error::Query { source: e })
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
    /// #  let data = client.query_with_params("SELECT * FROM taxi_trips LIMIT 10;", None).await;
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
    ) -> Result<RetryableQueryStream, Error> {
        let retry_strategy = FibonacciBackoffBuilder::new()
            .max_retries(Some(MAX_RETRIES as usize))
            .build();

        retry(retry_strategy, || async {
            match self.flight.query_with_params(query, params.clone()).await {
                Ok(stream) => Ok(RetryableQueryStream::new(
                    Arc::clone(&self.flight),
                    query,
                    params.clone(),
                    Box::pin(stream),
                )),
                Err(e) => {
                    if is_connection_reset_generic_error(&e) {
                        return Err(RetryError::transient(e));
                    }
                    Err(RetryError::Permanent(e))
                }
            }
        })
        .await
        .map_err(|e| Error::Query { source: e })
    }
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
    max_retries: u32,
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
    pub fn max_retries(mut self, max_retries: u32) -> Self {
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
            flight: Arc::new(SqlFlightClient::new(
                flight_channel,
                self.api_key.clone(),
                self.user_agent.clone(),
                self.cache_control.clone(),
                self.max_retries,
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_builder_default() {
        let builder = SpiceClientBuilder::default();
        assert!(builder.api_key.is_none());
        assert!(builder.user_agent.is_none());
        assert!(builder.flight_url.is_none());
        assert!(builder.cache_control.is_none());
        assert_eq!(builder.max_retries, MAX_RETRIES);
    }

    #[test]
    fn test_client_builder_new() {
        let builder = SpiceClientBuilder::new();
        assert!(builder.api_key.is_none());
        assert!(builder.user_agent.is_none());
        assert!(builder.flight_url.is_none());
        assert!(builder.cache_control.is_none());
        assert_eq!(builder.max_retries, MAX_RETRIES);
    }

    #[test]
    fn test_client_builder_api_key() {
        let builder = SpiceClientBuilder::new().api_key("test_key");
        assert_eq!(builder.api_key, Some("test_key".to_string()));
    }

    #[test]
    fn test_client_builder_user_agent() {
        let builder = SpiceClientBuilder::new().user_agent("custom-agent/1.0");
        assert_eq!(builder.user_agent, Some("custom-agent/1.0".to_string()));
    }

    #[test]
    fn test_client_builder_flight_url() {
        let builder = SpiceClientBuilder::new().flight_url("https://custom.endpoint.io");
        assert_eq!(
            builder.flight_url,
            Some("https://custom.endpoint.io".to_string())
        );
    }

    #[test]
    fn test_client_builder_max_retries() {
        let builder = SpiceClientBuilder::new().max_retries(10);
        assert_eq!(builder.max_retries, 10);
    }

    #[test]
    fn test_client_builder_cache_control() {
        let builder = SpiceClientBuilder::new().cache_control("no-cache");
        assert_eq!(builder.cache_control, Some("no-cache".to_string()));
    }

    #[test]
    fn test_client_builder_use_spiceai_cloud() {
        let builder = SpiceClientBuilder::new().use_spiceai_cloud();
        assert_eq!(
            builder.flight_url,
            Some(SPICE_CLOUD_FLIGHT_ADDR.to_string())
        );
    }

    #[test]
    fn test_client_builder_chaining() {
        let builder = SpiceClientBuilder::new()
            .api_key("my_api_key")
            .user_agent("my-agent/2.0")
            .max_retries(5)
            .cache_control("max-age=3600")
            .use_spiceai_cloud();

        assert_eq!(builder.api_key, Some("my_api_key".to_string()));
        assert_eq!(builder.user_agent, Some("my-agent/2.0".to_string()));
        assert_eq!(builder.max_retries, 5);
        assert_eq!(builder.cache_control, Some("max-age=3600".to_string()));
        assert_eq!(
            builder.flight_url,
            Some(SPICE_CLOUD_FLIGHT_ADDR.to_string())
        );
    }

    #[test]
    fn test_client_builder_flight_url_overrides_cloud() {
        let builder = SpiceClientBuilder::new()
            .use_spiceai_cloud()
            .flight_url("https://custom.endpoint.io");

        // flight_url should override the cloud endpoint
        assert_eq!(
            builder.flight_url,
            Some("https://custom.endpoint.io".to_string())
        );
    }

    #[test]
    fn test_client_builder_cloud_overrides_flight_url() {
        let builder = SpiceClientBuilder::new()
            .flight_url("https://custom.endpoint.io")
            .use_spiceai_cloud();

        // use_spiceai_cloud should override custom flight_url
        assert_eq!(
            builder.flight_url,
            Some(SPICE_CLOUD_FLIGHT_ADDR.to_string())
        );
    }

    #[test]
    fn test_spice_client_has_builder() {
        let builder = SpiceClient::builder();
        assert!(builder.api_key.is_none());
    }

    #[test]
    fn test_error_display_query() {
        let error = Error::Query {
            source: "test error".into(),
        };
        let display = format!("{error}");
        assert!(display.contains("Query execution failed"));
    }

    #[test]
    fn test_error_display_connection_reset() {
        let error = Error::ConnectionReset {
            message: "connection lost".to_string(),
        };
        let display = format!("{error}");
        assert!(display.contains("Connection reset"));
        assert!(display.contains("connection lost"));
    }

    // Edge case tests

    #[test]
    fn test_client_builder_empty_api_key() {
        let builder = SpiceClientBuilder::new().api_key("");
        assert_eq!(builder.api_key, Some(String::new()));
    }

    #[test]
    fn test_client_builder_empty_user_agent() {
        let builder = SpiceClientBuilder::new().user_agent("");
        assert_eq!(builder.user_agent, Some(String::new()));
    }

    #[test]
    fn test_client_builder_empty_flight_url() {
        let builder = SpiceClientBuilder::new().flight_url("");
        assert_eq!(builder.flight_url, Some(String::new()));
    }

    #[test]
    fn test_client_builder_zero_max_retries() {
        let builder = SpiceClientBuilder::new().max_retries(0);
        assert_eq!(builder.max_retries, 0);
    }

    #[test]
    fn test_client_builder_max_retries_u32_max() {
        let builder = SpiceClientBuilder::new().max_retries(u32::MAX);
        assert_eq!(builder.max_retries, u32::MAX);
    }

    #[test]
    fn test_client_builder_special_chars_in_api_key() {
        let api_key = "abc123!@#$%^&*()_+-=[]{}|;':\",./<>?";
        let builder = SpiceClientBuilder::new().api_key(api_key);
        assert_eq!(builder.api_key, Some(api_key.to_string()));
    }

    #[test]
    fn test_client_builder_unicode_user_agent() {
        let user_agent = "测试-agent/1.0 🚀";
        let builder = SpiceClientBuilder::new().user_agent(user_agent);
        assert_eq!(builder.user_agent, Some(user_agent.to_string()));
    }

    #[test]
    fn test_client_builder_multiple_calls_same_method() {
        let builder = SpiceClientBuilder::new()
            .api_key("first")
            .api_key("second")
            .api_key("third");
        assert_eq!(builder.api_key, Some("third".to_string()));
    }

    #[test]
    fn test_error_query_stream() {
        let error = Error::QueryStream {
            source: FlightError::NotYetImplemented("test".to_string()),
        };
        let display = format!("{error}");
        assert!(display.contains("Failed to process query stream"));
    }

    #[test]
    fn test_client_builder_cache_control_variations() {
        // Test various cache control header values
        let cases = ["no-cache", "max-age=0", "no-store", "private, max-age=3600"];

        for case in cases {
            let builder = SpiceClientBuilder::new().cache_control(case);
            assert_eq!(builder.cache_control, Some(case.to_string()));
        }
    }

    #[test]
    fn test_client_builder_whitespace_in_values() {
        let builder = SpiceClientBuilder::new()
            .api_key("  key with spaces  ")
            .user_agent("  agent  ");
        assert_eq!(builder.api_key, Some("  key with spaces  ".to_string()));
        assert_eq!(builder.user_agent, Some("  agent  ".to_string()));
    }
}
