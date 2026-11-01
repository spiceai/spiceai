/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! A hosted model's token limits, read from its provider's model API.
//!
//! Every model on one endpoint reads the same listing, so it is fetched once per URL and
//! credential and kept: the first model to load on an endpoint may wait on it, and the rest,
//! and every later `/v1/models`, read the kept copy. A provider that does not answer in
//! time is reported once and asked again in the background, so its limits appear once it
//! does; a kept listing is refreshed in the background once it is old, since a local server
//! can restart with another context size.

use std::collections::HashMap;
use std::hash::{Hash as _, Hasher as _};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use async_openai::config::Config;
use chat_api::TokenLimits;
use chat_api::model_info::{self, anthropic_token_limits_of};
use futures::FutureExt as _;
use futures::future::{BoxFuture, Shared};
use serde_json::Value;

/// How long the provider has to answer. The metadata is optional, so a slow or unreachable
/// model API costs the first load on its endpoint this much at most.
const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// How soon an endpoint that did not answer is asked again; each further miss doubles it,
/// up to [`RETRY_MAX`].
const RETRY_FIRST: Duration = Duration::from_secs(30);
const RETRY_MAX: Duration = Duration::from_mins(10);

/// How long a listing is kept before it is refreshed in the background.
const REFRESH_AFTER: Duration = Duration::from_mins(10);

/// The listings of every endpoint this process has read.
static LISTINGS: LazyLock<ListingCache> = LazyLock::new(|| {
    ListingCache::new(Timings {
        timeout: METADATA_TIMEOUT,
        retry_first: RETRY_FIRST,
        retry_max: RETRY_MAX,
        refresh_after: REFRESH_AFTER,
    })
});

/// The token limits the provider's model listing, `GET {base}/models`, reports for `model`:
/// the shape of the `OpenAI` API and the servers compatible with it.
pub(crate) async fn listed_token_limits<C: Config>(config: &C, model: &str) -> TokenLimits {
    LISTINGS.get(config, "/models", model, Limits::listed).await
}

/// The token limits the provider's model object, `GET {base}/models/{model}`, reports: the
/// shape of Anthropic's Models API.
pub(crate) async fn retrieved_token_limits<C: Config>(config: &C, model: &str) -> TokenLimits {
    LISTINGS
        .get(config, &format!("/models/{model}"), model, |object| {
            Limits::One(anthropic_token_limits_of(object))
        })
        .await
}

#[derive(Debug, Clone, Copy)]
struct Timings {
    timeout: Duration,
    retry_first: Duration,
    retry_max: Duration,
    refresh_after: Duration,
}

/// The token limits one answer reports, kept in place of the answer itself.
#[derive(Debug)]
enum Limits {
    /// A listing's, by model id.
    Listed(HashMap<String, TokenLimits>),
    /// One model object's.
    One(TokenLimits),
}

impl Limits {
    /// The limits of each model in a listing, `{"data": [{"id": ...}, ...]}`.
    fn listed(listing: &Value) -> Self {
        Self::Listed(model_info::listed_token_limits(listing))
    }

    fn of(&self, model: &str) -> TokenLimits {
        match self {
            Self::Listed(models) => models.get(model).copied().unwrap_or_default(),
            Self::One(limits) => *limits,
        }
    }
}

/// What one request for a listing came to.
#[derive(Debug, Clone)]
enum Fetched {
    /// The provider answered with JSON.
    Limits(Arc<Limits>),
    /// The provider answered with something else, such as a 404 from a server without a
    /// model API: asked again only when it is refreshed.
    Answered,
    /// The provider did not answer in time, could not be reached, or answered that it could
    /// not serve the request yet (429, or a server error such as the 503 a local server
    /// answers while it loads its model): asked again soon, keeping any limits already read.
    Unreachable(String),
}

impl Fetched {
    fn limits(&self) -> Option<Arc<Limits>> {
        match self {
            Self::Limits(limits) => Some(Arc::clone(limits)),
            Self::Answered | Self::Unreachable(_) => None,
        }
    }
}

enum State {
    Fetching(Shared<BoxFuture<'static, Fetched>>),
    Done {
        limits: Option<Arc<Limits>>,
        at: Instant,
        /// Whether a background request is already asking for this listing again.
        refreshing: bool,
    },
}

/// Listings by URL and credential.
struct ListingCache {
    timings: Timings,
    /// `None` only when no HTTP client can be built, in which case nothing is fetched.
    client: Option<reqwest::Client>,
    entries: Arc<parking_lot::Mutex<HashMap<String, State>>>,
}

/// How to ask for one listing again, outside the call that first asked for it.
#[derive(Clone)]
struct Request {
    client: reqwest::Client,
    url: String,
    headers: reqwest::header::HeaderMap,
    query: Vec<(String, String)>,
    timeout: Duration,
    read: fn(&Value) -> Limits,
}

impl ListingCache {
    fn new(timings: Timings) -> Self {
        Self {
            timings,
            client: crate::provider::create_http_client(),
            entries: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    /// The token limits of `model` that the JSON at `path` of `config`'s endpoint reports,
    /// as `read` takes them from it. The JSON is fetched at most once per URL and credential
    /// at a time.
    async fn get<C: Config>(
        &self,
        config: &C,
        path: &str,
        model: &str,
        read: fn(&Value) -> Limits,
    ) -> TokenLimits {
        let Some(client) = &self.client else {
            return TokenLimits::default();
        };
        let request = Request {
            client: client.clone(),
            url: config.url(path),
            headers: config.headers(),
            query: config
                .query()
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            timeout: self.timings.timeout,
            read,
        };
        let key = cache_key(&request);
        let limits_of = |limits: Option<Arc<Limits>>| {
            limits.map_or_else(TokenLimits::default, |limits| limits.of(model))
        };

        let fetching = {
            let mut entries = self.entries.lock();
            match entries.get_mut(&key) {
                Some(State::Done {
                    limits,
                    at,
                    refreshing,
                }) => {
                    let limits = limits.clone();
                    if at.elapsed() >= self.timings.refresh_after && !*refreshing {
                        *refreshing = true;
                        self.spawn_refresh(key, request, Duration::ZERO);
                    }
                    return limits_of(limits);
                }
                Some(State::Fetching(fetching)) => fetching.clone(),
                None => {
                    let fetching = fetch(request.clone()).boxed().shared();
                    entries.insert(key.clone(), State::Fetching(fetching.clone()));
                    fetching
                }
            }
        };

        let fetched = fetching.await;
        let mut entries = self.entries.lock();
        if let Some(state @ State::Fetching(_)) = entries.get_mut(&key) {
            let unreachable = match &fetched {
                Fetched::Unreachable(cause) => Some(cause),
                Fetched::Limits(_) | Fetched::Answered => None,
            };
            *state = State::Done {
                limits: fetched.limits(),
                at: Instant::now(),
                refreshing: unreachable.is_some(),
            };
            if let Some(cause) = unreachable {
                tracing::warn!(
                    "Failed to read the token limits of model '{model}' from '{}', so /v1/models reports none for it, or for other models on that endpoint, until the endpoint answers; coding agents started meanwhile use their own defaults. Retrying in the background. Cause: {cause}",
                    request.url
                );
                self.spawn_refresh(key, request, self.timings.retry_first);
            }
        }
        limits_of(fetched.limits())
    }

    /// Ask for the listing again after `delay`, and again with growing delays while the
    /// endpoint does not answer, replacing the kept limits once it does.
    fn spawn_refresh(&self, key: String, request: Request, delay: Duration) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let entries = Arc::clone(&self.entries);
        let timings = self.timings;
        runtime.spawn(async move {
            let mut delay = delay;
            loop {
                tokio::time::sleep(delay).await;
                let fetched = fetch(request.clone()).await;
                if !matches!(fetched, Fetched::Unreachable(_)) {
                    if matches!(fetched, Fetched::Limits(_)) {
                        tracing::debug!("Read the token limits at '{}'", request.url);
                    }
                    entries.lock().insert(
                        key,
                        State::Done {
                            limits: fetched.limits(),
                            at: Instant::now(),
                            refreshing: false,
                        },
                    );
                    return;
                }
                delay = if delay.is_zero() {
                    timings.retry_first
                } else {
                    (delay * 2).min(timings.retry_max)
                };
            }
        });
    }
}

/// The URL, and a hash of what is sent with it: two credentials can see different listings,
/// and no credential is kept in the key itself.
fn cache_key(request: &Request) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, value) in &request.headers {
        name.as_str().hash(&mut hasher);
        value.as_bytes().hash(&mut hasher);
    }
    request.query.hash(&mut hasher);
    format!("{} {:016x}", request.url, hasher.finish())
}

async fn fetch(request: Request) -> Fetched {
    let response = request
        .client
        .get(&request.url)
        .headers(request.headers)
        .query(&request.query)
        .timeout(request.timeout)
        .send()
        .await;
    let unreachable = |error: reqwest::Error| {
        if error.is_timeout() {
            Fetched::Unreachable(format!(
                "it did not answer within {}s",
                request.timeout.as_secs()
            ))
        } else {
            Fetched::Unreachable(error.to_string())
        }
    };
    match response {
        Ok(response) if response.status().is_success() => match response.json::<Value>().await {
            Ok(json) => Fetched::Limits(Arc::new((request.read)(&json))),
            Err(error) if error.is_timeout() => unreachable(error),
            Err(error) => {
                tracing::debug!("Model metadata at '{}' is not JSON: {error}", request.url);
                Fetched::Answered
            }
        },
        Ok(response)
            if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                || response.status().is_server_error() =>
        {
            Fetched::Unreachable(format!("it answered {}", response.status()))
        }
        Ok(response) => {
            tracing::debug!(
                "Model metadata request to '{}' answered {}",
                request.url,
                response.status()
            );
            Fetched::Answered
        }
        Err(error) => unreachable(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::config::OpenAIConfig;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(server: &MockServer) -> OpenAIConfig {
        OpenAIConfig::new().with_api_base(format!("{}/v1", server.uri()))
    }

    fn cache(timeout: Duration, retry: Duration) -> ListingCache {
        ListingCache::new(Timings {
            timeout,
            retry_first: retry,
            retry_max: retry,
            refresh_after: Duration::from_mins(10),
        })
    }

    async fn limits(cache: &ListingCache, config: &OpenAIConfig, model: &str) -> TokenLimits {
        cache.get(config, "/models", model, Limits::listed).await
    }

    #[tokio::test]
    async fn an_openai_compatible_listing_reports_the_limits_of_its_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"object": "list", "data": [
                    {"id": "other", "max_model_len": 1},
                    {"id": "qwen", "meta": {"n_ctx": 65_536}},
                    {"id": "routed", "context_length": 200_000, "top_provider": {"max_completion_tokens": 64_000}},
                ]}),
            ))
            .mount(&server)
            .await;
        let config = config(&server);
        assert_eq!(
            listed_token_limits(&config, "qwen").await,
            TokenLimits {
                context_window: Some(65_536),
                max_output_tokens: None,
            }
        );
        assert_eq!(
            listed_token_limits(&config, "routed").await,
            TokenLimits {
                context_window: Some(200_000),
                max_output_tokens: Some(64_000),
            }
        );
        assert_eq!(
            listed_token_limits(&config, "missing").await,
            TokenLimits::default()
        );
    }

    #[tokio::test]
    async fn a_model_object_reports_its_limits() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models/claude-opus"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"id": "claude-opus", "max_input_tokens": 200_000, "max_tokens": 64_000}),
            ))
            .mount(&server)
            .await;
        assert_eq!(
            retrieved_token_limits(&config(&server), "claude-opus").await,
            TokenLimits {
                context_window: Some(200_000),
                max_output_tokens: Some(64_000),
            }
        );
    }

    /// Every model on one endpoint reads the listing one request fetched.
    #[tokio::test]
    async fn models_on_one_endpoint_share_one_listing_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": [
                        {"id": "a", "context_length": 1_000},
                        {"id": "b", "context_length": 2_000},
                    ]}))
                    .set_delay(Duration::from_millis(200)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let cache = cache(Duration::from_secs(5), Duration::from_secs(60));
        let config = config(&server);
        // Concurrent loads wait on the one request in flight; a later one reads its copy.
        let (a, b) = tokio::join!(limits(&cache, &config, "a"), limits(&cache, &config, "b"));
        assert_eq!(
            (a.context_window, b.context_window),
            (Some(1_000), Some(2_000))
        );
        assert_eq!(
            limits(&cache, &config, "b").await.context_window,
            Some(2_000)
        );
        server.verify().await;
    }

    /// An endpoint that hangs costs its first model the timeout, and the rest nothing; its
    /// limits appear once a background retry gets an answer.
    #[tokio::test]
    async fn a_hung_listing_delays_one_model_and_is_retried_in_the_background() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": [{"id": "a", "context_length": 1_000}]}))
                    .set_delay(Duration::from_secs(30)),
            )
            // The first model's request, and the first background retry.
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": [{"id": "a", "context_length": 1_000}]})),
            )
            .mount(&server)
            .await;
        let cache = cache(Duration::from_millis(300), Duration::from_millis(100));
        let config = config(&server);

        let started = Instant::now();
        assert_eq!(limits(&cache, &config, "a").await, TokenLimits::default());
        let first = started.elapsed();
        assert!(first >= Duration::from_millis(300), "{first:?}");

        let started = Instant::now();
        assert_eq!(limits(&cache, &config, "b").await, TokenLimits::default());
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "a second model does not wait on the endpoint again: {:?}",
            started.elapsed()
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        while limits(&cache, &config, "a").await.context_window.is_none() {
            assert!(
                Instant::now() < deadline,
                "the retry never filled the limits"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// A server still loading its model answers its listing with 503, as llama.cpp's does;
    /// the limits appear once it has loaded, not only once the kept answer is old.
    #[tokio::test]
    async fn a_listing_that_answers_busy_is_asked_again_soon() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(503).set_body_json(
                json!({"error": {"code": 503, "message": "Loading model", "type": "unavailable_error"}}),
            ))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": [{"id": "a", "context_length": 1_000}]})),
            )
            .mount(&server)
            .await;
        let cache = cache(Duration::from_secs(5), Duration::from_millis(50));
        let config = config(&server);

        assert_eq!(limits(&cache, &config, "a").await, TokenLimits::default());
        let deadline = Instant::now() + Duration::from_secs(10);
        while limits(&cache, &config, "a").await.context_window.is_none() {
            assert!(
                Instant::now() < deadline,
                "the listing was not asked again after the 503; requests: {}",
                server.received_requests().await.unwrap_or_default().len()
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// A refresh the provider answers with a server error keeps the listing already read.
    #[tokio::test]
    async fn a_failed_refresh_keeps_the_kept_listing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"data": [{"id": "a", "context_length": 1_000}]})),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let cache = ListingCache::new(Timings {
            timeout: Duration::from_secs(5),
            retry_first: Duration::from_millis(20),
            retry_max: Duration::from_millis(20),
            refresh_after: Duration::ZERO,
        });
        let config = config(&server);

        assert_eq!(
            limits(&cache, &config, "a").await.context_window,
            Some(1_000)
        );
        // Old at once, so this read starts a refresh, which the server answers with 500.
        assert_eq!(
            limits(&cache, &config, "a").await.context_window,
            Some(1_000)
        );
        // Until a third request, which is sent only once the first refresh's answer has been
        // taken in, the listing must stay.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert_eq!(
                limits(&cache, &config, "a").await.context_window,
                Some(1_000),
                "a refresh answered with 500 replaced the listing"
            );
            if server.received_requests().await.unwrap_or_default().len() >= 3 {
                break;
            }
            assert!(Instant::now() < deadline, "the refresh was never retried");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn a_failing_model_api_leaves_the_limits_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        assert_eq!(
            listed_token_limits(&config(&server), "qwen").await,
            TokenLimits::default()
        );
        // Nothing listening at all.
        let unreachable = OpenAIConfig::new().with_api_base("http://127.0.0.1:9/v1");
        assert_eq!(
            listed_token_limits(&unreachable, "qwen").await,
            TokenLimits::default()
        );
    }

    #[test]
    fn the_cache_key_keeps_no_credential() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "authorization",
            "Bearer sk-secret".parse().expect("a header value"),
        );
        let request = Request {
            client: reqwest::Client::new(),
            url: "http://127.0.0.1:1/v1/models".to_string(),
            headers,
            query: Vec::new(),
            timeout: METADATA_TIMEOUT,
            read: Limits::listed,
        };
        let key = cache_key(&request);
        assert!(key.starts_with("http://127.0.0.1:1/v1/models "), "{key}");
        assert!(!key.contains("sk-secret"), "{key}");
    }
}
