/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use data_components::rate_limit::RateLimiter;

use super::{Result, client::GraphQLClient, client::UnnestBehavior};
use arrow::datatypes::SchemaRef;
use datafusion::execution::memory_pool::MemoryPool;
use runtime_rate_control::RateController;
use snafu::ResultExt;
use std::sync::Arc;
use token_provider::TokenProvider;
use tokio::sync::Semaphore;

use url::Url;

pub struct GraphQLClientBuilder {
    endpoint: Url,
    json_pointer: Option<Arc<str>>,
    unnest_behavior: UnnestBehavior,
    token_provider: Option<Arc<dyn TokenProvider>>,
    user: Option<String>,
    pass: Option<String>,
    schema: Option<SchemaRef>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
    rate_controller: Option<Arc<RateController>>,
    semaphore: Option<Arc<Semaphore>>,
    auth_header: Option<reqwest::header::HeaderName>,
    nested_pager: Option<super::client::NestedConnectionPager>,
    dataset: Option<Arc<str>>,
    connector: &'static str,
}

impl GraphQLClientBuilder {
    #[must_use]
    pub fn new(endpoint: Url, unnest_behavior: UnnestBehavior) -> Self {
        Self {
            endpoint,
            unnest_behavior,
            json_pointer: None,
            token_provider: None,
            user: None,
            pass: None,
            schema: None,
            rate_limiter: None,
            rate_controller: None,
            semaphore: None,
            auth_header: None,
            nested_pager: None,
            dataset: None,
            connector: "graphql",
        }
    }

    #[must_use]
    pub fn with_json_pointer(mut self, json_pointer: Option<&str>) -> Self {
        self.json_pointer = json_pointer.map(Arc::from);
        self
    }

    #[must_use]
    pub fn with_token_provider(mut self, token_provider: Option<Arc<dyn TokenProvider>>) -> Self {
        self.token_provider = token_provider;
        self
    }

    #[must_use]
    pub fn with_user(mut self, user: Option<String>) -> Self {
        self.user = user;
        self
    }

    #[must_use]
    pub fn with_pass(mut self, pass: Option<String>) -> Self {
        self.pass = pass;
        self
    }

    #[must_use]
    pub fn with_schema(mut self, schema: Option<SchemaRef>) -> Self {
        self.schema = schema;
        self
    }

    #[must_use]
    pub fn with_rate_limiter(mut self, rate_limiter: Option<Arc<dyn RateLimiter>>) -> Self {
        self.rate_limiter = rate_limiter;
        self
    }

    #[must_use]
    pub fn with_rate_controller(mut self, rate_controller: Option<Arc<RateController>>) -> Self {
        self.rate_controller = rate_controller;
        self
    }

    #[must_use]
    pub fn with_semaphore(mut self, semaphore: Option<Arc<Semaphore>>) -> Self {
        self.semaphore = semaphore;
        self
    }

    #[must_use]
    pub fn with_auth_header(mut self, auth_header: Option<reqwest::header::HeaderName>) -> Self {
        self.auth_header = auth_header;
        self
    }

    #[must_use]
    pub fn with_nested_pager(
        mut self,
        pager: Option<super::client::NestedConnectionPager>,
    ) -> Self {
        self.nested_pager = pager;
        self
    }

    /// Name the dataset and connector this client reads for, which a refused response
    /// names and links.
    #[must_use]
    pub fn with_source(mut self, dataset: impl Into<Arc<str>>, connector: &'static str) -> Self {
        self.dataset = Some(dataset.into());
        self.connector = connector;
        self
    }

    /// Build the client over `client`, built here to decode compressed responses itself
    /// (see `http_body_limit::decode_manually`), so a response that expands past what real
    /// data compresses to is refused as a compression bomb as it decodes. Every response
    /// body, as decompressed, is charged to `memory_pool`, the runtime's query memory
    /// pool, while it is held.
    ///
    /// # Errors
    ///
    /// Returns an error if `client` cannot be built, or the configuration is invalid.
    pub fn build(
        self,
        client: reqwest::ClientBuilder,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Result<GraphQLClient> {
        let client = http_body_limit::decode_manually(client)
            .build()
            .context(super::ReqwestInternalSnafu)?;
        let graphql_client = GraphQLClient::new(
            client,
            memory_pool,
            self.endpoint,
            self.json_pointer.as_deref(),
            self.token_provider,
            self.user,
            self.pass,
            self.unnest_behavior,
            self.schema,
            self.rate_limiter,
            self.rate_controller,
            self.semaphore,
            self.auth_header,
        )?;
        Ok(graphql_client
            .with_nested_pager(self.nested_pager)
            .with_source(self.dataset, self.connector))
    }
}
