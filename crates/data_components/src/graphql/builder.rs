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

use crate::{rate_limit::RateLimiter, token_provider::TokenProvider};

use super::{client::GraphQLClient, Result};
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use url::Url;

pub struct GraphQLClientBuilder {
    endpoint: Url,
    json_pointer: Option<Arc<str>>,
    unnest_depth: usize,
    token_provider: Option<Arc<dyn TokenProvider>>,
    user: Option<String>,
    pass: Option<String>,
    schema: Option<SchemaRef>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
}

impl GraphQLClientBuilder {
    #[must_use]
    pub fn new(endpoint: Url, unnest_depth: usize) -> Self {
        Self {
            endpoint,
            unnest_depth,
            json_pointer: None,
            token_provider: None,
            user: None,
            pass: None,
            schema: None,
            rate_limiter: None,
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

    pub fn build(self, client: reqwest::Client) -> Result<GraphQLClient> {
        GraphQLClient::new(
            client,
            self.endpoint,
            self.json_pointer.as_deref(),
            self.token_provider,
            self.user,
            self.pass,
            self.unnest_depth,
            self.schema,
            self.rate_limiter,
        )
    }
}
