/*
Copyright 2024 The Spice.ai OSS Authors

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

use crate::token_wrapper::TokenWrapper;

use super::{client::GraphQLClient, Result};
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use url::Url;

pub struct GraphQLClientBuilder {
    endpoint: Url,
    json_pointer: Option<Arc<str>>,
    unnest_depth: usize,
    wrapper: Option<Arc<dyn TokenWrapper>>,
    user: Option<String>,
    pass: Option<String>,
    schema: Option<SchemaRef>,
}

impl GraphQLClientBuilder {
    #[must_use]
    pub fn new(endpoint: Url, unnest_depth: usize) -> Self {
        Self {
            endpoint,
            unnest_depth,
            json_pointer: None,
            wrapper: None,
            user: None,
            pass: None,
            schema: None,
        }
    }

    #[must_use]
    pub fn with_json_pointer(mut self, json_pointer: Option<&str>) -> Self {
        self.json_pointer = json_pointer.map(Arc::from);
        self
    }

    #[must_use]
    pub fn with_token(mut self, wrapper: Option<Arc<dyn TokenWrapper>>) -> Self {
        self.wrapper = wrapper;
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

    pub fn build(self, client: reqwest::Client) -> Result<GraphQLClient> {
        GraphQLClient::new(
            client,
            self.endpoint,
            self.json_pointer.as_deref(),
            self.wrapper,
            self.user,
            self.pass,
            self.unnest_depth,
            self.schema,
        )
    }
}
