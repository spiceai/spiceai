use super::{client::GraphQLClient, GraphQLOptimizer, Result};
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

use url::Url;

pub struct GraphQLClientBuilder {
    endpoint: Url,
    json_pointer: Option<Arc<str>>,
    unnest_depth: usize,
    token: Option<Arc<str>>,
    user: Option<String>,
    pass: Option<String>,
    schema: Option<SchemaRef>,
    optimizer: Option<Arc<dyn GraphQLOptimizer>>,
}

impl GraphQLClientBuilder {
    #[must_use]
    pub fn new(endpoint: Url, unnest_depth: usize) -> Self {
        Self {
            endpoint,
            unnest_depth,
            json_pointer: None,
            token: None,
            user: None,
            pass: None,
            schema: None,
            optimizer: None,
        }
    }

    #[must_use]
    pub fn with_json_pointer(mut self, json_pointer: Option<&str>) -> Self {
        self.json_pointer = json_pointer.map(Arc::from);
        self
    }

    #[must_use]
    pub fn with_token(mut self, token: Option<Arc<str>>) -> Self {
        self.token = token;
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
    pub fn with_optimizer(mut self, optimizer: Option<Arc<dyn GraphQLOptimizer>>) -> Self {
        self.optimizer = optimizer;
        self
    }

    pub fn build(self, client: reqwest::Client) -> Result<GraphQLClient> {
        GraphQLClient::new(
            client,
            self.endpoint,
            self.json_pointer.as_deref(),
            self.token.as_deref(),
            self.user,
            self.pass,
            self.unnest_depth,
            self.schema,
            self.optimizer,
        )
    }
}
