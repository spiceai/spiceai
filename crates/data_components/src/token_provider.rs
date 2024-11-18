use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get token: {source}"))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait TokenProvider: Send + Sync + Debug {
    async fn get_token(&self) -> Result<String>;
}

pub struct StaticTokenProvider {
    token: Arc<str>,
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token.len()", &self.token.len())
            .finish()
    }
}

impl StaticTokenProvider {
    #[must_use]
    pub fn new(token: Arc<str>) -> Self {
        Self { token }
    }
}

#[async_trait]
impl TokenProvider for StaticTokenProvider {
    async fn get_token(&self) -> Result<String> {
        Ok(self.token.to_string())
    }
}
