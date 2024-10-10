use std::sync::Arc;

use async_trait::async_trait;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to get token: {source}"))]
    UnableToGetToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Unable to refresh token: {source}"))]
    UnableToRefreshToken {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[async_trait]
pub trait TokenProvider: Send + Sync {
    fn is_refreshable(&self) -> bool;

    async fn get_token(&self) -> Result<String>;

    async fn refresh_token(&self) -> Result<()>;
}

pub struct StaticTokenProvider {
    token: Arc<str>,
}

impl DefaultTokenWrapper {
    #[must_use]
    pub fn new(token: Arc<str>) -> Self {
        Self { token }
    }
}

#[async_trait]
impl TokenWrapper for DefaultTokenWrapper {
    fn is_refreshable(&self) -> bool {
        false
    }

    async fn get_token(&self) -> Result<String> {
        Ok(self.token.to_string())
    }

    async fn refresh_token(&self) -> Result<()> {
        Ok(())
    }
}
