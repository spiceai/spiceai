use std::sync::Arc;

use async_trait::async_trait;

#[async_trait]
pub trait TokenWrapper: Send + Sync {
    fn is_refreshable(&self) -> bool;

    async fn get_token(&self) -> String;

    async fn refresh_token(&self);
}

pub struct DefaultTokenWrapper {
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

    async fn get_token(&self) -> String {
        self.token.to_string()
    }

    async fn refresh_token(&self) {}
}
