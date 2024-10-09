use std::sync::Arc;

use async_trait::async_trait;
use data_components::token_wrapper::TokenWrapper;
use tokio::sync::RwLock;

pub struct GitHubAppTokenWrapper {
    token: Arc<RwLock<String>>,
    app_id: Arc<str>,
    private_key: Arc<str>,
    installation_id: Arc<str>,
}

impl GitHubAppTokenWrapper {
    #[must_use]
    pub fn new(app_id: Arc<str>, private_key: Arc<str>, installation_id: Arc<str>) -> Self {
        Self {
            token: Arc::new(RwLock::new(String::new())),
            app_id,
            private_key,
            installation_id,
        }
    }

    async fn generate_token(&self) -> String {
        // TODO: Implement token generation
        "test".to_string()
    }
}

#[async_trait]
impl TokenWrapper for GitHubAppTokenWrapper {
    fn is_refreshable(&self) -> bool {
        true
    }

    async fn get_token(&self) -> String {
        match self.token.read().await.clone() {
            token if !token.is_empty() => token,
            _ => {
                let token = self.generate_token().await;
                let mut lock = self.token.write().await;
                lock.clone_from(&token);
                token
            }
        }
    }

    async fn refresh_token(&self) {
        let token = self.generate_token().await;
        let mut lock = self.token.write().await;
        lock.clone_from(&token);
    }
}
