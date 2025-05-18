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
#![allow(clippy::missing_errors_doc)]

use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

use crate::TokenProvider;

#[derive(Default, Clone)]
pub struct TokenProviderRegistry {
    pub token_provider_registry: Arc<RwLock<HashMap<String, Arc<dyn TokenProvider>>>>,
}

impl TokenProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            token_provider_registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get or create a token provider for the given key and provider type.
    ///
    /// If the provider already exists, it will be returned.
    /// If the provider does not exist, it will be created using the provided factory function.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to use for the token provider.
    /// * `factory` - The factory function to use to create the token provider.
    ///
    /// # Returns
    ///
    /// A token provider, or an error if the provider could not be created.
    pub async fn get_or_create_provider<P, E, F, Fut>(
        &self,
        key: String,
        factory: F,
    ) -> Result<Arc<dyn TokenProvider>, E>
    where
        P: TokenProvider + 'static,
        E: std::error::Error + Send + Sync,
        Fut: std::future::Future<Output = Result<P, E>> + Send,
        F: FnOnce() -> Fut + Send,
    {
        {
            let registry = self.token_provider_registry.read().await;
            if let Some(provider) = registry.get(&key) {
                tracing::debug!("Using existing token provider for key: {key}",);
                return Ok(Arc::clone(provider));
            }
        }

        let mut registry = self.token_provider_registry.write().await;

        if let Some(provider) = registry.get(&key) {
            tracing::debug!("Using existing token provider for key: {key}",);
            return Ok(Arc::clone(provider));
        }

        tracing::debug!("Creating new token provider for key: {key}",);

        let provider = factory().await?;
        let provider_arc = Arc::new(provider) as Arc<dyn TokenProvider>;

        registry.insert(key, Arc::clone(&provider_arc));

        Ok(provider_arc)
    }

    /// Get a token provider for the given key and provider type.
    pub async fn get(&self, key: String) -> Option<Arc<dyn TokenProvider>> {
        let registry = self.token_provider_registry.read().await;
        if let Some(provider) = registry.get(&key) {
            return Some(Arc::clone(provider));
        }

        None
    }
}
