/*
Copyright 2025 The Spice.ai OSS Authors

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

use crate::{AnyErrorResult, SecretStore};
use async_trait::async_trait;
use secrecy::SecretString;

/// Trait for expanding secrets via the cluster service.
/// This abstracts over the different channel types that may be used.
#[async_trait]
pub trait ClusterSecretExpander: Send + Sync {
    async fn expand_secret(&self, executor_id: &str, key: &str) -> Result<String, String>;
}

/// Used by cluster mode to resolve secrets declared in the scheduler
/// via the internal cluster gRPC service.
pub struct SchedulerRPCSecretStore {
    executor_id: String,
    expander: Box<dyn ClusterSecretExpander>,
}

impl SchedulerRPCSecretStore {
    #[must_use]
    pub fn new(expander: Box<dyn ClusterSecretExpander>, executor_id: String) -> Self {
        Self {
            executor_id,
            expander,
        }
    }
}

#[async_trait]
impl SecretStore for SchedulerRPCSecretStore {
    async fn get_secret(&self, key: &str) -> AnyErrorResult<Option<SecretString>> {
        tracing::trace!("SchedulerRPCSecretStore: Requesting secret {}", key);

        let value = self
            .expander
            .expand_secret(&self.executor_id, key)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;

        Ok(Some(SecretString::from(value)))
    }
}
