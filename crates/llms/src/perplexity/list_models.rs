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

//! Model listing functionality for Perplexity provider.
//!
//! Perplexity does not have a public models list API.
//! This module returns known models from documentation.

use async_trait::async_trait;

use crate::provider::{ListModels, ListModelsResult};

const PROVIDER_NAME: &str = "Perplexity";

/// Perplexity model lister that returns known models from documentation.
///
/// Perplexity does not provide a public API for listing models,
/// so this returns a static list of known Sonar models.
pub struct PerplexityModelLister;

impl PerplexityModelLister {
    /// Creates a new model lister.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Returns the list of known Perplexity Sonar models.
    #[must_use]
    pub fn known_models() -> Vec<String> {
        vec![
            "sonar".to_string(),
            "sonar-pro".to_string(),
            "sonar-reasoning".to_string(),
            "sonar-reasoning-pro".to_string(),
        ]
    }
}

impl Default for PerplexityModelLister {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ListModels for PerplexityModelLister {
    fn provider_name(&self) -> &'static str {
        PROVIDER_NAME
    }

    async fn list_models(&self) -> ListModelsResult<Vec<String>> {
        Ok(Self::known_models())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_known_models_not_empty() {
        let models = PerplexityModelLister::known_models();
        assert!(!models.is_empty());
        assert!(models.iter().any(|m| m.contains("sonar")));
    }

    #[tokio::test]
    async fn test_list_models() {
        let lister = PerplexityModelLister::new();
        let models = lister.list_models().await.expect("list should succeed");
        assert!(!models.is_empty());
    }
}
