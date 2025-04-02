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

use http::{header::CACHE_CONTROL, HeaderMap, HeaderValue};

/// HTTP header to control cache key generation strategy.
///
/// Accepts:
/// - `default`: Use the server's default logic (e.g., `LogicalPlan` hash).
/// - `raw`: Use the raw input (e.g., unparsed SQL string) as the cache key.
pub const CACHE_KEY_STRATEGY: &str = "cache-key-strategy";

#[derive(Debug, Clone, Copy, Default)]
pub enum CacheKeyStrategy {
    #[default]
    Default,
    Raw,
}

impl CacheKeyStrategy {
    #[must_use]
    pub fn from_header_value(value: &HeaderValue) -> Self {
        let value_str = value.to_str().unwrap_or_default();
        match value_str {
            "raw" => Self::Raw,
            _ => Self::Default,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CacheControl {
    Cache(CacheKeyStrategy),
    NoCache,
}

impl Default for CacheControl {
    fn default() -> Self {
        Self::Cache(CacheKeyStrategy::Default)
    }
}

impl CacheControl {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let cache_key_strategy = headers
            .get(CACHE_KEY_STRATEGY)
            .map(CacheKeyStrategy::from_header_value)
            .unwrap_or_default();

        let Some(cache_control) = headers.get(CACHE_CONTROL) else {
            return Self::Cache(cache_key_strategy);
        };
        let Ok(cache_control_str) = cache_control.to_str() else {
            return Self::Cache(cache_key_strategy);
        };

        match cache_control_str {
            "no-cache" => Self::NoCache,
            _ => Self::Cache(cache_key_strategy),
        }
    }
}
