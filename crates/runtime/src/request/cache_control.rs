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

use std::sync::Arc;

use app::App;
use http::{HeaderMap, header::CACHE_CONTROL};

#[derive(Debug, Clone, Copy, Default)]
pub enum CacheKeyType {
    /// Use the server's default logic (e.g., `LogicalPlan` hash).
    #[default]
    Default,
    /// Use the raw input (e.g., unparsed SQL string) as the cache key.
    Raw,
}

impl CacheKeyType {
    #[must_use]
    pub fn from_app_runtime(app: Option<&Arc<App>>) -> Self {
        let Some(app) = app else {
            return Self::Default;
        };

        // Mapping from the user-facing `CacheKeyType` to the internal `CacheKeyType`.
        match app.runtime.results_cache.as_ref().map_or_else(
            || app.runtime.caching.sql_results.cache_key_type,
            |c| c.cache_key_type,
        ) {
            spicepod::component::caching::CacheKeyType::Plan => Self::Default,
            spicepod::component::caching::CacheKeyType::Sql => Self::Raw,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CacheControl {
    Cache(CacheKeyType),
    NoCache,
}

impl Default for CacheControl {
    fn default() -> Self {
        Self::Cache(CacheKeyType::Default)
    }
}

impl CacheControl {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        // This will be updated later if the runtime parameter `runtime.results_cache.cache_key_type` is present.
        let cache_key_type = CacheKeyType::Default;

        let Some(cache_control) = headers.get(CACHE_CONTROL) else {
            return Self::Cache(cache_key_type);
        };
        let Ok(cache_control_str) = cache_control.to_str() else {
            return Self::Cache(cache_key_type);
        };

        match cache_control_str {
            "no-cache" => Self::NoCache,
            _ => Self::Cache(cache_key_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use http::HeaderValue;

    #[test]
    fn test_cache_key_type_from_app_runtime() {
        // Test with None app
        assert!(matches!(
            CacheKeyType::from_app_runtime(None),
            CacheKeyType::Default
        ));

        // Create test App instances
        let app_with_plan = AppBuilder::new("app_with_plan")
            .with_runtime(spicepod::component::runtime::Runtime {
                results_cache: Some(spicepod::component::caching::ResultsCache {
                    cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build();

        let app_with_sql = AppBuilder::new("app_with_sql")
            .with_runtime(spicepod::component::runtime::Runtime {
                results_cache: Some(spicepod::component::caching::ResultsCache {
                    cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .build();

        // Test with Plan cache key type
        assert!(matches!(
            CacheKeyType::from_app_runtime(Some(&Arc::new(app_with_plan))),
            CacheKeyType::Default
        ));

        // Test with SQL cache key type
        assert!(matches!(
            CacheKeyType::from_app_runtime(Some(&Arc::new(app_with_sql))),
            CacheKeyType::Raw
        ));
    }

    #[test]
    fn test_cache_control_default() {
        assert!(matches!(
            CacheControl::default(),
            CacheControl::Cache(CacheKeyType::Default)
        ));
    }

    #[test]
    fn test_cache_control_from_headers() {
        let mut headers = HeaderMap::new();

        // Test with empty headers
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::Default)
        ));

        // Test with no-cache header
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::NoCache
        ));

        // Test with different cache-control value
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=3600"));
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::Default)
        ));

        // Test with invalid header value
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_bytes(b"\xFF\xFF").expect("Valid header value"),
        );
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::Default)
        ));
    }
}
