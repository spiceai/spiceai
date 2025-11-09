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
use std::time::Duration;

use app::App;
use http::{HeaderMap, header::CACHE_CONTROL};
use spicepod::component::caching::SQLResultsCacheConfig;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub enum CacheKeyType {
    /// Use the server's default logic (e.g., `LogicalPlan` hash).
    #[default]
    Default,
    /// Use the raw input (e.g., unparsed SQL string) as the cache key.
    Raw,
    /// Use a client-supplied cache key via the Spice-Cache-Key header.
    ClientSupplied,
}

impl CacheKeyType {
    #[must_use]
    pub fn from_app_runtime(app: Option<&Arc<App>>) -> Self {
        let Some(app) = app else {
            return Self::Default;
        };

        let sql_results_config = app
            .runtime
            .caching
            .sql_results
            .clone()
            .unwrap_or(SQLResultsCacheConfig::default());

        let cache_key_type = app.runtime.results_cache.as_ref().map_or_else(
            || sql_results_config.cache_key_type,
            |c| c.cache_key_type, // while results_cache is being deprecated, it has higher priority than sql_results
        );

        // Mapping from the user-facing `CacheKeyType` to the internal `CacheKeyType`.
        match cache_key_type {
            spicepod::component::caching::CacheKeyType::Plan => Self::Default,
            spicepod::component::caching::CacheKeyType::Sql => Self::Raw,
        }
    }
}

/// Cache control directives for query results.
///
/// # Stale-While-Revalidate
///
/// The `CacheWithStaleWhileRevalidate` variant enables serving stale content while asynchronously
/// refreshing it in the background. This is useful for improving response times when cache entries
/// are slightly stale.
///
/// When a cached entry is past its TTL but within the stale-while-revalidate window:
/// 1. The stale data is immediately returned to the client
/// 2. A background task refreshes the cache entry asynchronously
///
/// ## Example Usage
///
/// ```text
/// Cache-Control: max-age=60, stale-while-revalidate=30
/// ```
///
/// This means:
/// - Fresh content is served for 60 seconds after caching
/// - Between 60-90 seconds, stale content is served while a background refresh happens
/// - After 90 seconds, the cache entry is considered fully expired
///
/// ## HTTP Header Format
///
/// The `Cache-Control` header can include multiple directives:
/// - `no-cache`: Bypass the cache entirely (`CacheControl::NoCache`)
/// - `stale-while-revalidate=<seconds>`: Enable stale-while-revalidate with the specified window
///
/// ## Cache Key Types
///
/// The cache key type determines what is used as the cache key:
/// - `Default`: Hash of the logical query plan (whitespace-insensitive)
/// - `Raw`: The raw SQL string (whitespace-sensitive)
/// - `ClientSupplied`: Custom key provided via `Spice-Cache-Key` header
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CacheControl {
    Cache(CacheKeyType),
    /// Cache with stale-while-revalidate support.
    /// The Duration specifies how long after expiry stale content can be served while revalidating.
    CacheWithStaleWhileRevalidate(CacheKeyType, Duration),
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
        let cache_key_type = match headers.get("Spice-Cache-Key") {
            Some(header) if !header.is_empty() => CacheKeyType::ClientSupplied,
            _ => CacheKeyType::Default,
        };

        match headers.get(CACHE_CONTROL).and_then(|h| h.to_str().ok()) {
            Some(value) if value.contains("no-cache") => Self::NoCache,
            Some(value) => {
                // Parse stale-while-revalidate if present
                let stale_while_revalidate = Self::parse_stale_while_revalidate(value);
                match stale_while_revalidate {
                    Some(duration) => Self::CacheWithStaleWhileRevalidate(cache_key_type, duration),
                    None => Self::Cache(cache_key_type),
                }
            }
            _ => Self::Cache(cache_key_type),
        }
    }

    /// Get the cache key type from the `CacheControl` variant
    #[must_use]
    pub fn cache_key_type(&self) -> Option<CacheKeyType> {
        match self {
            Self::Cache(key_type) | Self::CacheWithStaleWhileRevalidate(key_type, _) => {
                Some(*key_type)
            }
            Self::NoCache => None,
        }
    }

    /// Get the stale-while-revalidate duration if present
    #[must_use]
    pub fn stale_while_revalidate_duration(&self) -> Option<Duration> {
        match self {
            Self::CacheWithStaleWhileRevalidate(_, duration) => Some(*duration),
            _ => None,
        }
    }

    /// Parse the stale-while-revalidate directive from a Cache-Control header value.
    /// Format: "stale-while-revalidate=<seconds>"
    fn parse_stale_while_revalidate(header_value: &str) -> Option<Duration> {
        for directive in header_value.split(',') {
            let directive = directive.trim();
            if let Some(value) = directive
                .strip_prefix("stale-while-revalidate=")
                .and_then(|v| v.trim().parse::<u64>().ok())
            {
                return Some(Duration::from_secs(value));
            }
        }
        None
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

        // Test with stale-while-revalidate
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("max-age=60, stale-while-revalidate=30"),
        );
        match CacheControl::from_headers(&headers) {
            CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, duration) => {
                assert_eq!(duration, Duration::from_secs(30));
            }
            _ => panic!("Expected CacheWithStaleWhileRevalidate"),
        }

        // Test with stale-while-revalidate only
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("stale-while-revalidate=120"),
        );
        match CacheControl::from_headers(&headers) {
            CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, duration) => {
                assert_eq!(duration, Duration::from_secs(120));
            }
            _ => panic!("Expected CacheWithStaleWhileRevalidate"),
        }

        // Test with client-supplied cache key and stale-while-revalidate
        headers.insert("Spice-Cache-Key", HeaderValue::from_static("my-key"));
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("stale-while-revalidate=60"),
        );
        match CacheControl::from_headers(&headers) {
            CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::ClientSupplied, duration) => {
                assert_eq!(duration, Duration::from_secs(60));
            }
            _ => panic!("Expected CacheWithStaleWhileRevalidate with ClientSupplied key"),
        }

        // Test with invalid header value
        headers.remove("Spice-Cache-Key");
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_bytes(b"\xFF\xFF").expect("Valid header value"),
        );
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::Default)
        ));
    }

    #[test]
    fn test_cache_key_type_helper() {
        assert_eq!(
            CacheControl::Cache(CacheKeyType::Default).cache_key_type(),
            Some(CacheKeyType::Default)
        );
        assert_eq!(
            CacheControl::CacheWithStaleWhileRevalidate(
                CacheKeyType::ClientSupplied,
                Duration::from_secs(60)
            )
            .cache_key_type(),
            Some(CacheKeyType::ClientSupplied)
        );
        assert_eq!(CacheControl::NoCache.cache_key_type(), None);
    }

    #[test]
    fn test_stale_while_revalidate_duration_helper() {
        assert_eq!(
            CacheControl::Cache(CacheKeyType::Default).stale_while_revalidate_duration(),
            None
        );
        assert_eq!(
            CacheControl::CacheWithStaleWhileRevalidate(
                CacheKeyType::Default,
                Duration::from_secs(120)
            )
            .stale_while_revalidate_duration(),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            CacheControl::NoCache.stale_while_revalidate_duration(),
            None
        );
    }
}
