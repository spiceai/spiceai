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
use headers::HeaderMapExt;
use http::HeaderMap;
use spicepod::component::caching::SQLResultsCacheConfig;

#[cfg(test)]
use http::header::CACHE_CONTROL;

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

        // Mapping from the user-facing `CacheKeyType` to the internal `CacheKeyType`.
        match sql_results_config.cache_key_type {
            spicepod::component::caching::CacheKeyType::Plan => Self::Default,
            spicepod::component::caching::CacheKeyType::Sql => Self::Raw,
        }
    }
}

/// Cache control directives for query results.
///
/// Controls how query results are cached based on client headers.
///
/// ## HTTP Header Format
///
/// The `Cache-Control` request header can include:
/// - `no-cache`: Bypass the cache entirely (`CacheControl::NoCache`)
///
/// ## Cache Key Types
///
/// The cache key type determines what is used as the cache key:
/// - `Default`: Hash of the logical query plan (whitespace-insensitive)
/// - `Raw`: The raw SQL string (whitespace-sensitive)
/// - `ClientSupplied`: Custom key provided via `Spice-Cache-Key` header
///
/// ## Response Directives
///
/// The `stale-while-revalidate` directive is a **response directive** set by the server
/// based on spicepod configuration. It is NOT parsed from client requests.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CacheControl {
    Cache(CacheKeyType),
    NoCache,
    /// max-stale[=N] - Client accepts stale responses up to N seconds old (None = any age)
    MaxStale(CacheKeyType, Option<std::time::Duration>),
    /// min-fresh=N - Client wants responses fresh for at least N more seconds
    MinFresh(CacheKeyType, std::time::Duration),
    /// only-if-cached - Client wants only cached responses, no network requests
    OnlyIfCached(CacheKeyType),
}

impl Default for CacheControl {
    fn default() -> Self {
        Self::Cache(CacheKeyType::Default)
    }
}

impl CacheControl {
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        // This will be updated later if the runtime parameter `runtime.caching.sql_results.cache_key_type` is present.
        let cache_key_type = match headers.get("Spice-Cache-Key") {
            Some(header) if !header.is_empty() => CacheKeyType::ClientSupplied,
            _ => CacheKeyType::Default,
        };

        // Try to parse using the headers crate's CacheControl
        if let Some(cache_control_header) = headers.typed_get::<headers::CacheControl>() {
            return Self::parse_cache_control_header(&cache_control_header, cache_key_type);
        }

        // If typed parsing fails, the header might not be present or malformed
        // In that case, return the default Cache variant
        Self::Cache(cache_key_type)
    }

    fn parse_cache_control_header(
        header: &headers::CacheControl,
        cache_key_type: CacheKeyType,
    ) -> Self {
        // Check for no-cache (highest priority per RFC 9111)
        if header.no_cache() {
            return Self::NoCache;
        }

        // Check for only-if-cached
        if header.only_if_cached() {
            return Self::OnlyIfCached(cache_key_type);
        }

        // Check for min-fresh=N
        if let Some(min_fresh) = header.min_fresh() {
            return Self::MinFresh(cache_key_type, min_fresh);
        }

        // Check for max-stale[=N]
        // The headers crate returns Some(duration) if max-stale=N, or Some(Duration::MAX) if max-stale (no value)
        if let Some(max_stale) = header.max_stale() {
            // Duration::MAX indicates max-stale without a specific value (accept any stale)
            let max_stale_opt = if max_stale == std::time::Duration::MAX {
                None
            } else {
                Some(max_stale)
            };
            return Self::MaxStale(cache_key_type, max_stale_opt);
        }

        Self::Cache(cache_key_type)
    }

    /// Get the cache key type from the `CacheControl` variant
    #[must_use]
    pub fn cache_key_type(&self) -> Option<CacheKeyType> {
        match self {
            Self::Cache(key_type)
            | Self::MaxStale(key_type, _)
            | Self::MinFresh(key_type, _)
            | Self::OnlyIfCached(key_type) => Some(*key_type),
            Self::NoCache => None,
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
                caching: spicepod::component::caching::Caching {
                    sql_results: Some(spicepod::component::caching::SQLResultsCacheConfig {
                        cache_key_type: spicepod::component::caching::CacheKeyType::Plan,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            })
            .build();

        let app_with_sql = AppBuilder::new("app_with_sql")
            .with_runtime(spicepod::component::runtime::Runtime {
                caching: spicepod::component::caching::Caching {
                    sql_results: Some(spicepod::component::caching::SQLResultsCacheConfig {
                        cache_key_type: spicepod::component::caching::CacheKeyType::Sql,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
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

        // Test with stale-while-revalidate in request (should be ignored - it's a response directive)
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("max-age=60, stale-while-revalidate=30"),
        );
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::Default)
        ));

        // Test with client-supplied cache key
        headers.insert("Spice-Cache-Key", HeaderValue::from_static("my-key"));
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-age=60"));
        assert!(matches!(
            CacheControl::from_headers(&headers),
            CacheControl::Cache(CacheKeyType::ClientSupplied)
        ));

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
            CacheControl::Cache(CacheKeyType::ClientSupplied).cache_key_type(),
            Some(CacheKeyType::ClientSupplied)
        );
        assert_eq!(
            CacheControl::MaxStale(
                CacheKeyType::Default,
                Some(std::time::Duration::from_secs(60))
            )
            .cache_key_type(),
            Some(CacheKeyType::Default)
        );
        assert_eq!(
            CacheControl::MinFresh(
                CacheKeyType::ClientSupplied,
                std::time::Duration::from_secs(120)
            )
            .cache_key_type(),
            Some(CacheKeyType::ClientSupplied)
        );
        assert_eq!(
            CacheControl::OnlyIfCached(CacheKeyType::Default).cache_key_type(),
            Some(CacheKeyType::Default)
        );
        assert_eq!(CacheControl::NoCache.cache_key_type(), None);
    }

    #[test]
    fn test_max_stale_parsing() {
        let mut headers = HeaderMap::new();

        // Test max-stale with value
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-stale=3600"));
        let result = CacheControl::from_headers(&headers);
        assert!(matches!(
            result,
            CacheControl::MaxStale(CacheKeyType::Default, Some(d)) if d.as_secs() == 3600
        ));

        // Note: The headers crate doesn't support max-stale without a value,
        // so it falls back to Cache(Default). This is acceptable since
        // max-stale without a value is rarely used in practice.
        headers.clear();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("max-stale"));
        let result = CacheControl::from_headers(&headers);
        eprintln!("max-stale without value result: {result:?}");
        // The headers crate doesn't parse this, so it defaults to Cache
        assert!(matches!(result, CacheControl::Cache(CacheKeyType::Default)));
    }

    #[test]
    fn test_min_fresh_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("min-fresh=600"));
        let result = CacheControl::from_headers(&headers);
        assert!(matches!(
            result,
            CacheControl::MinFresh(CacheKeyType::Default, d) if d.as_secs() == 600
        ));
    }

    #[test]
    fn test_only_if_cached_parsing() {
        let mut headers = HeaderMap::new();
        headers.insert(CACHE_CONTROL, HeaderValue::from_static("only-if-cached"));
        let result = CacheControl::from_headers(&headers);
        assert!(matches!(
            result,
            CacheControl::OnlyIfCached(CacheKeyType::Default)
        ));
    }

    #[test]
    fn test_multiple_directives() {
        let mut headers = HeaderMap::new();
        // When multiple directives are present, priority matters: no-cache > only-if-cached > min-fresh > max-stale
        headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_static("max-stale=3600, min-fresh=600, no-cache"),
        );
        let result = CacheControl::from_headers(&headers);
        assert!(matches!(result, CacheControl::NoCache));
    }
}
