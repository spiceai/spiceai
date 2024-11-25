/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_trait::async_trait;
use chrono::{DateTime, MappedLocalTime, TimeZone, Utc};
use data_components::graphql::rate_limit::RateLimiter;
use reqwest::header::HeaderMap;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct GitHubRateLimiter {
    // Track API response headers rate limits
    api_limit: Arc<RwLock<Option<RateLimitInfo>>>,
}

#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    pub limit: i32,
    pub remaining: i32,
    pub used: i32,
    pub reset_time: DateTime<Utc>,
    pub resource: String,
}

// See https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api#checking-the-status-of-your-primary-rate-limit
impl RateLimitInfo {
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let limit = headers
            .get("x-ratelimit-limit")?
            .to_str()
            .ok()?
            .parse::<i32>()
            .ok()?;
        let remaining = headers
            .get("x-ratelimit-remaining")?
            .to_str()
            .ok()?
            .parse::<i32>()
            .ok()?;
        let used = headers
            .get("x-ratelimit-used")?
            .to_str()
            .ok()?
            .parse::<i32>()
            .ok()?;
        let reset = headers
            .get("x-ratelimit-reset")?
            .to_str()
            .ok()?
            .parse::<i64>()
            .ok()?;
        let resource = headers
            .get("x-ratelimit-resource")?
            .to_str()
            .ok()?
            .to_string();

        let reset_time = match Utc.timestamp_opt(reset, 0) {
            MappedLocalTime::Single(t) => t,
            _ => unreachable!("timestamp_opt should never fail for Utc"),
        };

        Some(Self {
            limit,
            remaining,
            used,
            reset_time,
            resource,
        })
    }
}

impl GitHubRateLimiter {
    pub fn new() -> Self {
        Self {
            api_limit: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl RateLimiter for GitHubRateLimiter {
    async fn update_from_headers(&self, headers: &HeaderMap) {
        if let Some(rate_limit) = RateLimitInfo::from_headers(headers) {
            let mut api_limit = self.api_limit.write().await;
            *api_limit = Some(rate_limit);
        }
    }

    async fn check_rate_limit(&self) -> Result<(), data_components::graphql::Error> {
        // Check if we're rate limited based on the previous API response headers
        let api_limit_guard = self.api_limit.read().await;
        if let Some(api_limit) = &*api_limit_guard {
            if api_limit.remaining <= 0 {
                let now = Utc::now();
                if now < api_limit.reset_time {
                    let wait_duration = (api_limit.reset_time - now)
                        .to_std()
                        .unwrap_or(Duration::from_secs(1));
                    let wait_duration_secs = wait_duration.as_secs();
                    tracing::warn!(
                        "GitHub API rate limit exceeded. Waiting for {} second{} until {}. Limit: {}, Used: {}, Resource: {}",
                        wait_duration_secs,
                        if wait_duration_secs == 1 { "" } else { "s" },
                        api_limit.reset_time,
                        api_limit.limit,
                        api_limit.used,
                        api_limit.resource
                    );
                    tokio::time::sleep(wait_duration).await;
                }
            } else {
                tracing::debug!(
                    "GitHub API rate limit status: {}/{} remaining. Reset at {}. Resource: {}",
                    api_limit.remaining,
                    api_limit.limit,
                    api_limit.reset_time,
                    api_limit.resource
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use reqwest::header::HeaderValue;
    use std::collections::HashMap;

    fn create_test_headers(values: HashMap<&str, &str>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in values {
            headers.insert(key, HeaderValue::from_str(value).unwrap());
        }
        headers
    }

    #[tokio::test]
    async fn test_rate_limiter_internal_limit() {
        let mut rate_limiter = RateLimiter::new();

        // Make max-1 requests
        for _ in 0..GITHUB_MAX_REQUESTS_PER_HOUR - 1 {
            rate_limiter.check_rate_limit().await.unwrap();
        }

        // Next request should work
        rate_limiter.check_rate_limit().await.unwrap();

        // This request should trigger rate limiting
        let start = std::time::Instant::now();
        rate_limiter.check_rate_limit().await.unwrap();
        let elapsed = start.elapsed();

        // Should have waited close to an hour
        assert!(elapsed.as_secs() > 0);
    }

    #[tokio::test]
    async fn test_rate_limiter_api_limits() {
        let mut rate_limiter = RateLimiter::new();

        // Set up API headers indicating rate limit exceeded
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", "5000"),
            ("x-ratelimit-remaining", "0"),
            ("x-ratelimit-used", "5000"),
            (
                "x-ratelimit-reset",
                &(Utc::now() + Duration::milliseconds(100))
                    .timestamp()
                    .to_string(),
            ),
            ("x-ratelimit-resource", "graphql"),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        let start = std::time::Instant::now();
        rate_limiter.check_rate_limit().await.unwrap();
        let elapsed = start.elapsed();

        // Should have waited at least until reset time
        assert!(elapsed.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_rate_limiter_normal_operation() {
        let mut rate_limiter = RateLimiter::new();

        // Set up API headers indicating normal operation
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", "5000"),
            ("x-ratelimit-remaining", "4999"),
            ("x-ratelimit-used", "1"),
            (
                "x-ratelimit-reset",
                &(Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", "graphql"),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        // Should proceed without waiting
        let start = std::time::Instant::now();
        rate_limiter.check_rate_limit().await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 100);
    }
}
