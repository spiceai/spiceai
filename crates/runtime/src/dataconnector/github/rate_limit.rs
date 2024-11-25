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
pub enum RateLimitInfo {
    Primary(PrimaryRateLimitInfo),
    Secondary(SecondaryRateLimitInfo),
}

// A primary rate limit that is indicated by x-ratelimit headers
#[derive(Debug, Clone)]
pub struct PrimaryRateLimitInfo {
    pub limit: i32,
    pub remaining: i32,
    pub used: i32,
    pub reset_time: DateTime<Utc>,
    pub resource: String,
}

// A secondary rate limit that is indicated by a retry-after header
#[derive(Debug, Clone)]
pub struct SecondaryRateLimitInfo {
    pub retry_after: DateTime<Utc>,
}

// See https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api#checking-the-status-of-your-primary-rate-limit
impl RateLimitInfo {
    pub fn from_headers(headers: &HeaderMap) -> Option<Self> {
        let primary = Self::primary_rate_limit_from_headers(headers).map(RateLimitInfo::Primary);

        primary.or_else(|| {
            Self::secondary_rate_limit_from_headers(headers).map(RateLimitInfo::Secondary)
        })
    }

    fn secondary_rate_limit_from_headers(headers: &HeaderMap) -> Option<SecondaryRateLimitInfo> {
        headers
            .get("retry-after")
            .and_then(|h| h.to_str().ok().map(|s| s.parse::<u64>().ok()))
            .flatten()
            .map(|secs| Utc::now() + Duration::from_secs(secs))
            .map(|retry_after| SecondaryRateLimitInfo { retry_after })
    }

    fn primary_rate_limit_from_headers(headers: &HeaderMap) -> Option<PrimaryRateLimitInfo> {
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

        let MappedLocalTime::Single(reset_time) = Utc.timestamp_opt(reset, 0) else {
            unreachable!("timestamp_opt should never fail for Utc")
        };

        Some(PrimaryRateLimitInfo {
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
        tracing::debug!("GitHub rate limit headers: {:?}", headers);
        if let Some(rate_limit) = RateLimitInfo::from_headers(headers) {
            let mut api_limit = self.api_limit.write().await;
            tracing::debug!("GitHub rate limit: {:?}", rate_limit);
            *api_limit = Some(rate_limit);
        }
    }

    async fn check_rate_limit(&self) -> Result<(), data_components::graphql::Error> {
        // Check if we're rate limited based on the previous API response headers
        let api_limit_guard = self.api_limit.read().await;
        if let Some(api_limit) = &*api_limit_guard {
            tracing::debug!("GitHub rate limit: {:?}", api_limit);
            match api_limit {
                RateLimitInfo::Secondary(secondary) => {
                    tracing::debug!("GitHub rate limit retry-after: {}", secondary.retry_after);
                    let now = Utc::now();
                    let wait_duration = (secondary.retry_after - now)
                        .to_std()
                        .unwrap_or(Duration::from_secs(1));
                    let wait_duration_secs = wait_duration.as_secs();
                    tracing::warn!(
                    "GitHub API secondary rate limit exceeded. Waiting for {} second{} until {}.",
                    wait_duration_secs,
                    if wait_duration_secs == 1 { "" } else { "s" },
                        secondary.retry_after
                    );
                    tokio::time::sleep(wait_duration).await;
                }
                RateLimitInfo::Primary(primary) => {
                    // GitHub GraphQL requests consume more than 1 rate-limit unit, so add some buffer to ensure the proper handling
                    if primary.remaining <= 5 {
                        tracing::debug!("GitHub rate limit exceeded");
                        let now = Utc::now();
                        if now < primary.reset_time {
                            tracing::debug!("GitHub rate limit reset time: {}", primary.reset_time);
                            let wait_duration = (primary.reset_time - now)
                                .to_std()
                                .unwrap_or(Duration::from_secs(1));
                            let wait_duration_secs = wait_duration.as_secs();
                            tracing::warn!(
                                "GitHub API rate limit exceeded. Waiting for {} second{} until {}. Limit: {}, Used: {}, Resource: {}",
                                wait_duration_secs,
                                if wait_duration_secs == 1 { "" } else { "s" },
                                primary.reset_time,
                                primary.limit,
                                primary.used,
                                primary.resource
                            );
                            tokio::time::sleep(wait_duration).await;
                        }
                    } else {
                        tracing::debug!(
                            "GitHub API rate limit status: {}/{} remaining. Reset at {}. Resource: {}",
                            primary.remaining,
                            primary.limit,
                            primary.reset_time,
                            primary.resource
                        );
                    }
                }
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

    fn create_test_headers(values: HashMap<&'static str, String>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in values {
            headers.insert(
                key,
                HeaderValue::from_str(&value).expect("invalid header value"),
            );
        }
        headers
    }

    fn s(s: &'static str) -> String {
        s.to_string()
    }

    #[tokio::test]
    async fn test_rate_limiter_api_limits() {
        tokio::time::pause();
        let rate_limiter = GitHubRateLimiter::new();

        // Set up API headers indicating rate limit exceeded
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("0")),
            ("x-ratelimit-used", s("5000")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::milliseconds(200))
                    .timestamp()
                    .to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        let start = tokio::time::Instant::now();
        rate_limiter
            .check_rate_limit()
            .await
            .expect("rate limit check failed");
        let elapsed = start.elapsed();

        // Should have waited at least until reset time
        assert!(elapsed.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_rate_limiter_normal_operation() {
        let rate_limiter = GitHubRateLimiter::new();

        // Set up API headers indicating normal operation
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("4999")),
            ("x-ratelimit-used", s("1")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        // Should proceed without waiting
        let start = std::time::Instant::now();
        rate_limiter
            .check_rate_limit()
            .await
            .expect("rate limit check failed");
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() < 100);
    }
}
