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

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use data_components::rate_limit::RateLimiter;
use reqwest::header::HeaderMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Fraction of each GitHub rate limit we are willing to consume. The remaining
/// 10% is buffer against header lag, retries, and other users of the same token.
pub(crate) const GITHUB_RATE_LIMIT_FILL_NUM: u32 = 9;
pub(crate) const GITHUB_RATE_LIMIT_FILL_DEN: u32 = 10;

/// GitHub GraphQL secondary rate limit: 2,000 points per minute. A
/// non-mutation GraphQL request costs 1 point.
///
/// <https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api#secondary-rate-limits>
pub(crate) const GITHUB_GRAPHQL_SECONDARY_POINTS_PER_MINUTE: u32 = 2000;
pub(crate) const GITHUB_GRAPHQL_SECONDARY_QUERY_POINTS: u32 = 1;

/// 90% of `limit`, at least 1.
#[must_use]
pub(crate) fn fill_limited(limit: u32) -> u32 {
    let filled = u64::from(limit) * u64::from(GITHUB_RATE_LIMIT_FILL_NUM)
        / u64::from(GITHUB_RATE_LIMIT_FILL_DEN);
    u32::try_from(filled).unwrap_or(u32::MAX).max(1)
}

/// Remaining primary units at which we stop issuing requests (10% of `limit`).
fn primary_rate_limit_buffer(limit: i32) -> i32 {
    let limit = u32::try_from(limit.max(0)).unwrap_or(0);
    let reserved = limit.saturating_sub(fill_limited(limit)).max(1);
    i32::try_from(reserved).unwrap_or(i32::MAX)
}

/// GitHub GraphQL secondary points charged for a non-mutation query.
#[must_use]
pub(crate) const fn graphql_secondary_query_cost() -> u32 {
    GITHUB_GRAPHQL_SECONDARY_QUERY_POINTS
}

/// Honours GitHub's own rate-limit headers (`x-ratelimit-*`, `retry-after`).
/// GraphQL CPU is not estimated locally: HTTP duration is not GitHub CPU, and
/// a local 60s/min budget serializes scans that GitHub would still accept.
pub struct GitHubRateLimiter {
    // Track API response headers rate limits
    api_limit: Arc<RwLock<Option<RateLimitInfo>>>,
}

impl std::fmt::Debug for GitHubRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubRateLimiter")
            .field("api_limit", &self.api_limit)
            .finish()
    }
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
        // `retry-after` means stop now, even if primary remaining is still high.
        // GitHub's secondary/CPU cap returns 403 with both header families set.
        Self::secondary_rate_limit_from_headers(headers)
            .map(RateLimitInfo::Secondary)
            .or_else(|| Self::primary_rate_limit_from_headers(headers).map(RateLimitInfo::Primary))
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

        let reset_time = Utc.timestamp_opt(reset, 0).single()?;

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
        if let Some(rate_limit) = RateLimitInfo::from_headers(headers) {
            let mut api_limit = self.api_limit.write().await;
            *api_limit = Some(rate_limit);
        }
    }

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        enum Wait {
            Secondary {
                until: DateTime<Utc>,
            },
            Primary {
                until: DateTime<Utc>,
                resource: String,
                remaining: i32,
                limit: i32,
                used: i32,
            },
        }

        let wait = {
            let api_limit_guard = self.api_limit.read().await;
            match &*api_limit_guard {
                Some(RateLimitInfo::Secondary(secondary)) if Utc::now() < secondary.retry_after => {
                    Some(Wait::Secondary {
                        until: secondary.retry_after,
                    })
                }
                Some(RateLimitInfo::Primary(primary))
                    if primary.remaining <= primary_rate_limit_buffer(primary.limit)
                        && Utc::now() < primary.reset_time =>
                {
                    Some(Wait::Primary {
                        until: primary.reset_time,
                        resource: primary.resource.clone(),
                        remaining: primary.remaining,
                        limit: primary.limit,
                        used: primary.used,
                    })
                }
                Some(RateLimitInfo::Primary(primary)) => {
                    let usage_percent =
                        (f64::from(primary.used) / f64::from(primary.limit)) * 100.0;
                    if usage_percent >= 80.0 {
                        tracing::warn!(
                            "GitHub API rate limit is getting low for {}: {}/{} remaining ({:.1}% used). Reset at {}",
                            primary.resource,
                            primary.remaining,
                            primary.limit,
                            usage_percent,
                            primary.reset_time
                        );
                    } else {
                        tracing::trace!(
                            "GitHub API rate limit status for {}: {}/{} remaining. Reset at {}",
                            primary.resource,
                            primary.remaining,
                            primary.limit,
                            primary.reset_time
                        );
                    }
                    None
                }
                _ => None,
            }
        };

        match wait {
            Some(Wait::Secondary { until }) => {
                let wait_duration = (until - Utc::now())
                    .to_std()
                    .unwrap_or(Duration::from_secs(1));
                let wait_duration_secs = wait_duration.as_secs();
                tracing::warn!(
                    "GitHub API secondary rate limit exceeded. Waiting for {} second{} until {} before sending another request.",
                    wait_duration_secs,
                    if wait_duration_secs == 1 { "" } else { "s" },
                    until
                );
                tokio::time::sleep(wait_duration).await;
            }
            Some(Wait::Primary {
                until,
                resource,
                remaining,
                limit,
                used,
            }) => {
                let wait_duration = (until - Utc::now())
                    .to_std()
                    .unwrap_or(Duration::from_secs(1));
                let wait_duration_secs = wait_duration.as_secs();
                tracing::warn!(
                    "GitHub API primary rate limit is nearly exhausted for {}. Waiting for {} second{} until {}. Remaining: {}, Limit: {}, Used: {}",
                    resource,
                    wait_duration_secs,
                    if wait_duration_secs == 1 { "" } else { "s" },
                    until,
                    remaining,
                    limit,
                    used,
                );
                tokio::time::sleep(wait_duration).await;
            }
            None => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use futures::{FutureExt, poll};
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

    #[tokio::test(start_paused = true)]
    async fn test_rate_limiter_api_limits() {
        let rate_limiter = GitHubRateLimiter::new();

        let wait_duration = Duration::hours(1);
        let reset_time = Utc::now() + wait_duration;

        // Set up API headers indicating rate limit exceeded
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("0")),
            ("x-ratelimit-used", s("5000")),
            ("x-ratelimit-reset", reset_time.timestamp().to_string()),
            ("x-ratelimit-resource", s("graphql")),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "an exhausted quota must wait"
        );
        tokio::time::advance(std::time::Duration::from_mins(30)).await;
        assert!(
            poll!(&mut wait).is_pending(),
            "quota reset is still in the future"
        );
        tokio::time::advance(std::time::Duration::from_mins(30)).await;
        wait.now_or_never()
            .expect("quota reset must release the request")
            .expect("rate limit check failed");
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
        rate_limiter
            .check_rate_limit()
            .now_or_never()
            .expect("a healthy quota must not schedule a wait")
            .expect("rate limit check failed");
    }

    #[tokio::test]
    async fn test_small_primary_limit_does_not_wait_too_early() {
        let rate_limiter = GitHubRateLimiter::new();

        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("60")),
            ("x-ratelimit-remaining", s("52")),
            ("x-ratelimit-used", s("8")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::seconds(2)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("core")),
        ]));

        rate_limiter.update_from_headers(&headers).await;

        rate_limiter
            .check_rate_limit()
            .now_or_never()
            .expect("a healthy small public quota must not schedule a wait")
            .expect("rate limit check failed");
    }

    #[tokio::test(start_paused = true)]
    async fn test_secondary_rate_limit() {
        let rate_limiter = GitHubRateLimiter::new();

        // Set up headers indicating secondary rate limit
        let headers = create_test_headers(HashMap::from([("retry-after", s("3600"))]));

        rate_limiter.update_from_headers(&headers).await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "Retry-After must schedule a wait"
        );
        tokio::time::advance(std::time::Duration::from_mins(30)).await;
        assert!(
            poll!(&mut wait).is_pending(),
            "Retry-After is still in the future"
        );
        tokio::time::advance(std::time::Duration::from_mins(30)).await;
        wait.now_or_never()
            .expect("Retry-After must release the request")
            .expect("rate limit check failed");
    }

    #[test]
    fn test_secondary_rate_limit_parsing() {
        let headers = create_test_headers(HashMap::from([("retry-after", s("30"))]));

        let before = Utc::now() + Duration::seconds(30);
        let rate_limit = RateLimitInfo::from_headers(&headers);
        let after = Utc::now() + Duration::seconds(30);
        match rate_limit {
            Some(RateLimitInfo::Secondary(info)) => {
                assert!(
                    (before..=after).contains(&info.retry_after),
                    "Retry-After must be relative to the header parsing time"
                );
            }
            _ => panic!("Expected Secondary rate limit info"),
        }
    }

    #[test]
    fn fill_limited_is_ninety_percent() {
        assert_eq!(fill_limited(2000), 1800);
        assert_eq!(fill_limited(60_000), 54_000);
        assert_eq!(fill_limited(5000), 4500);
        assert_eq!(fill_limited(1), 1);
    }

    #[test]
    fn primary_buffer_is_ten_percent() {
        assert_eq!(primary_rate_limit_buffer(5000), 500);
        assert_eq!(primary_rate_limit_buffer(60), 6);
    }

    #[tokio::test]
    async fn primary_limit_waits_at_ten_percent_remaining() {
        let rate_limiter = GitHubRateLimiter::new();
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("500")),
            ("x-ratelimit-used", s("4500")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
        ]));
        rate_limiter.update_from_headers(&headers).await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "a 10% remaining primary quota must wait"
        );
    }

    #[tokio::test]
    async fn primary_limit_does_not_wait_above_ten_percent_remaining() {
        let rate_limiter = GitHubRateLimiter::new();
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("501")),
            ("x-ratelimit-used", s("4499")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
        ]));
        rate_limiter.update_from_headers(&headers).await;

        rate_limiter
            .check_rate_limit()
            .now_or_never()
            .expect("remaining above the 10% buffer must not wait")
            .expect("rate limit check failed");
    }

    #[test]
    fn test_rate_limit_header_precedence() {
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("1766")),
            ("x-ratelimit-used", s("3234")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
            ("retry-after", s("30")),
        ]));

        let rate_limit = RateLimitInfo::from_headers(&headers);
        match rate_limit {
            Some(RateLimitInfo::Secondary(_)) => (),
            _ => panic!("retry-after must win over primary remaining"),
        }
    }

    #[test]
    fn test_primary_rate_limit_parsing() {
        let now = Utc::now();
        let reset_time = now + Duration::hours(1);

        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("4990")),
            ("x-ratelimit-used", s("10")),
            ("x-ratelimit-reset", reset_time.timestamp().to_string()),
            ("x-ratelimit-resource", s("graphql")),
        ]));

        let rate_limit = RateLimitInfo::from_headers(&headers);
        match rate_limit {
            Some(RateLimitInfo::Primary(info)) => {
                assert_eq!(info.limit, 5000);
                assert_eq!(info.remaining, 4990);
                assert_eq!(info.used, 10);
                assert_eq!(info.resource, "graphql");

                assert_eq!(info.reset_time.timestamp(), reset_time.timestamp());
                assert_eq!(info.reset_time.timestamp_subsec_nanos(), 0);
            }
            _ => panic!("Expected Primary rate limit info"),
        }
    }
}
