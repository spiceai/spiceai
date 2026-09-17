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
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;

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

/// GitHub GraphQL secondary CPU budget: 60s of response time per 60s of wall
/// time. Estimated as the sum of HTTP durations.
pub(crate) const GITHUB_GRAPHQL_CPU_MS_PER_MINUTE: u32 = 60_000;

/// Wall window GitHub uses for GraphQL CPU: 60s of response time per 60s.
const CPU_WINDOW: Duration = Duration::from_mins(1);

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

/// Completed GraphQL HTTP intervals. Used to estimate CPU in the last minute
/// as the overlap of those intervals with `[now - 60s, now]`. GitHub's budget
/// is consumed *during* the request, so a 60s call that just finished occupies
/// the window now and only 6s later (at 90% of 60s) is there slack again —
/// not 60s of extra wait on top of the request.
struct CpuWindow {
    samples: Vec<(Instant, Instant)>,
}

impl CpuWindow {
    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(CPU_WINDOW).unwrap_or(now);
        self.samples.retain(|(_, end)| *end > cutoff);
    }

    fn used_ms(&self, now: Instant) -> u64 {
        let cutoff = now.checked_sub(CPU_WINDOW).unwrap_or(now);
        self.samples.iter().fold(0, |acc, (start, end)| {
            let lo = (*start).max(cutoff);
            if *end <= lo {
                acc
            } else {
                acc.saturating_add(
                    u64::try_from(end.duration_since(lo).as_millis()).unwrap_or(u64::MAX),
                )
            }
        })
    }
}

pub struct GitHubRateLimiter {
    // Track API response headers rate limits
    api_limit: Arc<RwLock<Option<RateLimitInfo>>>,
    cpu: Arc<Mutex<CpuWindow>>,
    cpu_burst_ms: u64,
}

impl std::fmt::Debug for GitHubRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitHubRateLimiter")
            .field("api_limit", &self.api_limit)
            .field("cpu_burst_ms", &self.cpu_burst_ms)
            .finish_non_exhaustive()
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
            cpu: Arc::new(Mutex::new(CpuWindow {
                samples: Vec::new(),
            })),
            cpu_burst_ms: u64::from(fill_limited(GITHUB_GRAPHQL_CPU_MS_PER_MINUTE)),
        }
    }

    async fn wait_for_cpu_budget(&self) {
        loop {
            let sleep_for = {
                let mut window = self.cpu.lock().await;
                let now = Instant::now();
                window.prune(now);
                let used = window.used_ms(now);
                if used < self.cpu_burst_ms {
                    return;
                }
                Duration::from_millis(used.saturating_sub(self.cpu_burst_ms).saturating_add(1))
            };
            tokio::time::sleep(sleep_for).await;
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
        // Check if we're rate limited based on the previous API response headers
        let api_limit_guard = self.api_limit.read().await;
        if let Some(api_limit) = &*api_limit_guard {
            match api_limit {
                RateLimitInfo::Secondary(secondary) => {
                    let now = Utc::now();
                    let wait_duration = (secondary.retry_after - now)
                        .to_std()
                        .unwrap_or(Duration::from_secs(1));
                    let wait_duration_secs = wait_duration.as_secs();
                    tracing::warn!(
                        "GitHub API secondary rate limit exceeded. Waiting for {} second{} until {} before sending another request.",
                        wait_duration_secs,
                        if wait_duration_secs == 1 { "" } else { "s" },
                        secondary.retry_after
                    );
                    tokio::time::sleep(wait_duration).await;
                }
                RateLimitInfo::Primary(primary) => {
                    // GitHub GraphQL requests can consume more than 1 rate-limit unit, so keep a
                    // small percentage-based buffer without stalling low-limit unauthenticated REST traffic.
                    if primary.remaining <= primary_rate_limit_buffer(primary.limit) {
                        let now = Utc::now();
                        if now < primary.reset_time {
                            let wait_duration = (primary.reset_time - now)
                                .to_std()
                                .unwrap_or(Duration::from_secs(1));
                            let wait_duration_secs = wait_duration.as_secs();
                            tracing::warn!(
                                "GitHub API primary rate limit is nearly exhausted for {}. Waiting for {} second{} until {}. Remaining: {}, Limit: {}, Used: {}",
                                primary.resource,
                                wait_duration_secs,
                                if wait_duration_secs == 1 { "" } else { "s" },
                                primary.reset_time,
                                primary.remaining,
                                primary.limit,
                                primary.used,
                            );
                            tokio::time::sleep(wait_duration).await;
                        }
                    } else {
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
                    }
                }
            }
        }

        self.wait_for_cpu_budget().await;

        Ok(())
    }

    async fn record_request_duration(&self, elapsed: Duration) {
        let now = Instant::now();
        let start = now.checked_sub(elapsed).unwrap_or(now);
        let mut window = self.cpu.lock().await;
        window.samples.push((start, now));
        window.prune(now);
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

    #[tokio::test(start_paused = true)]
    async fn cpu_window_waits_only_the_overage_of_a_slow_request() {
        let rate_limiter = GitHubRateLimiter::new();
        rate_limiter
            .record_request_duration(std::time::Duration::from_secs(60))
            .await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "a 60s request fills 90% of the 60s CPU window"
        );
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        assert!(
            poll!(&mut wait).is_pending(),
            "5s later the window still holds more than 54s of CPU"
        );
        tokio::time::advance(std::time::Duration::from_secs(2)).await;
        wait.now_or_never()
            .expect("after the 6s overage the next request must proceed")
            .expect("rate limit check failed");
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
        // Test that primary rate limit is preferred when both types of headers are present
        let headers = create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("4999")),
            ("x-ratelimit-used", s("1")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("graphql")),
            ("retry-after", s("30")), // This should be ignored when primary headers are present
        ]));

        let rate_limit = RateLimitInfo::from_headers(&headers);
        match rate_limit {
            Some(RateLimitInfo::Primary(_)) => (),
            _ => panic!("Expected Primary rate limit info when both header types are present"),
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
