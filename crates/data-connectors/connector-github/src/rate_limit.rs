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
use governor::Quota;
use reqwest::header::HeaderMap;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Fraction of each GitHub rate limit we are willing to consume. The remaining
/// 10% is buffer against header lag, retries, and other users of the same token.
const GITHUB_RATE_LIMIT_FILL_NUM: u32 = 9;
const GITHUB_RATE_LIMIT_FILL_DEN: u32 = 10;

/// Share of a primary limit left for other users of the same token. Separate
/// from the fill above: one paces how fast we spend, this sets how much we
/// refuse to spend at all.
const GITHUB_PRIMARY_RESERVE_DEN: u32 = 10;

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
    let reserved = limit.div_ceil(GITHUB_PRIMARY_RESERVE_DEN).max(1);
    i32::try_from(reserved).unwrap_or(i32::MAX)
}

/// GitHub GraphQL secondary points charged for a non-mutation query.
#[must_use]
pub(crate) const fn graphql_secondary_query_cost() -> u32 {
    GITHUB_GRAPHQL_SECONDARY_QUERY_POINTS
}

/// Quota for GitHub's GraphQL secondary point limit: replenish at the 90% fill,
/// and allow no burst beyond one query's cost.
///
/// GCRA admits `burst_size` points at once and then one per
/// `replenish_interval`, so `Quota::per_minute(fill)` on its own would admit the
/// fill twice over in the first minute — 3,599 of the 2,000 points GitHub
/// allows. Capping the burst at a single query holds the worst 60s window to
/// 1,801 while leaving the sustained rate at the full 1,800 the fill targets;
/// taking the burst out of the rate instead would cost sustained capacity for a
/// head start a scan bounded by `github_concurrent_connections_limit` recovers
/// within a second.
#[must_use]
pub(crate) fn graphql_secondary_quota() -> Quota {
    let per_minute = NonZeroU32::new(fill_limited(GITHUB_GRAPHQL_SECONDARY_POINTS_PER_MINUTE))
        .unwrap_or(NonZeroU32::MIN);
    // `until_n_ready(cost)` fails permanently once cost exceeds the burst, so
    // the burst has to cover a single query.
    let burst = NonZeroU32::new(graphql_secondary_query_cost()).unwrap_or(NonZeroU32::MIN);
    Quota::per_minute(per_minute).allow_burst(burst)
}

/// Honours GitHub's own rate-limit headers (`x-ratelimit-*`, `retry-after`).
/// GraphQL CPU is not estimated locally: HTTP duration is not GitHub CPU, and
/// a local 60s/min budget serializes scans that GitHub would still accept.
#[derive(Debug)]
pub struct GitHubRateLimiter {
    /// Latest primary state per `x-ratelimit-resource`. GitHub meters each
    /// resource separately, so a `core` response must not answer for the quota
    /// a `graphql` response reported.
    primary: Arc<RwLock<HashMap<String, PrimaryRateLimitInfo>>>,

    /// Deadline GitHub's `retry-after` holds the whole token to, kept until it
    /// elapses. One limiter is shared by every dataset on a token, so this is
    /// held apart from the primary state above: a sibling request completing
    /// normally must not retire a secondary limit still in force.
    secondary_retry_after: Arc<RwLock<Option<DateTime<Utc>>>>,
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

impl SecondaryRateLimitInfo {
    /// GitHub's secondary limit answers 403 with both header families set, so
    /// `retry-after` is recorded from the same response as the primary state
    /// rather than instead of it.
    fn from_headers(headers: &HeaderMap) -> Option<Self> {
        headers
            .get("retry-after")
            .and_then(|h| h.to_str().ok().map(|s| s.parse::<u64>().ok()))
            .flatten()
            .map(|secs| Utc::now() + Duration::from_secs(secs))
            .map(|retry_after| SecondaryRateLimitInfo { retry_after })
    }
}

// See https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api#checking-the-status-of-your-primary-rate-limit
impl PrimaryRateLimitInfo {
    fn from_headers(headers: &HeaderMap) -> Option<Self> {
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

    /// Share of this resource's limit already spent.
    fn usage_percent(&self) -> f64 {
        if self.limit <= 0 {
            return 0.0;
        }
        (f64::from(self.used) / f64::from(self.limit)) * 100.0
    }

    /// Whether this resource has spent everything above the reserve and has not
    /// yet reset.
    fn is_exhausted(&self) -> bool {
        self.remaining <= primary_rate_limit_buffer(self.limit) && Utc::now() < self.reset_time
    }
}

/// Reports the resource closest to its limit, which is the quota that will stop
/// the scan first and so the only one worth a line per check.
fn log_primary_status<'a>(limits: impl Iterator<Item = &'a PrimaryRateLimitInfo>) {
    let Some(primary) = limits.max_by(|a, b| a.usage_percent().total_cmp(&b.usage_percent()))
    else {
        return;
    };

    let usage_percent = primary.usage_percent();
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

impl GitHubRateLimiter {
    pub fn new() -> Self {
        Self {
            primary: Arc::new(RwLock::new(HashMap::new())),
            secondary_retry_after: Arc::new(RwLock::new(None)),
        }
    }

    /// A handle for an API whose primary quotas GitHub meters separately, still
    /// bound to this limiter's `retry-after`.
    ///
    /// GitHub meters `core` and `graphql` independently, so a REST scan that
    /// spends `core` must not hold back GraphQL requests that have their own
    /// 5,000 to spend. The secondary limit is the opposite: it is charged to the
    /// token, so both handles have to observe the same deadline.
    #[must_use]
    pub fn split_primary_quotas(&self) -> Self {
        Self {
            primary: Arc::new(RwLock::new(HashMap::new())),
            secondary_retry_after: Arc::clone(&self.secondary_retry_after),
        }
    }
}

#[async_trait]
impl RateLimiter for GitHubRateLimiter {
    async fn update_from_headers(&self, headers: &HeaderMap) {
        if let Some(secondary) = SecondaryRateLimitInfo::from_headers(headers) {
            let mut retry_after = self.secondary_retry_after.write().await;
            // Only ever extended: a response asking for a shorter wait than the
            // one in force cannot release the token early, and a response
            // carrying no `retry-after` at all cannot clear it.
            if retry_after.is_none_or(|current| secondary.retry_after > current) {
                *retry_after = Some(secondary.retry_after);
            }
        }

        if let Some(primary) = PrimaryRateLimitInfo::from_headers(headers) {
            let mut primary_limits = self.primary.write().await;
            primary_limits.insert(primary.resource.clone(), primary);
        }
    }

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // A deadline can be pushed out by a concurrent 403 while this request is
        // already asleep, so re-read after each wait and only return once the
        // latest deadline has been served. Returning on the snapshot taken before
        // the sleep sends at the old deadline and earns another 403.
        let mut served: Option<DateTime<Utc>> = None;

        loop {
            let Some(wait) = self.next_wait().await else {
                return Ok(());
            };

            if served.is_some_and(|previous| wait.until() <= previous) {
                return Ok(());
            }

            let until = wait.until();
            let wait_duration = (until - Utc::now())
                .to_std()
                .unwrap_or(Duration::from_secs(1));
            wait.warn(wait_duration);
            served = Some(until);
            tokio::time::sleep(wait_duration).await;
        }
    }
}

/// A reason to hold the next request back, and the wording the user sees for it.
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

impl Wait {
    fn until(&self) -> DateTime<Utc> {
        match self {
            Wait::Secondary { until } | Wait::Primary { until, .. } => *until,
        }
    }

    fn warn(&self, wait_duration: Duration) {
        let secs = wait_duration.as_secs();
        let plural = if secs == 1 { "" } else { "s" };
        match self {
            Wait::Secondary { until } => tracing::warn!(
                "GitHub API secondary rate limit exceeded. Waiting for {} second{} until {} before sending another request.",
                secs,
                plural,
                until
            ),
            Wait::Primary {
                until,
                resource,
                remaining,
                limit,
                used,
            } => tracing::warn!(
                "GitHub API primary rate limit is nearly exhausted for {}. Waiting for {} second{} until {}. Remaining: {}, Limit: {}, Used: {}",
                resource,
                secs,
                plural,
                until,
                remaining,
                limit,
                used,
            ),
        }
    }
}

impl GitHubRateLimiter {
    /// What the latest response headers say to wait for, if anything.
    async fn next_wait(&self) -> Option<Wait> {
        // `retry-after` means stop now, whatever the primary headers say.
        let secondary = {
            let retry_after = *self.secondary_retry_after.read().await;
            retry_after.filter(|until| Utc::now() < *until)
        };
        if let Some(until) = secondary {
            return Some(Wait::Secondary { until });
        }

        let primary_limits = self.primary.read().await;
        // Only the quotas this handle charges: GitHub meters each resource on its
        // own, so a spent `core` is not a reason to hold a `graphql` request.
        let exhausted = primary_limits
            .values()
            .filter(|primary| primary.is_exhausted())
            .max_by_key(|primary| primary.reset_time);

        if exhausted.is_none() {
            log_primary_status(primary_limits.values());
        }

        exhausted.map(|primary| Wait::Primary {
            until: primary.reset_time,
            resource: primary.resource.clone(),
            remaining: primary.remaining,
            limit: primary.limit,
            used: primary.used,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use futures::{FutureExt, poll};
    use governor::RateLimiter as GovernorRateLimiter;
    use governor::clock::FakeRelativeClock;
    use reqwest::header::HeaderValue;

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
        let info = SecondaryRateLimitInfo::from_headers(&headers)
            .expect("Retry-After must parse into a secondary limit");
        let after = Utc::now() + Duration::seconds(30);
        assert!(
            (before..=after).contains(&info.retry_after),
            "Retry-After must be relative to the header parsing time"
        );
    }

    #[test]
    fn fill_limited_is_ninety_percent() {
        assert_eq!(fill_limited(2000), 1800);
        assert_eq!(fill_limited(60_000), 54_000);
        assert_eq!(fill_limited(5000), 4500);
        assert_eq!(fill_limited(1), 1);
    }

    /// A quota built from `Quota::per_minute(n)` alone starts full, so GCRA admits
    /// `n` at once and another `n` as the minute replenishes — twice the intended
    /// rate. Drive the real limiter over a fake minute and count what gets through.
    #[test]
    fn secondary_quota_paces_one_minute_below_githubs_limit() {
        let clock = FakeRelativeClock::default();
        let limiter =
            GovernorRateLimiter::direct_with_clock(graphql_secondary_quota(), clock.clone());

        let mut admitted = 0_u32;
        for _ in 0..60_000 {
            while limiter.check().is_ok() {
                admitted += 1;
            }
            clock.advance(std::time::Duration::from_millis(1));
        }

        let limit = GITHUB_GRAPHQL_SECONDARY_POINTS_PER_MINUTE;
        assert!(
            admitted < limit,
            "{admitted} points admitted in one minute reaches GitHub's {limit}/min secondary limit"
        );
    }

    /// The count above is bounded by how finely the caller polls, so pin the
    /// sustained rate on the quota itself: replenishment must be the whole fill,
    /// not the fill minus a burst allowance.
    #[test]
    fn secondary_quota_replenishes_at_the_full_fill() {
        let quota = graphql_secondary_quota();
        let per_minute = u32::try_from(
            std::time::Duration::from_mins(1).as_nanos()
                / quota.replenish_interval().as_nanos().max(1),
        )
        .unwrap_or(u32::MAX);

        let fill = fill_limited(GITHUB_GRAPHQL_SECONDARY_POINTS_PER_MINUTE);
        assert_eq!(
            per_minute, fill,
            "the quota replenishes {per_minute} points/min, giving away part of the {fill}-point fill"
        );
        assert_eq!(
            quota.burst_size().get(),
            graphql_secondary_query_cost(),
            "the burst must cover exactly one query's cost: less fails `until_n_ready` forever, more overshoots the fill"
        );
    }

    /// A waiting request must not hold the lock that a completing request needs,
    /// or one dataset's `Retry-After` stalls every other dataset on the token.
    #[tokio::test(start_paused = true)]
    async fn a_pending_wait_does_not_block_recording_headers() {
        let rate_limiter = GitHubRateLimiter::new();
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([(
                "retry-after",
                s("3600"),
            )])))
            .await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "Retry-After must schedule a wait"
        );

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
        rate_limiter
            .update_from_headers(&headers)
            .now_or_never()
            .expect(
                "a pending rate-limit wait must not block a completing request from recording its headers",
            );

        let mut later_wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut later_wait).is_pending(),
            "a request arriving after those headers must still wait out the Retry-After"
        );
    }

    /// The limiter is shared by every dataset on one token, so a sibling request
    /// completing normally must not retire a secondary limit that has not elapsed.
    #[tokio::test(start_paused = true)]
    async fn a_normal_response_does_not_clear_an_active_secondary_wait() {
        let rate_limiter = GitHubRateLimiter::new();
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([(
                "retry-after",
                s("3600"),
            )])))
            .await;

        // A sibling dataset's request finishes normally, carrying only primary headers.
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([
                ("x-ratelimit-limit", s("5000")),
                ("x-ratelimit-remaining", s("4999")),
                ("x-ratelimit-used", s("1")),
                (
                    "x-ratelimit-reset",
                    (Utc::now() + Duration::hours(1)).timestamp().to_string(),
                ),
                ("x-ratelimit-resource", s("graphql")),
            ])))
            .await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "a normal response must not clear the Retry-After the token is still under"
        );
    }

    /// GitHub meters each `x-ratelimit-resource` separately, so a healthy `core`
    /// response must not retire an exhausted `graphql` quota.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_resource_does_not_clear_another_resources_exhausted_quota() {
        let rate_limiter = GitHubRateLimiter::new();
        let reset = (Utc::now() + Duration::hours(1)).timestamp().to_string();

        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([
                ("x-ratelimit-limit", s("5000")),
                ("x-ratelimit-remaining", s("0")),
                ("x-ratelimit-used", s("5000")),
                ("x-ratelimit-reset", reset.clone()),
                ("x-ratelimit-resource", s("graphql")),
            ])))
            .await;
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([
                ("x-ratelimit-limit", s("5000")),
                ("x-ratelimit-remaining", s("4999")),
                ("x-ratelimit-used", s("1")),
                ("x-ratelimit-reset", reset),
                ("x-ratelimit-resource", s("core")),
            ])))
            .await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "an exhausted graphql quota must still wait after a healthy core response"
        );
    }

    /// A 403 that lands while a request is already asleep pushes the deadline out.
    /// Returning at the deadline read before the sleep sends early and earns
    /// another 403.
    #[tokio::test(start_paused = true)]
    async fn a_later_retry_after_extends_a_wait_already_in_progress() {
        let rate_limiter = GitHubRateLimiter::new();
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([(
                "retry-after",
                s("60"),
            )])))
            .await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "the first Retry-After must schedule a wait"
        );

        // A sibling request is told to back off for longer.
        rate_limiter
            .update_from_headers(&create_test_headers(HashMap::from([(
                "retry-after",
                s("300"),
            )])))
            .await;

        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        assert!(
            poll!(&mut wait).is_pending(),
            "the extended deadline must still hold the request back"
        );

        tokio::time::advance(std::time::Duration::from_secs(301)).await;
        wait.now_or_never()
            .expect("the extended deadline must release the request once it passes")
            .expect("rate limit check failed");
    }

    /// GitHub meters `core` and `graphql` separately. A REST scan that spends
    /// `core` must not stall GraphQL requests with 5,000 of their own to spend.
    #[tokio::test]
    async fn an_exhausted_rest_quota_does_not_hold_back_graphql() {
        let rest = GitHubRateLimiter::new();
        let graphql = rest.split_primary_quotas();

        rest.update_from_headers(&create_test_headers(HashMap::from([
            ("x-ratelimit-limit", s("5000")),
            ("x-ratelimit-remaining", s("0")),
            ("x-ratelimit-used", s("5000")),
            (
                "x-ratelimit-reset",
                (Utc::now() + Duration::hours(1)).timestamp().to_string(),
            ),
            ("x-ratelimit-resource", s("core")),
        ])))
        .await;

        let mut rest_wait = std::pin::pin!(rest.check_rate_limit());
        assert!(
            poll!(&mut rest_wait).is_pending(),
            "the handle that spent core must wait for it to reset"
        );

        graphql
            .check_rate_limit()
            .now_or_never()
            .expect("a spent core quota must not hold back a graphql request")
            .expect("rate limit check failed");
    }

    /// The secondary limit is charged to the token, not the resource, so both
    /// handles have to honour one `retry-after`.
    #[tokio::test(start_paused = true)]
    async fn a_retry_after_on_one_handle_holds_the_other() {
        let rest = GitHubRateLimiter::new();
        let graphql = rest.split_primary_quotas();

        rest.update_from_headers(&create_test_headers(HashMap::from([(
            "retry-after",
            s("3600"),
        )])))
        .await;

        let mut wait = std::pin::pin!(graphql.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "a Retry-After charged to the token must hold every handle on it"
        );
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

    /// GitHub's secondary limit answers 403 with both header families set, and
    /// `retry-after` is what has to be honoured — a primary quota with room left
    /// says nothing about a secondary limit already tripped.
    #[tokio::test(start_paused = true)]
    async fn retry_after_wins_over_a_primary_quota_with_room_left() {
        let rate_limiter = GitHubRateLimiter::new();
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

        rate_limiter.update_from_headers(&headers).await;

        let mut wait = std::pin::pin!(rate_limiter.check_rate_limit());
        assert!(
            poll!(&mut wait).is_pending(),
            "retry-after must win over primary remaining"
        );
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

        let info = PrimaryRateLimitInfo::from_headers(&headers)
            .expect("x-ratelimit headers must parse into a primary limit");
        assert_eq!(info.limit, 5000);
        assert_eq!(info.remaining, 4990);
        assert_eq!(info.used, 10);
        assert_eq!(info.resource, "graphql");

        assert_eq!(info.reset_time.timestamp(), reset_time.timestamp());
        assert_eq!(info.reset_time.timestamp_subsec_nanos(), 0);
    }
}
