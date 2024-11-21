use chrono::{DateTime, TimeZone, Utc};
use reqwest::header::HeaderMap;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::sync::RwLock;
use tracing::{debug, warn};

const GITHUB_MAX_REQUESTS_PER_HOUR: u32 = 5000;

#[derive(Debug)]
pub struct RateLimiter {
    // Track API response headers rate limits
    api_limit: Arc<RwLock<Option<RateLimitInfo>>>,
    // Track our own rate limiting
    requests_made: u32,
    start_time: SystemTime,
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

        let reset_time = Utc.timestamp_opt(reset, 0).unwrap();

        Some(Self {
            limit,
            remaining,
            used,
            reset_time,
            resource,
        })
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        Self {
            api_limit: Arc::new(RwLock::new(None)),
            requests_made: 0,
            start_time: SystemTime::now(),
        }
    }

    pub async fn update_from_headers(&self, headers: &HeaderMap) {
        if let Some(rate_limit) = RateLimitInfo::from_headers(headers) {
            let mut api_limit = self.api_limit.write().await;
            *api_limit = Some(rate_limit);
        }
    }

    pub async fn check_rate_limit(&mut self) -> Result<(), String> {
        // First check our internal hourly limit
        if let Ok(elapsed) = self.start_time.elapsed() {
            if elapsed >= Duration::from_secs(3600) {
                // Reset counter if an hour has passed
                self.requests_made = 0;
                self.start_time = SystemTime::now();
            } else if self.requests_made >= GITHUB_MAX_REQUESTS_PER_HOUR {
                // Calculate time to wait until the hour is up
                let wait_time = Duration::from_secs(3600) - elapsed;
                warn!(
                    "Internal rate limit reached ({}/{}). Waiting for {} seconds.",
                    self.requests_made,
                    GITHUB_MAX_REQUESTS_PER_HOUR,
                    wait_time.as_secs()
                );
                tokio::time::sleep(wait_time).await;
                self.requests_made = 0;
                self.start_time = SystemTime::now();
            }
        }

        // Then check GitHub's rate limit from API responses
        if let Some(api_limit) = self.api_limit.read().await.clone() {
            if api_limit.remaining <= 0 {
                let now = Utc::now();
                if now < api_limit.reset_time {
                    let wait_duration = (api_limit.reset_time - now)
                        .to_std()
                        .unwrap_or(Duration::from_secs(1));
                    warn!(
                        "GitHub API rate limit exceeded. Waiting for {} seconds until {}. Limit: {}, Used: {}, Resource: {}",
                        wait_duration.as_secs(),
                        api_limit.reset_time,
                        api_limit.limit,
                        api_limit.used,
                        api_limit.resource
                    );
                    tokio::time::sleep(wait_duration).await;
                }
            } else {
                debug!(
                    "GitHub API rate limit status: {}/{} remaining. Reset at {}. Resource: {}",
                    api_limit.remaining, api_limit.limit, api_limit.reset_time, api_limit.resource
                );
            }
        }

        self.requests_made += 1;
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
