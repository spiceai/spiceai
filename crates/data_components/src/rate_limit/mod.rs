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

use async_trait::async_trait;
use reqwest::header::{HeaderMap, RETRY_AFTER};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time::Instant;

pub mod adaptive;

pub use adaptive::{
    AdaptiveController, AdaptiveRateControl, AdaptiveRateControlParseError, RequestOutcome,
    parse_adaptive_rate_control,
};

/// One throttle step the adaptive controller waits when it declines to admit a
/// request. Small and bounded: a declined request re-checks admission after this
/// pause rather than failing, so a throttled origin sees a lower request rate
/// without any request being dropped.
const ADAPTIVE_THROTTLE_STEP: Duration = Duration::from_millis(50);

/// Cap on the total time [`HttpRateLimiter::check_rate_limit`] will spend
/// waiting for the adaptive gate to admit a single request, so even a controller
/// pinned near its floor cannot stall a request indefinitely.
const ADAPTIVE_MAX_THROTTLE_WAIT: Duration = Duration::from_secs(5);

const RETRY_AFTER_MS_HEADER: &str = "retry-after-ms";
const X_RETRY_AFTER_MS_HEADER: &str = "x-retry-after-ms";
const RATE_LIMIT_REMAINING_HEADER: &str = "ratelimit-remaining";
const RATE_LIMIT_RESET_HEADER: &str = "ratelimit-reset";
const X_RATELIMIT_REMAINING_HEADER: &str = "x-ratelimit-remaining";
const X_RATELIMIT_RESET_HEADER: &str = "x-ratelimit-reset";
const X_RATELIMIT_RESET_AFTER_HEADER: &str = "x-ratelimit-reset-after";
const X_RATE_LIMIT_REMAINING_HEADER: &str = "x-rate-limit-remaining";
const X_RATE_LIMIT_RESET_HEADER: &str = "x-rate-limit-reset";
const X_RATE_LIMIT_RESET_AFTER_HEADER: &str = "x-rate-limit-reset-after";

#[async_trait]
pub trait RateLimiter: Debug + Send + Sync {
    async fn update_from_headers(&self, headers: &HeaderMap);

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Record the outcome of one request so an adaptive controller can raise or
    /// lower the effective rate for this origin.
    ///
    /// Defaults to a no-op: only [`HttpRateLimiter`] runs an adaptive controller;
    /// the per-connector limiters (GitHub, Git) deliberately do not participate.
    /// A decorator/wrapper over another `RateLimiter` MUST forward this to the
    /// inner limiter — inheriting the no-op would silently disable adaptive
    /// control for the wrapped origin.
    fn record_request_outcome(&self, _outcome: RequestOutcome) {}
}

#[derive(Debug, Default)]
pub struct HttpRateLimiterMetrics {
    updates_total: AtomicU64,
    waits_total: AtomicU64,
    wait_duration_ms_total: AtomicU64,
    deadline_unix_ms: AtomicU64,
    adaptive_effective_limit: AtomicU64,
    adaptive_admission_coefficient_permille: AtomicU64,
    adaptive_throttled_total: AtomicU64,
    adaptive_throttle_wait_duration_ms_total: AtomicU64,
}

impl HttpRateLimiterMetrics {
    #[must_use]
    pub fn retry_after_updates_total(&self) -> u64 {
        self.updates_total.load(Ordering::Relaxed)
    }

    /// Current adaptive effective request-rate limit, rounded to a whole number.
    /// Zero when adaptive control is disabled for this origin.
    #[must_use]
    pub fn adaptive_effective_limit(&self) -> u64 {
        self.adaptive_effective_limit.load(Ordering::Relaxed)
    }

    /// Current adaptive admission coefficient in parts-per-thousand (0..=1000):
    /// 1000 means "admit everything", lower means the origin is being throttled.
    /// Zero when adaptive control is disabled for this origin.
    #[must_use]
    pub fn adaptive_admission_coefficient_permille(&self) -> u64 {
        self.adaptive_admission_coefficient_permille
            .load(Ordering::Relaxed)
    }

    /// Total requests the adaptive controller has throttled (declined to admit
    /// on first check) for this origin.
    #[must_use]
    pub fn adaptive_throttled_total(&self) -> u64 {
        self.adaptive_throttled_total.load(Ordering::Relaxed)
    }

    /// Cumulative time requests have spent waiting on the adaptive throttle gate.
    #[must_use]
    pub fn adaptive_throttle_wait_duration_ms_total(&self) -> u64 {
        self.adaptive_throttle_wait_duration_ms_total
            .load(Ordering::Relaxed)
    }

    fn record_adaptive_state(&self, effective_limit: f64, admission_coefficient: f64) {
        self.adaptive_effective_limit
            .store(f64_round_to_u64(effective_limit), Ordering::Relaxed);
        self.adaptive_admission_coefficient_permille.store(
            f64_round_to_u64(admission_coefficient.clamp(0.0, 1.0) * 1000.0),
            Ordering::Relaxed,
        );
    }

    fn record_adaptive_throttle(&self, waited: Duration) {
        self.adaptive_throttled_total
            .fetch_add(1, Ordering::Relaxed);
        self.adaptive_throttle_wait_duration_ms_total
            .fetch_add(duration_millis_u64(waited), Ordering::Relaxed);
    }

    #[must_use]
    pub fn retry_after_waits_total(&self) -> u64 {
        self.waits_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn retry_after_wait_duration_ms_total(&self) -> u64 {
        self.wait_duration_ms_total.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn retry_after_remaining_ms(&self) -> u64 {
        let deadline = self.deadline_unix_ms.load(Ordering::Relaxed);
        let now = system_time_unix_ms(SystemTime::now());
        deadline.saturating_sub(now)
    }

    fn record_retry_after_update(&self, deadline_unix_ms: u64) {
        self.updates_total.fetch_add(1, Ordering::Relaxed);
        self.deadline_unix_ms
            .fetch_max(deadline_unix_ms, Ordering::Relaxed);
    }

    fn record_retry_after_wait(&self, duration: Duration) {
        self.waits_total.fetch_add(1, Ordering::Relaxed);
        self.wait_duration_ms_total
            .fetch_add(duration_millis_u64(duration), Ordering::Relaxed);
    }

    fn clear_retry_after_deadline(&self) {
        self.deadline_unix_ms.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
pub struct HttpRateLimiter {
    retry_after: RwLock<Option<Instant>>,
    metrics: Arc<HttpRateLimiterMetrics>,
    adaptive: Option<Arc<AdaptiveController>>,
}

impl HttpRateLimiter {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_metrics(metrics: Arc<HttpRateLimiterMetrics>) -> Self {
        Self {
            retry_after: RwLock::new(None),
            metrics,
            adaptive: None,
        }
    }

    /// Build a limiter with an adaptive controller for `control`, growing back
    /// toward `ceiling`. A [`AdaptiveRateControl::Disabled`] control leaves the
    /// limiter with no adaptive controller.
    #[must_use]
    pub fn with_adaptive(control: AdaptiveRateControl, ceiling: f64) -> Self {
        Self::with_metrics_and_adaptive(Arc::default(), control, ceiling)
    }

    /// Build a limiter with both explicit metrics and an adaptive controller.
    #[must_use]
    pub fn with_metrics_and_adaptive(
        metrics: Arc<HttpRateLimiterMetrics>,
        control: AdaptiveRateControl,
        ceiling: f64,
    ) -> Self {
        let adaptive = AdaptiveController::new(control, ceiling).map(Arc::new);
        let limiter = Self {
            retry_after: RwLock::new(None),
            metrics,
            adaptive,
        };
        limiter.publish_adaptive_state();
        limiter
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<HttpRateLimiterMetrics> {
        Arc::clone(&self.metrics)
    }

    /// The adaptive controller for this origin, if adaptive control is enabled.
    #[must_use]
    pub fn adaptive_controller(&self) -> Option<&Arc<AdaptiveController>> {
        self.adaptive.as_ref()
    }

    fn publish_adaptive_state(&self) {
        if let Some(adaptive) = &self.adaptive {
            self.metrics.record_adaptive_state(
                adaptive.effective_limit(),
                adaptive.admission_coefficient(),
            );
        }
    }
}

#[async_trait]
impl RateLimiter for HttpRateLimiter {
    async fn update_from_headers(&self, headers: &HeaderMap) {
        let now = SystemTime::now();
        let Some(retry_after_duration) = retry_after_duration(headers, now) else {
            return;
        };
        let Some(retry_after) = Instant::now().checked_add(retry_after_duration) else {
            return;
        };
        let retry_after_deadline_unix_ms = retry_after_deadline_unix_ms(now, retry_after_duration);

        let mut current_retry_after = self.retry_after.write().await;
        if current_retry_after.is_none_or(|current| retry_after > current) {
            *current_retry_after = Some(retry_after);
            self.metrics
                .record_retry_after_update(retry_after_deadline_unix_ms);
        }
    }

    async fn check_rate_limit(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let retry_after = *self.retry_after.read().await;
            let Some(retry_after) = retry_after else {
                break;
            };

            let now = Instant::now();
            if retry_after <= now {
                self.clear_elapsed_retry_after(now).await;
                continue;
            }

            let wait_duration = retry_after.saturating_duration_since(now);
            tracing::debug!(
                wait_duration_ms = wait_duration.as_millis(),
                "HTTP rate limit exceeded. Waiting before sending another request."
            );
            self.metrics.record_retry_after_wait(wait_duration);
            tokio::time::sleep(wait_duration).await;
            self.clear_elapsed_retry_after(Instant::now()).await;
        }

        self.wait_for_adaptive_admission().await;
        Ok(())
    }

    fn record_request_outcome(&self, outcome: RequestOutcome) {
        if let Some(adaptive) = &self.adaptive {
            adaptive.record(outcome);
            self.metrics.record_adaptive_state(
                adaptive.effective_limit(),
                adaptive.admission_coefficient(),
            );
        }
    }
}

impl HttpRateLimiter {
    async fn clear_elapsed_retry_after(&self, now: Instant) {
        let mut current_retry_after = self.retry_after.write().await;
        if current_retry_after.is_some_and(|retry_after| retry_after <= now) {
            *current_retry_after = None;
            self.metrics.clear_retry_after_deadline();
        }
    }

    /// Block until the adaptive controller admits this request, or until the
    /// bounded throttle budget is spent. A declined request pauses for one
    /// [`ADAPTIVE_THROTTLE_STEP`] and re-checks rather than being dropped, so a
    /// failing origin is served a lower request rate without any request being
    /// turned into an error.
    async fn wait_for_adaptive_admission(&self) {
        let Some(adaptive) = &self.adaptive else {
            return;
        };

        let started = Instant::now();
        let mut throttled = false;
        while !adaptive.try_admit() {
            throttled = true;
            let waited = started.elapsed();
            if waited >= ADAPTIVE_MAX_THROTTLE_WAIT {
                break;
            }

            let step = ADAPTIVE_THROTTLE_STEP.min(ADAPTIVE_MAX_THROTTLE_WAIT - waited);
            tracing::debug!(
                admission_coefficient = adaptive.admission_coefficient(),
                "Adaptive rate control throttling HTTP request to a failing origin."
            );
            tokio::time::sleep(step).await;
        }

        if throttled {
            self.metrics.record_adaptive_throttle(started.elapsed());
        }
    }
}

#[must_use]
pub fn retry_after_time(headers: &HeaderMap, now: SystemTime) -> Option<SystemTime> {
    retry_after_duration(headers, now).and_then(|duration| now.checked_add(duration))
}

#[must_use]
pub fn retry_after_duration(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    let retry_after = retry_after_millis_duration(headers).or_else(|| {
        headers
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| retry_after_duration_from_value(value, now))
    });

    [retry_after, rate_limit_reset_duration(headers, now)]
        .into_iter()
        .flatten()
        .max()
}

#[must_use]
pub fn retry_after_duration_from_value(value: &str, now: SystemTime) -> Option<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    trimmed
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
        .or_else(|| {
            httpdate::parse_http_date(trimmed)
                .ok()
                .map(|retry_after| retry_after.duration_since(now).unwrap_or(Duration::ZERO))
        })
}

fn retry_after_millis_duration(headers: &HeaderMap) -> Option<Duration> {
    [RETRY_AFTER_MS_HEADER, X_RETRY_AFTER_MS_HEADER]
        .into_iter()
        .find_map(|header_name| {
            headers
                .get(header_name)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.trim().parse::<u64>().ok())
                .map(Duration::from_millis)
        })
}

fn rate_limit_reset_duration(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    if !rate_limit_remaining_is_zero(headers) {
        return None;
    }

    [
        relative_rate_limit_reset_duration(headers),
        legacy_epoch_rate_limit_reset_duration(headers, now),
    ]
    .into_iter()
    .flatten()
    .max()
}

fn rate_limit_remaining_is_zero(headers: &HeaderMap) -> bool {
    [
        RATE_LIMIT_REMAINING_HEADER,
        X_RATELIMIT_REMAINING_HEADER,
        X_RATE_LIMIT_REMAINING_HEADER,
    ]
    .into_iter()
    .any(|header_name| header_u64(headers, header_name).is_some_and(|value| value == 0))
}

fn relative_rate_limit_reset_duration(headers: &HeaderMap) -> Option<Duration> {
    [
        RATE_LIMIT_RESET_HEADER,
        X_RATELIMIT_RESET_AFTER_HEADER,
        X_RATE_LIMIT_RESET_AFTER_HEADER,
    ]
    .into_iter()
    .filter_map(|header_name| header_duration_seconds(headers, header_name))
    .max()
}

fn legacy_epoch_rate_limit_reset_duration(
    headers: &HeaderMap,
    now: SystemTime,
) -> Option<Duration> {
    [X_RATELIMIT_RESET_HEADER, X_RATE_LIMIT_RESET_HEADER]
        .into_iter()
        .filter_map(|header_name| header_duration_seconds(headers, header_name))
        .filter_map(|reset_since_epoch| {
            SystemTime::UNIX_EPOCH
                .checked_add(reset_since_epoch)
                .map(|reset_time| reset_time.duration_since(now).unwrap_or(Duration::ZERO))
        })
        .max()
}

fn header_u64(headers: &HeaderMap, header_name: &str) -> Option<u64> {
    header_token(headers, header_name).and_then(|value| value.parse::<u64>().ok())
}

fn header_duration_seconds(headers: &HeaderMap, header_name: &str) -> Option<Duration> {
    header_token(headers, header_name).and_then(duration_seconds_from_value)
}

fn header_token<'a>(headers: &'a HeaderMap, header_name: &str) -> Option<&'a str> {
    headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .and_then(first_header_token)
}

fn first_header_token(value: &str) -> Option<&str> {
    let token = value
        .trim()
        .split(|character: char| {
            character == ';' || character == ',' || character.is_ascii_whitespace()
        })
        .next()
        .map(|token| token.trim_matches('"'))?;

    if token.is_empty() { None } else { Some(token) }
}

fn duration_seconds_from_value(value: &str) -> Option<Duration> {
    let (seconds, nanos) = if let Some((seconds, fractional)) = value.split_once('.') {
        (
            seconds.parse::<u64>().ok()?,
            fractional_seconds_to_nanos(fractional)?,
        )
    } else {
        (value.parse::<u64>().ok()?, 0)
    };

    Some(Duration::new(seconds, nanos))
}

fn fractional_seconds_to_nanos(value: &str) -> Option<u32> {
    if value.is_empty() {
        return None;
    }

    let mut nanos = 0_u32;
    let mut digits_used = 0_u8;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return None;
        }

        if digits_used < 9 {
            nanos = nanos.checked_mul(10)?.checked_add(u32::from(byte - b'0'))?;
            digits_used += 1;
        }
    }

    while digits_used < 9 {
        nanos = nanos.checked_mul(10)?;
        digits_used += 1;
    }

    Some(nanos)
}

fn retry_after_deadline_unix_ms(now: SystemTime, duration: Duration) -> u64 {
    now.checked_add(duration)
        .map_or(u64::MAX, system_time_unix_ms)
}

fn system_time_unix_ms(time: SystemTime) -> u64 {
    let Ok(duration) = time.duration_since(SystemTime::UNIX_EPOCH) else {
        return 0;
    };
    duration_millis_u64(duration)
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// Round a non-negative, finite `f64` to the nearest `u64`, saturating. Negative
/// or non-finite inputs map to 0. Used only for metric gauges, where an
/// approximate whole number is all that is reported.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "input is guarded finite and non-negative, and clamped below u64::MAX before the cast"
)]
fn f64_round_to_u64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }
    // u64::MAX is not exactly representable as f64; this bound is a safe
    // saturating ceiling well above any rate limit or per-mille value.
    const SATURATING_CEILING: f64 = u64::MAX as f64;
    value.round().min(SATURATING_CEILING) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn test_retry_after_duration_from_seconds() {
        let duration = retry_after_duration_from_value("42", SystemTime::UNIX_EPOCH)
            .expect("seconds-based Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(42));
    }

    #[test]
    fn test_retry_after_duration_from_http_date() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(50);
        let duration = retry_after_duration_from_value("Thu, 01 Jan 1970 00:01:40 GMT", now)
            .expect("HTTP-date Retry-After should parse");

        assert_eq!(duration, Duration::from_secs(50));
    }

    #[test]
    fn test_retry_after_duration_from_millisecond_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER_MS_HEADER, HeaderValue::from_static("1250"));

        assert_eq!(
            retry_after_duration(&headers, SystemTime::now()),
            Some(Duration::from_millis(1250))
        );
    }

    #[test]
    fn test_retry_after_duration_from_standard_rate_limit_reset() {
        let mut headers = HeaderMap::new();
        headers.insert(RATE_LIMIT_REMAINING_HEADER, HeaderValue::from_static("0"));
        headers.insert(RATE_LIMIT_RESET_HEADER, HeaderValue::from_static("3;w=60"));

        assert_eq!(
            retry_after_duration(&headers, SystemTime::now()),
            Some(Duration::from_secs(3))
        );
    }

    #[test]
    fn test_retry_after_duration_from_legacy_rate_limit_reset_epoch() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let mut headers = HeaderMap::new();
        headers.insert(X_RATELIMIT_REMAINING_HEADER, HeaderValue::from_static("0"));
        headers.insert(
            X_RATELIMIT_RESET_HEADER,
            HeaderValue::from_static("1700000042"),
        );

        assert_eq!(
            retry_after_duration(&headers, now),
            Some(Duration::from_secs(42))
        );
    }

    #[test]
    fn test_rate_limit_reset_ignored_when_remaining_quota_exists() {
        let mut headers = HeaderMap::new();
        headers.insert(RATE_LIMIT_REMAINING_HEADER, HeaderValue::from_static("1"));
        headers.insert(RATE_LIMIT_RESET_HEADER, HeaderValue::from_static("3"));

        assert_eq!(retry_after_duration(&headers, SystemTime::now()), None);
    }

    #[test]
    fn test_retry_after_duration_uses_longest_cooldown_hint() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("1"));
        headers.insert(RATE_LIMIT_REMAINING_HEADER, HeaderValue::from_static("0"));
        headers.insert(RATE_LIMIT_RESET_HEADER, HeaderValue::from_static("5"));

        assert_eq!(
            retry_after_duration(&headers, SystemTime::now()),
            Some(Duration::from_secs(5))
        );
    }

    use crate::rate_limit::adaptive::{AdaptiveRateControl, RequestOutcome};
    use reqwest::StatusCode;

    /// Classify a stub HTTP status the way the request path does, reusing the
    /// shared retry-reason classification so the demonstration exercises the
    /// same success/failure boundary the connector uses.
    fn outcome_for_status(status: StatusCode) -> RequestOutcome {
        if crate::resilient_http::status_is_retryable(status) {
            RequestOutcome::Failure
        } else {
            RequestOutcome::Success
        }
    }

    /// Measure the fraction of requests the limiter's adaptive gate admits over
    /// `samples` decisions — the effective admitted rate right now.
    fn admitted_fraction(limiter: &HttpRateLimiter, samples: u32) -> f64 {
        let controller = limiter
            .adaptive_controller()
            .expect("adaptive control should be enabled");
        let admitted = (0..samples).filter(|_| controller.try_admit()).count();
        f64::from(u32::try_from(admitted).unwrap_or(samples)) / f64::from(samples)
    }

    /// Behavioral demonstration (AIMD): an origin with a *configured* static
    /// limit that starts healthy, turns to 503s, then recovers. Adaptive control
    /// only modifies that defined limit, so the admitted rate must collapse under
    /// the failures and climb back on recovery — driven entirely through the
    /// public `record_request_outcome` path on a real `HttpRateLimiter`.
    #[tokio::test(flavor = "current_thread")]
    async fn adaptive_aimd_reduces_admitted_rate_under_failures_then_recovers() {
        // The origin's configured static rate limit — the reference AIMD scales.
        const STATIC_LIMIT: f64 = 32.0;
        const STATIC_LIMIT_U64: u64 = 32;
        let limiter = HttpRateLimiter::with_adaptive(AdaptiveRateControl::Aimd, STATIC_LIMIT);

        // Healthy origin: 200 OK. The effective limit sits at the configured
        // static limit and (nearly) every request is admitted.
        for _ in 0..64 {
            limiter.record_request_outcome(outcome_for_status(StatusCode::OK));
        }
        let healthy = admitted_fraction(&limiter, 1000);

        // Origin starts failing: 503 Service Unavailable and request timeouts.
        // Each failure halves the effective limit; it collapses to the floor.
        for i in 0..32 {
            let status = if i % 2 == 0 {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::REQUEST_TIMEOUT
            };
            limiter.record_request_outcome(outcome_for_status(status));
        }
        let failing = admitted_fraction(&limiter, 1000);

        // Origin recovers: 200 OK again. The effective limit climbs back by one
        // per success until it is near the configured static limit.
        for _ in 0..64 {
            limiter.record_request_outcome(outcome_for_status(StatusCode::OK));
        }
        let recovered = admitted_fraction(&limiter, 1000);

        eprintln!(
            "AIMD admitted fraction: healthy={healthy:.3} failing={failing:.3} recovered={recovered:.3}"
        );

        assert!(
            healthy > 0.9,
            "healthy origin should admit almost everything, got {healthy:.3}"
        );
        assert!(
            failing < 0.2,
            "failing origin admitted rate should collapse, got {failing:.3}"
        );
        assert!(
            failing < healthy,
            "failures must reduce the admitted rate ({failing:.3} !< {healthy:.3})"
        );
        assert!(
            recovered > 0.9,
            "recovered origin should admit almost everything again, got {recovered:.3}"
        );

        let metrics = limiter.metrics();
        assert!(
            metrics.adaptive_effective_limit() >= STATIC_LIMIT_U64 - 1,
            "effective-limit metric should recover toward the configured static limit, got {}",
            metrics.adaptive_effective_limit()
        );
    }

    /// Behavioral demonstration (Google SRE throttling): the admission
    /// coefficient must fall below 1 once the success ratio drops under `1/k`,
    /// and return to 1 after the failure burst decays out of the window. Uses
    /// paused tokio time so the decaying window is driven deterministically.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn adaptive_sre_reduces_admitted_rate_under_failures_then_recovers() {
        // The origin's configured static rate limit that SRE scales by the
        // admission coefficient. Its magnitude does not affect the coefficient.
        const STATIC_LIMIT: f64 = 32.0;
        let limiter = HttpRateLimiter::with_adaptive(
            AdaptiveRateControl::SreThrottle { k: 4.0 },
            STATIC_LIMIT,
        );

        // Healthy origin: accepts == requests, coefficient is exactly 1.
        for _ in 0..50 {
            limiter.record_request_outcome(outcome_for_status(StatusCode::OK));
        }
        let healthy = admitted_fraction(&limiter, 2000);

        // A long burst of 503s drives the success ratio well below 1/k.
        for _ in 0..2000 {
            limiter.record_request_outcome(outcome_for_status(StatusCode::SERVICE_UNAVAILABLE));
        }
        let failing = admitted_fraction(&limiter, 2000);

        // Let the failure burst age out of the decaying window, then resume
        // successes. The coefficient returns to 1.
        tokio::time::advance(Duration::from_secs(60)).await;
        for _ in 0..500 {
            limiter.record_request_outcome(outcome_for_status(StatusCode::OK));
        }
        let recovered = admitted_fraction(&limiter, 2000);

        eprintln!(
            "SRE admitted fraction: healthy={healthy:.3} failing={failing:.3} recovered={recovered:.3}"
        );

        assert!(
            healthy > 0.95,
            "healthy origin (accepts==requests) must never be throttled, got {healthy:.3}"
        );
        assert!(
            failing < 0.5,
            "sustained failures should throttle admissions, got {failing:.3}"
        );
        assert!(
            failing < healthy,
            "failures must reduce the admitted rate ({failing:.3} !< {healthy:.3})"
        );
        assert!(
            recovered > 0.95,
            "recovered origin should stop throttling once failures decay, got {recovered:.3}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_http_rate_limiter_waits_until_retry_after() {
        let rate_limiter = HttpRateLimiter::new();
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        rate_limiter.update_from_headers(&headers).await;
        let metrics = rate_limiter.metrics();

        assert_eq!(metrics.retry_after_updates_total(), 1);
        assert!(metrics.retry_after_remaining_ms() > 0);

        let start = tokio::time::Instant::now();
        rate_limiter
            .check_rate_limit()
            .await
            .expect("rate limit check should succeed");

        assert!(start.elapsed() >= Duration::from_secs(2));
        assert_eq!(metrics.retry_after_waits_total(), 1);
        assert_eq!(metrics.retry_after_wait_duration_ms_total(), 2000);
        assert_eq!(metrics.retry_after_remaining_ms(), 0);

        let start = tokio::time::Instant::now();
        rate_limiter
            .check_rate_limit()
            .await
            .expect("elapsed rate limit should be cleared");

        assert!(start.elapsed() < Duration::from_millis(1));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_http_rate_limiter_waits_for_extended_retry_after() {
        let rate_limiter = Arc::new(HttpRateLimiter::new());
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
        rate_limiter.update_from_headers(&headers).await;

        let waiter = tokio::spawn({
            let rate_limiter = Arc::clone(&rate_limiter);
            async move {
                let start = tokio::time::Instant::now();
                rate_limiter
                    .check_rate_limit()
                    .await
                    .expect("rate limit check should succeed");
                start.elapsed()
            }
        });
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(1)).await;
        let mut extended_headers = HeaderMap::new();
        extended_headers.insert(RETRY_AFTER, HeaderValue::from_static("3"));
        rate_limiter.update_from_headers(&extended_headers).await;

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        tokio::time::advance(Duration::from_secs(2)).await;
        let elapsed = waiter.await.expect("waiter task should complete");

        let metrics = rate_limiter.metrics();
        assert!(elapsed >= Duration::from_secs(4));
        assert_eq!(metrics.retry_after_updates_total(), 2);
        assert_eq!(metrics.retry_after_waits_total(), 2);
        assert_eq!(metrics.retry_after_wait_duration_ms_total(), 4000);
    }
}
