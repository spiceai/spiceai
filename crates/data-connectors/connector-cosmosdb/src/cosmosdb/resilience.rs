/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Connection-resilience primitives for the Azure Cosmos DB connector.
//!
//! The Cosmos DB SDK explicitly disables `typespec`'s retry pipeline
//! (`azure_data_cosmos::clients::cosmos_client` sets `RetryOptions::none()`),
//! so the connector owns retry, concurrency limiting, and permanent-error
//! detection itself. This matches the pattern used by the Git connector in
//! `crates/data_components/src/git.rs` and satisfies the RC
//! "Connection Resilience" gate in
//! `docs/criteria/connectors/rc.md`.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use azure_core::error::ErrorKind;
use azure_core::http::headers::{HeaderName, Headers, X_MS_RETRY_AFTER_MS};
use tokio::sync::Semaphore;

/// Default upper bound on in-flight Cosmos DB requests per account endpoint.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 4;

/// Default number of retries before a transient error is surfaced.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

const RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const RETRY_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Standard `Retry-After` header. typespec's header registry keeps it as a
/// standard header name but does not expose a `pub const`, so construct it
/// locally. Cosmos uses the integer-seconds form in practice.
const RETRY_AFTER_HEADER: HeaderName = HeaderName::from_static("retry-after");

/// Backoff strategy for retries on transient Cosmos DB errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackoffMethod {
    Exponential,
    Fibonacci,
}

impl BackoffMethod {
    /// Parse a user-supplied string. Accepted values are
    /// `"exponential"` and `"fibonacci"` (case-insensitive).
    ///
    /// # Errors
    /// Returns a human-readable message describing the invalid value.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "exponential" => Ok(Self::Exponential),
            "fibonacci" => Ok(Self::Fibonacci),
            other => Err(format!(
                "invalid backoff_method '{other}'. Expected 'exponential' or 'fibonacci'."
            )),
        }
    }
}

/// RAII guard that increments an in-flight counter on construction and
/// decrements it on drop. Cancellation-safe: if the surrounding future is
/// dropped before completion, the counter still returns to its prior value.
pub struct InflightGuard {
    counter: Arc<AtomicU64>,
}

impl InflightGuard {
    pub fn enter(counter: Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Configuration used to tune retry, concurrency, and permanent-error
/// behavior. Produced by the runtime factory from user-facing parameters.
#[derive(Debug, Clone)]
pub struct CosmosResilienceConfig {
    pub max_retries: u32,
    pub backoff: BackoffMethod,
    /// Bounds the number of concurrent Cosmos DB requests. `None` disables
    /// concurrency limiting.
    pub semaphore: Option<Arc<Semaphore>>,
    /// When true, a 401/403/404 response latches the connector as disabled.
    pub disable_on_permanent_error: bool,
    /// Counter driving the `inflight_operations` metric gauge.
    pub inflight: Arc<AtomicU64>,
    /// Shared latch inspected before every request; once set to `true`, all
    /// subsequent operations on the same account endpoint short-circuit.
    pub disabled: Arc<AtomicBool>,
}

impl Default for CosmosResilienceConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            backoff: BackoffMethod::Exponential,
            semaphore: None,
            disable_on_permanent_error: true,
            inflight: Arc::new(AtomicU64::new(0)),
            disabled: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// Result returned by [`run_with_resilience`].
#[derive(Debug)]
pub enum ResilienceError {
    /// The connector is latched in a disabled state from a prior permanent
    /// error. Callers should map this to a domain error surface.
    Disabled,
    /// The underlying SDK surfaced an error that either is non-retryable or
    /// exhausted the retry budget.
    Request(azure_core::Error),
}

impl std::fmt::Display for ResilienceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => f.write_str("Azure Cosmos DB connector has been disabled"),
            Self::Request(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ResilienceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Disabled => None,
            Self::Request(e) => Some(e),
        }
    }
}

/// Classify an SDK error as permanent (authn/authz/not-found) vs. transient.
#[must_use]
pub fn is_permanent_error(err: &azure_core::Error) -> bool {
    matches!(err.http_status().map(u16::from), Some(401 | 403 | 404))
}

/// Extract a `Retry-After` delay from an error's raw HTTP response headers,
/// if any. Honors both the standard `Retry-After` (seconds) and the
/// Cosmos-specific `x-ms-retry-after-ms` header.
#[must_use]
pub fn retry_after_from_error(err: &azure_core::Error) -> Option<Duration> {
    if let ErrorKind::HttpResponse {
        raw_response: Some(response),
        ..
    } = err.kind()
    {
        return retry_after_from_headers(response.headers());
    }
    None
}

fn retry_after_from_headers(headers: &Headers) -> Option<Duration> {
    if let Some(ms_str) = headers.get_optional_str(&X_MS_RETRY_AFTER_MS)
        && let Ok(ms) = ms_str.parse::<u64>()
    {
        return Some(Duration::from_millis(ms));
    }
    if let Some(secs_str) = headers.get_optional_str(&RETRY_AFTER_HEADER)
        && let Ok(secs) = secs_str.parse::<u64>()
    {
        return Some(Duration::from_secs(secs));
    }
    None
}

/// Compute the backoff delay for the given attempt under the configured
/// method, capped at [`RETRY_MAX_BACKOFF`].
#[must_use]
pub fn backoff_delay(method: BackoffMethod, attempt: u32) -> Duration {
    let factor_u64: u64 = match method {
        BackoffMethod::Exponential => 2u64.saturating_pow(attempt),
        BackoffMethod::Fibonacci => {
            let (mut a, mut b) = (1u64, 1u64);
            for _ in 0..attempt {
                let next = a.saturating_add(b);
                a = b;
                b = next;
            }
            b
        }
    };
    let factor = u32::try_from(factor_u64).unwrap_or(u32::MAX);
    RETRY_INITIAL_BACKOFF
        .saturating_mul(factor)
        .min(RETRY_MAX_BACKOFF)
}

/// Execute `operation` with concurrency limiting, retry on transient errors,
/// permanent-error detection, and in-flight tracking.
///
/// `operation` is invoked once per attempt to produce a fresh future; it must
/// therefore be idempotent (or, more precisely, safe to re-issue from scratch).
/// The semaphore permit and [`InflightGuard`] are held for the lifetime of all
/// attempts.
///
/// # Errors
/// Returns [`ResilienceError::Disabled`] if the shared disabled flag is set,
/// or [`ResilienceError::Request`] once retries are exhausted or on a
/// permanent error.
pub async fn run_with_resilience<T, F, Fut>(
    config: &CosmosResilienceConfig,
    endpoint: &str,
    operation: F,
) -> Result<T, ResilienceError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = azure_core::Result<T>>,
{
    if config.disabled.load(Ordering::Acquire) {
        return Err(ResilienceError::Disabled);
    }

    let _permit = match &config.semaphore {
        Some(s) => Some(
            Arc::<Semaphore>::clone(s)
                .acquire_owned()
                .await
                .map_err(|_| ResilienceError::Disabled)?,
        ),
        None => None,
    };

    let _inflight = InflightGuard::enter(Arc::<AtomicU64>::clone(&config.inflight));

    let mut attempt: u32 = 0;
    loop {
        match operation().await {
            Ok(v) => return Ok(v),
            Err(err) => {
                let is_perm = is_permanent_error(&err);

                if is_perm && config.disable_on_permanent_error {
                    config.disabled.store(true, Ordering::Release);
                    tracing::error!(
                        endpoint = %endpoint,
                        "Permanent error from Azure Cosmos DB; disabling connector. {err}"
                    );
                    return Err(ResilienceError::Request(err));
                }

                if is_perm || attempt >= config.max_retries {
                    return Err(ResilienceError::Request(err));
                }

                let backoff = backoff_delay(config.backoff, attempt);
                let retry_after = retry_after_from_error(&err);
                let delay = retry_after.map_or(backoff, |ra| ra.max(backoff));

                tracing::warn!(
                    endpoint = %endpoint,
                    attempt = attempt + 1,
                    max_retries = config.max_retries,
                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                    "Transient error from Azure Cosmos DB, retrying. {err}"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn backoff_method_parse_accepts_canonical_values() {
        assert_eq!(
            BackoffMethod::parse("exponential").expect("should parse 'exponential'"),
            BackoffMethod::Exponential
        );
        assert_eq!(
            BackoffMethod::parse("Fibonacci").expect("should parse 'Fibonacci'"),
            BackoffMethod::Fibonacci
        );
    }

    #[test]
    fn backoff_method_parse_rejects_unknown_values() {
        let err = BackoffMethod::parse("linear").expect_err("'linear' should be rejected");
        assert!(err.contains("invalid backoff_method"));
    }

    #[test]
    fn backoff_delay_exponential_doubles_and_caps() {
        assert_eq!(
            backoff_delay(BackoffMethod::Exponential, 0),
            RETRY_INITIAL_BACKOFF
        );
        assert_eq!(
            backoff_delay(BackoffMethod::Exponential, 1),
            RETRY_INITIAL_BACKOFF * 2
        );
        assert_eq!(
            backoff_delay(BackoffMethod::Exponential, 2),
            RETRY_INITIAL_BACKOFF * 4
        );
        // Large attempt saturates at the cap.
        assert_eq!(
            backoff_delay(BackoffMethod::Exponential, 100),
            RETRY_MAX_BACKOFF
        );
    }

    #[test]
    fn backoff_delay_fibonacci_grows_as_expected() {
        // Factors follow F(attempt+2) with the conventional Fibonacci indexing
        // F(1)=F(2)=1 → attempts 0, 1, 2, 3, 4 map to 1, 2, 3, 5, 8, ...
        assert_eq!(
            backoff_delay(BackoffMethod::Fibonacci, 0),
            RETRY_INITIAL_BACKOFF
        );
        assert_eq!(
            backoff_delay(BackoffMethod::Fibonacci, 1),
            RETRY_INITIAL_BACKOFF * 2
        );
        assert_eq!(
            backoff_delay(BackoffMethod::Fibonacci, 2),
            RETRY_INITIAL_BACKOFF * 3
        );
        assert_eq!(
            backoff_delay(BackoffMethod::Fibonacci, 3),
            RETRY_INITIAL_BACKOFF * 5
        );
    }

    #[test]
    fn is_permanent_error_flags_auth_and_not_found() {
        let auth_err = azure_core::Error::new(
            ErrorKind::HttpResponse {
                status: azure_core::http::StatusCode::Unauthorized,
                error_code: None,
                raw_response: None,
            },
            std::io::Error::other("401"),
        );
        assert!(is_permanent_error(&auth_err));

        let forbidden = azure_core::Error::new(
            ErrorKind::HttpResponse {
                status: azure_core::http::StatusCode::Forbidden,
                error_code: None,
                raw_response: None,
            },
            std::io::Error::other("403"),
        );
        assert!(is_permanent_error(&forbidden));

        let not_found = azure_core::Error::new(
            ErrorKind::HttpResponse {
                status: azure_core::http::StatusCode::NotFound,
                error_code: None,
                raw_response: None,
            },
            std::io::Error::other("404"),
        );
        assert!(is_permanent_error(&not_found));
    }

    #[test]
    fn is_permanent_error_skips_transient_statuses() {
        let throttled = azure_core::Error::new(
            ErrorKind::HttpResponse {
                status: azure_core::http::StatusCode::TooManyRequests,
                error_code: None,
                raw_response: None,
            },
            std::io::Error::other("429"),
        );
        assert!(!is_permanent_error(&throttled));

        let server_error = azure_core::Error::new(
            ErrorKind::HttpResponse {
                status: azure_core::http::StatusCode::InternalServerError,
                error_code: None,
                raw_response: None,
            },
            std::io::Error::other("500"),
        );
        assert!(!is_permanent_error(&server_error));

        let io_err = azure_core::Error::new(ErrorKind::Io, std::io::Error::other("io"));
        assert!(!is_permanent_error(&io_err));
    }

    fn make_error_with_headers(
        status: azure_core::http::StatusCode,
        headers: Vec<(HeaderName, String)>,
    ) -> azure_core::Error {
        use azure_core::http::response::RawResponse;

        let mut hs = Headers::new();
        for (name, value) in headers {
            hs.insert(name, value);
        }
        let raw = RawResponse::from_bytes(status, hs, Vec::<u8>::new());
        azure_core::Error::new(
            ErrorKind::HttpResponse {
                status,
                error_code: None,
                raw_response: Some(Box::new(raw)),
            },
            std::io::Error::other("sdk"),
        )
    }

    #[test]
    fn retry_after_prefers_millisecond_header() {
        let err = make_error_with_headers(
            azure_core::http::StatusCode::TooManyRequests,
            vec![(X_MS_RETRY_AFTER_MS, "250".into())],
        );
        assert_eq!(
            retry_after_from_error(&err),
            Some(Duration::from_millis(250))
        );
    }

    #[test]
    fn retry_after_falls_back_to_seconds_header() {
        let err = make_error_with_headers(
            azure_core::http::StatusCode::ServiceUnavailable,
            vec![(RETRY_AFTER_HEADER, "3".into())],
        );
        assert_eq!(retry_after_from_error(&err), Some(Duration::from_secs(3)));
    }

    #[test]
    fn retry_after_is_none_when_header_missing() {
        let err = make_error_with_headers(azure_core::http::StatusCode::TooManyRequests, vec![]);
        assert_eq!(retry_after_from_error(&err), None);
    }

    #[test]
    fn inflight_guard_increments_and_decrements() {
        let counter = Arc::new(AtomicU64::new(0));
        {
            let _guard = InflightGuard::enter(Arc::<AtomicU64>::clone(&counter));
            assert_eq!(counter.load(Ordering::Relaxed), 1);
        }
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn inflight_guard_tracks_nested_entries() {
        let counter = Arc::new(AtomicU64::new(0));
        let g1 = InflightGuard::enter(Arc::<AtomicU64>::clone(&counter));
        let g2 = InflightGuard::enter(Arc::<AtomicU64>::clone(&counter));
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        drop(g2);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        drop(g1);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn run_with_resilience_short_circuits_when_disabled() {
        let config = CosmosResilienceConfig::default();
        config.disabled.store(true, Ordering::Release);
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::<AtomicUsize>::clone(&attempts);
        let result: Result<(), _> = run_with_resilience(&config, "https://x", || {
            let a = Arc::<AtomicUsize>::clone(&attempts_clone);
            async move {
                a.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        })
        .await;
        assert!(matches!(result, Err(ResilienceError::Disabled)));
        assert_eq!(attempts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn run_with_resilience_retries_transient_then_succeeds() {
        let config = CosmosResilienceConfig {
            max_retries: 3,
            backoff: BackoffMethod::Exponential,
            ..CosmosResilienceConfig::default()
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::<AtomicUsize>::clone(&attempts);
        let result: Result<u32, _> = tokio::time::timeout(
            Duration::from_secs(30),
            run_with_resilience(&config, "https://x", || {
                let a = Arc::<AtomicUsize>::clone(&attempts_clone);
                async move {
                    let n = a.fetch_add(1, Ordering::Relaxed);
                    if n < 2 {
                        Err(azure_core::Error::new(
                            ErrorKind::HttpResponse {
                                status: azure_core::http::StatusCode::TooManyRequests,
                                error_code: None,
                                raw_response: None,
                            },
                            std::io::Error::other("429"),
                        ))
                    } else {
                        Ok(42)
                    }
                }
            }),
        )
        .await
        .expect("future did not time out");
        assert_eq!(result.expect("operation should succeed after retries"), 42);
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn run_with_resilience_surfaces_after_max_retries() {
        let config = CosmosResilienceConfig {
            max_retries: 2,
            backoff: BackoffMethod::Exponential,
            ..CosmosResilienceConfig::default()
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::<AtomicUsize>::clone(&attempts);
        let result: Result<u32, _> = tokio::time::timeout(
            Duration::from_secs(30),
            run_with_resilience(&config, "https://x", || {
                let a = Arc::<AtomicUsize>::clone(&attempts_clone);
                async move {
                    a.fetch_add(1, Ordering::Relaxed);
                    Err(azure_core::Error::new(
                        ErrorKind::HttpResponse {
                            status: azure_core::http::StatusCode::InternalServerError,
                            error_code: None,
                            raw_response: None,
                        },
                        std::io::Error::other("500"),
                    ))
                }
            }),
        )
        .await
        .expect("future did not time out");
        assert!(matches!(result, Err(ResilienceError::Request(_))));
        // max_retries=2 means: initial attempt + 2 retries = 3 total calls.
        assert_eq!(attempts.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn run_with_resilience_latches_disabled_on_permanent_error() {
        let config = CosmosResilienceConfig {
            max_retries: 3,
            disable_on_permanent_error: true,
            ..CosmosResilienceConfig::default()
        };
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = Arc::<AtomicUsize>::clone(&attempts);
        let result: Result<u32, _> = run_with_resilience(&config, "https://x", || {
            let a = Arc::<AtomicUsize>::clone(&attempts_clone);
            async move {
                a.fetch_add(1, Ordering::Relaxed);
                Err(azure_core::Error::new(
                    ErrorKind::HttpResponse {
                        status: azure_core::http::StatusCode::Forbidden,
                        error_code: None,
                        raw_response: None,
                    },
                    std::io::Error::other("403"),
                ))
            }
        })
        .await;
        assert!(matches!(result, Err(ResilienceError::Request(_))));
        // Permanent errors short-circuit without retrying.
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert!(config.disabled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn run_with_resilience_does_not_latch_when_disable_off() {
        let config = CosmosResilienceConfig {
            max_retries: 3,
            disable_on_permanent_error: false,
            ..CosmosResilienceConfig::default()
        };
        let result: Result<u32, _> = run_with_resilience(&config, "https://x", || async {
            Err(azure_core::Error::new(
                ErrorKind::HttpResponse {
                    status: azure_core::http::StatusCode::Unauthorized,
                    error_code: None,
                    raw_response: None,
                },
                std::io::Error::other("401"),
            ))
        })
        .await;
        assert!(matches!(result, Err(ResilienceError::Request(_))));
        assert!(!config.disabled.load(Ordering::Acquire));
    }
}
