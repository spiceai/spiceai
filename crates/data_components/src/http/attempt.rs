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

//! Complete-attempt observations shared by HTTP and GraphQL connectors.

use std::time::Duration;

use parking_lot::Mutex;
use reqwest::StatusCode;
use runtime_rate_control::{RateController, RequestOutcome};
use tokio::time::Instant;
use url::Url;

/// Canonical origin key shared by admission and HTTP attempt metrics.
#[must_use]
pub fn origin_key(url: &Url) -> String {
    let scheme = url.scheme();
    let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
    match url.port_or_known_default() {
        Some(port) => format!("{scheme}://{host}:{port}"),
        None => format!("{scheme}://{host}"),
    }
}

/// Dataset-local latency policy. Share this across every clone/page of a dataset,
/// not across datasets: thresholds and the warning's lifetime belong to the dataset.
#[derive(Debug)]
pub struct HttpAttemptObserver {
    origin: String,
    dataset: String,
    slow_threshold: Option<Duration>,
    warning: Mutex<SlowWarning>,
}

#[derive(Debug, Default)]
struct SlowWarning {
    slow_since: Option<Instant>,
    warned: bool,
}

impl SlowWarning {
    fn observe(&mut self, outcome: RequestOutcome, window: Duration, at_floor: bool) -> bool {
        let now = Instant::now();
        if outcome != RequestOutcome::Slow {
            self.slow_since = None;
            return false;
        }
        let since = *self.slow_since.get_or_insert(now);
        if !self.warned && at_floor && now.duration_since(since) >= window {
            self.warned = true;
            return true;
        }
        false
    }
}

impl HttpAttemptObserver {
    #[must_use]
    pub fn new(url: &Url, dataset: String, slow_threshold: Option<Duration>) -> Self {
        Self {
            origin: origin_key(url),
            dataset,
            slow_threshold: slow_threshold.filter(|threshold| !threshold.is_zero()),
            warning: Mutex::new(SlowWarning::default()),
        }
    }

    /// Start immediately before sending, after all admission and cooldown waits.
    #[must_use]
    pub fn start<'a>(&'a self, controller: Option<&'a RateController>) -> HttpAttempt<'a> {
        HttpAttempt {
            observer: self,
            controller,
            started: Instant::now(),
        }
    }

    fn record(
        &self,
        controller: Option<&RateController>,
        elapsed: Duration,
        status: Option<StatusCode>,
        body_complete: bool,
    ) {
        runtime_metrics::http::record_client_request(
            elapsed,
            &self.origin,
            status.map(|status| status.as_u16()),
        );
        if let Some(controller) = controller
            && let Some(outcome) = classify(status, body_complete, elapsed, self.slow_threshold)
        {
            controller.record_outcome(outcome);
            if let Some(window) = controller.adaptive_window()
                && let Some(threshold) = self.slow_threshold
                && self
                    .warning
                    .lock()
                    .observe(outcome, window, controller.at_floor_for_window())
            {
                tracing::warn!(
                    "{}",
                    slow_threshold_warning(&self.origin, &self.dataset, threshold)
                );
            }
        }
    }
}

/// One observation per actual send. Consuming `finish` prevents double counting.
pub struct HttpAttempt<'a> {
    observer: &'a HttpAttemptObserver,
    controller: Option<&'a RateController>,
    started: Instant,
}

impl HttpAttempt<'_> {
    /// Finish as soon as the body terminates, before decoding rows or retry waits.
    pub fn finish(self, status: Option<StatusCode>, body_complete: bool) {
        self.observer.record(
            self.controller,
            self.started.elapsed(),
            status,
            body_complete,
        );
    }
}

fn classify(
    status: Option<StatusCode>,
    body_complete: bool,
    elapsed: Duration,
    threshold: Option<Duration>,
) -> Option<RequestOutcome> {
    let Some(status) = status.filter(|_| body_complete) else {
        return Some(RequestOutcome::Failure);
    };
    if crate::resilient_http::status_is_retryable(status) {
        Some(RequestOutcome::Failure)
    } else if status.is_success() {
        Some(
            if threshold.is_some_and(|threshold| !threshold.is_zero() && elapsed > threshold) {
                RequestOutcome::Slow
            } else {
                RequestOutcome::Success
            },
        )
    } else {
        None
    }
}

fn slow_threshold_warning(origin: &str, dataset: &str, threshold: Duration) -> String {
    format!(
        "Responses from '{origin}' for dataset '{dataset}' still take longer than its {}s `rate_control_slow_response_threshold` at the minimum request rate, so the threshold may be below this API's normal response time. Check `http_client_request_duration_ms` and raise `rate_control_slow_response_threshold` for dataset '{dataset}'.",
        threshold.as_secs_f64()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_uses_complete_bodies_and_a_strict_boundary() {
        let threshold = Duration::from_millis(500);
        for elapsed in [Duration::ZERO, threshold] {
            assert_eq!(
                classify(Some(StatusCode::OK), true, elapsed, Some(threshold)),
                Some(RequestOutcome::Success)
            );
        }
        let elapsed = threshold + Duration::from_nanos(1);
        assert_eq!(
            classify(Some(StatusCode::OK), true, elapsed, Some(threshold)),
            Some(RequestOutcome::Slow)
        );
        for off in [None, Some(Duration::ZERO)] {
            assert_eq!(
                classify(Some(StatusCode::OK), true, elapsed, off),
                Some(RequestOutcome::Success)
            );
        }
        assert_eq!(
            classify(Some(StatusCode::OK), false, elapsed, Some(threshold)),
            Some(RequestOutcome::Failure)
        );
        assert_eq!(
            classify(None, false, elapsed, Some(threshold)),
            Some(RequestOutcome::Failure)
        );
        for status in [408, 429, 500, 503] {
            assert_eq!(
                classify(
                    Some(StatusCode::from_u16(status).expect("valid status")),
                    true,
                    elapsed,
                    Some(threshold)
                ),
                Some(RequestOutcome::Failure)
            );
        }
        for status in [301, 400, 401, 403, 404] {
            assert_eq!(
                classify(
                    Some(StatusCode::from_u16(status).expect("valid status")),
                    true,
                    elapsed,
                    Some(threshold)
                ),
                None
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn warnings_are_once_per_dataset_after_continued_slow_responses_at_floor() {
        let url = Url::parse("https://api.example.com/items").expect("test URL");
        let slow =
            HttpAttemptObserver::new(&url, "items".to_string(), Some(Duration::from_millis(100)));
        let fast =
            HttpAttemptObserver::new(&url, "search".to_string(), Some(Duration::from_secs(1)));
        let new_dataset =
            HttpAttemptObserver::new(&url, "new".to_string(), Some(Duration::from_millis(100)));
        let window = Duration::from_secs(10);
        let controller = RateController::builder()
            .with_max_concurrent_requests(2)
            .with_adaptive(
                runtime_rate_control::AdaptiveRateControl::new(0.1, window)
                    .expect("adaptive config"),
                "https://api.example.com:443",
            )
            .build();
        let elapsed = Duration::from_millis(200);
        for _ in 0..100 {
            slow.record(Some(&controller), elapsed, Some(StatusCode::OK), true);
        }
        assert!(!slow.warning.lock().warned);
        tokio::time::advance(window).await;
        slow.record(Some(&controller), elapsed, Some(StatusCode::OK), true);
        assert!(slow.warning.lock().warned);
        fast.record(Some(&controller), elapsed, Some(StatusCode::OK), true);
        assert!(
            !fast.warning.lock().warned,
            "a distinct threshold must not inherit a warning"
        );
        new_dataset.record(Some(&controller), elapsed, Some(StatusCode::OK), true);
        assert!(
            !new_dataset.warning.lock().warned,
            "one slow sample must not inherit another dataset's history"
        );
        assert!(
            !slow
                .warning
                .lock()
                .observe(RequestOutcome::Slow, window, true),
            "warning is emitted only once"
        );
        assert!(
            !slow
                .warning
                .lock()
                .observe(RequestOutcome::Success, window, false)
        );
        tokio::time::advance(window).await;
        assert!(
            !slow
                .warning
                .lock()
                .observe(RequestOutcome::Slow, window, true),
            "recovery does not reset the once-per-dataset warning"
        );
    }

    #[test]
    fn warning_names_the_dataset_and_fix() {
        assert_eq!(
            slow_threshold_warning("https://api.example.com", "items", Duration::from_secs(1)),
            "Responses from 'https://api.example.com' for dataset 'items' still take longer than its 1s `rate_control_slow_response_threshold` at the minimum request rate, so the threshold may be below this API's normal response time. Check `http_client_request_duration_ms` and raise `rate_control_slow_response_threshold` for dataset 'items'."
        );
    }
}
