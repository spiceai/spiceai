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

//! Adaptive (client-side circuit-breaker) rate control.
//!
//! When an origin starts failing or timing out, admitting requests at the
//! statically-configured rate only makes matters worse. An adaptive controller
//! watches the outcome of every request and produces an **admission
//! coefficient** in `[0, 1]` — the fraction of the configured rate to allow —
//! that the [`RateController`](crate::RateController) applies by weighting each
//! request against its fixed token buckets and concurrency permits. It lowers the
//! effective rate as the origin fails and raises it again as the origin recovers.
//!
//! The control law is Google SRE client-side throttling over a time-decaying
//! window of attempts and successes, with two hyperparameters:
//!
//! * **failure threshold** — the upstream error rate above which throttling
//!   begins, as a fraction in `(0, 1)`. It maps to the SRE coefficient
//!   `k = 1 / (1 - threshold)` (a 50% threshold is `k = 2`), the form the math
//!   below runs on.
//! * **window** — the reaction/recovery half-life (`> 0`). Request outcomes decay
//!   with this half-life, so a shorter window reacts and recovers faster.
//!
//! Adaptive control is a *modifier* on statically-configured limits, never a
//! limiter of its own: the single coefficient scales every configured limit
//! (per-second, per-minute, and concurrency) uniformly. An origin with no static
//! limit has nothing to modify, so the caller rejects that configuration before
//! building a controller.

use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// Default failure threshold: throttle once the error rate exceeds 10%
/// (equivalently, the SRE coefficient `k = 1 / (1 - 0.1) ≈ 1.11`).
pub const DEFAULT_ADAPTIVE_FAILURE_THRESHOLD: f64 = 0.1;

/// Default reaction/recovery window: the decay half-life over which a failure
/// burst ages out of the window.
pub const DEFAULT_ADAPTIVE_WINDOW: Duration = Duration::from_secs(10);

/// An enabled adaptive rate control, resolved from the user's hyperparameters.
///
/// Absence of adaptive control is represented by an `Option<AdaptiveRateControl>`
/// being `None` at the call site, not by a variant here: a disabled origin keeps
/// its static limits unscaled.
///
/// Built via [`AdaptiveRateControl::new`], which converts the user's failure
/// threshold to the SRE coefficient `k`. The user-facing parsing (the on/off
/// switch, the percentage/fraction threshold) lives in the connector wiring layer.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptiveRateControl {
    /// SRE coefficient, derived from the failure threshold (`k = 1 / (1 - threshold)`).
    k: f64,
    /// Decaying-window half-life.
    window: Duration,
}

impl AdaptiveRateControl {
    /// Build an adaptive control from the user-facing hyperparameters, validating
    /// both and converting the failure threshold to the SRE coefficient.
    ///
    /// `failure_threshold` is the error rate above which throttling begins, as a
    /// fraction in `(0, 1)` (e.g. `0.5` = throttle above a 50% error rate). It maps
    /// to the SRE coefficient `k = 1 / (1 - failure_threshold)`.
    ///
    /// # Errors
    /// Returns [`AdaptiveRateControlError::FailureThresholdInvalid`] when
    /// `failure_threshold` is not a finite fraction strictly between 0 and 1, and
    /// [`AdaptiveRateControlError::WindowInvalid`] when `window` is zero or
    /// non-finite (an infinite window would never let the origin recover).
    pub fn new(failure_threshold: f64, window: Duration) -> Result<Self, AdaptiveRateControlError> {
        if !failure_threshold.is_finite() || failure_threshold <= 0.0 || failure_threshold >= 1.0 {
            return Err(AdaptiveRateControlError::FailureThresholdInvalid { failure_threshold });
        }
        if window.is_zero() || window == Duration::MAX {
            return Err(AdaptiveRateControlError::WindowInvalid { window });
        }
        Ok(Self {
            k: 1.0 / (1.0 - failure_threshold),
            window,
        })
    }
}

/// Why the two adaptive hyperparameters could not be accepted.
///
/// The connector wiring layer turns this into a user-facing configuration error
/// that names the dataset, the offending parameter, and a fix.
#[derive(Clone, Debug, PartialEq)]
pub enum AdaptiveRateControlError {
    /// The failure threshold was not a finite fraction strictly between 0 and 1.
    FailureThresholdInvalid { failure_threshold: f64 },
    /// The window (decay half-life) was not a positive, finite duration.
    WindowInvalid { window: Duration },
}

/// The result of an HTTP request as the adaptive controller sees it: a 2xx is a
/// success, a retryable status (408/429/5xx) or a timeout/connection error is a
/// failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestOutcome {
    Success,
    Failure,
}

/// A live adaptive controller for one origin.
///
/// Cheap to share behind an `Arc`; window updates sit behind a short non-async
/// critical section. It computes the admission coefficient on demand (decayed to
/// the moment it is read), so a coefficient read after a quiet period reflects
/// recovery even with no new outcomes recorded.
#[derive(Debug)]
pub struct AdaptiveController {
    sre: SreState,
}

impl AdaptiveController {
    /// Build a controller for `control`.
    #[must_use]
    pub fn new(control: AdaptiveRateControl) -> Self {
        Self {
            sre: SreState::new(control.k, control.window),
        }
    }

    /// Record the outcome of one request.
    pub fn record(&self, outcome: RequestOutcome) {
        self.sre.record(outcome, Instant::now());
    }

    /// The fraction of requests the controller currently wants to admit, in
    /// `[0, 1]`. `1.0` means "admit everything" (a healthy origin).
    #[must_use]
    pub fn admission_coefficient(&self) -> f64 {
        self.admission_coefficient_at(Instant::now())
    }

    fn admission_coefficient_at(&self, now: Instant) -> f64 {
        self.sre.admission_coefficient(now).clamp(0.0, 1.0)
    }

    /// The real-valued weight one request should charge right now: `1 /
    /// coefficient` (`1.0` when the origin is healthy; `+inf` at coefficient 0).
    ///
    /// Charging `weight` cells against a fixed bucket scales the effective rate by
    /// the admission coefficient without mutating the bucket. This is the *desired*
    /// weight; each caller clamps it to the individual limiter's capacity and
    /// rounds to whole cells, so one small limit never bounds how deeply a larger
    /// one throttles, and reaching a limiter's capacity is its deepest throttle —
    /// roughly one request per window.
    #[must_use]
    pub fn acquire_weight(&self) -> f64 {
        let coefficient = self.admission_coefficient();
        if coefficient >= 1.0 {
            return 1.0;
        }
        1.0 / coefficient
    }
}

/// Google SRE client-side throttling over a time-decaying window.
///
/// `requests` counts attempts and `accepts` counts successes; both decay
/// exponentially with the window half-life. The admission coefficient is
/// `min(1, (k*accepts + 1) / (requests + 1))`.
#[derive(Debug)]
struct SreState {
    k: f64,
    half_life: Duration,
    window: Mutex<SreWindow>,
}

#[derive(Debug)]
struct SreWindow {
    requests: f64,
    accepts: f64,
    last_update: Option<Instant>,
}

impl SreState {
    fn new(k: f64, half_life: Duration) -> Self {
        Self {
            k,
            half_life,
            window: Mutex::new(SreWindow {
                requests: 0.0,
                accepts: 0.0,
                last_update: None,
            }),
        }
    }

    fn record(&self, outcome: RequestOutcome, now: Instant) {
        let mut window = self.window.lock();
        window.decay_to(now, self.half_life);
        window.requests += 1.0;
        if outcome == RequestOutcome::Success {
            window.accepts += 1.0;
        }
    }

    fn admission_coefficient(&self, now: Instant) -> f64 {
        let (requests, accepts) = {
            let mut window = self.window.lock();
            window.decay_to(now, self.half_life);
            (window.requests, window.accepts)
        };

        // Both `+1`s live inside the fraction: numerator `k*accepts + 1`,
        // denominator `requests + 1`. At 100% success (accepts == requests) the
        // ratio is `(k*r + 1) / (r + 1) >= 1` for `k > 1` and any finite `r`, so
        // a healthy origin is never throttled. The coefficient falls below 1
        // exactly when `accepts/requests < 1/k` — i.e. the error rate exceeds the
        // configured failure threshold.
        let coefficient = (self.k * accepts + 1.0) / (requests + 1.0);
        coefficient.min(1.0)
    }
}

impl SreWindow {
    fn decay_to(&mut self, now: Instant, half_life: Duration) {
        let Some(last) = self.last_update else {
            self.last_update = Some(now);
            return;
        };

        let elapsed = now.saturating_duration_since(last);
        self.last_update = Some(now);
        if elapsed.is_zero() {
            return;
        }

        // 0.5 ^ (elapsed / half_life)
        let half_lives = elapsed.as_secs_f64() / half_life.as_secs_f64();
        let factor = 0.5_f64.powf(half_lives);
        self.requests *= factor;
        self.accepts *= factor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled(failure_threshold: f64) -> AdaptiveRateControl {
        AdaptiveRateControl::new(failure_threshold, DEFAULT_ADAPTIVE_WINDOW)
            .expect("test control should be valid")
    }

    #[test]
    fn failure_threshold_converts_to_k() {
        // k = 1 / (1 - threshold): 50% -> 2, 75% -> 4, 90% -> 10.
        assert!((enabled(0.5).k - 2.0).abs() < 1e-9);
        assert!((enabled(0.75).k - 4.0).abs() < 1e-9);
        assert!((enabled(0.9).k - 10.0).abs() < 1e-9);
    }

    /// Control-logic evidence: at a fixed 10% failure threshold, a higher backend
    /// error rate settles the admission coefficient lower, matching
    /// `min(1, k * success_rate)` with `k = 1 / (1 - threshold)`. The coefficient
    /// is the fraction of the configured rate admitted, so this is the load
    /// reduction. (Deterministic — it exercises the control law, not governor
    /// throughput; the end-to-end test covers the wired request path.)
    #[test]
    fn admission_coefficient_tracks_backend_error_rate() {
        // Settle the coefficient at a 10% failure threshold for a backend that
        // fails every `fail_every`-th request (0 = never fail). A large sample
        // makes the `+1` smoothing negligible; the tight loop makes window decay
        // over the elapsed microseconds immaterial.
        fn settled(fail_every: u32) -> f64 {
            let control = AdaptiveRateControl::new(0.10, DEFAULT_ADAPTIVE_WINDOW)
                .expect("a 10% failure threshold is valid");
            let controller = AdaptiveController::new(control);
            for request in 1..=8000u32 {
                let failed = fail_every != 0 && request % fail_every == 0;
                controller.record(if failed {
                    RequestOutcome::Failure
                } else {
                    RequestOutcome::Success
                });
            }
            controller.admission_coefficient()
        }

        // k = 1 / (1 - 0.10) = 1.111.
        let healthy = settled(0); // 0% errors  -> min(1, 1.111 * 1.00) = 1.000
        let err_25 = settled(4); // 25% errors -> min(1, 1.111 * 0.75) = 0.833
        let err_50 = settled(2); // 50% errors -> min(1, 1.111 * 0.50) = 0.556

        assert!(
            (healthy - 1.0).abs() < 1e-6,
            "a healthy backend must not be throttled, got {healthy}"
        );
        assert!(
            (err_25 - 0.8333).abs() < 0.02,
            "25% errors at a 10% threshold should settle near 0.833 (~17% load reduction), got {err_25}"
        );
        assert!(
            (err_50 - 0.5556).abs() < 0.02,
            "50% errors at a 10% threshold should settle near 0.556 (~44% load reduction), got {err_50}"
        );
        // A worse backend must reduce load further.
        assert!(
            healthy > err_25 && err_25 > err_50,
            "load reduction must grow with the error rate: {healthy} > {err_25} > {err_50}"
        );
    }

    #[test]
    fn new_validates_failure_threshold() {
        AdaptiveRateControl::new(0.5, DEFAULT_ADAPTIVE_WINDOW)
            .expect("a threshold strictly between 0 and 1 is valid");
        assert_eq!(
            AdaptiveRateControl::new(0.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid {
                failure_threshold: 0.0
            })
        );
        assert_eq!(
            AdaptiveRateControl::new(1.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid {
                failure_threshold: 1.0
            })
        );
        assert!(matches!(
            AdaptiveRateControl::new(-0.1, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid { .. })
        ));
        // A bare percentage-like number (e.g. "50" read as 50.0) is out of range.
        assert!(matches!(
            AdaptiveRateControl::new(50.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid { .. })
        ));
        assert!(matches!(
            AdaptiveRateControl::new(f64::NAN, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid { .. })
        ));
    }

    #[test]
    fn new_validates_window() {
        assert_eq!(
            AdaptiveRateControl::new(0.5, Duration::ZERO),
            Err(AdaptiveRateControlError::WindowInvalid {
                window: Duration::ZERO
            })
        );
        // An infinite window (the saturated value a duration parser yields for
        // "inf") would never let the origin recover.
        assert_eq!(
            AdaptiveRateControl::new(0.5, Duration::MAX),
            Err(AdaptiveRateControlError::WindowInvalid {
                window: Duration::MAX
            })
        );
    }

    #[test]
    fn never_throttles_a_fully_healthy_origin() {
        let controller = AdaptiveController::new(enabled(0.5));
        controller.record(RequestOutcome::Success);
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
        for _ in 0..999 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn throttles_exactly_above_the_failure_threshold() {
        // 75% failure threshold => k = 4 => throttle when success rate < 1/4.
        let controller = AdaptiveController::new(enabled(0.75));
        let total = 1000;
        // Success ratio 0.30 > 0.25 (error rate 70% < 75%): not throttled.
        for i in 0..total {
            controller.record(if i < 300 {
                RequestOutcome::Success
            } else {
                RequestOutcome::Failure
            });
        }
        assert!(
            (controller.admission_coefficient() - 1.0).abs() < f64::EPSILON,
            "error rate below the threshold must not throttle, got {}",
            controller.admission_coefficient()
        );

        // Success ratio 0.20 < 0.25 (error rate 80% > 75%): throttled.
        let controller = AdaptiveController::new(enabled(0.75));
        for i in 0..total {
            controller.record(if i < 200 {
                RequestOutcome::Success
            } else {
                RequestOutcome::Failure
            });
        }
        assert!(
            controller.admission_coefficient() < 1.0,
            "error rate above the threshold must throttle, got {}",
            controller.admission_coefficient()
        );
    }

    #[test]
    fn coefficient_is_clamped_to_unit_interval() {
        let controller = AdaptiveController::new(enabled(0.9));
        controller.record(RequestOutcome::Success);
        let coefficient = controller.admission_coefficient();
        assert!(
            (0.0..=1.0).contains(&coefficient),
            "coefficient {coefficient} escaped [0, 1]"
        );
        assert!((coefficient - 1.0).abs() < f64::EPSILON);
    }

    /// Behavioral demonstration: the coefficient falls below 1 under a sustained
    /// failure burst and returns to 1 after the burst decays out of the window.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn coefficient_recovers_after_failures_decay() {
        // 75% failure threshold => k = 4.
        let controller = AdaptiveController::new(enabled(0.75));

        for _ in 0..50 {
            controller.record(RequestOutcome::Success);
        }
        let healthy = controller.admission_coefficient();

        for _ in 0..2000 {
            controller.record(RequestOutcome::Failure);
        }
        let failing = controller.admission_coefficient();

        // Let the failure burst age out of the decaying window, then resume
        // successes. The coefficient returns to 1.
        tokio::time::advance(Duration::from_mins(1)).await;
        for _ in 0..500 {
            controller.record(RequestOutcome::Success);
        }
        let recovered = controller.admission_coefficient();

        assert!(
            (healthy - 1.0).abs() < f64::EPSILON,
            "healthy origin must not be throttled, got {healthy}"
        );
        assert!(
            failing < 0.5,
            "sustained failures should throttle, got {failing}"
        );
        assert!(
            (recovered - 1.0).abs() < f64::EPSILON,
            "recovered origin should stop throttling, got {recovered}"
        );
    }
}
