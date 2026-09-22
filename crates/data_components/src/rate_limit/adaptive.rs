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

//! Adaptive (client-side circuit-breaker) rate control for the HTTP data
//! connector.
//!
//! When an origin starts failing or timing out, admitting requests at the
//! statically-configured rate only makes matters worse. An adaptive controller
//! watches the outcome of every request and *dynamically* lowers the effective
//! request rate, then raises it again as the origin recovers.
//!
//! The control law is Google SRE client-side throttling over a time-decaying
//! window of attempts and successes. It has two user-facing hyperparameters:
//!
//! * **failure threshold** — the upstream error rate above which throttling
//!   begins, as a fraction in `(0, 1)`. Below this error rate the configured
//!   limits are used unchanged; above it, admission is scaled down in proportion
//!   to the success rate. It maps to the SRE coefficient `k = 1 / (1 - threshold)`
//!   (so a 50% threshold is `k = 2`), the form the math below runs on.
//! * **window** — the reaction/recovery half-life (`> 0`). Request outcomes decay
//!   with this half-life, so a shorter window reacts to and recovers from failures
//!   faster; a longer one is smoother and slower.
//!
//! The controller exposes a single `admission_coefficient()` in `[0, 1]` — the
//! fraction of requests to admit — and a deterministic
//! [`AdaptiveController::try_admit`] gate that turns that fraction into an
//! admit/throttle decision.
//!
//! Adaptive control is a *modifier* on the origin's statically-configured rate
//! limits, never a limiter of its own. The single admission coefficient reduces
//! the admitted request rate uniformly in front of every configured limit
//! (per-second, per-minute, and concurrency), so an origin with several limits
//! is scaled coherently by one factor. An origin with no static limit has
//! nothing to modify, so enabling adaptive control there is a configuration
//! error caught before a controller is ever built.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// The smallest ceiling a controller will scale within. Never below one, so a
/// recovering origin always gets at least one probe request through.
const MIN_CEILING: f64 = 1.0;

/// Default failure threshold: throttle once the error rate exceeds 50%
/// (equivalently, the SRE coefficient `k = 1 / (1 - 0.5) = 2`).
pub const DEFAULT_ADAPTIVE_FAILURE_THRESHOLD: f64 = 0.5;

/// Default reaction/recovery window: the decay half-life over which a failure
/// burst ages out of the window.
pub const DEFAULT_ADAPTIVE_WINDOW: Duration = Duration::from_secs(10);

/// How much admission credit the deterministic [`AdaptiveController::try_admit`]
/// gate must accumulate before it admits one request. Kept at 1.0 so the
/// long-run admitted fraction equals the admission coefficient.
const ADMISSION_CREDIT_PER_REQUEST: f64 = 1.0;

/// The adaptive rate-control strategy resolved for an origin.
///
/// The wiring layer (`data-http-rate-control`) resolves the three
/// `adaptive_rate_control*` parameters into this: [`parse_adaptive_mode`] for the
/// on/off switch, then [`AdaptiveRateControl::enabled`] for the two
/// hyperparameters (which converts the user's failure threshold to `k`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AdaptiveRateControl {
    /// Adaptive control is off. The effective rate is exactly the static config.
    Disabled,
    /// SRE client-side throttling. `k` is the coefficient derived from the user's
    /// failure threshold (`k = 1 / (1 - failure_threshold)`); `window` is the
    /// decaying-window half-life.
    Enabled { k: f64, window: Duration },
}

impl AdaptiveRateControl {
    /// Build an enabled control from the user-facing hyperparameters, validating
    /// both and converting the failure threshold to the SRE coefficient.
    ///
    /// `failure_threshold` is the error rate above which throttling begins, as a
    /// fraction in `(0, 1)` (e.g. `0.5` = throttle above a 50% error rate). It maps
    /// to the SRE coefficient `k = 1 / (1 - failure_threshold)`.
    ///
    /// # Errors
    /// Returns [`AdaptiveRateControlParseError::FailureThresholdInvalid`] when
    /// `failure_threshold` is not a finite fraction strictly between 0 and 1, and
    /// [`AdaptiveRateControlParseError::WindowInvalid`] when `window` is zero or
    /// non-finite (an infinite window would never let the origin recover).
    pub fn enabled(
        failure_threshold: f64,
        window: Duration,
    ) -> Result<Self, AdaptiveRateControlParseError> {
        if !failure_threshold.is_finite() || failure_threshold <= 0.0 || failure_threshold >= 1.0 {
            return Err(AdaptiveRateControlParseError::FailureThresholdInvalid {
                failure_threshold,
            });
        }
        if window.is_zero() || window == Duration::MAX {
            return Err(AdaptiveRateControlParseError::WindowInvalid { window });
        }
        let k = 1.0 / (1.0 - failure_threshold);
        Ok(Self::Enabled { k, window })
    }
}

/// The parsed value of the `adaptive_rate_control` on/off switch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptiveMode {
    Disabled,
    Enabled,
}

/// Why an `adaptive_rate_control*` parameter value could not be parsed.
///
/// The wiring layer (`data-http-rate-control`) turns this into a user-facing
/// `InvalidConfiguration` error that names the dataset, the offending parameter,
/// and a fix.
#[derive(Clone, Debug, PartialEq)]
pub enum AdaptiveRateControlParseError {
    /// The `adaptive_rate_control` value was neither `disabled` nor `enabled`.
    UnrecognizedMode { value: String },
    /// The failure threshold was not a finite fraction strictly between 0 and 1.
    FailureThresholdInvalid { failure_threshold: f64 },
    /// The window (decay half-life) was not a positive, finite duration.
    WindowInvalid { window: Duration },
}

/// Parse the `adaptive_rate_control` on/off switch.
///
/// * absent / empty / `disabled` -> [`AdaptiveMode::Disabled`]
/// * `enabled` -> [`AdaptiveMode::Enabled`]
///
/// # Errors
/// Returns [`AdaptiveRateControlParseError::UnrecognizedMode`] for any other value.
pub fn parse_adaptive_mode(value: &str) -> Result<AdaptiveMode, AdaptiveRateControlParseError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "disabled" => Ok(AdaptiveMode::Disabled),
        "enabled" => Ok(AdaptiveMode::Enabled),
        _ => Err(AdaptiveRateControlParseError::UnrecognizedMode {
            value: value.trim().to_string(),
        }),
    }
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
/// Cheap to share behind an `Arc`; window updates are behind a short non-async
/// critical section and the admission gate is lock-free.
#[derive(Debug)]
pub struct AdaptiveController {
    ceiling: f64,
    sre: SreState,
    /// Fractional admission credit for the deterministic [`Self::try_admit`] gate.
    admission_credit: AtomicU64,
}

impl AdaptiveController {
    /// Build a controller for `control`, or `None` when control is disabled.
    ///
    /// `ceiling` is the origin's configured static rate limit — the reference the
    /// admission coefficient scales (the largest configured per-second /
    /// per-minute / concurrency limit). Adaptive control has no ceiling of its
    /// own: callers must derive this from the configured limits and reject an
    /// enabled controller with no static limit before reaching here. It is
    /// clamped to be at least [`MIN_CEILING`].
    #[must_use]
    pub fn new(control: AdaptiveRateControl, ceiling: f64) -> Option<Self> {
        let ceiling = if ceiling.is_finite() && ceiling >= MIN_CEILING {
            ceiling
        } else {
            MIN_CEILING
        };

        let (k, window) = match control {
            AdaptiveRateControl::Disabled => return None,
            AdaptiveRateControl::Enabled { k, window } => (k, window),
        };

        Some(Self {
            ceiling,
            sre: SreState::new(k, window),
            admission_credit: AtomicU64::new(0.0_f64.to_bits()),
        })
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

    /// The effective request-rate limit the controller currently allows: the
    /// admission coefficient scaled by the ceiling.
    #[must_use]
    pub fn effective_limit(&self) -> f64 {
        self.admission_coefficient_at(Instant::now()) * self.ceiling
    }

    /// The static ceiling this controller scales within.
    #[must_use]
    pub fn ceiling(&self) -> f64 {
        self.ceiling
    }

    /// Decide whether to admit one request right now.
    ///
    /// Deterministic token-style gate: each call adds the current admission
    /// coefficient to an internal credit; a request is admitted (and one unit of
    /// credit spent) once credit reaches [`ADMISSION_CREDIT_PER_REQUEST`].
    /// Over many calls the admitted fraction converges to the coefficient, so a
    /// coefficient of `0.1` admits roughly one request in ten and throttles the
    /// rest — without any randomness.
    #[must_use]
    pub fn try_admit(&self) -> bool {
        self.try_admit_at(Instant::now())
    }

    fn try_admit_at(&self, now: Instant) -> bool {
        let coefficient = self.admission_coefficient_at(now);
        // Fast path: a fully-open controller always admits and never needs to
        // touch the credit accumulator.
        if coefficient >= 1.0 {
            return true;
        }

        let mut current = self.admission_credit.load(Ordering::Relaxed);
        loop {
            let credit = f64::from_bits(current) + coefficient;
            let (admit, next_credit) = if credit >= ADMISSION_CREDIT_PER_REQUEST {
                (true, credit - ADMISSION_CREDIT_PER_REQUEST)
            } else {
                (false, credit)
            };

            match self.admission_credit.compare_exchange_weak(
                current,
                next_credit.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return admit,
                Err(observed) => current = observed,
            }
        }
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
        AdaptiveRateControl::enabled(failure_threshold, DEFAULT_ADAPTIVE_WINDOW)
            .expect("test control should be valid")
    }

    /// The `k` the controller runs on, for asserting the threshold conversion.
    fn coefficient_k(control: AdaptiveRateControl) -> f64 {
        match control {
            AdaptiveRateControl::Enabled { k, .. } => k,
            AdaptiveRateControl::Disabled => panic!("expected an enabled control"),
        }
    }

    #[test]
    fn parse_mode_disabled_and_enabled() {
        assert_eq!(parse_adaptive_mode(""), Ok(AdaptiveMode::Disabled));
        assert_eq!(parse_adaptive_mode("disabled"), Ok(AdaptiveMode::Disabled));
        assert_eq!(parse_adaptive_mode("DISABLED"), Ok(AdaptiveMode::Disabled));
        assert_eq!(parse_adaptive_mode("enabled"), Ok(AdaptiveMode::Enabled));
        assert_eq!(parse_adaptive_mode(" Enabled "), Ok(AdaptiveMode::Enabled));
    }

    #[test]
    fn parse_mode_rejects_unrecognized() {
        assert_eq!(
            parse_adaptive_mode("sometimes"),
            Err(AdaptiveRateControlParseError::UnrecognizedMode {
                value: "sometimes".to_string()
            })
        );
        // A bare number is no longer a valid mode: the threshold is its own parameter.
        assert_eq!(
            parse_adaptive_mode("2.0"),
            Err(AdaptiveRateControlParseError::UnrecognizedMode {
                value: "2.0".to_string()
            })
        );
    }

    #[test]
    fn failure_threshold_converts_to_k() {
        // k = 1 / (1 - threshold): 50% -> 2, 75% -> 4, 90% -> 10.
        assert!((coefficient_k(enabled(0.5)) - 2.0).abs() < 1e-9);
        assert!((coefficient_k(enabled(0.75)) - 4.0).abs() < 1e-9);
        assert!((coefficient_k(enabled(0.9)) - 10.0).abs() < 1e-9);
    }

    #[test]
    fn enabled_validates_failure_threshold() {
        assert!(matches!(
            AdaptiveRateControl::enabled(0.5, DEFAULT_ADAPTIVE_WINDOW),
            Ok(AdaptiveRateControl::Enabled { .. })
        ));
        // The threshold is an error rate strictly between 0 and 1.
        assert_eq!(
            AdaptiveRateControl::enabled(0.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlParseError::FailureThresholdInvalid {
                failure_threshold: 0.0
            })
        );
        assert_eq!(
            AdaptiveRateControl::enabled(1.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlParseError::FailureThresholdInvalid {
                failure_threshold: 1.0
            })
        );
        assert!(matches!(
            AdaptiveRateControl::enabled(-0.1, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlParseError::FailureThresholdInvalid { .. })
        ));
        // A bare percentage-like number (e.g. "50" read as 50.0) is out of range.
        assert!(matches!(
            AdaptiveRateControl::enabled(50.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlParseError::FailureThresholdInvalid { .. })
        ));
        assert!(matches!(
            AdaptiveRateControl::enabled(f64::NAN, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlParseError::FailureThresholdInvalid { .. })
        ));
    }

    #[test]
    fn enabled_validates_window() {
        // Zero window would make the decay collapse.
        assert_eq!(
            AdaptiveRateControl::enabled(0.5, Duration::ZERO),
            Err(AdaptiveRateControlParseError::WindowInvalid {
                window: Duration::ZERO
            })
        );
        // An infinite window (the saturated value a duration parser yields for
        // "inf") would never let the origin recover.
        assert_eq!(
            AdaptiveRateControl::enabled(0.5, Duration::MAX),
            Err(AdaptiveRateControlParseError::WindowInvalid {
                window: Duration::MAX
            })
        );
    }

    #[test]
    fn disabled_control_builds_no_controller() {
        assert!(AdaptiveController::new(AdaptiveRateControl::Disabled, 32.0).is_none());
    }

    #[test]
    fn sre_never_throttles_a_fully_healthy_origin() {
        let controller =
            AdaptiveController::new(enabled(0.5), 100.0).expect("sre controller should build");

        // accepts == requests at every finite count => coefficient exactly 1.
        controller.record(RequestOutcome::Success);
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
        for _ in 0..999 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sre_throttles_exactly_above_the_failure_threshold() {
        // 75% failure threshold => k = 4 => throttle when success rate < 1/4.
        let controller =
            AdaptiveController::new(enabled(0.75), 100.0).expect("sre controller should build");

        // Build a large window so the "+1" terms are negligible and the
        // threshold sits at accepts/requests == 1/k.
        // Success ratio 0.30 > 0.25 (error rate 70% < 75%): not throttled.
        let total = 1000;
        let accepts_above = 300;
        for i in 0..total {
            controller.record(if i < accepts_above {
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
        let controller =
            AdaptiveController::new(enabled(0.75), 100.0).expect("sre controller should build");
        let accepts_below = 200;
        for i in 0..total {
            controller.record(if i < accepts_below {
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
    fn sre_coefficient_is_clamped_to_unit_interval() {
        let controller =
            AdaptiveController::new(enabled(0.9), 100.0).expect("sre controller should build");
        // A single success with a large k would push the raw ratio above 1;
        // the coefficient must still clamp to 1.
        controller.record(RequestOutcome::Success);
        let coefficient = controller.admission_coefficient();
        assert!(
            (0.0..=1.0).contains(&coefficient),
            "coefficient {coefficient} escaped [0, 1]"
        );
        assert!((coefficient - 1.0).abs() < f64::EPSILON);
    }
}
