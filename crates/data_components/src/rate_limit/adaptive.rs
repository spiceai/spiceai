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
//! window of attempts and successes. It has exactly two hyperparameters, the two
//! degrees of freedom of the control system:
//!
//! * **magnitude** (`k`, `> 1`) — *where* the effective rate settles for a given
//!   failure rate. Throttling begins only when the success rate falls below
//!   `1 / k`, so a larger magnitude tolerates a higher failure rate before it
//!   throttles.
//! * **elasticity** (the window half-life, `> 0`) — *how fast* the controller
//!   reacts to and recovers from a change in the failure rate. A shorter
//!   half-life reacts and recovers faster; a longer one is smoother and slower.
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

/// Default magnitude (`k`): throttle when the success rate drops below 1/2.
pub const DEFAULT_ADAPTIVE_MAGNITUDE: f64 = 2.0;

/// Default elasticity: an attempt recorded one half-life ago counts half as much
/// toward the window as one recorded now, so a failure burst ages out over
/// roughly this long.
pub const DEFAULT_ADAPTIVE_ELASTICITY: Duration = Duration::from_secs(10);

/// How much admission credit the deterministic [`AdaptiveController::try_admit`]
/// gate must accumulate before it admits one request. Kept at 1.0 so the
/// long-run admitted fraction equals the admission coefficient.
const ADMISSION_CREDIT_PER_REQUEST: f64 = 1.0;

/// The adaptive rate-control strategy resolved for an origin.
///
/// The wiring layer (`data-http-rate-control`) resolves the three
/// `adaptive_rate_control*` parameters into this: [`parse_adaptive_mode`] for the
/// on/off switch, then [`AdaptiveRateControl::enabled`] for the two
/// hyperparameters.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AdaptiveRateControl {
    /// Adaptive control is off. The effective rate is exactly the static config.
    Disabled,
    /// SRE client-side throttling with `magnitude` (`k > 1`) and `elasticity`
    /// (the decaying-window half-life).
    Enabled {
        magnitude: f64,
        elasticity: Duration,
    },
}

impl AdaptiveRateControl {
    /// Build an enabled control from its two hyperparameters, validating both.
    ///
    /// # Errors
    /// Returns [`AdaptiveRateControlParseError::MagnitudeInvalid`] when `magnitude`
    /// is not a finite number greater than 1, and
    /// [`AdaptiveRateControlParseError::ElasticityInvalid`] when `elasticity` is
    /// zero or non-finite (an infinite half-life would never let the origin
    /// recover).
    pub fn enabled(
        magnitude: f64,
        elasticity: Duration,
    ) -> Result<Self, AdaptiveRateControlParseError> {
        if !magnitude.is_finite() || magnitude <= 1.0 {
            return Err(AdaptiveRateControlParseError::MagnitudeInvalid { magnitude });
        }
        if elasticity.is_zero() || elasticity == Duration::MAX {
            return Err(AdaptiveRateControlParseError::ElasticityInvalid { elasticity });
        }
        Ok(Self::Enabled {
            magnitude,
            elasticity,
        })
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
    /// The magnitude (SRE coefficient `k`) was not a finite number `> 1`.
    MagnitudeInvalid { magnitude: f64 },
    /// The elasticity (decay-window half-life) was not a positive, finite duration.
    ElasticityInvalid { elasticity: Duration },
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

        let (magnitude, elasticity) = match control {
            AdaptiveRateControl::Disabled => return None,
            AdaptiveRateControl::Enabled {
                magnitude,
                elasticity,
            } => (magnitude, elasticity),
        };

        Some(Self {
            ceiling,
            sre: SreState::new(magnitude, elasticity),
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
/// exponentially with the `half_life` (the elasticity). The admission
/// coefficient is `min(1, (k*accepts + 1) / (requests + 1))`.
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
        // exactly when `accepts/requests < 1/k`.
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

    fn enabled(magnitude: f64) -> AdaptiveRateControl {
        AdaptiveRateControl::enabled(magnitude, DEFAULT_ADAPTIVE_ELASTICITY)
            .expect("test control should be valid")
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
        // A bare number is no longer a valid mode: magnitude is its own parameter.
        assert_eq!(
            parse_adaptive_mode("2.0"),
            Err(AdaptiveRateControlParseError::UnrecognizedMode {
                value: "2.0".to_string()
            })
        );
    }

    #[test]
    fn enabled_validates_magnitude() {
        assert_eq!(
            AdaptiveRateControl::enabled(2.0, DEFAULT_ADAPTIVE_ELASTICITY),
            Ok(AdaptiveRateControl::Enabled {
                magnitude: 2.0,
                elasticity: DEFAULT_ADAPTIVE_ELASTICITY
            })
        );
        // Magnitude must be strictly greater than 1.
        assert_eq!(
            AdaptiveRateControl::enabled(1.0, DEFAULT_ADAPTIVE_ELASTICITY),
            Err(AdaptiveRateControlParseError::MagnitudeInvalid { magnitude: 1.0 })
        );
        assert_eq!(
            AdaptiveRateControl::enabled(0.5, DEFAULT_ADAPTIVE_ELASTICITY),
            Err(AdaptiveRateControlParseError::MagnitudeInvalid { magnitude: 0.5 })
        );
        assert!(matches!(
            AdaptiveRateControl::enabled(f64::INFINITY, DEFAULT_ADAPTIVE_ELASTICITY),
            Err(AdaptiveRateControlParseError::MagnitudeInvalid { .. })
        ));
    }

    #[test]
    fn enabled_validates_elasticity() {
        // Zero half-life would make the window collapse.
        assert_eq!(
            AdaptiveRateControl::enabled(2.0, Duration::ZERO),
            Err(AdaptiveRateControlParseError::ElasticityInvalid {
                elasticity: Duration::ZERO
            })
        );
        // An infinite half-life (the saturated value a duration parser yields for
        // "inf") would never let the origin recover.
        assert_eq!(
            AdaptiveRateControl::enabled(2.0, Duration::MAX),
            Err(AdaptiveRateControlParseError::ElasticityInvalid {
                elasticity: Duration::MAX
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
            AdaptiveController::new(enabled(2.0), 100.0).expect("sre controller should build");

        // accepts == requests at every finite count => coefficient exactly 1.
        controller.record(RequestOutcome::Success);
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
        for _ in 0..999 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sre_throttles_exactly_below_one_over_magnitude() {
        let magnitude = 4.0;
        let controller = AdaptiveController::new(enabled(magnitude), 100.0)
            .expect("sre controller should build");

        // Build a large window so the "+1" terms are negligible and the
        // threshold sits at accepts/requests == 1/magnitude.
        // Just above 1/k (ratio 0.30 > 0.25): not throttled (coefficient == 1).
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
            "ratio above 1/k must not throttle, got {}",
            controller.admission_coefficient()
        );

        // Just below 1/k (ratio 0.20 < 0.25): throttled (coefficient < 1).
        let controller = AdaptiveController::new(enabled(magnitude), 100.0)
            .expect("sre controller should build");
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
            "ratio below 1/k must throttle, got {}",
            controller.admission_coefficient()
        );
    }

    #[test]
    fn sre_coefficient_is_clamped_to_unit_interval() {
        let controller =
            AdaptiveController::new(enabled(10.0), 100.0).expect("sre controller should build");
        // A single success with a large magnitude would push the raw ratio above
        // 1; the coefficient must still clamp to 1.
        controller.record(RequestOutcome::Success);
        let coefficient = controller.admission_coefficient();
        assert!(
            (0.0..=1.0).contains(&coefficient),
            "coefficient {coefficient} escaped [0, 1]"
        );
        assert!((coefficient - 1.0).abs() < f64::EPSILON);
    }
}
