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
//! request rate, then raises it again as the origin recovers. Two interchangeable
//! strategies are implemented:
//!
//! * [`AdaptiveRateControl::Aimd`] — additive-increase / multiplicative-decrease.
//!   Keep an effective limit; halve it on a failure, add one on a success, and
//!   clamp it to `[floor, ceiling]`.
//! * [`AdaptiveRateControl::SreThrottle`] — the Google SRE client-side throttling
//!   formula over a time-decaying window of attempts and successes.
//!
//! Both expose a single `admission_coefficient()` in `[0, 1]` — the fraction of
//! requests the limiter should admit — and a deterministic [`AdaptiveController::try_admit`]
//! gate that turns that fraction into an admit/throttle decision.
//!
//! Adaptive control is a *modifier* on the origin's statically-configured rate
//! limits, never a limiter of its own. The single admission coefficient reduces
//! the admitted request rate uniformly in front of every configured limit
//! (per-second, per-minute, and concurrency), so an origin with several limits
//! is scaled coherently by one factor. An origin with no static limit has
//! nothing to modify, so enabling adaptive control there is a configuration
//! error caught before a controller is ever built.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::time::Instant;

/// The smallest effective limit AIMD will decay to. Never below one, so a
/// recovering origin always gets at least one probe request through.
const AIMD_FLOOR: f64 = 1.0;

/// Half-life of the SRE decaying window: an attempt recorded this long ago
/// counts half as much toward `requests`/`accepts` as one recorded now. This is
/// what lets the admission coefficient recover to 1 after a burst of failures
/// ages out.
const SRE_WINDOW_HALF_LIFE: std::time::Duration = std::time::Duration::from_secs(10);

/// How much admission credit the deterministic [`AdaptiveController::try_admit`]
/// gate must accumulate before it admits one request. Kept at 1.0 so the
/// long-run admitted fraction equals the admission coefficient.
const ADMISSION_CREDIT_PER_REQUEST: f64 = 1.0;

/// The adaptive rate-control strategy selected for an origin.
///
/// Parsed from the `adaptive_rate_control` dataset parameter via
/// [`parse_adaptive_rate_control`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AdaptiveRateControl {
    /// Adaptive control is off. The effective rate is exactly the static config.
    Disabled,
    /// Additive-increase / multiplicative-decrease control.
    Aimd,
    /// Google SRE client-side throttling with hyperparameter `k` (`k > 1`).
    SreThrottle { k: f64 },
}

/// Why an `adaptive_rate_control` parameter value could not be parsed.
///
/// The wiring layer (`data-http-rate-control`) turns this into a user-facing
/// `InvalidConfiguration` error that names the dataset and a fix.
#[derive(Clone, Debug, PartialEq)]
pub enum AdaptiveRateControlParseError {
    /// The value was neither `disabled`/`enabled` nor a valid positive float.
    Unrecognized { value: String },
    /// The value parsed as a float `k`, but SRE throttling needs `k > 1`.
    KNotGreaterThanOne { k: f64 },
}

/// Parse the `adaptive_rate_control` parameter string into a strategy.
///
/// * absent / empty / `disabled` -> [`AdaptiveRateControl::Disabled`]
/// * `enabled` -> [`AdaptiveRateControl::Aimd`] (the default enabled strategy)
/// * a finite positive float `k > 1` -> [`AdaptiveRateControl::SreThrottle`]
///
/// # Errors
/// Returns [`AdaptiveRateControlParseError::Unrecognized`] for a value that is
/// neither `disabled`/`enabled` nor a valid finite positive float, and
/// [`AdaptiveRateControlParseError::KNotGreaterThanOne`] for a float `k <= 1`.
pub fn parse_adaptive_rate_control(
    value: &str,
) -> Result<AdaptiveRateControl, AdaptiveRateControlParseError> {
    let trimmed = value.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "" | "disabled" => return Ok(AdaptiveRateControl::Disabled),
        "enabled" => return Ok(AdaptiveRateControl::Aimd),
        _ => {}
    }

    let k = trimmed
        .parse::<f64>()
        .ok()
        .filter(|k| k.is_finite() && *k > 0.0)
        .ok_or_else(|| AdaptiveRateControlParseError::Unrecognized {
            value: trimmed.to_string(),
        })?;

    if k <= 1.0 {
        return Err(AdaptiveRateControlParseError::KNotGreaterThanOne { k });
    }

    Ok(AdaptiveRateControl::SreThrottle { k })
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
/// Cheap to share behind an `Arc`; all interior state is lock-free (AIMD) or
/// behind a short non-async critical section (SRE).
#[derive(Debug)]
pub struct AdaptiveController {
    ceiling: f64,
    strategy: Strategy,
    /// Fractional admission credit for the deterministic [`Self::try_admit`] gate.
    admission_credit: AtomicU64,
}

#[derive(Debug)]
enum Strategy {
    Aimd(AimdState),
    Sre(SreState),
}

impl AdaptiveController {
    /// Build a controller for `control`, or `None` when control is disabled.
    ///
    /// `ceiling` is the origin's configured static rate limit — the reference
    /// the controller grows back toward (the largest configured per-second /
    /// per-minute / concurrency limit). Adaptive control has no ceiling of its
    /// own: callers must derive this from the configured limits and reject an
    /// enabled controller with no static limit before reaching here. It is
    /// clamped to be at least [`AIMD_FLOOR`].
    #[must_use]
    pub fn new(control: AdaptiveRateControl, ceiling: f64) -> Option<Self> {
        let ceiling = if ceiling.is_finite() && ceiling >= AIMD_FLOOR {
            ceiling
        } else {
            AIMD_FLOOR
        };

        let strategy = match control {
            AdaptiveRateControl::Disabled => return None,
            AdaptiveRateControl::Aimd => Strategy::Aimd(AimdState::new(ceiling)),
            AdaptiveRateControl::SreThrottle { k } => Strategy::Sre(SreState::new(k)),
        };

        Some(Self {
            ceiling,
            strategy,
            admission_credit: AtomicU64::new(0.0_f64.to_bits()),
        })
    }

    /// Record the outcome of one request.
    pub fn record(&self, outcome: RequestOutcome) {
        match &self.strategy {
            Strategy::Aimd(state) => state.record(outcome),
            Strategy::Sre(state) => state.record(outcome, Instant::now()),
        }
    }

    /// The fraction of requests the controller currently wants to admit, in
    /// `[0, 1]`. `1.0` means "admit everything" (a healthy origin).
    #[must_use]
    pub fn admission_coefficient(&self) -> f64 {
        self.admission_coefficient_at(Instant::now())
    }

    fn admission_coefficient_at(&self, now: Instant) -> f64 {
        let coefficient = match &self.strategy {
            Strategy::Aimd(state) => state.effective_limit() / self.ceiling,
            Strategy::Sre(state) => state.admission_coefficient(now),
        };
        coefficient.clamp(0.0, 1.0)
    }

    /// The effective request-rate limit the controller currently allows.
    ///
    /// For AIMD this is the tracked limit; for SRE it is the admission
    /// coefficient scaled by the ceiling, so both strategies report on the same
    /// scale.
    #[must_use]
    pub fn effective_limit(&self) -> f64 {
        match &self.strategy {
            Strategy::Aimd(state) => state.effective_limit(),
            Strategy::Sre(state) => state.admission_coefficient(Instant::now()) * self.ceiling,
        }
    }

    /// The static ceiling this controller grows back toward.
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

/// AIMD: an effective limit, halved on failure and grown by one on success,
/// clamped to `[AIMD_FLOOR, ceiling]`. Stored as the bits of an `f64` in an
/// `AtomicU64` so updates are lock-free.
#[derive(Debug)]
struct AimdState {
    effective_limit: AtomicU64,
    ceiling: f64,
}

impl AimdState {
    fn new(ceiling: f64) -> Self {
        Self {
            effective_limit: AtomicU64::new(ceiling.to_bits()),
            ceiling,
        }
    }

    fn effective_limit(&self) -> f64 {
        f64::from_bits(self.effective_limit.load(Ordering::Relaxed))
    }

    fn record(&self, outcome: RequestOutcome) {
        let mut current = self.effective_limit.load(Ordering::Relaxed);
        loop {
            let value = f64::from_bits(current);
            let next = match outcome {
                RequestOutcome::Success => (value + 1.0).min(self.ceiling),
                RequestOutcome::Failure => (value * 0.5).max(AIMD_FLOOR),
            };

            match self.effective_limit.compare_exchange_weak(
                current,
                next.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Google SRE client-side throttling over a time-decaying window.
///
/// `requests` counts attempts and `accepts` counts successes; both decay
/// exponentially with [`SRE_WINDOW_HALF_LIFE`]. The admission coefficient is
/// `min(1, (k*accepts + 1) / (requests + 1))`.
#[derive(Debug)]
struct SreState {
    k: f64,
    window: Mutex<SreWindow>,
}

#[derive(Debug)]
struct SreWindow {
    requests: f64,
    accepts: f64,
    last_update: Option<Instant>,
}

impl SreState {
    fn new(k: f64) -> Self {
        Self {
            k,
            window: Mutex::new(SreWindow {
                requests: 0.0,
                accepts: 0.0,
                last_update: None,
            }),
        }
    }

    fn record(&self, outcome: RequestOutcome, now: Instant) {
        let mut window = self.window.lock();
        window.decay_to(now);
        window.requests += 1.0;
        if outcome == RequestOutcome::Success {
            window.accepts += 1.0;
        }
    }

    fn admission_coefficient(&self, now: Instant) -> f64 {
        let (requests, accepts) = {
            let mut window = self.window.lock();
            window.decay_to(now);
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
    fn decay_to(&mut self, now: Instant) {
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
        let half_lives = elapsed.as_secs_f64() / SRE_WINDOW_HALF_LIFE.as_secs_f64();
        let factor = 0.5_f64.powf(half_lives);
        self.requests *= factor;
        self.accepts *= factor;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_disabled_enabled_and_sre() {
        assert_eq!(
            parse_adaptive_rate_control(""),
            Ok(AdaptiveRateControl::Disabled)
        );
        assert_eq!(
            parse_adaptive_rate_control("disabled"),
            Ok(AdaptiveRateControl::Disabled)
        );
        assert_eq!(
            parse_adaptive_rate_control("DISABLED"),
            Ok(AdaptiveRateControl::Disabled)
        );
        assert_eq!(
            parse_adaptive_rate_control("enabled"),
            Ok(AdaptiveRateControl::Aimd)
        );
        assert_eq!(
            parse_adaptive_rate_control(" Enabled "),
            Ok(AdaptiveRateControl::Aimd)
        );
        assert_eq!(
            parse_adaptive_rate_control("2.0"),
            Ok(AdaptiveRateControl::SreThrottle { k: 2.0 })
        );
    }

    #[test]
    fn parse_rejects_invalid_and_out_of_range_k() {
        assert_eq!(
            parse_adaptive_rate_control("sometimes"),
            Err(AdaptiveRateControlParseError::Unrecognized {
                value: "sometimes".to_string()
            })
        );
        // K must be strictly greater than 1.
        assert_eq!(
            parse_adaptive_rate_control("1.0"),
            Err(AdaptiveRateControlParseError::KNotGreaterThanOne { k: 1.0 })
        );
        assert_eq!(
            parse_adaptive_rate_control("0.5"),
            Err(AdaptiveRateControlParseError::KNotGreaterThanOne { k: 0.5 })
        );
        // A negative or zero float is not a valid throttling factor at all.
        assert_eq!(
            parse_adaptive_rate_control("-3"),
            Err(AdaptiveRateControlParseError::Unrecognized {
                value: "-3".to_string()
            })
        );
        assert!(matches!(
            parse_adaptive_rate_control("inf"),
            Err(AdaptiveRateControlParseError::Unrecognized { .. })
        ));
    }

    #[test]
    fn disabled_control_builds_no_controller() {
        assert!(AdaptiveController::new(AdaptiveRateControl::Disabled, 32.0).is_none());
    }

    // --- AIMD ---

    #[test]
    fn aimd_halves_on_failure_and_adds_one_on_success() {
        let controller = AdaptiveController::new(AdaptiveRateControl::Aimd, 32.0)
            .expect("aimd controller should build");
        assert!((controller.effective_limit() - 32.0).abs() < f64::EPSILON);

        controller.record(RequestOutcome::Failure);
        assert!((controller.effective_limit() - 16.0).abs() < f64::EPSILON);
        controller.record(RequestOutcome::Failure);
        assert!((controller.effective_limit() - 8.0).abs() < f64::EPSILON);

        controller.record(RequestOutcome::Success);
        assert!((controller.effective_limit() - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn aimd_clamps_to_floor_and_ceiling() {
        let controller = AdaptiveController::new(AdaptiveRateControl::Aimd, 4.0)
            .expect("aimd controller should build");

        // Ceiling: successes cannot push the limit past the configured max.
        for _ in 0..10 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.effective_limit() - 4.0).abs() < f64::EPSILON);

        // Floor: repeated failures never drive the limit below 1.
        for _ in 0..20 {
            controller.record(RequestOutcome::Failure);
        }
        assert!((controller.effective_limit() - 1.0).abs() < f64::EPSILON);
        assert!((controller.admission_coefficient() - 0.25).abs() < 1e-9);
    }

    // --- SRE ---

    #[test]
    fn sre_never_throttles_a_fully_healthy_origin() {
        let controller =
            AdaptiveController::new(AdaptiveRateControl::SreThrottle { k: 2.0 }, 100.0)
                .expect("sre controller should build");

        // accepts == requests at every finite count => coefficient exactly 1.
        for _ in 0..1 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
        for _ in 0..999 {
            controller.record(RequestOutcome::Success);
        }
        assert!((controller.admission_coefficient() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn sre_throttles_exactly_below_one_over_k() {
        let k = 4.0;
        let controller = AdaptiveController::new(AdaptiveRateControl::SreThrottle { k }, 100.0)
            .expect("sre controller should build");

        // Build a large window so the "+1" terms are negligible and the
        // threshold sits at accepts/requests == 1/k.
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
        let controller = AdaptiveController::new(AdaptiveRateControl::SreThrottle { k }, 100.0)
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
            AdaptiveController::new(AdaptiveRateControl::SreThrottle { k: 10.0 }, 100.0)
                .expect("sre controller should build");
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
