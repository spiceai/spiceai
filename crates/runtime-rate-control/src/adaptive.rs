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
//! A client, who's origin starts failing or timing out, can dynamically scale its
//! limits to avoid overloading the origin. An adaptive controller
//! watches the outcome of every request and produces an **admission
//! coefficient** in `[0, 1]`, the fraction of the configured rate to allow,
//! that the [`RateController`](crate::RateController) applies by weighting each
//! request against its fixed token buckets and concurrency permits. It lowers the
//! effective rate as the origin fails and raises it again as the origin recovers.
//!
//! * **failure threshold**: the upstream error rate above which throttling
//!   begins, as a fraction in `(0, 1)`. It maps to the coefficient
//!   `k = 1 / (1 - threshold)` (a 50% threshold is `k = 2`), the form the math
//!   below runs on.
//! * **window**: the reaction/recovery half-life (`> 0`). Request outcomes decay
//!   with this half-life, so a shorter window reacts and recovers faster.
//!
//! On each new request:
//! ```text
//!   count <- count · 0.5^(Δt / half_life)
//! ```
//!
//! And define the admission coefficient:
//! ```text
//!                   ┌                          ┐
//!  admission        │   K · accepts  +  1      │
//! coefficient = min │ ─────────────────── ,  1 │
//!                   │     requests   +  1      │
//!                   └                          ┘
//! ```
//!
//! Adaptive control is a *modifier* on the configured limits, never a limiter
//! of its own: the single coefficient scales every configured limit
//! (per-second, per-minute, and concurrency) uniformly. An origin with no
//! configured limit has nothing to modify, so adaptive control is a no-op there.

use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::phase_change_log::{Damping, PhaseChangeLog};

/// Documentation for the rate-control parameters, linked from the throttling log
/// line so an operator can act on it.
const RATE_CONTROL_DOCS_URL: &str =
    "https://spiceai.org/docs/components/data-connectors/https/deployment#rate-control";

/// The coefficient of a healthy origin: every configured limit applies in full.
/// The coefficient is exactly this for any error rate at or below the failure
/// threshold, and strictly below it above the threshold, so it is the whole
/// throttling test.
const FULL_ADMISSION_COEFFICIENT: f64 = 1.0;

/// Default failure threshold.
pub const DEFAULT_ADAPTIVE_FAILURE_THRESHOLD: f64 = 0.1;

/// Default reaction/recovery window: the decay half-life over which a failure
/// burst ages out of the window.
pub const DEFAULT_ADAPTIVE_WINDOW: Duration = Duration::from_secs(10);

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

/// The two validated adaptive hyperparameters, resolved from user config.
///
/// A small `Copy` value the rate-control config stores and compares; the live
/// [`AdaptiveController`] is built from it via [`AdaptiveController::new`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptiveRateControl {
    /// The error rate above which throttling begins, as a fraction in `(0, 1)`.
    /// Kept alongside `k` so user-facing messages can quote the configured value.
    failure_threshold: f64,
    /// Derived from the failure threshold (`k = 1 / (1 - threshold)`).
    k: f64,
    /// Decaying-window half-life.
    window: Duration,
    /// Whether `window` came from [`DEFAULT_ADAPTIVE_WINDOW`] rather than from
    /// an explicit `rate_control_window`.
    ///
    /// Cluster rate control defaults the half-life to the shared
    /// `refresh_interval` instead, because one window is the smallest unit of
    /// time its shared state records; only a value the user did not set may be
    /// retargeted that way.
    window_is_default: bool,
}

impl AdaptiveRateControl {
    /// Build from the user-facing hyperparameters, validating both and converting
    /// the failure threshold to the coefficient `k`.
    ///
    /// `failure_threshold` is the error rate above which throttling begins, as a
    /// fraction in `(0, 1)` (e.g. `0.5` = throttle above a 50% error rate).
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
            failure_threshold,
            k: 1.0 / (1.0 - failure_threshold),
            window,
            window_is_default: false,
        })
    }

    /// Build with the half-life left unset, so single-node and cluster rate
    /// control each apply their own default: [`DEFAULT_ADAPTIVE_WINDOW`] for a
    /// single node, and the shared `refresh_interval` for a cluster.
    ///
    /// # Errors
    /// Returns [`AdaptiveRateControlError::FailureThresholdInvalid`] when
    /// `failure_threshold` is not a finite fraction strictly between 0 and 1.
    pub fn with_default_window(failure_threshold: f64) -> Result<Self, AdaptiveRateControlError> {
        Self::new(failure_threshold, DEFAULT_ADAPTIVE_WINDOW).map(|control| Self {
            window_is_default: true,
            ..control
        })
    }

    /// The configured error rate above which throttling begins, as a fraction.
    #[must_use]
    pub fn failure_threshold(&self) -> f64 {
        self.failure_threshold
    }

    /// The decay half-life, with the single-node default applied.
    #[must_use]
    pub fn window(&self) -> Duration {
        self.window
    }

    /// The half-life the user configured, or `None` when they left it unset and
    /// single-node or cluster rate control applies its own default.
    #[must_use]
    pub fn configured_window(&self) -> Option<Duration> {
        (!self.window_is_default).then_some(self.window)
    }

    /// The factor the weighted accept count is multiplied by,
    /// `1 / (1 - failure_threshold)`.
    #[must_use]
    pub fn k(&self) -> f64 {
        self.k
    }
}

impl Default for AdaptiveRateControl {
    /// [`DEFAULT_ADAPTIVE_FAILURE_THRESHOLD`] with the window left unset, so
    /// cluster rate control may retarget it to its `refresh_interval`.
    fn default() -> Self {
        Self {
            failure_threshold: DEFAULT_ADAPTIVE_FAILURE_THRESHOLD,
            k: 1.0 / (1.0 - DEFAULT_ADAPTIVE_FAILURE_THRESHOLD),
            window: DEFAULT_ADAPTIVE_WINDOW,
            window_is_default: true,
        }
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
/// Cheap to share behind an `Arc`; window updates sit behind a short non-async
/// critical section. It computes the admission coefficient on demand (decayed to
/// the moment it is read), so a coefficient read after a quiet period reflects
/// recovery even with no new outcomes recorded.
#[derive(Debug)]
pub struct AdaptiveController {
    /// Derived from the failure threshold (`k = 1 / (1 - threshold)`).
    k: f64,
    /// The configured failure threshold, quoted in the throttling log line.
    failure_threshold: f64,
    /// Decaying-window half-life.
    half_life: Duration,
    /// The origin this controller governs, named in the log lines.
    origin: String,

    /// The decaying window and the log state it drives, under one lock so the
    /// coefficient and the transition decision cannot disagree.
    state: Mutex<ControllerState>,
}

/// What the origin is doing, as the log reports it.
///
/// Shared with the leased (cluster) bucket, which reaches the same two states
/// through the shared file rather than a local decaying window, so both modes
/// report a change of state in the same words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThrottleState {
    /// The error rate is at or below the failure threshold: the origin gets the
    /// full configured limits.
    Healthy,
    /// The error rate is above the failure threshold: the configured limits are
    /// scaled down.
    Throttling,
}

impl ThrottleState {
    /// The state an admission coefficient shows. The coefficient is exactly
    /// [`FULL_ADMISSION_COEFFICIENT`] at or below the failure threshold, and
    /// strictly below it above the threshold.
    fn of(coefficient: f64) -> Self {
        if coefficient >= FULL_ADMISSION_COEFFICIENT {
            Self::Healthy
        } else {
            Self::Throttling
        }
    }

    /// Report this state for `origin`.
    ///
    /// The change of state is logged, never the value: a rate is true only at
    /// the moment it is printed, so the live numbers belong in the metrics.
    pub(crate) fn report(self, origin: &str, failure_threshold: f64) {
        match self {
            Self::Throttling => tracing::warn!(
                "Upstream '{origin}' is failing more than the {} `rate_control_failure_threshold`, so adaptive rate control is reducing requests to it below the configured limits until it recovers. See: {RATE_CONTROL_DOCS_URL}",
                format_percentage(failure_threshold),
            ),
            Self::Healthy => tracing::info!(
                "Upstream '{origin}' has recovered, so adaptive rate control is sending it the full configured limits again."
            ),
        }
    }
}

/// Whether one more recorded outcome could flip the observed state back.
///
/// A flap is one outcome crossing the boundary, so this is exactly the condition
/// the log has to damp: a reading a single outcome away from the boundary waits,
/// and any firmer reading is written at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Confidence {
    /// No single outcome can flip the state back.
    Unambiguous,
    /// One outcome could flip the state back.
    NearBoundary,
}

#[derive(Debug)]
struct ControllerState {
    window: DecayWindow,
    /// Damps the throttling log in time: a state that one outcome could flip
    /// back must hold for one window before it is logged, and a firmer reading
    /// is logged as soon as it is seen.
    ///
    /// A fixed coefficient band would not work, because the coefficient is
    /// exactly 1 for every error rate up to the failure threshold and falls away
    /// immediately above it — any band wide enough to damp noise would also hide
    /// real throttling. How near the boundary a reading sits depends on how much
    /// traffic the window holds, which [`Confidence`] measures directly.
    phases: PhaseChangeLog<ThrottleState>,
}

impl AdaptiveController {
    /// Build a live controller from a validated [`AdaptiveRateControl`] for
    /// `origin`, which the throttling log lines name.
    #[must_use]
    pub fn new(control: AdaptiveRateControl, origin: impl Into<String>) -> Self {
        Self {
            k: control.k,
            failure_threshold: control.failure_threshold,
            half_life: control.window(),
            origin: origin.into(),
            state: Mutex::new(ControllerState {
                window: DecayWindow {
                    requests: 0.0,
                    accepts: 0.0,
                    last_update: None,
                },
                phases: PhaseChangeLog::new(ThrottleState::Healthy, control.window()),
            }),
        }
    }

    /// Record the outcome of one request.
    pub fn record(&self, outcome: RequestOutcome) {
        if let Some(state) = self.record_and_evaluate(outcome) {
            state.report(&self.origin, self.failure_threshold);
        }
    }

    /// Record one outcome and report the throttling state to log, if any.
    ///
    /// Evaluation happens here, per recorded outcome, so a throttled origin whose
    /// traffic stops reports no recovery until later requests arrive. That is
    /// accepted: with no traffic there is nothing to throttle, and the live
    /// coefficient is still in the metrics.
    ///
    /// A reading one outcome away from the boundary must hold for one window
    /// before it is logged, so a marginal episode is reported one window after it
    /// begins. A firmer reading is logged at once.
    fn record_and_evaluate(&self, outcome: RequestOutcome) -> Option<ThrottleState> {
        let now = Instant::now();
        let mut state = self.state.lock();
        state.window.decay_to(now, self.half_life);
        state.window.requests += 1.0;
        if outcome == RequestOutcome::Success {
            state.window.accepts += 1.0;
        }

        let (requests, accepts) = (state.window.requests, state.window.accepts);
        let observed = ThrottleState::of(self.coefficient_of(requests, accepts));
        // A reading one outcome from the boundary has to hold for a window; a
        // firmer one is reported the moment it is seen.
        let damping = match self.confidence_of(observed, requests, accepts) {
            Confidence::Unambiguous => Damping::Immediate,
            Confidence::NearBoundary => Damping::AfterHold,
        };
        state.phases.observe(observed, now, damping)
    }

    /// Whether one more recorded outcome could flip `observed` back.
    ///
    /// Decided on `k · accepts` against `requests`, never on the admission
    /// coefficient. The coefficient is clamped to 1, so an origin with a large
    /// success margin and one that is barely healthy both read exactly 1.0: the
    /// clamped value cannot tell a firm reading from a knife-edge one, and every
    /// recovery would look marginal.
    ///
    /// In coefficient terms the near-boundary band is `(k - 1) / (requests + 1)`
    /// wide, so it widens as traffic falls — which is where one outcome carries
    /// the most weight.
    fn confidence_of(&self, observed: ThrottleState, requests: f64, accepts: f64) -> Confidence {
        let weighted_accepts = self.k * accepts;
        let one_outcome_flips_it = match observed {
            // One success adds a request and `k` weighted accepts, so health
            // returns when `k · accepts >= requests + 1 - k`.
            ThrottleState::Throttling => weighted_accepts >= requests + 1.0 - self.k,
            // One failure adds a request, so health survives only while
            // `k · accepts >= requests + 1`.
            ThrottleState::Healthy => weighted_accepts < requests + 1.0,
        };

        if one_outcome_flips_it {
            Confidence::NearBoundary
        } else {
            Confidence::Unambiguous
        }
    }

    /// The fraction of requests the controller currently wants to admit, in
    /// `[0, 1]`. `1.0` means "admit everything" (a healthy origin).
    #[must_use]
    pub fn admission_coefficient(&self) -> f64 {
        self.admission_coefficient_at(Instant::now())
    }

    fn admission_coefficient_at(&self, now: Instant) -> f64 {
        let (requests, accepts) = {
            let mut state = self.state.lock();
            state.window.decay_to(now, self.half_life);
            (state.window.requests, state.window.accepts)
        };

        self.coefficient_of(requests, accepts)
    }

    /// The admission coefficient for a decayed window.
    ///
    /// Both `+1`s live inside the fraction: numerator `k*accepts + 1`,
    /// denominator `requests + 1`. At 100% success (accepts == requests) the
    /// ratio is `(k*r + 1) / (r + 1) >= 1` for `k > 1` and any finite `r`, so a
    /// healthy origin is never throttled. The coefficient falls below 1 exactly
    /// when `accepts/requests < 1/k`, i.e. the error rate exceeds the configured
    /// failure threshold.
    fn coefficient_of(&self, requests: f64, accepts: f64) -> f64 {
        ((self.k * accepts + 1.0) / (requests + 1.0)).clamp(0.0, 1.0)
    }

    /// The real-valued weight one request should charge right now: `1 /
    /// coefficient` (`1.0` when the origin is healthy; `+inf` at coefficient 0).
    ///
    /// Charging `weight` cells against a fixed bucket scales the effective rate by
    /// the admission coefficient without mutating the bucket. This is the *desired*
    /// weight; each caller clamps it to the individual limiter's capacity and
    /// rounds to whole cells, so one small limit never bounds how deeply a larger
    /// one throttles, and reaching a limiter's capacity is its deepest throttle,
    /// roughly one request per window.
    #[must_use]
    pub fn acquire_weight(&self) -> f64 {
        weight_of(self.admission_coefficient())
    }

    /// The weight [`Self::acquire_weight`] will read `after` from now if no
    /// further outcome is recorded. It never rises: the window decays toward
    /// empty, which raises the coefficient toward 1.
    #[must_use]
    pub fn weight_after(&self, after: Duration) -> f64 {
        let (requests, accepts) = self.decayed_window();
        let remaining = 0.5_f64.powf(after.as_secs_f64() / self.half_life.as_secs_f64());
        weight_of(self.coefficient_of(requests * remaining, accepts * remaining))
    }

    /// How long until, with no further outcomes, [`Self::acquire_weight`]
    /// falls to `weight`: zero if it already has, and `None` if decay alone
    /// never takes it there. The window only decays toward empty, which raises
    /// the coefficient toward 1 without reaching it, so a weight of 1 or less
    /// is never reached while the origin is throttled.
    #[must_use]
    pub fn decays_to_weight_in(&self, weight: f64) -> Option<Duration> {
        let (requests, accepts) = self.decayed_window();
        if weight_of(self.coefficient_of(requests, accepts)) <= weight {
            return Some(Duration::ZERO);
        }

        let target = 1.0 / weight;
        if target.is_nan() || target >= FULL_ADMISSION_COEFFICIENT {
            return None;
        }
        // After `n` half-lives the window holds `f = 0.5^n` of what it holds
        // now, and `(k·a·f + 1) / (r·f + 1) >= target` exactly when
        // `f <= (1 - target) / (target·r - k·a)`. The coefficient is below the
        // target now, so that denominator is positive.
        let denominator = target * requests - self.k * accepts;
        if denominator <= 0.0 {
            return Some(Duration::ZERO);
        }
        let fraction = (1.0 - target) / denominator;
        if fraction >= 1.0 {
            return Some(Duration::ZERO);
        }
        Duration::try_from_secs_f64(self.half_life.as_secs_f64() * -fraction.log2()).ok()
    }

    /// The window's request and accept counts, decayed to now.
    fn decayed_window(&self) -> (f64, f64) {
        let mut state = self.state.lock();
        state.window.decay_to(Instant::now(), self.half_life);
        (state.window.requests, state.window.accepts)
    }
}

/// The weight one request charges at `coefficient`: `1 / coefficient`, and
/// `1.0` for a healthy origin (`+inf` at coefficient 0).
fn weight_of(coefficient: f64) -> f64 {
    if coefficient >= FULL_ADMISSION_COEFFICIENT {
        1.0
    } else {
        1.0 / coefficient
    }
}

/// Format a fraction as a percentage for a user-facing message: `0.1` reads as
/// `10%`, `0.125` as `12.5%`.
fn format_percentage(fraction: f64) -> String {
    let percentage = fraction * 100.0;
    let text = format!("{percentage:.2}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    format!("{text}%")
}

#[derive(Debug)]
struct DecayWindow {
    requests: f64,
    accepts: f64,
    last_update: Option<Instant>,
}

impl DecayWindow {
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

    fn control(failure_threshold: f64) -> AdaptiveRateControl {
        AdaptiveRateControl::new(failure_threshold, DEFAULT_ADAPTIVE_WINDOW)
            .expect("test control should be valid")
    }

    fn enabled(failure_threshold: f64) -> AdaptiveController {
        AdaptiveController::new(control(failure_threshold), "https://origin.example.com")
    }

    /// The default must be a value `new` accepts, built from the documented
    /// defaults.
    #[test]
    fn default_matches_the_documented_defaults() {
        assert_eq!(
            AdaptiveRateControl::default(),
            AdaptiveRateControl::with_default_window(DEFAULT_ADAPTIVE_FAILURE_THRESHOLD)
                .expect("the defaults must be valid")
        );
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
    /// reduction. (Deterministic, it exercises the control law, not governor
    /// throughput; the end-to-end test covers the wired request path.)
    #[test]
    fn admission_coefficient_tracks_backend_error_rate() {
        // Settle the coefficient at a 10% failure threshold for a backend that
        // fails every `fail_every`-th request (0 = never fail). A large sample
        // makes the `+1` smoothing negligible; the tight loop makes window decay
        // over the elapsed microseconds immaterial.
        fn settled(fail_every: u32) -> f64 {
            let controller = AdaptiveController::new(
                AdaptiveRateControl::new(0.10, DEFAULT_ADAPTIVE_WINDOW)
                    .expect("a 10% failure threshold is valid"),
                "https://origin.example.com",
            );
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
        assert!(matches!(
            AdaptiveRateControl::new(0.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid {
                failure_threshold: 0.0
            })
        ));
        assert!(matches!(
            AdaptiveRateControl::new(1.0, DEFAULT_ADAPTIVE_WINDOW),
            Err(AdaptiveRateControlError::FailureThresholdInvalid {
                failure_threshold: 1.0
            })
        ));
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
        // A valid threshold with an invalid window must report the window.
        assert!(matches!(
            AdaptiveRateControl::new(0.5, Duration::ZERO),
            Err(AdaptiveRateControlError::WindowInvalid {
                window: Duration::ZERO
            })
        ));
        assert!(matches!(
            AdaptiveRateControl::new(0.5, Duration::MAX),
            Err(AdaptiveRateControlError::WindowInvalid {
                window: Duration::MAX
            })
        ));
    }

    #[test]
    fn never_throttles_a_fully_healthy_origin() {
        let controller = enabled(0.5);
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
        let controller = enabled(0.75);
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
        let controller = enabled(0.75);
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

    /// The number of accepts that puts a window of `requests` at exactly
    /// `coefficient`, for reading the near-boundary band in coefficient terms.
    fn accepts_for_coefficient(
        controller: &AdaptiveController,
        requests: f64,
        coefficient: f64,
    ) -> f64 {
        (coefficient * (requests + 1.0) - 1.0) / controller.k
    }

    /// (a) A throttle deep enough that no single success could restore health is
    /// reported at once, with no dwell.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_deep_throttle_is_reported_at_once() {
        // 10% failure threshold => k = 1.1111. One failure alone already sits
        // below `requests + 1 - k` (0 < 0.8889), so one success cannot undo it.
        let controller = enabled(0.1);

        assert_eq!(
            controller.record_and_evaluate(RequestOutcome::Failure),
            Some(ThrottleState::Throttling),
            "an unambiguous throttle must be reported without waiting a window"
        );
        assert_eq!(
            controller.record_and_evaluate(RequestOutcome::Failure),
            None,
            "the same state must not be reported twice"
        );
    }

    /// (d) A recovery with headroom is reported at once. This is the test that
    /// fails if the decision is routed through the clamped coefficient: the
    /// clamp hides the headroom, so every recovery would look marginal and wait.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_recovery_with_headroom_is_reported_at_once() {
        // 10% failure threshold. A failure burst, then the origin recovers.
        let controller = enabled(0.1);
        for _ in 0..200 {
            controller.record_and_evaluate(RequestOutcome::Failure);
        }

        // Let the burst age out, then drive successes with the clock stopped, so
        // any report can only come from confidence, never from a dwell elapsing.
        tokio::time::advance(DEFAULT_ADAPTIVE_WINDOW * 6).await;
        let reported: Vec<ThrottleState> = (0..200)
            .filter_map(|_| controller.record_and_evaluate(RequestOutcome::Success))
            .collect();

        assert_eq!(
            reported,
            vec![ThrottleState::Healthy],
            "recovery must be reported exactly once, and without the clock moving"
        );
    }

    /// The confidence test matches `k · accepts` against `requests`, so the
    /// near-boundary band is the documented `(k - 1) / (requests + 1)` wide.
    #[test]
    fn confidence_marks_only_readings_one_outcome_from_the_boundary() {
        // 10% failure threshold => k = 1.1111. At requests = 10 the boundary is
        // `k · accepts = requests + 1 - k` = 9.8889, i.e. accepts = 8.9.
        let controller = enabled(0.1);

        let firm = controller.confidence_of(ThrottleState::Throttling, 10.0, 8.8);
        let marginal = controller.confidence_of(ThrottleState::Throttling, 10.0, 8.95);
        assert_eq!(firm, Confidence::Unambiguous);
        assert_eq!(marginal, Confidence::NearBoundary);

        // The documented band edge at requests = 10 is coefficient 0.98990.
        assert!(controller.coefficient_of(10.0, 8.8) < 0.989_90);
        assert!(controller.coefficient_of(10.0, 8.95) > 0.989_90);
    }

    /// The band must widen as traffic falls, where one outcome carries the most
    /// weight. The same coefficient is a knife edge on a small window and firm
    /// on a large one.
    #[test]
    fn the_near_boundary_band_widens_as_traffic_falls() {
        let controller = enabled(0.1);
        let coefficient = 0.995;

        let quiet = accepts_for_coefficient(&controller, 10.0, coefficient);
        let busy = accepts_for_coefficient(&controller, 100.0, coefficient);

        assert_eq!(
            controller.confidence_of(ThrottleState::Throttling, 10.0, quiet),
            Confidence::NearBoundary,
            "on little traffic, coefficient 0.995 is one outcome from the boundary"
        );
        assert_eq!(
            controller.confidence_of(ThrottleState::Throttling, 100.0, busy),
            Confidence::Unambiguous,
            "on more traffic, the same coefficient is a firm reading"
        );
    }

    /// The clamp must stay out of the confidence decision: two healthy windows
    /// with the same clamped coefficient of exactly 1.0, one with headroom and
    /// one on the edge, must read differently.
    #[test]
    fn recovery_headroom_is_read_before_the_coefficient_is_clamped() {
        // 50% failure threshold => k = 2.
        let controller = enabled(0.5);

        // Raw ratio 1.909 and raw ratio 1.0. Both clamp to exactly 1.0.
        assert!((controller.coefficient_of(10.0, 10.0) - 1.0).abs() < f64::EPSILON);
        assert!((controller.coefficient_of(10.0, 5.0) - 1.0).abs() < f64::EPSILON);

        assert_eq!(
            controller.confidence_of(ThrottleState::Healthy, 10.0, 10.0),
            Confidence::Unambiguous,
            "a recovery with headroom must not look marginal"
        );
        assert_eq!(
            controller.confidence_of(ThrottleState::Healthy, 10.0, 5.0),
            Confidence::NearBoundary,
            "a recovery one failure from the boundary must wait"
        );
    }

    /// (e) Known limitation: the state is evaluated per recorded outcome, so a
    /// throttled origin whose traffic stops reports no recovery until enough
    /// later requests arrive.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_quiet_origin_reports_recovery_only_when_traffic_returns() {
        let window = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(ThrottleState::Healthy, window);
        let start = Instant::now();

        assert_eq!(
            phases.observe(ThrottleState::Throttling, start, Damping::AfterHold),
            None
        );
        assert_eq!(
            phases.observe(
                ThrottleState::Throttling,
                start + window,
                Damping::AfterHold
            ),
            Some(ThrottleState::Throttling)
        );

        // Traffic stops for an hour. Nothing is evaluated, so nothing is
        // reported, however long the origin has been healthy.
        let quiet_for = Duration::from_hours(1);
        assert_eq!(
            phases.observe(
                ThrottleState::Healthy,
                start + quiet_for,
                Damping::AfterHold
            ),
            None,
            "the first request back only starts the hold"
        );
        assert_eq!(
            phases.observe(
                ThrottleState::Healthy,
                start + quiet_for + window,
                Damping::AfterHold
            ),
            Some(ThrottleState::Healthy),
            "a marginal recovery is reported one window after traffic returns"
        );
    }

    /// `decays_to_weight_in` inverts the decay exactly: just before the time it
    /// returns the weight is still above the target, and at that time it has
    /// fallen to it.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn decay_reaches_a_weight_at_the_time_it_predicts() {
        // 10% failure threshold, 5s half-life, 100 failures: weight 101.
        let controller = AdaptiveController::new(
            AdaptiveRateControl::new(0.1, Duration::from_secs(5)).expect("valid control"),
            "https://origin.example.com",
        );
        for _ in 0..100 {
            controller.record(RequestOutcome::Failure);
        }
        assert!((controller.acquire_weight() - 101.0).abs() < 1e-9);

        // Weight 3 needs coefficient 1/3: the window must hold
        // (1 - 1/3) / (100 / 3) = 1/50 of its failures, 5s * log2(50) on.
        let wait = controller
            .decays_to_weight_in(3.0)
            .expect("decay alone reaches weight 3");
        assert!(
            (wait.as_secs_f64() - 5.0 * 50_f64.log2()).abs() < 1e-6,
            "predicted {wait:?}"
        );

        tokio::time::advance(wait.saturating_sub(Duration::from_millis(1))).await;
        assert!(controller.acquire_weight() > 3.0);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(controller.acquire_weight() <= 3.0 + 1e-9);
        assert_eq!(controller.decays_to_weight_in(3.0), Some(Duration::ZERO));

        // Decay raises the coefficient toward 1 but never to it.
        assert_eq!(controller.decays_to_weight_in(1.0), None);
    }

    #[test]
    fn percentages_read_as_the_configured_value() {
        assert_eq!(format_percentage(0.1), "10%");
        assert_eq!(format_percentage(0.25), "25%");
        assert_eq!(format_percentage(0.125), "12.5%");
    }

    #[test]
    fn coefficient_is_clamped_to_unit_interval() {
        let controller = enabled(0.9);
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
        let controller = enabled(0.75);

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
