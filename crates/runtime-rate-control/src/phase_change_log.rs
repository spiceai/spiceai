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

//! Debounced reporting of phase changes.
//!
//! Despite the name, this holds no log: it decides *when* a change of phase is
//! worth reporting, and the caller does the logging. A value that a noisy signal
//! flips back and forth would otherwise write a line every flip.
//!
//! [`PhaseChangeLog`] tracks the phase it last reported. A different phase must
//! hold continuously for a caller-supplied duration before it is reported, and a
//! return to the reported phase discards the wait. So a signal that crosses a
//! boundary faster than that duration reports nothing at all.
//!
//! The wait is a per-observation argument, not a property of the tracker: a
//! caller that can tell a firm reading from a marginal one passes
//! [`Duration::ZERO`] for the firm reading and reports it at once. How to make
//! that judgement is the caller's business, not this module's.
//!
//! The phase type is anything `Copy + PartialEq`, and nothing here assumes two
//! phases.

use std::time::Duration;

use tokio::time::Instant;

/// Reports a phase change once the new phase has held long enough.
#[derive(Debug)]
pub(crate) struct PhaseChangeLog<S> {
    /// The phase the last report named.
    reported: S,
    /// A different phase, and when it was first seen continuously.
    candidate: Option<(S, Instant)>,
}

impl<S: Copy + PartialEq> PhaseChangeLog<S> {
    /// Start from `initial`, which is treated as already reported.
    pub(crate) fn new(initial: S) -> Self {
        Self {
            reported: initial,
            candidate: None,
        }
    }

    /// Feed the phase observed at `now` and return it once it has held
    /// continuously for `hold`.
    ///
    /// [`Duration::ZERO`] reports on first sighting. Any return to the reported
    /// phase discards the candidate, so a phase that does not survive `hold`
    /// reports nothing.
    pub(crate) fn observe(&mut self, observed: S, now: Instant, hold: Duration) -> Option<S> {
        if observed == self.reported {
            self.candidate = None;
            return None;
        }

        // An ongoing candidate keeps its original timestamp, or it would never
        // reach `hold`.
        let since = match self.candidate {
            Some((candidate, since)) if candidate == observed => since,
            _ => {
                self.candidate = Some((observed, now));
                now
            }
        };

        // Checked after the candidate is recorded, so a zero hold reports on the
        // first sighting rather than on the one after it.
        if now.saturating_duration_since(since) < hold {
            return None;
        }

        self.reported = observed;
        self.candidate = None;
        Some(observed)
    }

    /// The phase the last report named.
    // Only the tests need this today. Drop the gate when a caller wants it.
    #[cfg(test)]
    pub(crate) fn reported(&self) -> S {
        self.reported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A phase type of this module's own, so these tests cannot lean on anything
    /// the rate controller knows.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Phase {
        Calm,
        Rough,
    }

    /// More than two phases, because nothing in the algorithm is binary.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Weather {
        Clear,
        Cloudy,
        Storm,
    }

    /// A zero hold reports the moment the phase is first seen.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_zero_hold_reports_on_first_sighting() {
        let hold = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(Phase::Calm);
        let start = Instant::now();

        assert_eq!(
            phases.observe(Phase::Rough, start, Duration::ZERO),
            Some(Phase::Rough)
        );
        assert_eq!(phases.reported(), Phase::Rough);
        assert_eq!(
            phases.observe(Phase::Rough, start + hold, Duration::ZERO),
            None,
            "the same phase must not be reported twice"
        );
    }

    /// A phase that does not last the hold reports nothing, and the hold starts
    /// again after the phase returns to the reported one.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_phase_shorter_than_the_hold_reports_nothing() {
        let hold = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(Phase::Calm);
        let start = Instant::now();

        assert_eq!(phases.observe(Phase::Rough, start, hold), None);
        assert_eq!(
            phases.observe(Phase::Rough, start + hold / 2, hold),
            None,
            "half the hold is not enough to report the phase"
        );
        // Back to the reported phase before the hold elapsed: the candidate is
        // discarded.
        assert_eq!(phases.observe(Phase::Calm, start + hold, hold), None);
        assert_eq!(
            phases.observe(Phase::Rough, start + hold * 2, hold),
            None,
            "the hold restarts after the phase returned to the reported one"
        );
        assert_eq!(phases.reported(), Phase::Calm);
    }

    /// A phase held for the whole hold is reported exactly once, each way. The
    /// candidate keeps its original timestamp, so repeated sightings do not push
    /// the report further out.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_phase_held_for_the_hold_is_reported_once() {
        let hold = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(Phase::Calm);
        let start = Instant::now();

        assert_eq!(phases.observe(Phase::Rough, start, hold), None);
        assert_eq!(phases.observe(Phase::Rough, start + hold / 2, hold), None);
        assert_eq!(
            phases.observe(Phase::Rough, start + hold, hold),
            Some(Phase::Rough),
            "a phase held for the hold must be reported"
        );
        assert_eq!(
            phases.observe(Phase::Rough, start + hold * 3, hold),
            None,
            "the same phase must not be reported twice"
        );

        assert_eq!(phases.observe(Phase::Calm, start + hold * 4, hold), None);
        assert_eq!(
            phases.observe(Phase::Calm, start + hold * 5, hold),
            Some(Phase::Calm),
            "the return must be reported too"
        );
        assert_eq!(
            phases.observe(Phase::Calm, start + hold * 9, hold),
            None,
            "the return must not be reported twice"
        );
    }

    /// A phase oscillating faster than the hold reports nothing at all.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_phase_oscillating_faster_than_the_hold_reports_nothing() {
        let hold = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(Phase::Calm);
        let start = Instant::now();
        let step = hold / 4;

        for tick in 0..20 {
            let observed = if tick % 2 == 0 {
                Phase::Rough
            } else {
                Phase::Calm
            };
            assert_eq!(
                phases.observe(observed, start + step * tick, hold),
                None,
                "oscillation faster than the hold must report nothing (tick {tick})"
            );
        }
    }

    /// Nothing here is binary: with three phases, switching to a third phase
    /// mid-hold restarts the hold, and each phase is reported on its own.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn more_than_two_phases_are_tracked_one_at_a_time() {
        let hold = Duration::from_secs(10);
        let mut phases = PhaseChangeLog::new(Weather::Clear);
        let start = Instant::now();

        // Cloudy waits, then Storm replaces it as the candidate and starts its
        // own hold — Cloudy's elapsed time must not count towards Storm.
        assert_eq!(phases.observe(Weather::Cloudy, start, hold), None);
        assert_eq!(
            phases.observe(Weather::Storm, start + hold, hold),
            None,
            "a different phase must start its own hold"
        );
        assert_eq!(
            phases.observe(Weather::Storm, start + hold * 2, hold),
            Some(Weather::Storm),
            "the phase that actually held must be the one reported"
        );
        assert_eq!(phases.reported(), Weather::Storm);

        // A third phase is reported on its own terms, with no reference to the
        // phase the tracker started from.
        assert_eq!(
            phases.observe(Weather::Cloudy, start + hold * 3, hold),
            None
        );
        assert_eq!(
            phases.observe(Weather::Cloudy, start + hold * 4, hold),
            Some(Weather::Cloudy)
        );
        assert_eq!(
            phases.observe(Weather::Clear, start + hold * 5, Duration::ZERO),
            Some(Weather::Clear),
            "returning to the initial phase is an ordinary change"
        );
    }
}
