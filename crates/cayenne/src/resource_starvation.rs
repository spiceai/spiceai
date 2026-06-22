/*
Copyright 2026 The Spice.ai OSS Authors

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

use std::time::{Duration, Instant};

/// Tracks episodes of resource starvation.
///
/// A starvation episode begins when an operation first fails to acquire a
/// required resource and ends when progress is eventually made. During an
/// episode, consecutive denied attempts are counted and the episode's
/// wall-clock duration is measured.
///
/// To avoid log spam, warning escalation is duration-based rather than
/// attempt-count-based. The first denied attempt starts the episode timer;
/// once the episode age reaches warn_after, record_denial() returns
/// Some(denied_attempts) exactly once for that episode so the caller may
/// emit a WARN-level log. Subsequent denials within the same episode do not
/// re-trigger escalation.
///
/// # Example
///
/// A background task periodically attempts work that requires a resource
/// (for example, a writer lock). While the resource remains unavailable,
/// denied attempts extend a single starvation episode. Once the episode
/// persists longer than the configured threshold, a warning is emitted once.
/// Successful acquisition ends the episode.
///
/// ```ignore
/// let mut starvation = ResourceStarvationTracker::new(Duration::from_secs(30));
/// loop {
///     let now = Instant::now();
///     if try_acquire_resource() {
///         starvation.reset();
///         do_work();
///     } else {
///         if let Some(denials) = starvation.record_denial(now) {
///         warn!(denials, "resource starvation has persisted for at least 30s");
///     }
///     trace!("resource unavailable; skipping work");
/// }
/// ```
#[derive(Debug)]
pub struct ResourceStarvationTracker {
    warn_after: Duration,
    episode: Option<ResourceStarvationEpisode>,
}

/// One contiguous resource-starvation episode.
///
/// An episode consists of consecutive denied attempts caused by a resource
/// remaining unavailable. It begins with the first denial and ends when the
/// resource is eventually acquired and progress is made.
#[derive(Debug)]
struct ResourceStarvationEpisode {
    started_at: Instant,
    denied_attempts: usize,
    warn_emitted: bool,
}

impl ResourceStarvationTracker {
    pub fn new(warn_after: Duration) -> Self {
        Self {
            warn_after,
            episode: None,
        }
    }

    /// Record an attempt that could not proceed because a required resource
    /// was unavailable.
    ///
    /// Returns `Some(denied_attempts)` exactly once per starvation episode:
    /// when the episode's wall-clock age first reaches the warning threshold.
    pub fn record_denial(&mut self) -> Option<usize> {
        self.record_denial_at(Instant::now())
    }

    fn record_denial_at(&mut self, now: Instant) -> Option<usize> {
        match &mut self.episode {
            Some(ep) => ep.record_denial(now, self.warn_after),
            None => {
                self.episode = Some(ResourceStarvationEpisode::new(now));
                None
            }
        }
    }

    /// The resource became available and progress was made, ending the current
    /// starvation episode (if any).
    pub fn reset(&mut self) {
        self.episode = None;
    }

    pub fn episode(&self) -> Option<&ResourceStarvationEpisode> {
        self.episode.as_ref()
    }
}

impl ResourceStarvationEpisode {
    fn new(now: Instant) -> Self {
        Self {
            started_at: now,
            denied_attempts: 1,
            warn_emitted: false,
        }
    }

    fn record_denial(&mut self, now: Instant, warn_after: Duration) -> Option<usize> {
        self.denied_attempts = self.denied_attempts.saturating_add(1);

        if !self.warn_emitted && now.duration_since(self.started_at) >= warn_after {
            self.warn_emitted = true;
            Some(self.denied_attempts)
        } else {
            None
        }
    }

    fn denied_attempts(&self) -> usize {
        self.denied_attempts
    }

    fn age(&self, now: Instant) -> Duration {
        now.duration_since(self.started_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The starvation WARN is WALL-CLOCK-based and one-shot per episode: it
    /// fires on the first denial at/after `warn_after` of continuous
    /// starvation — regardless of how many denials accrued (the denial cadence
    /// follows the caller's dynamically tuned wake interval) — stays silent for
    /// the rest of that episode, and re-arms after a successful acquisition
    /// resets the episode.
    #[test]
    fn record_denial_warns_on_wall_clock_once_per_episode() {
        let warn_after = Duration::from_secs(30);
        let t0 = Instant::now();
        let sec = Duration::from_secs;

        // Slow cadence (60s interval): the SECOND denial is already past the
        // bound — warn at 2 denials, not at some fixed count.
        let mut tracker = ResourceStarvationTracker::new(warn_after);
        assert_eq!(tracker.record_denial_at(t0), None, "episode start");
        assert_eq!(
            tracker.record_denial_at(t0 + sec(60)),
            Some(2),
            "60s of starvation crosses the 30s bound"
        );
        assert_eq!(
            tracker.record_denial_at(t0 + sec(120)),
            None,
            "one-shot: no second WARN within the episode"
        );

        // Fast cadence (2s interval): many denials stay silent until the bound.
        let mut tracker = ResourceStarvationTracker::new(warn_after);
        for i in 0..15 {
            assert_eq!(
                tracker.record_denial_at(t0 + sec(2 * i)),
                None,
                "{}s elapsed is under the 30s bound",
                2 * i
            );
        }
        assert_eq!(
            tracker.record_denial_at(t0 + sec(30)),
            Some(16),
            "fires exactly when 30s of wall-clock starvation is reached"
        );

        // A successful acquisition ends the episode; the next one re-arms.
        tracker.reset();
        assert_eq!(tracker.record_denial_at(t0 + sec(100)), None);
        assert_eq!(
            tracker.record_denial_at(t0 + sec(140)),
            Some(2),
            "a fresh episode warns again after reset"
        );
    }
}
