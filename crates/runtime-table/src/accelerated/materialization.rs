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

//! Generation of the rows currently in an accelerator, and whether they are the
//! configured definition's result.
//!
//! Dequeue begins a new generation under the accelerator write mutex — the same
//! lock the snapshot path samples — so a snapshot already in
//! `create_checkpoint_and_snapshot` finishes against the previous identity
//! before a new refresh can retract. The epoch is the second lock: a refresh
//! scan stamps attestation after that mutex is released, and the publish gate
//! binds the epoch sampled with the rows. A later refresh generation's plan
//! cannot approve the previous one. Ordinary queries do not record attestation.
//!
//! `(epoch, configured)` is one packed `AtomicU64` so a sample cannot observe a
//! torn pair (`configured = true` for epoch `N` with the epoch word already at
//! `N+1`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const CONFIGURED_BIT: u64 = 1;

/// Shared identity of one accelerator's current materialization.
///
/// Cheap to clone: every clone of a [`crate::accelerated::refresh::Refresh`]
/// points at the same cell, so the runner can retract or re-assert provenance
/// where the rows are written and the snapshot path can sample it where they
/// are archived.
#[derive(Clone, Debug)]
pub struct MaterializationIdentity {
    /// Bits `1..63` are the epoch; bit `0` is `configured`.
    stamp: Arc<AtomicU64>,
}

/// One consistent `(epoch, configured)` sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaterializationSample {
    pub epoch: u64,
    pub configured: bool,
}

impl Default for MaterializationIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl MaterializationIdentity {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stamp: Arc::new(AtomicU64::new(0)),
        }
    }

    const fn unpack(stamp: u64) -> MaterializationSample {
        MaterializationSample {
            epoch: stamp >> 1,
            configured: stamp & CONFIGURED_BIT != 0,
        }
    }

    const fn pack(epoch: u64, configured: bool) -> u64 {
        (epoch << 1) | if configured { CONFIGURED_BIT } else { 0 }
    }

    /// Start a new materialization: increment the epoch and retract `configured`.
    ///
    /// Called when a refresh is dequeued, while holding the accelerator write
    /// mutex. Returns the epoch the new run will attest under.
    #[must_use]
    pub fn begin_refresh(&self) -> u64 {
        loop {
            let current = self.stamp.load(Ordering::Acquire);
            let epoch = Self::unpack(current).epoch.saturating_add(1);
            let new = Self::pack(epoch, false);
            if self
                .stamp
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return epoch;
            }
        }
    }

    /// Set only the configured bit; the epoch is unchanged.
    ///
    /// A successful refresh re-asserts provenance on the epoch it began. A
    /// runtime `refresh_sql` PATCH retracts the bit without starting a run.
    pub fn set_configured(&self, configured: bool) {
        loop {
            let current = self.stamp.load(Ordering::Acquire);
            let sample = Self::unpack(current);
            let new = Self::pack(sample.epoch, configured);
            if self
                .stamp
                .compare_exchange_weak(current, new, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// One atomic load of the `(epoch, configured)` pair.
    #[must_use]
    pub fn sample(&self) -> MaterializationSample {
        Self::unpack(self.stamp.load(Ordering::Acquire))
    }

    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.sample().epoch
    }

    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.sample().configured
    }
}

/// What an acceleration holds when a process registers it — the input to
/// [`starts_configured`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccelerationAtStart<'a> {
    /// No checkpoint from an earlier acceleration: no refresh has completed here, so it
    /// holds no rows the definition did not produce — except rows a first refresh wrote
    /// before a stop that came ahead of its first checkpoint (see [`starts_configured`]).
    Empty,
    /// Downloaded from a snapshot that bootstrap checked against this definition.
    Bootstrapped,
    /// Rows an earlier process left, with the definition fingerprint it persisted
    /// alongside them in the local checkpoint, if any.
    Existing {
        persisted_fingerprint: Option<&'a str>,
    },
}

/// Whether the rows an acceleration starts a process with are known to be its configured
/// definition's result — the provenance its first refresh inherits.
///
/// Every other state starts a generation unconfigured, and only a `full` refresh
/// establishes provenance from nothing. Without this seed a dataset whose refreshes add
/// to what is already there (`append`) would never publish a snapshot, and a view over
/// one would never bootstrap.
///
/// Rows an earlier process left are vouched for only by the fingerprint it persisted with
/// them: it persists the configured definition's fingerprint with rows that definition
/// produced, and retracts it when an override or a runtime `refresh_sql` change lands
/// rows the definition does not describe. A missing or different fingerprint therefore
/// leaves them unconfigured until a `full` refresh replaces them.
///
/// That retraction is written by the checkpoint that follows the overriding refresh, not
/// before its rows land, so a process that stops between the two leaves a stamp that
/// still names the configured definition over the override's rows. The same window opens
/// on a first refresh: its rows land before the first checkpoint exists, so a stop
/// between the two leaves rows a later start reads as [`AccelerationAtStart::Empty`]. The
/// pre-recreation snapshot trusts the same persisted stamp; recording provenance before
/// the write is #14616.
#[must_use]
pub fn starts_configured(
    at_start: AccelerationAtStart<'_>,
    configured_fingerprint: Option<&str>,
) -> bool {
    match at_start {
        AccelerationAtStart::Empty | AccelerationAtStart::Bootstrapped => true,
        AccelerationAtStart::Existing {
            persisted_fingerprint,
        } => match configured_fingerprint {
            // A source with no definition has nothing its rows could disagree with.
            None => true,
            Some(configured) => persisted_fingerprint == Some(configured),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AccelerationAtStart, MaterializationIdentity, MaterializationSample, starts_configured,
    };
    use std::thread;

    /// An `append` dataset only ever inherits provenance, so a fresh or verified start must
    /// be configured or it never publishes; rows an earlier process could not vouch for must not.
    #[test]
    fn provenance_at_start_follows_what_the_acceleration_holds() {
        let configured = Some("sha256:orders");

        assert!(
            starts_configured(AccelerationAtStart::Empty, configured),
            "an empty acceleration holds no rows the definition did not produce"
        );
        assert!(
            starts_configured(AccelerationAtStart::Bootstrapped, configured),
            "bootstrap already checked the snapshot against this definition"
        );
        assert!(
            starts_configured(
                AccelerationAtStart::Existing {
                    persisted_fingerprint: Some("sha256:orders"),
                },
                configured,
            ),
            "rows persisted with this definition's fingerprint are this definition's"
        );
        assert!(
            !starts_configured(
                AccelerationAtStart::Existing {
                    persisted_fingerprint: None,
                },
                configured,
            ),
            "a retracted fingerprint means an override landed rows the definition does not describe"
        );
        assert!(
            !starts_configured(
                AccelerationAtStart::Existing {
                    persisted_fingerprint: Some("sha256:orders-before-from-change"),
                },
                configured,
            ),
            "rows of a previous definition are not this one's"
        );
    }

    #[test]
    fn begin_refresh_retracts_configured_and_increments_epoch() {
        let identity = MaterializationIdentity::new();
        assert_eq!(
            identity.sample(),
            MaterializationSample {
                epoch: 0,
                configured: false,
            }
        );

        let first = identity.begin_refresh();
        identity.set_configured(true);
        assert_eq!(
            identity.sample(),
            MaterializationSample {
                epoch: first,
                configured: true,
            }
        );

        let second = identity.begin_refresh();
        assert_eq!(second, first + 1);
        assert_eq!(
            identity.sample(),
            MaterializationSample {
                epoch: second,
                configured: false,
            },
            "dequeue must not leave configured=true on the new epoch"
        );
    }

    #[test]
    fn set_configured_does_not_advance_the_epoch() {
        let identity = MaterializationIdentity::new();
        let epoch = identity.begin_refresh();
        identity.set_configured(true);
        identity.set_configured(false);
        identity.set_configured(true);
        assert_eq!(identity.epoch(), epoch);
        assert!(identity.is_configured());
    }

    /// Sequential model of Copilot `discussion_r4010927556`: a snapshot samples
    /// `(configured=true, epoch=N)` under the write mutex, then a dequeued
    /// refresh begins `N+1` and retracts. A later `sample()` must not report
    /// `configured=true` at `N+1` — that torn pair is what would let the
    /// publish gate keep the old "publishable" decision while adopting the
    /// new run's attestation.
    #[test]
    fn sample_after_begin_is_never_configured_on_the_new_epoch() {
        let identity = MaterializationIdentity::new();
        let _ = identity.begin_refresh();
        identity.set_configured(true);
        let sampled = identity.sample();
        assert!(sampled.configured);

        let next = identity.begin_refresh();
        let after = identity.sample();
        assert_eq!(after.epoch, next);
        assert!(!after.configured);
        assert_ne!(
            after,
            MaterializationSample {
                epoch: next,
                configured: true,
            }
        );
        assert_eq!(next, sampled.epoch + 1);
    }

    #[test]
    fn concurrent_begin_and_sample_never_tears_the_pair() {
        let identity = MaterializationIdentity::new();
        let _ = identity.begin_refresh();
        identity.set_configured(true);

        let writer = {
            let identity = identity.clone();
            thread::spawn(move || {
                for _ in 0..8_000 {
                    let _ = identity.begin_refresh();
                    identity.set_configured(true);
                }
            })
        };

        for _ in 0..40_000 {
            let sample = identity.sample();
            // Every stored stamp is written as a complete pair. A torn read
            // would be `configured=true` with an epoch the writer has already
            // begun (and therefore retracted). `begin_refresh` CAS-es
            // `(epoch+1, false)` in one store, so that combination cannot
            // appear as a single load.
            if sample.configured {
                let again = identity.sample();
                if again.epoch == sample.epoch {
                    assert!(
                        again.configured,
                        "configured at epoch {} vanished without the epoch moving: {again:?}",
                        sample.epoch
                    );
                }
            }
        }

        writer.join().expect("writer thread");
    }
}
