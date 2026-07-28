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

//! A seqlock-style version gate for FORCED structural table events whose mutation
//! runs OFF the listing fence and could otherwise tear a straddling scan-view
//! capture. Today the sole wired writer is **live schema-evolution** (its all-shards
//! mem-tier flush runs off-fence — see `begin_mutation` at the widen site); the
//! primitive is deliberately general so other off-fence discontinuities can adopt it.
//!
//! Ordinary CDC churn (append / row-delete / upsert / checkpoint / compaction) does
//! NOT touch this, and neither do the FENCE-SERIALIZED snapshot events (truncate /
//! full-table delete / `INSERT OVERWRITE` / reopen): the listing fence already
//! serializes their capture, so they advance only the additive `scan_input_version`
//! and are served bounded-stale. Only an off-fence discontinuity that would make a
//! previously-computed scan view semantically WRONG (not merely stale) advances this.
//! This lets the demand-driven scan-view cache serve bounded-stale views freely for
//! everything else while GUARANTEEING that a scan capture straddling a schema-evolve
//! is discarded and retried rather than built into a pre-evolution bundle.
//!
//! Protocol (odd = mutation in flight, even = stable), a versioned seqlock:
//! - Forced-event writer: [`StructuralVersion::begin_mutation`] bumps the counter
//!   to ODD, and the returned [`StructuralMutationGuard`] bumps it to EVEN on drop.
//!   Forced events are serialized by the provider `write_lock`, so at most one
//!   guard is live at a time — the odd/even pair is a generation MARKER, not a
//!   mutual-exclusion mechanism.
//! - Demand capture (seqlock reader): [`StructuralVersion::read_validated_async`]
//!   (and the sync [`StructuralVersion::read_validated`] reference impl) captures an
//!   even `v0`, runs the capture, and returns the output stamped with `v0` ONLY if
//!   the counter is still `v0` afterwards — so a capture that raced a forced event
//!   is DISCARDED and retried rather than built into a torn or pre-event view.
//! - Key generation: [`StructuralVersion::current`] is folded into the demand cache's
//!   `ScanViewKey`, so a live schema-evolution mints a fresh identity (a read-current
//!   fast-path serve is gated on it); there is no wait/republish gate.
//!
//! The primitive lives here — not smeared across the mutation call sites — with
//! its own loom model, so the concurrency proof is local and audited once. See
//! `docs/cayenne/cayenne.md`.

#[cfg(cayenne_loom)]
use loom::sync::atomic::{AtomicU64, Ordering, fence};
#[cfg(not(cayenne_loom))]
use std::sync::atomic::{AtomicU64, Ordering, fence};

/// A monotonic seqlock version counter for forced structural table events. Even
/// values are stable; an odd value means a forced mutation is in flight. See the
/// module docs for the full protocol.
#[derive(Debug)]
pub(crate) struct StructuralVersion {
    version: AtomicU64,
}

impl StructuralVersion {
    /// A fresh counter at the stable, even baseline `0`.
    pub(crate) fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
        }
    }

    /// The current structural generation. The demand cache reads this at capture,
    /// folds it into the `ScanViewKey`, and (on the read-current fast path) compares
    /// it so a stale-tolerant serve is never a pre-evolution bundle.
    ///
    /// May observe an ODD (in-flight) value; the demand capture only keys on an even,
    /// validated generation (`read_validated_async` retries an odd/torn read), so an
    /// odd `v` observed here simply forces a rebuild rather than being trusted.
    /// `Acquire` so an observer of a forced bump also observes the writer's data swaps
    /// that preceded it (release/acquire with the guard's increments).
    pub(crate) fn current(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Begin a forced structural mutation: bump the counter to ODD and return a
    /// guard that bumps it to EVEN (advancing the generation) on drop. Hold the
    /// guard across the WHOLE forced mutation (swap listing / reset deletions /
    /// clear tier / widen schema) so the builder cannot publish a bundle built
    /// from a half-mutated table.
    pub(crate) fn begin_mutation(&self) -> StructuralMutationGuard<'_> {
        let prev = self.version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(
            prev & 1,
            0,
            "forced structural mutation began on an odd version (overlapping forced events?)"
        );
        StructuralMutationGuard {
            version: &self.version,
        }
    }

    /// Seqlock read: capture the even version, run `build`, and return
    /// `Some((version, output))` iff no forced mutation was in flight at the start
    /// AND none raced the build (the version is unchanged). `None` => the caller
    /// must re-capture and retry — never publish a raced build.
    ///
    /// Ordering: `v0` is `Acquire`, so `build`'s loads cannot be reordered before
    /// it; `build`'s own `ArcSwap` loads are `Acquire`, so the `v1` load cannot be
    /// reordered before them; hence `v0 <= build-loads <= v1` in observation order.
    /// If `build` observed any post-event data swap (a writer `Release` store made
    /// after its odd bump), release/acquire makes `v1` observe that bump (`>= odd`)
    /// and the equality check fails. The `Acquire` fence before `v1` covers any
    /// non-`ArcSwap` reads inside `build`. The loom model verifies this.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "sync reference impl exercised by the unit tests + loom model; production \
                      captures across an `.await` via `read_validated_async`, which mirrors this \
                      exact load ordering"
        )
    )]
    pub(crate) fn read_validated<T>(&self, build: impl FnOnce() -> T) -> Option<(u64, T)> {
        let v0 = self.version.load(Ordering::Acquire);
        if v0 & 1 != 0 {
            return None; // a forced mutation is in flight
        }
        let out = build();
        fence(Ordering::Acquire);
        let v1 = self.version.load(Ordering::Acquire);
        (v0 == v1).then_some((v0, out))
    }

    /// [`Self::read_validated`] for an ASYNC critical section: the scan-view builder's
    /// capture awaits `listing_fence`, so it cannot live inside a synchronous closure.
    /// This encapsulates the exact same seqlock protocol around an `.await` — so the
    /// caller cannot forget the after-check — with the identical load ordering the
    /// loom model of [`Self::read_validated`] verifies (`v0` Acquire before the
    /// captured `ArcSwap` loads, an Acquire fence + `v1` Acquire after them).
    ///
    /// Returns `Some((v0, output))` iff no forced mutation was in flight at the start
    /// AND none raced the awaited capture; `None` => discard and retry (never publish
    /// a torn or pre-event capture). Rejecting an odd `v0` up front is load-bearing:
    /// without it a capture that runs entirely within ONE forced mutation's odd
    /// window would see `v0 == v1` (both the same odd value) and wrongly validate a
    /// torn read.
    pub(crate) async fn read_validated_async<F, Fut, T>(&self, build: F) -> Option<(u64, T)>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let v0 = self.version.load(Ordering::Acquire);
        if v0 & 1 != 0 {
            return None; // a forced mutation is in flight
        }
        let out = build().await;
        fence(Ordering::Acquire);
        let v1 = self.version.load(Ordering::Acquire);
        (v0 == v1).then_some((v0, out))
    }
}

impl Default for StructuralVersion {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard that bumps its [`StructuralVersion`] to EVEN (stable, next
/// generation) on drop. Created by [`StructuralVersion::begin_mutation`], which
/// bumped it to ODD.
#[derive(Debug)]
pub(crate) struct StructuralMutationGuard<'a> {
    version: &'a AtomicU64,
}

impl Drop for StructuralMutationGuard<'_> {
    fn drop(&mut self) {
        let prev = self.version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(
            prev & 1,
            1,
            "structural mutation guard dropped on an even version"
        );
    }
}

#[cfg(all(test, not(cayenne_loom)))]
mod tests {
    use super::StructuralVersion;

    #[test]
    fn starts_even_and_publishes_stable_reads() {
        let sv = StructuralVersion::new();
        assert_eq!(sv.current(), 0);
        let (v, out) = sv.read_validated(|| 42).expect("stable read must publish");
        assert_eq!(v, 0);
        assert_eq!(out, 42);
    }

    #[test]
    fn mutation_guard_is_odd_in_flight_and_advances_by_two() {
        let sv = StructuralVersion::new();
        {
            let _g = sv.begin_mutation();
            assert_eq!(
                sv.current() & 1,
                1,
                "odd while a forced mutation is in flight"
            );
            // A build started during a forced mutation must NOT publish.
            assert!(
                sv.read_validated(|| 1).is_none(),
                "read_validated must refuse to publish during a forced mutation"
            );
        }
        assert_eq!(
            sv.current(),
            2,
            "even + advanced by two after the guard drops"
        );
        assert!(
            sv.read_validated(|| 1).is_some(),
            "stable again after the event"
        );
    }

    #[test]
    fn successive_forced_events_advance_monotonically() {
        let sv = StructuralVersion::new();
        drop(sv.begin_mutation());
        drop(sv.begin_mutation());
        assert_eq!(sv.current(), 4);
    }

    #[tokio::test]
    async fn read_validated_async_publishes_when_stable() {
        let sv = StructuralVersion::new();
        let published = sv.read_validated_async(|| async { 42 }).await;
        assert_eq!(published, Some((0, 42)), "a stable async read must publish");
    }

    #[tokio::test]
    async fn read_validated_async_discards_during_forced_mutation() {
        let sv = StructuralVersion::new();
        let _g = sv.begin_mutation(); // odd: forced mutation in flight
        assert!(
            sv.read_validated_async(|| async { 1 }).await.is_none(),
            "the async seqlock read must refuse to publish during a forced mutation"
        );
    }
}

// LOOM model. Run with:
//   RUSTFLAGS="--cfg cayenne_loom" cargo test -p cayenne --lib \
//     provider::structural_version --release
#[cfg(all(test, cayenne_loom))]
mod loom_tests {
    use super::{Ordering, StructuralVersion};
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicU64;

    /// The core seqlock invariant: whenever the builder PUBLISHES (`read_validated`
    /// -> Some), the data it captured belongs to the exact generation the version
    /// was stamped at — never a torn or pre-event view stamped as current. A forced
    /// writer swaps a "generation" atomic while holding the guard (odd); the builder
    /// captures it under the seqlock concurrently.
    #[test]
    fn publish_never_stamps_a_torn_generation() {
        loom::model(|| {
            let sv = Arc::new(StructuralVersion::new());
            // `data` mirrors the version: 0 while stable at version 0, 2 while
            // stable at version 2; written (Release) between the odd bump and the
            // guard drop.
            let data = Arc::new(AtomicU64::new(0));

            let writer = {
                let sv = Arc::clone(&sv);
                let data = Arc::clone(&data);
                loom::thread::spawn(move || {
                    let guard = sv.begin_mutation(); // 0 -> 1 (odd)
                    data.store(2, Ordering::Release);
                    drop(guard); // 1 -> 2 (even)
                })
            };

            let published = sv.read_validated(|| data.load(Ordering::Acquire));

            writer.join().expect("writer thread");

            if let Some((version, captured)) = published {
                assert_eq!(version & 1, 0, "must never publish an odd version");
                // data == version at every stable point (0 or 2) by construction, so
                // a published pair MUST match; a mismatch is a torn read stamped live.
                assert_eq!(
                    captured, version,
                    "published a torn generation (version != data)"
                );
            }
        });
    }

    /// A scan that observes a forced bump (`current()` advanced to the post-event
    /// even value) also observes the data the writer installed before that bump —
    /// the release/acquire pairing the scan gate relies on to never run post-event
    /// logic against pre-event data.
    #[test]
    fn observing_the_bump_implies_observing_the_data() {
        loom::model(|| {
            let sv = Arc::new(StructuralVersion::new());
            let data = Arc::new(AtomicU64::new(0));

            let writer = {
                let sv = Arc::clone(&sv);
                let data = Arc::clone(&data);
                loom::thread::spawn(move || {
                    let guard = sv.begin_mutation();
                    data.store(2, Ordering::Release);
                    drop(guard);
                })
            };

            let v = sv.current();
            if v >= 2 {
                assert_eq!(
                    data.load(Ordering::Acquire),
                    2,
                    "observed the post-event version but stale data"
                );
            }

            writer.join().expect("writer thread");
        });
    }
}
