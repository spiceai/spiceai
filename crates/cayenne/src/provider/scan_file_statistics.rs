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

//! Coalescing and publication fencing for per-file scan statistics.
//!
//! A scan that collects statistics takes each file's from the provider's
//! in-memory cache, else from its persisted `cayenne_snapshot_file_statistics`
//! row, else from the Vortex footer — and a footer read then rewrites the row.
//! Scans of one table that miss together — a self-join or a CTE read twice in
//! one plan, or two concurrent queries — would each read every footer and
//! upsert every row. [`ScanFileStatisticsFlights`] runs one collection per file
//! identity and hands its answer to every caller that asked meanwhile.
//!
//! The statistics a footer read yields are adjusted for the file's position
//! deletes, so they describe the deletion state at the moment of the read. Every
//! change to that state clears the cache, and a collection that a clear overtook
//! must not publish into the emptied cache. The registry therefore carries a
//! generation that each clear advances, and publication checks it under the same
//! lock the clear takes, so the two cannot interleave.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{DateTime, Utc};
use datafusion_common::Statistics;
use datafusion_execution::cache::SchemaFingerprint;
use object_store::ObjectMeta;
use object_store::path::Path;
use parking_lot::Mutex;
use tokio::sync::OnceCell;

/// The cell every caller of one collection waits on. `get_or_try_init` hands
/// initialization to the next waiter when the running collection fails or is
/// cancelled, so a dropped query never strands the scans that joined it.
pub(crate) type ScanFileStatisticsCell = OnceCell<Arc<Statistics>>;

/// Identity of one file's statistics collection: two callers share a collection
/// only when they would compute the same answer.
///
/// The object metadata is the identity the in-memory cache validates against,
/// the snapshot is the persisted row's key, and the schema fingerprint is the
/// layout the statistics are computed in. The generation separates a collection
/// that started before a cache clear — and may have read deletions the clear
/// invalidated — from every collection that starts after it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ScanFileStatisticsKey {
    snapshot_id: String,
    location: Path,
    size: u64,
    last_modified: DateTime<Utc>,
    e_tag: Option<String>,
    version: Option<String>,
    schema: Arc<SchemaFingerprint>,
    generation: u64,
}

impl ScanFileStatisticsKey {
    pub(crate) fn new(
        snapshot_id: &str,
        object_meta: &ObjectMeta,
        schema: &Arc<SchemaFingerprint>,
        generation: u64,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.to_string(),
            location: object_meta.location.clone(),
            size: object_meta.size,
            last_modified: object_meta.last_modified,
            e_tag: object_meta.e_tag.clone(),
            version: object_meta.version.clone(),
            schema: Arc::clone(schema),
            generation,
        }
    }
}

/// Per-file statistics collection accounting for one table, so a check can
/// prove that concurrent cold scans shared their footer reads and metastore
/// writes rather than repeating them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanFileStatisticsCounters {
    /// Files whose statistics were inferred from their Vortex footer — a footer
    /// read, unless the runtime's footer cache already held that footer.
    pub footer_reads: u64,
    /// Files served from their persisted metastore row instead of the footer.
    pub persisted_hits: u64,
    /// Persisted rows written after a footer read.
    pub persisted_upserts: u64,
    /// Callers that waited on a collection already in flight for the same file
    /// instead of starting their own.
    pub joined_in_flight: u64,
}

#[derive(Default)]
struct Counters {
    footer_reads: AtomicU64,
    persisted_hits: AtomicU64,
    persisted_upserts: AtomicU64,
    joined_in_flight: AtomicU64,
}

/// The per-table registry of in-flight statistics collections, shared by every
/// `clone_for_write` copy of the provider the way the cache it fills is.
#[derive(Default)]
pub(crate) struct ScanFileStatisticsFlights {
    /// Advanced by every clear of the per-file statistics cache, under
    /// [`Self::publication`].
    generation: AtomicU64,
    /// Held across a clear (empty the cache, then advance the generation) and
    /// across a publication (check the generation, then fill the cache), so a
    /// collection overtaken by a clear can never land in the cache after it.
    publication: Mutex<()>,
    in_flight: Mutex<HashMap<ScanFileStatisticsKey, Arc<ScanFileStatisticsCell>>>,
    counters: Counters,
}

impl ScanFileStatisticsFlights {
    /// The generation a collection starting now belongs to.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Whether no cache clear has happened since `generation` was read.
    pub(crate) fn is_current(&self, generation: u64) -> bool {
        self.generation() == generation
    }

    /// Run `clear` (which empties the cache) and advance the generation, with no
    /// publication able to interleave.
    pub(crate) fn invalidate(&self, clear: impl FnOnce()) {
        let _publication = self.publication.lock();
        clear();
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Run `publish` (which fills the cache) only if no clear has happened since
    /// `generation` was read; returns whether it ran.
    pub(crate) fn publish_if_current(&self, generation: u64, publish: impl FnOnce()) -> bool {
        let _publication = self.publication.lock();
        if !self.is_current(generation) {
            return false;
        }
        publish();
        true
    }

    /// Join the collection in flight for `key`, or register a new one for this
    /// caller to run. The returned flight deregisters itself when dropped — on
    /// completion, failure or cancellation — so the registry only ever holds
    /// collections that a caller is still waiting on.
    pub(crate) fn join(&self, key: ScanFileStatisticsKey) -> ScanFileStatisticsFlight<'_> {
        let (cell, joined) = match self.in_flight.lock().entry(key.clone()) {
            Entry::Occupied(registered) => (Arc::clone(registered.get()), true),
            Entry::Vacant(slot) => (Arc::clone(slot.insert(Arc::default())), false),
        };
        if joined {
            self.counters
                .joined_in_flight
                .fetch_add(1, Ordering::Relaxed);
        }
        ScanFileStatisticsFlight {
            flights: self,
            key,
            cell,
        }
    }

    pub(crate) fn record_footer_read(&self) {
        self.counters.footer_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_persisted_hit(&self) {
        self.counters.persisted_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_persisted_upsert(&self) {
        self.counters
            .persisted_upserts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn counters(&self) -> ScanFileStatisticsCounters {
        ScanFileStatisticsCounters {
            footer_reads: self.counters.footer_reads.load(Ordering::Relaxed),
            persisted_hits: self.counters.persisted_hits.load(Ordering::Relaxed),
            persisted_upserts: self.counters.persisted_upserts.load(Ordering::Relaxed),
            joined_in_flight: self.counters.joined_in_flight.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.in_flight.lock().len()
    }
}

/// One caller's handle on a registered collection.
pub(crate) struct ScanFileStatisticsFlight<'a> {
    flights: &'a ScanFileStatisticsFlights,
    key: ScanFileStatisticsKey,
    cell: Arc<ScanFileStatisticsCell>,
}

impl ScanFileStatisticsFlight<'_> {
    pub(crate) fn cell(&self) -> &ScanFileStatisticsCell {
        &self.cell
    }
}

impl Drop for ScanFileStatisticsFlight<'_> {
    fn drop(&mut self) {
        // The first participant to leave removes the registration; any other
        // still holds the cell, and a newer registration under the same key is
        // left alone. A caller arriving after the removal starts a collection of
        // its own, which re-checks the cache the finished one filled.
        let mut in_flight = self.flights.in_flight.lock();
        if in_flight
            .get(&self.key)
            .is_some_and(|registered| Arc::ptr_eq(registered, &self.cell))
        {
            in_flight.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(generation: u64) -> ScanFileStatisticsKey {
        let meta = ObjectMeta {
            location: Path::from("table/snapshot/file_00000.vortex"),
            last_modified: DateTime::<Utc>::from_timestamp(1_700_000_000, 0)
                .expect("valid timestamp"),
            size: 4096,
            e_tag: None,
            version: None,
        };
        let schema = Arc::new(SchemaFingerprint::from_schema(
            &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                "id",
                arrow::datatypes::DataType::Int64,
                false,
            )]),
        ));
        ScanFileStatisticsKey::new("snapshot", &meta, &schema, generation)
    }

    /// Callers of one file share a registration, and the registry empties when
    /// the last of them leaves, whichever leaves first.
    #[test]
    fn concurrent_callers_share_one_registration_and_leave_none_behind() {
        let flights = ScanFileStatisticsFlights::default();
        let first = flights.join(key(0));
        let second = flights.join(key(0));
        assert!(Arc::ptr_eq(&first.cell, &second.cell));
        assert_eq!(flights.in_flight_len(), 1);
        assert_eq!(flights.counters().joined_in_flight, 1);

        drop(first);
        assert_eq!(flights.in_flight_len(), 0);
        // The remaining caller still holds the cell the first one registered.
        assert!(!second.cell().initialized());
        let third = flights.join(key(0));
        assert!(
            !Arc::ptr_eq(&second.cell, &third.cell),
            "a caller arriving after the registration was removed starts afresh"
        );
        drop(second);
        assert_eq!(
            flights.in_flight_len(),
            1,
            "leaving must not remove a newer registration under the same key"
        );
        drop(third);
        assert_eq!(flights.in_flight_len(), 0);
    }

    /// A collection from before a clear is a different identity from one after.
    #[test]
    fn a_clear_separates_collections_by_generation() {
        let flights = ScanFileStatisticsFlights::default();
        let before = flights.join(key(flights.generation()));
        flights.invalidate(|| {});
        let after = flights.join(key(flights.generation()));
        assert!(!Arc::ptr_eq(&before.cell, &after.cell));
        assert_eq!(flights.counters().joined_in_flight, 0);
    }

    /// A publication from before a clear does not run; one from after does.
    #[test]
    fn publication_is_fenced_by_the_generation_it_started_in() {
        let flights = ScanFileStatisticsFlights::default();
        let started = flights.generation();
        flights.invalidate(|| {});
        let mut published = false;
        assert!(!flights.publish_if_current(started, || published = true));
        assert!(!published, "a collection a clear overtook must not publish");
        assert!(flights.publish_if_current(flights.generation(), || published = true));
        assert!(published);
    }

    /// A failed or cancelled collection hands initialization to the next waiter
    /// instead of caching the failure or stranding it.
    #[tokio::test]
    async fn a_failed_collection_is_retried_by_the_next_waiter() {
        let flights = ScanFileStatisticsFlights::default();
        let first = flights.join(key(0));
        let second = flights.join(key(0));
        let failed = first
            .cell()
            .get_or_try_init(|| async { Err::<Arc<Statistics>, &str>("footer unreadable") })
            .await;
        assert_eq!(
            failed.expect_err("first collection fails"),
            "footer unreadable"
        );
        let statistics = Arc::new(Statistics::new_unknown(&arrow::datatypes::Schema::empty()));
        let retried = second
            .cell()
            .get_or_try_init(|| async { Ok::<_, &str>(Arc::clone(&statistics)) })
            .await
            .expect("second collection succeeds");
        assert!(Arc::ptr_eq(retried, &statistics));
    }

    /// A collection whose caller is cancelled mid-flight — its query was dropped
    /// — hands initialization to the next waiter instead of stranding it.
    #[tokio::test]
    async fn a_cancelled_collection_hands_over_to_the_next_waiter() {
        let flights = ScanFileStatisticsFlights::default();
        let first = flights.join(key(0));
        let second = flights.join(key(0));
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            first
                .cell()
                .get_or_try_init(std::future::pending::<Result<Arc<Statistics>, &str>>),
        )
        .await
        .expect_err("the first collection never finishes, so its caller gives up");
        drop(first);
        let statistics = Arc::new(Statistics::new_unknown(&arrow::datatypes::Schema::empty()));
        let handed_over = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            second
                .cell()
                .get_or_try_init(|| async { Ok::<_, &str>(Arc::clone(&statistics)) }),
        )
        .await
        .expect("the waiter is not stranded by the cancelled collection")
        .expect("the waiter's own collection succeeds");
        assert!(Arc::ptr_eq(handed_over, &statistics));
    }
}
