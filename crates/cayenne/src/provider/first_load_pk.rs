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

//! The primary-key existence filter a first load builds while it writes.
//!
//! A load into an empty table holds the table's write lock from the emptiness
//! check through its publish, so the keys it writes are every key the table
//! then holds: the copies of a repeated key that the post-write resolution
//! drops share their key with the copy it keeps. A filter of every written key
//! therefore has no false negatives, and stands in for the one the next append
//! would otherwise rebuild by scanning every key of the table under the write
//! lock.
//!
//! The filter replaces only a rebuild that would itself end in a filter: one
//! whose exact keyset could not fit the table's keyset budget. Until a load has
//! written that many keys it keeps only their hashes, so a load that ends
//! smaller costs eight bytes per row rather than a filter of the whole budget.
//! The filter's bytes count against the process-wide keyset budget from its
//! allocation until the table publishes them as its index's, so tables loading
//! at the same time cannot each take the same headroom.
//!
//! Each writer hashes the keys it writes and queues the hashes for a thread of
//! the load's own, which records them, so no writer waits while the filter is
//! filled. A load that gets [`QUEUED_HASHES`] keys ahead of that thread, or
//! whose filter the process-wide budget has no room for, records no more and
//! installs no filter; the index is then built after the load, as it is for any
//! table.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow::array::{ArrayRef, RecordBatch};
use parking_lot::Mutex;
use vortex_datafusion::VortexWriteObserver;

use super::pk_index::{PkIndexBloom, pk_digest_bytes, pk_index_bloom_hash, pk_keyset_entry_bytes};
use super::pk_keyset_budget::{KeysetReservation, PkKeysetBudget, global_pk_keyset_budget};
use super::table::CayenneTableProvider;
use crate::row_converter::RowConverter;

/// Hashes queued for the recording thread and not yet recorded (256 MiB of
/// them), past which a load stops recording its keys.
const QUEUED_HASHES: usize = 32 << 20;

pub(super) struct FirstLoadPkObserver {
    shared: Arc<Shared>,
    indices: Vec<usize>,
    converter: RowConverter,
    /// To the recording thread; `None` once the load's keys are finished.
    queue: Mutex<Option<std::sync::mpsc::Sender<Hashes>>>,
    /// Receives the recorder once the thread has recorded every queued batch.
    recorded: Mutex<Option<tokio::sync::oneshot::Receiver<Recorder>>>,
}

/// What the writers and the recording thread share.
struct Shared {
    table: String,
    /// Hashes queued and not yet recorded, and the most allowed.
    queued_hashes: AtomicUsize,
    queue_limit: usize,
    /// Set when a written batch was not recorded: the filter would then miss
    /// its keys, so it is never installed.
    incomplete: AtomicBool,
}

/// One written batch's key hashes.
struct Hashes {
    hashes: Vec<u64>,
}

/// The keys written so far, owned by the recording thread.
struct Recorder {
    budget: usize,
    /// The process-wide keyset budget, when one is installed.
    fleet: Option<PkKeysetBudget>,
    keys: WrittenKeys,
}

enum WrittenKeys {
    /// The [`pk_index_bloom_hash`] of every key written, while an exact
    /// keyset of them may still fit the budget.
    Hashed(Vec<u64>),
    Filter(FirstLoadPkFilter),
}

/// A filter of a first load's keys, with its bytes held against the
/// process-wide keyset budget until the table publishes them.
pub(super) struct FirstLoadPkFilter {
    bloom: PkIndexBloom,
    reservation: Option<KeysetReservation>,
}

impl FirstLoadPkFilter {
    /// Caches the filter as `table`'s table-wide index, then returns the bytes
    /// held for it, which the table has published as its own by then.
    pub(super) fn install(self, table: &CayenneTableProvider) {
        table.install_first_load_pk_bloom(self.bloom);
        drop(self.reservation);
    }
}

impl std::fmt::Debug for FirstLoadPkObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirstLoadPkObserver")
            .field("table", &self.shared.table)
            .finish_non_exhaustive()
    }
}

impl FirstLoadPkObserver {
    /// An observer for a first load into `table`, or `None` when a filter could
    /// not stand in for its index: a table that is not upsert (only upsert may
    /// answer existence from a filter) or whose key cannot be encoded.
    pub(super) fn for_table(table: &CayenneTableProvider) -> Option<Self> {
        if !table.upsert_bloom_eligible() {
            return None;
        }
        let indices = table
            .primary_key_indices()
            .ok()
            .flatten()
            .filter(|indices| !indices.is_empty())?;
        let converter = table.build_pk_converter(&indices).ok()?;
        Self::start(
            table.table_name(),
            indices,
            converter,
            // The budget a rebuild of the table-wide index degrades to a filter
            // of, so the filter has the false-positive rate a rebuilt one would.
            table.effective_single_keyset_budget(),
            global_pk_keyset_budget(),
            QUEUED_HASHES,
            None,
        )
    }

    /// Starts the recording thread, which records only once `gate` (when
    /// given) receives, so a test can fill the queue. `None` when the thread
    /// could not be started.
    fn start(
        table: &str,
        indices: Vec<usize>,
        converter: RowConverter,
        budget: usize,
        fleet: Option<PkKeysetBudget>,
        queue_limit: usize,
        gate: Option<std::sync::mpsc::Receiver<()>>,
    ) -> Option<Self> {
        let shared = Arc::new(Shared {
            table: table.to_string(),
            queued_hashes: AtomicUsize::new(0),
            queue_limit,
            incomplete: AtomicBool::new(false),
        });
        let mut recorder = Recorder {
            budget,
            fleet,
            keys: WrittenKeys::Hashed(Vec::new()),
        };
        let (sender, receiver) = std::sync::mpsc::channel::<Hashes>();
        let (recorded_tx, recorded_rx) = tokio::sync::oneshot::channel();
        let recording = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("cayenne-first-load-pk".to_string())
            .spawn(move || {
                if let Some(gate) = gate {
                    let _ = gate.recv();
                }
                for batch in receiver {
                    let count = batch.hashes.len();
                    if !recording.incomplete.load(Ordering::Acquire)
                        && let Err(cause) = recorder.record(&batch)
                    {
                        recording.mark_incomplete(&cause);
                    }
                    recording.queued_hashes.fetch_sub(count, Ordering::AcqRel);
                }
                // The observer may be gone already (its load failed).
                let _ = recorded_tx.send(recorder);
            })
            .ok()?;
        Some(Self {
            shared,
            indices,
            converter,
            queue: Mutex::new(Some(sender)),
            recorded: Mutex::new(Some(recorded_rx)),
        })
    }

    /// The filter to install once every write has completed, when the load
    /// left `live_rows` keys too many for an exact keyset to fit the budget.
    /// `None` when it left fewer, which keeps a smaller table's exact index, or
    /// when a written batch was not recorded.
    pub(super) async fn finish(&self, live_rows: u64) -> Option<FirstLoadPkFilter> {
        // Closing the queue ends the thread once it has recorded what it holds.
        drop(self.queue.lock().take());
        let done = self.recorded.lock().take()?;
        let recorder = done.await.ok()?;
        if self.shared.incomplete.load(Ordering::Acquire) {
            return None;
        }
        recorder.finish(live_rows)
    }
}

impl Shared {
    fn mark_incomplete(&self, cause: &dyn std::fmt::Display) {
        if !self.incomplete.swap(true, Ordering::AcqRel) {
            tracing::debug!(
                table = self.table.as_str(),
                %cause,
                "The first load could not record its primary keys; the index is built after the load instead"
            );
        }
    }
}

impl Recorder {
    /// Records one written batch's key hashes, or says why the load cannot
    /// install a filter of its keys.
    fn record(&mut self, batch: &Hashes) -> Result<(), &'static str> {
        let hashes = match &mut self.keys {
            WrittenKeys::Filter(filter) => {
                filter.bloom.insert_hashes(&batch.hashes);
                return Ok(());
            }
            WrittenKeys::Hashed(hashes) => hashes,
        };
        hashes.extend_from_slice(&batch.hashes);
        let written = u64::try_from(hashes.len()).unwrap_or(u64::MAX);
        if exact_keyset_may_fit(written, self.budget) {
            return Ok(());
        }
        let mut bloom = PkIndexBloom::with_byte_budget(self.budget);
        let reservation = match &self.fleet {
            Some(fleet) => Some(
                fleet
                    .reserve(u64::try_from(bloom.size_bytes()).unwrap_or(u64::MAX))
                    .ok_or("the process-wide keyset budget has no room for the filter")?,
            ),
            None => None,
        };
        bloom.insert_hashes(hashes);
        self.keys = WrittenKeys::Filter(FirstLoadPkFilter { bloom, reservation });
        Ok(())
    }

    fn finish(self, live_rows: u64) -> Option<FirstLoadPkFilter> {
        if exact_keyset_may_fit(live_rows, self.budget) {
            return None;
        }
        match self.keys {
            WrittenKeys::Filter(mut filter) => {
                // Written copies include those the resolution dropped.
                filter
                    .bloom
                    .set_inserted_keys(usize::try_from(live_rows).unwrap_or(usize::MAX));
                Some(filter)
            }
            WrittenKeys::Hashed(_) => None,
        }
    }
}

/// Whether an exact keyset of `keys` keys may fit `budget`: its size, charging
/// each key as the keyset does, is within it.
fn exact_keyset_may_fit(keys: u64, budget: usize) -> bool {
    keys.saturating_mul(u64::try_from(pk_keyset_entry_bytes()).unwrap_or(u64::MAX))
        <= u64::try_from(budget).unwrap_or(u64::MAX)
}

impl VortexWriteObserver for FirstLoadPkObserver {
    fn batch_written(
        &self,
        _file_path: &object_store::path::Path,
        _first_row_position: u64,
        batch: &RecordBatch,
    ) {
        let shared = &self.shared;
        if shared.incomplete.load(Ordering::Acquire) {
            return;
        }
        let columns: Vec<ArrayRef> = self
            .indices
            .iter()
            .map(|index| Arc::clone(batch.column(*index)))
            .collect();
        // The encoding the keyset rebuild and the validation probe use.
        let rows = match self.converter.convert_columns(&columns) {
            Ok(rows) => rows,
            Err(error) => {
                shared.mark_incomplete(&error);
                return;
            }
        };
        let hashes: Vec<u64> = rows
            .iter()
            .map(|row| pk_index_bloom_hash(pk_digest_bytes(row.as_ref())))
            .collect();
        let count = hashes.len();
        let queue = self.queue.lock();
        let Some(sender) = queue.as_ref() else {
            drop(queue);
            shared.mark_incomplete(&"a batch was written after the load's keys were finished");
            return;
        };
        let queued = shared.queued_hashes.fetch_add(count, Ordering::AcqRel);
        let cause = if queued.saturating_add(count) > shared.queue_limit {
            "the load got too far ahead of recording its keys"
        } else if sender
            .send(Hashes { hashes })
            .is_err()
        {
            "the thread recording the keys is gone"
        } else {
            return;
        };
        drop(queue);
        shared.queued_hashes.fetch_sub(count, Ordering::AcqRel);
        shared.mark_incomplete(&cause);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use vortex_datafusion::VortexWriteObserver;

    use super::{FirstLoadPkFilter, FirstLoadPkObserver, PkKeysetBudget, pk_digest_bytes};
    use crate::row_converter::{RowConverter, SortField};

    const ROWS: usize = 1024;

    fn batch(first: i64) -> RecordBatch {
        let ids: Vec<i64> = (first..).take(ROWS).collect();
        let tenants: Vec<String> = ids.iter().map(|id| format!("tenant-{}", id % 7)).collect();
        let schema = Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::Int64, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(tenants)),
                Arc::new(Int64Array::from(ids)),
            ],
        )
        .expect("batch")
    }

    fn key_converter() -> RowConverter {
        RowConverter::new(vec![
            SortField::new(DataType::Utf8),
            SortField::new(DataType::Int64),
        ])
        .expect("converter")
    }

    /// Writes two batches of more keys than an exact keyset of a 4 KiB budget
    /// holds while the recording thread waits, under a queue of `queue_limit`
    /// hashes and a process-wide keyset budget of `fleet`.
    async fn load_two_batches(
        queue_limit: usize,
        fleet: Option<PkKeysetBudget>,
    ) -> Option<FirstLoadPkFilter> {
        let (release, gate) = std::sync::mpsc::channel();
        let observer = FirstLoadPkObserver::start(
            "t",
            vec![0, 1],
            key_converter(),
            4096,
            fleet,
            queue_limit,
            Some(gate),
        )
        .expect("recording thread");
        let path = object_store::path::Path::from("t.vortex");
        observer.batch_written(&path, 0, &batch(0));
        observer.batch_written(&path, 1024, &batch(1024));
        release.send(()).expect("release the recording thread");
        observer.finish(2 * 1024).await
    }

    #[tokio::test]
    async fn a_load_that_gets_ahead_of_recording_its_keys_installs_no_filter() {
        let filter = load_two_batches(usize::MAX, None)
            .await
            .expect("a load whose keys were all recorded yields a filter");
        assert_eq!(filter.bloom.inserted_keys(), 2 * ROWS);
        let converter = key_converter();
        for first in [0, 1024] {
            let keys = converter
                .convert_columns(batch(first).columns())
                .expect("keys");
            assert!(
                keys.iter()
                    .all(|key| filter.bloom.maybe_contains(pk_digest_bytes(key.as_ref()))),
                "the filter holds every key written"
            );
        }
        assert!(
            load_two_batches(ROWS, None).await.is_none(),
            "a load with a batch left unrecorded yields no filter"
        );
    }

    #[tokio::test]
    async fn a_filter_holds_its_bytes_against_the_process_wide_budget() {
        let fleet = PkKeysetBudget::new(1 << 20);
        let filter = load_two_batches(usize::MAX, Some(fleet.clone()))
            .await
            .expect("a budget with room yields a filter");
        assert_eq!(filter.bloom.size_bytes(), 4096);
        assert_eq!(
            fleet.used(),
            4096,
            "the filter's bytes are held while it is built"
        );
        drop(filter);
        assert_eq!(
            fleet.used(),
            0,
            "a filter that is not installed returns its bytes"
        );

        let full = PkKeysetBudget::new(4095);
        assert!(
            load_two_batches(usize::MAX, Some(full.clone()))
                .await
                .is_none(),
            "a load whose filter the budget has no room for yields no filter"
        );
        assert_eq!(full.used(), 0, "a refused filter holds nothing");
    }
}
