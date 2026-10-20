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

//! Process-local prototype diagnostics, separate from exported metrics.
//! Per-source sorted rows/time exclude permutation; the legacy global sorted
//! counters include it. Write counters count emitted attempts, not durable commits.
use parking_lot::Mutex;
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

#[derive(Debug)]
pub(crate) struct Counters {
    // Read slots are in the order destructured by `log`. Write slots count
    // pre-sink sorted output, refusal output, four exclusion reasons, native
    // input, and native output classified as sorted, unsorted or unknown.
    table: Arc<str>,
    path: Arc<str>,
    values: [AtomicU64; 14],
}
impl Counters {
    pub(crate) fn add(&self, index: usize, value: usize) {
        self.values[index].fetch_add(u64::try_from(value).unwrap_or(u64::MAX), Ordering::Relaxed);
    }
}
static READ: LazyLock<Mutex<Vec<Arc<Counters>>>> = LazyLock::new(Mutex::default);
static WRITE: LazyLock<Mutex<Vec<Arc<Counters>>>> = LazyLock::new(Mutex::default);
fn register(
    registry: &Mutex<Vec<Arc<Counters>>>,
    table: Arc<str>,
    path: Arc<str>,
) -> Arc<Counters> {
    let mut entries = registry.lock();
    if let Some(entry) = entries
        .iter()
        .find(|entry| entry.table == table && entry.path == path)
    {
        return Arc::clone(entry);
    }
    let entry = Arc::new(Counters {
        table,
        path,
        values: std::array::from_fn(|_| AtomicU64::new(0)),
    });
    entries.push(Arc::clone(&entry));
    entry
}
pub(crate) fn read(table: Arc<str>, source: &str) -> Arc<Counters> {
    register(&READ, table, Arc::from(source))
}
pub(crate) fn write(table: &str, path: &str) -> Arc<Counters> {
    register(&WRITE, Arc::from(table), Arc::from(path))
}
/// Called on batch boundaries; registry locks are taken only once per minute.
pub(crate) fn log() {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = START.elapsed().as_secs();
    let last = LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 60
        || LAST
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    for entry in READ.lock().iter() {
        let [
            batches,
            rows,
            sorted_batches,
            permuted_batches,
            sorted_ns,
            permuted_ns,
            runs_1_2,
            runs_3_16,
            runs_17_256,
            runs_gt_256,
            hash_batches,
            hash_ns,
            sorted_rows,
            permuted_rows,
        ] = std::array::from_fn(|i| entry.values[i].load(Ordering::Relaxed));
        tracing::info!(table=%entry.table, source=%entry.path, batches, rows, sorted_batches, permuted_batches, sorted_ns, permuted_ns, runs_1_2, runs_3_16, runs_17_256, runs_gt_256, hash_batches, hash_ns, sorted_rows, permuted_rows, "cayenne_key_delete_source_counters");
    }
    for entry in WRITE.lock().iter() {
        let [
            pre_sink_sorted_rows,
            pool_refusal,
            preserve_order,
            no_pk,
            explicit_layout,
            other,
            native_run_sort_input_rows,
            native_sorted_rows,
            native_unsorted_other_rows,
            native_unknown_rows,
            ..,
        ] = std::array::from_fn::<_, 14, _>(|i| entry.values[i].load(Ordering::Relaxed));
        let sorted_rows = pre_sink_sorted_rows.saturating_add(native_sorted_rows);
        tracing::info!(table=%entry.table, write_path=%entry.path, sorted_rows, pre_sink_sorted_rows, pool_refusal, preserve_order, no_pk, explicit_layout, other, native_run_sort_input_rows, native_sorted_rows, native_unsorted_other_rows, native_unknown_rows, "cayenne_write_sort_counters");
    }
}
