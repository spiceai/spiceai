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

//! Splitting a streaming write into layers, so a key its incoming data repeats
//! across record batches resolves as the later upsert it is.
//!
//! Cayenne orders rows by sequence only between snapshots, never within one, and
//! a write's parallel writers reorder rows as they go, so a later copy of a key
//! must be written after the earlier one has been: a batch holding a key the
//! current layer already holds starts a new layer, and each layer is written
//! only once the one before it is complete. [`LayerSplitter`] decides where each
//! batch goes from one exact map of every key the write has admitted, keyed by
//! its 128-bit digest (see [`KeyLayers`]).
//!
//! An overwrite writes every layer into its one snapshot. Its write observer
//! ([`LayerSource::observer`]) records where each admitted row lands, so when a
//! later layer admits a key again, the earlier copy's file and position are
//! already known — its layer finished writing first — and are recorded as
//! superseded. Once every layer is written, a table that deletes by position
//! hides those copies with position deletes, and one that deletes by key
//! rewrites the files that hold them without them. Nothing is read back.
//!
//! A streaming append publishes each layer as its own protected snapshot, which
//! supersedes the copies below it the way any later write does; it uses the map
//! only for the current layer, and cuts a layer at [`MAX_LAYER_ROWS`] to bound it.
//!
//! Under `drop` the first copy of a key wins, so a later copy must be dropped
//! outright, which [`FirstCopyFilter`] does.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_execution::memory_pool::MemoryReservation;
use futures::Stream;
use hash_index::PrehashedBuildHasher;
use parking_lot::Mutex;

use super::key_conflicts::{KeyResolver, ResolvedBatch};

/// Rows one layer of a streaming append holds at most, which bounds the map of
/// the layer it is writing.
pub(crate) const MAX_LAYER_ROWS: usize = 8 * 1024 * 1024;

/// Bytes of input a [`CollapseWindow`] holds before it resolves the keys they
/// repeat; the memory an upsert refresh or append holds for it.
pub(crate) const COLLAPSE_WINDOW_BYTES: usize = 128 * 1024 * 1024;

/// A key's file before the write observer has seen its row land.
const UNWRITTEN: u32 = u32::MAX;

/// The latest copy of one admitted key: its layer and, once written, where.
#[derive(Debug, Clone, Copy)]
struct KeyEntry {
    layer: u32,
    file: u32,
    position: u32,
}

/// Every key a layered write has admitted, by 128-bit digest, with where its
/// latest copy is, and the copies later layers superseded. Shared between the
/// splitter, which admits keys, and the write observer, which locates them.
///
/// The digest is the key's identity here: a superseded copy is hidden by it
/// without reading the key back, so it must not collide (see
/// [`super::pk_index::pk_digest`]).
#[derive(Debug, Default)]
pub(crate) struct KeyLayers {
    entries: HashMap<u128, KeyEntry, PrehashedBuildHasher>,
    files: Vec<Arc<str>>,
    file_ids: HashMap<Arc<str>, u32>,
    /// File-local positions of superseded copies, by file id.
    superseded: HashMap<u32, Vec<u32>>,
    superseded_rows: usize,
    /// The layer the latest admitted batch belongs to.
    admitted_layer: u32,
    /// The first inconsistency the observer met, reported when the superseded
    /// copies are taken; it cannot return an error itself.
    failure: Option<String>,
}

impl KeyLayers {
    fn fail(&mut self, message: impl FnOnce() -> String) {
        if self.failure.is_none() {
            self.failure = Some(message());
        }
    }

    fn file_id(&mut self, path: &str) -> Option<u32> {
        if let Some(&id) = self.file_ids.get(path) {
            return Some(id);
        }
        let id = u32::try_from(self.files.len())
            .ok()
            .filter(|&id| id < UNWRITTEN)?;
        let path: Arc<str> = Arc::from(path);
        self.files.push(Arc::clone(&path));
        self.file_ids.insert(path, id);
        Some(id)
    }

    fn memory_bytes(&self) -> usize {
        // hashbrown: one control byte per bucket beside each entry.
        self.entries.capacity() * (size_of::<(u128, KeyEntry)>() + 1)
            + self.superseded_rows * size_of::<u32>()
    }
}

/// Whether a split write locates its keys and records the copies they
/// supersede (an overwrite), or only keeps a layer free of repeats (an append).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SplitMode {
    Overwrite,
    Append,
}

/// Where a routed batch goes; see the module documentation.
#[derive(Debug)]
struct Routed {
    resolved: ResolvedBatch,
    /// The layer the batch belongs to.
    layer: u32,
    /// The batch opens a new layer.
    starts_layer: bool,
}

/// Assigns each batch of a layered write to a layer; see the module
/// documentation.
pub(crate) struct LayerSplitter {
    resolver: Arc<KeyResolver>,
    keys: Arc<Mutex<KeyLayers>>,
    mode: SplitMode,
    layer: u32,
    layer_rows: usize,
    max_layer_rows: usize,
    reservation: MemoryReservation,
}

impl LayerSplitter {
    /// Split an overwrite, every layer of which joins one snapshot.
    pub(crate) fn new(resolver: Arc<KeyResolver>, reservation: MemoryReservation) -> Self {
        Self::with_mode(resolver, usize::MAX, reservation, SplitMode::Overwrite)
    }

    /// Split an append, whose layers supersede by snapshot.
    pub(crate) fn for_append(
        resolver: Arc<KeyResolver>,
        max_layer_rows: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self::with_mode(resolver, max_layer_rows, reservation, SplitMode::Append)
    }

    fn with_mode(
        resolver: Arc<KeyResolver>,
        max_layer_rows: usize,
        reservation: MemoryReservation,
        mode: SplitMode,
    ) -> Self {
        Self {
            resolver,
            keys: Arc::new(Mutex::new(KeyLayers::default())),
            mode,
            layer: 0,
            layer_rows: 0,
            max_layer_rows: max_layer_rows.max(1),
            reservation,
        }
    }

    /// Size the map for `keys` keys up front, when the memory pool grants it:
    /// a map grown on demand briefly holds its old and new tables at each
    /// doubling, about twice the memory of one sized once. A refused reservation
    /// leaves the map to grow as keys arrive.
    pub(crate) fn with_expected_keys(self, keys: usize) -> Self {
        let mut map = self.keys.lock();
        let bytes = keys.saturating_mul(size_of::<(u128, KeyEntry)>() + 1);
        if keys > 0 && self.reservation.try_resize(bytes).is_ok() {
            map.entries.reserve(keys);
        }
        drop(map);
        self
    }

    fn resolve(&self, batch: &RecordBatch) -> super::Result<ResolvedBatch> {
        self.resolver.resolve_batch(batch)
    }

    /// Decide the batch's layer. The batch is admitted, and the copies it
    /// supersedes recorded, only when it is handed to its layer's write
    /// ([`Self::admit`]); for a batch that opens a layer, that is after the
    /// previous layer has finished writing, so its copies are all located.
    fn route(&mut self, resolved: ResolvedBatch) -> Option<Routed> {
        if resolved.batch.num_rows() == 0 {
            return None;
        }
        let rows = resolved.batch.num_rows();
        let starts_layer = self.layer_rows > 0
            && (self.layer_rows.saturating_add(rows) > self.max_layer_rows || {
                let keys = self.keys.lock();
                let layer = self.layer;
                resolved.digests.iter().any(|digest| {
                    keys.entries
                        .get(digest)
                        .is_some_and(|entry| entry.layer == layer)
                })
            });
        if starts_layer {
            self.layer += 1;
            self.layer_rows = 0;
        }
        self.layer_rows += rows;
        Some(Routed {
            resolved,
            layer: self.layer,
            starts_layer,
        })
    }

    /// Admit a routed batch to its layer: record its keys, and, on an
    /// overwrite, the earlier copies they supersede.
    fn admit(&mut self, routed: Routed) -> super::Result<RecordBatch> {
        let Routed {
            resolved, layer, ..
        } = routed;
        let bytes = {
            let mut keys = self.keys.lock();
            if self.mode == SplitMode::Append && keys.admitted_layer != layer {
                // An append's layers supersede by snapshot; the map only keeps
                // the current one free of repeats.
                keys.entries.clear();
            }
            keys.admitted_layer = layer;
            for &digest in &resolved.digests {
                let latest = KeyEntry {
                    layer,
                    file: UNWRITTEN,
                    position: 0,
                };
                match keys.entries.insert(digest, latest) {
                    None => {}
                    Some(earlier) if earlier.layer < layer && self.mode == SplitMode::Overwrite => {
                        if earlier.file == UNWRITTEN {
                            keys.fail(|| {
                                "a key's earlier copy was not written before a later layer admitted it"
                                    .to_string()
                            });
                            continue;
                        }
                        keys.superseded
                            .entry(earlier.file)
                            .or_default()
                            .push(earlier.position);
                        keys.superseded_rows += 1;
                    }
                    Some(_) => keys.fail(|| "a layer admitted a key it already holds".to_string()),
                }
            }
            keys.memory_bytes()
        };
        self.reservation.try_resize(bytes)?;
        Ok(resolved.batch)
    }
}

/// Records where each row an overwrite's layers admitted lands, and forwards
/// every batch to `inner`.
#[derive(Debug)]
struct KeyLocator {
    resolver: Arc<KeyResolver>,
    keys: Arc<Mutex<KeyLayers>>,
    inner: Option<Arc<dyn vortex_datafusion::VortexWriteObserver>>,
}

impl vortex_datafusion::VortexWriteObserver for KeyLocator {
    fn batch_written(
        &self,
        file_path: &object_store::path::Path,
        first_row_position: u64,
        batch: &RecordBatch,
    ) {
        // Digests are computed before the lock, on the writer's own thread.
        let digests = self.resolver.digests(batch);
        {
            let mut keys = self.keys.lock();
            match digests {
                Err(error) => keys.fail(|| format!("failed to encode written keys: {error}")),
                Ok(digests) => match keys.file_id(file_path.as_ref()) {
                    None => keys.fail(|| "too many files in one layered write".to_string()),
                    Some(file) => {
                        for (row, digest) in digests.into_iter().enumerate() {
                            let position = first_row_position
                                .checked_add(row as u64)
                                .and_then(|position| u32::try_from(position).ok());
                            let Some(position) = position else {
                                keys.fail(|| {
                                    format!(
                                        "row {first_row_position}+{row} of file {file_path} exceeds the \
                                         position-delete range; lower `cayenne_target_file_size_mb`"
                                    )
                                });
                                break;
                            };
                            match keys.entries.get_mut(&digest) {
                                Some(entry) if entry.file == UNWRITTEN => {
                                    entry.file = file;
                                    entry.position = position;
                                }
                                _ => keys.fail(|| {
                                    "a written row was not admitted, or was written twice"
                                        .to_string()
                                }),
                            }
                        }
                    }
                },
            }
        }
        if let Some(inner) = &self.inner {
            inner.batch_written(file_path, first_row_position, batch);
        }
    }
}

/// Collapses the batches of a bounded window of the input — the last copy of a
/// key within the window wins — before the splitter sees them, so a key the
/// window repeats never opens a layer or leaves a tombstone. Each batch is
/// resolved on arrival, so the work done per poll stays one batch's worth.
pub(crate) struct CollapseWindow {
    max_bytes: usize,
    batches: Vec<ResolvedBatch>,
    bytes: usize,
    /// The window position of each key's last copy.
    survivor: HashMap<u128, (usize, usize), PrehashedBuildHasher>,
    /// Whether the window holds a key more than once; when it does not, every
    /// row survives and the drain filters nothing.
    repeats: bool,
    reservation: MemoryReservation,
}

impl CollapseWindow {
    pub(crate) fn new(max_bytes: usize, reservation: MemoryReservation) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            batches: Vec::new(),
            bytes: 0,
            survivor: HashMap::with_hasher(PrehashedBuildHasher),
            repeats: false,
            reservation,
        }
    }

    fn push(&mut self, resolved: ResolvedBatch) -> super::Result<()> {
        let index = self.batches.len();
        for (row, &digest) in resolved.digests.iter().enumerate() {
            self.repeats |= self.survivor.insert(digest, (index, row)).is_some();
        }
        self.bytes += resolved.batch.get_array_memory_size();
        self.batches.push(resolved);
        self.reservation.try_resize(self.held_bytes())?;
        Ok(())
    }

    /// Everything the window holds: its rows and the map of their keys.
    fn held_bytes(&self) -> usize {
        // hashbrown: one control byte per bucket beside each digest and position.
        self.bytes
            + self.survivor.capacity() * (size_of::<u128>() + size_of::<(usize, usize)>() + 1)
    }

    fn is_full(&self) -> bool {
        self.held_bytes() >= self.max_bytes
    }

    /// Release the window's rows and its map, capacity included, so a window
    /// holds at most its bound and nothing between windows.
    fn reset(&mut self) {
        self.survivor = HashMap::with_hasher(PrehashedBuildHasher);
        self.bytes = 0;
        self.reservation.free();
    }

    fn drain(&mut self) -> super::Result<VecDeque<ResolvedBatch>> {
        if !std::mem::take(&mut self.repeats) {
            let out = std::mem::take(&mut self.batches).into();
            self.reset();
            return Ok(out);
        }
        let mut out = VecDeque::with_capacity(self.batches.len());
        for (index, resolved) in std::mem::take(&mut self.batches).into_iter().enumerate() {
            let keep: BooleanArray = resolved
                .digests
                .iter()
                .enumerate()
                .map(|(row, digest)| Some(self.survivor.get(digest) == Some(&(index, row))))
                .collect();
            let kept = keep.true_count();
            if kept == 0 {
                continue;
            }
            if kept == keep.len() {
                out.push_back(resolved);
                continue;
            }
            let digests = resolved
                .digests
                .iter()
                .zip(keep.values().iter())
                .filter_map(|(&digest, kept)| kept.then_some(digest))
                .collect();
            out.push_back(ResolvedBatch {
                batch: filter_record_batch(&resolved.batch, &keep)?,
                digests,
            });
        }
        self.reset();
        Ok(out)
    }
}

struct LayerSourceState {
    input: SendableRecordBatchStream,
    splitter: LayerSplitter,
    window: Option<CollapseWindow>,
    /// Resolved batches waiting to be routed.
    ready: VecDeque<ResolvedBatch>,
    /// The batch that opened the next layer, held until that layer's stream starts.
    carry: Option<Routed>,
    exhausted: bool,
}

/// Splits an overwrite's input into one stream per layer: [`Self::next_layer`]
/// yields the stream of the next layer once the previous one has been drained.
pub(crate) struct LayerSource {
    state: Arc<Mutex<LayerSourceState>>,
    schema: SchemaRef,
    next_layer: usize,
}

impl LayerSource {
    /// `window` collapses bounded windows of the input before they are routed;
    /// only for policies under which the last copy of a key wins.
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        splitter: LayerSplitter,
        window: Option<CollapseWindow>,
    ) -> Self {
        let schema = input.schema();
        Self {
            state: Arc::new(Mutex::new(LayerSourceState {
                input,
                splitter,
                window,
                ready: VecDeque::new(),
                carry: None,
                exhausted: false,
            })),
            schema,
            next_layer: 0,
        }
    }

    /// The stream of the next layer, or `None` once the input is exhausted. The
    /// first layer is always returned, even for an empty input.
    pub(crate) fn next_layer(&mut self) -> Option<SendableRecordBatchStream> {
        {
            let state = self.state.lock();
            if self.next_layer > 0 && state.carry.is_none() {
                return None;
            }
        }
        let layer = self.next_layer;
        self.next_layer += 1;
        Some(Box::pin(LayerStream {
            state: Arc::clone(&self.state),
            schema: Arc::clone(&self.schema),
            layer,
            started: false,
            done: false,
        }))
    }

    /// The write observer every layer's write must report to, so the copies
    /// later layers supersede are located; it forwards each batch to `inner`.
    pub(crate) fn observer(
        &self,
        inner: Option<Arc<dyn vortex_datafusion::VortexWriteObserver>>,
    ) -> Arc<dyn vortex_datafusion::VortexWriteObserver> {
        let state = self.state.lock();
        Arc::new(KeyLocator {
            resolver: Arc::clone(&state.splitter.resolver),
            keys: Arc::clone(&state.splitter.keys),
            inner,
        })
    }

    /// The file-local positions of the copies later layers superseded, by data
    /// file location, sorted; taken once every layer is written.
    ///
    /// # Errors
    ///
    /// Returns an error if a written row could not be located, which leaves
    /// which copies to hide unknown.
    pub(crate) fn take_superseded(&self, table: &str) -> super::Result<HashMap<String, Vec<u32>>> {
        let state = self.state.lock();
        let mut keys = state.splitter.keys.lock();
        if let Some(message) = keys.failure.take() {
            return Err(super::Error::Internal {
                table: table.to_string(),
                message: format!("Overwrite: {message}"),
            });
        }
        let superseded = std::mem::take(&mut keys.superseded);
        Ok(superseded
            .into_iter()
            .map(|(file, mut positions)| {
                positions.sort_unstable();
                (keys.files[file as usize].to_string(), positions)
            })
            .collect())
    }
}

struct LayerStream {
    state: Arc<Mutex<LayerSourceState>>,
    schema: SchemaRef,
    layer: usize,
    started: bool,
    done: bool,
}

impl LayerStream {
    fn accept(
        state: &mut LayerSourceState,
        routed: Routed,
    ) -> datafusion_common::Result<RecordBatch> {
        state.splitter.admit(routed).map_err(Into::into)
    }
}

impl Stream for LayerStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let mut guard = this.state.lock();
        let state = &mut *guard;
        if !this.started {
            this.started = true;
            if this.layer > 0
                && let Some(carry) = state.carry.take()
            {
                return Poll::Ready(Some(Self::accept(state, carry)));
            }
        }
        loop {
            if let Some(resolved) = state.ready.pop_front() {
                let Some(routed) = state.splitter.route(resolved) else {
                    continue;
                };
                if routed.starts_layer {
                    state.carry = Some(routed);
                    this.done = true;
                    return Poll::Ready(None);
                }
                return Poll::Ready(Some(Self::accept(state, routed)));
            }
            if state.exhausted {
                this.done = true;
                return Poll::Ready(None);
            }
            let step: super::Result<()> = match state.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    state.splitter.resolve(&batch).and_then(|resolved| {
                        match state.window.as_mut() {
                            None => state.ready.push_back(resolved),
                            Some(window) => {
                                window.push(resolved)?;
                                if window.is_full() {
                                    state.ready = window.drain()?;
                                }
                            }
                        }
                        Ok(())
                    })
                }
                Poll::Ready(Some(Err(error))) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    state.exhausted = true;
                    match state.window.as_mut() {
                        Some(window) => window.drain().map(|ready| state.ready = ready),
                        None => Ok(()),
                    }
                }
                Poll::Pending => return Poll::Pending,
            };
            if let Err(error) = step {
                this.done = true;
                return Poll::Ready(Some(Err(error.into())));
            }
        }
    }
}

impl RecordBatchStream for LayerStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Keeps the first copy of every key of an overwrite (`drop`), within and across
/// record batches. Exact: a later copy is dropped only when its key was written.
pub(crate) struct FirstCopyFilter {
    input: SendableRecordBatchStream,
    resolver: KeyResolver,
    written: HashSet<u128, PrehashedBuildHasher>,
    reservation: MemoryReservation,
}

impl FirstCopyFilter {
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        resolver: KeyResolver,
        reservation: MemoryReservation,
    ) -> Self {
        Self {
            input,
            resolver,
            written: HashSet::with_hasher(PrehashedBuildHasher),
            reservation,
        }
    }

    fn filter(&mut self, batch: &RecordBatch) -> super::Result<RecordBatch> {
        let resolved = self.resolver.resolve_batch(batch)?;
        let keep: BooleanArray = resolved
            .digests
            .iter()
            .map(|&digest| Some(self.written.insert(digest)))
            .collect();
        // hashbrown stores one control byte per bucket beside each 16-byte key.
        self.reservation
            .try_resize(self.written.capacity() * (size_of::<u128>() + 1))?;
        if keep.true_count() == keep.len() {
            Ok(resolved.batch)
        } else {
            Ok(filter_record_batch(&resolved.batch, &keep)?)
        }
    }
}

impl Stream for FirstCopyFilter {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some(this.filter(&batch).map_err(Into::into)))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for FirstCopyFilter {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::key_conflicts::ConflictPolicy;
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int64Type, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion_execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    use futures::StreamExt;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|(_, v)| *v))),
            ],
        )
        .expect("batch")
    }

    fn input(batches: Vec<RecordBatch>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(batches.into_iter().map(Ok)),
        ))
    }

    fn reservation() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn resolver(policy: ConflictPolicy) -> KeyResolver {
        KeyResolver::new("t", &schema(), &[0], policy).expect("resolver")
    }

    type Layers = Vec<Vec<(i64, String)>>;

    /// Drain `source` layer by layer the way a writer does, reporting each layer's
    /// rows to its observer as file `layer{n}` in arrival order.
    async fn drain(mut source: LayerSource, observe: bool) -> Layers {
        let observer = observe.then(|| source.observer(None));
        let mut out = Vec::new();
        while let Some(mut layer) = source.next_layer() {
            let file = object_store::path::Path::from(format!("layer{}", out.len()));
            let mut rows = Vec::new();
            while let Some(batch) = layer.next().await {
                let batch = batch.expect("batch");
                if let Some(observer) = &observer {
                    observer.batch_written(&file, rows.len() as u64, &batch);
                }
                let ids = batch.column(0).as_primitive::<Int64Type>();
                let values = batch.column(1).as_string::<i32>();
                for row in 0..batch.num_rows() {
                    rows.push((ids.value(row), values.value(row).to_string()));
                }
            }
            out.push(rows);
        }
        out
    }

    /// An overwrite's layers, and the positions of the copies they superseded.
    async fn overwrite_layers(
        batches: Vec<RecordBatch>,
        window: Option<CollapseWindow>,
    ) -> (Layers, Vec<(String, Vec<u32>)>) {
        let splitter = LayerSplitter::new(
            Arc::new(resolver(ConflictPolicy::UpsertKeepLast)),
            reservation(),
        );
        let source = LayerSource::new(input(batches), splitter, window);
        let state = Arc::clone(&source.state);
        let layers = drain(source, true).await;
        let source = LayerSource {
            state,
            schema: schema(),
            next_layer: 0,
        };
        let mut superseded: Vec<_> = source
            .take_superseded("t")
            .expect("superseded")
            .into_iter()
            .collect();
        superseded.sort();
        (layers, superseded)
    }

    fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
        rows.iter().map(|(id, v)| (*id, (*v).to_string())).collect()
    }

    #[tokio::test]
    async fn distinct_keys_stay_in_one_layer() {
        let (layers, superseded) =
            overwrite_layers(vec![batch(&[(1, "a"), (2, "b")]), batch(&[(3, "c")])], None).await;
        assert_eq!(layers, vec![owned(&[(1, "a"), (2, "b"), (3, "c")])]);
        assert!(superseded.is_empty());
    }

    #[tokio::test]
    async fn a_key_repeated_in_a_later_batch_opens_a_layer_that_supersedes_it() {
        let (layers, superseded) = overwrite_layers(
            vec![
                batch(&[(1, "a"), (2, "b")]),
                batch(&[(3, "c")]),
                batch(&[(1, "d"), (4, "e")]),
                batch(&[(4, "f")]),
            ],
            None,
        )
        .await;
        assert_eq!(
            layers,
            vec![
                owned(&[(1, "a"), (2, "b"), (3, "c")]),
                owned(&[(1, "d"), (4, "e")]),
                owned(&[(4, "f")]),
            ]
        );
        // Key 1's first copy is row 0 of layer 0; key 4's is row 1 of layer 1.
        assert_eq!(
            superseded,
            vec![
                ("layer0".to_string(), vec![0]),
                ("layer1".to_string(), vec![1])
            ]
        );
    }

    #[tokio::test]
    async fn an_appends_row_cap_opens_a_layer() {
        let splitter = LayerSplitter::for_append(
            Arc::new(resolver(ConflictPolicy::UpsertKeepLast)),
            2,
            reservation(),
        );
        let source = LayerSource::new(
            input(vec![
                batch(&[(1, "a")]),
                batch(&[(2, "b")]),
                batch(&[(3, "c")]),
            ]),
            splitter,
            None,
        );
        assert_eq!(
            drain(source, false).await,
            vec![owned(&[(1, "a"), (2, "b")]), owned(&[(3, "c")])]
        );
    }

    #[tokio::test]
    async fn an_overwrite_has_no_row_cap() {
        let (layers, _) =
            overwrite_layers((0..20).map(|id| batch(&[(id, "a")])).collect(), None).await;
        assert_eq!(layers.len(), 1);
    }

    #[tokio::test]
    async fn a_window_collapses_repeats_it_holds_without_a_layer() {
        let batches = vec![
            batch(&[(1, "a"), (2, "b")]),
            batch(&[(1, "c"), (3, "d")]),
            batch(&[(2, "e")]),
        ];
        let (layers, superseded) = overwrite_layers(
            batches,
            Some(CollapseWindow::new(COLLAPSE_WINDOW_BYTES, reservation())),
        )
        .await;
        assert_eq!(layers, vec![owned(&[(1, "c"), (3, "d"), (2, "e")])]);
        assert!(superseded.is_empty());
    }

    #[tokio::test]
    async fn a_window_without_repeats_passes_every_row_through() {
        let (layers, superseded) = overwrite_layers(
            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(3, "c")])],
            Some(CollapseWindow::new(COLLAPSE_WINDOW_BYTES, reservation())),
        )
        .await;
        assert_eq!(layers, vec![owned(&[(1, "a"), (2, "b"), (3, "c")])]);
        assert!(superseded.is_empty());
    }

    /// The window's bound covers everything it holds — its rows and the map of
    /// their keys — and holds no more than one window's worth after a drain.
    #[test]
    fn a_window_stays_within_its_bound() {
        const BOUND: usize = 8 * 1024 * 1024;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("window").register(&pool);
        let mut window = CollapseWindow::new(BOUND, reservation);
        let resolver = resolver(ConflictPolicy::UpsertKeepLast);
        let mut peak = 0;
        let mut next_id = 0_i64;
        let mut push = |window: &mut CollapseWindow, rows: i64| {
            let ids: Vec<(i64, String)> = (next_id..next_id + rows)
                .map(|id| (id, format!("value-{id:012}")))
                .collect();
            next_id += rows;
            let rows: Vec<(i64, &str)> = ids.iter().map(|(id, v)| (*id, v.as_str())).collect();
            window
                .push(resolver.resolve_batch(&batch(&rows)).expect("resolve"))
                .expect("push");
        };
        while !window.is_full() {
            push(&mut window, 8192);
            peak = peak.max(pool.reserved());
        }
        assert!(
            peak <= BOUND + BOUND / 4,
            "peak reservation {peak} exceeds the {BOUND}-byte bound"
        );
        window.drain().expect("drain");
        push(&mut window, 1);
        assert!(
            pool.reserved() <= BOUND / 4,
            "after a drain a one-row window reserves {}",
            pool.reserved()
        );
    }

    #[tokio::test]
    async fn a_repeat_across_windows_still_opens_a_layer() {
        // A one-byte window flushes after every batch.
        let (layers, superseded) = overwrite_layers(
            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "c")])],
            Some(CollapseWindow::new(1, reservation())),
        )
        .await;
        assert_eq!(
            layers,
            vec![owned(&[(1, "a"), (2, "b")]), owned(&[(1, "c")])]
        );
        assert_eq!(superseded, vec![("layer0".to_string(), vec![0])]);
    }

    #[tokio::test]
    async fn an_unlocated_earlier_copy_fails_the_write() {
        // Without the observer, layer 0's rows are never located, so layer 1
        // cannot tell where the copy it supersedes is.
        let splitter = LayerSplitter::new(
            Arc::new(resolver(ConflictPolicy::UpsertKeepLast)),
            reservation(),
        );
        let source = LayerSource::new(
            input(vec![batch(&[(1, "a")]), batch(&[(1, "b")])]),
            splitter,
            None,
        );
        let state = Arc::clone(&source.state);
        drain(source, false).await;
        let source = LayerSource {
            state,
            schema: schema(),
            next_layer: 0,
        };
        let error = source
            .take_superseded("t")
            .expect_err("an unlocated earlier copy must fail the write");
        assert!(error.to_string().contains("not written before"), "{error}");
    }

    #[tokio::test]
    async fn an_empty_input_yields_one_empty_layer() {
        let (layers, _) = overwrite_layers(Vec::new(), None).await;
        assert_eq!(layers, vec![Vec::new()]);
    }

    #[tokio::test]
    async fn first_copy_filter_drops_later_copies_across_batches() {
        let mut filter = FirstCopyFilter::new(
            input(vec![
                batch(&[(1, "a"), (2, "b"), (1, "x")]),
                batch(&[(2, "c"), (3, "d")]),
            ]),
            resolver(ConflictPolicy::KeepFirst),
            reservation(),
        );
        let mut rows = Vec::new();
        while let Some(batch) = filter.next().await {
            let batch = batch.expect("batch");
            let ids = batch.column(0).as_primitive::<Int64Type>();
            let values = batch.column(1).as_string::<i32>();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_string()));
            }
        }
        assert_eq!(rows, owned(&[(1, "a"), (2, "b"), (3, "d")]));
    }
}
