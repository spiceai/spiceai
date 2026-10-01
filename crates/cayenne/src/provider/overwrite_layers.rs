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
//! Cayenne orders rows by sequence only between snapshots, never within one, so
//! the later copy of a key must be written after the earlier one has been
//! closed off. [`LayerSplitter`] decides where each batch goes:
//!
//! 1. A batch holding a key the current layer may already hold starts a new
//!    layer, as does one that would take the layer past its row cap. A false
//!    positive only starts a layer early.
//! 2. In any layer above the first, a key an earlier layer may hold is recorded
//!    as possibly superseding an earlier copy. A false positive records a key no
//!    earlier layer holds, which a later read-back finds nothing for.
//!
//! An overwrite writes every layer into its one snapshot and, once all are
//! written, reads back the lower layers' keys to find the copies the recorded
//! keys supersede: a table that deletes by position hides them with position
//! deletes, and one that deletes by key rewrites the files that hold them
//! without them. A streaming append publishes each layer as its own protected
//! snapshot, which supersedes the copies below it the way any later write does.
//!
//! The first question is answered by a set of 64-bit key hashes for the current
//! layer, bounded by its row cap; the second by a bloom filter over the whole
//! write. Neither holds a row, and a false positive costs a layer or a
//! read-back of a key, never a row.
//!
//! Under `drop` the first copy of a key wins, so a later copy must be dropped
//! outright; that needs an exact answer, which [`FirstCopyFilter`] keeps.

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
use hash_index::{PrehashedBuildHasher, SplitBlockBloomFilter};
use parking_lot::Mutex;

use super::key_conflicts::{KeyResolver, ResolvedBatch};

/// Rows one layer holds at most. Bounds the per-layer filter, and so the memory
/// a layered overwrite holds for the layer it is writing.
pub(crate) const MAX_LAYER_ROWS: usize = 8 * 1024 * 1024;

/// Bytes of input a [`CollapseWindow`] holds before it resolves the keys they
/// repeat; the memory an upsert refresh or append holds for it.
pub(crate) const COLLAPSE_WINDOW_BYTES: usize = 128 * 1024 * 1024;

/// Capacity of the first filter of a [`ChainedBloom`], small so a write of a few
/// rows allocates a few kilobytes; each next filter is [`FILTER_GROWTH`] times
/// larger, so a full layer of [`MAX_LAYER_ROWS`] keys is probed through eight.
const FIRST_FILTER_KEYS: usize = 1024;
const FILTER_GROWTH: usize = 4;

/// A bloom filter that grows: when its newest split-block filter reaches the key
/// count it was sized for, it adds a larger one. A probe checks them all.
struct ChainedBloom {
    filters: Vec<SplitBlockBloomFilter>,
    /// Keys in the newest filter.
    newest_keys: usize,
    newest_capacity: usize,
    /// Multiple of the split-block filter's 16 bits per key.
    density: usize,
}

impl ChainedBloom {
    fn new(density: usize) -> Self {
        Self {
            filters: Vec::new(),
            newest_keys: 0,
            newest_capacity: 0,
            density,
        }
    }

    fn might_contain(&self, hash: u64) -> bool {
        self.filters.iter().any(|filter| filter.might_contain(hash))
    }

    fn insert(&mut self, hash: u64) {
        if self.newest_keys >= self.newest_capacity {
            self.newest_capacity = (self.newest_capacity * FILTER_GROWTH).max(FIRST_FILTER_KEYS);
            self.filters.push(SplitBlockBloomFilter::new(
                self.newest_capacity * self.density,
            ));
            self.newest_keys = 0;
        }
        if let Some(filter) = self.filters.last() {
            filter.insert(hash);
        }
        self.newest_keys += 1;
    }

    fn memory_bytes(&self) -> usize {
        self.filters
            .iter()
            .map(SplitBlockBloomFilter::memory_usage_bytes)
            .sum()
    }
}

/// Where a routed batch goes; see the module documentation.
#[derive(Debug)]
struct Routed {
    batch: RecordBatch,
    /// The batch opens a new layer.
    starts_layer: bool,
    /// Digests of the batch's keys an earlier layer may hold.
    superseding: Vec<u128>,
}

/// Assigns each batch of an overwrite to a layer; see the module documentation.
pub(crate) struct LayerSplitter {
    resolver: KeyResolver,
    /// 64-bit hashes of the current layer's keys. Exact up to a hash collision,
    /// which only starts a layer early, and it grows with the layer instead of
    /// being sized in advance; a chain of bloom filters would sum its filters'
    /// false-positive rates and split refreshes that repeat no key.
    layer_keys: HashSet<u64, PrehashedBuildHasher>,
    /// Keys of every layer written so far. 16 bits per key: a false positive
    /// here costs one key's read-back.
    written_keys: ChainedBloom,
    /// Whether to record the keys that may supersede an earlier layer's; an
    /// append's layers supersede by snapshot instead.
    track_superseding: bool,
    layer: usize,
    layer_rows: usize,
    max_layer_rows: usize,
    reservation: MemoryReservation,
}

impl LayerSplitter {
    pub(crate) fn new(
        resolver: KeyResolver,
        max_layer_rows: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self::with_superseding(resolver, max_layer_rows, reservation, true)
    }

    /// Split an append, whose layers supersede by snapshot, without recording
    /// the keys that may supersede an earlier layer's.
    pub(crate) fn for_append(
        resolver: KeyResolver,
        max_layer_rows: usize,
        reservation: MemoryReservation,
    ) -> Self {
        Self::with_superseding(resolver, max_layer_rows, reservation, false)
    }

    fn with_superseding(
        resolver: KeyResolver,
        max_layer_rows: usize,
        reservation: MemoryReservation,
        track_superseding: bool,
    ) -> Self {
        Self {
            resolver,
            layer_keys: HashSet::with_hasher(PrehashedBuildHasher),
            written_keys: ChainedBloom::new(1),
            track_superseding,
            layer: 0,
            layer_rows: 0,
            max_layer_rows: max_layer_rows.max(1),
            reservation,
        }
    }

    fn resolve(&self, batch: &RecordBatch) -> super::Result<ResolvedBatch> {
        self.resolver.resolve_batch(batch)
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "Bloom filters deliberately hash separate 64-bit halves of each 128-bit digest"
    )]
    fn route(&mut self, resolved: ResolvedBatch) -> super::Result<Option<Routed>> {
        if resolved.batch.num_rows() == 0 {
            return Ok(None);
        }
        let rows = resolved.batch.num_rows();
        // The two filters hash different halves of the digest, so their false
        // positives are independent.
        let layer_hash = |digest: u128| digest as u64;
        let written_hash = |digest: u128| (digest >> 64) as u64;
        let starts_layer = self.layer_rows > 0
            && (self.layer_rows + rows > self.max_layer_rows
                || resolved
                    .digests
                    .iter()
                    .any(|&digest| self.layer_keys.contains(&layer_hash(digest))));
        if starts_layer {
            self.layer += 1;
            self.layer_keys.clear();
            self.layer_rows = 0;
        }
        // In the first layer every key written so far is in the current layer,
        // which rule 1 has just ruled out, so a hit there is a false positive.
        let superseding: Vec<u128> = if !self.track_superseding || self.layer == 0 {
            Vec::new()
        } else {
            resolved
                .digests
                .iter()
                .copied()
                .filter(|&digest| self.written_keys.might_contain(written_hash(digest)))
                .collect()
        };
        for &digest in &resolved.digests {
            self.layer_keys.insert(layer_hash(digest));
            if self.track_superseding {
                self.written_keys.insert(written_hash(digest));
            }
        }
        self.layer_rows += rows;
        self.reservation.try_resize(
            // hashbrown: one control byte per bucket beside each 8-byte hash.
            self.layer_keys.capacity() * (size_of::<u64>() + 1) + self.written_keys.memory_bytes(),
        )?;
        Ok(Some(Routed {
            batch: resolved.batch,
            starts_layer,
            superseding,
        }))
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
    reservation: MemoryReservation,
}

impl CollapseWindow {
    pub(crate) fn new(max_bytes: usize, reservation: MemoryReservation) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            batches: Vec::new(),
            bytes: 0,
            survivor: HashMap::with_hasher(PrehashedBuildHasher),
            reservation,
        }
    }

    fn push(&mut self, resolved: ResolvedBatch) -> super::Result<()> {
        let index = self.batches.len();
        for (row, &digest) in resolved.digests.iter().enumerate() {
            self.survivor.insert(digest, (index, row));
        }
        self.bytes += resolved.batch.get_array_memory_size();
        self.batches.push(resolved);
        // hashbrown: one control byte per bucket beside each digest and position.
        self.reservation.try_resize(
            self.bytes
                + self.survivor.capacity() * (size_of::<u128>() + size_of::<(usize, usize)>() + 1),
        )?;
        Ok(())
    }

    fn is_full(&self) -> bool {
        self.bytes >= self.max_bytes
    }

    fn drain(&mut self) -> super::Result<VecDeque<ResolvedBatch>> {
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
        self.survivor.clear();
        self.bytes = 0;
        self.reservation.free();
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
    /// Digests of the keys each layer may supersede, indexed by layer.
    superseding: Vec<Vec<u128>>,
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
                superseding: Vec::new(),
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

    /// Digests of the keys each layer so far may supersede, indexed by layer,
    /// taken once all layers are written.
    pub(crate) fn take_superseding(&self) -> Vec<Vec<u128>> {
        std::mem::take(&mut self.state.lock().superseding)
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
    fn accept(state: &mut LayerSourceState, layer: usize, routed: Routed) -> RecordBatch {
        if state.superseding.len() <= layer {
            state.superseding.resize_with(layer + 1, Vec::new);
        }
        state.superseding[layer].extend(routed.superseding);
        routed.batch
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
                return Poll::Ready(Some(Ok(Self::accept(state, this.layer, carry))));
            }
        }
        loop {
            if let Some(resolved) = state.ready.pop_front() {
                let routed = match state.splitter.route(resolved) {
                    Ok(Some(routed)) => routed,
                    Ok(None) => continue,
                    Err(error) => {
                        this.done = true;
                        return Poll::Ready(Some(Err(error.into())));
                    }
                };
                if routed.starts_layer {
                    state.carry = Some(routed);
                    this.done = true;
                    return Poll::Ready(None);
                }
                return Poll::Ready(Some(Ok(Self::accept(state, this.layer, routed))));
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

    async fn layers(
        batches: Vec<RecordBatch>,
        max_layer_rows: usize,
    ) -> (Vec<Vec<(i64, String)>>, Vec<Vec<u128>>) {
        windowed_layers(batches, max_layer_rows, None).await
    }

    async fn windowed_layers(
        batches: Vec<RecordBatch>,
        max_layer_rows: usize,
        window: Option<CollapseWindow>,
    ) -> (Vec<Vec<(i64, String)>>, Vec<Vec<u128>>) {
        let splitter = LayerSplitter::new(
            resolver(ConflictPolicy::UpsertKeepLast),
            max_layer_rows,
            reservation(),
        );
        let mut source = LayerSource::new(input(batches), splitter, window);
        let mut out = Vec::new();
        while let Some(mut layer) = source.next_layer() {
            let mut rows = Vec::new();
            while let Some(batch) = layer.next().await {
                let batch = batch.expect("batch");
                let ids = batch.column(0).as_primitive::<Int64Type>();
                let values = batch.column(1).as_string::<i32>();
                for row in 0..batch.num_rows() {
                    rows.push((ids.value(row), values.value(row).to_string()));
                }
            }
            out.push(rows);
        }
        (out, source.take_superseding())
    }

    fn digest(id: i64) -> u128 {
        resolver(ConflictPolicy::UpsertKeepLast)
            .resolve_batch(&batch(&[(id, "")]))
            .expect("resolve")
            .digests[0]
    }

    fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
        rows.iter().map(|(id, v)| (*id, (*v).to_string())).collect()
    }

    #[tokio::test]
    async fn distinct_keys_stay_in_one_layer() {
        let (layers, superseding) = layers(
            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(3, "c")])],
            MAX_LAYER_ROWS,
        )
        .await;
        assert_eq!(layers, vec![owned(&[(1, "a"), (2, "b"), (3, "c")])]);
        assert!(superseding.iter().all(Vec::is_empty));
    }

    #[tokio::test]
    async fn a_key_repeated_in_a_later_batch_opens_a_layer_that_supersedes_it() {
        let (layers, superseding) = layers(
            vec![
                batch(&[(1, "a"), (2, "b")]),
                batch(&[(3, "c")]),
                batch(&[(1, "d"), (4, "e")]),
                batch(&[(4, "f")]),
            ],
            MAX_LAYER_ROWS,
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
        assert_eq!(
            superseding,
            vec![Vec::new(), vec![digest(1)], vec![digest(4)]]
        );
    }

    #[tokio::test]
    async fn the_row_cap_opens_a_layer_that_supersedes_nothing() {
        let (layers, superseding) = layers(
            vec![batch(&[(1, "a")]), batch(&[(2, "b")]), batch(&[(3, "c")])],
            2,
        )
        .await;
        assert_eq!(
            layers,
            vec![owned(&[(1, "a"), (2, "b")]), owned(&[(3, "c")])]
        );
        assert!(superseding.iter().all(Vec::is_empty));
    }

    #[tokio::test]
    async fn a_window_collapses_repeats_it_holds_without_a_layer() {
        let batches = vec![
            batch(&[(1, "a"), (2, "b")]),
            batch(&[(1, "c"), (3, "d")]),
            batch(&[(2, "e")]),
        ];
        let (layers, superseding) = windowed_layers(
            batches,
            MAX_LAYER_ROWS,
            Some(CollapseWindow::new(COLLAPSE_WINDOW_BYTES, reservation())),
        )
        .await;
        assert_eq!(layers, vec![owned(&[(1, "c"), (3, "d"), (2, "e")])]);
        assert!(superseding.iter().all(Vec::is_empty));
    }

    #[tokio::test]
    async fn a_repeat_across_windows_still_opens_a_layer() {
        // A one-byte window flushes after every batch.
        let (layers, superseding) = windowed_layers(
            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "c")])],
            MAX_LAYER_ROWS,
            Some(CollapseWindow::new(1, reservation())),
        )
        .await;
        assert_eq!(
            layers,
            vec![owned(&[(1, "a"), (2, "b")]), owned(&[(1, "c")])]
        );
        assert_eq!(superseding[1], vec![digest(1)]);
    }

    #[tokio::test]
    async fn an_empty_input_yields_one_empty_layer() {
        let (layers, _) = layers(Vec::new(), MAX_LAYER_ROWS).await;
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
