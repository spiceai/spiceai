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

//! Resolving the keys a write repeats within bounded windows of its input.
//!
//! A [`CollapseWindow`] targets [`COLLAPSE_WINDOW_BYTES`] of resolved batches
//! and keeps, for each key it holds more than once, the copy the policy keeps —
//! the last under the upsert policies, the first under `drop` — so a write that
//! cannot take the post-write resolution of [`super::overwrite_postpass`] (a
//! partition's staged append) still resolves repeats this close together.

use std::collections::{HashMap, VecDeque};
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

use super::key_conflicts::{KeyResolver, ResolvedBatch, Survivor};

/// Retained-byte target for a [`CollapseWindow`], checked after each batch.
/// The memory pool can reject a batch before the window reaches this target.
pub(crate) const COLLAPSE_WINDOW_BYTES: usize = 128 * 1024 * 1024;

/// Collapses the batches of a bounded window of the input — keeping the last
/// copy of a key within the window, or the first under `drop` — before they are
/// routed or written, so a key the window repeats never opens a layer or leaves
/// a tombstone. Each batch is resolved on arrival, so the work done per poll
/// stays one batch's worth.
pub(crate) struct CollapseWindow {
    max_bytes: usize,
    batches: Vec<ResolvedBatch>,
    bytes: usize,
    /// The window position of each key's surviving copy.
    survivor: HashMap<u128, (usize, usize), PrehashedBuildHasher>,
    /// Whether the window holds a key more than once; when it does not, every
    /// row survives and the drain filters nothing.
    repeats: bool,
    keeps: Survivor,
    reservation: MemoryReservation,
}

impl CollapseWindow {
    pub(crate) fn new(max_bytes: usize, keeps: Survivor, reservation: MemoryReservation) -> Self {
        Self {
            max_bytes: max_bytes.max(1),
            batches: Vec::new(),
            bytes: 0,
            survivor: HashMap::with_hasher(PrehashedBuildHasher),
            repeats: false,
            keeps,
            reservation,
        }
    }

    fn push(&mut self, resolved: ResolvedBatch, resolver: &KeyResolver) -> super::Result<()> {
        let index = self.batches.len();
        let mut conflicts = 0;
        for (row, &digest) in resolved.digests.iter().enumerate() {
            if self.keeps == Survivor::Identical
                && let Some(&(batch, previous_row)) = self.survivor.get(&digest)
                && self.batches[batch].contents[previous_row] != resolved.contents[row]
            {
                conflicts += 1;
            }
            if self.keeps == Survivor::Earliest {
                match self.survivor.entry(digest) {
                    std::collections::hash_map::Entry::Occupied(_) => self.repeats = true,
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert((index, row));
                    }
                }
            } else {
                self.repeats |= self.survivor.insert(digest, (index, row)).is_some();
            }
        }
        if conflicts > 0 {
            self.reset();
            return Err(resolver.conflicting_versions(conflicts));
        }
        self.bytes += resolved_bytes(&resolved);
        self.batches.push(resolved);
        if let Err(error) = self.reservation.try_resize(self.held_bytes()) {
            self.reset();
            return Err(error.into());
        }
        Ok(())
    }

    /// Retained Arrow buffers, digest vectors, batch slots and key-map buckets.
    fn held_bytes(&self) -> usize {
        // HashMap capacity excludes the empty buckets required by its load
        // factor. Round up to the bucket count and include the control group.
        let map_bytes = if self.survivor.capacity() == 0 {
            0
        } else {
            let buckets = (self.survivor.capacity() + 1).next_power_of_two();
            buckets * (size_of::<(u128, (usize, usize))>() + 1) + 16
        };
        self.bytes + self.batches.capacity() * size_of::<ResolvedBatch>() + map_bytes
    }

    fn is_full(&self) -> bool {
        self.held_bytes() >= self.max_bytes
    }

    /// Release all retained allocations and their reservation.
    fn reset(&mut self) {
        self.batches = Vec::new();
        self.survivor = HashMap::with_hasher(PrehashedBuildHasher);
        self.bytes = 0;
        self.repeats = false;
        self.reservation.free();
    }

    /// Transfer the charge to the batches waiting for the consumer.
    fn drain(&mut self) -> super::Result<(VecDeque<ResolvedBatch>, MemoryReservation)> {
        let reservation = self.reservation.take();
        let batches = std::mem::take(&mut self.batches);
        let result = (|| {
            let out = if self.repeats {
                self.filter_batches(batches, &reservation)?
            } else {
                batches.into()
            };
            let bytes = out.iter().map(resolved_bytes).sum::<usize>()
                + out.capacity() * size_of::<ResolvedBatch>();
            reservation.try_resize(bytes)?;
            Ok((out, reservation))
        })();
        self.reset();
        result
    }

    fn filter_batches(
        &self,
        batches: Vec<ResolvedBatch>,
        reservation: &MemoryReservation,
    ) -> super::Result<VecDeque<ResolvedBatch>> {
        // The original buffers stay charged while filtering allocates their
        // replacements. Include the output slots and temporary selection mask.
        let filtered = reservation.new_empty();
        filtered.try_grow(batches.len() * size_of::<ResolvedBatch>())?;
        let mut out = VecDeque::with_capacity(batches.len());
        for (index, resolved) in batches.into_iter().enumerate() {
            let mask = reservation.new_empty();
            mask.try_grow(resolved.digests.len().div_ceil(8) + 64 + size_of::<BooleanArray>())?;
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
            filtered.try_grow(resolved_bytes(&resolved))?;
            let digests = resolved
                .digests
                .iter()
                .zip(keep.values().iter())
                .filter_map(|(&digest, kept)| kept.then_some(digest))
                .collect();
            let contents = resolved
                .contents
                .iter()
                .zip(keep.values().iter())
                .filter_map(|(&content, kept)| kept.then_some(content))
                .collect();
            out.push_back(ResolvedBatch {
                batch: filter_record_batch(&resolved.batch, &keep)?,
                digests,
                contents,
            });
        }
        Ok(out)
    }
}

fn resolved_bytes(resolved: &ResolvedBatch) -> usize {
    resolved.batch.get_array_memory_size()
        + (resolved.digests.capacity() + resolved.contents.capacity()) * size_of::<u128>()
}

/// Resolves each input batch's own repeats and, with a window, the repeats a
/// window holds, yielding the resolved batches one at a time.
struct Collapser {
    input: SendableRecordBatchStream,
    resolver: Arc<KeyResolver>,
    window: Option<CollapseWindow>,
    /// Resolved batches waiting to be yielded.
    ready: VecDeque<ResolvedBatch>,
    ready_reservation: Option<MemoryReservation>,
    exhausted: bool,
}

impl Collapser {
    fn new(
        input: SendableRecordBatchStream,
        resolver: Arc<KeyResolver>,
        window: Option<CollapseWindow>,
    ) -> Self {
        Self {
            input,
            resolver,
            window,
            ready: VecDeque::new(),
            ready_reservation: None,
            exhausted: false,
        }
    }

    fn drain_window(&mut self) -> super::Result<()> {
        if let Some(window) = &mut self.window {
            let (ready, reservation) = window.drain()?;
            self.ready = ready;
            self.ready_reservation = Some(reservation);
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.exhausted = true;
        self.ready = VecDeque::new();
        self.ready_reservation = None;
        self.window = None;
    }

    /// The next non-empty resolved batch. After an error the collapser yields
    /// nothing more.
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<datafusion_common::Result<ResolvedBatch>>> {
        loop {
            if let Some(resolved) = self.ready.pop_front() {
                if self.ready.is_empty() {
                    self.ready = VecDeque::new();
                    self.ready_reservation = None;
                } else if let Some(reservation) = &self.ready_reservation {
                    reservation.shrink(resolved_bytes(&resolved));
                }
                if resolved.batch.num_rows() == 0 {
                    continue;
                }
                return Poll::Ready(Some(Ok(resolved)));
            }
            if self.exhausted {
                return Poll::Ready(None);
            }
            let step: super::Result<()> = match self.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.resolver.resolve_batch(&batch).and_then(|resolved| {
                        match self.window.as_mut() {
                            None => {
                                self.ready.push_back(resolved);
                                Ok(())
                            }
                            Some(window) => {
                                window.push(resolved, &self.resolver)?;
                                if window.is_full() {
                                    self.drain_window()?;
                                }
                                Ok(())
                            }
                        }
                    })
                }
                Poll::Ready(Some(Err(error))) => {
                    self.finish();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    self.exhausted = true;
                    self.drain_window()
                }
                Poll::Pending => return Poll::Pending,
            };
            if let Err(error) = step {
                self.finish();
                return Poll::Ready(Some(Err(error.into())));
            }
        }
    }
}

/// Resolves the keys its input repeats within bounded windows ([`CollapseWindow`])
/// and passes the result through unsplit: a write that cannot take layers
/// resolves repeats this close together, and leaves a key repeated across
/// windows for its conflict validation to reject, as it would without it.
pub(crate) struct CollapseStream {
    collapser: Collapser,
    schema: SchemaRef,
}

impl CollapseStream {
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        resolver: KeyResolver,
        window_bytes: usize,
        reservation: MemoryReservation,
    ) -> Self {
        let schema = input.schema();
        let window = CollapseWindow::new(
            window_bytes,
            Survivor::for_policy(resolver.policy()),
            reservation,
        );
        Self {
            collapser: Collapser::new(input, Arc::new(resolver), Some(window)),
            schema,
        }
    }
}

impl Stream for CollapseStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut()
            .collapser
            .poll_next(cx)
            .map(|next| next.map(|resolved| resolved.map(|resolved| resolved.batch)))
    }
}

impl RecordBatchStream for CollapseStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
    fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
        rows.iter().map(|(id, v)| (*id, (*v).to_string())).collect()
    }

    async fn collapse(batches: Vec<RecordBatch>, window_bytes: usize) -> Vec<(i64, String)> {
        let mut stream = CollapseStream::new(
            input(batches),
            resolver(ConflictPolicy::UpsertKeepLast),
            window_bytes,
            reservation(),
        );
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("batch");
            let ids = batch.column(0).as_primitive::<Int64Type>();
            let values = batch.column(1).as_string::<i32>();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_string()));
            }
        }
        rows
    }

    #[tokio::test]
    async fn a_window_collapses_the_repeats_it_holds() {
        let rows = collapse(
            vec![
                batch(&[(1, "a"), (2, "b")]),
                batch(&[(1, "c"), (3, "d")]),
                batch(&[(2, "e")]),
            ],
            COLLAPSE_WINDOW_BYTES,
        )
        .await;
        assert_eq!(rows, owned(&[(1, "c"), (3, "d"), (2, "e")]));
    }

    #[tokio::test]
    async fn a_window_without_repeats_passes_every_row_through() {
        let rows = collapse(
            vec![batch(&[(1, "a"), (2, "b")]), batch(&[(3, "c")])],
            COLLAPSE_WINDOW_BYTES,
        )
        .await;
        assert_eq!(rows, owned(&[(1, "a"), (2, "b"), (3, "c")]));
    }

    #[tokio::test]
    async fn a_repeat_across_windows_is_left_for_validation() {
        // A one-byte window drains after every batch.
        let rows = collapse(vec![batch(&[(1, "a"), (2, "b")]), batch(&[(1, "c")])], 1).await;
        assert_eq!(rows, owned(&[(1, "a"), (2, "b"), (1, "c")]));
    }

    #[test]
    fn retained_digests_are_charged_to_the_window() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("window").register(&pool);
        let mut window = CollapseWindow::new(COLLAPSE_WINDOW_BYTES, Survivor::Latest, reservation);
        let mut resolved = resolver(ConflictPolicy::UpsertKeepLast)
            .resolve_batch(&batch(&[(1, "a")]))
            .expect("resolve");
        resolved.digests.reserve(1024);
        let retained_digests = resolved.digests.capacity() * size_of::<u128>();
        let arrays = resolved.batch.get_array_memory_size();
        window
            .push(resolved, &resolver(ConflictPolicy::UpsertKeepLast))
            .expect("reserve window");
        assert!(
            pool.reserved() >= arrays + retained_digests,
            "the retained digest capacity must remain charged: {} reserved for at least {} bytes",
            pool.reserved(),
            arrays + retained_digests,
        );
    }

    #[tokio::test]
    async fn queued_batches_keep_their_reservation_until_yielded() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("window").register(&pool);
        let second = batch(&[(2, "b")]);
        let retained = second.get_array_memory_size() + size_of::<u128>();
        let mut stream = CollapseStream::new(
            input(vec![batch(&[(1, "a")]), second]),
            resolver(ConflictPolicy::UpsertKeepLast),
            COLLAPSE_WINDOW_BYTES,
            reservation,
        );
        assert_eq!(
            stream.next().await.expect("first batch").expect("collapse"),
            batch(&[(1, "a")]),
        );
        assert!(
            pool.reserved() >= retained,
            "the queued second batch needs at least {retained} bytes, reserved {}",
            pool.reserved(),
        );
        drop(stream);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn reservation_refusal_drops_buffered_data() {
        let pool: Arc<dyn MemoryPool> = Arc::new(
            datafusion_execution::memory_pool::GreedyMemoryPool::new(128),
        );
        let mut stream = CollapseStream::new(
            input(vec![batch(&[(1, "a")])]),
            resolver(ConflictPolicy::UpsertKeepLast),
            COLLAPSE_WINDOW_BYTES,
            MemoryConsumer::new("window").register(&pool),
        );
        let error = stream
            .next()
            .await
            .expect("reservation error")
            .expect_err("the window exceeds the pool");
        assert!(matches!(
            error.find_root(),
            datafusion_common::DataFusionError::ResourcesExhausted(_)
        ));
        assert_eq!(pool.reserved(), 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn input_error_releases_the_buffered_window() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let source = RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter([
                Ok(batch(&[(1, "a")])),
                Err(datafusion_common::DataFusionError::Execution(
                    "source failed".to_string(),
                )),
            ]),
        );
        let mut stream = CollapseStream::new(
            Box::pin(source),
            resolver(ConflictPolicy::UpsertKeepLast),
            COLLAPSE_WINDOW_BYTES,
            MemoryConsumer::new("window").register(&pool),
        );
        let error = stream
            .next()
            .await
            .expect("source error")
            .expect_err("the buffered window is not yielded after a source error");
        assert_eq!(error.to_string(), "Execution error: source failed");
        assert_eq!(pool.reserved(), 0);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn filtered_output_stays_charged_until_transferred() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let second = batch(&[(1, "c"), (3, "d")]);
        let retained = second.get_array_memory_size() + 2 * size_of::<u128>();
        let mut stream = CollapseStream::new(
            input(vec![batch(&[(1, "a"), (2, "b")]), second.clone()]),
            resolver(ConflictPolicy::UpsertKeepLast),
            COLLAPSE_WINDOW_BYTES,
            MemoryConsumer::new("window").register(&pool),
        );
        assert_eq!(
            stream.next().await.expect("first").expect("collapse"),
            batch(&[(2, "b")]),
        );
        assert!(pool.reserved() >= retained);
        assert_eq!(
            stream.next().await.expect("second").expect("collapse"),
            second,
        );
        assert_eq!(pool.reserved(), 0);
        assert!(stream.next().await.is_none());
    }

    /// The window's target covers its retained buffers and key-map allocation,
    /// and no allocation from the prior window remains after a drain.
    #[test]
    fn a_window_stays_within_its_bound() {
        const BOUND: usize = 8 * 1024 * 1024;
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let reservation = MemoryConsumer::new("window").register(&pool);
        let mut window = CollapseWindow::new(BOUND, Survivor::Latest, reservation);
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
                .push(
                    resolver.resolve_batch(&batch(&rows)).expect("resolve"),
                    &resolver,
                )
                .expect("reserve window");
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
}
