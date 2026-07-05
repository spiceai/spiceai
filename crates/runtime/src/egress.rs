/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Memory-pool accounting for query-result *egress* buffers — the encoded
//! `FlightData` sitting in the Flight send channel and the serialized HTTP
//! response chunks in flight. Historically these buffers were invisible to
//! `runtime.query.memory_limit` (only execution operators reserved against the
//! pool), so a burst of large concurrent results could grow memory past the
//! limit unaccounted. [`EgressAccount`] charges those bytes against the same
//! [`MemoryPool`] the query executed under and applies light back-pressure when
//! the pool is under real pressure.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

/// Cooperative yields [`EgressAccount::reserve`] makes while the pool is full
/// before it over-commits, so it never blocks egress waiting for pool space.
const MAX_RESERVE_YIELDS: usize = 8;

/// Charges query-result egress bytes against a [`MemoryPool`].
///
/// Reserve bytes when a chunk is buffered for send; release them once the chunk
/// has been handed downstream (to tonic / hyper). The held reservation therefore
/// reflects "egress bytes we are currently buffering," which — combined with the
/// per-stream bounded send channel — bounds egress memory and makes it visible to
/// `runtime.query.memory_limit`. Dropping the account frees any still-held bytes
/// (RAII via [`MemoryReservation`]'s `Drop`), so a client disconnect can't leak a
/// reservation.
pub(crate) struct EgressAccount {
    // `MemoryReservation` is `Send + Sync` and interior-mutable (its size is an
    // atomic; grow/shrink take `&self`), so it needs no lock — reserve (producer)
    // and release (consumer) can hit it from different threads on the Flight
    // off-runtime path.
    reservation: MemoryReservation,
}

impl EgressAccount {
    /// Register a new egress consumer against `pool`.
    pub(crate) fn register(pool: &Arc<dyn MemoryPool>, name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            reservation: MemoryConsumer::new(name).register(pool),
        })
    }

    /// Reserve `bytes` for a chunk about to be buffered for send.
    ///
    /// The `MemoryPool` is sync-only (no async wait-for-space), so back-pressure
    /// is a short bounded yield loop: while the pool is full we yield to let
    /// consumers drain and release memory. After a few attempts we over-commit
    /// with the infallible `grow` rather than fail the query or block forever — a
    /// single oversized result or a stalled client must never deadlock egress,
    /// and the per-stream bounded channel still caps buffering.
    pub(crate) async fn reserve(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        for _ in 0..MAX_RESERVE_YIELDS {
            if self.reservation.try_grow(bytes).is_ok() {
                return;
            }
            tokio::task::yield_now().await;
        }
        self.reservation.grow(bytes);
    }

    /// Release `bytes` once the chunk has been handed downstream. Never shrinks
    /// past the current reservation size (which would panic).
    pub(crate) fn release(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let to_free = bytes.min(self.reservation.size());
        self.reservation.shrink(to_free);
    }
}
