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

//! SMB connection pool — multiple authenticated TCP connections to the same
//! server, round-robin dispatched. Eliminates the single-connection mutex
//! bottleneck under concurrent requests.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::client::{SmbClient, SmbConfig};

/// A pool of authenticated SMB connections to the same server.
///
/// Requests are distributed across connections via round-robin. Each connection
/// is an independently authenticated SMB session with its own TCP stream, so
/// concurrent operations don't serialize on a single mutex.
///
/// Slots are recoverable: when a connection is poisoned (e.g. by a read
/// timeout) callers can ask the pool to swap in a freshly-authenticated
/// connection via [`SmbPool::reconnect`], so a brief outage does not
/// permanently brick the cached pool.
pub struct SmbPool {
    config: SmbConfig,
    /// Each slot holds a currently-installed `Arc<SmbClient>`. We use a sync
    /// `Mutex` because the lock is only ever held to clone or replace the
    /// `Arc` — never across an `.await` point. Concurrent reconnects are
    /// serialized by the caller (via `ShareSession`'s per-slot tree-id lock).
    slots: Vec<std::sync::Mutex<Arc<SmbClient>>>,
    next: AtomicUsize,
    /// Cached from the first connection's negotiate response.
    pub(crate) max_read_size: u32,
    pub(crate) max_write_size: u32,
    pub(crate) compound_max_read_size: u32,
    pub(crate) compound_max_write_size: u32,
}

impl SmbPool {
    /// Connect `n` authenticated sessions to the SMB server.
    ///
    /// All connections negotiate independently and authenticate with the same
    /// credentials. The pool uses the negotiated sizes from the first connection.
    pub async fn connect(config: SmbConfig, n: usize) -> io::Result<Arc<Self>> {
        let n = n.max(1);
        let mut clients = Vec::with_capacity(n);

        let first = SmbClient::connect(config.clone()).await?;
        let max_read_size = first.max_read_size;
        let max_write_size = first.max_write_size;
        let compound_max_read_size = first.compound_max_read_size;
        let compound_max_write_size = first.compound_max_write_size;
        clients.push(first);

        if n > 1 {
            let mut joins = Vec::with_capacity(n - 1);
            for _ in 1..n {
                let cfg = config.clone();
                joins.push(tokio::spawn(async move { SmbClient::connect(cfg).await }));
            }
            for join in joins {
                let client = join
                    .await
                    .map_err(|e| io::Error::other(format!("spawn failed: {e}")))??;
                clients.push(client);
            }
            tracing::debug!(target: "smb", "pool: {n} connections ready");
        }

        let slots = clients.into_iter().map(std::sync::Mutex::new).collect();

        Ok(Arc::new(Self {
            config,
            slots,
            next: AtomicUsize::new(0),
            max_read_size,
            max_write_size,
            compound_max_read_size,
            compound_max_write_size,
        }))
    }

    /// Pick the next healthy connection via round-robin, skipping poisoned ones.
    /// Falls back to a poisoned connection if all are poisoned (the caller is
    /// expected to call [`Self::reconnect`] on the returned slot to recover).
    #[must_use]
    pub fn get(&self) -> Arc<SmbClient> {
        let n = self.slots.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            let client = self.client(idx);
            if !client.is_poisoned() {
                return client;
            }
        }
        self.client(start % n)
    }

    /// Get the next round-robin index, preferring healthy connections.
    /// Returns the index even if all slots are poisoned — callers should
    /// invoke [`Self::reconnect`] to recover before using the connection.
    #[must_use]
    pub fn next_index(&self) -> usize {
        let n = self.slots.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if !self.client(idx).is_poisoned() {
                return idx;
            }
        }
        start % n
    }

    /// Access a specific connection by index. Returns a clone of the
    /// currently-installed `Arc<SmbClient>` (the slot may have been swapped
    /// in by a prior `reconnect`).
    #[must_use]
    pub fn client(&self, idx: usize) -> Arc<SmbClient> {
        Arc::clone(
            &self.slots[idx]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// Snapshot the current set of clients (for tree-connect setup).
    #[must_use]
    pub fn clients(&self) -> Vec<Arc<SmbClient>> {
        (0..self.slots.len()).map(|i| self.client(i)).collect()
    }

    /// Number of connections in the pool.
    #[must_use]
    pub fn size(&self) -> usize {
        self.slots.len()
    }

    /// Replace the slot at `idx` with a freshly-authenticated connection,
    /// returning the new client. The previous client is dropped once no
    /// outstanding `Arc` references remain.
    ///
    /// Concurrent reconnects to the same slot will each establish a new
    /// connection; callers should serialize per-slot reconnects (see
    /// `ShareSession::pick` for an example using a per-slot async lock).
    pub async fn reconnect(&self, idx: usize) -> io::Result<Arc<SmbClient>> {
        let new_client = SmbClient::connect(self.config.clone()).await?;
        {
            let mut slot = self.slots[idx]
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *slot = Arc::clone(&new_client);
        }
        tracing::info!(target: "smb", "pool: reconnected slot {idx}");
        Ok(new_client)
    }
}
