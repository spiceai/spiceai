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
pub struct SmbPool {
    clients: Vec<Arc<SmbClient>>,
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

        Ok(Arc::new(Self {
            clients,
            next: AtomicUsize::new(0),
            max_read_size,
            max_write_size,
            compound_max_read_size,
            compound_max_write_size,
        }))
    }

    /// Pick the next healthy connection via round-robin, skipping poisoned ones.
    /// Falls back to a poisoned connection if all are poisoned (error will
    /// surface on the first I/O attempt).
    #[must_use]
    pub fn get(&self) -> &Arc<SmbClient> {
        let n = self.clients.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if !self.clients[idx].is_poisoned() {
                return &self.clients[idx];
            }
        }
        &self.clients[start % n]
    }

    /// Get the next round-robin index, preferring healthy connections.
    #[must_use]
    pub fn next_index(&self) -> usize {
        let n = self.clients.len();
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let idx = (start + i) % n;
            if !self.clients[idx].is_poisoned() {
                return idx;
            }
        }
        start % n
    }

    /// Access a specific connection by index.
    #[must_use]
    pub fn client(&self, idx: usize) -> &Arc<SmbClient> {
        &self.clients[idx]
    }

    /// Access all connections (for tree-connect setup).
    #[must_use]
    pub fn clients(&self) -> &[Arc<SmbClient>] {
        &self.clients
    }

    /// Number of connections in the pool.
    #[must_use]
    pub fn size(&self) -> usize {
        self.clients.len()
    }
}
