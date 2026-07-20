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

//! The CDC checkpoint-store interface.
//!
//! A CDC data-source connector needs to persist and reload its stream position
//! (offsets, sequence numbers, …) across restarts. That persistence is provided by
//! the accelerator, which owns the sidecar tables. This crate is the small seam
//! between the two: the accelerator *satisfies* [`CheckpointStore`] (hence the
//! `runtime-` prefix — the runtime side owns the obligation), and a connector merely
//! *calls* it, so the connector never depends on the accelerator or the runtime
//! orchestrator.
//!
//! The stored payload is an opaque `String` the connector serializes itself: this
//! crate is deliberately connector-agnostic and names no source-specific type.

use std::time::SystemTime;

use async_trait::async_trait;
use snafu::Snafu;

/// A persisted CDC checkpoint for a single dataset: the connector-serialized, opaque
/// `data` blob plus the store-managed timestamp of its last write (connectors use
/// `updated_at` for staleness/expiry decisions).
#[derive(Clone, Debug)]
pub struct CheckpointRecord {
    /// Opaque, connector-serialized checkpoint payload.
    pub data: String,
    /// When the checkpoint was last written, as recorded by the store.
    pub updated_at: Option<SystemTime>,
}

#[derive(Debug, Snafu)]
pub enum CheckpointError {
    #[snafu(display("Checkpoint store operation failed: {source}"))]
    Store {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// A per-dataset checkpoint store, satisfied by the accelerator and called by a
/// data-source connector. Object-safe and `#[async_trait]` so it is used as
/// `Arc<dyn CheckpointStore>`.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Load the current checkpoint for this dataset, if one has been persisted.
    async fn get(&self) -> Option<CheckpointRecord>;

    /// Persist `data` as the current checkpoint for this dataset, overwriting any
    /// previously stored value.
    async fn upsert(&self, data: &str) -> Result<(), CheckpointError>;
}
