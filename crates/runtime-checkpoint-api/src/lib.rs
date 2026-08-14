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

//! Checkpoint-store **interface** for CDC data-source connectors.
//!
//! A CDC connector must persist and reload its stream position (offsets, sequence
//! numbers, resume tokens, …) across restarts. That persistence is provided by the
//! *accelerator*, which owns the sidecar tables inside the dataset's own accelerator
//! database. This crate is the small seam between the two: the accelerator side
//! *satisfies* the store traits (hence the `runtime-` prefix), a connector merely
//! *calls* them, and neither side depends on the other's libraries.
//!
//! # Shape-based design (why this crate is dependency-free)
//!
//! Connectors do **not** all checkpoint the same way, and forcing every one onto a
//! single opaque blob would be wrong — e.g. Kafka must persist a **per-partition**
//! offset and advance it monotonically, so squashing all partitions into one blob row
//! would create write contention and lose the merge. So instead of one universal type,
//! this crate defines a small **dependency-free `struct` per checkpoint shape** plus a
//! trait per shape:
//!
//! | shape (this crate) | store trait | used by |
//! |---|---|---|
//! | [`BlobCheckpoint`] — one opaque `String` | [`BlobCheckpointStore`] | `DynamoDB`, the `PostgreSQL` replication watermark (and any connector that serializes its whole state) |
//! | [`kafka::KafkaCheckpoint`] + [`kafka::KafkaOffset`] rows | [`kafka::KafkaCheckpointStore`] — per-partition upsert resolving to `GREATEST(new, old)` | `Kafka` |
//! | [`mysql_binlog::MySqlBinlogCheckpoint`] | [`mysql_binlog::MySqlBinlogStore`] | `MySQL` |
//! | [`mongodb::MongoCheckpointMetadata`] | [`mongodb::MongoCheckpointStore`] | `MongoDB` |
//!
//! The shape structs are **plain data** — this crate names no `rdkafka`, `aws-sdk-*`,
//! `mysql`, `mongodb`, or even Arrow type — so it takes zero source-library
//! dependencies and needs no capability features. A schema snapshot travels as its
//! durable JSON encoding (`schema_json`), which callers convert with
//! `arrow_tools::schema::{schema_to_json, schema_from_json}`. That keeps each
//! connector's on-disk schema intact (no forced blob migration) while still letting
//! every store be reached as a small object-safe `Arc<dyn …Store>`.
//!
//! # Where each shape is implemented
//!
//! In every case the `runtime` crate resolves a dataset to its accelerator connection
//! and constructs the matching store, so a caller never names an engine. Where the SQL
//! for that store lives differs by shape today:
//!
//! - [`BlobCheckpointStore`] is implemented **per storage engine** in the sibling
//!   `runtime-checkpoint-{duckdb,sqlite,postgres,turso}` crates — one crate per engine,
//!   so the stitch binary links only the engines it enables (`feature = crate`).
//! - The structured shapes ([`kafka`], [`mysql_binlog`], [`mongodb`]) are implemented
//!   inside `runtime`, on the `spice_sys` sidecar types, with their per-engine SQL
//!   behind `runtime`'s accelerator-backend features.
//!
//! The split is incidental rather than a design distinction: the engine crates are where
//! per-engine persistence belongs, and the structured shapes have not moved there yet.
//! Nothing in this crate's contract depends on which side a store is implemented on.

use std::time::SystemTime;

use async_trait::async_trait;
use snafu::Snafu;

pub mod kafka;
pub mod mongodb;
pub mod mysql_binlog;

/// A persisted **blob** checkpoint for one dataset: the connector-serialized, opaque
/// `data` payload plus the store-managed timestamp of its last write (connectors use
/// `updated_at` for staleness/expiry decisions).
///
/// This is the read record for the [`BlobCheckpointStore`] shape. Structured shapes
/// (`Kafka` offsets, `MySQL` binlog position, …) use their own dependency-free types.
#[derive(Clone, Debug)]
pub struct BlobCheckpoint {
    /// Opaque, connector-serialized checkpoint payload (stored as `TEXT`).
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

/// The **blob** checkpoint shape: a per-dataset store of one opaque `String` payload,
/// satisfied by the accelerator and called by a data-source connector. Object-safe and
/// `#[async_trait]` so it is used as `Arc<dyn BlobCheckpointStore>`.
///
/// Use this for connectors whose entire checkpoint serializes to a single value
/// (`DynamoDB`). Connectors with per-key/partition state use a structured shape's store
/// (e.g. `KafkaOffsetStore`) instead — see the crate-level docs.
#[async_trait]
pub trait BlobCheckpointStore: Send + Sync {
    /// Load the current blob checkpoint for this dataset.
    ///
    /// Returns `Ok(None)` when no checkpoint has been persisted yet, and `Err` when the
    /// store read itself fails (e.g. the accelerator is unavailable). Distinguishing the
    /// two lets a connector log/propagate a store failure instead of silently treating
    /// it as "no checkpoint" and re-bootstrapping from scratch.
    async fn get(&self) -> Result<Option<BlobCheckpoint>, CheckpointError>;

    /// Persist `data` as the current blob checkpoint for this dataset, overwriting any
    /// previously stored value.
    async fn upsert(&self, data: &str) -> Result<(), CheckpointError>;
}
