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

//! The sidecar seam: how the runtime reaches the metadata tables an accelerator
//! keeps beside a dataset's accelerated data.
//!
//! An accelerator engine hosts more than the dataset itself. It also stores the
//! runtime's own bookkeeping — a CDC connector's stream position, the dataset schema
//! checkpoint, the caching engine's fetch timestamp — in `spice_sys_*` tables inside
//! the same database. The SQL for those tables is per-engine, but *which* tables exist
//! and what they mean is not.
//!
//! [`AcceleratorSidecar`] is that split. An engine resolves a dataset to one bound
//! sidecar (its connection plus the dataset name) and hands back the stores; the
//! runtime asks the engine for a sidecar and never learns which engine answered. That
//! is what keeps the engines off the runtime's dependency graph: without this seam the
//! runtime has to name `DuckDBAccelerator`/`SqliteAccelerator`/`TursoAccelerator` to
//! borrow their connection pools.
//!
//! The store traits themselves live in `runtime-checkpoint-api`, which is deliberately
//! dependency-free so a connector can call one without naming an accelerator. This
//! trait sits one crate up because [`DatasetCheckpointer`] is Arrow-typed and that
//! crate is Arrow-free.
//!
//! # Not every engine hosts every store
//!
//! `caching_engine` is `DuckDB`-only, and the in-memory engines (`arrow`,
//! `partitioned_arrow`) host nothing at all. A store an engine cannot provide is
//! reported as [`CheckpointError::Store`] rather than skipped, so a connector that
//! needs one fails loudly at registration instead of silently running stateless and
//! re-bootstrapping on every restart.

use std::sync::Arc;

use async_trait::async_trait;
use runtime_checkpoint_api::{
    BlobCheckpointStore, CheckpointError, debezium::DebeziumCheckpointStore,
    kafka::KafkaCheckpointStore, mongodb::MongoCheckpointStore, mysql_binlog::MySqlBinlogStore,
};

use crate::dataset_checkpoint::DatasetCheckpointer;

/// Whether resolving a sidecar may create the accelerator's database, or must find one
/// that already exists.
///
/// `OpenExisting` is what a *read* path wants: creating an empty database to discover it
/// holds no checkpoint would leave a stray file behind and report "no checkpoint" for a
/// dataset whose data simply has not been written yet.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum OpenOption {
    CreateIfNotExists,
    OpenExisting,
}

/// One dataset's sidecar tables inside one accelerator engine.
///
/// Obtained from `DataAccelerator::sidecar`, already bound to the dataset and to the
/// engine's connection, so every method below is a pure lookup rather than another
/// round of resolution.
///
/// Implemented once per engine in the `runtime-checkpoint-{duckdb,sqlite,turso,postgres}`
/// crates. **Every method must be answered explicitly** — an engine that cannot host a
/// store returns [`CheckpointError::Store`] naming itself, so adding a store to this
/// trait cannot silently no-op on an engine that forgot to implement it.
#[async_trait]
pub trait AcceleratorSidecar: Send + Sync {
    /// The opaque single-value checkpoint store for `table_name`.
    ///
    /// `table_name` is a parameter because several connectors keep independent blob
    /// checkpoints in the same database, one sidecar table each.
    fn blob_checkpoint_store(
        &self,
        table_name: &'static str,
    ) -> Result<Arc<dyn BlobCheckpointStore>, CheckpointError>;

    /// The per-partition Kafka offset store.
    fn kafka_checkpoint_store(&self) -> Result<Arc<dyn KafkaCheckpointStore>, CheckpointError>;

    /// The Debezium checkpoint store. Shares the Kafka offset rows, because Debezium
    /// events arrive over Kafka.
    fn debezium_checkpoint_store(
        &self,
    ) -> Result<Arc<dyn DebeziumCheckpointStore>, CheckpointError>;

    /// The `MySQL` binlog position store.
    fn mysql_binlog_store(&self) -> Result<Arc<dyn MySqlBinlogStore>, CheckpointError>;

    /// The `MongoDB` resume-token store.
    fn mongo_checkpoint_store(&self) -> Result<Arc<dyn MongoCheckpointStore>, CheckpointError>;

    /// The dataset schema/refresh-SQL checkpoint.
    fn dataset_checkpointer(&self) -> Result<Arc<dyn DatasetCheckpointer>, CheckpointError>;

    /// Records that the caching engine fetched this dataset just now.
    ///
    /// `DuckDB`-only: it is the only engine Spice serves cached results from, so every
    /// other engine reports it cannot.
    async fn update_caching_engine_fetched_at(&self) -> Result<(), CheckpointError>;
}

/// Reports that `engine` does not host the `store` sidecar.
///
/// Shared so the message reads the same whichever engine declines, which is what a
/// caller sees when it asks an in-memory accelerator for a CDC checkpoint.
#[must_use]
pub fn unsupported_sidecar(engine: &str, store: &str) -> CheckpointError {
    CheckpointError::Store {
        source: format!(
            "The {engine} accelerator does not store {store} state. Accelerate this dataset with an engine that does (duckdb, sqlite, turso, or postgres), or remove the setting that needs it. See: https://spiceai.org/docs/components/data-accelerators"
        )
        .into(),
    }
}
