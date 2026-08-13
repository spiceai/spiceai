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

//! Runtime-side seam for the `PostgreSQL` replication watermark sidecar.
//!
//! Records the LSN a dataset's acceleration is complete as of, so a restart can
//! tell a resumable gap from an unfillable one. `PostgreSQL` CDC historically
//! kept no client-side position at all, relying on the slot's server-tracked
//! `confirmed_flush_lsn` — which is exactly what disappears when the slot is
//! dropped or invalidated, leaving the runtime to *infer* whether the source can
//! still supply the missing changes. `MySQL` has kept a client-side position all
//! along (`spice_sys_mysql_binlog`); this is the same idea for Postgres.
//!
//! The store lives in the dataset's own accelerator, reached through the
//! runtime-internal engine registry, so — like the `DynamoDB` Streams sidecar —
//! this module keeps that resolution inside `runtime` and hands the connector
//! back only the opaque [`BlobCheckpointStore`] trait object.
//!
//! `None` means no usable accelerator connection, i.e. nothing durable to record
//! a position in. The connector treats that as "never loaded", which is correct:
//! an acceleration that cannot persist a watermark cannot have persisted rows for
//! one to describe.

use std::sync::Arc;

use runtime_checkpoint_api::BlobCheckpointStore;

use super::checkpoint_store;
use crate::component::dataset::Dataset;

/// Re-exported so a connector can name the returned trait object without taking
/// its own dependency on the checkpoint-api crate — the point of this seam is
/// that the connector depends on `runtime` alone.
pub use runtime_checkpoint_api::BlobCheckpointStore as WatermarkBlobStore;

/// Sidecar table (in the dataset's own accelerator) holding the serialized
/// applied-LSN watermark.
const POSTGRES_REPLICATION_WATERMARK_TABLE: &str = "spice_sys_postgres_replication";

/// Resolves the dataset's accelerator into a blob checkpoint store for the
/// `PostgreSQL` replication watermark sidecar.
pub async fn init_watermark_store(dataset: &Dataset) -> Option<Arc<dyn BlobCheckpointStore>> {
    // `None` (no usable accelerator connection) is a graceful degradation, and
    // `checkpoint_store` already logs the underlying reason, so don't double-log.
    checkpoint_store(dataset, POSTGRES_REPLICATION_WATERMARK_TABLE).await
}
