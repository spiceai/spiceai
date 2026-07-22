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

//! Runtime-side seam for the `DynamoDB` Streams checkpoint sidecar.
//!
//! The `connector-dynamodb` crate must persist its stream position, but the store is
//! owned by the dataset's accelerator — reaching it goes through the runtime-internal
//! accelerator engine registry. This module keeps that resolution inside `runtime` and
//! hands the connector back only the opaque [`BlobCheckpointStore`] trait object, so the
//! connector never depends on the accelerator internals (or the per-engine store crates).

use std::sync::Arc;

use runtime_checkpoint_api::BlobCheckpointStore;

use super::checkpoint_store;
use crate::component::dataset::Dataset;

/// Sidecar table (in the dataset's own accelerator) holding this connector's
/// serialized stream checkpoint.
const DYNAMODB_STREAMS_CHECKPOINT_TABLE: &str = "spice_sys_dynamodb_streams";

/// Resolves the dataset's accelerator into a blob checkpoint store for the `DynamoDB`
/// Streams sidecar table. `None` means no usable accelerator connection, so the
/// connector state is ephemeral and the stream restarts on every runtime restart.
pub async fn init_checkpoint_store(dataset: &Dataset) -> Option<Arc<dyn BlobCheckpointStore>> {
    // `None` (no usable accelerator connection) is a graceful "checkpointing
    // unavailable" degradation; `checkpoint_store` already logs the underlying
    // reason, so don't double-log it here.
    checkpoint_store(dataset, DYNAMODB_STREAMS_CHECKPOINT_TABLE).await
}
