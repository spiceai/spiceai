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

//! Write-mode dispatch for [`AcceleratedTable`].
//!
//! This module defines [`WriteMode`], the enum that controls where an
//! accelerated table sends write operations (INSERT INTO), and exposes
//! per-mode executors in the [`write_back`] and [`dual_write`] submodules.
//!
//! The currently supported modes are:
//!
//! - [`WriteMode::WriteThrough`]: writes go to the federated source only; the
//!   accelerator is updated through the normal refresh cycle (WAL replication
//!   for `refresh_mode: changes`, periodic refresh otherwise). This is the
//!   default and matches the user-facing `write_mode: write_through` contract.
//! - [`WriteMode::AcceleratorOnly`]: writes go only to the local accelerator
//!   (used when `on_conflict` upserts into the accelerator without CDC).
//! - [`WriteMode::WriteBack`]: writes commit to the local accelerator first,
//!   then asynchronously persist the same mutation to the federated source.
//!   Source persistence failures are logged rather than returned to the caller,
//!   and `replication.enabled` is required as the caller's opt-in to those
//!   asynchronous durability semantics.
//! - [`WriteMode::DualWrite`]: writes go to both the Cayenne accelerator and
//!   the federated source simultaneously with staged commit/rollback. This is
//!   *not* exposed via spicepod `write_mode` — it is reserved for the Iceberg
//!   federated catalog cache path, where Cayenne acts as a write-through cache
//!   in front of an Iceberg catalog that has no CDC stream to propagate writes.
//!
//! [`AcceleratedTable`]: super::AcceleratedTable

pub(crate) mod dual_write;
pub(crate) mod write_back;

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;

pub(crate) use dual_write::CayenneWriteTarget;

use crate::federated_table::FederatedTable;

/// Controls where writes (INSERT INTO) are directed for an `AcceleratedTable`.
#[derive(Debug, Clone)]
pub(crate) enum WriteMode {
    /// Writes go to the federated source only. The acceleration refresh mechanism
    /// picks up new data on its next cycle. This is the default and matches the
    /// user-facing `write_mode: write_through` contract.
    WriteThrough,
    /// Writes go only to the local accelerator (not replicated to the source).
    /// Used when `on_conflict` is configured or for internal tables.
    AcceleratorOnly,
    /// Writes commit to the local accelerator first, then asynchronously persist
    /// the same mutation to the federated source.
    WriteBack,
    /// Writes go simultaneously to both the federated source and the local Cayenne
    /// accelerator using staged append/commit/rollback semantics. Reserved for
    /// the Iceberg federated catalog cache path.
    DualWrite {
        cayenne_target: Box<CayenneWriteTarget>,
        federated_provider: Arc<dyn TableProvider>,
    },
}

impl WriteMode {
    /// Returns `true` if this is the dual-write mode (Iceberg catalog cache path).
    #[must_use]
    pub fn is_dual_write(&self) -> bool {
        matches!(self, Self::DualWrite { .. })
    }

    /// Resolves the dual-write mode from the accelerator and federated table.
    ///
    /// Dual-write requires:
    /// 1. A Cayenne-backed accelerator (staged append/commit/rollback).
    /// 2. An immediately available federated table provider.
    pub(crate) fn resolve_dual_write(
        accelerator: &Arc<dyn TableProvider>,
        federated: &Arc<FederatedTable>,
    ) -> Result<Self, super::AcceleratedTableBuilderError> {
        let cayenne_target =
            dual_write::extract_cayenne_write_target(accelerator).ok_or_else(|| {
                super::AcceleratedTableBuilderError::AcceleratedTableError {
                    source: super::Error::FailedToWriteData {
                        source: DataFusionError::Execution(
                            "Dual-write acceleration currently requires the Cayenne accelerator"
                                .to_string(),
                        ),
                    },
                }
            })?;

        let federated_provider = federated.try_table_provider_sync().ok_or_else(|| {
            super::AcceleratedTableBuilderError::AcceleratedTableError {
                source: super::Error::FailedToWriteData {
                    source: DataFusionError::Execution(
                        "Dual-write acceleration requires an immediately available federated table provider"
                            .to_string(),
                    ),
                },
            }
        })?;

        Ok(Self::DualWrite {
            cayenne_target: Box::new(cayenne_target),
            federated_provider,
        })
    }
}
