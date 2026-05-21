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
//! per-mode executors in the [`write_back`] and [`write_through`] submodules.
//!
//! The currently supported modes are:
//!
//! - [`WriteMode::FederatedOnly`]: writes go to the federated source only;
//!   the accelerator is updated through the normal refresh cycle. Default.
//! - [`WriteMode::AcceleratorOnly`]: writes go only to the local accelerator
//!   (used when `on_conflict` upserts into the accelerator).
//! - [`WriteMode::WriteBack`]: writes commit to the local accelerator first,
//!   then asynchronously persist the same mutation to the federated source.
//!   Source persistence failures are logged rather than returned to the caller,
//!   and `replication.enabled` is required as the caller's opt-in to those
//!   asynchronous durability semantics.
//! - [`WriteMode::WriteThrough`]: writes go to both the Cayenne accelerator
//!   and the federated source simultaneously with staged commit/rollback.
//!
//! [`AcceleratedTable`]: super::AcceleratedTable

pub(crate) mod write_back;
pub(crate) mod write_through;

use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;

pub(crate) use write_through::CayenneWriteTarget;

use crate::federated_table::FederatedTable;

/// Controls where writes (INSERT INTO) are directed for an `AcceleratedTable`.
#[derive(Debug, Clone)]
pub(crate) enum WriteMode {
    /// Writes go to the federated source only. The acceleration refresh mechanism
    /// picks up new data on its next cycle. This is the default.
    FederatedOnly,
    /// Writes go only to the local accelerator (not replicated to the source).
    /// Used when `on_conflict` is configured or for internal tables.
    AcceleratorOnly,
    /// Writes commit to the local accelerator first, then asynchronously persist
    /// the same mutation to the federated source.
    WriteBack,
    /// Writes go simultaneously to both the federated source and the local Cayenne
    /// accelerator using staged append/commit/rollback semantics.
    WriteThrough {
        cayenne_target: Box<CayenneWriteTarget>,
        federated_provider: Arc<dyn TableProvider>,
    },
}

impl WriteMode {
    /// Returns `true` if this is a write-through mode.
    #[must_use]
    pub fn is_write_through(&self) -> bool {
        matches!(self, Self::WriteThrough { .. })
    }

    /// Resolves a write-through mode from the accelerator and federated table.
    ///
    /// Write-through requires:
    /// 1. A Cayenne-backed accelerator (staged append/commit/rollback).
    /// 2. An immediately available federated table provider.
    pub(crate) fn resolve_write_through(
        accelerator: &Arc<dyn TableProvider>,
        federated: &Arc<FederatedTable>,
    ) -> Result<Self, super::AcceleratedTableBuilderError> {
        let cayenne_target =
            write_through::extract_cayenne_write_target(accelerator).ok_or_else(|| {
                super::AcceleratedTableBuilderError::AcceleratedTableError {
                    source: super::Error::FailedToWriteData {
                        source: DataFusionError::Execution(
                            "Write-through acceleration currently requires the Cayenne accelerator"
                                .to_string(),
                        ),
                    },
                }
            })?;

        let federated_provider = federated.try_table_provider_sync().ok_or_else(|| {
            super::AcceleratedTableBuilderError::AcceleratedTableError {
                source: super::Error::FailedToWriteData {
                    source: DataFusionError::Execution(
                        "Write-through acceleration requires an immediately available federated table provider"
                            .to_string(),
                    ),
                },
            }
        })?;

        Ok(Self::WriteThrough {
            cayenne_target: Box::new(cayenne_target),
            federated_provider,
        })
    }
}
