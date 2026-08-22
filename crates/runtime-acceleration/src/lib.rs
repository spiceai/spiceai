/*
Copyright 2025 The Spice.ai OSS Authors
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
use snafu::Snafu;

pub mod acceleration;
pub mod acceleration_source;
pub mod dataset_checkpoint;
mod engine;
pub mod layout;
pub mod memory_budget;
pub mod schema_change;
pub mod sidecar;
pub mod snapshot;
// Test-only; behind a feature so it never reaches a shipped build.
#[cfg(feature = "test-support")]
pub mod testing;

pub use acceleration::Acceleration;
pub use acceleration::ParseError as AccelerationParseError;
pub use acceleration_source::AccelerationSource;
pub use engine::Engine;
pub use schema_change::OnSchemaChange;
pub use sidecar::{AcceleratorSidecar, OpenOption};
pub use snapshot::SnapshotDownloadInfo;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unknown acceleration engine '{name}'. Valid engines are: arrow, duckdb, sqlite, turso, postgres/postgresql, cayenne/vortex. Docs: https://spiceai.org/docs/components/data-accelerators"
    ))]
    AcceleratorEngineNotAvailable { name: String },
}

/// Indicates whether a data accelerator was bootstrapped (initialized from existing data)
/// during initialization, and carries any metadata from the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapStatus {
    Bootstrapped(SnapshotDownloadInfo),
    None,
}

impl BootstrapStatus {
    #[must_use]
    pub const fn bootstrapped(info: SnapshotDownloadInfo) -> Self {
        Self::Bootstrapped(info)
    }

    #[must_use]
    pub const fn none() -> Self {
        Self::None
    }

    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        matches!(self, Self::Bootstrapped { .. })
    }

    #[must_use]
    pub const fn last_updated_at(&self) -> Option<i64> {
        match self {
            Self::None => None,
            Self::Bootstrapped(info) => info.last_updated_at,
        }
    }

    /// The `snapshot_id` of the snapshot that was loaded at bootstrap, if any.
    /// `None` when no bootstrap occurred (no snapshot, or snapshots disabled).
    #[must_use]
    pub const fn loaded_snapshot_id(&self) -> Option<u64> {
        match self {
            Self::None => None,
            Self::Bootstrapped(info) => Some(info.snapshot_id),
        }
    }
}
pub mod dataupdate;
