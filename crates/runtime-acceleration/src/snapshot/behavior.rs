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

use std::sync::Arc;

use spicepod::acceleration as spicepod_acceleration;
use spicepod::component::snapshot::Snapshots;

#[cfg(feature = "snapshots")]
const SNAPSHOTS_ENABLED: bool = true;
#[cfg(not(feature = "snapshots"))]
const SNAPSHOTS_ENABLED: bool = false;

/// The behavior of snapshots for individual accelerated datasets.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum SnapshotBehavior {
    /// Snapshots are disabled (default).
    #[default]
    Disabled,
    /// Enable both creating and bootstrapping from snapshots.
    Enabled(Arc<Snapshots>),
    /// Only bootstrap from existing snapshots, don't attempt to create new ones.
    BootstrapOnly(Arc<Snapshots>),
    /// Only create new snapshots.
    CreateOnly(Arc<Snapshots>),
}

impl SnapshotBehavior {
    #[must_use]
    pub fn disabled() -> Self {
        SnapshotBehavior::Disabled
    }

    #[must_use]
    pub fn enabled(snapshots: Arc<Snapshots>) -> Self {
        // Snapshot support must be compiled in for bootstrapping to be possible.
        if !SNAPSHOTS_ENABLED {
            tracing::trace!(
                "Snapshot bootstrapping is not enabled because snapshot support is not compiled in."
            );
            return SnapshotBehavior::Disabled;
        }

        if !snapshots.enabled {
            return SnapshotBehavior::Disabled;
        }

        SnapshotBehavior::Enabled(snapshots)
    }

    #[must_use]
    pub fn bootstrap_only(snapshots: Arc<Snapshots>) -> Self {
        // Snapshot support must be compiled in for bootstrapping to be possible.
        if !SNAPSHOTS_ENABLED {
            tracing::trace!(
                "Snapshot bootstrapping is not enabled because snapshot support is not compiled in."
            );
            return SnapshotBehavior::Disabled;
        }

        if !snapshots.enabled {
            return SnapshotBehavior::Disabled;
        }

        SnapshotBehavior::BootstrapOnly(snapshots)
    }

    #[must_use]
    pub fn create_only(snapshots: Arc<Snapshots>) -> Self {
        // Snapshot support must be compiled in for snapshot creation to be possible.
        if !SNAPSHOTS_ENABLED {
            tracing::trace!(
                "Snapshot bootstrapping is not enabled because snapshot support is not compiled in."
            );
            return SnapshotBehavior::Disabled;
        }

        if !snapshots.enabled {
            return SnapshotBehavior::Disabled;
        }

        SnapshotBehavior::CreateOnly(snapshots)
    }

    #[must_use]
    pub fn bootstrap_enabled(&self) -> bool {
        matches!(
            self,
            SnapshotBehavior::Enabled(_) | SnapshotBehavior::BootstrapOnly(_)
        )
    }

    #[must_use]
    pub fn create_enabled(&self) -> bool {
        matches!(
            self,
            SnapshotBehavior::Enabled(_) | SnapshotBehavior::CreateOnly(_)
        )
    }

    #[must_use]
    pub fn from(
        snapshots: Option<Arc<Snapshots>>,
        snapshot_behavior: spicepod_acceleration::SnapshotBehavior,
    ) -> Self {
        let Some(snapshots) = snapshots else {
            return SnapshotBehavior::Disabled;
        };

        match snapshot_behavior {
            spicepod_acceleration::SnapshotBehavior::Disabled => SnapshotBehavior::Disabled,
            spicepod_acceleration::SnapshotBehavior::Enabled => {
                SnapshotBehavior::Enabled(snapshots)
            }
            spicepod_acceleration::SnapshotBehavior::BootstrapOnly => {
                SnapshotBehavior::BootstrapOnly(snapshots)
            }
            spicepod_acceleration::SnapshotBehavior::CreateOnly => {
                SnapshotBehavior::CreateOnly(snapshots)
            }
        }
    }
}
