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

use std::sync::{Arc, Weak};

use runtime_secrets::Secrets;
use spicepod::acceleration as spicepod_acceleration;
use spicepod::component::snapshot::Snapshots;
use tokio::sync::RwLock;

#[cfg(feature = "snapshots")]
const SNAPSHOTS_ENABLED: bool = true;
#[cfg(not(feature = "snapshots"))]
const SNAPSHOTS_ENABLED: bool = false;

/// The behavior of snapshots for individual accelerated datasets.
#[derive(Debug, Clone, Default)]
pub enum SnapshotBehavior {
    /// Snapshots are disabled (default).
    #[default]
    Disabled,
    /// Enable both creating and bootstrapping from snapshots.
    Enabled(Arc<Snapshots>, Weak<RwLock<Secrets>>),
    /// Only bootstrap from existing snapshots, don't attempt to create new ones.
    BootstrapOnly(Arc<Snapshots>, Weak<RwLock<Secrets>>),
    /// Only create new snapshots.
    CreateOnly(Arc<Snapshots>, Weak<RwLock<Secrets>>),
}

impl PartialEq for SnapshotBehavior {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SnapshotBehavior::Disabled, SnapshotBehavior::Disabled) => true,
            (SnapshotBehavior::Enabled(snap1, _), SnapshotBehavior::Enabled(snap2, _))
            | (SnapshotBehavior::CreateOnly(snap1, _), SnapshotBehavior::CreateOnly(snap2, _))
            | (
                SnapshotBehavior::BootstrapOnly(snap1, _),
                SnapshotBehavior::BootstrapOnly(snap2, _),
            ) => snap1 == snap2,
            _ => false,
        }
    }
}

impl SnapshotBehavior {
    #[must_use]
    pub fn disabled() -> Self {
        SnapshotBehavior::Disabled
    }

    #[must_use]
    pub fn enabled(snapshots: Arc<Snapshots>, secrets: Weak<RwLock<Secrets>>) -> Self {
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

        SnapshotBehavior::Enabled(snapshots, secrets)
    }

    #[must_use]
    pub fn bootstrap_only(snapshots: Arc<Snapshots>, secrets: Weak<RwLock<Secrets>>) -> Self {
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

        SnapshotBehavior::BootstrapOnly(snapshots, secrets)
    }

    #[must_use]
    pub fn create_only(snapshots: Arc<Snapshots>, secrets: Weak<RwLock<Secrets>>) -> Self {
        // Snapshot support must be compiled in for snapshot creation to be possible.
        if !SNAPSHOTS_ENABLED {
            tracing::trace!(
                "Snapshot creation is not enabled because snapshot support is not compiled in."
            );
            return SnapshotBehavior::Disabled;
        }

        if !snapshots.enabled {
            return SnapshotBehavior::Disabled;
        }

        SnapshotBehavior::CreateOnly(snapshots, secrets)
    }

    #[must_use]
    pub fn bootstrap_enabled(&self) -> bool {
        matches!(
            self,
            SnapshotBehavior::Enabled(_, _) | SnapshotBehavior::BootstrapOnly(_, _)
        )
    }

    #[must_use]
    pub fn create_enabled(&self) -> bool {
        matches!(
            self,
            SnapshotBehavior::Enabled(_, _) | SnapshotBehavior::CreateOnly(_, _)
        )
    }

    #[must_use]
    pub fn from(
        snapshots: Option<Arc<Snapshots>>,
        snapshot_behavior: spicepod_acceleration::SnapshotBehavior,
        secrets: Weak<RwLock<Secrets>>,
    ) -> Self {
        // Snapshot support must be compiled in for snapshot creation to be possible.
        if !SNAPSHOTS_ENABLED {
            tracing::trace!(
                "Snapshot creation is not enabled because snapshot support is not compiled in."
            );
            return SnapshotBehavior::Disabled;
        }

        let Some(snapshots) = snapshots else {
            return SnapshotBehavior::Disabled;
        };

        match snapshot_behavior {
            spicepod_acceleration::SnapshotBehavior::Disabled => SnapshotBehavior::Disabled,
            spicepod_acceleration::SnapshotBehavior::Enabled => {
                if !snapshots.enabled {
                    tracing::warn!(
                        "Snapshots are enabled for this dataset, but the spicepod snapshot configuration is disabled."
                    );
                    return SnapshotBehavior::Disabled;
                }

                SnapshotBehavior::Enabled(snapshots, secrets)
            }
            spicepod_acceleration::SnapshotBehavior::BootstrapOnly => {
                if !snapshots.enabled {
                    tracing::warn!(
                        "Snapshots are enabled for this dataset, but the spicepod snapshot configuration is disabled."
                    );
                    return SnapshotBehavior::Disabled;
                }

                SnapshotBehavior::BootstrapOnly(snapshots, secrets)
            }
            spicepod_acceleration::SnapshotBehavior::CreateOnly => {
                if !snapshots.enabled {
                    tracing::warn!(
                        "Snapshots are enabled for this dataset, but the spicepod snapshot configuration is disabled."
                    );
                    return SnapshotBehavior::Disabled;
                }

                SnapshotBehavior::CreateOnly(snapshots, secrets)
            }
        }
    }
}
