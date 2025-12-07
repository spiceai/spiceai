/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use super::{default_true, is_default};
use crate::param::Params;
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BootstrapOnFailureBehavior {
    #[default]
    Warn,
    Retry,
    Fallback,
}

/// Datasets accelerated using a file-mode acceleration
/// engine (i.e. `Sqlite` or `DuckDB`) can bootstrap from a DB
/// file on object storage (i.e. S3) if the acceleration file
/// does not exist on startup using this configuration.
///
/// Each dataset needs to opt-in for snapshots in addition to this config.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Snapshots {
    /// Global enable/disable for dataset snapshots.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// The object store location pointing to a folder
    /// containing the dataset snapshots.
    /// i.e. `s3://my-bucket/spice/snapshots/`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,

    /// The behavior when loading a snapshot fails: 'warn', 'retry' or 'fallback'
    ///
    /// 'warn' will continue with an empty acceleration after logging a warning.
    /// `retry` will retry loading the newest snapshot indefinitely until the snapshot is loaded correctly.
    /// 'fallback' will try older snapshot files if loading newer ones fail.
    #[serde(default, skip_serializing_if = "is_default")]
    pub bootstrap_on_failure_behavior: BootstrapOnFailureBehavior,

    /// Auth params for accessing the object store location.
    /// For S3, this is the same as the S3 dataset connector params
    /// with the notable exception that `s3_auth` is set to `iam_role` by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

impl Default for Snapshots {
    fn default() -> Self {
        Self {
            enabled: true,
            location: None,
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::default(),
            params: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_default_matches_spec() {
        let snapshots = Snapshots::default();
        assert!(snapshots.enabled);
        assert_eq!(
            snapshots.bootstrap_on_failure_behavior,
            BootstrapOnFailureBehavior::Warn
        );
        assert!(snapshots.location.is_none());
        assert!(snapshots.params.is_none());
    }
}
