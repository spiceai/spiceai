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

use super::runtime::RuntimeState;
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
    ///
    /// When not set, snapshots go to the `snapshots/` folder of
    /// `runtime.state.location`.
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
    /// When set, they replace `runtime.state.params` for a location taken from
    /// `runtime.state`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// The folder of `runtime.state.location` that holds acceleration snapshots when
/// `snapshots.location` is not set.
pub const SNAPSHOTS_STATE_FOLDER: &str = "snapshots";

impl Snapshots {
    /// The snapshot configuration for a spicepod that sets `runtime.state` and no
    /// `snapshots.location`: snapshots go to the `snapshots` folder of
    /// `runtime.state.location`. As with `runtime.scheduler.params`,
    /// `snapshots.params`, when set, replace `runtime.state.params`. Every other
    /// field comes from `snapshots`, or its default when the section is absent.
    #[must_use]
    pub fn in_runtime_state(snapshots: Option<&Snapshots>, state: &RuntimeState) -> Snapshots {
        let snapshots = snapshots.cloned().unwrap_or_default();
        let params = snapshots.params.or_else(|| state.params.clone());
        Snapshots {
            location: Some(runtime_state_folder(&state.location)),
            params,
            ..snapshots
        }
    }
}

/// `<location>/snapshots/`. A location that is not a URL is joined as text, so the
/// snapshot store still reports it as invalid.
fn runtime_state_folder(location: &str) -> String {
    let folder = format!("{SNAPSHOTS_STATE_FOLDER}/");
    match url::Url::parse(location) {
        Ok(mut url) => {
            if !url.path().ends_with('/') {
                let path = format!("{}/", url.path());
                url.set_path(&path);
            }
            url.join(&folder).map_or_else(
                |_| format!("{}/{folder}", location.trim_end_matches('/')),
                String::from,
            )
        }
        Err(_) => format!("{}/{folder}", location.trim_end_matches('/')),
    }
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

    fn runtime_state(location: &str, params: Option<&[(&str, &str)]>) -> RuntimeState {
        RuntimeState {
            location: location.to_string(),
            params: params.map(|params| {
                Params::from_string_map(
                    params
                        .iter()
                        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                        .collect(),
                )
            }),
        }
    }

    #[test]
    fn runtime_state_snapshots_go_to_the_snapshots_folder() {
        for (location, expected) in [
            ("s3://my-bucket/spice/", "s3://my-bucket/spice/snapshots/"),
            ("s3://my-bucket/spice", "s3://my-bucket/spice/snapshots/"),
            ("s3://my-bucket", "s3://my-bucket/snapshots/"),
            ("s3://my-bucket/", "s3://my-bucket/snapshots/"),
            ("file:///", "file:///snapshots/"),
            ("file:///var/spice/state", "file:///var/spice/state/snapshots/"),
            (
                "abfss://container@account.dfs.core.windows.net/spice/",
                "abfss://container@account.dfs.core.windows.net/spice/snapshots/",
            ),
            ("abfs://container", "abfs://container/snapshots/"),
            ("gs://bucket/a%20b/", "gs://bucket/a%20b/snapshots/"),
            ("not a url", "not a url/snapshots/"),
        ] {
            let snapshots = Snapshots::in_runtime_state(None, &runtime_state(location, None));
            assert_eq!(snapshots.location.as_deref(), Some(expected), "{location}");
        }
    }

    /// `snapshots.params` replace `runtime.state.params` as a whole, as
    /// `runtime.scheduler.params` do; the other fields keep their values.
    #[test]
    fn runtime_state_snapshots_take_snapshot_params_else_state_params() {
        let state = runtime_state(
            "s3://my-bucket/spice/",
            Some(&[("s3_region", "us-east-1"), ("s3_auth", "iam_role")]),
        );

        let defaults = Snapshots::in_runtime_state(None, &state);
        assert_eq!(
            defaults,
            Snapshots {
                enabled: true,
                location: Some("s3://my-bucket/spice/snapshots/".to_string()),
                bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Warn,
                params: state.params.clone(),
            }
        );

        let section = Snapshots {
            enabled: false,
            location: None,
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            params: Some(Params::from_string_map(
                [("s3_queue_url".to_string(), "https://sqs.us-east-1.amazonaws.com/1/q".to_string())]
                    .into_iter()
                    .collect(),
            )),
        };
        assert_eq!(
            Snapshots::in_runtime_state(Some(&section), &state),
            Snapshots {
                location: Some("s3://my-bucket/spice/snapshots/".to_string()),
                ..section
            }
        );
    }
}
