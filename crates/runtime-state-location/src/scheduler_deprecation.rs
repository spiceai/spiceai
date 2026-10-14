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

//! The startup warning for the deprecated `runtime.scheduler.state_location` and
//! `runtime.scheduler.params`. It shows the `runtime.state` configuration that
//! keeps the scheduler's objects where they are, or says that the scheduler
//! starts empty when none does. It names parameter keys only: their values can
//! hold secrets.

use std::fmt::Write;

use spicepod::component::runtime::{
    Runtime, SCHEDULER_STATE_FOLDER, runtime_state_folder, runtime_state_location_holding,
    same_folder,
};
use url::Url;

use crate::StateFeature;

const SCHEDULER_DOCS: &str = "https://spiceai.org/docs/reference/spicepod/runtime#runtimescheduler";

/// The configuration that moves the scheduler's state to `runtime.state`.
enum Remedy {
    /// `state_location` is the `runtime.state` location's `cluster/` folder.
    AlreadyThere,
    /// `runtime.state.location` set to this location, whose `cluster/` folder is
    /// `state_location`.
    RuntimeState(String),
    /// No `runtime.state` location holds the objects: the scheduler starts empty.
    StartEmpty,
}

/// The warning for `runtime`'s deprecated scheduler settings, or `None` when it
/// sets neither `runtime.scheduler.state_location` nor `runtime.scheduler.params`.
#[must_use]
pub fn scheduler_deprecation_warning(runtime: &Runtime) -> Option<String> {
    let scheduler = runtime.scheduler.as_ref()?;
    let location = scheduler.state_location.as_deref();
    let params = scheduler.params.as_ref();
    let settings = match (location, params) {
        (None, None) => return None,
        (Some(_), Some(_)) => {
            "`runtime.scheduler.state_location` and `runtime.scheduler.params` are"
        }
        (Some(_), None) => "`runtime.scheduler.state_location` is",
        (None, Some(_)) => "`runtime.scheduler.params` is",
    };
    let mut keys: Vec<&str> = params
        .iter()
        .flat_map(|params| params.data.keys())
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let keys = keys.join(", ");
    let state = runtime.state.as_ref();

    let mut warning = format!(
        "{settings} deprecated and will be removed in a future release. The scheduler keeps its state in the `cluster/` folder of `runtime.state.location`."
    );
    let mut keys_moved = false;
    if let Some(location) = location {
        // The scheduler's own setting keeps `abfs://` and `abfss://` state at the
        // container root, which no `runtime.state` folder is.
        let root_only = Url::parse(location)
            .is_ok_and(|url| matches!(url.scheme(), "abfs" | "abfss"))
            && !StateFeature::Scheduler.keeps_abfs_path_in_own_setting();
        let remedy = match state {
            _ if root_only => Remedy::StartEmpty,
            Some(state)
                if same_folder(
                    location,
                    &runtime_state_folder(&state.location, SCHEDULER_STATE_FOLDER),
                ) =>
            {
                Remedy::AlreadyThere
            }
            Some(_) => Remedy::StartEmpty,
            None => runtime_state_location_holding(location, SCHEDULER_STATE_FOLDER)
                .map_or(Remedy::StartEmpty, Remedy::RuntimeState),
        };
        match remedy {
            Remedy::AlreadyThere => warning.push_str(
                " Its state is already there: remove `runtime.scheduler.state_location`.",
            ),
            Remedy::RuntimeState(state_location) => {
                let _ = write!(
                    warning,
                    " To keep its state where it is, replace `runtime.scheduler.state_location` with:\n  runtime:\n    state:\n      location: {state_location}"
                );
                if !keys.is_empty() {
                    let _ = write!(
                        warning,
                        "\n      params: # move {keys} from runtime.scheduler.params"
                    );
                    keys_moved = true;
                }
            }
            Remedy::StartEmpty => {
                let set_state = if state.is_some() {
                    ""
                } else {
                    "set `runtime.state.location` and "
                };
                let folder = state.map_or_else(
                    || "there".to_string(),
                    |state| {
                        format!(
                            "in `{}`",
                            runtime_state_folder(&state.location, SCHEDULER_STATE_FOLDER)
                        )
                    },
                );
                let root = if root_only {
                    " (`runtime.scheduler.state_location` keeps it at the container root)"
                } else {
                    ""
                };
                let _ = write!(
                    warning,
                    " No `runtime.state.location` holds its current state{root}. To move, {set_state}remove `runtime.scheduler.state_location`: the scheduler then starts empty {folder}, so partition assignments are rebuilt and async query jobs are lost."
                );
            }
        }
    }
    if !keys_moved && params.is_some() {
        if keys.is_empty() {
            warning.push_str(" Remove `runtime.scheduler.params`.");
        } else {
            let _ = write!(
                warning,
                " Move {keys} from `runtime.scheduler.params` into `runtime.state.params`."
            );
        }
    }
    let _ = write!(warning, "\nSee: {SCHEDULER_DOCS}");
    Some(warning)
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::component::runtime::{RuntimeState, Scheduler};
    use spicepod::param::Params;

    const SECRET: &str = "AKIA-SECRET-VALUE";

    fn params() -> Params {
        Params::from_string_map(
            [
                ("s3_region".to_string(), "us-east-1".to_string()),
                ("s3_key".to_string(), SECRET.to_string()),
            ]
            .into_iter()
            .collect(),
        )
    }

    fn runtime(
        state_location: Option<&str>,
        params: Option<Params>,
        state: Option<&str>,
    ) -> Runtime {
        Runtime {
            scheduler: Some(Scheduler {
                state_location: state_location.map(ToString::to_string),
                params,
                ..Scheduler::default()
            }),
            state: state.map(|location| RuntimeState {
                location: location.to_string(),
                params: None,
            }),
            ..Runtime::default()
        }
    }

    fn checked_warning(runtime: &Runtime) -> String {
        let warning = scheduler_deprecation_warning(runtime).expect("a deprecated setting is set");
        assert!(!warning.contains(SECRET), "a param value leaked: {warning}");
        assert!(
            !warning.contains("us-east-1"),
            "a param value leaked: {warning}"
        );
        assert!(
            warning.ends_with(&format!("\nSee: {SCHEDULER_DOCS}")),
            "{warning}"
        );
        warning
    }

    #[test]
    fn nothing_deprecated_is_no_warning() {
        assert_eq!(scheduler_deprecation_warning(&Runtime::default()), None);
        assert_eq!(
            scheduler_deprecation_warning(&runtime(None, None, Some("s3://b/spice/"))),
            None,
            "the other `runtime.scheduler` fields are not deprecated"
        );
    }

    #[test]
    fn the_runtime_state_cluster_folder_needs_only_the_setting_removed() {
        for location in ["s3://b/spice/cluster/", "s3://b/spice/cluster"] {
            let warning = checked_warning(&runtime(
                Some(location),
                Some(params()),
                Some("s3://b/spice"),
            ));
            assert_eq!(
                warning,
                format!(
                    "`runtime.scheduler.state_location` and `runtime.scheduler.params` are deprecated and will be removed in a future release. The scheduler keeps its state in the `cluster/` folder of `runtime.state.location`. Its state is already there: remove `runtime.scheduler.state_location`. Move s3_key, s3_region from `runtime.scheduler.params` into `runtime.state.params`.\nSee: {SCHEDULER_DOCS}"
                )
            );
        }
    }

    #[test]
    fn a_cluster_folder_without_runtime_state_becomes_runtime_state() {
        let warning = checked_warning(&runtime(
            Some("s3://b/spice/cluster/"),
            Some(params()),
            None,
        ));
        assert_eq!(
            warning,
            format!(
                "`runtime.scheduler.state_location` and `runtime.scheduler.params` are deprecated and will be removed in a future release. The scheduler keeps its state in the `cluster/` folder of `runtime.state.location`. To keep its state where it is, replace `runtime.scheduler.state_location` with:\n  runtime:\n    state:\n      location: s3://b/spice/\n      params: # move s3_key, s3_region from runtime.scheduler.params\nSee: {SCHEDULER_DOCS}"
            )
        );

        let without_params =
            checked_warning(&runtime(Some("file:///var/spice/cluster"), None, None));
        assert!(
            without_params.starts_with("`runtime.scheduler.state_location` is deprecated"),
            "{without_params}"
        );
        assert!(
            without_params.contains("      location: file:///var/spice/\nSee:"),
            "{without_params}"
        );
    }

    #[test]
    fn any_other_location_starts_empty_without_copying() {
        for (location, state, folder) in [
            (
                "s3://b/scheduler/",
                Some("s3://b/spice/"),
                " the scheduler then starts empty in `s3://b/spice/cluster/`,",
            ),
            (
                "s3://b/spice/cluster/",
                Some("s3://other/spice/"),
                " the scheduler then starts empty in `s3://other/spice/cluster/`,",
            ),
            (
                "s3://b/scheduler/",
                None,
                " the scheduler then starts empty there,",
            ),
        ] {
            let warning = checked_warning(&runtime(Some(location), None, state));
            assert!(
                warning.contains(" No `runtime.state.location` holds its current state. To move, "),
                "{warning}"
            );
            assert!(warning.contains(folder), "{warning}");
            assert!(
                warning.contains(
                    "so partition assignments are rebuilt and async query jobs are lost."
                ),
                "{warning}"
            );
            assert_eq!(
                warning.contains("set `runtime.state.location` and remove"),
                state.is_none(),
                "{warning}"
            );
            assert!(!warning.contains("copy"), "{warning}");
        }
    }

    /// The scheduler's own `abfs://` or `abfss://` setting keeps its state at the
    /// container root, so even a `cluster/` path is not where `runtime.state` puts it.
    #[test]
    fn abfs_always_starts_empty() {
        let location = "abfss://container@account.dfs.core.windows.net/spice/cluster/";
        for state in [
            Some("abfss://container@account.dfs.core.windows.net/spice/"),
            None,
        ] {
            let warning = checked_warning(&runtime(Some(location), Some(params()), state));
            assert!(
                warning.contains(" No `runtime.state.location` holds its current state (`runtime.scheduler.state_location` keeps it at the container root). To move, "),
                "{warning}"
            );
            assert!(!warning.contains("already there"), "{warning}");
            assert!(!warning.contains("    state:\n"), "{warning}");
            assert!(
                warning.contains(
                    " Move s3_key, s3_region from `runtime.scheduler.params` into `runtime.state.params`."
                ),
                "{warning}"
            );
        }
    }

    #[test]
    fn params_alone_move_to_runtime_state_params() {
        let warning = checked_warning(&runtime(None, Some(params()), Some("s3://b/spice/")));
        assert_eq!(
            warning,
            format!(
                "`runtime.scheduler.params` is deprecated and will be removed in a future release. The scheduler keeps its state in the `cluster/` folder of `runtime.state.location`. Move s3_key, s3_region from `runtime.scheduler.params` into `runtime.state.params`.\nSee: {SCHEDULER_DOCS}"
            )
        );

        let empty = checked_warning(&runtime(
            None,
            Some(Params::from_string_map(std::collections::HashMap::new())),
            None,
        ));
        assert!(
            empty.contains(" Remove `runtime.scheduler.params`.\nSee:"),
            "{empty}"
        );
    }
}
