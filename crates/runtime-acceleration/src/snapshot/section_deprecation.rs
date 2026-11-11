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

//! The startup warning for the deprecated top-level `snapshots` section. It shows
//! the configuration that keeps every snapshot where it is, and names parameter
//! keys only: their values can hold secrets.

use spicepod::component::runtime::{
    RuntimeState, runtime_state_folder, runtime_state_location_holding, same_folder,
};
use spicepod::component::snapshot::{
    BootstrapOnFailureBehavior, SNAPSHOTS_STATE_FOLDER, Snapshots,
};
use std::fmt::Write;

use super::SNAPSHOTS_DOCS;

/// The warning for a spicepod with the top-level `snapshots` section `section`
/// and the `runtime.state` section `state`.
#[must_use]
pub fn section_deprecation_warning(section: &Snapshots, state: Option<&RuntimeState>) -> String {
    let mut keys: Vec<&str> = section
        .params
        .iter()
        .flat_map(|params| params.data.keys())
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let keys = keys.join(", ");
    let move_keys = |to: &str| {
        if keys.is_empty() {
            String::new()
        } else {
            format!(" Move {keys} from `snapshots.params` into {to}.")
        }
    };

    let remedy = match (section.location.as_deref(), state) {
        (None, Some(_)) => Remedy::AlreadyThere,
        (None, None) => Remedy::SetRuntimeState,
        (Some(location), Some(state))
            if same_folder(
                location,
                &runtime_state_folder(&state.location, SNAPSHOTS_STATE_FOLDER),
            ) =>
        {
            Remedy::AlreadyThere
        }
        (Some(location), None) => runtime_state_location_holding(location, SNAPSHOTS_STATE_FOLDER)
            .map_or(Remedy::Datasets(location), Remedy::RuntimeState),
        (Some(location), Some(_)) => Remedy::Datasets(location),
    };

    let mut warning = String::from(
        "The top-level `snapshots` section is deprecated and will be removed in a future release. Snapshots are stored in the `snapshots/` folder of `runtime.state.location`.",
    );
    match remedy {
        // The section's `params`, even empty or absent beside its own location,
        // replace `runtime.state.params` for the snapshots, so they move to the
        // datasets.
        Remedy::AlreadyThere
            if (section.params.is_some() || section.location.is_some())
                && state.is_some_and(|state| state.params.is_some()) =>
        {
            warning.push_str(" Your snapshots are already there: delete the section.");
            if keys.is_empty() {
                warning.push_str(
                    " Set `acceleration.snapshots_params: {}` on each dataset that enables snapshots and sets neither `snapshots_location` nor `snapshots_params`, so `runtime.state.params` do not apply to its snapshots.",
                );
            } else {
                warning.push_str(&move_keys(
                    "`acceleration.snapshots_params` on each dataset that enables snapshots and sets neither `snapshots_location` nor `snapshots_params`",
                ));
            }
        }
        Remedy::AlreadyThere => {
            warning.push_str(" Your snapshots are already there: delete the section.");
            warning.push_str(&move_keys("`runtime.state.params`"));
        }
        Remedy::SetRuntimeState => {
            warning.push_str(" Delete the section and set `runtime.state.location`.");
            warning.push_str(&move_keys("`runtime.state.params`"));
        }
        Remedy::RuntimeState(state_location) => {
            let _ = write!(
                warning,
                " To keep your snapshots where they are, delete the section and set `runtime.state.location: {state_location}`."
            );
            warning.push_str(&move_keys("`runtime.state.params`"));
        }
        Remedy::Datasets(location) => {
            let _ = write!(
                warning,
                " To keep your snapshots where they are, delete the section and set `acceleration.snapshots_location: {location}` on each dataset that enables snapshots and does not set it."
            );
            warning.push_str(&move_keys(
                "`acceleration.snapshots_params` on each of those datasets that does not set it",
            ));
        }
    }
    if section.bootstrap_on_failure_behavior != BootstrapOnFailureBehavior::default() {
        let _ = write!(
            warning,
            " Replace `snapshots.bootstrap_on_failure_behavior` with `acceleration.snapshots_bootstrap_on_failure_behavior: {}` on each dataset that enables snapshots and does not set it.",
            behavior_name(section.bootstrap_on_failure_behavior)
        );
    }
    if !section.enabled {
        warning.push_str(
            " `snapshots.enabled: false` has no replacement: set `snapshots: disabled`, the default, on each dataset instead.",
        );
    }
    warning.push_str(" See: ");
    warning.push_str(SNAPSHOTS_DOCS);
    warning
}

/// The configuration that keeps the section's snapshots where they are.
enum Remedy<'a> {
    /// The section's location is the `runtime.state` location's `snapshots/` folder.
    AlreadyThere,
    /// Neither the section nor `runtime.state` sets a location.
    SetRuntimeState,
    /// `runtime.state.location` set to this location, whose `snapshots/` folder is
    /// the section's location.
    RuntimeState(String),
    /// `acceleration.snapshots_location` set to this location on each dataset.
    Datasets(&'a str),
}

fn behavior_name(behavior: BootstrapOnFailureBehavior) -> &'static str {
    match behavior {
        BootstrapOnFailureBehavior::Warn => "warn",
        BootstrapOnFailureBehavior::Retry => "retry",
        BootstrapOnFailureBehavior::Fallback => "fallback",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spicepod::param::Params;

    const SECRET: &str = "AKIA-SECRET-VALUE";

    fn params() -> Params {
        Params::from_string_map(
            [
                ("s3_region".to_string(), "us-east-1".to_string()),
                ("s3_auth".to_string(), "key".to_string()),
                ("s3_key".to_string(), SECRET.to_string()),
            ]
            .into_iter()
            .collect(),
        )
    }

    fn section(location: Option<&str>, params: Option<Params>) -> Snapshots {
        Snapshots {
            location: location.map(ToString::to_string),
            params,
            ..Snapshots::default()
        }
    }

    fn state(location: &str) -> RuntimeState {
        RuntimeState {
            location: location.to_string(),
            params: None,
        }
    }

    fn assert_safe(warning: &str) {
        assert!(!warning.contains(SECRET), "a param value leaked: {warning}");
        assert!(
            !warning.contains("us-east-1"),
            "a param value leaked: {warning}"
        );
        assert!(
            warning.starts_with("The top-level `snapshots` section is deprecated"),
            "{warning}"
        );
        assert!(
            warning.ends_with(&format!(". See: {SNAPSHOTS_DOCS}")),
            "{warning}"
        );
        assert!(!warning.contains('\n'), "one log line: {warning}");
    }

    #[test]
    fn a_snapshots_folder_without_runtime_state_becomes_runtime_state() {
        let warning = section_deprecation_warning(
            &section(Some("s3://my-bucket/spice/snapshots/"), Some(params())),
            None,
        );
        assert_safe(&warning);
        assert_eq!(
            warning,
            format!(
                "The top-level `snapshots` section is deprecated and will be removed in a future release. Snapshots are stored in the `snapshots/` folder of `runtime.state.location`. To keep your snapshots where they are, delete the section and set `runtime.state.location: s3://my-bucket/spice/`. Move s3_auth, s3_key, s3_region from `snapshots.params` into `runtime.state.params`. See: {SNAPSHOTS_DOCS}"
            )
        );

        for (location, state_location) in [
            ("s3://my-bucket/snapshots", "s3://my-bucket/"),
            ("file:///var/spice/snapshots/", "file:///var/spice/"),
        ] {
            let warning = section_deprecation_warning(&section(Some(location), None), None);
            assert_safe(&warning);
            assert!(
                warning.contains(&format!(
                    " set `runtime.state.location: {state_location}`. See:"
                )),
                "{warning}"
            );
        }
    }

    #[test]
    fn the_runtime_state_snapshots_folder_needs_only_the_section_deleted() {
        for location in [
            "s3://my-bucket/spice/snapshots/",
            "s3://my-bucket/spice/snapshots",
        ] {
            let warning = section_deprecation_warning(
                &section(Some(location), Some(params())),
                Some(&state("s3://my-bucket/spice")),
            );
            assert_safe(&warning);
            assert!(
                warning.contains(" Your snapshots are already there: delete the section. Move s3_auth, s3_key, s3_region from `snapshots.params` into `runtime.state.params`. See:"),
                "{warning}"
            );
        }

        let without_location = section_deprecation_warning(
            &section(None, None),
            Some(&state("s3://my-bucket/spice/")),
        );
        assert!(
            without_location
                .contains(" Your snapshots are already there: delete the section. See:"),
            "{without_location}"
        );
    }

    /// The section's `params`, even empty or absent beside its own location,
    /// replace `runtime.state.params` for the snapshots, so deleting the section
    /// must not leave them to apply.
    #[test]
    fn section_params_beside_runtime_state_params_move_to_each_dataset() {
        let state = RuntimeState {
            params: Some(Params::from_string_map(
                [(
                    "s3_endpoint".to_string(),
                    "https://state.example".to_string(),
                )]
                .into_iter()
                .collect(),
            )),
            ..state("s3://my-bucket/spice/")
        };

        let warning = section_deprecation_warning(&section(None, Some(params())), Some(&state));
        assert_safe(&warning);
        assert!(
            warning.contains(" Your snapshots are already there: delete the section. Move s3_auth, s3_key, s3_region from `snapshots.params` into `acceleration.snapshots_params` on each dataset that enables snapshots and sets neither `snapshots_location` nor `snapshots_params`. See:"),
            "{warning}"
        );

        let empty =
            section_deprecation_warning(&section(None, Some(Params::default())), Some(&state));
        assert_safe(&empty);
        assert!(
            empty.contains(" Your snapshots are already there: delete the section. Set `acceleration.snapshots_params: {}` on each dataset that enables snapshots and sets neither `snapshots_location` nor `snapshots_params`, so `runtime.state.params` do not apply to its snapshots. See:"),
            "{empty}"
        );
        assert!(!empty.contains("https://state.example"), "{empty}");

        let location_alone = section_deprecation_warning(
            &section(Some("s3://my-bucket/spice/snapshots/"), None),
            Some(&state),
        );
        assert_safe(&location_alone);
        assert!(
            location_alone.contains(" Set `acceleration.snapshots_params: {}` on each dataset that enables snapshots and sets neither `snapshots_location` nor `snapshots_params`, so `runtime.state.params` do not apply to its snapshots. See:"),
            "{location_alone}"
        );
    }

    #[test]
    fn any_other_location_moves_to_each_dataset() {
        for (location, state) in [
            ("s3://archive-bucket/snaps/", None),
            (
                "s3://archive-bucket/snapshots/",
                Some(state("s3://my-bucket/spice/")),
            ),
        ] {
            let warning = section_deprecation_warning(
                &section(Some(location), Some(params())),
                state.as_ref(),
            );
            assert_safe(&warning);
            assert!(
                warning.contains(&format!(" To keep your snapshots where they are, delete the section and set `acceleration.snapshots_location: {location}` on each dataset that enables snapshots and does not set it. Move s3_auth, s3_key, s3_region from `snapshots.params` into `acceleration.snapshots_params` on each of those datasets that does not set it. See:")),
                "{warning}"
            );
        }

        let without_params =
            section_deprecation_warning(&section(Some("s3://archive-bucket/snaps/"), None), None);
        assert_safe(&without_params);
        assert!(
            without_params.contains(
                "`acceleration.snapshots_location: s3://archive-bucket/snaps/` on each dataset that enables snapshots and does not set it. See:"
            ),
            "no params to move: {without_params}"
        );
    }

    #[test]
    fn bootstrap_behavior_and_enabled_false_name_their_replacements() {
        let fallback = Snapshots {
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Fallback,
            ..section(Some("s3://archive-bucket/snaps/"), None)
        };
        let warning = section_deprecation_warning(&fallback, None);
        assert_safe(&warning);
        assert!(
            warning.contains(
                " on each dataset that enables snapshots and does not set it. Replace `snapshots.bootstrap_on_failure_behavior` with `acceleration.snapshots_bootstrap_on_failure_behavior: fallback` on each dataset that enables snapshots and does not set it. See:"
            ),
            "{warning}"
        );

        let retry = Snapshots {
            bootstrap_on_failure_behavior: BootstrapOnFailureBehavior::Retry,
            enabled: false,
            ..section(None, None)
        };
        let warning = section_deprecation_warning(&retry, Some(&state("s3://my-bucket/spice/")));
        assert_safe(&warning);
        assert!(
            warning.contains(" Replace `snapshots.bootstrap_on_failure_behavior` with `acceleration.snapshots_bootstrap_on_failure_behavior: retry` on each dataset that enables snapshots and does not set it. `snapshots.enabled: false` has no replacement: set `snapshots: disabled`, the default, on each dataset instead. See:"),
            "{warning}"
        );
    }

    #[test]
    fn no_location_and_no_runtime_state_asks_for_runtime_state() {
        let warning = section_deprecation_warning(&section(None, Some(params())), None);
        assert_safe(&warning);
        assert!(
            warning.contains(" Delete the section and set `runtime.state.location`. Move s3_auth, s3_key, s3_region from `snapshots.params` into `runtime.state.params`. See:"),
            "{warning}"
        );
    }
}
