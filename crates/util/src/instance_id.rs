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

//! The identity that names this instance to every feature that coordinates
//! with other instances through shared state.
//!
//! The identity is stable across restarts, so it tells instances apart but not
//! two runs of one instance. [`process_id`] tells runs apart: a feature that
//! records which process holds shared state pairs the identity with it.

use std::sync::LazyLock;

/// The environment variable that sets the instance identity.
pub const SPICE_INSTANCE_ID: &str = "SPICE_INSTANCE_ID";

/// The environment variables read for the identity, in order of precedence.
const IDENTITY_VARIABLES: [&str; 3] = [SPICE_INSTANCE_ID, "HOSTNAME", "COMPUTERNAME"];

static INSTANCE_ID: LazyLock<String> = LazyLock::new(|| {
    resolve(
        |name| std::env::var(name).ok(),
        || gethostname::gethostname().to_string_lossy().into_owned(),
    )
});

/// This instance's identity, resolved once per process: `SPICE_INSTANCE_ID`,
/// else `HOSTNAME`, else `COMPUTERNAME`, else the operating system's host name.
/// An empty variable counts as unset.
#[must_use]
pub fn instance_id() -> &'static str {
    &INSTANCE_ID
}

static PROCESS_ID: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::now_v7);

/// This process's id: a UUID created once per process, so two runs of one
/// instance never share it.
#[must_use]
pub fn process_id() -> uuid::Uuid {
    *PROCESS_ID
}

/// Resolves the identity from `env`, which returns a variable's value, falling
/// back to `host_name` when no variable gives one.
#[must_use]
pub fn resolve(env: impl Fn(&str) -> Option<String>, host_name: impl FnOnce() -> String) -> String {
    IDENTITY_VARIABLES
        .into_iter()
        .find_map(|name| env(name).filter(|value| !value.is_empty()))
        .unwrap_or_else(host_name)
}

/// The error a feature logs when it finds another running process using this
/// instance's identity at the shared state `location`.
#[must_use]
pub fn duplicate_instance_message(instance_id: &str, location: &str) -> String {
    format!(
        "Another running instance uses {SPICE_INSTANCE_ID} '{instance_id}' at state location '{location}'. Give each instance its own {SPICE_INSTANCE_ID} (for example the pod name)."
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn resolve_with(vars: &[(&str, &str)]) -> String {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect();
        resolve(|name| vars.get(name).cloned(), || "os-host".to_string())
    }

    #[test]
    fn spice_instance_id_comes_first() {
        assert_eq!(
            resolve_with(&[
                ("SPICE_INSTANCE_ID", "spice-0"),
                ("HOSTNAME", "pod-1"),
                ("COMPUTERNAME", "WIN-1"),
            ]),
            "spice-0"
        );
    }

    #[test]
    fn hostname_is_next() {
        assert_eq!(
            resolve_with(&[("HOSTNAME", "pod-1"), ("COMPUTERNAME", "WIN-1")]),
            "pod-1"
        );
    }

    #[test]
    fn computername_is_after_hostname() {
        assert_eq!(resolve_with(&[("COMPUTERNAME", "WIN-1")]), "WIN-1");
    }

    #[test]
    fn the_host_name_is_last() {
        assert_eq!(resolve_with(&[]), "os-host");
    }

    #[test]
    fn an_empty_variable_counts_as_unset() {
        assert_eq!(
            resolve_with(&[("SPICE_INSTANCE_ID", ""), ("HOSTNAME", "pod-1")]),
            "pod-1"
        );
        assert_eq!(
            resolve_with(&[
                ("SPICE_INSTANCE_ID", ""),
                ("HOSTNAME", ""),
                ("COMPUTERNAME", "WIN-1")
            ]),
            "WIN-1"
        );
        assert_eq!(
            resolve_with(&[
                ("SPICE_INSTANCE_ID", ""),
                ("HOSTNAME", ""),
                ("COMPUTERNAME", "")
            ]),
            "os-host"
        );
    }

    #[test]
    fn duplicate_instance_message_names_the_identity_the_location_and_the_fix() {
        assert_eq!(
            duplicate_instance_message("spice-0", "s3://my-bucket/spice/"),
            "Another running instance uses SPICE_INSTANCE_ID 'spice-0' at state location 's3://my-bucket/spice/'. Give each instance its own SPICE_INSTANCE_ID (for example the pod name)."
        );
    }
}
