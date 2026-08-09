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

//! The `distributed_backend` model param's own value, kept distinct from the
//! `local_llm`-gated [`crate::chat::DistributedBackend`] (which needs the mistral.rs
//! engine) so the model parameter spec compiles unconditionally.

use std::str::FromStr;

/// Operator-selected topology for a locally served model, from the
/// `distributed_backend` model param.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistributedBackendSetting {
    /// Single-node: the default.
    #[default]
    None,
    /// Pool the model over the `nodes` list via mistral.rs's ring all-reduce
    /// (a Spice enterprise feature; standard builds are single-node only).
    Ring,
}

impl DistributedBackendSetting {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["none", "ring"];
}

impl FromStr for DistributedBackendSetting {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // An empty value means the operator wrote the key but left it blank; treat that
        // as unset rather than as a typo, matching the other model params.
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "none" => Ok(Self::None),
            "ring" => Ok(Self::Ring),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_documented_values_case_insensitively() {
        for (raw, expected) in [
            ("none", DistributedBackendSetting::None),
            ("None", DistributedBackendSetting::None),
            // Blank is unset, not invalid.
            ("", DistributedBackendSetting::None),
            ("ring", DistributedBackendSetting::Ring),
            ("Ring", DistributedBackendSetting::Ring),
        ] {
            assert_eq!(
                raw.parse::<DistributedBackendSetting>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn rejects_values_outside_the_spec() {
        for raw in ["nccl", "true", "false", "1", "0"] {
            assert!(
                raw.parse::<DistributedBackendSetting>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    /// The parameter spec advertises `VALUES`, so every entry has to parse — otherwise
    /// config validation accepts a value the parser then rejects.
    #[test]
    fn every_advertised_value_parses() {
        for value in DistributedBackendSetting::VALUES {
            value
                .parse::<DistributedBackendSetting>()
                .unwrap_or_else(|e| panic!("{value:?} is advertised but does not parse: {e}"));
        }
    }
}
