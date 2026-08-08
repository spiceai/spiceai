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

//! Whether a locally served model may load Python-pickle weight files
//! (`.pt`/`.pth`/`.ckpt`/`.bin`), which execute arbitrary code on load.

use std::str::FromStr;

/// Operator opt-in for loading pickle-format weights, from the `trust_pickle`
/// model param. Pickle deserialization is RCE by design, so the default rejects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PickleTrust {
    /// Reject pickle-format weights before the model loader touches them.
    #[default]
    Untrusted,
    /// Allow pickle-format weights. Only for weights from a fully trusted source.
    Trusted,
}

impl PickleTrust {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["true", "false"];

    #[must_use]
    pub fn is_trusted(self) -> bool {
        matches!(self, Self::Trusted)
    }
}

impl FromStr for PickleTrust {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // An empty value means the operator wrote the key but left it blank; treat that
        // as unset (untrusted) rather than as a typo, matching the other model params.
        // `yes`/`no`/`1`/`0` are accepted alongside `true`/`false` for operators coming
        // from other tools' boolean conventions.
        match s.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => Ok(Self::Trusted),
            "" | "false" | "no" | "0" => Ok(Self::Untrusted),
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
    fn parses_the_documented_and_alias_values_case_insensitively() {
        for (raw, expected) in [
            ("true", PickleTrust::Trusted),
            ("TRUE", PickleTrust::Trusted),
            ("yes", PickleTrust::Trusted),
            ("1", PickleTrust::Trusted),
            ("false", PickleTrust::Untrusted),
            ("False", PickleTrust::Untrusted),
            ("no", PickleTrust::Untrusted),
            ("0", PickleTrust::Untrusted),
            // Blank is unset, not invalid.
            ("", PickleTrust::Untrusted),
        ] {
            assert_eq!(
                raw.parse::<PickleTrust>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn rejects_values_outside_the_spec() {
        for raw in ["truthy", "maybe", "2"] {
            assert!(
                raw.parse::<PickleTrust>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    /// The parameter spec advertises `VALUES`, so every entry has to parse — otherwise
    /// config validation accepts a value the parser then rejects.
    #[test]
    fn every_advertised_value_parses() {
        for value in PickleTrust::VALUES {
            value
                .parse::<PickleTrust>()
                .unwrap_or_else(|e| panic!("{value:?} is advertised but does not parse: {e}"));
        }
    }
}
