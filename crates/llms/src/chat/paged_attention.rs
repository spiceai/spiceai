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

//! How a locally served model implements attention: `PagedAttention` (a paged KV cache,
//! which lets the scheduler overcommit and share cache blocks) or dense attention with
//! a contiguous cache.

use std::str::FromStr;

/// Operator-selected attention implementation for a locally served model, from the
/// `paged_attention` model param.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PagedAttentionMode {
    /// Use `PagedAttention` where the build and the model support it, and fall back to
    /// dense attention where they do not.
    #[default]
    Auto,
    /// Never use `PagedAttention`: always serve with dense attention and a contiguous
    /// KV cache. The escape hatch for a model whose paged path misbehaves.
    Disabled,
}

impl PagedAttentionMode {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["auto", "disabled"];
}

impl FromStr for PagedAttentionMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // An empty value means the operator wrote the key but left it blank; treat that
        // as unset rather than as a typo, matching the other model params.
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "auto" => Ok(Self::Auto),
            "disabled" => Ok(Self::Disabled),
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
            ("auto", PagedAttentionMode::Auto),
            ("AUTO", PagedAttentionMode::Auto),
            ("  auto ", PagedAttentionMode::Auto),
            // Blank is unset, not invalid.
            ("", PagedAttentionMode::Auto),
            ("disabled", PagedAttentionMode::Disabled),
            ("Disabled", PagedAttentionMode::Disabled),
        ] {
            assert_eq!(
                raw.parse::<PagedAttentionMode>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn rejects_values_outside_the_spec() {
        // Booleans in particular: a Spicepod that spells this `true`/`false` has to fail
        // loudly rather than have one of them quietly read as a mode.
        for raw in ["true", "false", "1", "0", "on", "off", "eager", "maybe"] {
            assert!(
                raw.parse::<PagedAttentionMode>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    /// The parameter spec advertises `VALUES`, so every entry has to parse — otherwise
    /// config validation accepts a value the parser then rejects.
    #[test]
    fn every_advertised_value_parses() {
        for value in PagedAttentionMode::VALUES {
            value
                .parse::<PagedAttentionMode>()
                .unwrap_or_else(|e| panic!("{value:?} is advertised but does not parse: {e}"));
        }
    }
}
