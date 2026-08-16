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

//! Project-name suggestions for the interactive Cloud Connect flow.

use std::path::Path;

use rand::RngExt as _;

pub(super) const PROJECT_NAME_MIN_LEN: usize = 4;
pub(super) const PROJECT_NAME_MAX_LEN: usize = 38;

const FALLBACK_ADJECTIVES: &[&str] = &[
    "amber", "bright", "calm", "clever", "crisp", "eager", "gentle", "lively", "rapid", "steady",
    "vivid", "warm",
];

/// Validate a project name exactly as the connect contract accepts it.
pub(super) fn validate_project_name(name: &str) -> std::result::Result<(), &'static str> {
    if !(PROJECT_NAME_MIN_LEN..=PROJECT_NAME_MAX_LEN).contains(&name.len()) {
        return Err("must be between 4 and 38 characters");
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err("must contain only lowercase letters, digits, and dashes");
    }
    if name.starts_with('-') || name.ends_with('-') {
        return Err("must start and end with a letter or digit");
    }
    Ok(())
}

/// Derive the editable first suggestion from a canonical instance directory.
///
/// The result uses the directory basename when it can produce a valid name.
/// An unusable basename gets exactly one CLI-local `adjective-spice` fallback.
pub(super) fn initial_suggestion(directory: &Path) -> String {
    let derived = directory
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .map(normalize_basename)
        .unwrap_or_default();
    if validate_project_name(&derived).is_ok() {
        return derived;
    }

    let mut rng = rand::rng();
    let index = rng.random_range(0..FALLBACK_ADJECTIVES.len());
    format!("{}-spice", FALLBACK_ADJECTIVES[index])
}

fn normalize_basename(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len().min(PROJECT_NAME_MAX_LEN));
    let mut separator_pending = false;

    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() {
            if separator_pending
                && !normalized.is_empty()
                && normalized.len() < PROJECT_NAME_MAX_LEN
            {
                normalized.push('-');
            }
            separator_pending = false;
            if normalized.len() == PROJECT_NAME_MAX_LEN {
                break;
            }
            normalized.push(byte.to_ascii_lowercase() as char);
        } else if !normalized.is_empty() {
            separator_pending = true;
        }
    }

    normalized.truncate(PROJECT_NAME_MAX_LEN);
    while normalized.ends_with('-') {
        normalized.pop();
    }
    normalized
}

/// Suggest the next editable name after an authoritative create conflict.
pub(super) fn collision_suggestion(base: &str, number: u32) -> String {
    let suffix = format!("-{number}");
    let base_budget = PROJECT_NAME_MAX_LEN.saturating_sub(suffix.len());
    let mut truncated = base[..base.len().min(base_budget)].trim_end_matches('-');
    if truncated.len() < PROJECT_NAME_MIN_LEN {
        truncated = "spice";
    }
    format!("{truncated}{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_directory_basename_is_normalized() {
        assert_eq!(
            normalize_basename("  Retail__Analytics 2026 "),
            "retail-analytics-2026"
        );
        assert_eq!(normalize_basename("A".repeat(60).as_str()).len(), 38);
        assert_eq!(normalize_basename("café/東京"), "caf");
    }

    #[test]
    fn unusable_basename_gets_one_valid_local_fallback() {
        let suggestion = initial_suggestion(Path::new("/tmp/---"));
        validate_project_name(&suggestion).expect("fallback must be valid");
        assert!(suggestion.ends_with("-spice"));
    }

    #[test]
    fn explicit_names_are_validated_without_rewriting() {
        for invalid in ["ABC", "abc", "-valid", "valid-", "has space", "a_b"] {
            assert!(validate_project_name(invalid).is_err(), "{invalid}");
        }
        validate_project_name("valid-project-2").expect("valid project name");
    }

    #[test]
    fn normalization_and_collision_suffixes_preserve_the_name_contract() {
        let alphabet = [b'a', b'Z', b'0', b'-', b'_', b' ', 0xf0, b'.'];
        let mut state = 0x5eed_u64;
        for length in 0..256 {
            let mut input = Vec::with_capacity(length);
            for _ in 0..length {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                input.push(alphabet[usize::from(state.to_le_bytes()[0]) % alphabet.len()]);
            }
            let lossy = String::from_utf8_lossy(&input);
            let normalized = normalize_basename(&lossy);
            assert!(normalized.len() <= PROJECT_NAME_MAX_LEN);
            assert!(normalized.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
            }));
            assert!(!normalized.starts_with('-'));
            assert!(!normalized.ends_with('-'));

            let base = if validate_project_name(&normalized).is_ok() {
                normalized
            } else {
                "steady-spice".to_string()
            };
            for number in [2, 9, 10, 999, u32::MAX] {
                validate_project_name(&collision_suggestion(&base, number))
                    .expect("collision suggestion must stay valid");
            }
        }
    }
}
