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

//! Field paths in Spicepod parse errors, so `Invalid type string "true".
//! Expected a boolean` reads `datasets[0].acceleration.enabled: Invalid type ...`.

use serde::de::DeserializeOwned;

/// Prefixes `message` with the field `path`. An empty path (`.`) leaves the
/// message unchanged. When `message` already starts with a nested path (from a
/// component parsed separately), the two paths are joined.
pub(crate) fn prefix_path(path: &str, message: &str) -> String {
    if path.is_empty() || path == "." {
        return message.to_string();
    }
    if let Some((inner, rest)) = message.split_once(": ")
        && is_path(inner)
    {
        let separator = if inner.starts_with('[') { "" } else { "." };
        return format!("{path}{separator}{inner}: {rest}");
    }
    format!("{path}: {message}")
}

fn is_path(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']' | '-'))
}

/// Deserializes `value` like `yaml::from_value`, naming the failing field in the error.
pub(crate) fn from_value<T: DeserializeOwned>(value: yaml::Value) -> Result<T, yaml::Error> {
    serde_path_to_error::deserialize(yaml::ValueDeserializer::new(value)).map_err(|err| {
        let path = err.path().to_string();
        let inner = err.into_inner();
        if path.is_empty() || path == "." {
            inner
        } else {
            <yaml::Error as serde::de::Error>::custom(prefix_path(&path, &inner.to_string()))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::SpicepodDefinition;

    fn parse_error(manifest: &str) -> String {
        let value: yaml::Value = yaml::from_str(manifest).expect("manifest should be YAML");
        from_value::<SpicepodDefinition>(value)
            .expect_err("manifest should not parse")
            .to_string()
    }

    #[test]
    fn type_error_names_the_dataset_field() {
        let message = parse_error(
            "version: v2\nkind: Spicepod\nname: t\ndatasets:\n  - name: d\n    from: file:/tmp/x.parquet\n    acceleration:\n      enabled: \"true\"\n",
        );
        assert!(
            message.starts_with("datasets[0].acceleration.enabled: "),
            "{message}"
        );
        assert!(message.contains("Expected a boolean"), "{message}");
    }

    #[test]
    fn type_error_names_the_entry_index() {
        let message = parse_error(
            "version: v2\nkind: Spicepod\nname: t\ndatasets:\n  - name: a\n    from: file:/a\n  - name: b\n    from: file:/b\n    acceleration:\n      refresh_retry_max_attempts: lots\n",
        );
        assert!(
            message.starts_with("datasets[1].acceleration.refresh_retry_max_attempts: "),
            "{message}"
        );
    }

    #[test]
    fn type_error_names_a_runtime_field() {
        let message = parse_error(
            "version: v2\nkind: Spicepod\nname: t\nruntime:\n  dataset_load_parallelism: lots\n",
        );
        assert!(
            message.starts_with("runtime.dataset_load_parallelism: "),
            "{message}"
        );
    }

    #[test]
    fn prefix_path_joins_nested_paths() {
        assert_eq!(
            prefix_path("datasets[0]", "acceleration.enabled: bad"),
            "datasets[0].acceleration.enabled: bad"
        );
        assert_eq!(prefix_path(".", "bad"), "bad");
        assert_eq!(prefix_path("name", "Invalid type"), "name: Invalid type");
    }
}
