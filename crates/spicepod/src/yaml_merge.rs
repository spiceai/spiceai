/*
Copyright 2025 The Spice.ai OSS Authors

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

//! YAML merge key support for spicepod parsing.
//!
//! This module provides support for YAML merge keys (`<<:`) which allow
//! reusing common configuration across multiple entries in a spicepod.
//!
//! # Example
//!
//! ```yaml
//! # Define common configuration with an anchor
//! common_config: &common
//!   refresh: 1h
//!   retention: 7d
//!
//! datasets:
//!   - name: dataset1
//!     from: source1
//!     <<: *common  # Merge common_config into this dataset
//!
//!   - name: dataset2
//!     from: source2
//!     <<: *common  # Reuse the same common configuration
//! ```
//!
//! See: <http://yaml.org/type/merge.html>

use serde::de::DeserializeOwned;
use std::io::Read;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Error {
    ReadError { source: std::io::Error },

    ParseError { source: serde_yaml::Error },

    MergeKeyError { message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Deserializes a YAML document from a reader, applying YAML merge keys (`<<:`) before
/// deserialization.
///
/// This function first parses the YAML into a `serde_yaml::Value`, applies the merge key
/// extension to resolve all merge keys, and then deserializes the result into the target type.
///
/// # Errors
///
/// Returns an error if:
/// - Reading from the reader fails
/// - The YAML is invalid
/// - Merge key processing fails
/// - Deserialization into the target type fails
pub fn from_reader_with_merge<T: DeserializeOwned>(
    mut reader: impl Read,
) -> Result<T, serde_yaml::Error> {
    let mut content = String::new();
    if let Err(e) = reader.read_to_string(&mut content) {
        return Err(serde::de::Error::custom(format!(
            "Failed to read YAML content: {e}"
        )));
    }
    from_str_with_merge(&content)
}

/// Deserializes a YAML document from a string, applying YAML merge keys (`<<:`) before
/// deserialization.
///
/// This function first parses the YAML into a `serde_yaml::Value`, applies the merge key
/// extension to resolve all merge keys, and then deserializes the result into the target type.
///
/// # Errors
///
/// Returns an error if:
/// - The YAML is invalid
/// - Merge key processing fails
/// - Deserialization into the target type fails
pub fn from_str_with_merge<T: DeserializeOwned>(s: &str) -> Result<T, serde_yaml::Error> {
    // First parse as a generic YAML value
    let value: serde_yaml::Value = serde_yaml::from_str(s)?;

    // Apply merge keys
    let merged =
        yaml_merge_keys::merge_keys_serde(value).map_err(|e| -> serde_yaml::Error {
            // MergeKeyError implements Display, so we can use to_string()
            serde::de::Error::custom(format!("Failed to apply YAML merge keys: {e}"))
        })?;

    // Deserialize the merged value into the target type
    serde_yaml::from_value(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Debug, Deserialize, PartialEq, Default)]
    struct TestConfig {
        name: String,
        #[serde(default)]
        value1: Option<String>,
        #[serde(default)]
        value2: Option<String>,
        #[serde(default)]
        value3: Option<String>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestRoot {
        #[serde(default)]
        configs: Vec<TestConfig>,
    }

    #[test]
    fn test_simple_merge_key() {
        let yaml = r"
common: &common
  value1: v1
  value2: v2

configs:
  - name: config1
    <<: *common
    value3: v3
";

        let result: TestRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].name, "config1");
        assert_eq!(result.configs[0].value1, Some("v1".to_string()));
        assert_eq!(result.configs[0].value2, Some("v2".to_string()));
        assert_eq!(result.configs[0].value3, Some("v3".to_string()));
    }

    #[test]
    fn test_merge_key_override() {
        let yaml = r"
defaults: &defaults
  value1: default1
  value2: default2

configs:
  - name: config1
    <<: *defaults
    value1: overridden1
";

        let result: TestRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        // The explicit value should override the merged value
        assert_eq!(result.configs[0].value1, Some("overridden1".to_string()));
        assert_eq!(result.configs[0].value2, Some("default2".to_string()));
    }

    #[test]
    fn test_multiple_merge_keys() {
        let yaml = r"
base1: &base1
  value1: v1

base2: &base2
  value2: v2

configs:
  - name: config1
    <<: [*base1, *base2]
    value3: v3
";

        let result: TestRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].value1, Some("v1".to_string()));
        assert_eq!(result.configs[0].value2, Some("v2".to_string()));
        assert_eq!(result.configs[0].value3, Some("v3".to_string()));
    }

    #[test]
    fn test_no_merge_keys() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct SimpleConfig {
            name: String,
            #[serde(default)]
            params: HashMap<String, String>,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct SimpleRoot {
            #[serde(default)]
            configs: Vec<SimpleConfig>,
        }

        let yaml = r"
configs:
  - name: config1
    params:
      param1: value1
";

        let result: SimpleRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].name, "config1");
        assert_eq!(
            result.configs[0].params.get("param1"),
            Some(&"value1".to_string())
        );
    }

    #[test]
    fn test_from_reader_with_merge() {
        let yaml = r"
common: &common
  value1: v1

configs:
  - name: config1
    <<: *common
";

        let reader = std::io::Cursor::new(yaml);
        let result: TestRoot = from_reader_with_merge(reader).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].value1, Some("v1".to_string()));
    }

    #[test]
    fn test_nested_merge_keys() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct NestedConfig {
            name: String,
            #[serde(default)]
            inner: InnerConfig,
        }

        #[derive(Debug, Deserialize, PartialEq, Default)]
        struct InnerConfig {
            #[serde(default)]
            value: String,
            #[serde(default)]
            extra: String,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct NestedRoot {
            #[serde(default)]
            configs: Vec<NestedConfig>,
        }

        let yaml = r"
inner_defaults: &inner_defaults
  value: default_value
  extra: default_extra

configs:
  - name: config1
    inner:
      <<: *inner_defaults
      value: custom_value
";

        let result: NestedRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].inner.value, "custom_value");
        assert_eq!(result.configs[0].inner.extra, "default_extra");
    }

    #[test]
    fn test_merge_into_hashmap() {
        // Test that merge keys work with flattened HashMap fields
        #[derive(Debug, Deserialize, PartialEq)]
        struct ConfigWithParams {
            name: String,
            #[serde(flatten)]
            params: HashMap<String, serde_yaml::Value>,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct ParamsRoot {
            #[serde(default)]
            configs: Vec<ConfigWithParams>,
        }

        let yaml = r"
common_params: &common_params
  refresh_interval: 1h
  mode: file

configs:
  - name: config1
    <<: *common_params
    custom_param: value1
";

        let result: ParamsRoot = from_str_with_merge(yaml).expect("Failed to parse YAML");

        assert_eq!(result.configs.len(), 1);
        assert_eq!(result.configs[0].name, "config1");
        assert_eq!(
            result.configs[0]
                .params
                .get("refresh_interval")
                .and_then(|v| v.as_str()),
            Some("1h")
        );
        assert_eq!(
            result.configs[0]
                .params
                .get("mode")
                .and_then(|v| v.as_str()),
            Some("file")
        );
        assert_eq!(
            result.configs[0]
                .params
                .get("custom_param")
                .and_then(|v| v.as_str()),
            Some("value1")
        );
    }
}
