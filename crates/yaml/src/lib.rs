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

//! A YAML serialization and deserialization library for Spice.ai.
//!
//! This library provides functionality for parsing YAML strings and files
//! into Rust types, and serializing Rust types to YAML strings.
//!
//! # Example
//!
//! ```
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize, PartialEq)]
//! struct Config {
//!     name: String,
//!     count: u32,
//! }
//!
//! let yaml = "name: example\ncount: 42";
//! let config: Config = yaml::from_str(yaml).unwrap();
//! assert_eq!(config.name, "example");
//! assert_eq!(config.count, 42);
//!
//! let yaml_out = yaml::to_string(&config).unwrap();
//! assert!(yaml_out.contains("name: example"));
//! ```

#![deny(clippy::expect_used)]
#![deny(clippy::unwrap_used)]

mod de;
mod error;
mod ser;
mod value;

pub use error::{Error, Location, Result};
pub use value::{Index, Mapping, Number, Value};

use serde::{Deserialize, Serialize};
use std::io::Read;

/// Deserialize an instance of type `T` from a YAML string.
///
/// # Errors
///
/// Returns an error if the YAML string is invalid or cannot be deserialized
/// into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = "name: test";
/// let config: Config = yaml::from_str(yaml).unwrap();
/// assert_eq!(config.name, "test");
/// ```
pub fn from_str<'de, T>(s: &'de str) -> Result<T>
where
    T: Deserialize<'de>,
{
    let value = de::parse_yaml(s)?;
    T::deserialize(de::ValueDeserializer::new(value))
}

/// Deserialize an instance of type `T` from an I/O reader containing YAML.
///
/// # Errors
///
/// Returns an error if reading fails, the YAML is invalid, or the data cannot
/// be deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
/// use std::io::Cursor;
///
/// #[derive(Deserialize)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = "name: test";
/// let reader = Cursor::new(yaml);
/// let config: Config = yaml::from_reader(reader).unwrap();
/// assert_eq!(config.name, "test");
/// ```
pub fn from_reader<R, T>(mut reader: R) -> Result<T>
where
    R: Read,
    T: for<'de> Deserialize<'de>,
{
    let mut s = String::new();
    reader.read_to_string(&mut s)?;
    from_str(&s)
}

/// Deserialize an instance of type `T` from a byte slice containing YAML.
///
/// # Errors
///
/// Returns an error if the bytes are not valid UTF-8, the YAML is invalid,
/// or the data cannot be deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = b"name: test";
/// let config: Config = yaml::from_slice(yaml).unwrap();
/// assert_eq!(config.name, "test");
/// ```
pub fn from_slice<T>(slice: &[u8]) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let s = std::str::from_utf8(slice)
        .map_err(|e| Error::from(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
    from_str(s)
}

/// Serialize the given data structure to a YAML string.
///
/// # Errors
///
/// Returns an error if the data cannot be serialized to YAML.
///
/// # Example
///
/// ```
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Config {
///     name: String,
///     count: u32,
/// }
///
/// let config = Config {
///     name: "test".to_string(),
///     count: 42,
/// };
/// let yaml = yaml::to_string(&config).unwrap();
/// assert!(yaml.contains("name: test"));
/// assert!(yaml.contains("count: 42"));
/// ```
pub fn to_string<T>(value: &T) -> Result<String>
where
    T: Serialize + ?Sized,
{
    let v = to_value(value)?;
    ser::emit_yaml(&v)
}

/// Serialize the given data structure to a YAML string and write it to a writer.
///
/// # Errors
///
/// Returns an error if the data cannot be serialized to YAML or if writing fails.
///
/// # Example
///
/// ```
/// use serde::Serialize;
/// use std::io::Cursor;
///
/// #[derive(Serialize)]
/// struct Config {
///     name: String,
/// }
///
/// let config = Config { name: "test".to_string() };
/// let mut buffer = Vec::new();
/// yaml::to_writer(&mut buffer, &config).unwrap();
/// let yaml = String::from_utf8(buffer).unwrap();
/// assert!(yaml.contains("name: test"));
/// ```
pub fn to_writer<W, T>(writer: W, value: &T) -> Result<()>
where
    W: std::io::Write,
    T: Serialize + ?Sized,
{
    let yaml_str = to_string(value)?;
    let mut writer = writer;
    writer.write_all(yaml_str.as_bytes())?;
    Ok(())
}

/// Convert a `T` into a `Value`.
///
/// # Errors
///
/// Returns an error if the value cannot be serialized.
///
/// # Example
///
/// ```
/// use serde::Serialize;
/// use yaml::Value;
///
/// #[derive(Serialize)]
/// struct Config {
///     name: String,
/// }
///
/// let config = Config { name: "test".to_string() };
/// let value: Value = yaml::to_value(&config).unwrap();
/// assert_eq!(value.get("name").and_then(|v| v.as_str()), Some("test"));
/// ```
pub fn to_value<T>(value: &T) -> Result<Value>
where
    T: Serialize + ?Sized,
{
    value.serialize(ser::Serializer)
}

/// Interpret a `Value` as an instance of type `T`.
///
/// # Errors
///
/// Returns an error if the value cannot be deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
/// use yaml::{Value, Mapping};
///
/// #[derive(Deserialize, PartialEq, Debug)]
/// struct Config {
///     name: String,
/// }
///
/// let mut map = Mapping::new();
/// map.insert(Value::String("name".into()), Value::String("test".into()));
/// let value = Value::Mapping(map);
///
/// let config: Config = yaml::from_value(value).unwrap();
/// assert_eq!(config, Config { name: "test".to_string() });
/// ```
pub fn from_value<T>(value: Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    T::deserialize(de::ValueDeserializer::new(value))
}

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    reason = "tests use unwrap for concise assertions"
)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_from_str_simple() {
        let yaml = "42";
        let result: i32 = from_str(yaml).expect("should parse 42");
        assert_eq!(result, 42);
    }

    #[test]
    fn test_from_str_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Config {
            name: String,
            count: u32,
            enabled: bool,
        }

        let yaml = r"
name: test
count: 42
enabled: true
";
        let config: Config = from_str(yaml).expect("should parse config");
        assert_eq!(
            config,
            Config {
                name: "test".into(),
                count: 42,
                enabled: true,
            }
        );
    }

    #[test]
    fn test_from_str_nested() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Inner {
            value: i32,
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct Outer {
            inner: Inner,
            name: String,
        }

        let yaml = r"
inner:
  value: 42
name: test
";
        let outer: Outer = from_str(yaml).expect("should parse nested");
        assert_eq!(
            outer,
            Outer {
                inner: Inner { value: 42 },
                name: "test".into(),
            }
        );
    }

    #[test]
    fn test_from_str_sequence() {
        let yaml = "[1, 2, 3]";
        let result: Vec<i32> = from_str(yaml).expect("should parse sequence");
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_from_str_map() {
        use std::collections::HashMap;

        let yaml = r"
one: 1
two: 2
three: 3
";
        let result: HashMap<String, i32> = from_str(yaml).expect("should parse map");
        assert_eq!(result.get("one"), Some(&1));
        assert_eq!(result.get("two"), Some(&2));
        assert_eq!(result.get("three"), Some(&3));
    }

    #[test]
    fn test_from_str_enum() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum Status {
            Active,
            Inactive,
        }

        let yaml = "Active";
        let result: Status = from_str(yaml).expect("should parse enum");
        assert_eq!(result, Status::Active);
    }

    #[test]
    fn test_from_str_option() {
        let yaml = "null";
        let result: Option<i32> = from_str(yaml).expect("should parse null");
        assert_eq!(result, None);

        let yaml = "42";
        let result: Option<i32> = from_str(yaml).expect("should parse Some");
        assert_eq!(result, Some(42));
    }

    #[test]
    fn test_from_reader() {
        use std::io::Cursor;

        #[derive(Deserialize)]
        struct Config {
            name: String,
        }

        let yaml = "name: test";
        let reader = Cursor::new(yaml);
        let config: Config = from_reader(reader).expect("should read config");
        assert_eq!(config.name, "test");
    }

    #[test]
    fn test_to_string_simple() {
        let yaml = to_string(&42).expect("should serialize 42");
        assert!(yaml.trim() == "42");
    }

    #[test]
    fn test_to_string_struct() {
        #[derive(Serialize)]
        struct Config {
            name: String,
            count: u32,
        }

        let config = Config {
            name: "test".into(),
            count: 42,
        };
        let yaml = to_string(&config).expect("should serialize config");
        assert!(yaml.contains("name: test"));
        assert!(yaml.contains("count: 42"));
    }

    #[test]
    fn test_to_string_ends_with_newline() {
        #[derive(Serialize)]
        struct Config {
            name: String,
            value: i32,
        }

        let config = Config {
            name: "test".into(),
            value: 42,
        };
        let yaml = to_string(&config).expect("test");
        eprintln!("YAML repr: {yaml:?}");
        assert!(
            yaml.ends_with('\n'),
            "YAML output should end with newline for safe file appending"
        );
    }

    #[test]
    fn test_to_string_sequence() {
        let vec = vec![1, 2, 3];
        let yaml = to_string(&vec).expect("should serialize vec");
        assert!(yaml.contains("- 1"));
        assert!(yaml.contains("- 2"));
        assert!(yaml.contains("- 3"));
    }

    #[test]
    fn test_to_value() {
        #[derive(Serialize)]
        struct Config {
            name: String,
        }

        let config = Config {
            name: "test".into(),
        };
        let value = to_value(&config).expect("should convert to value");
        assert!(value.is_mapping());
        assert_eq!(value.get("name").and_then(|v| v.as_str()), Some("test"));
    }

    #[test]
    fn test_from_value() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Config {
            name: String,
        }

        let mut map = Mapping::new();
        map.insert(Value::String("name".into()), Value::String("test".into()));
        let value = Value::Mapping(map);

        let config: Config = from_value(value).expect("should convert from value");
        assert_eq!(
            config,
            Config {
                name: "test".into()
            }
        );
    }

    #[test]
    fn test_roundtrip() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Config {
            name: String,
            count: u32,
            items: Vec<String>,
            enabled: bool,
        }

        let original = Config {
            name: "test".into(),
            count: 42,
            items: vec!["one".into(), "two".into()],
            enabled: true,
        };

        let yaml = to_string(&original).expect("should serialize");
        let parsed: Config = from_str(&yaml).expect("should parse back");
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_nested_struct_with_optional_fields() {
        // Test struct layout similar to dataset configuration
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct AccelerationSpec {
            enabled: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            refresh_check_interval: Option<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            refresh_mode: Option<String>,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct DatasetSpec {
            from: String,
            name: String,
            #[serde(skip_serializing_if = "String::is_empty")]
            description: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            acceleration: Option<AccelerationSpec>,
        }

        let dataset = DatasetSpec {
            from: "duckdb:customer".to_string(),
            name: "tpch_customer".to_string(),
            description: "TPC-H customer table".to_string(),
            acceleration: Some(AccelerationSpec {
                enabled: true,
                refresh_check_interval: Some("10s".to_string()),
                refresh_mode: Some("full".to_string()),
            }),
        };

        let yaml = to_string(&dataset).expect("test: serialize dataset");
        eprintln!("YAML output:\n{yaml}");

        // Verify proper line separation (no concatenated lines)
        assert!(
            !yaml.contains("customerfrom:"),
            "Lines are concatenated: found 'customerfrom:' in output"
        );
        assert!(
            !yaml.contains("fullacceleration:"),
            "Lines are concatenated: found 'fullacceleration:' in output"
        );

        // Verify each field is on its own line
        assert!(yaml.contains("from:"), "Missing 'from:' field");
        assert!(yaml.contains("name:"), "Missing 'name:' field");
        assert!(
            yaml.contains("description:"),
            "Missing 'description:' field"
        );
        assert!(
            yaml.contains("acceleration:"),
            "Missing 'acceleration:' field"
        );

        // Roundtrip test
        let parsed: DatasetSpec = from_str(&yaml).expect("test: parse dataset");
        assert_eq!(dataset, parsed);
    }

    #[test]
    fn test_special_yaml_values() {
        // Test that we handle special YAML boolean strings
        let yaml = "yes";
        let result: bool = from_str(yaml).expect("should parse yes");
        assert!(result);

        let yaml = "no";
        let result: bool = from_str(yaml).expect("should parse no");
        assert!(!result);
    }

    #[test]
    fn test_multiline_string() {
        #[derive(Deserialize)]
        struct Config {
            description: String,
        }

        let yaml = r"
description: |
  This is a
  multiline string
";

        let config: Config = from_str(yaml).expect("should parse multiline");
        assert!(config.description.contains("This is a"));
        assert!(config.description.contains("multiline string"));
    }

    #[test]
    fn test_complex_nested_structure() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Database {
            host: String,
            port: u16,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Server {
            address: String,
            databases: Vec<Database>,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Config {
            name: String,
            server: Server,
        }

        let yaml = r"
name: myapp
server:
  address: localhost
  databases:
    - host: db1.example.com
      port: 5432
    - host: db2.example.com
      port: 5433
";

        let config: Config = from_str(yaml).expect("should parse complex nested");
        assert_eq!(config.name, "myapp");
        assert_eq!(config.server.address, "localhost");
        assert_eq!(config.server.databases.len(), 2);
        assert_eq!(config.server.databases[0].host, "db1.example.com");
        assert_eq!(config.server.databases[0].port, 5432);

        // Test roundtrip
        let yaml_out = to_string(&config).expect("should serialize");
        let parsed: Config = from_str(&yaml_out).expect("should parse back");
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_error_on_invalid_yaml() {
        let yaml = "key: [unclosed bracket";
        let result: Result<Value> = from_str(yaml);
        result.expect_err("should fail on invalid yaml");
    }

    #[test]
    fn test_deserialize_value_directly() {
        let yaml = r"
key: value
number: 42
list:
  - one
  - two
";
        let value: Value = from_str(yaml).expect("should parse value");
        assert!(value.is_mapping());
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
        assert_eq!(
            value.get("number").and_then(super::value::Value::as_i64),
            Some(42)
        );
        assert!(value.get("list").and_then(|v| v.as_sequence()).is_some());
    }

    #[test]
    fn test_skip_serializing_if() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Config {
            name: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            optional: Option<String>,
        }

        let config = Config {
            name: "test".into(),
            optional: None,
        };

        let yaml = to_string(&config).expect("should serialize");
        assert!(!yaml.contains("optional"));
    }

    #[test]
    fn test_default_values() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Config {
            name: String,
            #[serde(default)]
            count: u32,
        }

        let yaml = "name: test";
        let config: Config = from_str(yaml).expect("should parse with default");
        assert_eq!(config.count, 0);
    }

    #[test]
    fn test_rename() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Config {
            #[serde(rename = "display-name")]
            name: String,
        }

        let yaml = "display-name: test";
        let config: Config = from_str(yaml).unwrap();
        assert_eq!(config.name, "test");

        let config = Config {
            name: "example".into(),
        };
        let yaml = to_string(&config).unwrap();
        assert!(yaml.contains("display-name:"));
    }

    #[test]
    fn test_alias() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Config {
            #[serde(alias = "display_name")]
            name: String,
        }

        // Test original field name
        let yaml = "name: test";
        let config: Config = from_str(yaml).unwrap();
        assert_eq!(config.name, "test");

        // Test alias
        let yaml = "display_name: test";
        let config: Config = from_str(yaml).unwrap();
        assert_eq!(config.name, "test");
    }

    #[test]
    fn test_flatten() {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Inner {
            value: i32,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Outer {
            name: String,
            #[serde(flatten)]
            inner: Inner,
        }

        let yaml = r"
name: test
value: 42
";
        let outer: Outer = from_str(yaml).unwrap();
        assert_eq!(outer.name, "test");
        assert_eq!(outer.inner.value, 42);
    }

    // ============================================================
    // YAML Spec Compliance Tests - Anchors and Aliases
    // ============================================================

    #[test]
    fn test_yaml_anchor_and_alias_simple() {
        // Test basic anchor and alias functionality
        let yaml = r"
anchor_value: &my_anchor hello
alias_value: *my_anchor
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("anchor_value").and_then(|v| v.as_str()),
            Some("hello")
        );
        assert_eq!(
            value.get("alias_value").and_then(|v| v.as_str()),
            Some("hello")
        );
    }

    #[test]
    fn test_yaml_anchor_and_alias_mapping() {
        // Test anchor and alias with a mapping
        let yaml = r"
defaults: &defaults
  adapter: postgres
  host: localhost

development:
  database: dev_db
  settings: *defaults
";
        let value: Value = from_str(yaml).unwrap();

        // Check defaults
        let defaults = value.get("defaults").unwrap();
        assert_eq!(
            defaults.get("adapter").and_then(|v| v.as_str()),
            Some("postgres")
        );
        assert_eq!(
            defaults.get("host").and_then(|v| v.as_str()),
            Some("localhost")
        );

        // Check that alias resolves correctly
        let settings = value.get("development").unwrap().get("settings").unwrap();
        assert_eq!(
            settings.get("adapter").and_then(|v| v.as_str()),
            Some("postgres")
        );
        assert_eq!(
            settings.get("host").and_then(|v| v.as_str()),
            Some("localhost")
        );
    }

    #[test]
    fn test_yaml_anchor_and_alias_sequence() {
        // Test anchor and alias with a sequence
        let yaml = r"
colors: &colors
  - red
  - green
  - blue

primary_colors: *colors
";
        let value: Value = from_str(yaml).unwrap();

        let colors = value.get("colors").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(colors.len(), 3);

        let primary = value
            .get("primary_colors")
            .and_then(|v| v.as_sequence())
            .unwrap();
        assert_eq!(primary.len(), 3);
        assert_eq!(primary[0].as_str(), Some("red"));
    }

    #[test]
    fn test_yaml_multiple_anchors() {
        // Test multiple anchors in the same document
        let yaml = r"
first: &first 1
second: &second 2
ref_first: *first
ref_second: *second
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("first").and_then(super::value::Value::as_i64),
            Some(1)
        );
        assert_eq!(
            value.get("second").and_then(super::value::Value::as_i64),
            Some(2)
        );
        assert_eq!(
            value.get("ref_first").and_then(super::value::Value::as_i64),
            Some(1)
        );
        assert_eq!(
            value
                .get("ref_second")
                .and_then(super::value::Value::as_i64),
            Some(2)
        );
    }

    #[test]
    fn test_yaml_anchor_in_sequence() {
        // Test anchors defined within sequences
        let yaml = r"
items:
  - &item1
    name: first
  - &item2
    name: second
refs:
  - *item1
  - *item2
";
        let value: Value = from_str(yaml).unwrap();
        let refs = value.get("refs").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(refs[0].get("name").and_then(|v| v.as_str()), Some("first"));
        assert_eq!(refs[1].get("name").and_then(|v| v.as_str()), Some("second"));
    }

    #[test]
    fn test_yaml_merge_key() {
        // Test YAML 1.1 merge key functionality (<<:)
        // Note: yaml-rust2 supports this as an extension
        let yaml = r"
defaults: &defaults
  adapter: postgres
  host: localhost

development:
  <<: *defaults
  database: dev_db
";
        let value: Value = from_str(yaml).unwrap();
        let dev = value.get("development").unwrap();

        // Check that merge happened
        assert_eq!(dev.get("database").and_then(|v| v.as_str()), Some("dev_db"));
        assert_eq!(
            dev.get("adapter").and_then(|v| v.as_str()),
            Some("postgres")
        );
        assert_eq!(dev.get("host").and_then(|v| v.as_str()), Some("localhost"));
    }

    #[test]
    fn test_yaml_merge_key_override() {
        // Test that local keys override merged keys
        let yaml = r"
defaults: &defaults
  adapter: postgres
  host: localhost

production:
  <<: *defaults
  host: prod.example.com
";
        let value: Value = from_str(yaml).unwrap();
        let prod = value.get("production").unwrap();

        // adapter should come from merge
        assert_eq!(
            prod.get("adapter").and_then(|v| v.as_str()),
            Some("postgres")
        );
        // host should be overridden
        assert_eq!(
            prod.get("host").and_then(|v| v.as_str()),
            Some("prod.example.com")
        );
    }

    #[test]
    fn test_yaml_merge_multiple() {
        // Test merging from multiple anchors
        let yaml = r"
base: &base
  name: base

extra: &extra
  enabled: true

combined:
  <<: [*base, *extra]
  value: 42
";
        let value: Value = from_str(yaml).unwrap();
        let combined = value.get("combined").unwrap();

        assert_eq!(combined.get("name").and_then(|v| v.as_str()), Some("base"));
        assert_eq!(
            combined
                .get("enabled")
                .and_then(super::value::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            combined.get("value").and_then(super::value::Value::as_i64),
            Some(42)
        );
    }

    // ============================================================
    // YAML Spec Compliance Tests - Scalars
    // ============================================================

    #[test]
    fn test_yaml_null_variations() {
        // YAML 1.2 null representations: null, ~, and empty value
        // Note: Null, NULL are NOT recognized as null in YAML 1.2 (they are strings)
        let yaml = r"
null1: null
null2: ~
null3:
";
        let value: Value = from_str(yaml).unwrap();
        assert!(value.get("null1").unwrap().is_null());
        assert!(value.get("null2").unwrap().is_null());
        assert!(value.get("null3").unwrap().is_null());

        // Verify that capitalized versions are strings in YAML 1.2
        let yaml_11_style = r"
null_cap: Null
null_upper: NULL
";
        let value: Value = from_str(yaml_11_style).unwrap();
        // These are strings in YAML 1.2, not null
        assert_eq!(value.get("null_cap").and_then(|v| v.as_str()), Some("Null"));
        assert_eq!(
            value.get("null_upper").and_then(|v| v.as_str()),
            Some("NULL")
        );
    }

    #[test]
    fn test_yaml_boolean_variations() {
        // YAML 1.2 only recognizes true/false (case-insensitive) as booleans
        // Note: yes/no/on/off are NOT booleans in YAML 1.2 (they are strings)
        let yaml = r"
true1: true
true2: True
true3: TRUE
false1: false
false2: False
false3: FALSE
";
        let value: Value = from_str(yaml).unwrap();

        // True variations (case-insensitive in yaml-rust2)
        assert_eq!(
            value.get("true1").and_then(super::value::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            value.get("true2").and_then(super::value::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            value.get("true3").and_then(super::value::Value::as_bool),
            Some(true)
        );

        // False variations (case-insensitive in yaml-rust2)
        assert_eq!(
            value.get("false1").and_then(super::value::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            value.get("false2").and_then(super::value::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            value.get("false3").and_then(super::value::Value::as_bool),
            Some(false)
        );

        // Verify YAML 1.1 style booleans are strings in YAML 1.2
        let yaml_11_style = r"
yes_val: yes
no_val: no
on_val: on
off_val: off
";
        let value: Value = from_str(yaml_11_style).unwrap();
        assert_eq!(value.get("yes_val").and_then(|v| v.as_str()), Some("yes"));
        assert_eq!(value.get("no_val").and_then(|v| v.as_str()), Some("no"));
        assert_eq!(value.get("on_val").and_then(|v| v.as_str()), Some("on"));
        assert_eq!(value.get("off_val").and_then(|v| v.as_str()), Some("off"));
    }

    #[test]
    fn test_yaml_integer_formats() {
        // YAML supports decimal, hex, and octal integers
        let yaml = r"
decimal: 42
negative: -17
hex: 0x2A
octal: 0o52
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("decimal").and_then(super::value::Value::as_i64),
            Some(42)
        );
        assert_eq!(
            value.get("negative").and_then(super::value::Value::as_i64),
            Some(-17)
        );
        assert_eq!(
            value.get("hex").and_then(super::value::Value::as_i64),
            Some(42)
        );
        assert_eq!(
            value.get("octal").and_then(super::value::Value::as_i64),
            Some(42)
        );
    }

    #[test]
    fn test_yaml_float_formats() {
        // YAML supports various float representations
        let yaml = r"
float1: 3.15
float2: -0.5
scientific: 1.2e+3
infinity: .inf
neg_infinity: -.inf
not_a_number: .nan
";
        let value: Value = from_str(yaml).unwrap();

        assert!(
            (value
                .get("float1")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                - 3.15)
                .abs()
                < 0.001
        );
        assert!(
            (value
                .get("float2")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                - (-0.5))
                .abs()
                < 0.001
        );
        assert!(
            (value
                .get("scientific")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                - 1200.0)
                .abs()
                < 0.001
        );
        assert!(
            value
                .get("infinity")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                .is_infinite()
        );
        assert!(
            value
                .get("neg_infinity")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                .is_infinite()
        );
        assert!(
            value
                .get("neg_infinity")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                .is_sign_negative()
        );
        assert!(
            value
                .get("not_a_number")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                .is_nan()
        );
    }

    // ============================================================
    // YAML Spec Compliance Tests - Strings
    // ============================================================

    #[test]
    fn test_yaml_quoted_strings() {
        let yaml = r#"
single: 'hello world'
double: "hello world"
single_escape: 'it''s a test'
double_escape: "line1\nline2"
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("single").and_then(|v| v.as_str()),
            Some("hello world")
        );
        assert_eq!(
            value.get("double").and_then(|v| v.as_str()),
            Some("hello world")
        );
        assert_eq!(
            value.get("single_escape").and_then(|v| v.as_str()),
            Some("it's a test")
        );
        assert_eq!(
            value.get("double_escape").and_then(|v| v.as_str()),
            Some("line1\nline2")
        );
    }

    #[test]
    fn test_yaml_literal_block_scalar() {
        // Literal block scalar preserves newlines
        let yaml = r"
literal: |
  Line 1
  Line 2
  Line 3
";
        let value: Value = from_str(yaml).unwrap();
        let literal = value.get("literal").and_then(|v| v.as_str()).unwrap();
        assert!(literal.contains("Line 1"));
        assert!(literal.contains("Line 2"));
        assert!(literal.contains("Line 3"));
        assert!(literal.contains('\n'));
    }

    #[test]
    fn test_yaml_folded_block_scalar() {
        // Folded block scalar folds newlines into spaces
        let yaml = r"
folded: >
  This is a long
  line that will be
  folded into one.
";
        let value: Value = from_str(yaml).unwrap();
        let folded = value.get("folded").and_then(|v| v.as_str()).unwrap();
        // Folded should join lines with spaces
        assert!(folded.contains("This is a long"));
    }

    #[test]
    fn test_yaml_block_chomping() {
        // Test block chomping indicators (-, +)
        let yaml = r"
strip: |-
  text
clip: |
  text
keep: |+
  text

";
        let value: Value = from_str(yaml).unwrap();
        let strip = value.get("strip").and_then(|v| v.as_str()).unwrap();
        let clip = value.get("clip").and_then(|v| v.as_str()).unwrap();
        let keep = value.get("keep").and_then(|v| v.as_str()).unwrap();

        // Strip removes all trailing newlines
        assert!(!strip.ends_with('\n'));
        // Clip keeps a single trailing newline
        assert!(clip.ends_with('\n'));
        assert!(!clip.ends_with("\n\n"));
        // Keep preserves all trailing newlines
        assert!(keep.ends_with('\n'));
    }

    // ============================================================
    // YAML Spec Compliance Tests - Collections
    // ============================================================

    #[test]
    fn test_yaml_flow_sequence() {
        let yaml = r"
flow: [1, 2, 3, 4, 5]
nested: [[1, 2], [3, 4]]
";
        let value: Value = from_str(yaml).unwrap();
        let flow = value.get("flow").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(flow.len(), 5);

        let nested = value.get("nested").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].as_sequence().unwrap().len(), 2);
    }

    #[test]
    fn test_yaml_flow_mapping() {
        let yaml = r"
flow: {name: John, age: 30}
nested: {outer: {inner: value}}
";
        let value: Value = from_str(yaml).unwrap();
        let flow = value.get("flow").unwrap();
        assert_eq!(flow.get("name").and_then(|v| v.as_str()), Some("John"));
        assert_eq!(
            flow.get("age").and_then(super::value::Value::as_i64),
            Some(30)
        );

        let nested = value.get("nested").unwrap();
        assert_eq!(
            nested
                .get("outer")
                .unwrap()
                .get("inner")
                .and_then(|v| v.as_str()),
            Some("value")
        );
    }

    #[test]
    fn test_yaml_mixed_flow_block() {
        let yaml = r"
items:
  - {name: item1, value: 1}
  - {name: item2, value: 2}
config:
  list: [a, b, c]
  map: {key: value}
";
        let value: Value = from_str(yaml).unwrap();
        let items = value.get("items").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get("name").and_then(|v| v.as_str()), Some("item1"));
    }

    #[test]
    fn test_yaml_complex_keys() {
        // YAML allows complex keys (though uncommon)
        let yaml = r"
? - a
  - b
: value
";
        let value: Value = from_str(yaml).unwrap();
        assert!(value.is_mapping());
    }

    // ============================================================
    // YAML Spec Compliance Tests - Tags
    // ============================================================

    #[test]
    fn test_yaml_explicit_typing() {
        // Test explicit type tags
        // Note: yaml-rust2 supports basic type tags for !!str, !!int, !!float
        let yaml = r"
string_num: !!str 123
float_val: !!float 42
";
        let value: Value = from_str(yaml).unwrap();
        // !!str should make it a string
        assert_eq!(
            value.get("string_num").and_then(|v| v.as_str()),
            Some("123")
        );
        // !!float should make it a float
        let float_val = value.get("float_val").and_then(super::value::Value::as_f64);
        assert!(float_val.is_some());
        assert!((float_val.unwrap() - 42.0).abs() < 0.001);
    }

    // ============================================================
    // YAML Spec Compliance Tests - Comments
    // ============================================================

    #[test]
    fn test_yaml_comments() {
        let yaml = r"
# This is a comment
key: value # inline comment
# Another comment
list:
  - item1 # comment
  - item2
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
        let list = value.get("list").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(list.len(), 2);
    }

    // ============================================================
    // YAML Spec Compliance Tests - Edge Cases
    // ============================================================

    #[test]
    fn test_yaml_empty_values() {
        let yaml = r#"
empty_string: ""
empty_array: []
empty_map: {}
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(value.get("empty_string").and_then(|v| v.as_str()), Some(""));
        assert!(
            value
                .get("empty_array")
                .and_then(|v| v.as_sequence())
                .unwrap()
                .is_empty()
        );
        assert!(value.get("empty_map").unwrap().is_mapping());
    }

    #[test]
    fn test_yaml_special_characters_in_strings() {
        let yaml = r#"
colon: "has: colon"
hash: "has # hash"
bracket: "has [bracket]"
brace: "has {brace}"
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("colon").and_then(|v| v.as_str()),
            Some("has: colon")
        );
        assert_eq!(
            value.get("hash").and_then(|v| v.as_str()),
            Some("has # hash")
        );
        assert_eq!(
            value.get("bracket").and_then(|v| v.as_str()),
            Some("has [bracket]")
        );
        assert_eq!(
            value.get("brace").and_then(|v| v.as_str()),
            Some("has {brace}")
        );
    }

    #[test]
    fn test_yaml_unicode() {
        let yaml = r#"
emoji: 🎉
chinese: 中文
mixed: "Hello 世界 🌍"
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(value.get("emoji").and_then(|v| v.as_str()), Some("🎉"));
        assert_eq!(value.get("chinese").and_then(|v| v.as_str()), Some("中文"));
        assert_eq!(
            value.get("mixed").and_then(|v| v.as_str()),
            Some("Hello 世界 🌍")
        );
    }

    #[test]
    fn test_yaml_deeply_nested() {
        let yaml = r"
level1:
  level2:
    level3:
      level4:
        level5:
          value: deep
";
        let value: Value = from_str(yaml).unwrap();
        let deep = value
            .get("level1")
            .unwrap()
            .get("level2")
            .unwrap()
            .get("level3")
            .unwrap()
            .get("level4")
            .unwrap()
            .get("level5")
            .unwrap()
            .get("value");
        assert_eq!(deep.and_then(|v| v.as_str()), Some("deep"));
    }

    #[test]
    fn test_yaml_large_numbers() {
        let yaml = r"
large_int: 9223372036854775807
large_neg: -9223372036854775808
large_float: 1.7976931348623157e+308
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("large_int").and_then(super::value::Value::as_i64),
            Some(i64::MAX)
        );
        assert_eq!(
            value.get("large_neg").and_then(super::value::Value::as_i64),
            Some(i64::MIN)
        );
        assert!(
            value
                .get("large_float")
                .and_then(super::value::Value::as_f64)
                .unwrap()
                > 1e300
        );
    }

    #[test]
    fn test_yaml_document_markers() {
        // Test document start/end markers
        let yaml = r"---
key: value
...";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_yaml_indentation_variants() {
        // YAML allows various indentation levels
        let yaml = r"
two_space:
  nested: value
four_space:
    deeply:
        nested: value
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value
                .get("two_space")
                .unwrap()
                .get("nested")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("four_space")
                .unwrap()
                .get("deeply")
                .unwrap()
                .get("nested")
                .and_then(|v| v.as_str()),
            Some("value")
        );
    }

    // ============================================================
    // Additional Edge Case Tests
    // ============================================================

    #[test]
    fn test_yaml_nested_anchors_and_merges() {
        // Test nested anchor and merge scenarios
        let yaml = r"
base: &base
  name: base
  config: &config
    timeout: 30
    retries: 3

derived:
  <<: *base
  config:
    <<: *config
    timeout: 60
";
        let value: Value = from_str(yaml).unwrap();
        let derived = value.get("derived").unwrap();
        assert_eq!(derived.get("name").and_then(|v| v.as_str()), Some("base"));
        let config = derived.get("config").unwrap();
        assert_eq!(
            config.get("timeout").and_then(super::value::Value::as_i64),
            Some(60)
        );
        assert_eq!(
            config.get("retries").and_then(super::value::Value::as_i64),
            Some(3)
        );
    }

    #[test]
    fn test_yaml_anchor_reuse() {
        // Test using the same anchor multiple times
        let yaml = r"
template: &tmpl
  key: value

use1: *tmpl
use2: *tmpl
use3: *tmpl
";
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value
                .get("use1")
                .unwrap()
                .get("key")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("use2")
                .unwrap()
                .get("key")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("use3")
                .unwrap()
                .get("key")
                .and_then(|v| v.as_str()),
            Some("value")
        );
    }

    #[test]
    fn test_yaml_merge_priority() {
        // Test that later merges don't override earlier values
        // When merging multiple mappings, the first one takes precedence
        let yaml = r"
first: &first
  key: from_first
  only_first: true

second: &second
  key: from_second
  only_second: true

merged:
  <<: [*first, *second]
";
        let value: Value = from_str(yaml).unwrap();
        let merged = value.get("merged").unwrap();
        // First anchor takes precedence for duplicate keys
        assert_eq!(
            merged.get("key").and_then(|v| v.as_str()),
            Some("from_first")
        );
        assert_eq!(
            merged
                .get("only_first")
                .and_then(super::value::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            merged
                .get("only_second")
                .and_then(super::value::Value::as_bool),
            Some(true)
        );
    }

    #[test]
    fn test_yaml_binary_data() {
        // Test binary data handling (base64 encoded)
        let yaml = r"
binary: !!binary |
  R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7
";
        let value: Value = from_str(yaml).unwrap();
        // Binary data is typically returned as a string
        assert!(value.get("binary").is_some());
    }

    #[test]
    fn test_yaml_timestamp() {
        // Test timestamp parsing (ISO 8601 format)
        let yaml = r"
date1: 2024-01-15
date2: 2024-01-15T10:30:00Z
date3: 2024-01-15 10:30:00 -05:00
";
        let value: Value = from_str(yaml).unwrap();
        // Timestamps are typically returned as strings in yaml-rust2
        assert!(value.get("date1").is_some());
        assert!(value.get("date2").is_some());
        assert!(value.get("date3").is_some());
    }

    #[test]
    fn test_yaml_escape_sequences() {
        // Test various escape sequences in double-quoted strings
        let yaml = r#"
tab: "hello\tworld"
newline: "line1\nline2"
carriage: "hello\rworld"
backslash: "path\\to\\file"
quote: "say \"hello\""
unicode: "smiley: \u263A"
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("tab").and_then(|v| v.as_str()),
            Some("hello\tworld")
        );
        assert_eq!(
            value.get("newline").and_then(|v| v.as_str()),
            Some("line1\nline2")
        );
        assert_eq!(
            value.get("backslash").and_then(|v| v.as_str()),
            Some("path\\to\\file")
        );
        assert_eq!(
            value.get("quote").and_then(|v| v.as_str()),
            Some("say \"hello\"")
        );
    }

    #[test]
    fn test_yaml_multiline_key() {
        // Test complex keys using explicit key indicator
        let yaml = r"
? complex_key
: complex_value
simple_key: simple_value
";
        let value: Value = from_str(yaml).unwrap();
        assert!(value.is_mapping());
        assert_eq!(
            value.get("complex_key").and_then(|v| v.as_str()),
            Some("complex_value")
        );
        assert_eq!(
            value.get("simple_key").and_then(|v| v.as_str()),
            Some("simple_value")
        );
    }

    #[test]
    fn test_yaml_empty_document() {
        // Test empty and whitespace-only documents
        let yaml = "";
        let value: Value = from_str(yaml).unwrap();
        assert!(value.is_null());

        let yaml = "   \n\n   ";
        let value: Value = from_str(yaml).unwrap();
        assert!(value.is_null());
    }

    #[test]
    fn test_yaml_colon_in_value() {
        // Test colons in values (common gotcha)
        // Colons in flow context and quoted strings are fine
        // Unquoted colons need proper spacing or quoting
        let yaml = r#"
url: http://example.com
time: "10:30:00"
message: "key: value pair"
"#;
        let value: Value = from_str(yaml).unwrap();
        assert_eq!(
            value.get("url").and_then(|v| v.as_str()),
            Some("http://example.com")
        );
        assert_eq!(value.get("time").and_then(|v| v.as_str()), Some("10:30:00"));
        assert_eq!(
            value.get("message").and_then(|v| v.as_str()),
            Some("key: value pair")
        );
    }

    #[test]
    fn test_yaml_roundtrip_complex() {
        // Test roundtrip with complex nested structures
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct ComplexConfig {
            name: String,
            enabled: bool,
            count: i64,
            ratio: f64,
            tags: Vec<String>,
            metadata: std::collections::HashMap<String, String>,
        }

        let original = ComplexConfig {
            name: "test".into(),
            enabled: true,
            count: 42,
            ratio: 3.15,
            tags: vec!["a".into(), "b".into(), "c".into()],
            metadata: [
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ]
            .into_iter()
            .collect(),
        };

        let yaml = to_string(&original).unwrap();
        let parsed: ComplexConfig = from_str(&yaml).unwrap();
        assert_eq!(original, parsed);
    }

    // ============================================================
    // Serde Feature Tests
    // ============================================================

    #[test]
    fn test_serde_newtype_struct() {
        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        struct UserId(u64);

        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        struct UserName(String);

        let yaml = "42";
        let id: UserId = from_str(yaml).expect("should parse newtype u64");
        assert_eq!(id, UserId(42));

        let yaml = "alice";
        let name: UserName = from_str(yaml).expect("should parse newtype string");
        assert_eq!(name, UserName("alice".into()));

        // Roundtrip
        let id = UserId(99);
        let yaml = to_string(&id).expect("should serialize newtype");
        let parsed: UserId = from_str(&yaml).expect("should roundtrip newtype");
        assert_eq!(parsed, UserId(99));
    }

    #[test]
    fn test_serde_newtype_struct_in_struct() {
        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        struct Port(u16);

        #[derive(Debug, Deserialize, Serialize, PartialEq)]
        struct ServerConfig {
            host: String,
            port: Port,
        }

        let yaml = r"
host: localhost
port: 8080
";
        let config: ServerConfig = from_str(yaml).expect("should parse struct with newtype field");
        assert_eq!(
            config,
            ServerConfig {
                host: "localhost".into(),
                port: Port(8080),
            }
        );
    }

    #[test]
    fn test_serde_deny_unknown_fields() {
        #[derive(Debug, Deserialize, PartialEq)]
        #[serde(deny_unknown_fields)]
        struct Strict {
            name: String,
            age: u32,
        }

        // Valid: only known fields
        let yaml = r"
name: Alice
age: 30
";
        let result: Strict = from_str(yaml).expect("should parse known fields");
        assert_eq!(
            result,
            Strict {
                name: "Alice".into(),
                age: 30,
            }
        );

        // Invalid: unknown field present
        let yaml = r"
name: Bob
age: 25
email: bob@example.com
";
        let result: Result<Strict> = from_str(yaml);
        assert!(
            result.is_err(),
            "should reject unknown field 'email' with deny_unknown_fields"
        );
    }

    #[test]
    fn test_serde_untagged_enum() {
        #[derive(Debug, Deserialize, PartialEq)]
        #[serde(untagged)]
        enum StringOrInt {
            Int(i64),
            Str(String),
        }

        let yaml = "42";
        let result: StringOrInt = from_str(yaml).expect("should parse untagged int");
        assert_eq!(result, StringOrInt::Int(42));

        let yaml = "\"hello\"";
        let result: StringOrInt = from_str(yaml).expect("should parse untagged string");
        assert_eq!(result, StringOrInt::Str("hello".into()));
    }

    #[expect(clippy::approx_constant)]
    #[test]
    fn test_serde_untagged_enum_in_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        #[serde(untagged)]
        enum Value2 {
            Num(f64),
            Text(String),
            List(Vec<String>),
        }

        #[derive(Debug, Deserialize, PartialEq)]
        struct Entry {
            name: String,
            data: Value2,
        }

        let yaml = r"
name: numbers
data: 3.14
";
        let result: Entry = from_str(yaml).expect("should parse untagged float variant");
        assert_eq!(
            result,
            Entry {
                name: "numbers".into(),
                data: Value2::Num(3.14),
            }
        );

        let yaml = r"
name: words
data: hello
";
        let result: Entry = from_str(yaml).expect("should parse untagged string variant");
        assert_eq!(
            result,
            Entry {
                name: "words".into(),
                data: Value2::Text("hello".into()),
            }
        );

        let yaml = r"
name: items
data:
  - one
  - two
";
        let result: Entry = from_str(yaml).expect("should parse untagged list variant");
        assert_eq!(
            result,
            Entry {
                name: "items".into(),
                data: Value2::List(vec!["one".into(), "two".into()]),
            }
        );
    }

    // ============================================================
    // YAML Spec §6.1: Indentation — Error Cases
    // ============================================================

    #[test]
    fn test_yaml_tab_in_indentation() {
        // YAML §6.1: "tab characters must not be used in indentation"
        let yaml = "key:\n\t- value1\n\t- value2";
        let result: Result<Value> = from_str(yaml);
        assert!(
            result.is_err(),
            "should reject tab characters used for indentation"
        );
    }

    #[test]
    fn test_yaml_inconsistent_sibling_indentation() {
        // YAML §6.1: "All sibling nodes must use the exact same indentation level"
        let yaml = r"
parent:
  child1: a
    child2: b
";
        let result: Result<Value> = from_str(yaml);
        // Inconsistent indentation among siblings should be an error
        // (child2 is indented more than child1 but both are children of parent)
        assert!(
            result.is_err(),
            "should reject inconsistent sibling indentation"
        );
    }

    // ============================================================
    // YAML Spec §5.1/§5.7: Characters & Escape Sequences
    // ============================================================

    #[test]
    fn test_yaml_control_characters_allowed() {
        // TAB (x09), LF (x0A), CR (x0D) ARE allowed
        let yaml = "key: val\tue";
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_ok(), "should allow tab character in value");
    }

    #[test]
    fn test_yaml_invalid_escape_character() {
        // YAML §5.7 Example 5.14: Invalid escaped characters
        let yaml = r#"bad: "\c""#;
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_err(), "should reject invalid escape \\c");

        let yaml = r#"bad: "\m""#;
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_err(), "should reject invalid escape \\m");

        let yaml = r#"bad: "\w""#;
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_err(), "should reject invalid escape \\w");
    }

    #[test]
    fn test_yaml_invalid_hex_escape() {
        // YAML §5.7: Invalid hex digits in escape sequences
        let yaml = r#"bad: "\xZZ""#;
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_err(), "should reject invalid hex escape \\xZZ");

        let yaml = r#"bad: "\uGGGG""#;
        let result: Result<Value> = from_str(yaml);
        assert!(
            result.is_err(),
            "should reject invalid unicode escape \\uGGGG"
        );

        let yaml = r#"bad: "\UZZZZZZZZ""#;
        let result: Result<Value> = from_str(yaml);
        assert!(
            result.is_err(),
            "should reject invalid 32-bit unicode escape"
        );
    }

    #[test]
    fn test_yaml_escape_vertical_tab() {
        // YAML §5.7 [47]: \v → x0B
        let yaml = r#"value: "\v""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\v escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x0B"));
    }

    #[test]
    fn test_yaml_escape_form_feed() {
        // YAML §5.7 [48]: \f → x0C
        let yaml = r#"value: "\f""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\f escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x0C"));
    }

    #[test]
    fn test_yaml_escape_carriage_return() {
        // YAML §5.7 [49]: \r → x0D
        let yaml = r#"value: "\r""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\r escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\r"));
    }

    #[test]
    fn test_yaml_escape_null() {
        // YAML §5.7 [42]: \0 → x00
        let yaml = r#"value: "\0""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\0 escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x00"));
    }

    #[test]
    fn test_yaml_escape_bell() {
        // YAML §5.7 [43]: \a → x07
        let yaml = r#"value: "\a""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\a escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x07"));
    }

    #[test]
    fn test_yaml_escape_backspace() {
        // YAML §5.7 [44]: \b → x08
        let yaml = r#"value: "\b""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\b escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x08"));
    }

    #[test]
    fn test_yaml_escape_escape_char() {
        // YAML §5.7 [50]: \e → x1B
        let yaml = r#"value: "\e""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\e escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("\x1B"));
    }

    #[test]
    fn test_yaml_escape_space() {
        // YAML §5.7 [51]: \<space> → x20
        let yaml = "value: \"\\ \"";
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse escaped space");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some(" "));
    }

    #[test]
    fn test_yaml_escape_slash() {
        // YAML §5.7 [53]: \/ → x2F (JSON compatibility)
        let yaml = r#"value: "\/""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\/ escape");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("/"));
    }

    #[test]
    fn test_yaml_escape_next_line() {
        // YAML §5.7 [55]: \N → x85 (NEL)
        let yaml = r#"value: "\N""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\N escape (NEL)");
        assert_eq!(
            value.get("value").and_then(|v| v.as_str()),
            Some("\u{0085}")
        );
    }

    #[test]
    fn test_yaml_escape_non_breaking_space() {
        // YAML §5.7 [56]: \_ → xA0 (non-breaking space)
        let yaml = r#"value: "\_""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\_ escape (NBSP)");
        assert_eq!(
            value.get("value").and_then(|v| v.as_str()),
            Some("\u{00A0}")
        );
    }

    #[test]
    fn test_yaml_escape_line_separator() {
        // YAML §5.7 [57]: \L → x2028 (line separator)
        let yaml = r#"value: "\L""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\L escape (line separator)");
        assert_eq!(
            value.get("value").and_then(|v| v.as_str()),
            Some("\u{2028}")
        );
    }

    #[test]
    fn test_yaml_escape_paragraph_separator() {
        // YAML §5.7 [58]: \P → x2029 (paragraph separator)
        let yaml = r#"value: "\P""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\P escape (paragraph separator)");
        assert_eq!(
            value.get("value").and_then(|v| v.as_str()),
            Some("\u{2029}")
        );
    }

    #[test]
    fn test_yaml_escape_8bit_hex() {
        // YAML §5.7 [59]: \xNN → 8-bit Unicode
        let yaml = r#"value: "\x41""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\x41 (letter A)");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("A"));
    }

    #[test]
    fn test_yaml_escape_16bit_unicode() {
        // YAML §5.7 [60]: \uNNNN → 16-bit Unicode
        let yaml = r#"value: "\u0041""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\u0041 (letter A)");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("A"));
    }

    #[test]
    fn test_yaml_escape_32bit_unicode() {
        // YAML §5.7 [61]: \UNNNNNNNN → 32-bit Unicode
        let yaml = r#"value: "\U00000041""#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse \\U00000041 (letter A)");
        assert_eq!(value.get("value").and_then(|v| v.as_str()), Some("A"));
    }

    #[test]
    fn test_yaml_escape_combined() {
        // YAML §5.7 Example 5.13: Combined escape sequences
        let yaml = r#"
fun_with_backslash: "Fun with \\"
special_chars: "\" \a \b \e \f"
whitespace: "\n \r \t \v \0"
unicode_chars: "\  \_ \N \L \P \x41 \u0041 \U00000041"
"#;
        let result: Result<Value> = from_str(yaml);
        let value = result.expect("should parse combined escape sequences");

        assert_eq!(
            value.get("fun_with_backslash").and_then(|v| v.as_str()),
            Some("Fun with \\")
        );
        assert_eq!(
            value.get("special_chars").and_then(|v| v.as_str()),
            Some("\" \x07 \x08 \x1B \x0C")
        );
        assert_eq!(
            value.get("whitespace").and_then(|v| v.as_str()),
            Some("\n \r \t \x0B \x00")
        );
        assert_eq!(
            value.get("unicode_chars").and_then(|v| v.as_str()),
            Some("  \u{00A0} \u{0085} \u{2028} \u{2029} A A A")
        );
    }

    // ============================================================
    // YAML Spec §6.6/§8.1: Comments
    // ============================================================

    #[test]
    fn test_yaml_comment_after_block_scalar_header() {
        // YAML §8.1.1: Block scalar header may have a trailing comment
        let yaml = r"
literal: | # This is a comment on the header
  some text here
folded: > # Another header comment
  folded text here
";
        let value: Value = from_str(yaml).expect("should parse block scalar with header comment");
        let literal = value.get("literal").and_then(|v| v.as_str()).unwrap();
        assert!(
            literal.contains("some text here"),
            "literal content should not include the header comment"
        );
        let folded = value.get("folded").and_then(|v| v.as_str()).unwrap();
        assert!(
            folded.contains("folded text here"),
            "folded content should not include the header comment"
        );
    }

    #[test]
    fn test_yaml_comment_after_chomping_indicator() {
        // Block scalar with chomping indicator AND comment
        let yaml = r"
strip: |- # strip comment
  stripped text
keep: |+ # keep comment
  kept text

";
        let value: Value =
            from_str(yaml).expect("should parse block scalar with chomping + comment");
        let strip = value.get("strip").and_then(|v| v.as_str()).unwrap();
        assert_eq!(strip, "stripped text");
        let keep = value.get("keep").and_then(|v| v.as_str()).unwrap();
        assert!(keep.starts_with("kept text"));
    }

    #[test]
    fn test_yaml_comment_only_document() {
        // A stream with only comments and no content nodes
        let yaml = r"
# This is just a comment
# Another comment line
";
        let value: Value = from_str(yaml).expect("should parse comment-only document");
        assert!(
            value.is_null(),
            "comment-only document should resolve to null"
        );
    }

    #[test]
    fn test_yaml_comment_only_with_markers() {
        // Document markers with only comments
        let yaml = r"
---
# only a comment
...
";
        let value: Value = from_str(yaml).expect("should parse comment-only with markers");
        assert!(value.is_null(), "should resolve to null");
    }

    // ============================================================
    // YAML Spec §7.4: Flow Collections — Edge Cases
    // ============================================================

    #[test]
    fn test_yaml_trailing_comma_flow_sequence() {
        // YAML §7.4.1: "The final ',' may be omitted"
        // Trailing comma should be valid
        let yaml = "[1, 2, 3,]";
        let result: Vec<i32> =
            from_str(yaml).expect("should parse flow sequence with trailing comma");
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn test_yaml_trailing_comma_flow_mapping() {
        // YAML §7.4.2: Trailing comma in flow mapping
        let yaml = "{a: 1, b: 2,}";
        let value: Value = from_str(yaml).expect("should parse flow mapping with trailing comma");
        assert_eq!(value.get("a").and_then(Value::as_u64), Some(1));
        assert_eq!(value.get("b").and_then(Value::as_u64), Some(2));
    }

    #[test]
    fn test_yaml_trailing_comma_nested() {
        // Trailing commas in nested flow collections
        let yaml = "{items: [1, 2, 3,], name: test,}";
        let value: Value =
            from_str(yaml).expect("should parse nested flow collections with trailing commas");
        let items = value.get("items").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_yaml_empty_flow_sequence() {
        // Empty flow sequence
        let yaml = "items: []";
        let value: Value = from_str(yaml).expect("should parse empty flow sequence");
        let items = value.get("items").and_then(|v| v.as_sequence()).unwrap();
        assert!(items.is_empty(), "empty flow sequence should have 0 items");
    }

    #[test]
    fn test_yaml_empty_flow_mapping() {
        // Empty flow mapping
        let yaml = "data: {}";
        let value: Value = from_str(yaml).expect("should parse empty flow mapping");
        let data = value.get("data").and_then(|v| v.as_mapping()).unwrap();
        assert!(data.is_empty(), "empty flow mapping should have 0 entries");
    }

    #[test]
    fn test_yaml_empty_flow_collections_bare() {
        // Bare empty collections as top-level
        let yaml = "[]";
        let result: Vec<i32> = from_str(yaml).expect("should parse bare empty sequence");
        assert!(result.is_empty());

        let yaml = "{}";
        let value: Value = from_str(yaml).expect("should parse bare empty mapping");
        assert!(value.is_mapping());
        assert!(value.as_mapping().unwrap().is_empty());
    }

    #[test]
    fn test_yaml_empty_flow_collections_in_struct() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Config {
            tags: Vec<String>,
            metadata: std::collections::HashMap<String, String>,
        }

        let yaml = r"
tags: []
metadata: {}
";
        let config: Config = from_str(yaml).expect("should parse struct with empty collections");
        assert!(config.tags.is_empty());
        assert!(config.metadata.is_empty());
    }

    // ============================================================
    // YAML Spec §8.2: Compact Nested Notation
    // ============================================================

    #[test]
    fn test_yaml_compact_nested_sequence_in_mapping() {
        // YAML §8.2.1 Example 8.15: Compact block sequence entries
        let yaml = r"
- one: two
  three: four
- five: six
";
        let value: Value = from_str(yaml).expect("should parse compact nested mapping in sequence");
        let seq = value.as_sequence().unwrap();
        assert_eq!(seq.len(), 2);
        assert_eq!(seq[0].get("one").and_then(|v| v.as_str()), Some("two"));
        assert_eq!(seq[0].get("three").and_then(|v| v.as_str()), Some("four"));
        assert_eq!(seq[1].get("five").and_then(|v| v.as_str()), Some("six"));
    }

    #[test]
    fn test_yaml_compact_nested_sequence_in_sequence() {
        // YAML §8.2.1 Example 8.15: Compact nested sequences
        let yaml = r"
- - one
  - two
- - three
  - four
";
        let value: Value = from_str(yaml).expect("should parse compact nested sequences");
        let outer = value.as_sequence().unwrap();
        assert_eq!(outer.len(), 2);
        let inner0 = outer[0].as_sequence().unwrap();
        assert_eq!(inner0.len(), 2);
        assert_eq!(inner0[0].as_str(), Some("one"));
        assert_eq!(inner0[1].as_str(), Some("two"));
    }

    #[test]
    fn test_yaml_compact_nested_mapping_value() {
        // Compact mapping as value in another mapping
        let yaml = r"
products:
- item: Super Hoop
  quantity: 1
- item: Basketball
  quantity: 4
- item: Big Shoes
  quantity: 1
";
        let value: Value =
            from_str(yaml).expect("should parse compact nested mapping in sequence value");
        let products = value.get("products").and_then(|v| v.as_sequence()).unwrap();
        assert_eq!(products.len(), 3);
        assert_eq!(
            products[0].get("item").and_then(Value::as_str),
            Some("Super Hoop")
        );
        assert_eq!(products[0].get("quantity").and_then(Value::as_u64), Some(1));
        assert_eq!(
            products[2].get("item").and_then(Value::as_str),
            Some("Big Shoes")
        );
    }

    // ============================================================
    // YAML Spec §7.3.2: Multi-line Single-Quoted Scalars
    // ============================================================

    #[test]
    fn test_yaml_multiline_single_quoted() {
        // YAML §7.3.2 Example 7.9: Multi-line single-quoted scalar
        // Line breaks are folded into spaces, empty lines become line feeds
        let yaml = "value: ' 1st non-empty\n\n 2nd non-empty \n 3rd non-empty '";
        let value: Value = from_str(yaml).expect("should parse multi-line single-quoted scalar");
        let s = value.get("value").and_then(|v| v.as_str()).unwrap();
        // Per YAML spec, single newline folds to space, blank line becomes \n
        assert_eq!(s, " 1st non-empty\n2nd non-empty 3rd non-empty ");
    }

    #[test]
    fn test_yaml_multiline_single_quoted_simple() {
        // Simple multi-line single-quoted string
        let yaml = "value: 'line one\n  line two'";
        let value: Value =
            from_str(yaml).expect("should parse simple multi-line single-quoted scalar");
        let s = value.get("value").and_then(|v| v.as_str()).unwrap();
        // Single newline folds to space
        assert_eq!(s, "line one line two");
    }

    #[test]
    fn test_yaml_single_quoted_no_escapes() {
        // YAML §7.3.2: No escape sequences in single-quoted (except '' for ')
        // The \n should be literal backslash-n, not a newline
        let yaml = r"value: 'no \n escape'";
        let value: Value =
            from_str(yaml).expect("should parse single-quoted with literal backslash");
        assert_eq!(
            value.get("value").and_then(|v| v.as_str()),
            Some("no \\n escape")
        );
    }
}
