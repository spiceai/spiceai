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

// ============================================================
// Multi-document YAML support
// ============================================================

/// Deserialize multiple instances of type `T` from a multi-document YAML string.
///
/// YAML supports multiple documents separated by `---`. This function parses
/// all documents and deserializes each one into the target type.
///
/// # Errors
///
/// Returns an error if the YAML string is invalid or any document cannot be
/// deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug, PartialEq)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = "---\nname: first\n---\nname: second";
/// let configs: Vec<Config> = yaml::from_str_multi(yaml).unwrap();
/// assert_eq!(configs.len(), 2);
/// assert_eq!(configs[0].name, "first");
/// assert_eq!(configs[1].name, "second");
/// ```
pub fn from_str_multi<'de, T>(s: &'de str) -> Result<Vec<T>>
where
    T: Deserialize<'de>,
{
    let values = de::parse_yaml_multi(s)?;
    values
        .into_iter()
        .map(|v| T::deserialize(de::ValueDeserializer::new(v)))
        .collect()
}

/// Deserialize multiple instances of type `T` from an I/O reader containing multi-document YAML.
///
/// # Errors
///
/// Returns an error if reading fails, the YAML is invalid, or any document cannot
/// be deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
/// use std::io::Cursor;
///
/// #[derive(Deserialize, Debug, PartialEq)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = "---\nname: first\n---\nname: second";
/// let reader = Cursor::new(yaml);
/// let configs: Vec<Config> = yaml::from_reader_multi(reader).unwrap();
/// assert_eq!(configs.len(), 2);
/// ```
pub fn from_reader_multi<R, T>(mut reader: R) -> Result<Vec<T>>
where
    R: Read,
    T: for<'de> Deserialize<'de>,
{
    let mut s = String::new();
    reader.read_to_string(&mut s)?;
    from_str_multi(&s)
}

/// Deserialize multiple instances of type `T` from a byte slice containing multi-document YAML.
///
/// # Errors
///
/// Returns an error if the bytes are not valid UTF-8, the YAML is invalid,
/// or any document cannot be deserialized into the target type.
///
/// # Example
///
/// ```
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug, PartialEq)]
/// struct Config {
///     name: String,
/// }
///
/// let yaml = b"---\nname: first\n---\nname: second";
/// let configs: Vec<Config> = yaml::from_slice_multi(yaml).unwrap();
/// assert_eq!(configs.len(), 2);
/// ```
pub fn from_slice_multi<T>(slice: &[u8]) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let s = std::str::from_utf8(slice)
        .map_err(|e| Error::from(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?;
    from_str_multi(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn test_from_str_simple() {
        let yaml = "42";
        let result: i32 = from_str(yaml).expect("test");
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
        let config: Config = from_str(yaml).expect("test");
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
        let outer: Outer = from_str(yaml).expect("test");
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
        let result: Vec<i32> = from_str(yaml).expect("test");
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
        let result: HashMap<String, i32> = from_str(yaml).expect("test");
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
        let result: Status = from_str(yaml).expect("test");
        assert_eq!(result, Status::Active);
    }

    #[test]
    fn test_from_str_option() {
        let yaml = "null";
        let result: Option<i32> = from_str(yaml).expect("test");
        assert_eq!(result, None);

        let yaml = "42";
        let result: Option<i32> = from_str(yaml).expect("test");
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

        let config: Config = from_reader(reader).expect("test");
        assert_eq!(config.name, "test");
    }

    #[test]
    fn test_to_string_simple() {
        let yaml = to_string(&42).expect("test");
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
        let yaml = to_string(&config).expect("test");
        assert!(yaml.contains("name: test"));
        assert!(yaml.contains("count: 42"));
    }

    #[test]
    fn test_to_string_sequence() {
        let vec = vec![1, 2, 3];
        let yaml = to_string(&vec).expect("test");
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
        let value = to_value(&config).expect("test");
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

        let config: Config = from_value(value).expect("test");
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

        let yaml = to_string(&original).expect("test");
        let parsed: Config = from_str(&yaml).expect("test");
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_special_yaml_values() {
        // Test that we handle special YAML boolean strings
        let yaml = "yes";
        let result: bool = from_str(yaml).expect("test");
        assert!(result);

        let yaml = "no";
        let result: bool = from_str(yaml).expect("test");
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

        let config: Config = from_str(yaml).expect("test");
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

        let config: Config = from_str(yaml).expect("test");
        assert_eq!(config.name, "myapp");
        assert_eq!(config.server.address, "localhost");
        assert_eq!(config.server.databases.len(), 2);
        assert_eq!(config.server.databases[0].host, "db1.example.com");
        assert_eq!(config.server.databases[0].port, 5432);

        // Test roundtrip
        let yaml_out = to_string(&config).expect("test");
        let parsed: Config = from_str(&yaml_out).expect("test");
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_error_on_invalid_yaml() {
        let yaml = "key: [unclosed bracket";
        let result: Result<Value> = from_str(yaml);
        let _ = result.expect_err("should fail on invalid YAML");
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
        let value: Value = from_str(yaml).expect("test");
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

        let yaml = to_string(&config).expect("test");
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
        let config: Config = from_str(yaml).expect("test");
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
        let config: Config = from_str(yaml).expect("test");
        assert_eq!(config.name, "test");

        let config = Config {
            name: "example".into(),
        };
        let yaml = to_string(&config).expect("test");
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
        let config: Config = from_str(yaml).expect("test");
        assert_eq!(config.name, "test");

        // Test alias
        let yaml = "display_name: test";
        let config: Config = from_str(yaml).expect("test");
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
        let outer: Outer = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");

        // Check defaults
        let defaults = value.get("defaults").expect("test");
        assert_eq!(
            defaults.get("adapter").and_then(|v| v.as_str()),
            Some("postgres")
        );
        assert_eq!(
            defaults.get("host").and_then(|v| v.as_str()),
            Some("localhost")
        );

        // Check that alias resolves correctly
        let settings = value
            .get("development")
            .expect("test")
            .get("settings")
            .expect("test");
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
        let value: Value = from_str(yaml).expect("test");

        let colors = value
            .get("colors")
            .and_then(|v| v.as_sequence())
            .expect("test");
        assert_eq!(colors.len(), 3);

        let primary = value
            .get("primary_colors")
            .and_then(|v| v.as_sequence())
            .expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let refs = value
            .get("refs")
            .and_then(|v| v.as_sequence())
            .expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let dev = value.get("development").expect("test");

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
        let value: Value = from_str(yaml).expect("test");
        let prod = value.get("production").expect("test");

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
        let value: Value = from_str(yaml).expect("test");
        let combined = value.get("combined").expect("test");

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
        let value: Value = from_str(yaml).expect("test");
        assert!(value.get("null1").expect("test").is_null());
        assert!(value.get("null2").expect("test").is_null());
        assert!(value.get("null3").expect("test").is_null());

        // Verify that capitalized versions are strings in YAML 1.2
        let yaml_11_style = r"
null_cap: Null
null_upper: NULL
";
        let value: Value = from_str(yaml_11_style).expect("test");
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
        let value: Value = from_str(yaml).expect("test");

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
        let value: Value = from_str(yaml_11_style).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
float1: 3.125
float2: -0.5
scientific: 1.2e+3
infinity: .inf
neg_infinity: -.inf
not_a_number: .nan
";
        let value: Value = from_str(yaml).expect("test");

        assert!(
            (value
                .get("float1")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                - 3.125)
                .abs()
                < 0.001
        );
        assert!(
            (value
                .get("float2")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                - (-0.5))
                .abs()
                < 0.001
        );
        assert!(
            (value
                .get("scientific")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                - 1200.0)
                .abs()
                < 0.001
        );
        assert!(
            value
                .get("infinity")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                .is_infinite()
        );
        assert!(
            value
                .get("neg_infinity")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                .is_infinite()
        );
        assert!(
            value
                .get("neg_infinity")
                .and_then(super::value::Value::as_f64)
                .expect("test")
                .is_sign_negative()
        );
        assert!(
            value
                .get("not_a_number")
                .and_then(super::value::Value::as_f64)
                .expect("test")
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let literal = value.get("literal").and_then(|v| v.as_str()).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let folded = value.get("folded").and_then(|v| v.as_str()).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let strip = value.get("strip").and_then(|v| v.as_str()).expect("test");
        let clip = value.get("clip").and_then(|v| v.as_str()).expect("test");
        let keep = value.get("keep").and_then(|v| v.as_str()).expect("test");

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
        let value: Value = from_str(yaml).expect("test");
        let flow = value
            .get("flow")
            .and_then(|v| v.as_sequence())
            .expect("test");
        assert_eq!(flow.len(), 5);

        let nested = value
            .get("nested")
            .and_then(|v| v.as_sequence())
            .expect("test");
        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].as_sequence().expect("test").len(), 2);
    }

    #[test]
    fn test_yaml_flow_mapping() {
        let yaml = r"
flow: {name: John, age: 30}
nested: {outer: {inner: value}}
";
        let value: Value = from_str(yaml).expect("test");
        let flow = value.get("flow").expect("test");
        assert_eq!(flow.get("name").and_then(|v| v.as_str()), Some("John"));
        assert_eq!(
            flow.get("age").and_then(super::value::Value::as_i64),
            Some(30)
        );

        let nested = value.get("nested").expect("test");
        assert_eq!(
            nested
                .get("outer")
                .expect("test")
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
        let value: Value = from_str(yaml).expect("test");
        let items = value
            .get("items")
            .and_then(|v| v.as_sequence())
            .expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        // !!str should make it a string
        assert_eq!(
            value.get("string_num").and_then(|v| v.as_str()),
            Some("123")
        );
        // !!float should make it a float
        let float_val = value.get("float_val").and_then(super::value::Value::as_f64);
        assert!(float_val.is_some());
        assert!((float_val.expect("test") - 42.0).abs() < 0.001);
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
        let value: Value = from_str(yaml).expect("test");
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
        let list = value
            .get("list")
            .and_then(|v| v.as_sequence())
            .expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        assert_eq!(value.get("empty_string").and_then(|v| v.as_str()), Some(""));
        assert!(
            value
                .get("empty_array")
                .and_then(|v| v.as_sequence())
                .expect("test")
                .is_empty()
        );
        assert!(value.get("empty_map").expect("test").is_mapping());
    }

    #[test]
    fn test_yaml_special_characters_in_strings() {
        let yaml = r#"
colon: "has: colon"
hash: "has # hash"
bracket: "has [bracket]"
brace: "has {brace}"
"#;
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        let deep = value
            .get("level1")
            .expect("test")
            .get("level2")
            .expect("test")
            .get("level3")
            .expect("test")
            .get("level4")
            .expect("test")
            .get("level5")
            .expect("test")
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
        let value: Value = from_str(yaml).expect("test");
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
                .expect("test")
                > 1e300
        );
    }

    #[test]
    fn test_yaml_document_markers() {
        // Test document start/end markers
        let yaml = r"---
key: value
...";
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        assert_eq!(
            value
                .get("two_space")
                .expect("test")
                .get("nested")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("four_space")
                .expect("test")
                .get("deeply")
                .expect("test")
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
        let value: Value = from_str(yaml).expect("test");
        let derived = value.get("derived").expect("test");
        assert_eq!(derived.get("name").and_then(|v| v.as_str()), Some("base"));
        let config = derived.get("config").expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        assert_eq!(
            value
                .get("use1")
                .expect("test")
                .get("key")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("use2")
                .expect("test")
                .get("key")
                .and_then(|v| v.as_str()),
            Some("value")
        );
        assert_eq!(
            value
                .get("use3")
                .expect("test")
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
        let value: Value = from_str(yaml).expect("test");
        let merged = value.get("merged").expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
        assert!(value.is_null());

        let yaml = "   \n\n   ";
        let value: Value = from_str(yaml).expect("test");
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
        let value: Value = from_str(yaml).expect("test");
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
            ratio: 3.125,
            tags: vec!["a".into(), "b".into(), "c".into()],
            metadata: [
                ("key1".into(), "value1".into()),
                ("key2".into(), "value2".into()),
            ]
            .into_iter()
            .collect(),
        };

        let yaml = to_string(&original).expect("serialization should work");
        let parsed: ComplexConfig = from_str(&yaml).expect("deserialization should work");
        assert_eq!(original, parsed);
    }

    // ============================================================
    // Multi-document YAML tests
    // ============================================================

    #[test]
    fn test_from_str_multi_basic() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            name: String,
        }

        let yaml = "---\nname: first\n---\nname: second\n---\nname: third";
        let docs: Vec<Doc> = from_str_multi(yaml).expect("valid multi-doc YAML");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].name, "first");
        assert_eq!(docs[1].name, "second");
        assert_eq!(docs[2].name, "third");
    }

    #[test]
    fn test_from_str_multi_single_doc() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            value: i32,
        }

        let yaml = "value: 42";
        let docs: Vec<Doc> = from_str_multi(yaml).expect("valid YAML");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].value, 42);
    }

    #[test]
    fn test_from_str_multi_empty() {
        let yaml = "";
        let docs: Vec<Value> = from_str_multi(yaml).expect("empty YAML is valid");
        assert!(docs.is_empty());
    }

    #[test]
    fn test_from_str_multi_different_types() {
        // Each document can have different structure
        let yaml = "---\n42\n---\nhello\n---\n- a\n- b";
        let docs: Vec<Value> = from_str_multi(yaml).expect("valid multi-doc YAML");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].as_i64(), Some(42));
        assert_eq!(docs[1].as_str(), Some("hello"));
        assert!(docs[2].is_sequence());
    }

    #[test]
    fn test_from_str_multi_with_document_end() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            name: String,
        }

        // YAML also supports `...` to end a document
        let yaml = "---\nname: first\n...\n---\nname: second";
        let docs: Vec<Doc> = from_str_multi(yaml).expect("valid multi-doc YAML");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].name, "first");
        assert_eq!(docs[1].name, "second");
    }

    #[test]
    fn test_from_reader_multi() {
        use std::io::Cursor;

        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            id: u32,
        }

        let yaml = "---\nid: 1\n---\nid: 2";
        let reader = Cursor::new(yaml);
        let docs: Vec<Doc> = from_reader_multi(reader).expect("valid multi-doc YAML");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, 1);
        assert_eq!(docs[1].id, 2);
    }

    #[test]
    fn test_from_slice_multi() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            key: String,
        }

        let yaml = b"---\nkey: a\n---\nkey: b";
        let docs: Vec<Doc> = from_slice_multi(yaml).expect("valid multi-doc YAML");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].key, "a");
        assert_eq!(docs[1].key, "b");
    }

    #[test]
    fn test_multi_doc_complex_structures() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Server {
            name: String,
            port: u16,
            tags: Vec<String>,
        }

        let yaml = r"
---
name: server1
port: 8080
tags:
  - production
  - web
---
name: server2
port: 9090
tags:
  - staging
";
        let servers: Vec<Server> = from_str_multi(yaml).expect("valid multi-doc YAML");
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "server1");
        assert_eq!(servers[0].port, 8080);
        assert_eq!(servers[0].tags, vec!["production", "web"]);
        assert_eq!(servers[1].name, "server2");
        assert_eq!(servers[1].port, 9090);
        assert_eq!(servers[1].tags, vec!["staging"]);
    }

    #[test]
    fn test_single_doc_error_on_multi() {
        // The single-doc function should error on multi-doc input
        let yaml = "---\nfirst: 1\n---\nsecond: 2";
        let result: Result<Value> = from_str(yaml);
        assert!(result.is_err());
        let err = result.expect_err("should be an error");
        assert!(
            err.to_string().contains("multi-document"),
            "Error should mention multi-document: {err}"
        );
    }

    // ============================================================
    // Comprehensive Single-document Tests
    // ============================================================

    #[test]
    fn test_single_doc_with_explicit_start() {
        // Single document with explicit document start marker
        let yaml = "---\nkey: value";
        let value: Value = from_str(yaml).expect("single doc with --- is valid");
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_single_doc_with_explicit_end() {
        // Single document with explicit document end marker
        let yaml = "key: value\n...";
        let value: Value = from_str(yaml).expect("single doc with ... is valid");
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_single_doc_with_both_markers() {
        // Single document with both start and end markers
        let yaml = "---\nkey: value\n...";
        let value: Value = from_str(yaml).expect("single doc with both markers is valid");
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn test_single_doc_null() {
        let yaml = "null";
        let value: Value = from_str(yaml).expect("null is valid");
        assert!(value.is_null());
    }

    #[test]
    fn test_single_doc_scalar_types() {
        // Integer
        let yaml = "42";
        let value: i64 = from_str(yaml).expect("integer is valid");
        assert_eq!(value, 42);

        // Negative integer
        let yaml = "-42";
        let value: i64 = from_str(yaml).expect("negative integer is valid");
        assert_eq!(value, -42);

        // Float
        let yaml = "3.125";
        let value: f64 = from_str(yaml).expect("float is valid");
        assert!((value - 3.125).abs() < f64::EPSILON);

        // Boolean true
        let yaml = "true";
        let value: bool = from_str(yaml).expect("bool is valid");
        assert!(value);

        // Boolean false
        let yaml = "false";
        let value: bool = from_str(yaml).expect("bool is valid");
        assert!(!value);

        // String
        let yaml = "hello world";
        let value: String = from_str(yaml).expect("string is valid");
        assert_eq!(value, "hello world");
    }

    #[test]
    fn test_single_doc_with_comments() {
        let yaml = r"
# This is a comment
key: value  # inline comment
# another comment
other: stuff
";
        let value: Value = from_str(yaml).expect("comments are valid");
        assert_eq!(value.get("key").and_then(|v| v.as_str()), Some("value"));
        assert_eq!(value.get("other").and_then(|v| v.as_str()), Some("stuff"));
    }

    #[test]
    fn test_single_doc_empty_mapping() {
        let yaml = "{}";
        let value: Value = from_str(yaml).expect("empty mapping is valid");
        assert!(value.is_mapping());
        assert_eq!(value.as_mapping().map(Mapping::len), Some(0));
    }

    #[test]
    fn test_single_doc_empty_sequence() {
        let yaml = "[]";
        let value: Value = from_str(yaml).expect("empty sequence is valid");
        assert!(value.is_sequence());
        assert_eq!(value.as_sequence().map(Vec::len), Some(0));
    }

    // ============================================================
    // Comprehensive Multi-document Tests
    // ============================================================

    #[test]
    fn test_multi_doc_with_null_documents() {
        // Multi-doc where some documents are null
        let yaml = "---\nfirst: 1\n---\nnull\n---\nthird: 3";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc with null is valid");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].get("first").and_then(Value::as_i64), Some(1));
        assert!(docs[1].is_null());
        assert_eq!(docs[2].get("third").and_then(Value::as_i64), Some(3));
    }

    #[test]
    fn test_multi_doc_with_empty_documents() {
        // Multi-doc where some documents are empty (implicit null)
        let yaml = "---\n---\nkey: value\n---\n";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc with empty docs is valid");
        // First doc is empty (null), second has key: value, third is empty (null)
        assert_eq!(docs.len(), 3);
        assert!(docs[0].is_null(), "first empty doc should be null");
        assert_eq!(docs[1].get("key").and_then(Value::as_str), Some("value"));
        assert!(docs[2].is_null(), "trailing empty doc should be null");
    }

    #[test]
    fn test_multi_doc_with_comments_between() {
        let yaml = r"
---
# First document
first: 1
---
# Second document with comment
second: 2
---
# Third
third: 3
";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc with comments is valid");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].get("first").and_then(Value::as_i64), Some(1));
        assert_eq!(docs[1].get("second").and_then(Value::as_i64), Some(2));
        assert_eq!(docs[2].get("third").and_then(Value::as_i64), Some(3));
    }

    #[test]
    fn test_multi_doc_all_scalars() {
        let yaml = "---\n42\n---\nhello\n---\ntrue\n---\n3.14";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc scalars is valid");
        assert_eq!(docs.len(), 4);
        assert_eq!(docs[0].as_i64(), Some(42));
        assert_eq!(docs[1].as_str(), Some("hello"));
        assert_eq!(docs[2].as_bool(), Some(true));
        assert!(docs[3].as_f64().is_some());
    }

    #[test]
    fn test_multi_doc_all_sequences() {
        let yaml = "---\n- a\n- b\n---\n- 1\n- 2\n- 3\n---\n- x";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc sequences is valid");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].as_sequence().map(Vec::len), Some(2));
        assert_eq!(docs[1].as_sequence().map(Vec::len), Some(3));
        assert_eq!(docs[2].as_sequence().map(Vec::len), Some(1));
    }

    #[test]
    fn test_multi_doc_mixed_explicit_markers() {
        // Mix of --- and ... markers
        let yaml = "---\nfirst: 1\n...\n---\nsecond: 2\n...\n---\nthird: 3";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc with mixed markers is valid");
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[0].get("first").and_then(Value::as_i64), Some(1));
        assert_eq!(docs[1].get("second").and_then(Value::as_i64), Some(2));
        assert_eq!(docs[2].get("third").and_then(Value::as_i64), Some(3));
    }

    #[test]
    fn test_multi_doc_with_anchors() {
        // Each document can have its own anchors
        let yaml = r"
---
name: &name first
ref: *name
---
name: &name second
ref: *name
";
        let docs: Vec<Value> = from_str_multi(yaml).expect("multi-doc with anchors is valid");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get("name").and_then(Value::as_str), Some("first"));
        assert_eq!(docs[0].get("ref").and_then(Value::as_str), Some("first"));
        assert_eq!(docs[1].get("name").and_then(Value::as_str), Some("second"));
        assert_eq!(docs[1].get("ref").and_then(Value::as_str), Some("second"));
    }

    #[test]
    fn test_multi_doc_deeply_nested() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Level3 {
            value: i32,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Level2 {
            level3: Level3,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Level1 {
            level2: Level2,
        }
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            level1: Level1,
        }

        let yaml = r"
---
level1:
  level2:
    level3:
      value: 1
---
level1:
  level2:
    level3:
      value: 2
";
        let docs: Vec<Doc> = from_str_multi(yaml).expect("multi-doc nested is valid");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].level1.level2.level3.value, 1);
        assert_eq!(docs[1].level1.level2.level3.value, 2);
    }

    #[test]
    fn test_multi_doc_large_number() {
        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            id: i32,
        }

        // Test with more than a few documents
        let yaml = (0..10)
            .map(|i| format!("---\nid: {i}"))
            .collect::<Vec<_>>()
            .join("\n");

        let docs: Vec<Doc> = from_str_multi(&yaml).expect("many docs is valid");
        assert_eq!(docs.len(), 10);
        for (i, doc) in docs.iter().enumerate() {
            assert_eq!(doc.id, i32::try_from(i).expect("i fits in i32"));
        }
    }

    #[test]
    fn test_from_slice_multi_invalid_utf8() {
        // Invalid UTF-8 should error
        let invalid = b"\xff\xfe";
        let result: Result<Vec<Value>> = from_slice_multi(invalid);
        let _ = result.expect_err("invalid UTF-8 should fail");
    }

    #[test]
    fn test_multi_doc_deserialization_error() {
        // Valid YAML but wrong type for deserialization
        #[derive(Debug, Deserialize)]
        #[expect(dead_code)]
        struct Doc {
            required_field: String,
        }

        let yaml = "---\nrequired_field: ok\n---\nwrong_field: oops";
        let result: Result<Vec<Doc>> = from_str_multi(yaml);
        let _ = result.expect_err("missing field should fail");
    }

    #[test]
    fn test_multi_doc_whitespace_only_between() {
        // Whitespace between documents
        let yaml = "---\na: 1\n\n\n---\nb: 2";
        let docs: Vec<Value> = from_str_multi(yaml).expect("whitespace between docs is valid");
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].get("a").and_then(Value::as_i64), Some(1));
        assert_eq!(docs[1].get("b").and_then(Value::as_i64), Some(2));
    }

    #[test]
    fn test_single_doc_vs_multi_doc_same_result() {
        // A single document should parse the same way with both functions
        let yaml = "name: test\nvalue: 42";

        let single: Value = from_str(yaml).expect("single-doc parse works");
        let multi: Vec<Value> = from_str_multi(yaml).expect("multi-doc parse works");

        assert_eq!(multi.len(), 1);
        assert_eq!(single, multi[0]);
    }

    #[test]
    fn test_multi_doc_reader_large_input() {
        use std::fmt::Write;
        use std::io::Cursor;

        #[derive(Debug, Deserialize, PartialEq)]
        struct Doc {
            id: i32,
            name: String,
            items: Vec<String>,
        }

        // Build a larger multi-doc input
        let yaml = (0..5).fold(String::new(), |mut acc, i| {
            write!(
                acc,
                r"---
id: {i}
name: document_{i}
items:
  - item1
  - item2
  - item3
"
            )
            .expect("write to string");
            acc
        });

        let reader = Cursor::new(yaml);
        let docs: Vec<Doc> = from_reader_multi(reader).expect("reader multi-doc works");
        assert_eq!(docs.len(), 5);
        for (i, doc) in docs.iter().enumerate() {
            assert_eq!(doc.id, i32::try_from(i).expect("i fits in i32"));
            assert_eq!(doc.name, format!("document_{i}"));
            assert_eq!(doc.items.len(), 3);
        }
    }
}
