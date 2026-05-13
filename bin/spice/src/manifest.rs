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

//! Helpers for reading and editing the root Spicepod manifest.

use crate::error::{ConfigIoSnafu, Result};
use snafu::ResultExt;
use spicepod::spec::SpicepodDefinition;
use std::path::{Path, PathBuf};
use yaml::{Mapping, Value};

pub const SPICEPOD_YAML: &str = "spicepod.yaml";
pub const SPICEPOD_YML: &str = "spicepod.yml";

const SPICEPOD_FILENAMES: [&str; 2] = [SPICEPOD_YAML, SPICEPOD_YML];
const SCHEMA_DIRECTIVE: &str = "# yaml-language-server: $schema=https://raw.githubusercontent.com/spiceai/spiceai/trunk/.schema/spicepod.schema.json";

#[must_use]
pub fn existing_spicepod_path(base_dir: &Path) -> Option<PathBuf> {
    SPICEPOD_FILENAMES
        .iter()
        .map(|filename| base_dir.join(filename))
        .find(|path| path.exists())
}

#[must_use]
pub fn default_spicepod_path(base_dir: &Path) -> PathBuf {
    base_dir.join(SPICEPOD_YAML)
}

#[must_use]
pub fn create_spicepod_yaml(name: &str) -> String {
    format!("{SCHEMA_DIRECTIVE}\nversion: v2\nkind: Spicepod\nname: {name}\n")
}

pub fn read_spicepod_value(path: &Path) -> Result<Value> {
    let content = std::fs::read_to_string(path).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;

    let value: Value =
        yaml::from_str(&content).map_err(|source| crate::error::Error::ConfigParse {
            message: format!("Failed to parse {}: {source}", path.display()),
        })?;

    validate_spicepod_value(&value, path)?;
    Ok(value)
}

pub fn load_or_create_spicepod_value(
    base_dir: &Path,
    name: &str,
) -> Result<(PathBuf, Value, bool)> {
    if let Some(path) = existing_spicepod_path(base_dir) {
        let value = read_spicepod_value(&path)?;
        return Ok((path, value, false));
    }

    let path = default_spicepod_path(base_dir);
    let value: Value = yaml::from_str(&create_spicepod_yaml(name)).map_err(|source| {
        crate::error::Error::ConfigParse {
            message: format!("Failed to create default Spicepod manifest: {source}"),
        }
    })?;
    Ok((path, value, true))
}

pub fn write_spicepod_value(path: &Path, value: &Value) -> Result<()> {
    validate_spicepod_value(value, path)?;

    let updated_yaml =
        yaml::to_string(value).map_err(|source| crate::error::Error::ConfigParse {
            message: format!("Failed to serialize {}: {source}", path.display()),
        })?;

    write_secure_file(path, updated_yaml.as_bytes())
}

pub fn write_secure_file(path: &Path, contents: &[u8]) -> Result<()> {
    std::fs::write(path, contents).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        std::fs::set_permissions(path, permissions).context(ConfigIoSnafu {
            operation: "set permissions on",
            path: path.to_path_buf(),
        })?;
    }

    Ok(())
}

pub fn ensure_string_sequence_item(value: &mut Value, field: &str, item: &str) -> Result<bool> {
    let sequence = ensure_sequence_field(value, field)?;

    if sequence
        .iter()
        .any(|entry| entry.as_str().is_some_and(|value| value == item))
    {
        return Ok(false);
    }

    sequence.push(Value::String(item.to_string()));
    Ok(true)
}

pub fn ensure_component_reference(value: &mut Value, field: &str, reference: &str) -> Result<bool> {
    let sequence = ensure_sequence_field(value, field)?;

    if sequence.iter().any(|entry| {
        entry
            .get("ref")
            .and_then(Value::as_str)
            .is_some_and(|entry_ref| entry_ref == reference)
    }) {
        return Ok(false);
    }

    let mut reference_map = Mapping::new();
    reference_map.insert(
        Value::String("ref".to_string()),
        Value::String(reference.to_string()),
    );
    sequence.push(Value::Mapping(reference_map));
    Ok(true)
}

#[must_use]
pub fn path_to_spicepod_ref(path: &Path) -> String {
    path.to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

fn ensure_sequence_field<'value>(
    value: &'value mut Value,
    field: &str,
) -> Result<&'value mut Vec<Value>> {
    let root = value
        .as_mapping_mut()
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: "Spicepod manifest must be a YAML mapping".to_string(),
        })?;

    let field_key = Value::String(field.to_string());
    if !root.contains_key(&field_key) {
        root.insert(field_key.clone(), Value::Sequence(Vec::new()));
    }

    root.get_mut(&field_key)
        .and_then(Value::as_sequence_mut)
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!("Spicepod field '{field}' must be a sequence"),
        })
}

fn validate_spicepod_value(value: &Value, path: &Path) -> Result<()> {
    yaml::from_value::<SpicepodDefinition>(value.clone()).map_err(|source| {
        crate::error::Error::ConfigParse {
            message: format!("Failed to parse {}: {source}", path.display()),
        }
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_PRIMITIVES_SPICEPOD: &str = r#"version: v2
kind: Spicepod
name: all_primitives
runtime: {}
management:
  enabled: true
  api_key: test-key
snapshots: {}
extensions:
  test_extension: {}
secrets: []
metadata:
  org: spiceai
catalogs: []
datasets: []
views: []
models: []
embeddings: []
rerankers: []
tools: []
workers: []
functions: []
dependencies: []
future_primitive:
  keep: true
"#;

    #[test]
    fn existing_spicepod_path_prefers_yaml() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        std::fs::write(temp_dir.path().join(SPICEPOD_YML), ALL_PRIMITIVES_SPICEPOD)
            .expect("spicepod.yml should be written");
        std::fs::write(temp_dir.path().join(SPICEPOD_YAML), ALL_PRIMITIVES_SPICEPOD)
            .expect("spicepod.yaml should be written");

        assert_eq!(
            existing_spicepod_path(temp_dir.path()).expect("manifest should exist"),
            temp_dir.path().join(SPICEPOD_YAML)
        );
    }

    #[test]
    fn edits_existing_yml_and_preserves_all_top_level_primitives() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let spicepod_path = temp_dir.path().join(SPICEPOD_YML);
        std::fs::write(&spicepod_path, ALL_PRIMITIVES_SPICEPOD)
            .expect("spicepod.yml should be written");

        let (resolved_path, mut value, created) =
            load_or_create_spicepod_value(temp_dir.path(), "ignored")
                .expect("manifest should load");
        assert_eq!(resolved_path, spicepod_path);
        assert!(!created, "existing manifest should be edited");

        assert!(
            ensure_string_sequence_item(&mut value, "dependencies", "spiceai/quickstart")
                .expect("dependency should be added")
        );
        assert!(
            ensure_component_reference(&mut value, "datasets", "datasets/orders")
                .expect("dataset reference should be added")
        );
        write_spicepod_value(&resolved_path, &value).expect("manifest should be written");

        let updated = std::fs::read_to_string(&resolved_path).expect("manifest should be read");
        let updated_value: Value = yaml::from_str(&updated).expect("manifest should parse");
        let root = updated_value
            .as_mapping()
            .expect("manifest should remain a mapping");

        for field in [
            "version",
            "kind",
            "name",
            "runtime",
            "management",
            "snapshots",
            "extensions",
            "secrets",
            "metadata",
            "catalogs",
            "datasets",
            "views",
            "models",
            "embeddings",
            "rerankers",
            "tools",
            "workers",
            "functions",
            "dependencies",
            "future_primitive",
        ] {
            assert!(
                root.contains_key(&Value::String(field.to_string())),
                "field {field} should be preserved"
            );
        }

        assert_eq!(
            updated_value
                .get("dependencies")
                .and_then(Value::as_sequence)
                .expect("dependencies should be a sequence")
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>(),
            vec!["spiceai/quickstart"]
        );

        assert_eq!(
            updated_value
                .get("datasets")
                .and_then(Value::as_sequence)
                .expect("datasets should be a sequence")
                .first()
                .and_then(|entry| entry.get("ref"))
                .and_then(Value::as_str),
            Some("datasets/orders")
        );
    }
}
