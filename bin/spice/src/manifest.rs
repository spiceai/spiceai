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
use spicepod::spec::{SpicepodKind, SpicepodVersion};
#[cfg(unix)]
use std::io::Write;
use std::path::{Path, PathBuf};
use yaml::{Mapping, Value};

/// Canonical Spicepod manifest filename written by the CLI.
pub const SPICEPOD_YAML: &str = "spicepod.yaml";
/// Alternate Spicepod manifest filename accepted by CLI commands.
pub const SPICEPOD_YML: &str = "spicepod.yml";

const SPICEPOD_FILENAMES: [&str; 2] = [SPICEPOD_YAML, SPICEPOD_YML];
const SCHEMA_DIRECTIVE: &str = "# yaml-language-server: $schema=https://raw.githubusercontent.com/spiceai/spiceai/trunk/.schema/spicepod.schema.json";

/// Returns the first existing root Spicepod manifest path, preferring `spicepod.yaml` over `spicepod.yml`.
#[must_use]
pub fn existing_spicepod_path(base_dir: &Path) -> Option<PathBuf> {
    SPICEPOD_FILENAMES
        .iter()
        .map(|filename| base_dir.join(filename))
        .find(|path| path.exists())
}

/// Returns the default manifest path for new Spice apps.
#[must_use]
pub fn default_spicepod_path(base_dir: &Path) -> PathBuf {
    base_dir.join(SPICEPOD_YAML)
}

/// Builds the default Spicepod YAML content for a new app.
#[must_use]
pub fn create_spicepod_yaml(name: &str) -> String {
    format!("{SCHEMA_DIRECTIVE}\nversion: v2\nkind: Spicepod\nname: {name}\n")
}

/// Reads a Spicepod manifest as YAML while validating its root header.
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

/// Loads an existing root manifest, or returns a new default manifest value and path.
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

/// Validates and writes a Spicepod manifest value to disk.
pub fn write_spicepod_value(path: &Path, value: &Value) -> Result<()> {
    validate_spicepod_value(value, path)?;

    let mut updated_yaml =
        yaml::to_string(value).map_err(|source| crate::error::Error::ConfigParse {
            message: format!("Failed to serialize {}: {source}", path.display()),
        })?;

    let leading_comments = leading_manifest_comments(path)?;
    if !leading_comments.is_empty() {
        let prefix = leading_comments.join("\n");
        if !updated_yaml.starts_with(&prefix) {
            updated_yaml = format!("{prefix}\n{updated_yaml}");
        }
    }

    ensure_parent_dir(path)?;
    std::fs::write(path, updated_yaml.as_bytes()).context(ConfigIoSnafu {
        operation: "write",
        path: path.to_path_buf(),
    })
}

fn leading_manifest_comments(path: &Path) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(vec![SCHEMA_DIRECTIVE.to_string()]);
    }

    let content = std::fs::read_to_string(path).context(ConfigIoSnafu {
        operation: "read",
        path: path.to_path_buf(),
    })?;

    let mut comments = Vec::new();
    for line in content.lines() {
        if line.trim_start().starts_with('#') || (!comments.is_empty() && line.trim().is_empty()) {
            comments.push(line.to_string());
            continue;
        }
        break;
    }

    while comments.last().is_some_and(|line| line.trim().is_empty()) {
        comments.pop();
    }

    Ok(comments)
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(parent).context(ConfigIoSnafu {
        operation: "create directory",
        path: parent.to_path_buf(),
    })
}

/// Writes a file and restricts permissions to the owner on Unix platforms.
///
/// On non-Unix platforms this falls back to the standard library write path;
/// callers must not assume owner-only ACL hardening there.
pub fn write_secure_file(path: &Path, contents: &[u8]) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::fs::PermissionsExt;

        let permissions = std::fs::Permissions::from_mode(0o600);
        if path.exists() {
            std::fs::set_permissions(path, permissions).context(ConfigIoSnafu {
                operation: "set permissions on",
                path: path.to_path_buf(),
            })?;
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .context(ConfigIoSnafu {
                operation: "open",
                path: path.to_path_buf(),
            })?;
        file.write_all(contents).context(ConfigIoSnafu {
            operation: "write",
            path: path.to_path_buf(),
        })?;
    }

    #[cfg(not(unix))]
    {
        std::fs::write(path, contents).context(ConfigIoSnafu {
            operation: "write",
            path: path.to_path_buf(),
        })?;
    }

    Ok(())
}

/// Ensures a YAML sequence field contains a string item, returning whether it changed the value.
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

/// Ensures a component reference sequence contains a `ref` entry, returning whether it changed the value.
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

/// Formats a path as a portable Spicepod reference using `/` separators.
#[must_use]
pub fn path_to_spicepod_ref(path: &Path) -> String {
    path.to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/")
}

/// Returns a mutable YAML sequence field, creating an empty sequence when the field is absent.
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

/// Validates the root manifest header without rejecting newer fields this CLI does not edit.
fn validate_spicepod_value(value: &Value, path: &Path) -> Result<()> {
    let mapping = value
        .as_mapping()
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!(
                "Failed to parse {}: manifest must be a YAML mapping",
                path.display()
            ),
        })?;

    let name = required_header_field(mapping, "name", path)?;
    if !name.is_string() {
        return Err(crate::error::Error::ConfigParse {
            message: format!(
                "Failed to parse {}: field 'name' must be a string",
                path.display()
            ),
        });
    }

    parse_header_field::<SpicepodVersion>(mapping, "version", path)?;
    parse_header_field::<SpicepodKind>(mapping, "kind", path)?;

    Ok(())
}

fn required_header_field<'a>(mapping: &'a Mapping, field: &str, path: &Path) -> Result<&'a Value> {
    mapping
        .get(&Value::String(field.to_string()))
        .ok_or_else(|| crate::error::Error::ConfigParse {
            message: format!(
                "Failed to parse {}: missing field '{field}'",
                path.display()
            ),
        })
}

fn parse_header_field<T>(mapping: &Mapping, field: &str, path: &Path) -> Result<()>
where
    T: serde::de::DeserializeOwned,
{
    let value = required_header_field(mapping, field, path)?;
    yaml::from_value::<T>(value.clone()).map_err(|source| crate::error::Error::ConfigParse {
        message: format!(
            "Failed to parse {} field '{field}': {source}",
            path.display()
        ),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_PRIMITIVES_SPICEPOD: &str = r"version: v2
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
";

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
    fn write_spicepod_value_preserves_schema_directive() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let path = temp_dir.path().join(SPICEPOD_YAML);
        std::fs::write(&path, create_spicepod_yaml("test")).expect("spicepod should be written");
        let mut value = read_spicepod_value(&path).expect("spicepod should parse");
        ensure_string_sequence_item(&mut value, "dependencies", "spicepods/localpod")
            .expect("dependency should be added");

        write_spicepod_value(&path, &value).expect("spicepod should be written");

        let content = std::fs::read_to_string(path).expect("spicepod should be readable");
        assert!(
            content.starts_with(SCHEMA_DIRECTIVE),
            "schema directive should remain at the top of the manifest"
        );
    }

    #[test]
    fn write_spicepod_value_preserves_leading_comments() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let path = temp_dir.path().join(SPICEPOD_YAML);
        std::fs::write(
            &path,
            "# managed by tests\n# keep this note\nversion: v2\nkind: Spicepod\nname: comments\n",
        )
        .expect("spicepod should be written");
        let mut value = read_spicepod_value(&path).expect("spicepod should parse");
        ensure_string_sequence_item(&mut value, "dependencies", "spicepods/localpod")
            .expect("dependency should be added");

        write_spicepod_value(&path, &value).expect("spicepod should be written");

        let content = std::fs::read_to_string(path).expect("spicepod should be readable");
        assert!(
            content.starts_with("# managed by tests\n# keep this note\n"),
            "leading comments should remain at the top of the manifest"
        );
    }

    #[test]
    fn write_spicepod_value_trims_extra_blank_after_leading_comments() {
        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let path = temp_dir.path().join(SPICEPOD_YAML);
        std::fs::write(
            &path,
            "# managed by tests\n\nversion: v2\nkind: Spicepod\nname: comments\n",
        )
        .expect("spicepod should be written");
        let mut value = read_spicepod_value(&path).expect("spicepod should parse");
        ensure_string_sequence_item(&mut value, "dependencies", "spicepods/localpod")
            .expect("dependency should be added");

        write_spicepod_value(&path, &value).expect("spicepod should be written");

        let content = std::fs::read_to_string(path).expect("spicepod should be readable");
        assert!(content.starts_with("# managed by tests\nversion: v2\n"));
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

    #[test]
    fn validates_header_without_rejecting_newer_fields() {
        let value: Value = yaml::from_str(
            r"version: v2
kind: Spicepod
name: future_manifest
runtime:
  future_runtime_field: keep
future_primitive:
  keep: true
",
        )
        .expect("future manifest should parse as YAML");

        validate_spicepod_value(&value, Path::new("spicepod.yaml"))
            .expect("newer fields should not block manifest edits");
    }

    #[cfg(unix)]
    #[test]
    fn write_spicepod_value_preserves_existing_manifest_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let spicepod_path = temp_dir.path().join(SPICEPOD_YAML);
        std::fs::write(&spicepod_path, "version: v2\nkind: Spicepod\nname: perms\n")
            .expect("spicepod.yaml should be written");
        std::fs::set_permissions(&spicepod_path, std::fs::Permissions::from_mode(0o644))
            .expect("permissions should be set");

        let mut value = read_spicepod_value(&spicepod_path).expect("manifest should load");
        ensure_string_sequence_item(&mut value, "dependencies", "spiceai/quickstart")
            .expect("dependency should be added");
        write_spicepod_value(&spicepod_path, &value).expect("manifest should be written");

        let mode = std::fs::metadata(&spicepod_path)
            .expect("metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o644);
    }

    #[cfg(unix)]
    #[test]
    fn write_secure_file_creates_owner_only_file() {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir().expect("tempdir should be created");
        let path = temp_dir.path().join("secret.env");

        write_secure_file(&path, b"API_KEY=secret\n").expect("secure file should be written");

        let mode = std::fs::metadata(&path)
            .expect("metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}
