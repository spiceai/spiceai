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

//! Typing for `--set PATH=VALUE` values.
//!
//! A value is typed by the Spicepod JSON schema when the schema describes the
//! target field. `enabled=true` becomes a boolean because `acceleration.enabled`
//! is a boolean, while `version=1.10` stays the string `"1.10"` when the field
//! is a string. When the schema has nothing to say about the path (free-form
//! maps such as `metadata`), the value is read as a YAML scalar, but numbers
//! that would not round-trip (`1.10`, `007`) stay strings.
//!
//! `string:` forces a literal string, `yaml:` forces YAML parsing, and a
//! YAML-quoted value (`'"true"'`) is always a string.

use serde_json::Value as Json;
use std::sync::LazyLock;
use yaml::Value;

static SPICEPOD_SCHEMA: LazyLock<Option<Json>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../../../.schema/spicepod.schema.json")).ok()
});

/// Bound on `$ref` and composition nesting while walking the schema.
const MAX_SCHEMA_DEPTH: usize = 64;

/// Where in the Spicepod a `--set` path starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetScope<'a> {
    /// An entry of a component list such as `datasets` or `models`.
    ComponentEntry(&'a str),
    /// A top-level mapping such as `runtime` or `management`.
    Section(&'a str),
    /// An entry under `extensions`.
    ExtensionEntry,
}

impl SetScope<'_> {
    fn pointer(self) -> String {
        match self {
            Self::ComponentEntry(field) => format!("/properties/{field}/items"),
            Self::Section(field) => format!("/properties/{field}"),
            Self::ExtensionEntry => "/properties/extensions/additionalProperties".to_string(),
        }
    }
}

/// JSON types a schema location accepts, as a bit set.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Kinds(u8);

impl Kinds {
    const STRING: u8 = 1;
    const BOOLEAN: u8 = 1 << 1;
    const INTEGER: u8 = 1 << 2;
    const NUMBER: u8 = 1 << 3;
    const NULL: u8 = 1 << 4;
    const OBJECT: u8 = 1 << 5;
    const ARRAY: u8 = 1 << 6;

    fn insert(&mut self, kind: u8) {
        self.0 |= kind;
    }

    const fn has(self, kind: u8) -> bool {
        self.0 & kind != 0
    }

    fn add_type_name(&mut self, name: &str) {
        match name {
            "string" => self.insert(Self::STRING),
            "boolean" => self.insert(Self::BOOLEAN),
            "integer" => self.insert(Self::INTEGER),
            "number" => self.insert(Self::NUMBER),
            "null" => self.insert(Self::NULL),
            "object" => self.insert(Self::OBJECT),
            "array" => self.insert(Self::ARRAY),
            _ => {}
        }
    }

    fn add_json_value(&mut self, value: &Json) {
        self.insert(match value {
            Json::Null => Self::NULL,
            Json::Bool(_) => Self::BOOLEAN,
            Json::Number(number) if number.is_f64() => Self::NUMBER,
            Json::Number(_) => Self::INTEGER,
            Json::String(_) => Self::STRING,
            Json::Array(_) => Self::ARRAY,
            Json::Object(_) => Self::OBJECT,
        });
    }

    /// Whether the field accepts a string.
    #[must_use]
    pub const fn accepts_string(self) -> bool {
        self.has(Self::STRING)
    }

    fn accepts(self, value: &Value) -> bool {
        match value {
            Value::Null => self.has(Self::NULL),
            Value::Bool(_) => self.has(Self::BOOLEAN),
            Value::Number(yaml::Number::Float(_)) => self.has(Self::NUMBER),
            Value::Number(_) => self.has(Self::INTEGER) || self.has(Self::NUMBER),
            Value::String(_) => self.has(Self::STRING),
            Value::Sequence(_) => self.has(Self::ARRAY),
            Value::Mapping(_) => self.has(Self::OBJECT),
        }
    }
}

/// Types the Spicepod schema allows at `path` under `scope`, or `None` when the
/// schema does not constrain that location.
#[must_use]
pub fn schema_kinds(scope: SetScope<'_>, path: &str) -> Option<Kinds> {
    let schema = SPICEPOD_SCHEMA.as_ref()?;
    kinds_at(schema, &scope.pointer(), path)
}

/// Whether the schema defines `path` under `scope`. Returns `true` when the
/// embedded schema is unavailable, so a missing schema never blocks an edit.
#[must_use]
pub fn has_field(scope: SetScope<'_>, path: &str) -> bool {
    match SPICEPOD_SCHEMA.as_ref() {
        Some(schema) => kinds_at(schema, &scope.pointer(), path).is_some(),
        None => true,
    }
}

fn kinds_at(root: &Json, pointer: &str, path: &str) -> Option<Kinds> {
    let start = root.pointer(pointer)?;
    let mut candidates = Vec::new();
    resolve(root, start, &mut candidates, 0);
    for segment in path.split('.') {
        candidates = child(root, &candidates, segment);
        if candidates.is_empty() {
            return None;
        }
    }
    leaf_kinds(&candidates)
}

/// Expands `$ref`, `allOf`/`anyOf`/`oneOf` and `if`/`then`/`else` into the
/// schemas that directly describe a value.
fn resolve<'s>(root: &'s Json, schema: &'s Json, out: &mut Vec<&'s Json>, depth: usize) {
    if depth > MAX_SCHEMA_DEPTH {
        return;
    }
    let Json::Object(map) = schema else {
        if schema == &Json::Bool(true) {
            out.push(schema);
        }
        return;
    };

    let mut composite = false;
    if let Some(target) = map
        .get("$ref")
        .and_then(Json::as_str)
        .and_then(|reference| reference.strip_prefix('#'))
        .and_then(|pointer| root.pointer(pointer))
    {
        composite = true;
        resolve(root, target, out, depth + 1);
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(items) = map.get(key).and_then(Json::as_array) {
            composite = true;
            for item in items {
                resolve(root, item, out, depth + 1);
            }
        }
    }
    for key in ["then", "else"] {
        if let Some(branch) = map.get(key) {
            composite = true;
            resolve(root, branch, out, depth + 1);
        }
    }
    composite |= map.contains_key("if");

    let describes_value = [
        "type",
        "properties",
        "additionalProperties",
        "items",
        "enum",
        "const",
    ]
    .iter()
    .any(|key| map.contains_key(*key));
    if describes_value || !composite {
        out.push(schema);
    }
}

fn child<'s>(root: &'s Json, candidates: &[&'s Json], segment: &str) -> Vec<&'s Json> {
    let mut next = Vec::new();
    for candidate in candidates {
        let Some(map) = candidate.as_object() else {
            continue;
        };
        if let Some(property) = map.get("properties").and_then(|props| props.get(segment)) {
            resolve(root, property, &mut next, 0);
        } else if let Some(additional) = map.get("additionalProperties")
            && additional != &Json::Bool(false)
        {
            resolve(root, additional, &mut next, 0);
        }
    }
    next
}

fn leaf_kinds(candidates: &[&Json]) -> Option<Kinds> {
    let mut kinds = Kinds::default();
    for candidate in candidates {
        let Some(map) = candidate.as_object() else {
            // `true` accepts anything.
            return None;
        };
        match map.get("type") {
            Some(Json::String(name)) => kinds.add_type_name(name),
            Some(Json::Array(names)) => {
                for name in names.iter().filter_map(Json::as_str) {
                    kinds.add_type_name(name);
                }
            }
            _ => {
                if let Some(constant) = map.get("const") {
                    kinds.add_json_value(constant);
                } else if let Some(values) = map.get("enum").and_then(Json::as_array) {
                    for value in values {
                        kinds.add_json_value(value);
                    }
                } else if map.contains_key("properties") || map.contains_key("additionalProperties")
                {
                    kinds.insert(Kinds::OBJECT);
                } else if map.contains_key("items") {
                    kinds.insert(Kinds::ARRAY);
                } else {
                    return None;
                }
            }
        }
    }
    (kinds != Kinds::default()).then_some(kinds)
}

/// Types a raw `--set` value for the field at `path`.
///
/// # Errors
///
/// Returns a message when a `yaml:` value does not parse, or when the field only
/// accepts structured values and the input is not valid YAML.
pub fn typed_value(kinds: Option<Kinds>, path: &str, raw: &str) -> Result<Value, String> {
    if let Some(yaml_value) = raw.strip_prefix("yaml:") {
        return yaml::from_str(yaml_value)
            .map_err(|source| format!("Failed to parse YAML value for '{path}': {source}"));
    }
    if let Some(string_value) = raw.strip_prefix("string:") {
        return Ok(Value::String(string_value.to_string()));
    }
    if raw.trim().is_empty() {
        return Ok(Value::String(raw.to_string()));
    }

    let parsed = yaml::from_str::<Value>(raw).map(|value| raw_unless_quoted(value, raw));
    match (parsed, kinds) {
        (Ok(value), Some(kinds)) => {
            if kinds.accepts(&value) {
                Ok(value)
            } else if kinds.accepts_string() {
                Ok(Value::String(raw.to_string()))
            } else {
                // Leave the mismatch for validation to report with the field path.
                Ok(value)
            }
        }
        (Ok(value), None) => Ok(infer_scalar(value, raw)),
        (Err(source), Some(kinds)) if !kinds.accepts_string() => {
            Err(format!("Failed to parse YAML value for '{path}': {source}"))
        }
        (Err(_), _) => Ok(Value::String(raw.to_string())),
    }
}

/// An unquoted string scalar keeps its raw text, so YAML syntax inside it (a
/// ` #` comment, say) is not dropped. Quoted values keep their decoded string.
fn raw_unless_quoted(value: Value, raw: &str) -> Value {
    match value {
        Value::String(_) if !raw.trim_start().starts_with(['"', '\'']) => {
            Value::String(raw.to_string())
        }
        value => value,
    }
}

/// Keeps numbers that would not round-trip (`1.10`, `007`, `1e3`) as strings when
/// no schema says the field is numeric.
fn infer_scalar(value: Value, raw: &str) -> Value {
    if let Value::Number(number) = &value
        && number.to_string() != raw.trim()
    {
        return Value::String(raw.to_string());
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(path: &str, raw: &str) -> Value {
        let kinds = schema_kinds(SetScope::ComponentEntry("datasets"), path);
        typed_value(kinds, path, raw).expect("value should type")
    }

    #[test]
    fn embedded_schema_parses() {
        assert!(SPICEPOD_SCHEMA.is_some());
    }

    #[test]
    fn booleans_follow_the_schema() {
        assert_eq!(dataset("acceleration.enabled", "true"), Value::Bool(true));
        assert_eq!(dataset("acceleration.enabled", "false"), Value::Bool(false));
        assert_eq!(
            dataset("acceleration.refresh_retry_enabled", "false"),
            Value::Bool(false)
        );
    }

    #[test]
    fn integers_follow_the_schema() {
        assert_eq!(
            dataset("acceleration.refresh_retry_max_attempts", "5"),
            Value::Number(yaml::Number::PosInt(5))
        );
    }

    #[test]
    fn params_take_typed_scalars() {
        assert_eq!(
            dataset("acceleration.params.cayenne_target_file_size_mb", "64"),
            Value::Number(yaml::Number::PosInt(64))
        );
        assert_eq!(
            dataset("params.file_format", "parquet"),
            Value::String("parquet".to_string())
        );
    }

    #[test]
    fn objects_and_arrays_parse_as_yaml() {
        let indexes = dataset("acceleration.indexes", "{id: unique}");
        assert_eq!(
            indexes.get("id").and_then(Value::as_str),
            Some("unique"),
            "{indexes:?}"
        );
        let depends = dataset("dependsOn", "[a, b]");
        assert_eq!(
            depends,
            Value::Sequence(vec![
                Value::String("a".to_string()),
                Value::String("b".to_string())
            ])
        );
        let json = dataset("acceleration.indexes", r#"{"id": "enabled"}"#);
        assert_eq!(json.get("id").and_then(Value::as_str), Some("enabled"));
    }

    #[test]
    fn string_fields_keep_the_raw_text() {
        assert_eq!(
            dataset("acceleration.refresh_check_interval", "10s"),
            Value::String("10s".to_string())
        );
        assert_eq!(
            dataset("acceleration.refresh_append_overlap", "5"),
            Value::String("5".to_string())
        );
        assert_eq!(
            dataset("description", "a: b"),
            Value::String("a: b".to_string())
        );
        assert_eq!(
            dataset("ready_state", "on_registration"),
            Value::String("on_registration".to_string())
        );
        assert_eq!(
            dataset("from", "file:/tmp/x.parquet"),
            Value::String("file:/tmp/x.parquet".to_string())
        );
    }

    #[test]
    fn unquoted_strings_keep_yaml_comment_characters() {
        assert_eq!(
            dataset("description", "report #1"),
            Value::String("report #1".to_string())
        );
        assert_eq!(
            typed_value(None, "x", "a #b"),
            Ok(Value::String("a #b".to_string()))
        );
        assert_eq!(
            typed_value(None, "x", "1 #b"),
            Ok(Value::String("1 #b".to_string()))
        );
    }

    #[test]
    fn nullable_fields_accept_null() {
        assert_eq!(dataset("acceleration.engine", "null"), Value::Null);
    }

    #[test]
    fn quoting_forces_a_string() {
        assert_eq!(
            dataset("acceleration.params.flag", "\"true\""),
            Value::String("true".to_string())
        );
        assert_eq!(
            dataset("acceleration.enabled", "string:true"),
            Value::String("true".to_string())
        );
    }

    #[test]
    fn explicit_yaml_prefix_still_wins() {
        assert_eq!(
            dataset("acceleration.refresh_append_overlap", "yaml:5"),
            Value::Number(yaml::Number::PosInt(5))
        );
    }

    #[test]
    fn invalid_yaml_for_structured_fields_fails() {
        let kinds = schema_kinds(SetScope::ComponentEntry("datasets"), "acceleration.indexes");
        let error = typed_value(kinds, "acceleration.indexes", "{id: unique")
            .expect_err("unterminated mapping should fail");
        assert!(error.contains("acceleration.indexes"), "{error}");
    }

    #[test]
    fn runtime_section_is_typed() {
        let kinds = schema_kinds(SetScope::Section("runtime"), "telemetry.enabled");
        assert_eq!(
            typed_value(kinds, "telemetry.enabled", "false"),
            Ok(Value::Bool(false))
        );
    }

    #[test]
    fn model_entries_are_typed() {
        let kinds = schema_kinds(SetScope::ComponentEntry("models"), "datasets");
        assert_eq!(
            typed_value(kinds, "datasets", "[documents, orders]"),
            Ok(Value::Sequence(vec![
                Value::String("documents".to_string()),
                Value::String("orders".to_string())
            ]))
        );
    }

    #[test]
    fn enabled_exists_only_where_the_schema_has_it() {
        assert!(!has_field(SetScope::ComponentEntry("datasets"), "enabled"));
        assert!(!has_field(SetScope::ComponentEntry("models"), "enabled"));
        assert!(has_field(SetScope::ComponentEntry("functions"), "enabled"));
        assert!(has_field(
            SetScope::ComponentEntry("datasets"),
            "acceleration.enabled"
        ));
    }

    #[test]
    fn unknown_paths_infer_lossless_scalars() {
        assert_eq!(typed_value(None, "x", "true"), Ok(Value::Bool(true)));
        assert_eq!(
            typed_value(None, "x", "64"),
            Ok(Value::Number(yaml::Number::PosInt(64)))
        );
        assert_eq!(
            typed_value(None, "version", "1.10"),
            Ok(Value::String("1.10".to_string()))
        );
        assert_eq!(
            typed_value(None, "x", "007"),
            Ok(Value::String("007".to_string()))
        );
        assert_eq!(typed_value(None, "x", "~"), Ok(Value::Null));
    }
}
