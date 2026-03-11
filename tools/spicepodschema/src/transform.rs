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

//! Transform module for converting `ParameterSpec` to JSON Schema.

use crate::collector::ConnectorSchema;
use runtime_parameters::{ParameterSpec, ParameterType};
use serde_json::{Map, Value};

/// Converts a single `ParameterSpec` to a JSON Schema property.
///
/// The mapping follows:
/// - `description` → `description`
/// - `default` → `default`
/// - `secret: true` → `x-secret: true` extension
/// - `one_of` → `enum` array
/// - `deprecation_message` → `deprecated: true` + message in description
/// - `examples` → `examples` array
/// - `help_link` → appended to description
#[must_use]
pub fn param_spec_to_schema(spec: &ParameterSpec) -> Value {
    let mut schema = Map::new();

    // All parameters are strings in spicepod
    schema.insert("type".to_string(), Value::String("string".to_string()));

    // Build description with optional deprecation message and help link
    let mut description_parts = Vec::new();

    if let Some(deprecation_msg) = spec.deprecation_message {
        description_parts.push(format!("**DEPRECATED**: {deprecation_msg}"));
    }

    if !spec.description.is_empty() {
        description_parts.push(spec.description.to_string());
    }

    if !spec.help_link.is_empty() {
        description_parts.push(format!("See: {}", spec.help_link));
    }

    if !description_parts.is_empty() {
        schema.insert(
            "description".to_string(),
            Value::String(description_parts.join("\n\n")),
        );
    }

    // Set deprecated flag
    if spec.deprecation_message.is_some() {
        schema.insert("deprecated".to_string(), Value::Bool(true));
    }

    // Set default value
    if let Some(default_value) = spec.default {
        schema.insert(
            "default".to_string(),
            Value::String(default_value.to_string()),
        );
    }

    // Set examples
    if !spec.examples.is_empty() {
        let examples: Vec<Value> = spec
            .examples
            .iter()
            .map(|e| Value::String((*e).to_string()))
            .collect();
        schema.insert("examples".to_string(), Value::Array(examples));
    }

    // Handle enum values (one_of)
    if let Some(options) = spec.one_of {
        let enum_values: Vec<Value> = options
            .iter()
            .map(|o| Value::String((*o).to_string()))
            .collect();
        schema.insert("enum".to_string(), Value::Array(enum_values));
    }

    // Add x-secret extension
    if spec.secret {
        schema.insert("x-secret".to_string(), Value::Bool(true));
    }

    Value::Object(schema)
}

/// Generates the property name for a parameter based on its type and the connector's prefix.
///
/// Component parameters are prefixed (e.g., `pg_host` for postgres).
/// Runtime parameters are not prefixed (e.g., `mode`).
#[must_use]
pub fn get_property_name(spec: &ParameterSpec, prefix: &str) -> String {
    match spec.r#type {
        ParameterType::Component => {
            if prefix.is_empty() {
                spec.name.to_string()
            } else {
                format!("{prefix}_{}", spec.name)
            }
        }
        ParameterType::Runtime => spec.name.to_string(),
    }
}

/// Converts a connector's parameters to a JSON Schema object.
///
/// The returned schema represents the `params` object for this connector,
/// with all parameters as optional properties (unless marked as required).
#[must_use]
pub fn connector_params_to_schema(connector: &ConnectorSchema) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for spec in connector.parameters {
        let property_name = get_property_name(spec, connector.prefix);
        let property_schema = param_spec_to_schema(spec);

        if spec.required {
            required.push(Value::String(property_name.clone()));
        }

        properties.insert(property_name, property_schema);
    }

    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert(
        "title".to_string(),
        Value::String(format!("{}Params", to_pascal_case(&connector.name))),
    );
    schema.insert("properties".to_string(), Value::Object(properties));

    if !required.is_empty() {
        schema.insert("required".to_string(), Value::Array(required));
    }

    // Allow additional properties for flexibility
    schema.insert("additionalProperties".to_string(), Value::Bool(true));

    Value::Object(schema)
}

/// Converts a `snake_case` or kebab-case string to `PascalCase`.
#[must_use]
pub fn to_pascal_case(s: &str) -> String {
    s.split(['_', '-', '.'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_pascal_case() {
        assert_eq!(to_pascal_case("postgres"), "Postgres");
        assert_eq!(to_pascal_case("delta_lake"), "DeltaLake");
        assert_eq!(to_pascal_case("spice.ai"), "SpiceAi");
        assert_eq!(to_pascal_case("unity_catalog"), "UnityCatalog");
    }

    #[test]
    fn test_param_spec_to_schema_basic() {
        let spec = ParameterSpec::component("host").description("The database host");
        let schema = param_spec_to_schema(&spec);

        let obj = schema.as_object().expect("should be object");
        assert_eq!(obj.get("type"), Some(&Value::String("string".to_string())));
        assert_eq!(
            obj.get("description"),
            Some(&Value::String("The database host".to_string()))
        );
    }

    #[test]
    fn test_param_spec_to_schema_with_secret() {
        let spec = ParameterSpec::component("password").secret();
        let schema = param_spec_to_schema(&spec);

        let obj = schema.as_object().expect("should be object");
        assert_eq!(obj.get("x-secret"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_param_spec_to_schema_with_enum() {
        let spec = ParameterSpec::component("mode").one_of(&["read", "write"]);
        let schema = param_spec_to_schema(&spec);

        let obj = schema.as_object().expect("should be object");
        let expected = vec![
            Value::String("read".to_string()),
            Value::String("write".to_string()),
        ];
        assert_eq!(obj.get("enum"), Some(&Value::Array(expected)));
    }

    #[test]
    fn test_get_property_name_component() {
        let spec = ParameterSpec::component("host");
        assert_eq!(get_property_name(&spec, "pg"), "pg_host");
        assert_eq!(get_property_name(&spec, ""), "host");
    }

    #[test]
    fn test_get_property_name_runtime() {
        let spec = ParameterSpec::runtime("mode");
        assert_eq!(get_property_name(&spec, "pg"), "mode");
    }
}
