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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::ser::{SerializeSeq, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct PartitionedBy {
    pub name: String,
    pub expression: String,
}

#[cfg(feature = "schemars")]
#[derive(JsonSchema)]
#[serde(untagged)]
pub enum PartitionedBySchema {
    Expression(String),
    #[schemars(extend("minProperties" = 1, "maxProperties" = 1))]
    Named(std::collections::HashMap<String, String>),
}

pub fn deserialize_partition_by<'de, D>(deserializer: D) -> Result<Vec<PartitionedBy>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;

    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;

    let mut result = Vec::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        match value {
            serde_json::Value::String(expression) => {
                let name = format!("expr{i}", i = result.len());
                result.push(PartitionedBy { name, expression });
            }
            serde_json::Value::Object(map) => {
                // Accepts only a single-entry `{name: expression_string}` mapping.
                if map.len() != 1 {
                    return Err(D::Error::custom(format!(
                        "partition_by[{idx}]: named partition must be a single-entry mapping of `name: expression_string`, found {} entries",
                        map.len()
                    )));
                }
                // Safe: len == 1.
                let (name, v) = map.into_iter().next().ok_or_else(|| {
                    D::Error::custom(format!("partition_by[{idx}]: unexpected empty mapping"))
                })?;
                let serde_json::Value::String(expression) = v else {
                    return Err(D::Error::custom(format!(
                        "partition_by[{idx}]: named partition value for `{name}` must be a string expression"
                    )));
                };
                result.push(PartitionedBy { name, expression });
            }
            other => {
                let kind = match other {
                    serde_json::Value::Null => "null",
                    serde_json::Value::Bool(_) => "bool",
                    serde_json::Value::Number(_) => "number",
                    serde_json::Value::Array(_) => "array",
                    // String/Object handled above.
                    _ => "unsupported value",
                };
                return Err(D::Error::custom(format!(
                    "partition_by[{idx}]: expected a string expression or a single-entry `{{name: expression}}` mapping, found {kind}"
                )));
            }
        }
    }

    Ok(result)
}

pub fn serialize_partition_by<S>(
    partition_by: &[PartitionedBy],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = serializer.serialize_seq(Some(partition_by.len()))?;

    for (idx, item) in partition_by.iter().enumerate() {
        // If the name is auto-generated for this position (matches "expr{idx}"), serialize as just the expression string
        if item.name == format!("expr{idx}") {
            seq.serialize_element(&item.expression)?;
        } else {
            // Otherwise, serialize as an object with the custom name
            let mut map = std::collections::HashMap::new();
            map.insert(&item.name, &item.expression);
            seq.serialize_element(&map)?;
        }
    }

    seq.end()
}

#[cfg(test)]
mod tests {
    use super::*;

    use yaml::from_str;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[cfg_attr(feature = "schemars", derive(JsonSchema))]
    #[serde(deny_unknown_fields)]
    pub struct Test {
        #[serde(
            default,
            skip_serializing_if = "Vec::is_empty",
            deserialize_with = "deserialize_partition_by"
        )]
        pub partition_by: Vec<PartitionedBy>,
    }
    #[test]
    fn deserialize_partition_by_unnamed() -> Result<(), yaml::Error> {
        let yaml = r#"
partition_by:
  - "YEAR(created_at)"
  - "MONTH(created_at)"
  - "DAY(created_at)"
"#;
        let result: Test = from_str(yaml)?;

        assert_eq!(result.partition_by.len(), 3);
        assert_eq!(result.partition_by[0].name, "expr0");
        assert_eq!(result.partition_by[0].expression, "YEAR(created_at)");
        assert_eq!(result.partition_by[1].name, "expr1");
        assert_eq!(result.partition_by[1].expression, "MONTH(created_at)");
        assert_eq!(result.partition_by[2].name, "expr2");
        assert_eq!(result.partition_by[2].expression, "DAY(created_at)");
        Ok(())
    }

    #[test]
    fn deserialize_partition_by_named() -> Result<(), yaml::Error> {
        let yaml = r#"
partition_by:
  - year: "YEAR(created_at)"
  - month: "MONTH(created_at)"
  - day: "DAY(created_at)"
"#;
        let result: Test = from_str(yaml)?;

        assert_eq!(result.partition_by.len(), 3);
        assert_eq!(result.partition_by[0].name, "year");
        assert_eq!(result.partition_by[0].expression, "YEAR(created_at)");
        assert_eq!(result.partition_by[1].name, "month");
        assert_eq!(result.partition_by[1].expression, "MONTH(created_at)");
        assert_eq!(result.partition_by[2].name, "day");
        assert_eq!(result.partition_by[2].expression, "DAY(created_at)");
        Ok(())
    }

    #[test]
    fn deserialize_partition_by_rejects_multi_entry_map() {
        let yaml = r#"
partition_by:
  - year: "YEAR(created_at)"
    month: "MONTH(created_at)"
"#;
        let err = from_str::<Test>(yaml).expect_err("multi-entry mapping must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("single-entry mapping"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn deserialize_partition_by_rejects_non_string_value() {
        let yaml = r"
partition_by:
  - year: 2024
";
        let err = from_str::<Test>(yaml).expect_err("non-string expression must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("must be a string expression"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn deserialize_partition_by_rejects_scalar_items() {
        let yaml = r"
partition_by:
  - 42
";
        let err = from_str::<Test>(yaml).expect_err("non-string, non-object item must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("expected a string expression"),
            "unexpected error: {msg}"
        );
    }

    /// Guards against regressions in the generated JSON schema for
    /// `PartitionedBySchema`: it must describe both a plain expression string
    /// and a single-entry `{name: expr}` object (with `minProperties = 1` /
    /// `maxProperties = 1`).
    #[cfg(feature = "schemars")]
    #[test]
    fn partition_by_schema_shapes() {
        use schemars::schema_for;

        let schema = schema_for!(PartitionedBySchema);
        let value = serde_json::to_value(&schema).expect("serialize schema");

        let any_of = value
            .get("anyOf")
            .and_then(|v| v.as_array())
            .expect("PartitionedBySchema must generate an anyOf of the accepted shapes");
        assert_eq!(
            any_of.len(),
            2,
            "PartitionedBySchema should have two accepted shapes (string | single-entry map)"
        );

        // Shape 1: plain expression string.
        assert!(
            any_of
                .iter()
                .any(|v| v.get("type").and_then(|t| t.as_str()) == Some("string")),
            "PartitionedBySchema must accept a plain string expression"
        );

        // Shape 2: single-entry object mapping name -> expression.
        let named = any_of
            .iter()
            .find(|v| v.get("type").and_then(|t| t.as_str()) == Some("object"))
            .expect("PartitionedBySchema must accept an object shape");
        assert_eq!(
            named
                .get("minProperties")
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "named partition mapping must require at least one entry"
        );
        assert_eq!(
            named
                .get("maxProperties")
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "named partition mapping must allow at most one entry"
        );
        assert_eq!(
            named
                .get("additionalProperties")
                .and_then(|v| v.get("type"))
                .and_then(|t| t.as_str()),
            Some("string"),
            "named partition mapping values must be strings"
        );
    }
}
