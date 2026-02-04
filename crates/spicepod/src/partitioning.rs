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
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct PartitionedBy {
    pub name: String,
    pub expression: String,
}

pub fn deserialize_partition_by<'de, D>(deserializer: D) -> Result<Vec<PartitionedBy>, D::Error>
where
    D: Deserializer<'de>,
{
    let values = Vec::<serde_json::Value>::deserialize(deserializer)?;

    let mut result = Vec::new();

    for value in values {
        match value {
            serde_json::Value::String(expression) => {
                let name = format!("expr{i}", i = result.len());
                let partitioned_by = PartitionedBy { name, expression };
                result.push(partitioned_by);
            }
            serde_json::Value::Object(map) => {
                // case where {"year": "YEAR(created_at)"}
                for (name, v) in map {
                    if let serde_json::Value::String(expression) = v {
                        let partitioned_by = PartitionedBy { name, expression };
                        result.push(partitioned_by);
                        break; // take first string and ignore others
                    }
                }
            }
            _ => {}
        }
    }

    Ok(result)
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
}
