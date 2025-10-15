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
        };
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_yaml::from_str;

    #[test]
    fn deserialize_partition_by_unnamed() -> Result<(), serde_yaml::Error> {
        let yaml = r#"
- "YEAR(created_at)"
- "MONTH(created_at)"
- "DAY(created_at)"
"#;
        let result: Vec<PartitionedBy> = from_str(yaml)?;

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "expr0");
        assert_eq!(result[0].expression, "YEAR(created_at)");
        assert_eq!(result[1].name, "expr1");
        assert_eq!(result[1].expression, "MONTH(created_at)");
        assert_eq!(result[1].name, "expr2");
        assert_eq!(result[1].expression, "DAY(created_at)");
        Ok(())
    }

    #[test]
    fn deserialize_partition_by_named() -> Result<(), serde_yaml::Error> {
        let yaml = r#"
- year: "YEAR(created_at)"
- month: "MONTH(created_at)"
- day: "DAY(created_at)"
"#;
        let result: Vec<PartitionedBy> = from_str(yaml)?;

        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "year");
        assert_eq!(result[0].expression, "YEAR(created_at)");
        assert_eq!(result[1].name, "month");
        assert_eq!(result[1].expression, "MONTH(created_at)");
        assert_eq!(result[2].name, "day");
        assert_eq!(result[2].expression, "DAY(created_at)");
        Ok(())
    }
}
