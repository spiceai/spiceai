use crate::dynamodb::{DynamoDBRow, JsonSerializationSnafu, Result};
use aws_sdk_dynamodb::types::AttributeValue;
use base64::{Engine as _, engine::general_purpose};
use serde_json::{Value, json};
use snafu::ResultExt;
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct JsonNesting {
    pub static_fields: HashSet<String>,
    pub json_field_name: String,
}

/// With the following configuration: `JsonNesting` {`static_fields`: {"PK", "SK", "Baz"}, `json_field_name`: "Data"}
///
/// This schema:
/// PK (string) | SK (string) | Foo (Map) | Bar (List) | Baz (string)
///
/// Becomes this:
/// PK (string) | SK (string) | Baz (string) | Data ({"Foo": <map>, "Bar": <list>})
pub fn json_nest_except_fields(
    rows: Vec<DynamoDBRow>,
    json_nesting: &JsonNesting,
) -> Result<Vec<DynamoDBRow>> {
    rows.into_iter()
        .map(|row| {
            let mut result = HashMap::new();
            // To make fields sorted alphabetically
            let mut data_map = BTreeMap::new();

            for (key, value) in row {
                if json_nesting.static_fields.contains(&key) {
                    result.insert(key, value);
                } else {
                    data_map.insert(key, attribute_value_to_json(&value));
                }
            }

            if !data_map.is_empty() {
                let json_string =
                    serde_json::to_string(&data_map).context(JsonSerializationSnafu)?;
                result.insert(
                    json_nesting.json_field_name.clone(),
                    AttributeValue::S(json_string),
                );
            }

            Ok(result)
        })
        .collect()
}

fn attribute_value_to_json(attr: &AttributeValue) -> Value {
    match attr {
        AttributeValue::S(s) => Value::String(s.clone()),
        AttributeValue::N(n) => {
            // Try to parse as number, fallback to string
            n.parse::<f64>()
                .map_or_else(|_| Value::String(n.clone()), |num| json!(num))
        }
        AttributeValue::Bool(b) => Value::Bool(*b),
        AttributeValue::M(m) => {
            let mut map = serde_json::Map::new();
            for (k, v) in m {
                map.insert(k.clone(), attribute_value_to_json(v));
            }
            Value::Object(map)
        }
        AttributeValue::L(list) => Value::Array(list.iter().map(attribute_value_to_json).collect()),
        AttributeValue::Ss(ss) => {
            Value::Array(ss.iter().map(|s| Value::String(s.clone())).collect())
        }
        AttributeValue::Ns(ns) => {
            Value::Array(ns.iter().map(|n| Value::String(n.clone())).collect())
        }
        AttributeValue::B(blob) => Value::String(general_purpose::STANDARD.encode(blob.as_ref())),
        AttributeValue::Bs(blobs) => Value::Array(
            blobs
                .iter()
                .map(|b| Value::String(general_purpose::STANDARD.encode(b.as_ref())))
                .collect(),
        ),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::AttributeValue;
    use serde_json::Value;
    use std::collections::{HashMap, HashSet};

    fn make_string_attr(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_string())
    }

    fn make_number_attr(n: &str) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn make_bool_attr(b: bool) -> AttributeValue {
        AttributeValue::Bool(b)
    }

    fn make_list_attr(items: Vec<AttributeValue>) -> AttributeValue {
        AttributeValue::L(items)
    }

    fn make_map_attr(items: HashMap<String, AttributeValue>) -> AttributeValue {
        AttributeValue::M(items)
    }

    fn extract_data_field(row: &DynamoDBRow) -> Option<Value> {
        if let Some(AttributeValue::S(json_str)) = row.get("Data") {
            serde_json::from_str(json_str).ok()
        } else {
            None
        }
    }

    #[test]
    fn test_basic_nesting() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("SK".to_string(), make_string_attr("sk1"));
        row.insert("Foo".to_string(), make_string_attr("foo_value"));
        row.insert("Bar".to_string(), make_string_attr("bar_value"));

        let static_fields: HashSet<String> =
            ["PK".to_string(), "SK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // Check static fields are at top level
        assert_eq!(result_row.get("PK"), Some(&make_string_attr("pk1")));
        assert_eq!(result_row.get("SK"), Some(&make_string_attr("sk1")));

        // Check nested fields are in Data
        let data = extract_data_field(result_row).expect("Data field should exist");
        assert_eq!(data["Foo"], "foo_value");
        assert_eq!(data["Bar"], "bar_value");
    }

    #[test]
    fn test_all_static_fields() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("SK".to_string(), make_string_attr("sk1"));

        let static_fields: HashSet<String> =
            ["PK".to_string(), "SK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // All fields should be at top level
        assert_eq!(result_row.get("PK"), Some(&make_string_attr("pk1")));
        assert_eq!(result_row.get("SK"), Some(&make_string_attr("sk1")));

        // No Data field should exist
        assert!(result_row.get("Data").is_none());
    }

    #[test]
    fn test_no_static_fields() {
        let mut row = HashMap::new();
        row.insert("Foo".to_string(), make_string_attr("foo_value"));
        row.insert("Bar".to_string(), make_string_attr("bar_value"));

        let static_fields: HashSet<String> = HashSet::new();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        // All fields should be nested in Data
        let data = extract_data_field(result_row).expect("Data field should exist");
        assert_eq!(data["Foo"], "foo_value");
        assert_eq!(data["Bar"], "bar_value");

        // Only Data field should exist
        assert_eq!(result_row.len(), 1);
    }

    #[test]
    fn test_empty_rows() {
        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_multiple_rows() {
        let mut row1 = HashMap::new();
        row1.insert("PK".to_string(), make_string_attr("pk1"));
        row1.insert("Foo".to_string(), make_string_attr("foo1"));

        let mut row2 = HashMap::new();
        row2.insert("PK".to_string(), make_string_attr("pk2"));
        row2.insert("Foo".to_string(), make_string_attr("foo2"));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row1, row2],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 2);

        // Check first row
        assert_eq!(result[0].get("PK"), Some(&make_string_attr("pk1")));
        let data1 = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data1["Foo"], "foo1");

        // Check second row
        assert_eq!(result[1].get("PK"), Some(&make_string_attr("pk2")));
        let data2 = extract_data_field(&result[1]).expect("Data field should exist");
        assert_eq!(data2["Foo"], "foo2");
    }

    #[test]
    fn test_different_attribute_types() {
        let mut inner_map = HashMap::new();
        inner_map.insert("nested_key".to_string(), make_string_attr("nested_value"));

        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("StringField".to_string(), make_string_attr("string_value"));
        row.insert("NumberField".to_string(), make_number_attr("42.5"));
        row.insert("BoolField".to_string(), make_bool_attr(true));
        row.insert(
            "ListField".to_string(),
            make_list_attr(vec![make_string_attr("item1"), make_string_attr("item2")]),
        );
        row.insert("MapField".to_string(), make_map_attr(inner_map));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        assert_eq!(result.len(), 1);
        let result_row = &result[0];

        assert_eq!(result_row.get("PK"), Some(&make_string_attr("pk1")));

        let data = extract_data_field(result_row).expect("Data field should exist");

        // Check different types
        assert_eq!(data["StringField"], "string_value");
        assert_eq!(data["NumberField"], 42.5);
        assert_eq!(data["BoolField"], true);
        assert_eq!(data["ListField"], json!(["item1", "item2"]));
        assert_eq!(data["MapField"]["nested_key"], "nested_value");
    }

    #[test]
    fn test_null_attribute() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("NullField".to_string(), AttributeValue::Null(true));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data["NullField"], Value::Null);
    }

    #[test]
    fn test_string_set() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert(
            "StringSet".to_string(),
            AttributeValue::Ss(vec![
                "value1".to_string(),
                "value2".to_string(),
                "value3".to_string(),
            ]),
        );

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data["StringSet"], json!(["value1", "value2", "value3"]));
    }

    #[test]
    fn test_number_set() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert(
            "NumberSet".to_string(),
            AttributeValue::Ns(vec!["1".to_string(), "2".to_string(), "3".to_string()]),
        );

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data["NumberSet"], json!(["1", "2", "3"]));
    }

    #[test]
    fn test_binary_data() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert(
            "BinaryField".to_string(),
            AttributeValue::B(aws_smithy_types::Blob::new(vec![1, 2, 3, 4])),
        );

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");

        // Verify it's a base64 encoded string
        if let Value::String(encoded) = &data["BinaryField"] {
            let decoded = general_purpose::STANDARD.decode(encoded).expect("result");
            assert_eq!(decoded, vec![1, 2, 3, 4]);
        } else {
            panic!("BinaryField should be a string");
        }
    }

    #[test]
    fn test_case_sensitive_field_names() {
        let mut row = HashMap::new();
        row.insert("pk".to_string(), make_string_attr("lowercase"));
        row.insert("PK".to_string(), make_string_attr("uppercase"));
        row.insert("Pk".to_string(), make_string_attr("mixedcase"));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let result_row = &result[0];

        // Only exact match "PK" should be static
        assert_eq!(result_row.get("PK"), Some(&make_string_attr("uppercase")));

        // Others should be nested
        let data = extract_data_field(result_row).expect("Data field should exist");
        assert_eq!(data["pk"], "lowercase");
        assert_eq!(data["Pk"], "mixedcase");
    }

    #[test]
    fn test_deeply_nested_structures() {
        let mut level2_map = HashMap::new();
        level2_map.insert("deep".to_string(), make_string_attr("value"));

        let mut level1_map = HashMap::new();
        level1_map.insert("level2".to_string(), make_map_attr(level2_map));

        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("NestedMap".to_string(), make_map_attr(level1_map));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data["NestedMap"]["level2"]["deep"], "value");
    }

    #[test]
    fn test_number_parsing() {
        let mut row = HashMap::new();
        row.insert("PK".to_string(), make_string_attr("pk1"));
        row.insert("IntNum".to_string(), make_number_attr("42"));
        row.insert(
            "FloatNum".to_string(),
            make_number_attr("3.141592653589793"),
        );
        row.insert("ScientificNum".to_string(), make_number_attr("1.23e10"));

        let static_fields: HashSet<String> = ["PK".to_string()].into_iter().collect();

        let result = json_nest_except_fields(
            vec![row],
            &JsonNesting {
                static_fields,
                json_field_name: "Data".to_string(),
            },
        )
        .expect("result");

        let data = extract_data_field(&result[0]).expect("Data field should exist");
        assert_eq!(data["IntNum"], 42.0);
        assert_eq!(data["FloatNum"], std::f64::consts::PI);
        assert_eq!(data["ScientificNum"], 1.23e10);
    }
}
