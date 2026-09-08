use crate::DynamoDBRow;
use aws_sdk_dynamodb::types::AttributeValue;
use base64::{Engine as _, engine::general_purpose};
use serde_json::{Value, json};
use std::collections::HashMap;

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

/// `RowShape` adapter for `DynamoDB`. A row (`HashMap<String, AttributeValue>`)
/// is viewed as an `AttributeValue::M`; nested objects are nested `M` values.
/// This lets the generic [`data_components::schema_projection`] core reshape `DynamoDB`
/// rows without any DynamoDB-specific projection logic.
///
/// `RowShape` is defined in the `datafusion-table-providers` fork, and
/// `AttributeValue` is also a foreign type, so the impl goes on this local
/// newtype to satisfy the orphan rule.
struct AttrShape(AttributeValue);

impl data_components::schema_projection::RowShape for AttrShape {
    fn into_object(self) -> std::result::Result<Vec<(String, Self)>, Self> {
        match self.0 {
            AttributeValue::M(map) => Ok(map.into_iter().map(|(k, v)| (k, AttrShape(v))).collect()),
            other => Err(AttrShape(other)),
        }
    }

    fn from_object(entries: Vec<(String, Self)>) -> Self {
        AttrShape(AttributeValue::M(
            entries.into_iter().map(|(k, v)| (k, v.0)).collect(),
        ))
    }

    fn to_json(&self) -> Value {
        attribute_value_to_json(&self.0)
    }

    fn from_json_string(json: String) -> Self {
        AttrShape(AttributeValue::S(json))
    }
}

/// Reshape one `DynamoDB` row through a [`SchemaProjection`], preserving the
/// `HashMap` row type. Wraps the row as an `AttributeValue::M`, projects, and
/// unwraps. A non-`M` result (only possible for a non-object row, which a
/// `DynamoDB` item never is) yields an empty row.
///
/// [`SchemaProjection`]: data_components::schema_projection::SchemaProjection
#[must_use]
pub fn project_dynamodb_row(
    row: DynamoDBRow,
    projection: &data_components::schema_projection::SchemaProjection,
) -> DynamoDBRow {
    let wrapped = AttrShape(AttributeValue::M(row.into_iter().collect()));
    match projection.project_row(wrapped).0 {
        AttributeValue::M(map) => map.into_iter().collect(),
        _ => HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_types::Blob;
    use serde_json::json;

    fn decode_b64(v: &Value) -> Vec<u8> {
        general_purpose::STANDARD
            .decode(v.as_str().expect("base64 string"))
            .expect("valid base64")
    }

    #[test]
    fn string_becomes_json_string() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::S("hello".to_string())),
            json!("hello")
        );
    }

    #[test]
    fn number_parses_int_and_float() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::N("42".to_string())),
            json!(42.0)
        );
        assert_eq!(
            attribute_value_to_json(&AttributeValue::N("2.5".to_string())),
            json!(2.5)
        );
    }

    #[test]
    fn number_falls_back_to_string_when_not_f64_parseable() {
        // DynamoDB numbers can exceed f64 range / be non-numeric text; those are
        // preserved verbatim as a JSON string rather than lost.
        assert_eq!(
            attribute_value_to_json(&AttributeValue::N("not-a-number".to_string())),
            json!("not-a-number")
        );
    }

    #[test]
    fn bool_becomes_json_bool() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Bool(true)),
            json!(true)
        );
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Bool(false)),
            json!(false)
        );
    }

    #[test]
    fn map_becomes_object() {
        let map = HashMap::from([
            ("name".to_string(), AttributeValue::S("Alice".to_string())),
            ("age".to_string(), AttributeValue::N("30".to_string())),
        ]);
        let v = attribute_value_to_json(&AttributeValue::M(map));
        assert_eq!(v["name"], json!("Alice"));
        assert_eq!(v["age"], json!(30.0));
    }

    #[test]
    fn list_becomes_array_preserving_element_types() {
        let list = vec![
            AttributeValue::S("a".to_string()),
            AttributeValue::N("1".to_string()),
            AttributeValue::Bool(true),
        ];
        assert_eq!(
            attribute_value_to_json(&AttributeValue::L(list)),
            json!(["a", 1.0, true])
        );
    }

    #[test]
    fn string_set_becomes_array_of_strings() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Ss(vec!["x".to_string(), "y".to_string()])),
            json!(["x", "y"])
        );
    }

    #[test]
    fn number_set_is_preserved_as_strings() {
        // Number sets keep their string representation (unlike a scalar `N`, which
        // is parsed to a JSON number).
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Ns(vec!["1".to_string(), "2".to_string()])),
            json!(["1", "2"])
        );
    }

    #[test]
    fn binary_becomes_base64_string() {
        let v = attribute_value_to_json(&AttributeValue::B(Blob::new(vec![1u8, 2, 3, 4])));
        assert_eq!(decode_b64(&v), vec![1, 2, 3, 4]);
    }

    #[test]
    fn binary_set_becomes_array_of_base64_strings() {
        let v = attribute_value_to_json(&AttributeValue::Bs(vec![
            Blob::new(vec![0u8]),
            Blob::new(vec![255u8]),
        ]));
        let arr = v.as_array().expect("array");
        assert_eq!(arr.len(), 2);
        assert_eq!(decode_b64(&arr[0]), vec![0]);
        assert_eq!(decode_b64(&arr[1]), vec![255]);
    }

    #[test]
    fn null_becomes_json_null() {
        assert_eq!(
            attribute_value_to_json(&AttributeValue::Null(true)),
            Value::Null
        );
    }

    #[test]
    fn nested_map_and_list_recurse() {
        let inner = HashMap::from([("deep".to_string(), AttributeValue::S("value".to_string()))]);
        let map = HashMap::from([
            ("nested".to_string(), AttributeValue::M(inner)),
            (
                "items".to_string(),
                AttributeValue::L(vec![AttributeValue::N("1".to_string())]),
            ),
        ]);
        let v = attribute_value_to_json(&AttributeValue::M(map));
        assert_eq!(v["nested"]["deep"], json!("value"));
        assert_eq!(v["items"], json!([1.0]));
    }
}
