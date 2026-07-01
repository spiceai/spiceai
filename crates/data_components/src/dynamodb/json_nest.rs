use crate::dynamodb::DynamoDBRow;
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
/// This lets the generic [`crate::schema_projection`] core reshape `DynamoDB`
/// rows without any DynamoDB-specific projection logic.
///
/// `RowShape` is defined in the `datafusion-table-providers` fork, and
/// `AttributeValue` is also a foreign type, so the impl goes on this local
/// newtype to satisfy the orphan rule.
struct AttrShape(AttributeValue);

impl crate::schema_projection::RowShape for AttrShape {
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
/// [`SchemaProjection`]: crate::schema_projection::SchemaProjection
#[must_use]
pub fn project_dynamodb_row(
    row: DynamoDBRow,
    projection: &crate::schema_projection::SchemaProjection,
) -> DynamoDBRow {
    let wrapped = AttrShape(AttributeValue::M(row.into_iter().collect()));
    match projection.project_row(wrapped).0 {
        AttributeValue::M(map) => map.into_iter().collect(),
        _ => HashMap::new(),
    }
}
