use aws_sdk_dynamodb::types::AttributeValue as DynamoDbAttributeValue;
use aws_sdk_dynamodbstreams::types::AttributeValue as StreamsAttributeValue;
use std::collections::HashMap;

pub fn streams_to_dynamodb_item(
    item: HashMap<String, StreamsAttributeValue>,
) -> HashMap<String, DynamoDbAttributeValue> {
    item.into_iter()
        .map(|(k, v)| (k, streams_to_dynamodb_attribute(&v)))
        .collect()
}

fn streams_to_dynamodb_attribute(value: &StreamsAttributeValue) -> DynamoDbAttributeValue {
    match value {
        StreamsAttributeValue::B(blob) => DynamoDbAttributeValue::B(blob.clone()),
        StreamsAttributeValue::Bool(b) => DynamoDbAttributeValue::Bool(*b),
        StreamsAttributeValue::Bs(blobs) => DynamoDbAttributeValue::Bs(blobs.clone()),
        StreamsAttributeValue::L(list) => {
            DynamoDbAttributeValue::L(list.iter().map(streams_to_dynamodb_attribute).collect())
        }
        StreamsAttributeValue::M(map) => DynamoDbAttributeValue::M(
            map.iter()
                .map(|(k, v)| (k.clone(), streams_to_dynamodb_attribute(v)))
                .collect(),
        ),
        StreamsAttributeValue::N(n) => DynamoDbAttributeValue::N(n.clone()),
        StreamsAttributeValue::Ns(ns) => DynamoDbAttributeValue::Ns(ns.clone()),
        StreamsAttributeValue::Null(n) => DynamoDbAttributeValue::Null(*n),
        StreamsAttributeValue::S(s) => DynamoDbAttributeValue::S(s.clone()),
        StreamsAttributeValue::Ss(ss) => DynamoDbAttributeValue::Ss(ss.clone()),
        _ => DynamoDbAttributeValue::Null(true),
    }
}
