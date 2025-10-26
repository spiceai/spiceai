use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::HashMap;

use super::{Error, Result};

pub fn unnest_dynamodb_items(
    items: Vec<HashMap<String, AttributeValue>>,
    unnest_depth: usize,
) -> Result<Vec<HashMap<String, AttributeValue>>> {
    let mut all_items = Vec::new();

    for item in items {
        let result = unnest_dynamodb_item(&item, unnest_depth)?;
        all_items.push(result);
    }

    Ok(all_items)
}

fn unnest_dynamodb_item(
    item: &HashMap<String, AttributeValue>,
    depth: usize,
) -> Result<HashMap<String, AttributeValue>> {
    let mut new_item = HashMap::new();
    flatten_item_recursive(item, "", &mut new_item, depth, 0)?;
    Ok(new_item)
}

fn flatten_item_recursive(
    item: &HashMap<String, AttributeValue>,
    current_path: &str,
    flattened_item: &mut HashMap<String, AttributeValue>,
    max_depth: usize,
    current_depth: usize,
) -> Result<()> {
    for (key, value) in item {
        let new_path = if current_path.is_empty() {
            key.clone()
        } else {
            format!("{current_path}.{key}")
        };

        match value {
            AttributeValue::M(inner_map) if current_depth < max_depth => {
                flatten_item_recursive(
                    inner_map,
                    &new_path,
                    flattened_item,
                    max_depth,
                    current_depth + 1,
                )?;
            }
            _ => {
                if flattened_item.contains_key(&new_path) {
                    return Err(Error::InvalidItemAccess {
                        message: format!("Column '{key}' already exists in the item."),
                    });
                }
                flattened_item.insert(new_path, value.clone());
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn av_string(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_string())
    }

    fn av_number(n: &str) -> AttributeValue {
        AttributeValue::N(n.to_string())
    }

    fn av_map(map: HashMap<String, AttributeValue>) -> AttributeValue {
        AttributeValue::M(map)
    }

    #[test]
    fn test_unnest_flat_item() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("age".to_string(), av_number("30"));

        let result = unnest_dynamodb_item(&item, 10).expect("unwrapped item");

        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("age"), Some(AttributeValue::N(n)) if n == "30"));
    }

    #[test]
    fn test_unnest_nested_map_depth_1() {
        let mut inner_map = HashMap::new();
        inner_map.insert("city".to_string(), av_string("NYC"));
        inner_map.insert("zip".to_string(), av_string("10001"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("address".to_string(), av_map(inner_map));

        let result = unnest_dynamodb_item(&item, 1).expect("unwrapped item");

        assert_eq!(result.len(), 3);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("address.city"), Some(AttributeValue::S(s)) if s == "NYC"));
        assert!(matches!(result.get("address.zip"), Some(AttributeValue::S(s)) if s == "10001"));
    }

    #[test]
    fn test_unnest_deeply_nested_map() {
        let mut level3 = HashMap::new();
        level3.insert("value".to_string(), av_string("deep"));

        let mut level2 = HashMap::new();
        level2.insert("level3".to_string(), av_map(level3));

        let mut level1 = HashMap::new();
        level1.insert("level2".to_string(), av_map(level2));

        let mut item = HashMap::new();
        item.insert("level1".to_string(), av_map(level1));

        let result = unnest_dynamodb_item(&item, 10).expect("unwrapped item");

        assert_eq!(result.len(), 1);
        assert!(matches!(
            result.get("level1.level2.level3.value"),
            Some(AttributeValue::S(s)) if s == "deep"
        ));
    }

    #[test]
    fn test_unnest_depth_0() {
        let mut inner_map = HashMap::new();
        inner_map.insert("city".to_string(), av_string("NYC"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("address".to_string(), av_map(inner_map));

        let result = unnest_dynamodb_item(&item, 0).expect("unwrapped item");

        // At depth 0, maps should not be flattened
        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("address"), Some(AttributeValue::M(_))));
    }

    #[test]
    fn test_unnest_limited_depth() {
        let mut level3 = HashMap::new();
        level3.insert("value".to_string(), av_string("deep"));

        let mut level2 = HashMap::new();
        level2.insert("level3".to_string(), av_map(level3));

        let mut level1 = HashMap::new();
        level1.insert("level2".to_string(), av_map(level2));

        let mut item = HashMap::new();
        item.insert("level1".to_string(), av_map(level1));

        let result = unnest_dynamodb_item(&item, 1).expect("unwrapped item");

        // Only flatten one level deep
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result.get("level1.level2"),
            Some(AttributeValue::M(_))
        ));
    }

    #[test]
    fn test_unnest_duplicate_key_error() {
        let mut inner_map = HashMap::new();
        inner_map.insert("name".to_string(), av_string("Bob"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("user".to_string(), av_map(inner_map));

        let result = unnest_dynamodb_item(&item, 10).expect("unwrapped item");

        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("user.name"), Some(AttributeValue::S(s)) if s == "Bob"));
    }

    #[test]
    fn test_unnest_multiple_items() {
        let mut inner_map1 = HashMap::new();
        inner_map1.insert("city".to_string(), av_string("NYC"));

        let mut item1 = HashMap::new();
        item1.insert("name".to_string(), av_string("Alice"));
        item1.insert("address".to_string(), av_map(inner_map1));

        let mut inner_map2 = HashMap::new();
        inner_map2.insert("city".to_string(), av_string("LA"));

        let mut item2 = HashMap::new();
        item2.insert("name".to_string(), av_string("Bob"));
        item2.insert("address".to_string(), av_map(inner_map2));

        let items = vec![item1, item2];

        let results = unnest_dynamodb_items(items, 1).expect("unwrapped item");

        assert_eq!(results.len(), 2);

        assert!(matches!(results[0].get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(results[0].get("address.city"), Some(AttributeValue::S(s)) if s == "NYC"));

        assert!(matches!(results[1].get("name"), Some(AttributeValue::S(s)) if s == "Bob"));
        assert!(matches!(results[1].get("address.city"), Some(AttributeValue::S(s)) if s == "LA"));
    }

    #[test]
    fn test_unnest_mixed_types_in_map() {
        let mut inner_map = HashMap::new();
        inner_map.insert("count".to_string(), av_number("42"));
        inner_map.insert("label".to_string(), av_string("test"));

        let mut item = HashMap::new();
        item.insert("id".to_string(), av_string("1"));
        item.insert("metadata".to_string(), av_map(inner_map));

        let result = unnest_dynamodb_item(&item, 1).expect("unwrapped item");

        assert_eq!(result.len(), 3);
        assert!(matches!(result.get("id"), Some(AttributeValue::S(s)) if s == "1"));
        assert!(matches!(result.get("metadata.count"), Some(AttributeValue::N(n)) if n == "42"));
        assert!(matches!(result.get("metadata.label"), Some(AttributeValue::S(s)) if s == "test"));
    }

    #[test]
    fn test_unnest_empty_map() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("empty".to_string(), av_map(HashMap::new()));

        let result = unnest_dynamodb_item(&item, 1).expect("unwrapped item");

        // Empty map shouldn't add any keys
        assert_eq!(result.len(), 1);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
    }
}
