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
use aws_sdk_dynamodb::types::AttributeValue;
use std::collections::{HashMap, HashSet};

use super::{DynamoDBRow, Error, Result};

pub fn unnest_dynamodb_rows(
    rows: Vec<DynamoDBRow>,
    unnest_depth: usize,
) -> Result<(Vec<DynamoDBRow>, HashSet<String>)> {
    let mut unnested_rows = Vec::new();
    let mut all_flattened_fields = HashSet::new();

    for row in rows {
        let (result, flattened_fields) = unnest_dynamodb_row(&row, unnest_depth)?;
        unnested_rows.push(result);
        all_flattened_fields.extend(flattened_fields);
    }

    Ok((unnested_rows, all_flattened_fields))
}

pub fn unnest_dynamodb_row(
    row: &DynamoDBRow,
    depth: usize,
) -> Result<(HashMap<String, AttributeValue>, HashSet<String>)> {
    let mut new_row = HashMap::new();
    let mut flattened_fields = HashSet::new();
    flatten_row_recursive(row, "", &mut new_row, &mut flattened_fields, depth, 0)?;
    Ok((new_row, flattened_fields))
}

fn flatten_row_recursive(
    row: &DynamoDBRow,
    current_path: &str,
    flattened_row: &mut DynamoDBRow,
    flattened_fields: &mut HashSet<String>,
    max_depth: usize,
    current_depth: usize,
) -> Result<()> {
    for (key, value) in row {
        let new_path = if current_path.is_empty() {
            key.clone()
        } else {
            format!("{current_path}.{key}")
        };

        match value {
            AttributeValue::M(inner_map) if current_depth < max_depth => {
                // Track the parent field as completely flattened (removed)
                flattened_fields.insert(new_path.clone());

                flatten_row_recursive(
                    inner_map,
                    &new_path,
                    flattened_row,
                    flattened_fields,
                    max_depth,
                    current_depth + 1,
                )?;
            }
            _ => {
                if flattened_row.contains_key(&new_path) {
                    return Err(Error::InvalidItemAccess {
                        message: format!("Column '{key}' already exists in the item."),
                    });
                }
                // Track only leaf (non-Map) fields that contain dots
                // Don't track Maps that hit the depth limit
                if new_path.contains('.') && !matches!(value, AttributeValue::M(_)) {
                    flattened_fields.insert(new_path.clone());
                }
                flattened_row.insert(new_path, value.clone());
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

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 10).expect("unnested item");

        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("age"), Some(AttributeValue::N(n)) if n == "30"));

        // No nested maps, so no flattening occurred
        assert!(flattened_fields.is_empty());
    }

    #[test]
    fn test_unnest_nested_map_depth_1() {
        let mut inner_map = HashMap::new();
        inner_map.insert("city".to_string(), av_string("NYC"));
        inner_map.insert("zip".to_string(), av_string("10001"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("address".to_string(), av_map(inner_map));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 1).expect("unnested item");

        assert_eq!(result.len(), 3);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("address.city"), Some(AttributeValue::S(s)) if s == "NYC"));
        assert!(matches!(result.get("address.zip"), Some(AttributeValue::S(s)) if s == "10001"));

        // "address" was completely flattened, and leaf fields contain dots
        assert_eq!(flattened_fields.len(), 3);
        assert!(flattened_fields.contains("address"));
        assert!(flattened_fields.contains("address.city"));
        assert!(flattened_fields.contains("address.zip"));
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

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 10).expect("unnested item");

        assert_eq!(result.len(), 1);
        assert!(matches!(
            result.get("level1.level2.level3.value"),
            Some(AttributeValue::S(s)) if s == "deep"
        ));

        // All intermediate maps were completely flattened
        assert_eq!(flattened_fields.len(), 4);
        assert!(flattened_fields.contains("level1"));
        assert!(flattened_fields.contains("level1.level2"));
        assert!(flattened_fields.contains("level1.level2.level3"));
        assert!(flattened_fields.contains("level1.level2.level3.value"));
    }

    #[test]
    fn test_unnest_depth_0() {
        let mut inner_map = HashMap::new();
        inner_map.insert("city".to_string(), av_string("NYC"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("address".to_string(), av_map(inner_map));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 0).expect("unnested item");

        // At depth 0, maps should not be flattened
        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("address"), Some(AttributeValue::M(_))));

        // No flattening at depth 0
        assert!(flattened_fields.is_empty());
    }

    #[test]
    fn test_unnest_duplicate_key_error() {
        let mut inner_map = HashMap::new();
        inner_map.insert("name".to_string(), av_string("Bob"));

        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("user".to_string(), av_map(inner_map));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 10).expect("unnested item");

        assert_eq!(result.len(), 2);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(result.get("user.name"), Some(AttributeValue::S(s)) if s == "Bob"));

        // "user" was completely flattened
        assert_eq!(flattened_fields.len(), 2);
        assert!(flattened_fields.contains("user"));
        assert!(flattened_fields.contains("user.name"));
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

        let (results, flattened_fields) = unnest_dynamodb_rows(items, 1).expect("unnested items");

        assert_eq!(results.len(), 2);

        assert!(matches!(results[0].get("name"), Some(AttributeValue::S(s)) if s == "Alice"));
        assert!(matches!(results[0].get("address.city"), Some(AttributeValue::S(s)) if s == "NYC"));

        assert!(matches!(results[1].get("name"), Some(AttributeValue::S(s)) if s == "Bob"));
        assert!(matches!(results[1].get("address.city"), Some(AttributeValue::S(s)) if s == "LA"));

        // Both items had "address" completely flattened
        assert_eq!(flattened_fields.len(), 2);
        assert!(flattened_fields.contains("address"));
        assert!(flattened_fields.contains("address.city"));
    }

    #[test]
    fn test_unnest_mixed_types_in_map() {
        let mut inner_map = HashMap::new();
        inner_map.insert("count".to_string(), av_number("42"));
        inner_map.insert("label".to_string(), av_string("test"));

        let mut item = HashMap::new();
        item.insert("id".to_string(), av_string("1"));
        item.insert("metadata".to_string(), av_map(inner_map));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 1).expect("unnested item");

        assert_eq!(result.len(), 3);
        assert!(matches!(result.get("id"), Some(AttributeValue::S(s)) if s == "1"));
        assert!(matches!(result.get("metadata.count"), Some(AttributeValue::N(n)) if n == "42"));
        assert!(matches!(result.get("metadata.label"), Some(AttributeValue::S(s)) if s == "test"));

        // "metadata" was completely flattened
        assert_eq!(flattened_fields.len(), 3);
        assert!(flattened_fields.contains("metadata"));
        assert!(flattened_fields.contains("metadata.count"));
        assert!(flattened_fields.contains("metadata.label"));
    }

    #[test]
    fn test_unnest_empty_map() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), av_string("Alice"));
        item.insert("empty".to_string(), av_map(HashMap::new()));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 1).expect("unnested item");

        // Empty map shouldn't add any keys
        assert_eq!(result.len(), 1);
        assert!(matches!(result.get("name"), Some(AttributeValue::S(s)) if s == "Alice"));

        // Empty map was encountered and marked as completely flattened
        assert_eq!(flattened_fields.len(), 1);
        assert!(flattened_fields.contains("empty"));
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

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 1).expect("unnested item");

        // Only flatten one level deep
        assert_eq!(result.len(), 1);
        assert!(matches!(
            result.get("level1.level2"),
            Some(AttributeValue::M(_))
        ));

        // Only level1 was recursed into and flattened
        // level1.level2 is NOT tracked (it's a Map that hit the depth limit)
        assert_eq!(flattened_fields.len(), 1);
        assert!(flattened_fields.contains("level1"));
    }

    #[test]
    fn test_unnest_partially_flattened_nested_map() {
        // Test case where a nested map is NOT completely flattened due to depth limit
        let mut level3 = HashMap::new();
        level3.insert("deep_value".to_string(), av_string("very_deep"));

        let mut level2 = HashMap::new();
        level2.insert("level3".to_string(), av_map(level3));
        level2.insert("other".to_string(), av_string("data"));

        let mut level1 = HashMap::new();
        level1.insert("level2".to_string(), av_map(level2));

        let mut item = HashMap::new();
        item.insert("level1".to_string(), av_map(level1));
        item.insert("top_level".to_string(), av_string("value"));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 2).expect("unnested item");

        // Should flatten 2 levels deep
        assert_eq!(result.len(), 3);
        assert!(matches!(result.get("top_level"), Some(AttributeValue::S(s)) if s == "value"));
        assert!(
            matches!(result.get("level1.level2.other"), Some(AttributeValue::S(s)) if s == "data")
        );
        assert!(matches!(
            result.get("level1.level2.level3"),
            Some(AttributeValue::M(_))
        ));

        // level1 and level1.level2 were recursed into
        // level1.level2.other is a leaf field with dots
        // level1.level2.level3 hit depth limit - NOT tracked (still a Map)
        assert_eq!(flattened_fields.len(), 3);
        assert!(flattened_fields.contains("level1"));
        assert!(flattened_fields.contains("level1.level2"));
        assert!(flattened_fields.contains("level1.level2.other"));
    }

    #[test]
    fn test_unnest_completely_vs_not_completely_flattened() {
        // Item with both completely flattened and not completely flattened maps
        let mut deep_map = HashMap::new();
        deep_map.insert("very_deep".to_string(), av_string("value"));

        let mut partial_map = HashMap::new();
        partial_map.insert("nested".to_string(), av_map(deep_map));

        let mut complete_map = HashMap::new();
        complete_map.insert("field".to_string(), av_string("data"));

        let mut item = HashMap::new();
        item.insert("complete".to_string(), av_map(complete_map));
        item.insert("partial".to_string(), av_map(partial_map));

        let (result, flattened_fields) = unnest_dynamodb_row(&item, 1).expect("unnested item");

        // "complete" is completely flattened (1 level deep)
        assert!(matches!(result.get("complete.field"), Some(AttributeValue::S(s)) if s == "data"));

        // "partial" is NOT completely flattened (would need 2 levels)
        assert!(matches!(
            result.get("partial.nested"),
            Some(AttributeValue::M(_))
        ));

        // Verify flattened_fields: only Maps recursed into and leaf fields with dots
        assert_eq!(flattened_fields.len(), 3);
        assert!(flattened_fields.contains("complete")); // Map that was recursed into
        assert!(flattened_fields.contains("complete.field")); // Leaf field with dot
        assert!(flattened_fields.contains("partial")); // Map that was recursed into
        // "partial.nested" is NOT in flattened_fields (Map that hit depth limit)
    }
}
