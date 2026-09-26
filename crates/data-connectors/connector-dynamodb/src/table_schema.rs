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
use crate::filter::{KeyReading, Translator};
use aws_sdk_dynamodb::types::ScalarAttributeType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use std::collections::HashSet;
use std::sync::Arc;

/// Encapsulates `DynamoDB` table schema, keys, and expression conversion logic.
/// This struct knows WHAT the table structure is and WHAT operations are supported.
#[derive(Debug, Clone)]
pub struct DynamoDBTableSchema {
    table_name: Arc<str>,
    table_schema: SchemaRef,
    primary_keys: Vec<String>,
    partition_key: String,
    sort_key: Option<String>,
    flattened_fields: HashSet<String>,
    time_format: Arc<String>,
    partition_key_type: Option<ScalarAttributeType>,
    sort_key_type: Option<ScalarAttributeType>,
    /// A JSON nesting catch-all column, assembled from every attribute not
    /// declared as a column, so it names no attribute of its own.
    catch_all: Option<String>,
    /// How many levels of maps unnesting flattens into dotted columns.
    unnest_depth: usize,
}

impl DynamoDBTableSchema {
    pub fn new(
        table_name: Arc<str>,
        table_schema: SchemaRef,
        partition_key: String,
        sort_key: Option<String>,
        flattened_fields: HashSet<String>,
        time_format: &str,
    ) -> Self {
        let mut primary_keys = vec![partition_key.clone()];
        if let Some(sort_key) = &sort_key {
            primary_keys.push(sort_key.clone());
        }
        Self {
            table_name,
            table_schema,
            primary_keys,
            partition_key,
            sort_key,
            flattened_fields,
            time_format: Arc::from(time_format.to_string()),
            partition_key_type: None,
            sort_key_type: None,
            catch_all: None,
            unnest_depth: 0,
        }
    }

    /// How many levels of maps unnesting flattens into dotted columns, if any.
    #[must_use]
    pub fn with_unnest_depth(mut self, depth: Option<usize>) -> Self {
        self.unnest_depth = depth.unwrap_or(0);
        self
    }

    pub fn unnest_depth(&self) -> usize {
        self.unnest_depth
    }

    /// The `DynamoDB` types of the key attributes, from the table description.
    /// Without them no key condition is written.
    #[must_use]
    pub fn with_key_types(
        mut self,
        partition_key_type: Option<ScalarAttributeType>,
        sort_key_type: Option<ScalarAttributeType>,
    ) -> Self {
        self.partition_key_type = partition_key_type;
        self.sort_key_type = sort_key_type;
        self
    }

    #[must_use]
    pub fn with_catch_all(mut self, catch_all: Option<String>) -> Self {
        self.catch_all = catch_all;
        self
    }

    pub fn partition_key_type(&self) -> Option<&ScalarAttributeType> {
        self.partition_key_type.as_ref()
    }

    pub fn sort_key_type(&self) -> Option<&ScalarAttributeType> {
        self.sort_key_type.as_ref()
    }

    pub fn is_catch_all(&self, column: &str) -> bool {
        self.catch_all.as_deref() == Some(column)
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub fn primary_keys(&self) -> Vec<String> {
        self.primary_keys.clone()
    }

    pub fn time_format(&self) -> Arc<String> {
        Arc::clone(&self.time_format)
    }

    pub fn partition_key(&self) -> &str {
        &self.partition_key
    }

    pub fn sort_key(&self) -> Option<&str> {
        self.sort_key.as_deref()
    }

    /// How the sort key's column reads the key's values.
    pub(crate) fn sort_key_reading(&self) -> KeyReading {
        self.sort_key
            .as_deref()
            .and_then(|sort_key| self.table_schema.field_with_name(sort_key).ok())
            .map_or(KeyReading::Stored, |field| {
                KeyReading::of(field.data_type())
            })
    }

    pub fn is_flattened_field(&self, field_name: &str) -> bool {
        if self.flattened_fields.contains(field_name) {
            return true;
        }

        // Check if any parent prefix is flattened
        let mut parts: Vec<&str> = field_name.split('.').collect();
        while parts.len() > 1 {
            parts.pop();
            let parent = parts.join(".");
            if self.flattened_fields.contains(&parent) {
                return true;
            }
        }

        false
    }

    pub fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        Translator::new(self).classify(filters)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashSet;
    use std::sync::Arc;

    fn create_test_schema_with_flattened() -> DynamoDBTableSchema {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("metadata.name", DataType::Utf8, true),
            Field::new("metadata.tags.version", DataType::Utf8, true),
        ]));

        let mut flattened = HashSet::new();
        flattened.insert("metadata".to_string());

        DynamoDBTableSchema::new(
            Arc::from("test_table"),
            schema,
            "id".to_string(),
            None,
            flattened,
            "2006-01-02T15:04:05.000Z07:00",
        )
    }

    #[test]
    fn test_is_flattened_field_nested() {
        let schema = create_test_schema_with_flattened();

        // If parent is flattened, children should also be considered flattened
        assert!(schema.is_flattened_field("metadata"));
        assert!(schema.is_flattened_field("metadata.name"));
        assert!(schema.is_flattened_field("metadata.tags"));
        assert!(schema.is_flattened_field("metadata.tags.version"));
        assert!(!schema.is_flattened_field("id"));
        assert!(!schema.is_flattened_field("other.field"));
    }

    #[test]
    fn test_primary_keys() {
        let schema = create_test_schema_with_flattened();
        assert_eq!(schema.table_name(), "test_table");
        assert_eq!(schema.partition_key(), "id");
        assert_eq!(schema.sort_key(), None);
        assert_eq!(schema.primary_keys(), vec!["id".to_string()]);
    }
}
