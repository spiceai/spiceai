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
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug)]
pub enum DynamoDBRequestPlan {
    Query(QueryParams),
    Scan(ScanParams),
}

#[derive(Default, Clone)]
pub struct QueryParams {
    pub table_name: String,
    pub key_condition_expression: Option<String>,
    pub filter_expression: Option<String>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub projection_expression: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Default, Clone)]
pub struct QueryParamsBuilder {
    table_name: String,
    key_condition_expression: Option<String>,
    filter_expression: Option<String>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    expression_attribute_names: Option<HashMap<String, String>>,
    projection_expression: Option<String>,
    limit: Option<i32>,
}

impl QueryParams {
    pub fn builder() -> QueryParamsBuilder {
        QueryParamsBuilder::default()
    }
}

impl QueryParamsBuilder {
    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = value.into();
        self
    }

    // strip_option means these setters take T, not Option<T>
    pub fn key_condition_expression(mut self, value: impl Into<String>) -> Self {
        self.key_condition_expression = Some(value.into());
        self
    }

    pub fn filter_expression(mut self, value: impl Into<String>) -> Self {
        self.filter_expression = Some(value.into());
        self
    }

    pub fn expression_attribute_values(
        mut self,
        value: impl Into<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = Some(value.into());
        self
    }

    pub fn expression_attribute_names(mut self, value: impl Into<HashMap<String, String>>) -> Self {
        self.expression_attribute_names = Some(value.into());
        self
    }

    pub fn projection_expression(mut self, value: impl Into<String>) -> Self {
        self.projection_expression = Some(value.into());
        self
    }

    pub fn limit(mut self, value: impl Into<i32>) -> Self {
        self.limit = Some(value.into());
        self
    }

    pub fn build(self) -> QueryParams {
        QueryParams {
            table_name: self.table_name,
            key_condition_expression: self.key_condition_expression,
            filter_expression: self.filter_expression,
            expression_attribute_values: self.expression_attribute_values,
            expression_attribute_names: self.expression_attribute_names,
            projection_expression: self.projection_expression,
            limit: self.limit,
        }
    }
}

// Same pattern for ScanParams
#[derive(Default, Clone)]
pub struct ScanParams {
    pub table_name: String,
    pub filter_expression: Option<String>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub projection_expression: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Default, Clone)]
pub struct ScanParamsBuilder {
    table_name: String,
    filter_expression: Option<String>,
    expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    expression_attribute_names: Option<HashMap<String, String>>,
    projection_expression: Option<String>,
    limit: Option<i32>,
}

impl ScanParams {
    pub fn builder() -> ScanParamsBuilder {
        ScanParamsBuilder::default()
    }
}

impl ScanParamsBuilder {
    pub fn table_name(mut self, value: impl Into<String>) -> Self {
        self.table_name = value.into();
        self
    }

    pub fn filter_expression(mut self, value: impl Into<String>) -> Self {
        self.filter_expression = Some(value.into());
        self
    }

    pub fn expression_attribute_values(
        mut self,
        value: impl Into<HashMap<String, AttributeValue>>,
    ) -> Self {
        self.expression_attribute_values = Some(value.into());
        self
    }

    pub fn expression_attribute_names(mut self, value: impl Into<HashMap<String, String>>) -> Self {
        self.expression_attribute_names = Some(value.into());
        self
    }

    pub fn projection_expression(mut self, value: impl Into<String>) -> Self {
        self.projection_expression = Some(value.into());
        self
    }

    pub fn limit(mut self, value: impl Into<i32>) -> Self {
        self.limit = Some(value.into());
        self
    }

    pub fn build(self) -> ScanParams {
        ScanParams {
            table_name: self.table_name,
            filter_expression: self.filter_expression,
            expression_attribute_values: self.expression_attribute_values,
            expression_attribute_names: self.expression_attribute_names,
            projection_expression: self.projection_expression,
            limit: self.limit,
        }
    }
}

impl fmt::Debug for QueryParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("QueryParams");

        debug_struct.field("table_name", &self.table_name);
        debug_struct.field("key_condition_expression", &self.key_condition_expression);
        debug_struct.field("filter_expression", &self.filter_expression);

        // Sort expression_attribute_values
        if let Some(ref values) = self.expression_attribute_values {
            let mut sorted: Vec<_> = values.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            debug_struct.field("expression_attribute_values", &DebugSortedMap(&sorted));
        } else {
            debug_struct.field(
                "expression_attribute_values",
                &self.expression_attribute_values,
            );
        }

        // Sort expression_attribute_names
        if let Some(ref names) = self.expression_attribute_names {
            let mut sorted: Vec<_> = names.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            debug_struct.field("expression_attribute_names", &DebugSortedMap(&sorted));
        } else {
            debug_struct.field(
                "expression_attribute_names",
                &self.expression_attribute_names,
            );
        }

        debug_struct.field("projection_expression", &self.projection_expression);
        debug_struct.field("limit", &self.limit);

        debug_struct.finish()
    }
}

impl fmt::Debug for ScanParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("ScanParams");

        debug_struct.field("table_name", &self.table_name);
        debug_struct.field("filter_expression", &self.filter_expression);

        // Sort expression_attribute_values
        if let Some(ref values) = self.expression_attribute_values {
            let mut sorted: Vec<_> = values.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            debug_struct.field("expression_attribute_values", &DebugSortedMap(&sorted));
        } else {
            debug_struct.field(
                "expression_attribute_values",
                &self.expression_attribute_values,
            );
        }

        // Sort expression_attribute_names
        if let Some(ref names) = self.expression_attribute_names {
            let mut sorted: Vec<_> = names.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            debug_struct.field("expression_attribute_names", &DebugSortedMap(&sorted));
        } else {
            debug_struct.field(
                "expression_attribute_names",
                &self.expression_attribute_names,
            );
        }

        debug_struct.field("projection_expression", &self.projection_expression);
        debug_struct.field("limit", &self.limit);

        debug_struct.finish()
    }
}

// Helper struct to format sorted maps
struct DebugSortedMap<'a, K, V>(&'a [(&'a K, &'a V)]);

impl<K: fmt::Debug, V: fmt::Debug> fmt::Debug for DebugSortedMap<'_, K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.0.iter().copied()).finish()
    }
}
