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

#[derive(Clone, Debug)]
pub enum DynamoDBRequestPlan {
    Query(QueryParams),
    Scan(ScanParams),
}

#[derive(Debug, Default, Clone)]
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
#[derive(Debug, Default, Clone)]
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
