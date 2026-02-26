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
use crate::dynamodb::table_schema::DynamoDBTableSchema;
use aws_sdk_dynamodb::types::AttributeValue;
use chrono::{DateTime, FixedOffset};
use datafusion::common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::HashMap;
use std::str::FromStr;
use util::time_format::format_datetime;

pub fn scalar_to_attribute_value(
    scalar: &ScalarValue,
    time_format: &str,
) -> datafusion::error::Result<AttributeValue> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Ok(AttributeValue::S(s.clone())),
        ScalarValue::Int64(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Int32(Some(i)) => Ok(AttributeValue::N(i.to_string())),
        ScalarValue::Float64(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Float32(Some(f)) => Ok(AttributeValue::N(f.to_string())),
        ScalarValue::Boolean(Some(b)) => Ok(AttributeValue::Bool(*b)),
        ScalarValue::TimestampMillisecond(Some(timestamp_in_millis), tz_opt) => {
            let Some(dt_utc) = DateTime::from_timestamp_millis(*timestamp_in_millis) else {
                return Err(DataFusionError::Internal(format!(
                    "Failed to convert timestamp in millis to DateTime: {timestamp_in_millis}"
                )));
            };

            let dt: DateTime<FixedOffset> = match tz_opt {
                Some(tz_str) => {
                    let tz = FixedOffset::from_str(tz_str).map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Failed to parse TimeZone \"{tz_str}\": {e}"
                        ))
                    })?;
                    dt_utc.with_timezone(&tz)
                }
                None => dt_utc.fixed_offset(),
            };

            let Some(formatted) = format_datetime(dt, time_format) else {
                return Err(DataFusionError::Internal(format!(
                    "Failed to parse timestamp. Verify format is valid: \"{time_format}\""
                )));
            };

            Ok(AttributeValue::S(formatted))
        }
        ScalarValue::Null => Ok(AttributeValue::Null(true)),
        _ => Err(DataFusionError::NotImplemented(
            "ScalarValue type not supported".to_string(),
        )),
    }
}

pub struct FilterStringVisitor<'a> {
    schema: &'a DynamoDBTableSchema,
    attribute_values: &'a mut HashMap<String, AttributeValue>,
    value_counter: &'a mut usize,
    pub result_stack: Vec<String>,
    pub error: Option<DataFusionError>,
}

impl<'a> FilterStringVisitor<'a> {
    pub fn new(
        schema: &'a DynamoDBTableSchema,
        attribute_values: &'a mut HashMap<String, AttributeValue>,
        value_counter: &'a mut usize,
    ) -> Self {
        Self {
            schema,
            attribute_values,
            value_counter,
            result_stack: Vec::new(),
            error: None,
        }
    }

    fn get_column_alias(&self, column_name: &str) -> String {
        if self.schema.is_flattened_field(column_name) {
            column_name
                .split('.')
                .map(|segment| format!("#{segment}"))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            format!("#{column_name}")
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for FilterStringVisitor<'_> {
    type Node = Expr;

    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion, DataFusionError> {
        if self.error.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }

        match node {
            Expr::Column(col) => {
                self.result_stack.push(self.get_column_alias(col.name()));
                Ok(TreeNodeRecursion::Continue)
            }
            Expr::Literal(scalar, _) => {
                let value_key = format!(":v{}", self.value_counter);
                *self.value_counter += 1;

                match scalar_to_attribute_value(scalar, &self.schema.time_format()) {
                    Ok(attr_value) => {
                        self.attribute_values.insert(value_key.clone(), attr_value);
                        self.result_stack.push(value_key);
                        Ok(TreeNodeRecursion::Continue)
                    }
                    Err(e) => {
                        self.error = Some(e);
                        Ok(TreeNodeRecursion::Stop)
                    }
                }
            }
            Expr::BinaryExpr(BinaryExpr { op, .. }) => {
                let Some(right_str) = self.result_stack.pop() else {
                    self.error = Some(DataFusionError::Internal(
                        "Missing right operand in result stack".to_string(),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                };

                let Some(left_str) = self.result_stack.pop() else {
                    self.error = Some(DataFusionError::Internal(
                        "Missing left operand in result stack".to_string(),
                    ));
                    return Ok(TreeNodeRecursion::Stop);
                };

                let op_str = match op {
                    Operator::Eq => "=",
                    Operator::NotEq => "<>",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => {
                        self.error = Some(DataFusionError::NotImplemented(format!(
                            "Operator {op:?} not supported"
                        )));
                        return Ok(TreeNodeRecursion::Stop);
                    }
                };

                self.result_stack
                    .push(format!("({left_str} {op_str} {right_str})"));
                Ok(TreeNodeRecursion::Continue)
            }
            _ => {
                self.error = Some(DataFusionError::NotImplemented(
                    "Expression type not supported in filters".to_string(),
                ));
                Ok(TreeNodeRecursion::Stop)
            }
        }
    }
}
