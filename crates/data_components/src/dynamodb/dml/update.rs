/*
Copyright 2026 The Spice.ai OSS Authors

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

use super::keys::extract_primary_keys;
use crate::dynamodb::utils::scalar_to_attribute_value;
use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodb::types::AttributeValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::Arc;

pub struct UpdateConfig {
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub time_format: Arc<String>,
    /// Pairs of (`column_name`, `new_value_expr`). Only `Expr::Literal` values are supported.
    pub assignments: Vec<(String, Expr)>,
    pub filters: Vec<Expr>,
    pub parallelism: usize,
}

pub struct DynamoDBUpdateExec {
    pub db_client: Arc<DbClient>,
    pub table_name: String,
    pub config: UpdateConfig,
    properties: Arc<PlanProperties>,
}

impl DynamoDBUpdateExec {
    #[must_use]
    pub fn new(db_client: Arc<DbClient>, table_name: String, config: UpdateConfig) -> Self {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            db_client,
            table_name,
            config,
            properties,
        }
    }
}

impl std::fmt::Debug for DynamoDBUpdateExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamoDBUpdateExec")
            .field("table_name", &self.table_name)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DynamoDBUpdateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DynamoDBUpdateExec(table={})", self.table_name)
    }
}

impl ExecutionPlan for DynamoDBUpdateExec {
    fn name(&self) -> &'static str {
        "DynamoDBUpdateExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let client = Arc::clone(&self.db_client);
        let table_name = self.table_name.clone();
        let config = UpdateConfig {
            partition_key: self.config.partition_key.clone(),
            sort_key: self.config.sort_key.clone(),
            time_format: Arc::clone(&self.config.time_format),
            assignments: self.config.assignments.clone(),
            filters: self.config.filters.clone(),
            parallelism: self.config.parallelism,
        };

        let stream = futures::stream::once(async move {
            let count = execute_update(&client, &table_name, &config).await?;

            let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
            RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).map_err(|e| {
                DataFusionError::Execution(format!("Failed to build count batch: {e}"))
            })
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

async fn execute_update(
    client: &Arc<DbClient>,
    table_name: &str,
    config: &UpdateConfig,
) -> DataFusionResult<u64> {
    let keys = extract_primary_keys(
        &config.filters,
        &config.partition_key,
        config.sort_key.as_deref(),
        &config.time_format,
    )?;

    // Build the SET expression and attribute maps from assignments.
    let UpdateExpression {
        expression: update_expression,
        attr_names,
        attr_values,
    } = build_update_expression(&config.assignments, &config.time_format)?;

    // Issue UpdateItem calls in parallel chunks.
    let mut total: u64 = 0;
    for chunk in keys.chunks(config.parallelism) {
        let futures: Vec<_> = chunk
            .iter()
            .map(|(pk_attr, sk_attr)| {
                let client = Arc::clone(client);
                let table_name = table_name.to_string();
                let partition_key = config.partition_key.clone();
                let sort_key = config.sort_key.clone();
                let update_expression = update_expression.clone();
                let attr_names = attr_names.clone();
                let attr_values = attr_values.clone();
                let pk_attr = pk_attr.clone();
                let sk_attr = sk_attr.clone();

                async move {
                    let mut req = client
                        .update_item()
                        .table_name(table_name)
                        .key(partition_key, pk_attr)
                        .update_expression(update_expression)
                        .set_expression_attribute_names(Some(attr_names))
                        .set_expression_attribute_values(Some(attr_values));

                    if let (Some(sk_name), Some(sk_val)) = (sort_key, sk_attr) {
                        req = req.key(sk_name, sk_val);
                    }

                    req.send().await.map_err(|e| {
                        DataFusionError::Execution(format!("DynamoDB UpdateItem failed: {e}"))
                    })
                }
            })
            .collect();

        let results = join_all(futures).await;
        for result in results {
            result?;
            total += 1;
        }
    }

    Ok(total)
}

struct UpdateExpression {
    /// The `SET #n0 = :v0, ...` expression string passed to `UpdateItem`.
    expression: String,
    /// Maps placeholder names (e.g. `#n0`) to actual attribute names.
    attr_names: HashMap<String, String>,
    /// Maps placeholder names (e.g. `:v0`) to `AttributeValue`s.
    attr_values: HashMap<String, AttributeValue>,
}

/// Build a `DynamoDB` `UpdateExpression` of the form `SET #n0 = :v0, #n1 = :v1, ...`
/// from a list of (column, `Expr::Literal`) assignments.
///
/// Uses expression attribute names (`#n<i>`) to avoid reserved word conflicts.
fn build_update_expression(
    assignments: &[(String, Expr)],
    time_format: &str,
) -> DataFusionResult<UpdateExpression> {
    if assignments.is_empty() {
        return Err(DataFusionError::Plan(
            "DynamoDB UPDATE requires at least one column assignment".to_string(),
        ));
    }

    let mut set_parts: Vec<String> = Vec::with_capacity(assignments.len());
    let mut attr_names: HashMap<String, String> = HashMap::new();
    let mut attr_values: HashMap<String, AttributeValue> = HashMap::new();

    for (i, (col_name, value_expr)) in assignments.iter().enumerate() {
        let name_placeholder = format!("#n{i}");
        let value_placeholder = format!(":v{i}");

        let Expr::Literal(scalar, _) = value_expr else {
            return Err(DataFusionError::Plan(format!(
                "DynamoDB UPDATE only supports literal values in SET assignments, got: {value_expr}"
            )));
        };

        let attr_value = scalar_to_attribute_value(scalar, time_format)?;

        set_parts.push(format!("{name_placeholder} = {value_placeholder}"));
        attr_names.insert(name_placeholder, col_name.clone());
        attr_values.insert(value_placeholder, attr_value);
    }

    Ok(UpdateExpression {
        expression: format!("SET {}", set_parts.join(", ")),
        attr_names,
        attr_values,
    })
}
