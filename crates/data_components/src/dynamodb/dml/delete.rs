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

use super::streaming_batch_write;
use crate::delete::DeletionSink;
use crate::dynamodb::utils::scalar_to_attribute_value;
use arrow_array::Array;
use async_trait::async_trait;
use aws_sdk_dynamodb::Client as DbClient;
use aws_sdk_dynamodb::types::{DeleteRequest, WriteRequest};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use std::collections::HashMap;
use std::sync::Arc;

use aws_sdk_dynamodb::types::AttributeValue;

pub struct DynamoDBDeletionSink {
    pub db_client: Arc<DbClient>,
    pub table_name: String,
    pub partition_key: String,
    pub sort_key: Option<String>,
    pub time_format: Arc<String>,
    pub filters: Vec<Expr>,
    pub parallelism: usize,
}

/// A single primary key to delete: (`partition_key_value`, optional `sort_key_value`).
type KeyToDelete = (AttributeValue, Option<AttributeValue>);

#[async_trait]
impl DeletionSink for DynamoDBDeletionSink {
    async fn delete_from(
        &self,
    ) -> std::result::Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let keys = extract_delete_keys(
            &self.filters,
            &self.partition_key,
            self.sort_key.as_deref(),
            &self.time_format,
        )
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let mut write_requests: Vec<WriteRequest> = Vec::with_capacity(keys.len());
        for (pk_val, sk_val) in &keys {
            let mut key = HashMap::new();
            key.insert(self.partition_key.clone(), pk_val.clone());
            if let (Some(sk_name), Some(sk_val)) = (&self.sort_key, sk_val) {
                key.insert(sk_name.clone(), sk_val.clone());
            }

            let delete_request = DeleteRequest::builder()
                .set_key(Some(key))
                .build()
                .map_err(|e| {
                    Box::new(DataFusionError::Execution(format!(
                        "Failed to build DeleteRequest: {e}"
                    ))) as Box<dyn std::error::Error + Send + Sync>
                })?;
            write_requests.push(
                WriteRequest::builder()
                    .delete_request(delete_request)
                    .build(),
            );
        }

        let request_stream = futures::stream::iter(write_requests.into_iter().map(Ok));

        streaming_batch_write(
            &self.db_client,
            &self.table_name,
            Box::pin(request_stream),
            self.parallelism,
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Extract primary key values from DELETE filter expressions.
///
/// Supports:
/// - `pk = <literal>` (single delete, no sort key)
/// - `pk = <literal> AND sk = <literal>` (single delete with sort key)
/// - `pk = <lit> OR pk = <lit> OR ...` (multiple deletes, no sort key)
/// - `(pk = <lit> AND sk = <lit>) OR (pk = <lit> AND sk = <lit>) OR ...`
///
/// Rejects filters on non-primary-key columns.
fn extract_delete_keys(
    filters: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
) -> Result<Vec<KeyToDelete>, DataFusionError> {
    if filters.is_empty() {
        return Err(DataFusionError::Plan(
            "DynamoDB DELETE requires a WHERE clause on the primary key".to_string(),
        ));
    }

    let mut keys = Vec::new();
    for filter in filters {
        extract_keys_from_expr(filter, partition_key, sort_key, time_format, &mut keys)?;
    }

    if keys.is_empty() {
        return Err(DataFusionError::Plan(
            "DynamoDB DELETE: no primary key values found in WHERE clause".to_string(),
        ));
    }

    Ok(keys)
}

fn extract_keys_from_expr(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    keys: &mut Vec<KeyToDelete>,
) -> Result<(), DataFusionError> {
    match expr {
        // OR: each side is a separate key to delete
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            extract_keys_from_expr(left, partition_key, sort_key, time_format, keys)?;
            extract_keys_from_expr(right, partition_key, sort_key, time_format, keys)?;
        }
        // AND: combine pk and sk from both sides into a single key
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            let mut pk_val = None;
            let mut sk_val = None;
            collect_key_equalities(
                left,
                partition_key,
                sort_key,
                time_format,
                &mut pk_val,
                &mut sk_val,
            )?;
            collect_key_equalities(
                right,
                partition_key,
                sort_key,
                time_format,
                &mut pk_val,
                &mut sk_val,
            )?;

            let Some(pk) = pk_val else {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DELETE: AND expression must include partition key '{partition_key}'"
                )));
            };
            keys.push((pk, sk_val));
        }
        // Simple equality: pk = <literal>
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            match (left.as_ref(), right.as_ref()) {
                // Simple: column = literal
                (Expr::Column(col), Expr::Literal(scalar, _))
                | (Expr::Literal(scalar, _), Expr::Column(col)) => {
                    let col = col.name();
                    if col == partition_key {
                        let attr = scalar_to_attribute_value(scalar, time_format)?;
                        keys.push((attr, None));
                    } else if sort_key.is_some_and(|sk| sk == col) {
                        return Err(DataFusionError::Plan(
                            "DynamoDB DELETE: sort key filter must be combined with partition key using AND".to_string(),
                        ));
                    } else {
                        return Err(DataFusionError::Plan(format!(
                            "DynamoDB DELETE only supports filters on primary key columns, got '{col}'"
                        )));
                    }
                }
                // Struct equality: struct(pk, sk) = struct(v1, v2)
                // DataFusion rewrites single-element IN lists like (pk, sk) IN ((v1, v2))
                // into struct(pk, sk) = struct(v1, v2)
                (Expr::ScalarFunction(func), Expr::Literal(scalar, _))
                | (Expr::Literal(scalar, _), Expr::ScalarFunction(func)) => {
                    extract_keys_from_struct_eq(
                        func,
                        scalar,
                        partition_key,
                        sort_key,
                        time_format,
                        keys,
                    )?;
                }
                _ => {
                    return Err(DataFusionError::Plan(
                        "DynamoDB DELETE only supports column = literal or struct = struct filters on primary key columns".to_string(),
                    ));
                }
            }
        }
        // Simple IN list: pk IN (v1, v2, v3)
        Expr::InList(InList {
            expr: in_expr,
            list,
            negated,
        }) => {
            if *negated {
                return Err(DataFusionError::Plan(
                    "DynamoDB DELETE does not support NOT IN".to_string(),
                ));
            }
            extract_keys_from_in_list(in_expr, list, partition_key, sort_key, time_format, keys)?;
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "DynamoDB DELETE: unsupported filter expression: {expr}"
            )));
        }
    }
    Ok(())
}

/// Extract a single column = literal equality from an expression, matching it to pk or sk.
fn collect_key_equalities(
    expr: &Expr,
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    pk_val: &mut Option<AttributeValue>,
    sk_val: &mut Option<AttributeValue>,
) -> Result<(), DataFusionError> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => {
            collect_key_equalities(left, partition_key, sort_key, time_format, pk_val, sk_val)?;
            collect_key_equalities(right, partition_key, sort_key, time_format, pk_val, sk_val)?;
        }
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            let (col, scalar) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(scalar, _))
                | (Expr::Literal(scalar, _), Expr::Column(col)) => (col.name(), scalar),
                _ => {
                    return Err(DataFusionError::Plan(
                        "DynamoDB DELETE only supports column = literal filters on primary key columns".to_string(),
                    ));
                }
            };

            let attr = scalar_to_attribute_value(scalar, time_format)?;
            if col == partition_key {
                *pk_val = Some(attr);
            } else if sort_key.is_some_and(|sk| sk == col) {
                *sk_val = Some(attr);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DELETE only supports filters on primary key columns, got '{col}'"
                )));
            }
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "DynamoDB DELETE: unsupported filter expression in AND clause: {expr}"
            )));
        }
    }
    Ok(())
}

/// Handle `pk IN (v1, v2, ...)` and `(pk, sk) IN ((v1, v2), (v3, v4), ...)`.
fn extract_keys_from_in_list(
    in_expr: &Expr,
    list: &[Expr],
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    keys: &mut Vec<KeyToDelete>,
) -> Result<(), DataFusionError> {
    match in_expr {
        // Simple: pk IN (v1, v2, v3)
        Expr::Column(col) => {
            if col.name() != partition_key {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DELETE IN list only supports partition key column '{}', got '{}'",
                    partition_key,
                    col.name()
                )));
            }
            for item in list {
                let Expr::Literal(scalar, _) = item else {
                    return Err(DataFusionError::Plan(format!(
                        "DynamoDB DELETE IN list values must be literals, got: {item}"
                    )));
                };
                let attr = scalar_to_attribute_value(scalar, time_format)?;
                keys.push((attr, None));
            }
            Ok(())
        }
        // Tuple: (pk, sk) IN ((v1, v2), (v3, v4))
        // DataFusion represents the tuple as a ScalarFunction (named_struct or struct)
        // with Column args. The list items are struct literals.
        Expr::ScalarFunction(func) => {
            let columns: Vec<&str> = func
                .args
                .iter()
                .map(|arg| match arg {
                    Expr::Column(col) => Ok(col.name()),
                    _ => Err(DataFusionError::Plan(
                        "DynamoDB DELETE: tuple IN list expr must contain only columns".to_string(),
                    )),
                })
                .collect::<Result<_, _>>()?;

            let pk_idx = columns
                .iter()
                .position(|&c| c == partition_key)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "DynamoDB DELETE: tuple IN list must include partition key '{partition_key}'"
                    ))
                })?;

            let sk_idx = sort_key
                .map(|sk| {
                    columns.iter().position(|&c| c == sk).ok_or_else(|| {
                        DataFusionError::Plan(
                            "DynamoDB DELETE: tuple IN list includes non-primary-key columns"
                                .to_string(),
                        )
                    })
                })
                .transpose()?;

            let expected_len = if sort_key.is_some() { 2 } else { 1 };
            if columns.len() != expected_len {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DELETE: tuple IN list must contain only primary key columns, got {} columns",
                    columns.len()
                )));
            }

            for item in list {
                let (pk_attr, sk_attr) = match item {
                    // Optimized case: struct literal (e.g., after constant folding)
                    Expr::Literal(ScalarValue::Struct(struct_array), _) => {
                        if struct_array.len() != 1 {
                            return Err(DataFusionError::Plan(
                                "DynamoDB DELETE: expected single-row struct in IN list"
                                    .to_string(),
                            ));
                        }
                        let pk_scalar = ScalarValue::try_from_array(struct_array.column(pk_idx), 0)
                            .map_err(|e| {
                                DataFusionError::Plan(format!(
                                    "Failed to extract pk from struct: {e}"
                                ))
                            })?;
                        let pk_attr = scalar_to_attribute_value(&pk_scalar, time_format)?;
                        let sk_attr = sk_idx
                            .map(|idx| {
                                let sk_scalar =
                                    ScalarValue::try_from_array(struct_array.column(idx), 0)
                                        .map_err(|e| {
                                            DataFusionError::Plan(format!(
                                                "Failed to extract sk from struct: {e}"
                                            ))
                                        })?;
                                scalar_to_attribute_value(&sk_scalar, time_format)
                            })
                            .transpose()?;
                        (pk_attr, sk_attr)
                    }
                    // Unfolded case: struct(Literal, Literal) — a ScalarFunction with literal args
                    Expr::ScalarFunction(func) => {
                        extract_key_pair_from_struct_func(func, pk_idx, sk_idx, time_format)?
                    }
                    _ => {
                        return Err(DataFusionError::Plan(format!(
                            "DynamoDB DELETE: tuple IN list values must be struct literals, got: {item}"
                        )));
                    }
                };
                keys.push((pk_attr, sk_attr));
            }
            Ok(())
        }
        _ => Err(DataFusionError::Plan(format!(
            "DynamoDB DELETE: unsupported IN list expression: {in_expr}"
        ))),
    }
}

/// Handle `struct(pk, sk) = struct(v1, v2)` — produced when `DataFusion` optimizes
/// a single-element tuple IN list: `(pk, sk) IN ((v1, v2))` → `struct(pk, sk) = struct(v1, v2)`.
fn extract_keys_from_struct_eq(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    scalar: &ScalarValue,
    partition_key: &str,
    sort_key: Option<&str>,
    time_format: &str,
    keys: &mut Vec<KeyToDelete>,
) -> Result<(), DataFusionError> {
    let columns: Vec<&str> = func
        .args
        .iter()
        .map(|arg| match arg {
            Expr::Column(col) => Ok(col.name()),
            _ => Err(DataFusionError::Plan(
                "DynamoDB DELETE: struct equality expr must contain only columns".to_string(),
            )),
        })
        .collect::<Result<_, _>>()?;

    let pk_idx = columns
        .iter()
        .position(|&c| c == partition_key)
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "DynamoDB DELETE: struct equality must include partition key '{partition_key}'"
            ))
        })?;

    let sk_idx = sort_key
        .map(|sk| {
            columns.iter().position(|&c| c == sk).ok_or_else(|| {
                DataFusionError::Plan(
                    "DynamoDB DELETE: struct equality includes non-primary-key columns".to_string(),
                )
            })
        })
        .transpose()?;

    let expected_len = if sort_key.is_some() { 2 } else { 1 };
    if columns.len() != expected_len {
        return Err(DataFusionError::Plan(format!(
            "DynamoDB DELETE: struct equality must contain only primary key columns, got {} columns",
            columns.len()
        )));
    }

    let ScalarValue::Struct(struct_array) = scalar else {
        return Err(DataFusionError::Plan(
            "DynamoDB DELETE: struct equality right-hand side must be a struct literal".to_string(),
        ));
    };

    if struct_array.len() != 1 {
        return Err(DataFusionError::Plan(
            "DynamoDB DELETE: expected single-row struct in equality".to_string(),
        ));
    }

    let pk_scalar = ScalarValue::try_from_array(struct_array.column(pk_idx), 0)
        .map_err(|e| DataFusionError::Plan(format!("Failed to extract pk from struct: {e}")))?;
    let pk_attr = scalar_to_attribute_value(&pk_scalar, time_format)?;

    let sk_attr = sk_idx
        .map(|idx| {
            let sk_scalar =
                ScalarValue::try_from_array(struct_array.column(idx), 0).map_err(|e| {
                    DataFusionError::Plan(format!("Failed to extract sk from struct: {e}"))
                })?;
            scalar_to_attribute_value(&sk_scalar, time_format)
        })
        .transpose()?;

    keys.push((pk_attr, sk_attr));
    Ok(())
}

/// Extract pk/sk values from a `struct(Literal, Literal)` `ScalarFunction` expression.
/// This handles the case where `DataFusion` represents tuple values as struct constructor
/// calls with literal arguments (e.g., `struct(Utf8("a"), Utf8("1"))`).
fn extract_key_pair_from_struct_func(
    func: &datafusion::logical_expr::expr::ScalarFunction,
    pk_idx: usize,
    sk_idx: Option<usize>,
    time_format: &str,
) -> Result<KeyToDelete, DataFusionError> {
    let pk_arg = func.args.get(pk_idx).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "DynamoDB DELETE: struct has no argument at index {pk_idx}"
        ))
    })?;
    let Expr::Literal(pk_scalar, _) = pk_arg else {
        return Err(DataFusionError::Plan(format!(
            "DynamoDB DELETE: struct argument at index {pk_idx} is not a literal: {pk_arg}"
        )));
    };
    let pk_attr = scalar_to_attribute_value(pk_scalar, time_format)?;

    let sk_attr = sk_idx
        .map(|idx| {
            let sk_arg = func.args.get(idx).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "DynamoDB DELETE: struct has no argument at index {idx}"
                ))
            })?;
            let Expr::Literal(sk_scalar, _) = sk_arg else {
                return Err(DataFusionError::Plan(format!(
                    "DynamoDB DELETE: struct argument at index {idx} is not a literal: {sk_arg}"
                )));
            };
            scalar_to_attribute_value(sk_scalar, time_format)
        })
        .transpose()?;

    Ok((pk_attr, sk_attr))
}
