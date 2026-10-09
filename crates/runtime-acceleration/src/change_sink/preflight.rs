/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Scoped write conflict probes. Incoming rows are never deduplicated or rewritten.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use data_components::pk_filter_expr::balanced_binary;
use datafusion::common::{Column, Constraint, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::{Expr, col, lit};

use super::refusal::before_mutation;
use crate::change_sink::batch::AppendValidation;
use crate::change_sink::{ChangeSinkContext, SetKey};

struct ScopedAppend {
    scope: SetKey,
    batches: Range<usize>,
    filters: Vec<Expr>,
    identity: usize,
}

type InputKeyScopes = HashMap<Vec<ScalarValue>, usize>;

/// Validate every member before a coalesced append reaches provider planning.
/// Ranges describe whole transport chunks, never rows or a merged logical set.
pub async fn validate_append_scopes(
    context: &ChangeSinkContext,
    batches: &[RecordBatch],
    validations: &[AppendValidation],
    cap: usize,
    ctx: &SessionContext,
) -> Result<()> {
    if validations.is_empty() {
        return Ok(());
    }
    let schema = context.table.schema();
    let mut scopes: Vec<ScopedAppend> = Vec::with_capacity(validations.len());
    let mut cursor = 0;
    for validation in validations {
        let range = validation.batch_range();
        if range.start != cursor || range.end < range.start || range.end > batches.len() {
            return Err(before_mutation(DataFusionError::Plan(
                "Scoped append ranges must cover each input chunk exactly once, in order".into(),
            )));
        }
        let scope = SetKey::from_filters(Arc::clone(&schema), &validation.scope().filters())
            .map_err(before_mutation)?;
        for batch in &batches[range.clone()] {
            scope.validate_batch(batch).map_err(before_mutation)?;
        }
        let filters = scope.filters();
        let identity = scopes
            .iter()
            .find(|previous| previous.filters == filters)
            .map_or(scopes.len(), |previous| previous.identity);
        cursor = range.end;
        scopes.push(ScopedAppend {
            scope,
            batches: range,
            filters,
            identity,
        });
    }
    if cursor != batches.len() {
        return Err(before_mutation(DataFusionError::Plan(
            "Scoped append ranges must cover every input chunk".into(),
        )));
    }
    validate_incoming_scopes(context, batches, &scopes, ctx).await?;
    for scoped in scopes {
        validate_scope_constraints(context, &scoped.scope, &batches[scoped.batches], cap, ctx)
            .await?;
    }
    Ok(())
}

async fn validate_incoming_scopes(
    context: &ChangeSinkContext,
    batches: &[RecordBatch],
    scopes: &[ScopedAppend],
    ctx: &SessionContext,
) -> Result<()> {
    if scopes.iter().all(|scope| scope.identity == 0) {
        return Ok(());
    }
    let Some(constraints) = context.table.constraints() else {
        return Ok(());
    };
    let schema = context.table.schema();
    let all_grouping_columns = scopes
        .iter()
        .flat_map(|scope| &scope.filters)
        .flat_map(Expr::column_refs)
        .map(|column| schema.index_of(&column.name))
        .collect::<std::result::Result<HashSet<_>, _>>()
        .map_err(|error| before_mutation(error.into()))?;
    for constraint in constraints.iter() {
        let (Constraint::PrimaryKey(columns) | Constraint::Unique(columns)) = constraint;
        if columns.is_empty() || columns.iter().any(|&index| index >= schema.fields().len()) {
            return Err(before_mutation(DataFusionError::Plan(
                "Unique key columns do not match the scoped append schema".into(),
            )));
        }
        // Equal grouping shapes distinguish scopes by key values. A union of
        // different shapes does not prove that the scopes are disjoint.
        if scopes
            .iter()
            .all(|scope| scope.filters.len() == all_grouping_columns.len())
            && all_grouping_columns
                .iter()
                .all(|column| columns.contains(column))
        {
            continue;
        }
        if columns
            .iter()
            .any(|&index| !supports_key_equality(schema.field(index).data_type()))
        {
            return Err(before_mutation(DataFusionError::NotImplemented(format!(
                "Cannot validate unique key equality between scoped appends for dataset '{}': the key type is unsupported",
                context.dataset_name,
            ))));
        }
        let task = ctx.task_ctx();
        let reservation =
            MemoryConsumer::new("ChangeSink scoped append keys").register(task.memory_pool());
        let mut keys = InputKeyScopes::new();
        for scoped in scopes {
            for batch_index in scoped.batches.clone() {
                let batch = &batches[batch_index];
                for row in 0..batch.num_rows() {
                    let key = columns
                        .iter()
                        .map(|&column| {
                            ScalarValue::try_from_array(batch.column(column), row)
                                .map_err(before_mutation)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if key.iter().any(ScalarValue::is_null) {
                        return Err(before_mutation(DataFusionError::NotImplemented(format!(
                            "Cannot validate cross-scope unique key conflicts for dataset '{}': NULL-bearing unique keys require an explicit NULL uniqueness contract",
                            context.dataset_name,
                        ))));
                    }
                    if let Some(&identity) = keys.get(&key) {
                        if identity != scoped.identity {
                            return Err(before_mutation(DataFusionError::Plan(format!(
                                "Unique key conflicts between scoped appends for dataset '{}'",
                                context.dataset_name,
                            ))));
                        }
                    } else {
                        // Account for retained scalar values and hash-table spare capacity.
                        let bytes = key
                            .iter()
                            .map(ScalarValue::size)
                            .fold(0_usize, usize::saturating_add)
                            .saturating_add(4 * size_of::<(Vec<ScalarValue>, usize)>());
                        reservation.try_grow(bytes).map_err(before_mutation)?;
                        keys.insert(key, scoped.identity);
                    }
                    if row % 128 == 127 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Check only incoming keys that can collide with rows outside the scope.
/// The caller holds the table owner's write lock through both probe and write.
/// Providers without a known SQL equality contract for their unique keys must
/// not opt into this replacement path.
pub async fn validate_scope_constraints(
    context: &ChangeSinkContext,
    scope: &SetKey,
    batches: &[RecordBatch],
    cap: usize,
    ctx: &SessionContext,
) -> Result<()> {
    let Some(constraints) = context.table.constraints() else {
        return Ok(());
    };
    let schema = context.table.schema();
    let filters = scope.filters();
    let grouping_columns = filters
        .iter()
        .flat_map(Expr::column_refs)
        .map(|column| schema.index_of(&column.name))
        .collect::<std::result::Result<HashSet<_>, _>>()
        .map_err(|error| before_mutation(error.into()))?;
    let predicate = balanced_binary(filters, Expr::and).ok_or_else(|| {
        before_mutation(DataFusionError::Plan(
            "Write validation scope is empty".into(),
        ))
    })?;
    // NOT alone excludes NULL when a non-null grouping value is requested.
    // IS NOT TRUE includes both false and unknown, preserving NULL scope identity.
    let outside = Expr::IsNotTrue(Box::new(predicate));
    for constraint in constraints.iter() {
        let (Constraint::PrimaryKey(columns) | Constraint::Unique(columns)) = constraint;
        if columns.is_empty() || columns.iter().any(|&index| index >= schema.fields().len()) {
            return Err(before_mutation(DataFusionError::Plan(
                "Unique key columns do not match the scoped write schema".into(),
            )));
        }
        let primary = matches!(constraint, Constraint::PrimaryKey(_));
        if primary
            && batches.iter().any(|batch| {
                columns
                    .iter()
                    .any(|&index| batch.column(index).null_count() > 0)
            })
        {
            return Err(before_mutation(DataFusionError::Plan(format!(
                "Cannot write scoped rows in dataset '{}': primary key values must not be NULL",
                context.dataset_name,
            ))));
        }
        if grouping_columns
            .iter()
            .all(|column| columns.contains(column))
        {
            // Equal keys necessarily identify this same scope, even for empty input.
            continue;
        }
        if !batches.iter().any(|batch| batch.num_rows() > 0) {
            continue;
        }
        let names = columns.iter().map(|&index| {
            let field = schema.fields().get(index).ok_or_else(|| {
                before_mutation(DataFusionError::Plan("Unique key column is outside the schema".into()))
            })?;
            if !supports_key_equality(field.data_type()) {
                return Err(before_mutation(DataFusionError::NotImplemented(format!(
                    "Cannot validate cross-scope unique key conflicts for dataset '{}': key column '{}' has unsupported type {}",
                    context.dataset_name, field.name(), field.data_type(),
                ))));
            }
            Ok(field.name().clone())
        }).collect::<Result<Vec<_>>>()?;

        // Bound predicate size and retained scalar keys. Repeated keys across
        // probe chunks may be checked again; the original batches stay intact so
        // the provider applies its configured on_conflict policy unchanged.
        let mut keys = HashSet::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let key = columns
                    .iter()
                    .map(|&index| {
                        ScalarValue::try_from_array(batch.column(index), row)
                            .map_err(before_mutation)
                    })
                    .collect::<Result<Vec<_>>>()?;
                if key.iter().any(ScalarValue::is_null) {
                    // TableProvider does not expose NULLS DISTINCT/NOT DISTINCT.
                    // Do not guess which equality a nullable unique index uses.
                    return Err(before_mutation(DataFusionError::NotImplemented(format!(
                        "Cannot validate cross-scope unique key conflicts for dataset '{}': NULL-bearing unique keys require an explicit NULL uniqueness contract",
                        context.dataset_name,
                    ))));
                }
                keys.insert(key);
                if keys.len() >= cap.max(1) {
                    probe_keys(context, &names, &keys, &outside, ctx).await?;
                    keys.clear();
                }
            }
        }
        if !keys.is_empty() {
            probe_keys(context, &names, &keys, &outside, ctx).await?;
        }
    }
    Ok(())
}

async fn probe_keys(
    context: &ChangeSinkContext,
    names: &[String],
    keys: &HashSet<Vec<ScalarValue>>,
    outside: &Expr,
    ctx: &SessionContext,
) -> Result<()> {
    let rows = keys
        .iter()
        .filter_map(|key| {
            let equalities = names
                .iter()
                .zip(key)
                .map(|(name, value)| col(Column::from_name(name)).eq(lit(value.clone())))
                .collect();
            balanced_binary(equalities, Expr::and)
        })
        .collect();
    let Some(keys) = balanced_binary(rows, Expr::or) else {
        return Err(before_mutation(DataFusionError::Plan(
            "Unique key has no columns".into(),
        )));
    };
    // Filter and LIMIT are in the logical plan even when the provider cannot
    // push them down. Never materialize the old scope or collect matching rows.
    let found = ctx
        .read_table(Arc::clone(&context.table))?
        .filter(outside.clone().and(keys))?
        .select(vec![lit(1_i32)])?
        .limit(0, Some(1))?
        .collect()
        .await?;
    if found.iter().any(|batch| batch.num_rows() > 0) {
        return Err(before_mutation(DataFusionError::Execution(format!(
            "Cannot write scoped rows in dataset '{}': incoming unique key ({}) conflicts with a row outside the declared scope",
            context.dataset_name,
            names.join(", "),
        ))));
    }
    Ok(())
}

fn supports_key_equality(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}
