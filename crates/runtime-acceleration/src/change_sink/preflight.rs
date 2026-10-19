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
//!
//! A scoped append or set replacement declares the set of rows it writes.
//! Before `ProviderChangeSinkBackend::apply_rows` mutates, these checks refuse
//! (with `before_mutation`) incoming rows whose unique keys collide with rows
//! outside the declared scope, or with rows of another scope in the same
//! coalesced append. They also refuse keys they cannot compare safely, such
//! as NULL-bearing unique keys.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use data_components::pk_filter_expr::balanced_binary;
use datafusion::common::{Column, Constraint, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::logical_expr::{Expr, col, lit};
use futures::StreamExt;

use super::refusal::{before_mutation, member_refusal};
use crate::change_sink::batch::{ScopeIdentity, ScopedInput, row_scalar, supports_key_equality};
use crate::change_sink::{ChangeSinkContext, ScopeKind, SetKey};

struct Scoped {
    scope: SetKey,
    batches: Range<usize>,
    grouping_columns: HashSet<usize>,
    identity: usize,
    replaces: bool,
}

type InputKeyScopes = HashMap<Vec<ScalarValue>, usize>;

/// Incoming unique keys of one probe, each with the inputs that carry it.
type ProbeKeys = HashMap<Vec<ScalarValue>, Vec<usize>>;

/// Validate every scoped input before provider planning: chunk coverage and
/// scope membership, unique keys shared between inputs, and unique keys already
/// stored outside each input's scope. Ranges describe whole transport chunks,
/// never rows or a merged logical set. A key conflict refuses the write with
/// the position of the first input that would be refused had the inputs been
/// applied one at a time in order.
///
/// Returns the replaced scopes, resolved against the target schema, in input order.
pub async fn validate_scoped_inputs(
    context: &ChangeSinkContext,
    batches: &[RecordBatch],
    inputs: &[ScopedInput],
    cap: usize,
    ctx: &SessionContext,
) -> Result<Vec<SetKey>> {
    if inputs.is_empty() {
        return Ok(Vec::new());
    }
    let schema = context.table.schema();
    let mut scopes: Vec<Scoped> = Vec::with_capacity(inputs.len());
    let mut hashed: HashMap<ScopeIdentity, usize> = HashMap::new();
    let mut cursor = 0;
    for input in inputs {
        let range = input.batch_range();
        if range.start != cursor || range.end < range.start || range.end > batches.len() {
            return Err(before_mutation(DataFusionError::Plan(
                "Scoped write ranges must cover each input chunk exactly once, in order".into(),
            )));
        }
        let scope = SetKey::from_filters(Arc::clone(&schema), &input.scope().filters())
            .map_err(before_mutation)?;
        for batch in &batches[range.clone()] {
            scope.validate_batch(batch).map_err(before_mutation)?;
        }
        // Equal scopes share an identity. Hashable scopes are matched in
        // constant time so a full burst does not compare every pair.
        let identity = if let Some(key) = scope.hash_identity() {
            *hashed.entry(key).or_insert(scopes.len())
        } else {
            let filters = scope.filters();
            scopes
                .iter()
                .find(|previous| previous.scope.filters() == filters)
                .map_or(scopes.len(), |previous| previous.identity)
        };
        cursor = range.end;
        scopes.push(Scoped {
            grouping_columns: scope.column_indices().collect(),
            scope,
            batches: range,
            identity,
            replaces: input.kind() == ScopeKind::Replace,
        });
    }
    if cursor != batches.len() {
        return Err(before_mutation(DataFusionError::Plan(
            "Scoped write ranges must cover every input chunk".into(),
        )));
    }
    let incoming = validate_incoming_scopes(context, batches, &scopes, ctx).await?;
    let stored = validate_scope_constraints(context, batches, &scopes, cap, ctx).await?;
    if let Some((position, error)) = [incoming, stored]
        .into_iter()
        .flatten()
        .min_by_key(|(position, _)| *position)
    {
        return Err(member_refusal(position, error));
    }
    Ok(scopes
        .into_iter()
        .filter(|scoped| scoped.replaces)
        .map(|scoped| scoped.scope)
        .collect())
}

/// Validate every member before a coalesced write reaches provider planning.
/// Returns the first member whose unique key an earlier member with another
/// scope also writes.
async fn validate_incoming_scopes(
    context: &ChangeSinkContext,
    batches: &[RecordBatch],
    scopes: &[Scoped],
    ctx: &SessionContext,
) -> Result<Option<(usize, DataFusionError)>> {
    if scopes.iter().all(|scope| scope.identity == 0) {
        return Ok(None);
    }
    let Some(constraints) = context.table.constraints() else {
        return Ok(None);
    };
    let mut first: Option<(usize, DataFusionError)> = None;
    let schema = context.table.schema();
    let all_grouping_columns = scopes
        .iter()
        .flat_map(|scope| scope.grouping_columns.iter().copied())
        .collect::<HashSet<_>>();
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
            .all(|scope| scope.grouping_columns.len() == all_grouping_columns.len())
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
        'members: for (position, scoped) in scopes.iter().enumerate() {
            if first
                .as_ref()
                .is_some_and(|(earliest, _)| *earliest <= position)
            {
                break;
            }
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
                            first = Some((
                                position,
                                DataFusionError::Plan(format!(
                                    "Unique key conflicts between scoped appends for dataset '{}'",
                                    context.dataset_name,
                                )),
                            ));
                            break 'members;
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
    Ok(first)
}

/// Reject incoming unique keys already stored outside the input's scope. A row
/// in a scope an earlier input of this write replaces is gone by this input's
/// turn, so it is not a conflict; a row in a later input's scope still is. One
/// probe per `cap` distinct keys serves every input.
/// The caller holds the table owner's write lock through both probe and write.
/// Providers without a known SQL equality contract for their unique keys must
/// not opt into this replacement path.
async fn validate_scope_constraints(
    context: &ChangeSinkContext,
    batches: &[RecordBatch],
    scopes: &[Scoped],
    cap: usize,
    ctx: &SessionContext,
) -> Result<Option<(usize, DataFusionError)>> {
    let Some(constraints) = context.table.constraints() else {
        return Ok(None);
    };
    let schema = context.table.schema();
    let mut first: Option<(usize, DataFusionError)> = None;
    let replaced = ReplacedScopes::new(
        scopes
            .iter()
            .enumerate()
            .filter(|(_, scoped)| scoped.replaces)
            .map(|(position, scoped)| (position, &scoped.scope)),
    );
    let task = ctx.task_ctx();
    let reservation =
        MemoryConsumer::new("ChangeSink scoped write probe keys").register(task.memory_pool());
    for constraint in constraints.iter() {
        let (Constraint::PrimaryKey(columns) | Constraint::Unique(columns)) = constraint;
        if columns.is_empty() || columns.iter().any(|&index| index >= schema.fields().len()) {
            return Err(before_mutation(DataFusionError::Plan(
                "Unique key columns do not match the scoped write schema".into(),
            )));
        }
        let primary = matches!(constraint, Constraint::PrimaryKey(_));
        let mut probed = Vec::new();
        for (position, scoped) in scopes.iter().enumerate() {
            let input = &batches[scoped.batches.clone()];
            if primary
                && input.iter().any(|batch| {
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
            // Equal keys necessarily identify this same scope, even for empty input.
            if scoped
                .grouping_columns
                .iter()
                .all(|column| columns.contains(column))
                || !input.iter().any(|batch| batch.num_rows() > 0)
            {
                continue;
            }
            probed.push(position);
        }
        if probed.is_empty() {
            continue;
        }
        let names = columns
            .iter()
            .map(|&index| {
                let field = schema.fields().get(index).ok_or_else(|| {
                    before_mutation(DataFusionError::Plan(
                        "Unique key column is outside the schema".into(),
                    ))
                })?;
                if !supports_key_equality(field.data_type()) {
                    return Err(before_mutation(DataFusionError::NotImplemented(format!(
                        "Cannot validate cross-scope unique key conflicts for dataset '{}': key column '{}' has unsupported type {}",
                        context.dataset_name,
                        field.name(),
                        field.data_type(),
                    ))));
                }
                Ok(field.name().clone())
            })
            .collect::<Result<Vec<_>>>()?;

        // Bound predicate size and retained scalar keys. The original batches
        // stay intact so the provider applies its configured conflict policy.
        let mut keys = ProbeKeys::new();
        for &position in &probed {
            for batch in &batches[scopes[position].batches.clone()] {
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
                    match keys.entry(key) {
                        Entry::Occupied(mut entry) => {
                            if entry.get().last() != Some(&position) {
                                reservation
                                    .try_grow(size_of::<usize>())
                                    .map_err(before_mutation)?;
                                entry.get_mut().push(position);
                            }
                        }
                        Entry::Vacant(entry) => {
                            let bytes = entry
                                .key()
                                .iter()
                                .map(ScalarValue::size)
                                .fold(0_usize, usize::saturating_add)
                                .saturating_add(4 * size_of::<(Vec<ScalarValue>, Vec<usize>)>());
                            reservation.try_grow(bytes).map_err(before_mutation)?;
                            entry.insert(vec![position]);
                        }
                    }
                    if keys.len() >= cap.max(1) {
                        probe_keys(context, &names, &keys, scopes, &replaced, &mut first, ctx)
                            .await?;
                        keys.clear();
                        reservation.free();
                    }
                    if row % 128 == 127 {
                        tokio::task::yield_now().await;
                    }
                }
            }
        }
        if !keys.is_empty() {
            probe_keys(context, &names, &keys, scopes, &replaced, &mut first, ctx).await?;
            reservation.free();
        }
    }
    Ok(first)
}

/// Read the stored rows that can carry an incoming key and check each against
/// the scopes of the inputs carrying that key, keeping the earliest input with
/// a conflict in `first`. One `IN` list per key column selects a superset of the
/// incoming keys that a key index can serve; exact keys are matched here.
/// Matching rows are streamed, never collected.
async fn probe_keys(
    context: &ChangeSinkContext,
    names: &[String],
    keys: &ProbeKeys,
    scopes: &[Scoped],
    replaced: &ReplacedScopes<'_>,
    first: &mut Option<(usize, DataFusionError)>,
    ctx: &SessionContext,
) -> Result<()> {
    let schema = context.table.schema();
    let per_column = names
        .iter()
        .enumerate()
        .map(|(position, name)| {
            let values = keys
                .keys()
                .map(|key| &key[position])
                .collect::<HashSet<_>>()
                .into_iter()
                .map(|value| lit(value.clone()))
                .collect();
            col(Column::from_name(name)).in_list(values, false)
        })
        .collect();
    let Some(predicate) = balanced_binary(per_column, Expr::and) else {
        return Err(before_mutation(DataFusionError::Plan(
            "Unique key has no columns".into(),
        )));
    };
    let mut projection = names.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let referenced = keys.values().flatten().collect::<HashSet<_>>();
    for &position in referenced {
        projection.extend(
            scopes[position]
                .grouping_columns
                .iter()
                .map(|&index| schema.field(index).name().as_str()),
        );
    }
    projection.extend(
        replaced
            .column_indices()
            .map(|index| schema.field(index).name().as_str()),
    );
    let mut stream = ctx
        .read_table(Arc::clone(&context.table))?
        .filter(predicate)?
        .select(
            projection
                .into_iter()
                .map(|name| col(Column::from_name(name))),
        )?
        .execute_stream()
        .await?;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for row in 0..batch.num_rows() {
            let key = names
                .iter()
                .map(|name| row_scalar(&schema, &batch, name, row))
                .collect::<Result<Vec<_>>>()?;
            let Some(positions) = keys.get(&key) else {
                continue;
            };
            for &position in positions {
                if first
                    .as_ref()
                    .is_none_or(|(earliest, _)| position < *earliest)
                    && !scopes[position].scope.contains_row(&batch, row)?
                    && replaced
                        .first_containing(&batch, row)?
                        .is_none_or(|replacer| replacer > position)
                {
                    *first = Some((
                        position,
                        DataFusionError::Execution(format!(
                            "Cannot write scoped rows in dataset '{}': incoming unique key ({}) conflicts with a row outside the declared scope",
                            context.dataset_name,
                            names.join(", "),
                        )),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Scopes one write deletes before it appends, by input position and indexed
/// for row membership.
struct ReplacedScopes<'a> {
    hashed: HashMap<Vec<usize>, HashMap<Vec<ScalarValue>, usize>>,
    other: Vec<(usize, &'a SetKey)>,
    schema: Option<arrow::datatypes::SchemaRef>,
}

impl<'a> ReplacedScopes<'a> {
    /// `scopes` arrive in ascending input position.
    fn new(scopes: impl Iterator<Item = (usize, &'a SetKey)>) -> Self {
        let mut hashed: HashMap<Vec<usize>, HashMap<Vec<ScalarValue>, usize>> = HashMap::new();
        let mut other = Vec::new();
        let mut schema = None;
        for (position, scope) in scopes {
            schema.get_or_insert_with(|| Arc::clone(scope.schema()));
            match scope.hash_identity() {
                Some((columns, values)) => {
                    hashed
                        .entry(columns)
                        .or_default()
                        .entry(values)
                        .or_insert(position);
                }
                None => other.push((position, scope)),
            }
        }
        Self {
            hashed,
            other,
            schema,
        }
    }

    fn column_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.hashed.keys().flatten().copied().chain(
            self.other
                .iter()
                .flat_map(|(_, scope)| scope.column_indices()),
        )
    }

    /// The earliest input position whose replaced scope holds the row.
    fn first_containing(&self, batch: &RecordBatch, row: usize) -> Result<Option<usize>> {
        let mut first: Option<usize> = None;
        if let Some(schema) = &self.schema {
            for (columns, values) in &self.hashed {
                let candidate = columns
                    .iter()
                    .map(|&index| row_scalar(schema, batch, schema.field(index).name(), row))
                    .collect::<Result<Vec<_>>>()?;
                if let Some(&position) = values.get(&candidate) {
                    first = Some(first.map_or(position, |earlier| earlier.min(position)));
                }
            }
        }
        for &(position, scope) in &self.other {
            if first.is_none_or(|earlier| position < earlier) && scope.contains_row(batch, row)? {
                first = Some(position);
            }
        }
        Ok(first)
    }
}
