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

//! Logical changes and validated, nonunique replacement scopes.
//!
//! Two types describe a write. A producer submits a `ChangeBatch`: one write
//! of one kind, optionally on a coalescing lane. A backend applies a
//! `ChangeWrite`: the same data without the lane, after the owner merged the
//! compatible row writes of one lane into one `RowsWrite`.

use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray},
    compute::{cast, kernels::cmp::not_distinct},
    datatypes::{DataType, Schema, SchemaRef},
};
use arrow_tools::record_batch::try_cast_to;
use data_components::cdc;
use data_components::pk_filter_expr::balanced_binary;
use std::ops::{ControlFlow, Range};
use std::sync::Arc;

use super::batching::{AppendIngress, CdcIngress};
use datafusion::{
    common::{Column, ScalarValue},
    error::{DataFusionError, Result},
    logical_expr::{Expr, Operator, col, lit},
};

/// One write that a producer submits. Source acknowledgement stays with the
/// producer. A row write with a lane can share one backend write with other
/// writes of the same lane; see `AppendIngress`.
#[derive(Debug)]
pub enum ChangeBatch {
    /// CDC operations from a source, with operation codes, before-images, and
    /// source timestamps unchanged.
    Cdc(CdcRows),
    /// Flat rows that impose no uniqueness on any grouping columns.
    Append {
        rows: Rows,
        lane: Option<Arc<AppendIngress>>,
    },
    /// Rows of one scope that is known to be fresh. Nothing is deleted. The
    /// backend refuses a unique key that collides with stored rows outside the
    /// scope, or with another scope in the same write.
    AppendScoped {
        rows: ScopedRows,
        lane: Option<Arc<AppendIngress>>,
    },
    /// Delete the stored rows of one scope, then append these rows. This is
    /// ordered, not atomic: readers can observe the deletion gap, and an append
    /// failure after the delete can lose the previous rows. An empty replacement
    /// deletes its scope; it is not a heartbeat.
    ///
    /// The caller must establish source completeness, including pagination and
    /// limits; a finite vector does not prove completeness.
    Replace {
        rows: ScopedRows,
        lane: Option<Arc<AppendIngress>>,
    },
}

/// What a backend applies: one write with no lane. The owner builds it from one
/// `ChangeBatch`, or from several row writes of one lane.
#[derive(Debug)]
pub enum ChangeWrite {
    Cdc(CdcRows),
    Rows(RowsWrite),
}

/// Row chunks that all use one schema, including metadata. The chunks belong
/// to one logical operation, including when the vector is empty.
#[derive(Debug, Clone)]
pub struct Rows {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

/// Rows that all lie in one scope. The scope and the rows use one schema.
#[derive(Debug)]
pub struct ScopedRows {
    scope: SetKey,
    rows: Rows,
}

/// Row writes as a backend applies them. Plain appends and scoped writes never
/// share one write.
#[derive(Debug)]
pub enum RowsWrite {
    /// One or more plain appends.
    Unscoped(Rows),
    /// One or more scoped appends and replacements.
    Scoped(ScopedWrite),
}

/// One or more scoped inputs applied as one write: the backend deletes every
/// replaced scope once, then appends all chunks. The inputs cover the chunks
/// in order, one range each, and there is at least one input. An input with no
/// rows keeps an empty range.
#[derive(Debug, Clone)]
pub struct ScopedWrite {
    rows: Rows,
    inputs: Vec<ScopedInput>,
}

/// Whether a scoped input deletes its scope before its rows are appended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeKind {
    /// Only append; the scope is known to hold no stored rows.
    Fresh,
    /// Delete the scope's stored rows first.
    Replace,
}

/// One input of a `ScopedWrite`: its scope, its exact range of unchanged Arrow
/// chunks, and its kind.
#[derive(Debug, Clone)]
pub struct ScopedInput {
    scope: SetKey,
    chunks: Range<usize>,
    kind: ScopeKind,
}

impl ScopedInput {
    #[must_use]
    pub fn scope(&self) -> &SetKey {
        &self.scope
    }

    #[must_use]
    pub fn batch_range(&self) -> Range<usize> {
        self.chunks.clone()
    }

    #[must_use]
    pub fn kind(&self) -> ScopeKind {
        self.kind
    }
}

/// Ready or deferred CDC rows with optional burst policy. No source committer
/// or source control flag is retained here.
#[derive(Debug)]
pub struct CdcRows {
    rows: cdc::LazyChangeBatch,
    ingress: Option<Arc<CdcIngress>>,
}

impl CdcRows {
    #[must_use]
    pub fn as_built(&self) -> Option<&cdc::ChangeBatch> {
        self.rows.as_built()
    }

    #[must_use]
    pub fn is_materialized(&self) -> bool {
        self.rows.is_materialized()
    }

    #[must_use]
    pub fn ingress(&self) -> Option<&Arc<CdcIngress>> {
        self.ingress.as_ref()
    }

    #[must_use]
    pub fn into_lazy_parts(self) -> (cdc::LazyChangeBatch, Option<Arc<CdcIngress>>) {
        (self.rows, self.ingress)
    }

    /// Backends receive only materialized rows. Deferred decoding belongs to
    /// the owner and must complete for the entire burst before mutation.
    ///
    /// # Errors
    /// Returns an error if the rows are unmaterialized or cannot form a CDC batch.
    pub fn into_built(self) -> Result<cdc::ChangeBatch> {
        if !self.rows.is_materialized() {
            return Err(DataFusionError::Internal(
                "CDC rows reached a backend before burst decoding".into(),
            ));
        }
        self.rows
            .into_built()
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    /// Whether the operations permit the native upsert-only burst pipeline.
    /// Primary-key changes must include an explicit delete operation; before-images
    /// are not additional operations. Target and schema barriers remain separate.
    #[must_use]
    pub fn permits_pipelined_append(&self) -> bool {
        let Some(batch) = self.as_built() else {
            return false;
        };
        if batch.is_heartbeat() || batch.rebuild_from_this_batch() {
            return false;
        }
        let Some(operations) = batch
            .record
            .column_by_name("op")
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        else {
            return false;
        };
        operations
            .iter()
            .all(|operation| matches!(operation, Some("c" | "r" | "u")))
    }

    pub(crate) fn estimated_bytes(&self) -> usize {
        self.rows.encoded_len()
    }
}

impl Rows {
    /// Rows for one logical operation.
    ///
    /// # Errors
    /// Returns an error if any batch differs from the supplied schema, including metadata.
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in &batches {
            validate_schema(&schema, &batch.schema())?;
        }
        Ok(Self { schema, batches })
    }

    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.batches
            .iter()
            .map(RecordBatch::num_rows)
            .fold(0, usize::saturating_add)
    }

    #[must_use]
    pub fn into_parts(self) -> (SchemaRef, Vec<RecordBatch>) {
        (self.schema, self.batches)
    }

    /// Cast every chunk to `target`. The rows then use `target` as their schema.
    ///
    /// # Errors
    /// Returns an error if a chunk cannot be cast.
    pub fn cast_to(self, target: &SchemaRef) -> Result<Self> {
        let batches = self
            .batches
            .into_iter()
            .map(|batch| try_cast_to(batch, Arc::clone(target)).map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema: Arc::clone(target),
            batches,
        })
    }

    fn accepts(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self
                .batches
                .len()
                .checked_add(other.batches.len())
                .is_some()
    }

    /// Append `other`'s chunks and return their range in the merged vector.
    fn extend(&mut self, other: Self) -> Range<usize> {
        let start = self.batches.len();
        // Preserve the sum of the inputs' charged vector capacities rather
        // than growing geometrically while the owner retains their claims.
        self.batches.reserve_exact(other.batches.len());
        self.batches.extend(other.batches);
        start..self.batches.len()
    }

    fn estimated_bytes(&self) -> usize {
        let metadata = self
            .batches
            .capacity()
            .saturating_mul(std::mem::size_of::<RecordBatch>());
        self.batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(metadata, usize::saturating_add)
    }
}

impl ScopedRows {
    /// Rows for one scope.
    ///
    /// # Errors
    /// Returns an error if schemas differ or any row falls outside the scope.
    pub fn try_new(scope: SetKey, schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        validate_schema(scope.schema(), &schema)?;
        for batch in &batches {
            scope.validate_batch(batch)?;
        }
        Ok(Self {
            scope,
            rows: Rows { schema, batches },
        })
    }

    #[must_use]
    pub fn scope(&self) -> &SetKey {
        &self.scope
    }

    #[must_use]
    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    fn estimated_bytes(&self) -> usize {
        self.rows
            .estimated_bytes()
            .saturating_add(std::mem::size_of::<ScopedInput>())
            .saturating_add(self.scope.estimated_bytes())
    }
}

impl ScopedWrite {
    fn single(rows: ScopedRows, kind: ScopeKind) -> Self {
        let chunks = 0..rows.rows.batches.len();
        Self {
            rows: rows.rows,
            inputs: vec![ScopedInput {
                scope: rows.scope,
                chunks,
                kind,
            }],
        }
    }

    #[must_use]
    pub fn rows(&self) -> &Rows {
        &self.rows
    }

    /// The inputs in arrival order. Never empty.
    #[must_use]
    pub fn inputs(&self) -> &[ScopedInput] {
        &self.inputs
    }

    /// Scopes whose stored rows are deleted before the rows are appended.
    pub fn replaced_scopes(&self) -> impl Iterator<Item = &SetKey> {
        self.inputs
            .iter()
            .filter(|input| input.kind == ScopeKind::Replace)
            .map(|input| &input.scope)
    }

    #[must_use]
    pub fn into_parts(self) -> (Rows, Vec<ScopedInput>) {
        (self.rows, self.inputs)
    }

    /// Cast every chunk to `target`. Scopes keep their schema; a backend
    /// resolves them against its target.
    ///
    /// # Errors
    /// Returns an error if a chunk cannot be cast.
    pub fn cast_to(self, target: &SchemaRef) -> Result<Self> {
        Ok(Self {
            rows: self.rows.cast_to(target)?,
            inputs: self.inputs,
        })
    }

    /// Append `other`'s inputs after this write's inputs. Each range keeps its
    /// input identity; rows are neither concatenated nor deduplicated here. The
    /// caller checks `Rows::accepts` and decides whether the scopes may share
    /// one write.
    fn extend(&mut self, other: Self) {
        let offset = self.rows.extend(other.rows).start;
        self.inputs.reserve_exact(other.inputs.len());
        self.inputs
            .extend(other.inputs.into_iter().map(|mut input| {
                input.chunks.start += offset;
                input.chunks.end += offset;
                input
            }));
    }

    /// Each input as a write of its own, in order, sharing the Arrow buffers.
    pub(crate) fn members(&self) -> Vec<Self> {
        self.inputs
            .iter()
            .map(|input| Self {
                rows: Rows {
                    schema: Arc::clone(&self.rows.schema),
                    batches: self.rows.batches[input.chunks.clone()].to_vec(),
                },
                inputs: vec![ScopedInput {
                    scope: input.scope.clone(),
                    chunks: 0..input.chunks.len(),
                    kind: input.kind,
                }],
            })
            .collect()
    }

    /// One write holding every member in order, sharing the Arrow buffers.
    /// `None` when there are no members or their schemas differ.
    pub(crate) fn coalesce(members: &[Self]) -> Option<Self> {
        let (first, rest) = members.split_first()?;
        let mut merged = first.clone();
        for member in rest {
            if !merged.rows.accepts(&member.rows) {
                return None;
            }
            merged.extend(member.clone());
        }
        Some(merged)
    }
}

impl RowsWrite {
    #[must_use]
    pub fn rows(&self) -> &Rows {
        match self {
            Self::Unscoped(rows) => rows,
            Self::Scoped(write) => &write.rows,
        }
    }

    #[must_use]
    pub fn num_rows(&self) -> usize {
        self.rows().num_rows()
    }

    /// Each scoped input in order; empty for plain appends.
    #[must_use]
    pub fn scoped_inputs(&self) -> &[ScopedInput] {
        match self {
            Self::Unscoped(_) => &[],
            Self::Scoped(write) => &write.inputs,
        }
    }

    /// Scopes whose stored rows are deleted before the rows are appended.
    pub fn replaced_scopes(&self) -> impl Iterator<Item = &SetKey> {
        self.scoped_inputs()
            .iter()
            .filter(|input| input.kind == ScopeKind::Replace)
            .map(|input| &input.scope)
    }

    /// Cast every chunk to `target`.
    ///
    /// # Errors
    /// Returns an error if a chunk cannot be cast.
    pub fn cast_to(self, target: &SchemaRef) -> Result<Self> {
        match self {
            Self::Unscoped(rows) => rows.cast_to(target).map(Self::Unscoped),
            Self::Scoped(write) => write.cast_to(target).map(Self::Scoped),
        }
    }

    /// Whether `next` can join this write: the same row kind and schema. Lane,
    /// limits, and scope overlap are the burst's decision.
    pub(crate) fn accepts(&self, next: &ChangeBatch) -> bool {
        match (self, next) {
            (Self::Unscoped(rows), ChangeBatch::Append { rows: next, .. }) => rows.accepts(next),
            (
                Self::Scoped(write),
                ChangeBatch::AppendScoped { rows: next, .. }
                | ChangeBatch::Replace { rows: next, .. },
            ) => write.rows.accepts(&next.rows),
            _ => false,
        }
    }

    /// Extend this write with `next`. Returns `Break` with the unchanged input
    /// when it cannot join.
    pub(crate) fn merge(&mut self, next: ChangeBatch) -> ControlFlow<ChangeBatch> {
        if !self.accepts(&next) {
            return ControlFlow::Break(next);
        }
        match (self, next) {
            (Self::Unscoped(rows), ChangeBatch::Append { rows: next, .. }) => {
                rows.extend(next);
            }
            (Self::Scoped(write), ChangeBatch::AppendScoped { rows: next, .. }) => {
                write.extend(ScopedWrite::single(next, ScopeKind::Fresh));
            }
            (Self::Scoped(write), ChangeBatch::Replace { rows: next, .. }) => {
                write.extend(ScopedWrite::single(next, ScopeKind::Replace));
            }
            (_, next) => return ControlFlow::Break(next),
        }
        ControlFlow::Continue(())
    }
}

impl From<ScopedWrite> for RowsWrite {
    fn from(write: ScopedWrite) -> Self {
        Self::Scoped(write)
    }
}

impl From<RowsWrite> for ChangeWrite {
    fn from(write: RowsWrite) -> Self {
        Self::Rows(write)
    }
}

impl From<ChangeBatch> for ChangeWrite {
    fn from(batch: ChangeBatch) -> Self {
        batch.into_parts().0
    }
}

impl ChangeWrite {
    /// Whether this write may stage while the previous write still finalizes.
    /// Only decoded, upsert-only CDC qualifies.
    #[must_use]
    pub fn permits_pipelined_append(&self) -> bool {
        matches!(self, Self::Cdc(rows) if rows.permits_pipelined_append())
    }

    /// Scopes whose stored rows are deleted before the rows are appended.
    pub fn replaced_scopes(&self) -> impl Iterator<Item = &SetKey> {
        let inputs: &[ScopedInput] = match self {
            Self::Cdc(_) => &[],
            Self::Rows(write) => write.scoped_inputs(),
        };
        inputs
            .iter()
            .filter(|input| input.kind == ScopeKind::Replace)
            .map(|input| &input.scope)
    }
}

impl ChangeBatch {
    #[must_use]
    pub fn cdc(batch: cdc::ChangeBatch) -> Self {
        Self::cdc_with_ingress(cdc::LazyChangeBatch::ready(batch), None)
    }

    /// Transfer row ownership without building Arrow arrays on the source task.
    #[must_use]
    pub fn cdc_rows(rows: cdc::LazyChangeBatch, ingress: Arc<CdcIngress>) -> Self {
        Self::cdc_with_ingress(rows, Some(ingress))
    }

    #[must_use]
    pub fn cdc_with_ingress(rows: cdc::LazyChangeBatch, ingress: Option<Arc<CdcIngress>>) -> Self {
        Self::Cdc(CdcRows { rows, ingress })
    }

    /// Append flat rows without a lane. See `ChangeBatch::Append`.
    ///
    /// # Errors
    /// Returns an error if any batch differs from the supplied schema, including metadata.
    pub fn append(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        Ok(Self::Append {
            rows: Rows::try_new(schema, batches)?,
            lane: None,
        })
    }

    /// Append a fresh scope without a lane. See `ChangeBatch::AppendScoped`.
    ///
    /// # Errors
    /// Returns an error if schemas differ or any row falls outside the scope.
    pub fn append_scoped(
        scope: SetKey,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        Ok(Self::AppendScoped {
            rows: ScopedRows::try_new(scope, schema, batches)?,
            lane: None,
        })
    }

    /// Replace a scope without a lane. See `ChangeBatch::Replace`.
    ///
    /// # Errors
    /// Returns an error if schemas differ or any row falls outside the scope.
    pub fn replace_set(
        scope: SetKey,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        Ok(Self::Replace {
            rows: ScopedRows::try_new(scope, schema, batches)?,
            lane: None,
        })
    }

    /// The coalescing lane of a row write. CDC input coalesces by its `CdcIngress`.
    #[must_use]
    pub fn append_ingress(&self) -> Option<&Arc<AppendIngress>> {
        match self {
            Self::Cdc(_) => None,
            Self::Append { lane, .. }
            | Self::AppendScoped { lane, .. }
            | Self::Replace { lane, .. } => lane.as_ref(),
        }
    }

    /// The scope and kind of a scoped write; `None` for CDC and plain appends.
    #[must_use]
    pub fn scoped_input(&self) -> Option<(&SetKey, ScopeKind)> {
        match self {
            Self::Cdc(_) | Self::Append { .. } => None,
            Self::AppendScoped { rows, .. } => Some((&rows.scope, ScopeKind::Fresh)),
            Self::Replace { rows, .. } => Some((&rows.scope, ScopeKind::Replace)),
        }
    }

    #[must_use]
    pub fn cdc_batch(&self) -> Option<&cdc::ChangeBatch> {
        match self {
            Self::Cdc(rows) => rows.as_built(),
            _ => None,
        }
    }

    /// Separate the write a backend applies from the coalescing lane.
    #[must_use]
    pub fn into_parts(self) -> (ChangeWrite, Option<Arc<AppendIngress>>) {
        match self {
            Self::Cdc(rows) => (ChangeWrite::Cdc(rows), None),
            Self::Append { rows, lane } => (ChangeWrite::Rows(RowsWrite::Unscoped(rows)), lane),
            Self::AppendScoped { rows, lane } => (
                ChangeWrite::Rows(ScopedWrite::single(rows, ScopeKind::Fresh).into()),
                lane,
            ),
            Self::Replace { rows, lane } => (
                ChangeWrite::Rows(ScopedWrite::single(rows, ScopeKind::Replace).into()),
                lane,
            ),
        }
    }

    /// Separate a row write from its lane. Returns `Break` with the unchanged
    /// input when it is CDC or has no lane.
    pub(crate) fn into_laned_rows(self) -> ControlFlow<Self, (RowsWrite, Arc<AppendIngress>)> {
        match self {
            Self::Append {
                rows,
                lane: Some(lane),
            } => ControlFlow::Continue((RowsWrite::Unscoped(rows), lane)),
            Self::AppendScoped {
                rows,
                lane: Some(lane),
            } => ControlFlow::Continue((ScopedWrite::single(rows, ScopeKind::Fresh).into(), lane)),
            Self::Replace {
                rows,
                lane: Some(lane),
            } => {
                ControlFlow::Continue((ScopedWrite::single(rows, ScopeKind::Replace).into(), lane))
            }
            other => ControlFlow::Break(other),
        }
    }

    /// Estimate retained Arrow buffers and scope values, not a bound on process memory.
    /// Shared buffers can be counted more than once.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        match self {
            Self::Cdc(rows) => rows.estimated_bytes(),
            Self::Append { rows, .. } => rows.estimated_bytes(),
            Self::AppendScoped { rows, .. } | Self::Replace { rows, .. } => rows.estimated_bytes(),
        }
    }

    #[must_use]
    pub fn num_rows(&self) -> usize {
        match self {
            Self::Cdc(rows) => rows.rows.num_rows_hint(),
            Self::Append { rows, .. } => rows.num_rows(),
            Self::AppendScoped { rows, .. } | Self::Replace { rows, .. } => rows.rows.num_rows(),
        }
    }

    /// Only native CDC control input can be a heartbeat, not an empty row payload.
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        matches!(self, Self::Cdc(rows) if rows.rows.is_heartbeat())
    }
}

/// A typed conjunction identifying a nonunique group of rows in one schema.
/// NULL matches NULL and differs from an empty string. A namespace, when needed,
/// is another grouping column, not a row primary key.
#[derive(Debug, Clone)]
pub struct SetKey {
    schema: SchemaRef,
    values: Vec<(usize, ScalarValue)>,
}

impl SetKey {
    /// Parse column equalities, `IS NULL`, and `IS NOT DISTINCT FROM` literals.
    ///
    /// Columns must be unqualified and unambiguous in the schema. Literals must
    /// have the column's type; string layout conversions are lossless and allowed.
    /// General predicates, NULL equality, conflicting values, and an unconstrained
    /// scope are rejected. Repeated equivalent constraints identify one component.
    ///
    /// # Errors
    /// Returns an error for an empty or unsupported scope, unresolved columns,
    /// incompatible literals, conflicting constraints, or unsupported comparisons.
    pub fn from_filters(schema: SchemaRef, filters: &[Expr]) -> Result<Self> {
        let mut values: Vec<(usize, ScalarValue)> = Vec::new();
        let mut pending: Vec<_> = filters.iter().collect();
        while let Some(expression) = pending.pop() {
            if let Expr::BinaryExpr(binary) = expression
                && binary.op == Operator::And
            {
                pending.push(&binary.right);
                pending.push(&binary.left);
                continue;
            }
            let (column, literal) = scope_component(expression)?;
            if column.relation.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "Replacement grouping column '{column}' must be unqualified"
                )));
            }
            let index = schema.index_of(&column.name)?;
            if schema
                .fields()
                .iter()
                .filter(|field| field.name() == &column.name)
                .count()
                != 1
            {
                return Err(DataFusionError::Plan(format!(
                    "Replacement grouping column '{}' is ambiguous",
                    column.name
                )));
            }
            let field = schema.field(index);
            let value = typed_value(literal, field.data_type())?;
            if value.is_null() && !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "Non-nullable replacement grouping column '{}' cannot identify a NULL group",
                    field.name()
                )));
            }

            // Check comparison support even when a replacement has no incoming rows.
            let scalar = value.to_scalar()?;
            not_distinct(&scalar, &scalar)?;
            if let Some((_, previous)) = values.iter().find(|(i, _)| *i == index) {
                if !not_distinct(&previous.to_scalar()?, &scalar)?.value(0) {
                    return Err(DataFusionError::Plan(format!(
                        "Conflicting replacement values for grouping column '{}'",
                        field.name()
                    )));
                }
            } else {
                values.push((index, value));
            }
        }
        if values.is_empty() {
            return Err(DataFusionError::Plan(
                "A replacement scope requires at least one grouping column".into(),
            ));
        }
        values.sort_unstable_by_key(|(index, _)| *index);
        Ok(Self { schema, values })
    }

    fn estimated_bytes(&self) -> usize {
        let spare = self
            .values
            .capacity()
            .saturating_sub(self.values.len())
            .saturating_mul(std::mem::size_of::<(usize, ScalarValue)>());
        self.values
            .iter()
            .map(|(_, value)| value.size().saturating_add(std::mem::size_of::<usize>()))
            .fold(spare, usize::saturating_add)
    }

    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Schema indices of the grouping columns, ascending.
    pub fn column_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.values.iter().map(|(index, _)| *index)
    }

    /// The scope's columns and values when `ScalarValue` equality is exactly
    /// `IS NOT DISTINCT FROM` for every value, so equal scopes hash equally.
    #[must_use]
    pub(crate) fn hash_identity(&self) -> Option<ScopeIdentity> {
        self.values
            .iter()
            .all(|(index, _)| supports_key_equality(self.schema.field(*index).data_type()))
            .then(|| self.values.iter().cloned().unzip())
    }

    /// Whether one row of `batch`, which carries this scope's columns by name,
    /// lies in the scope under `IS NOT DISTINCT FROM`.
    ///
    /// # Errors
    /// Returns an error if a grouping column is missing or cannot be compared.
    pub(crate) fn contains_row(&self, batch: &RecordBatch, row: usize) -> Result<bool> {
        for (index, value) in &self.values {
            let field = self.schema.field(*index);
            let column = row_column(batch, field.name(), field.data_type(), row)?;
            if !not_distinct(&column.as_ref(), &value.to_scalar()?)?.value(0) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Build the conjunction's predicates without interpreting dots in column names.
    #[must_use]
    pub fn filters(&self) -> Vec<Expr> {
        self.values
            .iter()
            .map(|(index, value)| {
                let column = col(Column::from_name(self.schema.field(*index).name()));
                if value.is_null() {
                    column.is_null()
                } else {
                    column.eq(lit(value.clone()))
                }
            })
            .collect()
    }

    /// Validate every row with Arrow's null-aware, vectorized equality kernel.
    /// Duplicate rows remain valid; this scope does not imply uniqueness.
    ///
    /// # Errors
    /// Returns an error if schemas differ, a comparison fails, or a row is outside the scope.
    pub fn validate_batch(&self, batch: &RecordBatch) -> Result<()> {
        validate_schema(&self.schema, &batch.schema())?;
        for (index, value) in &self.values {
            let column = batch.column(*index);
            let matching = not_distinct(&column.as_ref(), &value.to_scalar()?)?;
            if matching.true_count() != batch.num_rows() {
                return Err(DataFusionError::Plan(format!(
                    "Replacement rows do not match grouping column '{}'",
                    self.schema.field(*index).name()
                )));
            }
        }
        Ok(())
    }
}

/// The delete predicate for replacing `scopes`: one expression, a balanced
/// disjunction of every scope's conjunction, so a provider never has to combine
/// a filter list. `None` when nothing is replaced.
#[must_use]
pub fn replaced_scope_filters<'a>(
    scopes: impl IntoIterator<Item = &'a SetKey>,
) -> Option<Vec<Expr>> {
    let per_scope = scopes
        .into_iter()
        .filter_map(|scope| balanced_binary(scope.filters(), Expr::and))
        .collect();
    balanced_binary(per_scope, Expr::or).map(|predicate| vec![predicate])
}

/// A scope's grouping column indices and their values, ascending by index.
pub(crate) type ScopeIdentity = (Vec<usize>, Vec<ScalarValue>);

/// Key types whose SQL equality matches `ScalarValue` equality and hashing.
pub(crate) fn supports_key_equality(data_type: &DataType) -> bool {
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

/// One row of a named column, cast to the type the target schema declares.
fn row_column(
    batch: &RecordBatch,
    name: &str,
    data_type: &DataType,
    row: usize,
) -> Result<ArrayRef> {
    let column = batch.column_by_name(name).ok_or_else(|| {
        DataFusionError::Internal(format!("Stored rows lack grouping column '{name}'"))
    })?;
    let value = column.slice(row, 1);
    if value.data_type() == data_type {
        Ok(value)
    } else {
        Ok(cast(&value, data_type)?)
    }
}

/// One stored value, typed as `schema` declares the named column.
///
/// # Errors
/// Returns an error if the column is missing from either schema or cannot be cast.
pub(crate) fn row_scalar(
    schema: &Schema,
    batch: &RecordBatch,
    name: &str,
    row: usize,
) -> Result<ScalarValue> {
    let field = schema.field_with_name(name)?;
    ScalarValue::try_from_array(&row_column(batch, name, field.data_type(), row)?, 0)
}

fn validate_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    if expected != actual {
        return Err(DataFusionError::Plan(
            "Change rows and replacement scope must use the supplied schema, including metadata"
                .into(),
        ));
    }
    Ok(())
}

fn scope_component(expression: &Expr) -> Result<(&Column, Option<&ScalarValue>)> {
    match expression {
        Expr::BinaryExpr(binary)
            if matches!(binary.op, Operator::Eq | Operator::IsNotDistinctFrom) =>
        {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(column), Expr::Literal(value, _))
                | (Expr::Literal(value, _), Expr::Column(column))
                    if binary.op == Operator::IsNotDistinctFrom || !value.is_null() =>
                {
                    Ok((column, Some(value)))
                }
                _ => Err(DataFusionError::Plan(
                    "Replacement equality requires a column and a non-NULL literal; use IS NULL for a NULL group".into(),
                )),
            }
        }
        Expr::IsNull(inner) => match inner.as_ref() {
            Expr::Column(column) => Ok((column, None)),
            _ => Err(DataFusionError::Plan(
                "A NULL replacement scope requires a grouping column".into(),
            )),
        },
        _ => Err(DataFusionError::Plan(
            "A replacement scope requires a conjunction of column equalities or IS NULL predicates"
                .into(),
        )),
    }
}

fn typed_value(literal: Option<&ScalarValue>, data_type: &DataType) -> Result<ScalarValue> {
    let Some(value) = literal else {
        return ScalarValue::try_from(data_type);
    };
    if value.data_type() == *data_type {
        return Ok(value.clone());
    }
    if matches!(value, ScalarValue::Null) {
        return ScalarValue::try_from(data_type);
    }
    if matches!(
        value.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) && matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return value.cast_to(data_type);
    }
    Err(DataFusionError::Plan(format!(
        "Replacement grouping literal has type {}, expected {data_type}; provide a literal with the column's type",
        value.data_type()
    )))
}
