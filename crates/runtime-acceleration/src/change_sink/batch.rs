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

use arrow::{
    array::{Array, ArrayRef, RecordBatch, StringArray},
    compute::{cast, kernels::cmp::not_distinct},
    datatypes::{DataType, Schema, SchemaRef},
};
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

/// Input for one ordered submission. Source acknowledgement stays with the producer.
#[derive(Debug)]
pub struct ChangeBatch {
    payload: ChangePayload,
    append_ingress: Option<Arc<AppendIngress>>,
}

/// CDC operations or flat row chunks, without source committers.
#[derive(Debug)]
pub enum ChangePayload {
    /// Retains operation codes, before-images, and source timestamps unchanged.
    Cdc(CdcRows),
    /// All chunks belong to one logical operation, including when the vector is empty.
    Rows {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        /// Each input's scope, covering the chunks in order. Empty for an append
        /// that imposes no uniqueness on grouping columns.
        scoped_inputs: Vec<ScopedInput>,
    },
}

/// One input's scope and exact range of unchanged Arrow chunks. Scoped inputs
/// cover the chunk vector in order; an empty input retains an empty range.
/// A fresh input only appends. A replacing input deletes its scope first.
#[derive(Debug)]
pub struct ScopedInput {
    scope: SetKey,
    batches: Range<usize>,
    replaces: bool,
}

impl ScopedInput {
    #[must_use]
    pub fn scope(&self) -> &SetKey {
        &self.scope
    }

    #[must_use]
    pub fn batch_range(&self) -> Range<usize> {
        self.batches.clone()
    }

    /// Whether the scope's stored rows are deleted before this input's rows are appended.
    #[must_use]
    pub fn replaces(&self) -> bool {
        self.replaces
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
        Self {
            payload: ChangePayload::Cdc(CdcRows { rows, ingress }),
            append_ingress: None,
        }
    }

    /// Append flat rows without imposing uniqueness on any grouping columns.
    ///
    /// # Errors
    /// Returns an error if any batch differs from the supplied schema, including metadata.
    pub fn append(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in &batches {
            validate_schema(&schema, &batch.schema())?;
        }
        Ok(Self {
            payload: ChangePayload::Rows {
                schema,
                batches,
                scoped_inputs: Vec::new(),
            },
            append_ingress: None,
        })
    }

    /// Append a proven-fresh logical scope without deleting existing rows.
    /// Backends must reject conflicting physical keys outside each scope,
    /// including conflicts between scopes combined into the same append.
    ///
    /// # Errors
    /// Returns an error if schemas differ or any row falls outside the scope.
    pub fn append_scoped(
        scope: SetKey,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        validate_schema(scope.schema(), &schema)?;
        for batch in &batches {
            scope.validate_batch(batch)?;
        }
        Ok(Self::scoped(scope, schema, batches, false))
    }

    /// Delete the scope once, then append all chunks as one ordered, non-atomic operation.
    /// An empty replacement deletes its scope; it is not a heartbeat.
    ///
    /// Validates schemas and membership before admission. The caller must establish
    /// source completeness, including pagination and limits; a finite vector does not
    /// prove completeness. Readers may observe the deletion gap, and an append failure
    /// after deletion may leave the previous rows lost.
    ///
    /// # Errors
    /// Returns an error if schemas differ or any row falls outside the scope.
    pub fn replace_set(
        scope: SetKey,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
        validate_schema(scope.schema(), &schema)?;
        for batch in &batches {
            scope.validate_batch(batch)?;
        }
        Ok(Self::scoped(scope, schema, batches, true))
    }

    fn scoped(scope: SetKey, schema: SchemaRef, batches: Vec<RecordBatch>, replaces: bool) -> Self {
        let range = 0..batches.len();
        Self {
            payload: ChangePayload::Rows {
                schema,
                batches,
                scoped_inputs: vec![ScopedInput {
                    scope,
                    batches: range,
                    replaces,
                }],
            },
            append_ingress: None,
        }
    }

    /// Opt rows into this producer lane's bounded owner batching. Clones of one
    /// writer must retain the same ingress identity. A coalesced burst deletes
    /// every replaced scope before it appends, so the owner admits a member only
    /// while no replaced scope in the burst can hold another member's rows.
    ///
    /// # Errors
    /// Returns an error for CDC input.
    pub fn with_append_ingress(mut self, ingress: Arc<AppendIngress>) -> Result<Self> {
        if !matches!(self.payload, ChangePayload::Rows { .. }) {
            return Err(DataFusionError::Plan(
                "Only Rows writes can join an append batching lane".into(),
            ));
        }
        self.append_ingress = Some(ingress);
        Ok(self)
    }

    #[must_use]
    pub fn append_ingress(&self) -> Option<&Arc<AppendIngress>> {
        self.append_ingress.as_ref()
    }

    pub(crate) fn can_merge_append(&self, other: &Self) -> bool {
        match (&self.payload, &other.payload) {
            (
                ChangePayload::Rows {
                    schema,
                    batches,
                    scoped_inputs,
                },
                ChangePayload::Rows {
                    schema: other_schema,
                    batches: other_batches,
                    scoped_inputs: other_inputs,
                },
            ) => {
                schema == other_schema
                    && scoped_inputs.is_empty() == other_inputs.is_empty()
                    && batches.len().checked_add(other_batches.len()).is_some()
            }
            _ => false,
        }
    }

    /// Extend only compatible rows. Each scoped range keeps its input identity;
    /// rows are neither concatenated nor deduplicated here. The caller decides
    /// whether the members' scopes may share one burst.
    /// Returns `Break` with the unchanged input at a compatibility boundary.
    pub(crate) fn merge_append(&mut self, other: Self) -> ControlFlow<Self> {
        if !self.can_merge_append(&other) {
            return ControlFlow::Break(other);
        }
        if let (
            ChangePayload::Rows {
                batches,
                scoped_inputs,
                ..
            },
            ChangePayload::Rows {
                batches: other_batches,
                scoped_inputs: other_inputs,
                ..
            },
        ) = (&mut self.payload, other.payload)
        {
            let offset = batches.len();
            // Preserve the sum of the inputs' charged vector capacities rather
            // than growing geometrically while the owner retains their claims.
            batches.reserve_exact(other_batches.len());
            scoped_inputs.reserve_exact(other_inputs.len());
            batches.extend(other_batches);
            scoped_inputs.extend(other_inputs.into_iter().map(|mut input| {
                input.batches.start += offset;
                input.batches.end += offset;
                input
            }));
        }
        ControlFlow::Continue(())
    }

    #[must_use]
    pub fn payload(&self) -> &ChangePayload {
        &self.payload
    }

    /// Each input's scope and chunk range; empty for CDC and unscoped appends.
    #[must_use]
    pub fn scoped_inputs(&self) -> &[ScopedInput] {
        match &self.payload {
            ChangePayload::Rows { scoped_inputs, .. } => scoped_inputs,
            ChangePayload::Cdc(_) => &[],
        }
    }

    /// Each scoped input as a batch of its own, in order, sharing the Arrow
    /// buffers. `None` for CDC input and unscoped appends, which record no members.
    pub(crate) fn scoped_members(&self) -> Option<Vec<Self>> {
        let ChangePayload::Rows {
            schema,
            batches,
            scoped_inputs,
        } = &self.payload
        else {
            return None;
        };
        if scoped_inputs.is_empty() {
            return None;
        }
        Some(
            scoped_inputs
                .iter()
                .map(|input| Self {
                    payload: ChangePayload::Rows {
                        schema: Arc::clone(schema),
                        batches: batches[input.batches.clone()].to_vec(),
                        scoped_inputs: vec![ScopedInput {
                            scope: input.scope.clone(),
                            batches: 0..input.batches.len(),
                            replaces: input.replaces,
                        }],
                    },
                    append_ingress: None,
                })
                .collect(),
        )
    }

    /// One batch holding every member in order, sharing the Arrow buffers.
    /// `None` when there are no members.
    pub(crate) fn coalesce_members(members: &[Self]) -> Option<Self> {
        let (first, rest) = members.split_first()?;
        let ChangePayload::Rows {
            schema,
            batches,
            scoped_inputs,
        } = &first.payload
        else {
            return None;
        };
        let mut merged = Self {
            payload: ChangePayload::Rows {
                schema: Arc::clone(schema),
                batches: batches.clone(),
                scoped_inputs: scoped_inputs
                    .iter()
                    .map(|input| ScopedInput {
                        scope: input.scope.clone(),
                        batches: input.batches.clone(),
                        replaces: input.replaces,
                    })
                    .collect(),
            },
            append_ingress: None,
        };
        for member in rest {
            let copy = Self::coalesce_members(std::slice::from_ref(member))?;
            if merged.merge_append(copy).is_break() {
                return None;
            }
        }
        Some(merged)
    }

    /// Scopes whose stored rows are deleted before this batch's rows are appended.
    pub fn replaced_scopes(&self) -> impl Iterator<Item = &SetKey> {
        self.scoped_inputs()
            .iter()
            .filter(|input| input.replaces)
            .map(|input| &input.scope)
    }

    #[must_use]
    pub fn cdc_batch(&self) -> Option<&cdc::ChangeBatch> {
        match &self.payload {
            ChangePayload::Cdc(rows) => rows.as_built(),
            ChangePayload::Rows { .. } => None,
        }
    }

    /// Whether the operations permit the native upsert-only burst pipeline.
    /// Primary-key changes must include an explicit delete operation; before-images
    /// are not additional operations. Target and schema barriers remain separate.
    #[must_use]
    pub fn permits_pipelined_append(&self) -> bool {
        let Some(batch) = self.cdc_batch() else {
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

    /// Scopes stay attached to their chunk ranges when transforming the payload.
    #[must_use]
    pub fn into_payload(self) -> ChangePayload {
        self.payload
    }

    /// Estimate retained Arrow buffers and scope values, not a bound on process memory.
    /// Shared buffers can be counted more than once.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        match &self.payload {
            ChangePayload::Cdc(rows) => rows.rows.encoded_len(),
            ChangePayload::Rows {
                batches,
                scoped_inputs,
                ..
            } => {
                let metadata = batches
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RecordBatch>())
                    .saturating_add(
                        scoped_inputs
                            .capacity()
                            .saturating_mul(std::mem::size_of::<ScopedInput>()),
                    );
                let scoped = scoped_inputs
                    .iter()
                    .map(|input| input.scope.estimated_bytes())
                    .fold(metadata, usize::saturating_add);
                batches
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .fold(scoped, usize::saturating_add)
            }
        }
    }

    #[must_use]
    pub fn num_rows(&self) -> usize {
        match &self.payload {
            ChangePayload::Cdc(rows) => rows.rows.num_rows_hint(),
            ChangePayload::Rows { batches, .. } => batches
                .iter()
                .map(RecordBatch::num_rows)
                .fold(0, usize::saturating_add),
        }
    }

    /// Only native CDC control input can be a heartbeat, not an empty row payload.
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        matches!(&self.payload, ChangePayload::Cdc(rows) if rows.rows.is_heartbeat())
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
