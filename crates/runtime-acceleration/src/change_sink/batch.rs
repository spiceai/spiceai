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
    array::{RecordBatch, StringArray},
    compute::kernels::cmp::not_distinct,
    datatypes::{DataType, SchemaRef},
};
use data_components::cdc;
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
    replace_set: Option<SetKey>,
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
        /// Validation-only scopes for fresh appends. They never authorize deletion.
        append_validations: Vec<AppendValidation>,
    },
}

/// One fresh input's scope and exact range of unchanged Arrow chunks. Scoped
/// inputs cover the chunk vector in order; an empty input retains an empty range.
#[derive(Debug)]
pub struct AppendValidation {
    scope: SetKey,
    batches: Range<usize>,
}

impl AppendValidation {
    #[must_use]
    pub fn scope(&self) -> &SetKey {
        &self.scope
    }

    #[must_use]
    pub fn batch_range(&self) -> Range<usize> {
        self.batches.clone()
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
            replace_set: None,
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
                append_validations: Vec::new(),
            },
            replace_set: None,
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
        let range = 0..batches.len();
        Ok(Self {
            payload: ChangePayload::Rows {
                schema,
                batches,
                append_validations: vec![AppendValidation {
                    scope,
                    batches: range,
                }],
            },
            replace_set: None,
            append_ingress: None,
        })
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
        Ok(Self {
            payload: ChangePayload::Rows {
                schema,
                batches,
                append_validations: Vec::new(),
            },
            replace_set: Some(scope),
            append_ingress: None,
        })
    }

    /// Opt an append into this producer lane's bounded owner batching. Clones
    /// of one writer must retain the same ingress identity.
    ///
    /// # Errors
    /// Returns an error for CDC input or a replacement rather than an append.
    pub fn with_append_ingress(mut self, ingress: Arc<AppendIngress>) -> Result<Self> {
        if self.replace_set.is_some() || !matches!(self.payload, ChangePayload::Rows { .. }) {
            return Err(DataFusionError::Plan(
                "Only Rows appends can join an append batching lane".into(),
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
        if self.replace_set.is_some() || other.replace_set.is_some() {
            return false;
        }
        match (&self.payload, &other.payload) {
            (
                ChangePayload::Rows {
                    schema,
                    batches,
                    append_validations,
                },
                ChangePayload::Rows {
                    schema: other_schema,
                    batches: other_batches,
                    append_validations: other_validations,
                },
            ) => {
                schema == other_schema
                    && append_validations.is_empty() == other_validations.is_empty()
                    && batches.len().checked_add(other_batches.len()).is_some()
            }
            _ => false,
        }
    }

    /// Extend only compatible appends. Each validation range keeps its input
    /// identity; rows are neither concatenated nor deduplicated here.
    /// Returns `Break` with the unchanged input at a compatibility boundary.
    pub(crate) fn merge_append(&mut self, other: Self) -> ControlFlow<Self> {
        if !self.can_merge_append(&other) {
            return ControlFlow::Break(other);
        }
        if let (
            ChangePayload::Rows {
                batches,
                append_validations,
                ..
            },
            ChangePayload::Rows {
                batches: other_batches,
                append_validations: other_validations,
                ..
            },
        ) = (&mut self.payload, other.payload)
        {
            let offset = batches.len();
            // Preserve the sum of the inputs' charged vector capacities rather
            // than growing geometrically while the owner retains their claims.
            batches.reserve_exact(other_batches.len());
            append_validations.reserve_exact(other_validations.len());
            batches.extend(other_batches);
            append_validations.extend(other_validations.into_iter().map(|mut validation| {
                validation.batches.start += offset;
                validation.batches.end += offset;
                validation
            }));
        }
        ControlFlow::Continue(())
    }

    #[must_use]
    pub fn payload(&self) -> &ChangePayload {
        &self.payload
    }

    #[must_use]
    pub fn replacement_scope(&self) -> Option<&SetKey> {
        self.replace_set.as_ref()
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
        if self.replace_set.is_some() || batch.is_heartbeat() || batch.rebuild_from_this_batch() {
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

    /// Keep the scope attached to the whole payload when transforming its chunks.
    #[must_use]
    pub fn into_parts(self) -> (ChangePayload, Option<SetKey>) {
        (self.payload, self.replace_set)
    }

    /// Estimate retained Arrow buffers and scope values, not a bound on process memory.
    /// Shared buffers can be counted more than once.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        let rows = match &self.payload {
            ChangePayload::Cdc(rows) => rows.rows.encoded_len(),
            ChangePayload::Rows {
                batches,
                append_validations,
                ..
            } => {
                let metadata = batches
                    .capacity()
                    .saturating_mul(std::mem::size_of::<RecordBatch>())
                    .saturating_add(
                        append_validations
                            .capacity()
                            .saturating_mul(std::mem::size_of::<AppendValidation>()),
                    );
                let scoped = append_validations
                    .iter()
                    .map(|validation| validation.scope.estimated_bytes())
                    .fold(metadata, usize::saturating_add);
                batches
                    .iter()
                    .map(RecordBatch::get_array_memory_size)
                    .fold(scoped, usize::saturating_add)
            }
        };
        self.replace_set
            .as_ref()
            .map_or(rows, |scope| rows.saturating_add(scope.estimated_bytes()))
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
