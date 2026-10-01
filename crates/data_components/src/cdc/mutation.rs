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

//! Complete-set mutations over independently stored rows.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Expr, col, lit};

/// Recovery contract for mutations without a replayable source position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Recovery {
    /// Publication must follow durable storage.
    #[default]
    Durable,
    /// Deferral requires a real source committer and the engine's configured
    /// replay gate. The label alone does not authorize memory publication.
    Replayable,
    /// Uncheckpointed mutations may be discarded and rebuilt by the producer.
    Rebuildable,
}

/// Typed equality key for a nonunique group of rows in one schema.
/// A null key component matches null, rather than SQL's unknown equality.
#[derive(Debug, Clone)]
pub struct SetKey {
    schema: SchemaRef,
    values: Vec<(String, ScalarValue)>,
}

impl SetKey {
    pub fn try_new(schema: SchemaRef, values: Vec<(String, ScalarValue)>) -> Result<Self> {
        if values.is_empty() {
            return Err(DataFusionError::Plan(
                "A replacement set requires a grouping key".into(),
            ));
        }
        let mut columns = HashSet::with_capacity(values.len());
        for (name, value) in &values {
            if !columns.insert(name) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate replacement key column '{name}'"
                )));
            }
            let field = schema.field_with_name(name)?;
            if field.data_type() != &value.data_type() {
                return Err(DataFusionError::Plan(format!(
                    "Replacement key column '{name}' has type {}, expected {}",
                    value.data_type(),
                    field.data_type()
                )));
            }
        }
        Ok(Self { schema, values })
    }

    /// Parses a conjunction of typed equalities. General predicates cannot
    /// identify a complete group and are rejected before admission.
    pub fn from_filters(schema: SchemaRef, filters: &[Expr]) -> Result<Self> {
        fn append(
            schema: &SchemaRef,
            expression: &Expr,
            values: &mut Vec<(String, ScalarValue)>,
        ) -> Result<()> {
            use datafusion::logical_expr::Operator;
            let (column, value) = match expression {
                Expr::BinaryExpr(binary) if binary.op == Operator::And => {
                    append(schema, &binary.left, values)?;
                    return append(schema, &binary.right, values);
                }
                Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
                    match (binary.left.as_ref(), binary.right.as_ref()) {
                        (Expr::Column(column), Expr::Literal(value, _))
                        | (Expr::Literal(value, _), Expr::Column(column))
                            if !value.is_null() =>
                        {
                            (
                                column,
                                value.cast_to(schema.field_with_name(&column.name)?.data_type())?,
                            )
                        }
                        _ => {
                            return Err(DataFusionError::Plan(
                                "A replacement key requires column equality".into(),
                            ));
                        }
                    }
                }
                Expr::IsNull(inner) => {
                    let Expr::Column(column) = inner.as_ref() else {
                        return Err(DataFusionError::Plan(
                            "A null replacement key requires a column".into(),
                        ));
                    };
                    (
                        column,
                        ScalarValue::try_from(schema.field_with_name(&column.name)?.data_type())?,
                    )
                }
                _ => {
                    return Err(DataFusionError::Plan(
                        "A replacement key requires column equality".into(),
                    ));
                }
            };
            if let Some((_, previous)) = values.iter().find(|(name, _)| name == &column.name) {
                if previous != &value {
                    return Err(DataFusionError::Plan(
                        "Conflicting replacement key values".into(),
                    ));
                }
            } else {
                values.push((column.name.clone(), value));
            }
            Ok(())
        }
        let mut values = Vec::new();
        for filter in filters {
            append(&schema, filter, &mut values)?;
        }
        values.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Self::try_new(schema, values)
    }

    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[must_use]
    pub fn values(&self) -> &[(String, ScalarValue)] {
        &self.values
    }

    /// Null-aware membership in this group, including view/storage string layouts.
    pub fn matching_rows(&self, batch: &RecordBatch) -> Result<arrow::array::BooleanArray> {
        use arrow::array::BooleanArray;
        use arrow::buffer::BooleanBuffer;
        use arrow::compute::{and, kernels::cmp::not_distinct};

        let mut mask = BooleanArray::new(BooleanBuffer::new_set(batch.num_rows()), None);
        for (name, value) in &self.values {
            let column = batch.column(batch.schema().index_of(name)?);
            let expected = value.cast_to(column.data_type())?.to_scalar()?;
            mask = and(&mask, &not_distinct(&column.as_ref(), &expected)?)?;
        }
        Ok(mask)
    }

    #[must_use]
    pub fn filters(&self) -> Vec<Expr> {
        self.values
            .iter()
            .map(|(name, value)| {
                let column = col(datafusion::common::Column::from_name(name));
                if value.is_null() {
                    column.is_null()
                } else {
                    column.eq(lit(value.clone()))
                }
            })
            .collect()
    }
}

/// Complete finite multiset replacing exactly one group. Construction validates
/// the schema and every row's membership before any storage mutation. Empty
/// rows represent deletion of this group, not a heartbeat or table truncation.
#[derive(Debug)]
pub struct ReplaceSet {
    key: SetKey,
    batches: Vec<RecordBatch>,
}

impl ReplaceSet {
    pub fn try_new(key: SetKey, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in &batches {
            if batch.schema() != *key.schema() {
                return Err(DataFusionError::Plan(
                    "Replacement rows do not match the grouping schema".into(),
                ));
            }
            for (name, value) in key.values() {
                let column = batch.column(batch.schema().index_of(name)?);
                let expected = value.to_scalar()?;
                let matching =
                    arrow::compute::kernels::cmp::not_distinct(&column.as_ref(), &expected)?;
                if matching.true_count() != batch.num_rows() {
                    return Err(DataFusionError::Plan(format!(
                        "Replacement row does not belong to group column '{name}'"
                    )));
                }
            }
        }
        Ok(Self { key, batches })
    }

    #[must_use]
    pub fn key(&self) -> &SetKey {
        &self.key
    }

    #[must_use]
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .fold(0, usize::saturating_add)
    }

    #[must_use]
    pub fn into_parts(self) -> (SetKey, Vec<RecordBatch>) {
        (self.key, self.batches)
    }

    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(self.key.schema())
    }
}
