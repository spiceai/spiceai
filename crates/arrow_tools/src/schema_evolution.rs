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

//! Classifies the difference between a stored (accelerator/checkpoint) schema and an
//! incoming source schema as [`SchemaEvolution::Identical`], a lossless
//! [`SchemaEvolution::Widening`], or [`SchemaEvolution::Incompatible`].
//!
//! The canonical evolved schema preserves the stored (current) field order, applies type
//! widening in place, and appends added columns at the end — accelerator engines can only
//! `ADD COLUMN`, so the canonical order must never interleave new columns mid-schema.
//!
//! Both input schemas are dictionary-normalized (`Dictionary(_, v)` → `v`, recursively, via
//! [`crate::type_rewrite::normalize_dictionary_types`]) before comparison, matching how the
//! runtime normalizes schemas before registration; the returned
//! [`WideningPlan::evolved_schema`] is therefore dictionary-free.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};

use crate::type_rewrite::normalize_dictionary_types;

/// The result of classifying an incoming schema against the current stored schema.
#[derive(Debug)]
pub enum SchemaEvolution {
    /// No material difference. Pure field reordering and nullability tightening
    /// (nullable → non-nullable) are treated as identical — the current schema
    /// remains canonical.
    Identical,
    /// The incoming schema is a lossless widening of the current schema.
    Widening(WideningPlan),
    /// The incoming schema cannot be evolved to losslessly: removed/renamed columns,
    /// narrowed or non-widening type changes, timezone changes, non-nullable column
    /// additions, or widening/nullability changes touching constraint columns.
    Incompatible { reason: String },
}

/// A lossless widening from the current schema to [`Self::evolved_schema`].
#[derive(Debug, Clone)]
pub struct WideningPlan {
    /// New nullable columns, appended at the end of [`Self::evolved_schema`].
    pub added_columns: Vec<FieldRef>,
    /// Columns whose type widened losslessly (applied in place in [`Self::evolved_schema`]).
    pub widened_columns: Vec<ColumnWidening>,
    /// Names of columns whose nullability relaxed from non-nullable to nullable.
    pub relaxed_nullability: Vec<String>,
    /// Canonical evolved schema: current field order with widened types applied in
    /// place and added columns appended at the end.
    pub evolved_schema: SchemaRef,
}

/// A single column type widening, e.g. `Int32` → `Int64`.
#[derive(Debug, Clone)]
pub struct ColumnWidening {
    pub name: String,
    pub from: DataType,
    pub to: DataType,
}

/// Dataset context consulted by [`classify`].
#[derive(Debug, Clone, Copy)]
pub struct EvolutionContext<'a> {
    /// Column names referenced by the dataset's primary key / unique / index constraints.
    /// Widening or relaxing the nullability of any of these is classified
    /// [`SchemaEvolution::Incompatible`]: engines persist typed key encodings and cannot
    /// alter constraint column types in place without corrupting them.
    pub constraint_columns: &'a [String],
}

impl WideningPlan {
    /// Returns `true` when the plan only adds new nullable columns (no type widening,
    /// no nullability relaxing) — the evolution set permitted by `append_new_columns`.
    #[must_use]
    pub fn is_additive_only(&self) -> bool {
        self.widened_columns.is_empty() && self.relaxed_nullability.is_empty()
    }

    /// Short human summary for logs, e.g.
    /// `2 added columns (c, d), 1 widened (a: Int32 -> Int64)`.
    #[must_use]
    pub fn describe(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if !self.added_columns.is_empty() {
            let names = self
                .added_columns
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let plural = if self.added_columns.len() == 1 {
                ""
            } else {
                "s"
            };
            parts.push(format!(
                "{} added column{plural} ({names})",
                self.added_columns.len()
            ));
        }
        if !self.widened_columns.is_empty() {
            let details = self
                .widened_columns
                .iter()
                .map(|w| format!("{}: {} -> {}", w.name, w.from, w.to))
                .collect::<Vec<_>>()
                .join(", ");
            parts.push(format!(
                "{} widened ({details})",
                self.widened_columns.len()
            ));
        }
        if !self.relaxed_nullability.is_empty() {
            parts.push(format!(
                "{} nullability relaxed ({})",
                self.relaxed_nullability.len(),
                self.relaxed_nullability.join(", ")
            ));
        }
        if parts.is_empty() {
            "no schema changes".to_string()
        } else {
            parts.join(", ")
        }
    }
}

/// Classifies `incoming` against `current` using name-based field matching.
///
/// Rules:
/// - Removed/renamed columns, non-widening type changes (including timezone changes),
///   and non-nullable column additions are [`SchemaEvolution::Incompatible`].
/// - Added columns must be nullable and are appended at the end of the evolved schema,
///   preserving their relative order in `incoming`.
/// - Type widening follows [`is_widening_cast`]; widened types are applied in place.
/// - Nullability relaxing (non-nullable → nullable) is part of the widening set;
///   nullability tightening keeps the current nullable field and records no change.
/// - Any widened or nullability-relaxed column named in
///   [`EvolutionContext::constraint_columns`] is [`SchemaEvolution::Incompatible`].
#[must_use]
pub fn classify(current: &Schema, incoming: &Schema, ctx: &EvolutionContext) -> SchemaEvolution {
    let current = normalize_dictionary_types(current);
    let incoming = normalize_dictionary_types(incoming);

    let mut incompatibilities: Vec<String> = Vec::new();
    let mut evolved_fields: Vec<FieldRef> = Vec::with_capacity(incoming.fields().len());
    let mut added_columns: Vec<FieldRef> = Vec::new();
    let mut widened_columns: Vec<ColumnWidening> = Vec::new();
    let mut relaxed_nullability: Vec<String> = Vec::new();

    for current_field in current.fields() {
        let Some(incoming_field) = incoming
            .fields()
            .iter()
            .find(|f| f.name() == current_field.name())
        else {
            incompatibilities.push(format!("The column `{}` is missing", current_field.name()));
            continue;
        };

        match widen_field(current_field, incoming_field) {
            FieldWidening::Incompatible => {
                incompatibilities.push(format!(
                    "The type of `{}` changed from `{}` to `{}`, which is not a lossless widening",
                    current_field.name(),
                    current_field.data_type(),
                    incoming_field.data_type()
                ));
            }
            FieldWidening::Equal => evolved_fields.push(Arc::clone(current_field)),
            FieldWidening::Widened(evolved) => {
                let type_widened = evolved.data_type() != current_field.data_type();
                let nullability_relaxed = evolved.is_nullable() && !current_field.is_nullable();
                if ctx
                    .constraint_columns
                    .iter()
                    .any(|c| c == current_field.name())
                {
                    let mut changes: Vec<String> = Vec::new();
                    if type_widened {
                        changes.push(format!(
                            "widened from `{}` to `{}`",
                            current_field.data_type(),
                            evolved.data_type()
                        ));
                    }
                    if nullability_relaxed {
                        changes.push("relaxed from non-nullable to nullable".to_string());
                    }
                    incompatibilities.push(format!(
                        "The column `{}` is part of the dataset's primary key, unique, or index constraints and cannot be {}",
                        current_field.name(),
                        changes.join(" or ")
                    ));
                    continue;
                }
                if type_widened {
                    widened_columns.push(ColumnWidening {
                        name: current_field.name().clone(),
                        from: current_field.data_type().clone(),
                        to: evolved.data_type().clone(),
                    });
                }
                if nullability_relaxed {
                    relaxed_nullability.push(current_field.name().clone());
                }
                evolved_fields.push(Arc::new(evolved));
            }
        }
    }

    for incoming_field in incoming.fields() {
        if current
            .fields()
            .iter()
            .any(|f| f.name() == incoming_field.name())
        {
            continue;
        }
        if incoming_field.is_nullable() {
            added_columns.push(Arc::clone(incoming_field));
        } else {
            incompatibilities.push(format!(
                "The added column `{}` is non-nullable; new columns must be nullable so existing rows can be backfilled with NULL",
                incoming_field.name()
            ));
        }
    }

    if !incompatibilities.is_empty() {
        return SchemaEvolution::Incompatible {
            reason: incompatibilities.join(". "),
        };
    }

    if added_columns.is_empty() && widened_columns.is_empty() && relaxed_nullability.is_empty() {
        return SchemaEvolution::Identical;
    }

    evolved_fields.extend(added_columns.iter().map(Arc::clone));
    let evolved_schema = Arc::new(Schema::new_with_metadata(
        evolved_fields,
        current.metadata().clone(),
    ));

    SchemaEvolution::Widening(WideningPlan {
        added_columns,
        widened_columns,
        relaxed_nullability,
        evolved_schema,
    })
}

/// Returns `true` when `from` → `to` is a lossless widening cast per the v1 widening
/// matrix. Equal types are not a widening. Dictionary types are compared by value type.
#[must_use]
pub fn is_widening_cast(from: &DataType, to: &DataType) -> bool {
    matches!(widen_type(from, to), TypeWidening::Widened(_))
}

enum TypeWidening {
    /// No material type change; the current type stays canonical.
    Equal,
    /// Lossless widening to the contained evolved type.
    Widened(DataType),
    Incompatible,
}

enum FieldWidening {
    /// No material change; the current field stays canonical (covers nullability
    /// tightening and element-name/metadata-only differences in nested types).
    Equal,
    /// Lossless widening; the evolved field keeps the current name and metadata.
    Widened(Field),
    Incompatible,
}

fn widen_field(current: &Field, incoming: &Field) -> FieldWidening {
    let evolved_type = match widen_type(current.data_type(), incoming.data_type()) {
        TypeWidening::Incompatible => return FieldWidening::Incompatible,
        TypeWidening::Equal => current.data_type().clone(),
        TypeWidening::Widened(data_type) => data_type,
    };
    // Nullability tightening (nullable -> non-nullable) keeps the current nullable field:
    // stored NULLs must stay representable, and stricter incoming values still fit.
    let evolved_nullable = current.is_nullable() || incoming.is_nullable();
    if evolved_type == *current.data_type() && evolved_nullable == current.is_nullable() {
        return FieldWidening::Equal;
    }
    FieldWidening::Widened(
        current
            .clone()
            .with_data_type(evolved_type)
            .with_nullable(evolved_nullable),
    )
}

fn widen_type(current: &DataType, incoming: &DataType) -> TypeWidening {
    // Dictionary encoding is transparent for evolution: compare value types, mirroring
    // `type_rewrite::normalize_dictionary_types`.
    let current = unwrap_dictionary(current);
    let incoming = unwrap_dictionary(incoming);
    if current == incoming {
        return TypeWidening::Equal;
    }

    if let (Some((from_tier, from_p, from_s)), Some((to_tier, to_p, to_s))) =
        (decimal_parts(current), decimal_parts(incoming))
    {
        // Lossless iff the physical width does not shrink (32 -> 64 -> 128 -> 256), the
        // scale does not shrink, and the integer-digit capacity (p - s) does not shrink.
        return if to_tier >= from_tier
            && to_s >= from_s
            && i16::from(to_p) - i16::from(to_s) >= i16::from(from_p) - i16::from(from_s)
        {
            TypeWidening::Widened(incoming.clone())
        } else {
            TypeWidening::Incompatible
        };
    }

    match (current, incoming) {
        // Null columns carry no values; any concrete type can absorb them.
        (DataType::Null, _)
        // Int/UInt up to 16 bits fit Float32's 24-bit mantissa; up to 32 bits fit
        // Float64's 53-bit mantissa. Int64/UInt64 -> Float64 is lossy and excluded.
        | (
            DataType::Int8,
            DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64,
        )
        | (
            DataType::Int16,
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64,
        )
        | (DataType::Int32, DataType::Int64 | DataType::Float64)
        | (
            DataType::UInt8,
            DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64,
        )
        | (
            DataType::UInt16,
            DataType::UInt32
            | DataType::UInt64
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64,
        )
        | (DataType::UInt32, DataType::UInt64 | DataType::Int64 | DataType::Float64)
        | (DataType::Float16, DataType::Float32 | DataType::Float64)
        | (DataType::Float32, DataType::Float64)
        | (DataType::Utf8, DataType::LargeUtf8 | DataType::Utf8View)
        | (DataType::Binary, DataType::LargeBinary | DataType::BinaryView)
        | (DataType::FixedSizeBinary(_), DataType::Binary)
        | (DataType::Date32, DataType::Date64) => TypeWidening::Widened(incoming.clone()),
        // A view/large string-or-binary column already holds the full value
        // domain of a narrower same-family column, so an accelerator advertising
        // view types (e.g. Cayenne's force-view read schema over `Utf8`/`Binary`
        // storage) compared against a non-view source is NOT a schema change —
        // classify it `Equal`. The narrow -> wide direction above stays `Widened`
        // so a genuine source widening for non-view accelerators still evolves.
        (DataType::Utf8View, DataType::Utf8 | DataType::LargeUtf8)
        | (DataType::LargeUtf8, DataType::Utf8 | DataType::Utf8View)
        | (DataType::BinaryView, DataType::Binary | DataType::LargeBinary)
        | (DataType::LargeBinary, DataType::Binary | DataType::BinaryView) => TypeWidening::Equal,
        (DataType::Time32(from_unit), DataType::Time32(to_unit) | DataType::Time64(to_unit))
        | (DataType::Time64(from_unit), DataType::Time64(to_unit)) => {
            if time_unit_rank(*to_unit) > time_unit_rank(*from_unit) {
                TypeWidening::Widened(incoming.clone())
            } else {
                TypeWidening::Incompatible
            }
        }
        (DataType::Timestamp(from_unit, from_tz), DataType::Timestamp(to_unit, to_tz)) => {
            // Same timezone only. Nanosecond is excluded as a target: rescaling to ns
            // overflows the i64 range for dates beyond ~2262 (cast_column's documented
            // timestamp-unit fallback in record_batch.rs NULLs such values).
            if from_tz == to_tz
                && *to_unit != TimeUnit::Nanosecond
                && time_unit_rank(*to_unit) > time_unit_rank(*from_unit)
            {
                TypeWidening::Widened(incoming.clone())
            } else {
                TypeWidening::Incompatible
            }
        }
        (DataType::List(from_el), DataType::List(to_el)) => {
            widen_container(from_el, to_el, DataType::List, false)
        }
        (DataType::List(from_el), DataType::LargeList(to_el)) => {
            widen_container(from_el, to_el, DataType::LargeList, true)
        }
        (DataType::LargeList(from_el), DataType::LargeList(to_el)) => {
            widen_container(from_el, to_el, DataType::LargeList, false)
        }
        (DataType::FixedSizeList(from_el, from_size), DataType::FixedSizeList(to_el, to_size))
            if from_size == to_size =>
        {
            let size = *from_size;
            widen_container(
                from_el,
                to_el,
                move |f| DataType::FixedSizeList(f, size),
                false,
            )
        }
        (DataType::Struct(current_fields), DataType::Struct(incoming_fields)) => {
            widen_struct(current_fields, incoming_fields)
        }
        _ => TypeWidening::Incompatible,
    }
}

fn widen_container(
    current_element: &FieldRef,
    incoming_element: &FieldRef,
    rebuild: impl FnOnce(FieldRef) -> DataType,
    container_widened: bool,
) -> TypeWidening {
    match widen_field(current_element, incoming_element) {
        FieldWidening::Incompatible => TypeWidening::Incompatible,
        FieldWidening::Equal if container_widened => {
            TypeWidening::Widened(rebuild(Arc::clone(current_element)))
        }
        FieldWidening::Equal => TypeWidening::Equal,
        FieldWidening::Widened(field) => TypeWidening::Widened(rebuild(Arc::new(field))),
    }
}

fn widen_struct(current_fields: &Fields, incoming_fields: &Fields) -> TypeWidening {
    // arrow-cast silently falls back to POSITIONAL struct casting when the field-name
    // sets differ, which transposes values — require identical name sets so the cast
    // is name-based.
    let current_names: HashSet<&str> = current_fields.iter().map(|f| f.name().as_str()).collect();
    let incoming_names: HashSet<&str> = incoming_fields.iter().map(|f| f.name().as_str()).collect();
    if current_names != incoming_names {
        return TypeWidening::Incompatible;
    }

    let mut any_change = false;
    let mut evolved: Vec<FieldRef> = Vec::with_capacity(current_fields.len());
    for current_field in current_fields {
        let Some(incoming_field) = incoming_fields
            .iter()
            .find(|f| f.name() == current_field.name())
        else {
            return TypeWidening::Incompatible;
        };
        match widen_field(current_field, incoming_field) {
            FieldWidening::Incompatible => return TypeWidening::Incompatible,
            FieldWidening::Equal => evolved.push(Arc::clone(current_field)),
            FieldWidening::Widened(field) => {
                any_change = true;
                evolved.push(Arc::new(field));
            }
        }
    }
    if any_change {
        TypeWidening::Widened(DataType::Struct(evolved.into()))
    } else {
        // Reordered-only (or element-metadata-only) struct: keep the current field order.
        TypeWidening::Equal
    }
}

fn unwrap_dictionary(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, value_type) => unwrap_dictionary(value_type),
        _ => dt,
    }
}

fn decimal_parts(dt: &DataType) -> Option<(u8, u8, i8)> {
    match dt {
        DataType::Decimal32(precision, scale) => Some((0, *precision, *scale)),
        DataType::Decimal64(precision, scale) => Some((1, *precision, *scale)),
        DataType::Decimal128(precision, scale) => Some((2, *precision, *scale)),
        DataType::Decimal256(precision, scale) => Some((3, *precision, *scale)),
        _ => None,
    }
}

fn time_unit_rank(unit: TimeUnit) -> u8 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 1,
        TimeUnit::Microsecond => 2,
        TimeUnit::Nanosecond => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_CONSTRAINTS: EvolutionContext<'static> = EvolutionContext {
        constraint_columns: &[],
    };

    fn expect_widening(evolution: SchemaEvolution) -> WideningPlan {
        match evolution {
            SchemaEvolution::Widening(plan) => plan,
            other => panic!("expected Widening, got {other:?}"),
        }
    }

    fn expect_incompatible(evolution: SchemaEvolution) -> String {
        match evolution {
            SchemaEvolution::Incompatible { reason } => reason,
            other => panic!("expected Incompatible, got {other:?}"),
        }
    }

    fn assert_identical(evolution: &SchemaEvolution) {
        assert!(
            matches!(evolution, SchemaEvolution::Identical),
            "expected Identical, got {evolution:?}"
        );
    }

    fn assert_widening_pairs(pairs: &[(DataType, DataType)]) {
        for (from, to) in pairs {
            assert!(
                is_widening_cast(from, to),
                "expected {from} -> {to} to be a widening cast"
            );
        }
    }

    fn assert_not_widening_pairs(pairs: &[(DataType, DataType)]) {
        for (from, to) in pairs {
            assert!(
                !is_widening_cast(from, to),
                "expected {from} -> {to} to NOT be a widening cast"
            );
        }
    }

    #[test]
    fn view_over_nonview_string_binary_is_not_an_evolution() {
        // A view/large accelerator column compared against a narrower same-family
        // source column is NOT a schema change (e.g. Cayenne's force-view read
        // schema over a Utf8/Binary source). classify(current=view, incoming=
        // nonview) must be Identical — not Widening or Incompatible.
        let pairs = [
            (DataType::Utf8View, DataType::Utf8),
            (DataType::Utf8View, DataType::LargeUtf8),
            (DataType::LargeUtf8, DataType::Utf8),
            (DataType::BinaryView, DataType::Binary),
            (DataType::BinaryView, DataType::LargeBinary),
            (DataType::LargeBinary, DataType::Binary),
        ];
        for (current, incoming) in pairs {
            let cur = Schema::new(vec![Field::new("c", current.clone(), true)]);
            let inc = Schema::new(vec![Field::new("c", incoming.clone(), true)]);
            assert_identical(&classify(&cur, &inc, &NO_CONSTRAINTS));
        }
        // The narrow -> wide direction stays a (lossless) widening for non-view
        // accelerators that genuinely evolve their stored type.
        assert!(is_widening_cast(&DataType::Utf8, &DataType::Utf8View));
    }

    mod widening_matrix {
        use super::*;

        #[test]
        fn signed_int_widening() {
            assert_widening_pairs(&[
                (DataType::Int8, DataType::Int16),
                (DataType::Int8, DataType::Int32),
                (DataType::Int8, DataType::Int64),
                (DataType::Int16, DataType::Int32),
                (DataType::Int16, DataType::Int64),
                (DataType::Int32, DataType::Int64),
            ]);
        }

        #[test]
        fn signed_int_narrowing_and_equal_excluded() {
            assert_not_widening_pairs(&[
                (DataType::Int16, DataType::Int8),
                (DataType::Int32, DataType::Int16),
                (DataType::Int64, DataType::Int32),
                (DataType::Int32, DataType::Int32),
                (DataType::Int8, DataType::UInt16),
                (DataType::Int32, DataType::UInt64),
            ]);
        }

        #[test]
        fn unsigned_int_widening() {
            assert_widening_pairs(&[
                (DataType::UInt8, DataType::UInt16),
                (DataType::UInt8, DataType::UInt32),
                (DataType::UInt8, DataType::UInt64),
                (DataType::UInt16, DataType::UInt32),
                (DataType::UInt16, DataType::UInt64),
                (DataType::UInt32, DataType::UInt64),
            ]);
        }

        #[test]
        fn unsigned_to_larger_signed_widening() {
            assert_widening_pairs(&[
                (DataType::UInt8, DataType::Int16),
                (DataType::UInt8, DataType::Int32),
                (DataType::UInt8, DataType::Int64),
                (DataType::UInt16, DataType::Int32),
                (DataType::UInt16, DataType::Int64),
                (DataType::UInt32, DataType::Int64),
            ]);
        }

        #[test]
        fn unsigned_to_same_or_smaller_signed_excluded() {
            assert_not_widening_pairs(&[
                (DataType::UInt8, DataType::Int8),
                (DataType::UInt16, DataType::Int16),
                (DataType::UInt32, DataType::Int32),
                (DataType::UInt64, DataType::Int64),
                (DataType::UInt16, DataType::Int8),
            ]);
        }

        #[test]
        fn float_widening() {
            assert_widening_pairs(&[
                (DataType::Float16, DataType::Float32),
                (DataType::Float16, DataType::Float64),
                (DataType::Float32, DataType::Float64),
            ]);
            assert_not_widening_pairs(&[
                (DataType::Float64, DataType::Float32),
                (DataType::Float32, DataType::Float16),
                (DataType::Float64, DataType::Float64),
            ]);
        }

        #[test]
        fn small_int_to_float_widening() {
            assert_widening_pairs(&[
                (DataType::Int8, DataType::Float32),
                (DataType::Int16, DataType::Float32),
                (DataType::UInt8, DataType::Float32),
                (DataType::UInt16, DataType::Float32),
                (DataType::Int8, DataType::Float64),
                (DataType::Int16, DataType::Float64),
                (DataType::Int32, DataType::Float64),
                (DataType::UInt8, DataType::Float64),
                (DataType::UInt16, DataType::Float64),
                (DataType::UInt32, DataType::Float64),
            ]);
        }

        #[test]
        fn large_int_to_float_excluded() {
            assert_not_widening_pairs(&[
                (DataType::Int32, DataType::Float32),
                (DataType::UInt32, DataType::Float32),
                (DataType::Int64, DataType::Float64),
                (DataType::UInt64, DataType::Float64),
                (DataType::Int64, DataType::Float32),
                (DataType::Int8, DataType::Float16),
            ]);
        }

        #[test]
        fn decimal_widening() {
            assert_widening_pairs(&[
                (DataType::Decimal32(5, 2), DataType::Decimal64(7, 2)),
                (DataType::Decimal64(10, 2), DataType::Decimal128(12, 2)),
                (DataType::Decimal128(20, 4), DataType::Decimal256(30, 4)),
                (DataType::Decimal32(5, 2), DataType::Decimal128(10, 4)),
                // Same physical width, more precision / more scale with integer digits kept.
                (DataType::Decimal128(10, 2), DataType::Decimal128(12, 2)),
                (DataType::Decimal128(10, 2), DataType::Decimal128(14, 4)),
            ]);
        }

        #[test]
        fn decimal_lossy_changes_excluded() {
            assert_not_widening_pairs(&[
                // Scale loss drops fractional digits.
                (DataType::Decimal128(10, 4), DataType::Decimal128(12, 2)),
                // Integer-digit capacity loss: p - s shrinks 8 -> 7.
                (DataType::Decimal128(10, 2), DataType::Decimal128(11, 4)),
                (DataType::Decimal128(10, 2), DataType::Decimal128(9, 2)),
                // Physical width may only grow.
                (DataType::Decimal128(5, 2), DataType::Decimal64(7, 2)),
                (DataType::Decimal256(5, 2), DataType::Decimal128(10, 2)),
                // Equal is not a widening.
                (DataType::Decimal128(10, 2), DataType::Decimal128(10, 2)),
            ]);
        }

        #[test]
        fn utf8_and_binary_family_widening_one_direction() {
            assert_widening_pairs(&[
                (DataType::Utf8, DataType::LargeUtf8),
                (DataType::Utf8, DataType::Utf8View),
                (DataType::Binary, DataType::LargeBinary),
                (DataType::Binary, DataType::BinaryView),
                (DataType::FixedSizeBinary(16), DataType::Binary),
            ]);
            assert_not_widening_pairs(&[
                (DataType::LargeUtf8, DataType::Utf8),
                (DataType::Utf8View, DataType::Utf8),
                (DataType::LargeUtf8, DataType::Utf8View),
                (DataType::Utf8View, DataType::LargeUtf8),
                (DataType::LargeBinary, DataType::Binary),
                (DataType::BinaryView, DataType::LargeBinary),
                (DataType::FixedSizeBinary(16), DataType::LargeBinary),
                (DataType::FixedSizeBinary(16), DataType::BinaryView),
                (DataType::FixedSizeBinary(16), DataType::FixedSizeBinary(32)),
                (DataType::Binary, DataType::FixedSizeBinary(16)),
                (DataType::Utf8, DataType::Binary),
            ]);
        }

        #[test]
        fn date_widening() {
            assert_widening_pairs(&[(DataType::Date32, DataType::Date64)]);
            assert_not_widening_pairs(&[(DataType::Date64, DataType::Date32)]);
        }

        #[test]
        fn time_hierarchy_widening() {
            assert_widening_pairs(&[
                (
                    DataType::Time32(TimeUnit::Second),
                    DataType::Time32(TimeUnit::Millisecond),
                ),
                (
                    DataType::Time32(TimeUnit::Second),
                    DataType::Time64(TimeUnit::Microsecond),
                ),
                (
                    DataType::Time32(TimeUnit::Second),
                    DataType::Time64(TimeUnit::Nanosecond),
                ),
                (
                    DataType::Time32(TimeUnit::Millisecond),
                    DataType::Time64(TimeUnit::Microsecond),
                ),
                (
                    DataType::Time32(TimeUnit::Millisecond),
                    DataType::Time64(TimeUnit::Nanosecond),
                ),
                (
                    DataType::Time64(TimeUnit::Microsecond),
                    DataType::Time64(TimeUnit::Nanosecond),
                ),
            ]);
            assert_not_widening_pairs(&[
                (
                    DataType::Time32(TimeUnit::Millisecond),
                    DataType::Time32(TimeUnit::Second),
                ),
                (
                    DataType::Time64(TimeUnit::Nanosecond),
                    DataType::Time64(TimeUnit::Microsecond),
                ),
                (
                    DataType::Time64(TimeUnit::Microsecond),
                    DataType::Time32(TimeUnit::Millisecond),
                ),
            ]);
        }

        #[test]
        fn timestamp_same_tz_widening_excludes_nanosecond() {
            assert_widening_pairs(&[
                (
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                ),
                (
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+05:00".into())),
                    DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into())),
                ),
            ]);
            assert_not_widening_pairs(&[
                // Nanosecond target excluded: overflows for dates beyond ~2262.
                (
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ),
                // Narrowing.
                (
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ),
                // Timezone change.
                (
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+05:00".into())),
                ),
                (
                    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                ),
                (
                    DataType::Timestamp(TimeUnit::Second, None),
                    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                ),
            ]);
        }

        #[test]
        fn null_widens_to_any_concrete_type() {
            assert_widening_pairs(&[
                (DataType::Null, DataType::Int32),
                (DataType::Null, DataType::Utf8),
                (
                    DataType::Null,
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                ),
            ]);
            assert_not_widening_pairs(&[
                (DataType::Int32, DataType::Null),
                (DataType::Null, DataType::Null),
            ]);
        }

        #[test]
        fn list_element_widening_recurses() {
            let list = |dt: DataType| DataType::List(Arc::new(Field::new("item", dt, true)));
            let large_list =
                |dt: DataType| DataType::LargeList(Arc::new(Field::new("item", dt, true)));
            let fixed = |dt: DataType, n: i32| {
                DataType::FixedSizeList(Arc::new(Field::new("item", dt, true)), n)
            };
            assert_widening_pairs(&[
                (list(DataType::Int32), list(DataType::Int64)),
                (large_list(DataType::Int32), large_list(DataType::Int64)),
                (list(DataType::Int32), large_list(DataType::Int32)),
                (list(DataType::Int32), large_list(DataType::Int64)),
                (fixed(DataType::Int32, 3), fixed(DataType::Int64, 3)),
                // Nested recursion: list of lists.
                (list(list(DataType::Utf8)), list(list(DataType::LargeUtf8))),
            ]);
            assert_not_widening_pairs(&[
                (list(DataType::Int64), list(DataType::Int32)),
                (large_list(DataType::Int32), list(DataType::Int32)),
                (fixed(DataType::Int32, 3), fixed(DataType::Int32, 4)),
                (fixed(DataType::Int32, 3), list(DataType::Int32)),
                (list(DataType::Int32), list(DataType::Int32)),
            ]);
        }

        #[test]
        fn list_element_nullability_relax_is_widening_tighten_is_not() {
            let nullable = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
            let non_null = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
            assert!(is_widening_cast(&non_null, &nullable));
            // Tightening keeps the current nullable element; not a widening.
            assert!(!is_widening_cast(&nullable, &non_null));
        }

        #[test]
        fn struct_per_field_widening_order_insensitive() {
            let current = DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Utf8, true),
                ]
                .into(),
            );
            let incoming = DataType::Struct(
                vec![
                    Field::new("y", DataType::Utf8, true),
                    Field::new("x", DataType::Int64, false),
                ]
                .into(),
            );
            assert!(is_widening_cast(&current, &incoming));
        }

        #[test]
        fn struct_with_different_field_name_sets_is_not_widening() {
            // arrow-cast silently falls back to positional struct casting when the
            // field-name sets differ — must never classify as a widening.
            let current = DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Int32, true),
                ]
                .into(),
            );
            let renamed = DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("z", DataType::Int32, true),
                ]
                .into(),
            );
            let added_field = DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Int32, true),
                    Field::new("z", DataType::Int32, true),
                ]
                .into(),
            );
            assert!(!is_widening_cast(&current, &renamed));
            assert!(!is_widening_cast(&current, &added_field));
        }

        #[test]
        fn struct_reordered_identical_fields_is_not_a_widening() {
            let current = DataType::Struct(
                vec![
                    Field::new("x", DataType::Int32, true),
                    Field::new("y", DataType::Utf8, true),
                ]
                .into(),
            );
            let reordered = DataType::Struct(
                vec![
                    Field::new("y", DataType::Utf8, true),
                    Field::new("x", DataType::Int32, true),
                ]
                .into(),
            );
            assert!(!is_widening_cast(&current, &reordered));
        }

        #[test]
        fn struct_with_narrowed_field_is_not_widening() {
            let current = DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into());
            let narrowed = DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into());
            assert!(!is_widening_cast(&current, &narrowed));
        }

        #[test]
        fn dictionary_types_compared_by_value_type() {
            let dict_utf8 =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            assert!(is_widening_cast(&dict_utf8, &DataType::LargeUtf8));
            assert!(is_widening_cast(
                &DataType::Utf8,
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::LargeUtf8))
            ));
            // Key-type-only difference is no change at all.
            assert!(!is_widening_cast(
                &dict_utf8,
                &DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8))
            ));
            assert!(!is_widening_cast(&dict_utf8, &DataType::Utf8));
        }

        #[test]
        fn unrelated_type_changes_excluded() {
            assert_not_widening_pairs(&[
                (DataType::Int32, DataType::Utf8),
                (DataType::Utf8, DataType::Int32),
                (DataType::Boolean, DataType::Int8),
                (
                    DataType::Date32,
                    DataType::Timestamp(TimeUnit::Second, None),
                ),
            ]);
        }
    }

    mod classify_tests {
        use super::*;

        #[test]
        fn identical_schemas() {
            let schema = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            assert_identical(&classify(&schema, &schema, &NO_CONSTRAINTS));
        }

        #[test]
        fn reordered_but_otherwise_identical_fields_are_identical() {
            // Pure reordering is Identical: the caller keeps the current (checkpoint)
            // order canonical and rebinds registration to it.
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            let incoming = Schema::new(vec![
                Field::new("b", DataType::Utf8, true),
                Field::new("a", DataType::Int32, false),
            ]);
            assert_identical(&classify(&current, &incoming, &NO_CONSTRAINTS));
        }

        #[test]
        fn nullability_tightening_is_identical() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            assert_identical(&classify(&current, &incoming, &NO_CONSTRAINTS));
        }

        #[test]
        fn added_nullable_column_mid_schema_appends_at_end() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("c", DataType::Float64, true),
                Field::new("b", DataType::Utf8, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(plan.is_additive_only());
            assert_eq!(plan.added_columns.len(), 1);
            assert_eq!(plan.added_columns[0].name(), "c");
            assert!(plan.widened_columns.is_empty());
            assert!(plan.relaxed_nullability.is_empty());
            let names: Vec<&str> = plan
                .evolved_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(
                names,
                ["a", "b", "c"],
                "added columns must land appended at the END of the current field order"
            );
        }

        #[test]
        fn added_non_nullable_column_is_incompatible() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("c", DataType::Utf8, false),
            ]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(
                reason.contains("`c`"),
                "reason should name the column: {reason}"
            );
            assert!(
                reason.contains("nullable"),
                "reason should explain the nullability requirement: {reason}"
            );
        }

        #[test]
        fn widened_type_applied_in_place_preserves_order() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float32, true),
            ]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Float64, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(!plan.is_additive_only());
            assert_eq!(plan.widened_columns.len(), 1);
            assert_eq!(plan.widened_columns[0].name, "c");
            assert_eq!(plan.widened_columns[0].from, DataType::Float32);
            assert_eq!(plan.widened_columns[0].to, DataType::Float64);
            let names: Vec<&str> = plan
                .evolved_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(names, ["a", "b", "c"]);
            assert_eq!(plan.evolved_schema.field(2).data_type(), &DataType::Float64);
        }

        #[test]
        fn nullability_relax_is_recorded_and_applied() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(!plan.is_additive_only());
            assert_eq!(plan.relaxed_nullability, ["a".to_string()]);
            assert!(plan.widened_columns.is_empty());
            assert!(plan.added_columns.is_empty());
            assert!(plan.evolved_schema.field(0).is_nullable());
        }

        #[test]
        fn type_widening_with_nullability_tightening_keeps_nullable() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(plan.widened_columns.len(), 1);
            assert!(plan.relaxed_nullability.is_empty());
            assert_eq!(plan.evolved_schema.field(0).data_type(), &DataType::Int64);
            assert!(
                plan.evolved_schema.field(0).is_nullable(),
                "tightening must keep the current nullable field"
            );
        }

        #[test]
        fn removed_column_is_incompatible() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(
                reason.contains("The column `b` is missing"),
                "unexpected reason: {reason}"
            );
        }

        #[test]
        fn renamed_column_is_incompatible() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let incoming = Schema::new(vec![Field::new("a2", DataType::Int32, true)]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(
                reason.contains("The column `a` is missing"),
                "unexpected reason: {reason}"
            );
        }

        #[test]
        fn narrowed_column_is_incompatible() {
            let current = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(
                reason.contains("The type of `a` changed from `Int64` to `Int32`"),
                "unexpected reason: {reason}"
            );
        }

        #[test]
        fn timezone_change_is_incompatible() {
            let current = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into())),
                true,
            )]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(reason.contains("`ts`"), "unexpected reason: {reason}");
        }

        #[test]
        fn multiple_incompatibilities_are_joined() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(reason.contains("The column `b` is missing"));
            assert!(reason.contains("The type of `a` changed from `Int64` to `Int32`"));
            assert!(reason.contains(". "), "reasons should be joined: {reason}");
        }

        #[test]
        fn constraint_column_widening_is_incompatible() {
            let current = Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("v", DataType::Utf8, true),
            ]);
            let incoming = Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("v", DataType::Utf8, true),
            ]);
            let constraints = ["id".to_string()];
            let ctx = EvolutionContext {
                constraint_columns: &constraints,
            };
            let reason = expect_incompatible(classify(&current, &incoming, &ctx));
            assert!(reason.contains("`id`"), "unexpected reason: {reason}");
            assert!(
                reason.contains("constraint"),
                "reason should name the constraint: {reason}"
            );
            assert!(
                reason.contains("`Int32`") && reason.contains("`Int64`"),
                "reason should name the types: {reason}"
            );

            // The same change is a fine widening when the constraint names another column.
            let unrelated = ["v".to_string()];
            let ctx = EvolutionContext {
                constraint_columns: &unrelated,
            };
            let plan = expect_widening(classify(&current, &incoming, &ctx));
            assert_eq!(plan.widened_columns[0].name, "id");
        }

        #[test]
        fn constraint_column_nullability_relax_is_incompatible() {
            let current = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
            let incoming = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
            let constraints = ["id".to_string()];
            let ctx = EvolutionContext {
                constraint_columns: &constraints,
            };
            let reason = expect_incompatible(classify(&current, &incoming, &ctx));
            assert!(reason.contains("`id`"), "unexpected reason: {reason}");
            assert!(
                reason.contains("nullable"),
                "reason should mention nullability: {reason}"
            );
        }

        #[test]
        fn dictionary_normalization_applies_to_both_sides() {
            let dict_utf8 =
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
            let current = Schema::new(vec![Field::new("status", dict_utf8, true)]);

            let incoming_same = Schema::new(vec![Field::new("status", DataType::Utf8, true)]);
            assert_identical(&classify(&current, &incoming_same, &NO_CONSTRAINTS));

            let incoming_wider = Schema::new(vec![Field::new("status", DataType::LargeUtf8, true)]);
            let plan = expect_widening(classify(&current, &incoming_wider, &NO_CONSTRAINTS));
            assert_eq!(
                plan.widened_columns[0].from,
                DataType::Utf8,
                "the recorded `from` type is the dictionary-normalized value type"
            );
            assert_eq!(plan.widened_columns[0].to, DataType::LargeUtf8);
        }

        #[test]
        fn struct_widening_keeps_current_field_order() {
            let current = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, false),
                        Field::new("y", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("y", DataType::Utf8, true),
                        Field::new("x", DataType::Int64, false),
                    ]
                    .into(),
                ),
                true,
            )]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(plan.widened_columns.len(), 1);
            let DataType::Struct(evolved_fields) = plan.evolved_schema.field(0).data_type() else {
                panic!("expected a struct type");
            };
            assert_eq!(evolved_fields[0].name(), "x");
            assert_eq!(evolved_fields[0].data_type(), &DataType::Int64);
            assert_eq!(evolved_fields[1].name(), "y");
            assert_eq!(evolved_fields[1].data_type(), &DataType::Utf8);
        }

        #[test]
        fn struct_name_set_change_is_incompatible() {
            let current = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(vec![Field::new("x", DataType::Int32, true)].into()),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(vec![Field::new("z", DataType::Int32, true)].into()),
                true,
            )]);
            let reason = expect_incompatible(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert!(
                reason.contains("`s`"),
                "reason should name the column: {reason}"
            );
        }

        #[test]
        fn struct_reordered_only_is_identical() {
            let current = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int32, true),
                        Field::new("y", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "s",
                DataType::Struct(
                    vec![
                        Field::new("y", DataType::Utf8, true),
                        Field::new("x", DataType::Int32, true),
                    ]
                    .into(),
                ),
                true,
            )]);
            assert_identical(&classify(&current, &incoming, &NO_CONSTRAINTS));
        }

        #[test]
        fn list_element_name_difference_is_identical() {
            let current = Schema::new(vec![Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
                true,
            )]);
            assert_identical(&classify(&current, &incoming, &NO_CONSTRAINTS));
        }

        #[test]
        fn list_element_tightening_is_identical() {
            let current = Schema::new(vec![Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )]);
            let incoming = Schema::new(vec![Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                true,
            )]);
            assert_identical(&classify(&current, &incoming, &NO_CONSTRAINTS));
        }

        #[test]
        fn null_column_widens_to_concrete_type() {
            let current = Schema::new(vec![Field::new("a", DataType::Null, true)]);
            let incoming = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(plan.widened_columns[0].from, DataType::Null);
            assert_eq!(plan.widened_columns[0].to, DataType::Int32);
            assert_eq!(plan.evolved_schema.field(0).data_type(), &DataType::Int32);
        }

        #[test]
        fn evolved_schema_preserves_current_schema_metadata() {
            use std::collections::HashMap;
            let mut metadata = HashMap::new();
            metadata.insert("source".to_string(), "test".to_string());
            let current = Schema::new_with_metadata(
                vec![Field::new("a", DataType::Int32, false)],
                metadata.clone(),
            );
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(plan.evolved_schema.metadata(), &metadata);
        }

        #[test]
        fn combined_changes_and_idempotent_reclassify() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
                Field::new("d", DataType::Int16, true),
            ]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("c", DataType::Float64, true),
                Field::new("b", DataType::LargeUtf8, true),
                Field::new("d", DataType::Int16, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(plan.added_columns.len(), 1);
            assert_eq!(plan.added_columns[0].name(), "c");
            assert_eq!(plan.widened_columns.len(), 1);
            assert_eq!(plan.widened_columns[0].name, "b");
            assert_eq!(plan.relaxed_nullability, ["b".to_string()]);
            assert!(!plan.is_additive_only());

            let names: Vec<&str> = plan
                .evolved_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(names, ["a", "b", "d", "c"]);

            // Applying the plan and re-classifying against the same incoming schema
            // must be a no-op (engine-first crash recovery relies on this).
            let again = classify(&plan.evolved_schema, &incoming, &NO_CONSTRAINTS);
            assert_identical(&again);
        }

        #[test]
        fn describe_summarizes_changes() {
            let current = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("c", DataType::Utf8, true),
                Field::new("d", DataType::Utf8, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(
                plan.describe(),
                "2 added columns (c, d), 1 widened (a: Int32 -> Int64)"
            );
        }

        #[test]
        fn describe_singular_added_column_and_relaxed_nullability() {
            let current = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, false),
            ]);
            let incoming = Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
                Field::new("c", DataType::Utf8, true),
            ]);
            let plan = expect_widening(classify(&current, &incoming, &NO_CONSTRAINTS));
            assert_eq!(
                plan.describe(),
                "1 added column (c), 1 nullability relaxed (b)"
            );
        }
    }
}
