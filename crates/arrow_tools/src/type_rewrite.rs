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

use std::sync::Arc;

use arrow::array::{ArrayData, make_array};
use arrow::buffer::NullBuffer;
use arrow::error::ArrowError;
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use arrow_schema::{
    DataType, Field, FieldRef, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};

/// A rewrite rule applied by [`apply_rules`] to every [`DataType`] node in a schema.
///
/// Rules are applied post-order: children are rewritten before the parent sees them.
///
/// Two kinds of rule live here, and only one belongs in an engine's rule list. Most describe
/// what a storage engine *can hold* — `DuckDB` has no Arrow Dictionary, no Null type — and are
/// named in that engine's static list. [`MapEntriesNonNullable`] instead describes what a
/// *source got wrong*, so it is applied where that source's data enters and belongs to no
/// engine's list.
///
/// `Debug` is required so a rule list can sit in a `#[derive(Debug)]` struct; the unit
/// structs below satisfy it by name.
pub trait TypeRewriteRule: std::fmt::Debug + Send + Sync {
    /// Return `Some(replacement)` when the rule applies, `None` to leave the type unchanged.
    fn rewrite(&self, dt: &DataType) -> Option<DataType>;
}

/// Rewrites `DataType::Dictionary(_, value_type)` → `value_type` (recursively).
///
/// Used by accelerators (`DuckDB`, `SQLite`, Turso) that do not support Arrow Dictionary encoding.
#[derive(Debug)]
pub struct DictionaryUnwrap;
impl TypeRewriteRule for DictionaryUnwrap {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Dictionary(_, value_type) => Some(value_type.as_ref().clone()),
            _ => None,
        }
    }
}

/// Rewrites `DataType::Map(entries, _)` so the `entries` field is non-nullable.
///
/// The Arrow specification requires it, and `MapArray::try_new` refuses a nullable
/// `entries` field outright — so a map declared this way decodes from IPC without
/// complaint and then fails in the first kernel that rebuilds it, reporting
/// `MapArray entries cannot contain nulls` even when no null is involved. Nullability is
/// part of the type and not of any buffer, so the correction is metadata-only — pair it with
/// [`relabel_array_data`] to carry the arrays over unchanged.
///
/// This is a source-conformance rule, not an engine-capability one: do not add it to an
/// accelerator's rule list by analogy with the rules above it.
#[derive(Debug)]
pub struct MapEntriesNonNullable;
impl TypeRewriteRule for MapEntriesNonNullable {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Map(entries, sorted) if entries.is_nullable() => Some(DataType::Map(
                Arc::new(entries.as_ref().clone().with_nullable(false)),
                *sorted,
            )),
            _ => None,
        }
    }
}

/// Rewrites `DataType::Null` → `DataType::Int32`.
///
/// `DuckDB` has no Null type and silently coerces it to INT32 when creating tables.
#[derive(Debug)]
pub struct NullToInt32;
impl TypeRewriteRule for NullToInt32 {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        matches!(dt, DataType::Null).then_some(DataType::Int32)
    }
}

/// Rewrites `DataType::Interval(YearMonth | DayTime)` → `DataType::Interval(MonthDayNano)`.
///
/// `DuckDB`'s native INTERVAL storage uses the `MonthDayNano` layout.
#[derive(Debug)]
pub struct IntervalToMonthDayNano;
impl TypeRewriteRule for IntervalToMonthDayNano {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Interval(IntervalUnit::YearMonth | IntervalUnit::DayTime) => {
                Some(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            _ => None,
        }
    }
}

/// Rewrites a timezone-aware `DataType::Timestamp(unit, Some(tz))` with a
/// non-microsecond `unit` → `DataType::Timestamp(Microsecond, Some(tz))`,
/// preserving the timezone. Timezone-naive timestamps (`tz = None`) are left
/// unchanged.
///
/// `DuckDB`'s `TIMESTAMP WITH TIME ZONE` (`TIMESTAMPTZ`) type is always
/// microsecond-precision — `DuckDB` has no nanosecond-with-timezone type. So a
/// column registered as `Timestamp(Nanosecond, "UTC")` (e.g. a Postgres
/// `timestamptz` source) is physically stored — and read back — as
/// `Timestamp(Microsecond, "UTC")`. When the registered schema keeps the
/// original non-µs unit, `DataFusion` plans against nanoseconds while `DuckDB`
/// returns microseconds and queries that sort or range-join on the column panic
/// with
/// `RowConverter column schema mismatch, expected Timestamp(ns, "UTC") got Timestamp(µs, "UTC")`.
/// Normalizing the unit to microsecond keeps the registered schema in lockstep
/// with what `DuckDB` stores and returns.
///
/// Timezone-naive timestamps are deliberately excluded: `DuckDB` has a native
/// nanosecond `TIMESTAMP_NS` type and preserves the precision of `TIMESTAMP`
/// columns without a zone (the runtime's internal `_fetched_at` caching column
/// relies on this), so normalizing them would instead *introduce* a mismatch.
#[derive(Debug)]
pub struct TimestampTzToMicrosecond;
impl TypeRewriteRule for TimestampTzToMicrosecond {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Timestamp(unit, Some(tz)) if *unit != TimeUnit::Microsecond => Some(
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(tz))),
            ),
            _ => None,
        }
    }
}

/// Rewrites `DataType::Float16` → `DataType::Float32`.
///
/// Vortex has no half-precision float, so Cayenne stores `Float16` columns widened
/// to `Float32`.
#[derive(Debug)]
pub struct Float16ToFloat32;
impl TypeRewriteRule for Float16ToFloat32 {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        matches!(dt, DataType::Float16).then_some(DataType::Float32)
    }
}

/// Rewrites any non-microsecond `DataType::Timestamp(unit, tz)` →
/// `DataType::Timestamp(Microsecond, tz)`, preserving the timezone (including its
/// absence).
///
/// Cayenne applied this at table creation before it stored the source's own unit, so
/// a table created by one of those builds holds microseconds for the life of that
/// table. The rule is retained for the acceleration write path, which must recognize
/// that stored type as one the engine produced rather than as the acceleration having
/// fallen behind its source — see `cayenne::CAYENNE_TYPE_REWRITE_RULES`. It is not a
/// creation rewrite: a table created now keeps the unit its source reports.
///
/// This differs from [`TimestampTzToMicrosecond`] in covering timezone-naive
/// timestamps too, because the rewrite it describes did.
#[derive(Debug)]
pub struct TimestampToMicrosecond;
impl TypeRewriteRule for TimestampToMicrosecond {
    fn rewrite(&self, dt: &DataType) -> Option<DataType> {
        match dt {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                Some(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))
            }
            _ => None,
        }
    }
}

/// A rule list an acceleration engine declares once and hands out by reference.
pub type TypeRewriteRules = &'static [&'static dyn TypeRewriteRule];

/// Applies `rules` to every type in `schema`, descending post-order into nested container types.
///
/// Returns a new schema with the same metadata; fields whose types are unchanged are cloned as-is.
#[must_use]
pub fn apply_rules(schema: &Schema, rules: &[&dyn TypeRewriteRule]) -> Schema {
    // An unchanged field is shared by refcount rather than deep-copied: most fields match
    // no rule, and cloning one copies its name and metadata map.
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| {
            let new_type = rewrite_data_type(f.data_type(), rules);
            if &new_type == f.data_type() {
                Arc::clone(f)
            } else {
                Arc::new(f.as_ref().clone().with_data_type(new_type))
            }
        })
        .collect();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// Replaces Arrow `Dictionary`-encoded fields with the dictionary's value type.
///
/// Data accelerators such as `DuckDB` and `SQLite` do not natively support Arrow
/// Dictionary types.  By unpacking the dictionary encoding at the schema level
/// (and later casting the data via `arrow_cast::cast`), the downstream
/// accelerator receives only primitive types it can handle.
///
/// For example, `Dictionary(Int32, Utf8)` is normalised to `Utf8`.
#[must_use]
pub fn normalize_dictionary_types(schema: &Schema) -> Schema {
    apply_rules(schema, &[&DictionaryUnwrap])
}

/// Post-order recursive type rewriter: children are rewritten before the parent.
///
/// Public so a caller holding one [`DataType`] can ask what the rules make of it without
/// building a whole [`Schema`] to hold it.
#[must_use]
pub fn rewrite_data_type(dt: &DataType, rules: &[&dyn TypeRewriteRule]) -> DataType {
    let dt = match dt {
        DataType::Dictionary(key_type, value_type) => {
            let inner = rewrite_data_type(value_type.as_ref(), rules);
            DataType::Dictionary(key_type.clone(), Box::new(inner))
        }
        DataType::List(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::List(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeList(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::LargeList(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::FixedSizeList(field, size) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::FixedSizeList(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *size,
            )
        }
        DataType::ListView(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::ListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::LargeListView(field) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::LargeListView(Arc::new(field.as_ref().clone().with_data_type(inner)))
        }
        DataType::Map(field, sorted) => {
            let inner = rewrite_data_type(field.data_type(), rules);
            DataType::Map(
                Arc::new(field.as_ref().clone().with_data_type(inner)),
                *sorted,
            )
        }
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| {
                    let inner = rewrite_data_type(f.data_type(), rules);
                    f.as_ref().clone().with_data_type(inner)
                })
                .collect();
            DataType::Struct(new_fields.into())
        }
        DataType::Union(fields, mode) => DataType::Union(
            fields
                .iter()
                .map(|(type_id, f)| {
                    let inner = rewrite_data_type(f.data_type(), rules);
                    (type_id, Arc::new(f.as_ref().clone().with_data_type(inner)))
                })
                .collect(),
            *mode,
        ),
        DataType::RunEndEncoded(run_ends, values) => {
            let inner = rewrite_data_type(values.data_type(), rules);
            DataType::RunEndEncoded(
                Arc::clone(run_ends),
                Arc::new(values.as_ref().clone().with_data_type(inner)),
            )
        }
        other => other.clone(),
    };
    for rule in rules {
        if let Some(rewritten) = rule.rewrite(&dt) {
            return rewritten;
        }
    }
    dt
}

/// Recursively rebuilds `data` so its (possibly nested) [`DataType`] becomes `target_type`,
/// without changing any value, buffer or null mask.
///
/// This is the array-side counterpart to [`rewrite_data_type`]: that decides what a type
/// should become, this carries the arrays across to it. Only the parts of a type that hold no
/// data may differ — field names and nested nullability flags — and children are relabelled
/// positionally, so `target_type` has to describe the layout `data` already has.
///
/// Positional pairing plus permitted renames has a consequence worth stating: a target whose
/// same-typed sibling fields are *reordered* is indistinguishable from one that renames each of
/// them, so it is accepted and each child keeps the values it already had under the other field's
/// name. Callers must supply a target in the source's field order. This cannot be checked here —
/// renaming is the Delta column-mapping caller's entire purpose, since its physical field names are
/// opaque column-mapping ids — so it needs a column identity this function is not given (#13434).
///
/// The result shares `data`'s buffers, but the call is not free: each rebuilt level goes back
/// through [`ArrayData`] validation, which is `O(rows)` in its offsets and `O(bytes)` for a
/// `Utf8` leaf. Only the levels whose type actually changes are rebuilt.
///
/// # Errors
///
/// Returns an `ArrowError` when `target_type` changes what the buffers mean — a different unit,
/// timezone, signedness, width, precision or scale, an extension type, or a nested field's
/// `dict_is_ordered` all read the same bytes as different values, so they are refused rather than
/// reinterpreted. Field names, nullability flags, and field metadata outside the two
/// `ARROW:extension:*` keys are permitted, since none of them changes how a value is read — except
/// that a nested field going nullable → non-nullable is refused unless the child it describes
/// provably holds no logical null, since a schema that understates its nulls is read as fact by
/// the planner. Also returns an `ArrowError` when `target_type` does not describe the layout
/// `data` holds.
pub fn relabel_array_data(
    data: ArrayData,
    target_type: &DataType,
) -> Result<ArrayData, ArrowError> {
    // `build` below only validates buffer *shape*, so every same-layout target it accepts would
    // be rebuilt — `Int32` under a `UInt32` label turns -1 into 4294967295 without an error.
    // Meaning is checked here instead, once for the whole tree rather than at each level.
    //
    // Checked before the equality short-circuit rather than after it: `Field`'s `PartialEq` leaves
    // `dict_is_ordered` out, so a target differing from `data`'s type in only that flag compares
    // equal and would be waved through as nothing-to-do — handing back unordered values to a caller
    // that asked for, and will go on to describe, an ordered dictionary. The walk is `O(type tree)`
    // against the rebuild's `O(rows)`, so paying it on the identical case costs nothing that shows.
    ensure_relabel_is_metadata_only(data.data_type(), target_type)?;

    if data.data_type() == target_type {
        return Ok(data);
    }

    // Nullability is the one admitted difference the guard above cannot settle on its own: whether
    // narrowing a field to non-nullable is a correction or a lie is a property of the values, not
    // of the types. Checked only past the short-circuit — `Field`'s `PartialEq` does compare
    // `nullable`, so equal types narrow nothing — and only where a narrowing is actually found, so
    // the common relabel pays a walk of the type tree and no null counting at all.
    ensure_narrowing_is_backed_by_the_data(&data, target_type)?;

    relabel_validated_array_data(data, target_type)
}

/// Rejects a `target_type` that declares a nested field non-nullable while the child it describes
/// still holds nulls.
///
/// Widening (non-nullable → nullable) is always sound and is left alone. Narrowing is the direction
/// that can lie: a field declared non-nullable over a child that holds nulls is published as fact,
/// and `DataFusion` derives expression nullability from those fields — it constant-folds `IS NULL`
/// over a non-nullable column to `false`, so rows that really are null are filtered out. That is a
/// wrong-results shape rather than a crash, which is why it is refused here rather than left to
/// surface downstream.
///
/// `ArrayData::build` catches part of this and cannot be relied on for the rest. Its
/// `validate_nulls` checks non-nullable children only for `Struct`, `List`, `LargeList`, `Map` and
/// `FixedSizeList`; `Union`, `ListView`, `LargeListView` and `RunEndEncoded` are not in that match
/// at all, and every arm reads the child's *physical* null buffer, so a logical null a
/// `RunEndEncoded` or `Dictionary` child states one level further down is invisible to it. Measured
/// against arrow-rs: narrowing over a run-end-encoded `values` child, over a union child, and over
/// a dictionary whose values hold a null the keys select are all accepted by `build`. This walk
/// covers every shape uniformly and reports the shapes `build` does catch with a message that names
/// the two fields and what the narrowing would cost, which its own does not.
///
/// [`MapEntriesNonNullable`] is not exempted from this and does not need to be. It narrows a
/// `Map`'s `entries` field because the Arrow specification requires that field to be non-nullable
/// and a well-formed map's entries genuinely carry no nulls, so it satisfies the proof the same way
/// any other caller must. A map whose entries really do hold nulls is malformed, and refusing it
/// here reports that at the relabel instead of leaving `MapArray::try_new` to fail later.
fn ensure_narrowing_is_backed_by_the_data(
    data: &ArrayData,
    target_type: &DataType,
) -> Result<(), ArrowError> {
    // Only reached once `ensure_relabel_is_metadata_only` has admitted the pair, so the two agree
    // on every child-bearing shape and this can pair fields with children positionally. `zip`
    // truncates rather than indexing: a child count that still disagrees is a layout disagreement,
    // and `build` refuses it with a better message than a panic here would give.
    for (index, (source_field, target_field)) in relabel_field_pairs(data.data_type(), target_type)
        .into_iter()
        .enumerate()
    {
        let Some(child) = data.child_data().get(index) else {
            continue;
        };

        if source_field.is_nullable()
            && !target_field.is_nullable()
            && narrowed_child_holds_a_reachable_null(data, index)
        {
            return Err(relabel_narrows_a_field_that_holds_nulls(
                source_field,
                target_field,
            ));
        }

        ensure_narrowing_is_backed_by_the_data(child, target_field.data_type())?;
    }

    // A `Dictionary`'s value type is not carried on a `Field`, so it declares no nullability of its
    // own and contributes no pair above — but a narrowing can still sit inside it.
    if let (DataType::Dictionary(_, source_value), DataType::Dictionary(_, target_value)) =
        (data.data_type(), target_type)
        && let Some(values) = data.child_data().first()
    {
        debug_assert_eq!(values.data_type(), source_value.as_ref());
        ensure_narrowing_is_backed_by_the_data(values, target_value)?;
    }

    Ok(())
}

/// The nested fields of `source` paired with `target`'s, in `ArrayData::child_data` order.
///
/// Mirrors [`target_child_types`], which is what lets the caller read each pair's position as the
/// index of the child it describes — every shape here lays its children out in field order, so a
/// single-field type owns child 0 and `Struct`/`Union`/`RunEndEncoded` pair field *i* with child
/// *i*. Yields the `Field`s rather than their types because nullability lives on the field.
/// `Dictionary` is absent for the same reason: its value type carries no field, so its child is
/// walked by the caller instead.
fn relabel_field_pairs<'a>(
    source: &'a DataType,
    target: &'a DataType,
) -> Vec<(&'a Field, &'a Field)> {
    let pair = |source_field: &'a FieldRef, target_field: &'a FieldRef| {
        vec![(source_field.as_ref(), target_field.as_ref())]
    };

    match (source, target) {
        (DataType::List(source_item), DataType::List(target_item))
        | (DataType::LargeList(source_item), DataType::LargeList(target_item))
        | (DataType::ListView(source_item), DataType::ListView(target_item))
        | (DataType::LargeListView(source_item), DataType::LargeListView(target_item))
        | (DataType::FixedSizeList(source_item, _), DataType::FixedSizeList(target_item, _))
        | (DataType::Map(source_item, _), DataType::Map(target_item, _)) => {
            pair(source_item, target_item)
        }
        (DataType::Struct(source_fields), DataType::Struct(target_fields)) => source_fields
            .iter()
            .map(Arc::as_ref)
            .zip(target_fields.iter().map(Arc::as_ref))
            .collect(),
        (DataType::Union(source_fields, _), DataType::Union(target_fields, _)) => source_fields
            .iter()
            .map(|(_, f)| f.as_ref())
            .zip(target_fields.iter().map(|(_, f)| f.as_ref()))
            .collect(),
        (
            DataType::RunEndEncoded(source_run_ends, source_values),
            DataType::RunEndEncoded(target_run_ends, target_values),
        ) => vec![
            (source_run_ends.as_ref(), target_run_ends.as_ref()),
            (source_values.as_ref(), target_values.as_ref()),
        ],
        _ => Vec::new(),
    }
}

/// Whether the child at `index` holds a null a reader can actually reach, given the parent holding
/// it.
///
/// Reachability is the whole difficulty, and `record_batch.rs` already records why: Arrow requires a
/// non-nullable struct child's nulls to be a *subset of its parent's* rather than absent, so a
/// masked null is legal, and the rule differs again for a list-like parent whose offsets decide
/// which child slots are addressed. A second, stricter transcription of those rules would refuse
/// arrays Arrow considers valid. So this does not invent a rule: it applies **Arrow's own
/// exemption** for each parent shape, and differs from `validate_nulls` in exactly two places where
/// that function cannot reach the answer.
///
/// 1. It reads the child's **logical** nulls rather than its physical null buffer, which is what
///    lets a `RunEndEncoded` or `Dictionary` child stop hiding nulls stated a level below.
/// 2. It covers `Union`, which `validate_nulls` omits — and covers it by *selection*, because a
///    sparse union pads every child at every row another variant is selected, so those children
///    routinely carry nulls no reader can reach.
///
/// Every other shape keeps Arrow's exemption unchanged, including the strict one it applies to
/// list-like parents. That is deliberate: matching `validate_nulls` means this can only ever refuse
/// what Arrow would refuse anyway, never more. `ListView` and `LargeListView` are absent from
/// `validate_nulls` and stay unchecked here for the same reason — deciding them needs the offset
/// reachability rule this is careful not to re-derive.
fn narrowed_child_holds_a_reachable_null(parent: &ArrayData, index: usize) -> bool {
    let Some(child) = parent.child_data().get(index) else {
        return false;
    };
    let Some(nulls) = logical_nulls_of(child) else {
        return false;
    };

    match parent.data_type() {
        // Arrow's `Struct` arm: a child null sitting under a null parent slot is unreachable.
        DataType::Struct(_) => !parent
            .nulls()
            .is_some_and(|reachable_only_where_parent_is_null| {
                reachable_only_where_parent_is_null.contains(&nulls)
            }),
        // Arrow's `FixedSizeList` arm: the parent's mask, expanded over the fixed element count.
        DataType::FixedSizeList(_, len) => {
            let element_len = usize::try_from(*len).unwrap_or(0);
            !parent
                .nulls()
                .is_some_and(|parent_nulls| parent_nulls.expand(element_len).contains(&nulls))
        }
        DataType::Union(fields, mode) => {
            union_variant_selects_a_null(parent, fields, *mode, index, &nulls)
        }
        DataType::ListView(_) | DataType::LargeListView(_) => false,
        // `List`, `LargeList` and `Map` take no exemption in `validate_nulls`, so neither here.
        _ => true,
    }
}

/// The nulls of `child` as a reader sees them, rather than as its own null buffer states them.
///
/// Only two encodings differ, and they are the reason this exists: a `RunEndEncoded` array has no
/// null buffer of its own — a null run is a null in its `values` child, expanded over the run — and
/// a `Dictionary` can hold nulls in its values, reachable only through the keys that select them.
/// Both are answered by Arrow's own [`Array::logical_nulls`] rather than by re-deriving them.
///
/// Every other type reports its physical nulls, which for them *are* the logical ones. Taking that
/// branch without building an array also keeps this away from `make_array`, which would panic on
/// the very shape [`MapEntriesNonNullable`] exists to correct: `MapArray::try_new` refuses a
/// nullable `entries` field, so a `Map` child awaiting that correction cannot be materialized here.
fn logical_nulls_of(child: &ArrayData) -> Option<NullBuffer> {
    match child.data_type() {
        DataType::RunEndEncoded(..) | DataType::Dictionary(..) => {
            make_array(child.clone()).logical_nulls()
        }
        _ => child.nulls().cloned(),
    }
}

/// Whether the union variant at `index` is selected at any row where its child is null.
///
/// A union's children are not addressed row-for-row by the parent: a **sparse** union gives every
/// child the parent's full length and selects one per row, so the others are padding; a **dense**
/// union addresses its child through the offsets buffer. Either way a null the type ids never
/// select is unreachable, and refusing it would reject the ordinary shape of a sparse union.
fn union_variant_selects_a_null(
    parent: &ArrayData,
    fields: &UnionFields,
    mode: UnionMode,
    index: usize,
    child_nulls: &NullBuffer,
) -> bool {
    let Some((variant_type_id, _)) = fields.iter().nth(index) else {
        return false;
    };
    // `ArrayData::buffer` indexes without checking, and this walk is reached from a public entry
    // point, so the count is confirmed first rather than trusted: a union always carries its type
    // ids, and a dense one its offsets, but a panic here would be a crash on malformed input where
    // the whole function's job is to answer a question about it.
    let dense_offsets = match mode {
        UnionMode::Sparse => None,
        UnionMode::Dense if parent.buffers().len() > 1 => Some(parent.buffer::<i32>(1)),
        UnionMode::Dense => return false,
    };
    if parent.buffers().is_empty() {
        return false;
    }
    let type_ids = parent.buffer::<i8>(0);

    let selected_child_row = |row: usize| -> Option<usize> {
        match dense_offsets {
            // A sparse union gives the child the parent's length, so the row indexes it directly.
            None => Some(row),
            Some(offsets) => usize::try_from(*offsets.get(row)?).ok(),
        }
    };

    (0..parent.len()).any(|row| {
        type_ids.get(row) == Some(&variant_type_id)
            && selected_child_row(row).is_some_and(|child_row| {
                child_row < child_nulls.len() && child_nulls.is_null(child_row)
            })
    })
}

/// The error [`ensure_narrowing_is_backed_by_the_data`] reports for an unsupported narrowing.
fn relabel_narrows_a_field_that_holds_nulls(source: &Field, target: &Field) -> ArrowError {
    // Field names come from the schema, so escape them: an embedded newline would break the
    // one-line contract this error is read under, and split one log record into two.
    ArrowError::InvalidArgumentError(format!(
        "Cannot relabel the Arrow field '{}' as '{}': the target declares it non-nullable while the \
         column still holds nulls, so a query using `IS NULL` or `IS NOT NULL` on it would be \
         planned against a schema that contradicts the data and drop rows that really are null. \
         Declare the field nullable, or remove the nulls before relabelling.",
        source.name().escape_debug(),
        target.name().escape_debug(),
    ))
}

/// The rebuild half of [`relabel_array_data`], called once `target_type` is known to differ from
/// `data`'s type only in field names, nullability flags, and field metadata outside the two
/// `ARROW:extension:*` keys — the three differences
/// [`ensure_relabel_is_metadata_only`] admits, and so the three this may be handed.
fn relabel_validated_array_data(
    data: ArrayData,
    target_type: &DataType,
) -> Result<ArrayData, ArrowError> {
    // Redundant for the entry call, which has already compared these, but load-bearing for the
    // recursion below: a sibling child often already carries its target type.
    if data.data_type() == target_type {
        return Ok(data);
    }

    let targets = target_child_types(target_type);

    // Usually a flag changes at one level and every child below it is already correct, so the
    // child spine is carried over by move instead of cloned. A child count that disagrees with
    // the target is a layout disagreement, and `build` refuses it below rather than leaving a
    // rebuilt parent over children still carrying the old type.
    let children_change = targets.len() == data.child_data().len()
        && data
            .child_data()
            .iter()
            .zip(&targets)
            .any(|(child, target)| child.data_type() != *target);

    if !children_change {
        return data.into_builder().data_type(target_type.clone()).build();
    }

    let children = data
        .child_data()
        .iter()
        .zip(&targets)
        .map(|(child, target)| relabel_validated_array_data(child.clone(), target))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    data.into_builder()
        .data_type(target_type.clone())
        .child_data(children)
        .build()
}

/// Rejects a `target_type` that would change what `source`'s buffers mean.
///
/// [`relabel_array_data`] promises to carry values across unchanged, so only the parts of a type
/// that hold no data may differ: field names, nullability flags, and the field metadata that is
/// not an `ARROW:extension:*` key, at every level. Everything
/// a buffer's interpretation depends on — primitive width and signedness, timestamp/interval/
/// duration unit, timezone, decimal precision and scale, `FixedSizeList` size, `Union` mode and
/// type ids, `Dictionary` key type, `Map` `sorted` flag — has to match exactly.
///
/// The walk covers the same child-bearing types as [`target_child_types`]. Anything else is
/// compared whole, so a type this does not know about is refused rather than relabelled: a new
/// Arrow variant fails closed here instead of being reinterpreted.
/// `DataType::equals_datatype` is deliberately not used here: it requires nullability to match,
/// which this guard must allow to differ, and it ignores field metadata, so it would miss a changed
/// extension type. The two walks look interchangeable and are not.
fn ensure_relabel_is_metadata_only(source: &DataType, target: &DataType) -> Result<(), ArrowError> {
    match (source, target) {
        (DataType::List(source_item), DataType::List(target_item))
        | (DataType::LargeList(source_item), DataType::LargeList(target_item))
        | (DataType::ListView(source_item), DataType::ListView(target_item))
        | (DataType::LargeListView(source_item), DataType::LargeListView(target_item)) => {
            ensure_field_relabel_is_metadata_only(source_item, target_item)
        }
        (
            DataType::FixedSizeList(source_item, source_len),
            DataType::FixedSizeList(target_item, target_len),
        ) if source_len == target_len => {
            ensure_field_relabel_is_metadata_only(source_item, target_item)
        }
        (
            DataType::Map(source_entries, source_sorted),
            DataType::Map(target_entries, target_sorted),
        ) if source_sorted == target_sorted => {
            ensure_field_relabel_is_metadata_only(source_entries, target_entries)
        }
        (DataType::Struct(source_fields), DataType::Struct(target_fields))
            if source_fields.len() == target_fields.len() =>
        {
            for (source_field, target_field) in source_fields.iter().zip(target_fields) {
                ensure_field_relabel_is_metadata_only(source_field, target_field)?;
            }
            Ok(())
        }
        (
            DataType::Union(source_fields, source_mode),
            DataType::Union(target_fields, target_mode),
        ) if source_mode == target_mode && source_fields.len() == target_fields.len() => {
            for ((source_id, source_field), (target_id, target_field)) in
                source_fields.iter().zip(target_fields.iter())
            {
                if source_id != target_id {
                    return Err(relabel_changes_meaning(source, target));
                }
                ensure_field_relabel_is_metadata_only(source_field, target_field)?;
            }
            Ok(())
        }
        (
            DataType::RunEndEncoded(source_run_ends, source_values),
            DataType::RunEndEncoded(target_run_ends, target_values),
        ) => {
            // Run ends are a data buffer of their own, so their type is compared like any leaf.
            ensure_field_relabel_is_metadata_only(source_run_ends, target_run_ends)?;
            ensure_field_relabel_is_metadata_only(source_values, target_values)
        }
        (
            DataType::Dictionary(source_key, source_value),
            DataType::Dictionary(target_key, target_value),
        ) if source_key == target_key => {
            ensure_relabel_is_metadata_only(source_value, target_value)
        }
        _ if source == target => Ok(()),
        _ => Err(relabel_changes_meaning(source, target)),
    }
}

/// Compares one nested field pair: its extension type, then its data type.
///
/// A field carries more than the type the walk recurses into. An Arrow **extension type** lives in
/// field metadata (`ARROW:extension:name`, `ARROW:extension:metadata`) and is precisely a claim
/// about what identical storage buffers mean — a `Utf8` labelled `arrow.uuid` and a bare `Utf8`
/// have the same layout and different meaning — so it belongs to the part of a type that holds
/// data, and the target installs it wholesale when the level is rebuilt.
///
/// Only those two keys are compared. Other metadata (a Parquet field id, a comment) annotates a
/// field without changing how its values are read, and the Delta column-mapping caller relabels
/// across schemas whose fields differ in exactly that way — rejecting all metadata differences
/// would refuse a correct relabel to guard something that is not a reinterpretation.
///
/// A field's `dict_is_ordered` is checked here for the same reason and needs checking *here*
/// specifically: it claims the dictionary's values carry an order, which is a statement about what
/// the same key buffer means, exactly as `Map`'s `sorted` flag is. It lives on the field rather
/// than in `DataType::Dictionary`, and `Field`'s `PartialEq` leaves it out, so no comparison of
/// types or fields anywhere else in this walk can see it.
///
/// Its neighbour `dict_id` is `PartialEq`-invisible in the same way and is deliberately *not*
/// compared. It names the IPC dictionary batch a key buffer indexes into, so it would belong here
/// on meaning grounds, but nothing in this repository sets or preserves it — every field is built
/// through a constructor that leaves it `0` — and Arrow has deprecated the whole mechanism for
/// removal since 54.0.0. Comparing it would add a use of an API on its way out in order to refuse
/// a relabel no caller can construct.
fn ensure_field_relabel_is_metadata_only(source: &Field, target: &Field) -> Result<(), ArrowError> {
    // The data type is walked first so that the two field-level checks below report only on a
    // relabel that is otherwise metadata-only. Both arms refuse, so the order cannot change what
    // is admitted — only which cause is named, and the outer one is the misleading half:
    // `Dictionary<_, Utf8>` -> `Utf8` differs in `dict_is_ordered` (`Some(false)` vs `None`)
    // *because* it drops the dictionary encoding, and reporting it as a dictionary that merely
    // needs sorting sends the reader after the wrong change.
    ensure_relabel_is_metadata_only(source.data_type(), target.data_type())?;

    if source.dict_is_ordered() != target.dict_is_ordered() {
        return Err(relabel_changes_dictionary_order(source, target));
    }

    let extension_parts = [
        (
            EXTENSION_TYPE_NAME_KEY,
            source.extension_type_name(),
            target.extension_type_name(),
        ),
        (
            EXTENSION_TYPE_METADATA_KEY,
            source.extension_type_metadata(),
            target.extension_type_metadata(),
        ),
    ];
    for (key, source_value, target_value) in extension_parts {
        if source_value != target_value {
            return Err(relabel_changes_extension_type(
                source,
                target,
                key,
                source_value,
                target_value,
            ));
        }
    }
    Ok(())
}

/// The error [`ensure_field_relabel_is_metadata_only`] reports for a changed `dict_is_ordered`.
fn relabel_changes_dictionary_order(source: &Field, target: &Field) -> ArrowError {
    // `dict_is_ordered` reads as `None` for a field that is not a dictionary at all, which the
    // data-type walk refuses on its own — spell it rather than printing an `Option`, so the one
    // line an operator sees never asks them to read Rust.
    let ordered = |field: &Field| match field.dict_is_ordered() {
        Some(true) => "ordered",
        Some(false) => "unordered",
        None => "not a dictionary",
    };

    // Field names come from the schema, so escape them: an embedded newline would break the
    // one-line contract this error is read under, and split one log record into two.
    ArrowError::InvalidArgumentError(format!(
        "Cannot relabel the Arrow field '{}' as '{}': `dict_is_ordered` differs ({} vs {}), which \
         republishes the same dictionary keys as carrying an order they do not. Sort the dictionary \
         values instead of relabelling them.",
        source.name().escape_debug(),
        target.name().escape_debug(),
        ordered(source),
        ordered(target),
    ))
}

/// The error [`ensure_field_relabel_is_metadata_only`] reports for an extension-type change.
fn relabel_changes_extension_type(
    source: &Field,
    target: &Field,
    key: &str,
    source_value: Option<&str>,
    target_value: Option<&str>,
) -> ArrowError {
    // Field names, the metadata key, and the extension values all come from the schema, so
    // escape them: an embedded newline would break the one-line contract this error is read
    // under, and split one log record into two.
    ArrowError::InvalidArgumentError(format!(
        "Cannot relabel the Arrow field '{}' as '{}': `{}` differs ({} vs {}), which republishes \
         the same values as a different extension type. Convert the values instead of relabelling \
         them.",
        source.name().escape_debug(),
        target.name().escape_debug(),
        key.escape_debug(),
        source_value.unwrap_or("unset").escape_debug(),
        target_value.unwrap_or("unset").escape_debug(),
    ))
}

/// The error [`ensure_relabel_is_metadata_only`] reports, naming the pair that disagrees.
///
/// Separate from the shape error `ArrayData::build` raises: the target here fits the buffers, and
/// the complaint is that it makes them mean something else.
fn relabel_changes_meaning(source: &DataType, target: &DataType) -> ArrowError {
    // A rendered `DataType` embeds the names of every field nested under it, and those come
    // from the schema, so escape the rendering rather than the type: see
    // `relabel_changes_extension_type` for the same reason.
    ArrowError::InvalidArgumentError(format!(
        "Cannot relabel an Arrow array of type {} as {}: that changes how the values \
         are read, not only field names and nullability. Convert the values instead of relabelling \
         them — see `rewrite_data_type` for the rules that change a type's layout.",
        source.to_string().escape_debug(),
        target.to_string().escape_debug(),
    ))
}

/// The types `target_type`'s children must carry, in the order [`ArrayData`] holds them.
///
/// This mirrors `ArrayData`'s own `validate_child_data`, and it has to cover every
/// child-bearing type [`rewrite_data_type`] descends into: a type this misses is one whose
/// parent gets rebuilt while its children keep the old type, which `build` then rejects.
fn target_child_types(target_type: &DataType) -> Vec<&DataType> {
    match target_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => vec![field.data_type()],
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type()).collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, f)| f.data_type()).collect(),
        DataType::RunEndEncoded(run_ends, values) => {
            vec![run_ends.data_type(), values.data_type()]
        }
        DataType::Dictionary(_, value_type) => vec![value_type.as_ref()],
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, ListArray, RunArray, StringArray,
        StructArray, UnionArray,
    };
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use arrow::datatypes::Int32Type;
    use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema, UnionFields, UnionMode};

    /// A `Map` whose `entries` field is nullable, built the way IPC decode delivers it —
    /// `MapArray::try_new` refuses this shape outright, which is the defect
    /// [`MapEntriesNonNullable`] corrects.
    fn map_with_nullable_entries() -> (ArrayData, DataType) {
        let entries_fields = Fields::from(vec![
            Field::new("keys", DataType::Int32, false),
            Field::new("values", DataType::Int32, true),
        ]);
        let entries = ArrayData::builder(DataType::Struct(entries_fields.clone()))
            .len(2)
            .add_child_data(Int32Array::from(vec![1, 2]).to_data())
            .add_child_data(Int32Array::from(vec![10, 20]).to_data())
            .build()
            .expect("the entries struct is well formed");
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries_fields.clone()),
                true,
            )),
            false,
        );
        let map = ArrayData::builder(map_type)
            .len(1)
            .add_buffer(Buffer::from_slice_ref([0i32, 2]))
            .add_child_data(entries)
            .build()
            .expect("a map with nullable entries decodes even though Arrow forbids it");
        let target = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entries_fields),
                false,
            )),
            false,
        );
        (map, target)
    }

    #[test]
    fn relabel_refuses_a_signedness_flip() {
        let data = Int32Array::from(vec![-1, 2]).to_data();
        let err = relabel_array_data(data, &DataType::UInt32).expect_err(
            "relabelling Int32 as UInt32 must be refused: the buffers fit, so it would \
             republish -1 as 4294967295",
        );
        let message = err.to_string();
        assert!(
            message.contains("Int32") && message.contains("UInt32"),
            "the error must name both types, got: {message}"
        );
    }

    #[test]
    fn relabel_refuses_a_timestamp_unit_change() {
        let data = ArrayData::builder(DataType::Timestamp(TimeUnit::Second, None))
            .len(1)
            .add_buffer(Buffer::from_slice_ref([1_i64]))
            .build()
            .expect("a one-element second-resolution timestamp is well formed");
        let err = relabel_array_data(data, &DataType::Timestamp(TimeUnit::Nanosecond, None))
            .expect_err(
                "relabelling Second as Nanosecond must be refused: it would reread 1970-01-01 \
                 00:00:01 as 1970-01-01 00:00:00.000000001",
            );
        assert!(
            err.to_string().contains("Timestamp(ns)"),
            "the error must name the target unit, got: {err}"
        );
    }

    #[test]
    fn relabel_refuses_a_value_change_at_depth() {
        let values = Int32Array::from(vec![-1, 2]);
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(values),
            None,
        );
        let target = DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));
        let err = relabel_array_data(list.to_data(), &target).expect_err(
            "a signedness flip on the list item must be refused at depth, not only at the top \
             level",
        );
        assert!(
            err.to_string().contains("UInt32"),
            "the error must name the offending child type, got: {err}"
        );
    }

    #[test]
    fn relabel_refuses_a_reinterpretation_that_keeps_the_layout() {
        // Every pair here shares a buffer layout, so `ArrayData::build` accepts all of them; each
        // one reads those bytes as something else. One arm per part of a type that carries data.
        let cases = [
            (DataType::Int32, DataType::Date32),
            (DataType::Int64, DataType::Float64),
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                DataType::Interval(IntervalUnit::DayTime),
            ),
            (
                DataType::Duration(TimeUnit::Second),
                DataType::Duration(TimeUnit::Millisecond),
            ),
            (DataType::Decimal128(10, 2), DataType::Decimal128(10, 4)),
        ];
        for (source, target) in cases {
            assert!(
                ensure_relabel_is_metadata_only(&source, &target).is_err(),
                "{source} must not be relabellable as {target}"
            );
        }
    }

    #[test]
    fn relabel_refuses_a_nested_shape_change_that_holds_data() {
        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let cases = [
            (
                DataType::FixedSizeList(Arc::clone(&item), 2),
                DataType::FixedSizeList(Arc::clone(&item), 3),
            ),
            (
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
            ),
            (
                DataType::Map(Arc::clone(&item), false),
                DataType::Map(Arc::clone(&item), true),
            ),
            (
                DataType::List(Arc::clone(&item)),
                DataType::LargeList(Arc::clone(&item)),
            ),
            (
                DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)])),
                DataType::Struct(Fields::from(vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Int32, true),
                ])),
            ),
            (
                DataType::Union(
                    UnionFields::try_new(vec![0_i8], vec![Field::new("a", DataType::Int32, true)])
                        .expect("one type id for one field"),
                    UnionMode::Sparse,
                ),
                DataType::Union(
                    UnionFields::try_new(vec![0_i8], vec![Field::new("a", DataType::Int32, true)])
                        .expect("one type id for one field"),
                    UnionMode::Dense,
                ),
            ),
            (
                DataType::Union(
                    UnionFields::try_new(vec![0_i8], vec![Field::new("a", DataType::Int32, true)])
                        .expect("one type id for one field"),
                    UnionMode::Dense,
                ),
                DataType::Union(
                    UnionFields::try_new(vec![1_i8], vec![Field::new("a", DataType::Int32, true)])
                        .expect("one type id for one field"),
                    UnionMode::Dense,
                ),
            ),
            (
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int16, false)),
                    Arc::new(Field::new("values", DataType::Int32, true)),
                ),
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Int32, true)),
                ),
            ),
        ];
        for (source, target) in cases {
            assert!(
                ensure_relabel_is_metadata_only(&source, &target).is_err(),
                "{source} must not be relabellable as {target}"
            );
        }
    }

    /// A `Struct<a: Int32>` over `values`, plus the target that differs from its type only in
    /// narrowing `a` to non-nullable.
    fn struct_with_nullable_child(values: Vec<Option<i32>>) -> (ArrayData, DataType) {
        let source = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(values)) as ArrayRef],
            None,
        );
        let target = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)]));
        (source.to_data(), target)
    }

    /// `ArrayData::build` already refuses this shape — `validate_nulls` covers `Struct` — so what
    /// this pins is that the refusal now arrives from the guard, before the `O(rows)` rebuild, and
    /// says which fields and what it would have cost. Arrow's own line reads
    /// `non-nullable child of type Int32 contains nulls not present in parent Struct(..)`, which
    /// names neither field and gives the reader nothing to do about it.
    #[test]
    fn relabel_reports_a_struct_narrowing_that_arrow_would_refuse_less_usefully() {
        let (data, target) = struct_with_nullable_child(vec![Some(1), None]);

        let message = relabel_array_data(data, &target)
            .expect_err("narrowing a child that holds a null is refused")
            .to_string();

        assert!(
            message.contains("non-nullable") && message.contains("IS NULL"),
            "the guard's message must reach the caller ahead of arrow's, got: {message}"
        );
    }

    #[test]
    fn relabel_still_carries_a_narrowing_the_data_supports() {
        let (data, target) = struct_with_nullable_child(vec![Some(1), Some(2)]);
        let values_before = data.child_data()[0].clone();

        let relabelled = relabel_array_data(data, &target)
            .expect("a child with no null satisfies the proof, so the narrowing is admitted");

        assert_eq!(relabelled.data_type(), &target);
        assert_eq!(
            relabelled.child_data()[0].buffers(),
            values_before.buffers(),
            "the value buffer must be carried over untouched"
        );
    }

    #[test]
    fn relabel_still_carries_a_widening() {
        // Widening cannot lie about the data, so it is never the guard's business.
        let source_fields = Fields::from(vec![Field::new("a", DataType::Int32, false)]);
        let source = StructArray::new(
            source_fields,
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
            None,
        );
        let target = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, true)]));

        let relabelled = relabel_array_data(source.to_data(), &target)
            .expect("non-nullable -> nullable is always sound");

        assert_eq!(relabelled.data_type(), &target);
    }

    #[test]
    fn relabel_weighs_each_narrowing_against_its_own_child() {
        // A null anywhere in the tree must not refuse a narrowing elsewhere: `a` keeps its nulls
        // and stays nullable, while `b` is narrowed and holds none.
        let source_fields = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let source = StructArray::new(
            source_fields,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
            ],
            None,
        );
        let target = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
        ]));

        let relabelled = relabel_array_data(source.to_data(), &target)
            .expect("`b` holds no null, so narrowing it stands regardless of `a`");

        assert_eq!(relabelled.data_type(), &target);
    }

    /// Arrow permits a non-nullable struct child's nulls to be a subset of its parent's — the slot
    /// does not exist, so the value under it is unreachable. The guard takes Arrow's exemption
    /// rather than a stricter rule of its own, so this legal relabel must still pass.
    #[test]
    fn relabel_still_carries_a_narrowing_whose_nulls_the_parent_masks() {
        let source = StructArray::new(
            Fields::from(vec![Field::new("a", DataType::Int32, true)]),
            vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
            Some(NullBuffer::from(vec![true, false])),
        );
        let target = DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int32, false)]));

        let relabelled = relabel_array_data(source.to_data(), &target).expect(
            "the child's only null sits under a null parent slot, so no reader can reach it",
        );

        assert_eq!(relabelled.data_type(), &target);
    }

    /// A sparse union gives every child the parent's full length and selects one per row, so each
    /// child is padded at every row another variant is selected. Refusing on those nulls would
    /// reject the ordinary shape of a sparse union, which is why the union check is by selection.
    #[test]
    fn relabel_still_carries_a_union_narrowing_whose_null_no_type_id_selects() {
        let fields = |nullable| {
            UnionFields::try_new(
                vec![0_i8, 1],
                vec![
                    Field::new("a", DataType::Int32, nullable),
                    Field::new("b", DataType::Int32, true),
                ],
            )
            .expect("two type ids for two fields")
        };
        // Row 0 selects `a`, row 1 selects `b` — so `a`'s null at row 1 is padding.
        let source = UnionArray::try_new(
            fields(true),
            vec![0_i8, 1].into(),
            None,
            vec![
                Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(9), Some(9)])) as ArrayRef,
            ],
        )
        .expect("a sparse union over two variants");
        let target = DataType::Union(fields(false), UnionMode::Sparse);

        let relabelled = relabel_array_data(source.to_data(), &target).expect(
            "`a` is not selected at the row where it is null, so nothing can read that null",
        );

        assert_eq!(relabelled.data_type(), &target);
    }

    /// One of the three shapes `ArrayData::build` accepts outright — `RunEndEncoded` is absent from
    /// `validate_nulls`, and the nulls are a level below anything a physical null count would read.
    /// Measured: without the guard this relabel returns `Ok`.
    #[test]
    fn relabel_refuses_narrowing_a_run_end_encoded_values_child_whose_nulls_are_logical_only() {
        // The sharpest case: a run-end-encoded array has no null buffer of its own, so a check
        // that only read this level's null count would see zero and admit the narrowing.
        let run_ends = Int32Array::from(vec![2, 4]);
        let values = Int32Array::from(vec![Some(7), None]);
        let source = RunArray::try_new(&run_ends, &values)
            .expect("two runs over two values is a well-formed run-end-encoded array");
        assert_eq!(
            source.to_data().null_count(),
            0,
            "the parent carries no null bitmap — that is what makes this case sharp"
        );

        let target = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Int32, false)),
        );

        let err = relabel_array_data(source.to_data(), &target).expect_err(
            "the null run is a logical null of the whole array, so narrowing `values` must be \
             refused",
        );

        assert!(
            err.to_string().contains("values"),
            "the error must name the field it refused, got: {err}"
        );
    }

    /// The second shape `build` accepts: `List` *is* in `validate_nulls`, but the arm reads the
    /// child's physical null count, and a dictionary's is zero while a key selects a null value.
    /// Measured: without the guard this relabel returns `Ok`.
    #[test]
    fn relabel_refuses_narrowing_over_a_dictionary_whose_values_hold_nulls() {
        // A dictionary states its nulls one level below the keys, so the same top-level null count
        // reads as zero here too.
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        )
        .expect("two keys over two dictionary values");
        let dictionary_type = dictionary.data_type().clone();
        let source = ListArray::new(
            Arc::new(Field::new("item", dictionary_type.clone(), true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(dictionary),
            None,
        );
        let target = DataType::List(Arc::new(Field::new("item", dictionary_type, false)));

        let err = relabel_array_data(source.to_data(), &target).expect_err(
            "a key selecting a null dictionary value is a null of the column, so narrowing the \
             item must be refused",
        );

        assert!(
            err.to_string().contains("item"),
            "the error must name the field it refused, got: {err}"
        );
    }

    /// The third shape `build` accepts: `Union` is absent from `validate_nulls` altogether, so a
    /// narrowed variant field is published unchecked. Measured: without the guard this relabel
    /// returns `Ok`.
    #[test]
    fn relabel_refuses_narrowing_a_union_variant_that_holds_nulls() {
        let source_fields =
            UnionFields::try_new(vec![0_i8], vec![Field::new("a", DataType::Int32, true)])
                .expect("one type id for one field");
        let source = UnionArray::try_new(
            source_fields,
            vec![0_i8, 0].into(),
            None,
            vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
        )
        .expect("a sparse union over one variant");
        let target = DataType::Union(
            UnionFields::try_new(vec![0_i8], vec![Field::new("a", DataType::Int32, false)])
                .expect("one type id for one field"),
            UnionMode::Sparse,
        );

        let err = relabel_array_data(source.to_data(), &target)
            .expect_err("the variant holds a null, so narrowing it must be refused");

        assert!(
            err.to_string().contains("'a'"),
            "the error must name the field it refused, got: {err}"
        );
    }

    /// `MapEntriesNonNullable` is not exempt from the proof. `build` refuses this one too (`Map` is
    /// in `validate_nulls`), so what this pins is that a malformed map is reported as the
    /// narrowing it is rather than reaching `MapArray::try_new` later.
    #[test]
    fn relabel_refuses_the_map_entries_correction_when_the_entries_hold_nulls() {
        let (map, target) = map_with_nullable_entries();
        let entries = map.child_data()[0].clone();
        let nulled_entries = entries
            .into_builder()
            .null_bit_buffer(Some(Buffer::from([0b0000_0001])))
            .build()
            .expect("a struct may carry a null bitmap");
        let malformed = map
            .into_builder()
            .child_data(vec![nulled_entries])
            .build()
            .expect("the map shape is unchanged");

        let err = relabel_array_data(malformed, &target).expect_err(
            "entries holding a null cannot be republished as the non-nullable field Arrow requires",
        );

        assert!(
            err.to_string().contains("entries"),
            "the error must name the field it refused, got: {err}"
        );
    }

    #[test]
    fn relabel_narrowing_refusal_names_both_fields_and_the_repair() {
        let source_fields = Fields::from(vec![Field::new("physical", DataType::Int32, true)]);
        let source = StructArray::new(
            source_fields,
            vec![Arc::new(Int32Array::from(vec![None, Some(2)])) as ArrayRef],
            None,
        );
        let target = DataType::Struct(Fields::from(vec![Field::new(
            "logical",
            DataType::Int32,
            false,
        )]));

        let message = relabel_array_data(source.to_data(), &target)
            .expect_err("the child holds a null")
            .to_string();

        for expected in [
            "'physical'",
            "'logical'",
            "Declare the field nullable, or remove the nulls before relabelling.",
        ] {
            assert!(
                message.contains(expected),
                "the error must contain {expected}, got: {message}"
            );
        }
        assert!(
            !message.contains('\n'),
            "the error must stay on one line, got: {message}"
        );
    }

    #[test]
    fn relabel_still_carries_a_map_entries_nullability_correction() {
        let (map, target) = map_with_nullable_entries();
        let keys_before = map.child_data()[0].child_data()[0].clone();

        let relabelled = relabel_array_data(map, &target)
            .expect("flipping the entries nullability flag is metadata-only");

        assert_eq!(relabelled.data_type(), &target);
        assert_eq!(
            relabelled.child_data()[0].child_data()[0].buffers(),
            keys_before.buffers(),
            "the key buffer must be carried over untouched"
        );
    }

    #[test]
    fn relabel_still_carries_a_field_rename_at_depth() {
        let values = Int32Array::from(vec![-1, 2]);
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(values),
            None,
        );
        // A Delta column-mapping projection renames the child and may tighten its nullability.
        // The rename is metadata outright; the narrowing is admitted because this child holds no
        // null, which is the proof `ensure_narrowing_is_backed_by_the_data` asks every caller for.
        let target = DataType::List(Arc::new(Field::new("renamed", DataType::Int32, false)));

        let relabelled = relabel_array_data(list.to_data(), &target)
            .expect("renaming a child field and tightening its nullability is metadata-only");

        assert_eq!(relabelled.data_type(), &target);
        assert_eq!(
            relabelled.child_data()[0].buffers(),
            Int32Array::from(vec![-1, 2]).to_data().buffers(),
            "the value buffer must be carried over untouched"
        );
    }

    /// A `List<Int32>` whose item field carries `metadata`, plus a target that differs from it only
    /// in that metadata.
    fn list_with_item_metadata(
        key: &str,
        source_value: &str,
        target_value: &str,
    ) -> (ArrayData, DataType) {
        let item = |value: &str| {
            Arc::new(
                Field::new("item", DataType::Int32, true)
                    .with_metadata([(key.to_owned(), value.to_owned())].into()),
            )
        };
        let list = ListArray::new(
            item(source_value),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(Int32Array::from(vec![-1, 2])),
            None,
        );
        (list.to_data(), DataType::List(item(target_value)))
    }

    #[test]
    fn relabel_refuses_an_extension_type_change_at_depth() {
        // An extension type is a claim about what identical buffers mean, so swapping it is the
        // same class of reinterpretation as a signedness flip — and it lives in field metadata,
        // which the type walk alone never sees.
        for key in [EXTENSION_TYPE_NAME_KEY, EXTENSION_TYPE_METADATA_KEY] {
            let (data, target) = list_with_item_metadata(key, "one", "another");
            let err = relabel_array_data(data, &target).expect_err(
                "changing a nested field's extension type must be refused: the buffers are \
                 unchanged but the values now mean something else",
            );
            assert!(
                err.to_string().contains(key) && err.to_string().contains("item"),
                "the error must name the key and the field, got: {err}"
            );
        }
    }

    #[test]
    fn relabel_refuses_adding_an_extension_type_to_a_bare_field() {
        let item = Arc::new(Field::new("item", DataType::Int32, true));
        let list = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(Int32Array::from(vec![-1, 2])),
            None,
        );
        let target = DataType::List(Arc::new(
            Field::new("item", DataType::Int32, true).with_metadata(
                [(EXTENSION_TYPE_NAME_KEY.to_owned(), "arrow.uuid".to_owned())].into(),
            ),
        ));
        let err = relabel_array_data(list.to_data(), &target)
            .expect_err("promoting a bare field to an extension type must be refused");
        assert!(
            err.to_string().contains("unset"),
            "the error must say the source had no extension type, got: {err}"
        );
    }

    /// `dict_is_ordered` claims the dictionary's values carry an order, so republishing unordered
    /// keys under it is the same class of reinterpretation as `Map`'s `sorted` flag, which this
    /// guard already refuses. It hides better: the flag lives on `Field` rather than in
    /// `DataType::Dictionary`, and `Field`'s `PartialEq` leaves it out, so a target differing only
    /// in that flag compares *equal* to the source everywhere else in this module.
    #[test]
    fn relabel_refuses_a_dictionary_order_claim_at_depth() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let item =
            Arc::new(Field::new("item", dictionary.clone(), true).with_dict_is_ordered(false));
        let values = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec!["b", "a"])),
        )
        .expect("a dictionary over two values");
        let list = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(values),
            None,
        );

        // Renamed as well as reordered, so the rebuild is genuinely reached: a rename alone is
        // permitted, which is exactly the "another permitted field change" that carries the
        // ordered claim into the rebuilt level.
        let target = DataType::List(Arc::new(
            Field::new("renamed", dictionary.clone(), true).with_dict_is_ordered(true),
        ));
        let err = relabel_array_data(list.to_data(), &target)
            .expect_err("publishing an unordered dictionary as ordered must be refused");
        assert!(
            err.to_string().contains("dict_is_ordered"),
            "the error must name the flag that differs, got: {err}"
        );

        // The same claim with *no* other change: `Field: PartialEq` ignores `dict_is_ordered`, so
        // this target compares equal to the source's type and would be waved through by an
        // equality short-circuit that ran before the check.
        let ordered_only = DataType::List(Arc::new(
            Field::new("item", dictionary, true).with_dict_is_ordered(true),
        ));
        assert_eq!(
            list.data_type(),
            &ordered_only,
            "fixture check: these two types must compare equal, or this arm proves nothing about \
             the short-circuit"
        );
        let err = relabel_array_data(list.to_data(), &ordered_only).expect_err(
            "an ordered claim that changes nothing `PartialEq` can see must still be refused",
        );
        assert!(
            err.to_string().contains("dict_is_ordered"),
            "the short-circuit must not bypass the check, got: {err}"
        );
    }

    /// A relabel that drops the dictionary encoding differs in `dict_is_ordered` too, because the
    /// flag reads `None` for a field that is not a dictionary at all. Reported as a dictionary
    /// order claim, it sends the reader after a sort they cannot perform, so the data-type walk has
    /// to be the half that answers.
    #[test]
    fn dropping_a_dictionary_is_reported_as_the_type_change_it_is() {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let item = Arc::new(Field::new("item", dictionary, true).with_dict_is_ordered(false));
        let values = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec!["b", "a"])),
        )
        .expect("a dictionary over two values");
        let list = ListArray::new(
            Arc::clone(&item),
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(values),
            None,
        );

        // `Some(false)` vs `None`: the flags differ, but only as a consequence of the encoding
        // being removed.
        let undictionaried = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let err = relabel_array_data(list.to_data(), &undictionaried)
            .expect_err("removing dictionary encoding must be refused");
        let message = err.to_string();
        assert!(
            !message.contains("dict_is_ordered"),
            "removing the encoding must not be reported as an order claim, got: {message}"
        );
        assert!(
            message.contains("Dictionary") && message.contains("Utf8"),
            "the error must name the types it refuses to relabel between, got: {message}"
        );
    }

    #[test]
    fn relabel_still_carries_a_change_to_metadata_that_is_not_an_extension_type() {
        // The Delta column-mapping caller relabels between schemas whose fields carry different
        // descriptive metadata. That is not a reinterpretation, so it must pass — this is the arm
        // that keeps the extension-type guard from over-rejecting. Deliberately not a field id:
        // an id names *which column* this is, so a change there is not merely descriptive.
        let (data, target) = list_with_item_metadata("comment", "physical", "logical");
        let relabelled = relabel_array_data(data, &target)
            .expect("a `comment` is descriptive, not a claim about what the values mean");
        assert_eq!(relabelled.data_type(), &target);
        assert_eq!(
            relabelled.child_data()[0].buffers(),
            Int32Array::from(vec![-1, 2]).to_data().buffers(),
            "the value buffer must be carried over untouched"
        );
    }

    #[test]
    fn relabel_reports_one_line_naming_both_types() {
        let err = relabel_changes_meaning(&DataType::Int32, &DataType::UInt32).to_string();
        assert!(
            !err.contains('\n'),
            "an error message must stay on one line, got: {err}"
        );
        assert!(
            err.contains("type Int32 as UInt32"),
            "the message must read as prose naming both types, got: {err}"
        );
    }

    /// Every string these errors interpolate comes from the schema, and a schema is not ours to
    /// trust: a field name, an extension metadata key, or an extension value may hold a newline.
    /// The one-line contract has to hold for those too, not only for the leaf types that cannot
    /// carry one.
    #[test]
    fn a_newline_in_a_schema_string_cannot_break_an_error_across_lines() {
        let hostile = "item\nERROR: fabricated";

        let nested = |name: &str| DataType::List(Arc::new(Field::new(name, DataType::Int32, true)));
        let err = relabel_changes_meaning(&nested(hostile), &nested("item")).to_string();
        assert!(
            !err.contains('\n'),
            "a nested field name renders inside the type, so it must be escaped, got: {err}"
        );
        assert!(
            err.contains("ERROR: fabricated"),
            "escaping must keep the name readable rather than dropping it, got: {err}"
        );

        let err = relabel_changes_extension_type(
            &Field::new(hostile, DataType::Int32, true),
            &Field::new("item", DataType::Int32, true),
            "ARROW:extension:name\nkey",
            Some("arrow.json\nvalue"),
            None,
        )
        .to_string();
        assert!(
            !err.contains('\n'),
            "the field name, the metadata key and the value are all schema-controlled, got: {err}"
        );
    }

    #[test]
    fn float16_to_float32_top_level_and_nested() {
        let schema = Schema::new(vec![
            Field::new("half", DataType::Float16, true),
            Field::new("single", DataType::Float32, true),
            Field::new(
                "halves",
                DataType::List(Arc::new(Field::new("item", DataType::Float16, true))),
                true,
            ),
        ]);
        let result = apply_rules(&schema, &[&Float16ToFloat32]);
        assert_eq!(result.field(0).data_type(), &DataType::Float32);
        assert_eq!(result.field(1).data_type(), &DataType::Float32);
        assert_eq!(
            result.field(2).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        );
    }

    #[test]
    fn timestamp_to_microsecond_covers_naive_and_zoned() {
        let schema = Schema::new(vec![
            Field::new(
                "zoned",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("naive", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "already",
                DataType::Timestamp(TimeUnit::Microsecond, Some("+02:00".into())),
                true,
            ),
        ]);
        let result = apply_rules(&schema, &[&TimestampToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            result.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            result.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+02:00".into()))
        );
    }

    /// The distinguishing case against [`TimestampTzToMicrosecond`], which leaves a
    /// timezone-naive timestamp alone because `DuckDB` stores it at nanosecond
    /// precision.
    #[test]
    fn timestamp_tz_rule_leaves_naive_timestamps_where_the_all_rule_rewrites_them() {
        let schema = Schema::new(vec![Field::new(
            "naive",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        assert_eq!(
            apply_rules(&schema, &[&TimestampTzToMicrosecond])
                .field(0)
                .data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            apply_rules(&schema, &[&TimestampToMicrosecond])
                .field(0)
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn null_to_int32_top_level() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("untyped", DataType::Null, true),
            Field::new("name", DataType::Utf8, false),
        ]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result.field(0).data_type(), &DataType::Int64);
        assert_eq!(result.field(1).data_type(), &DataType::Int32);
        assert_eq!(result.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn null_to_int32_nested_in_list() {
        let schema = Schema::new(vec![Field::new(
            "col",
            DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
            false,
        )]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(result.field(0).data_type(), &expected);
    }

    #[test]
    fn null_to_int32_nested_in_struct() {
        let schema = Schema::new(vec![Field::new(
            "rec",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("val", DataType::Null, true),
                ]
                .into(),
            ),
            false,
        )]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        let expected = DataType::Struct(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("val", DataType::Int32, true),
            ]
            .into(),
        );
        assert_eq!(result.field(0).data_type(), &expected);
    }

    #[test]
    fn interval_year_month_normalized() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::YearMonth),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn interval_day_time_normalized() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn interval_month_day_nano_unchanged() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn timestamp_nanosecond_with_tz_normalized_to_microsecond() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]);
        let result = apply_rules(&schema, &[&TimestampTzToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            "timezone must be preserved while the unit is normalized to microsecond"
        );
    }

    #[test]
    fn timestamp_nanosecond_without_tz_unchanged() {
        // DuckDB has a native nanosecond TIMESTAMP_NS type and preserves the
        // precision of timezone-naive timestamps, so the no-timezone case must
        // NOT be normalized — doing so would introduce a mismatch.
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        let result = apply_rules(&schema, &[&TimestampTzToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            "timezone-naive timestamps must be left untouched"
        );
    }

    #[test]
    fn timestamp_tz_second_and_millisecond_normalized_to_microsecond() {
        let schema = Schema::new(vec![
            Field::new(
                "s",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ms",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+05:00".into())),
                true,
            ),
        ]);
        let result = apply_rules(&schema, &[&TimestampTzToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            result.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+05:00".into()))
        );
    }

    #[test]
    fn timestamp_microsecond_with_tz_unchanged() {
        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("id", DataType::Int64, false),
        ]);
        let result = apply_rules(&schema, &[&TimestampTzToMicrosecond]);
        assert_eq!(result, schema, "already-microsecond schema must be a no-op");
    }

    #[test]
    fn timestamp_nanosecond_nested_in_list_and_struct() {
        // tz-aware nested timestamp normalizes; tz-naive nested timestamp does not.
        let schema = Schema::new(vec![
            Field::new(
                "events",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                    true,
                ))),
                false,
            ),
            Field::new(
                "rec",
                DataType::Struct(
                    vec![Field::new(
                        "at",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    )]
                    .into(),
                ),
                false,
            ),
        ]);
        let result = apply_rules(&schema, &[&TimestampTzToMicrosecond]);
        assert_eq!(
            result.field(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ))),
            "tz-aware nested timestamp must normalize to microsecond"
        );
        assert_eq!(
            result.field(1).data_type(),
            &DataType::Struct(
                vec![Field::new(
                    "at",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                )]
                .into(),
            ),
            "tz-naive nested timestamp must be left untouched"
        );
    }

    #[test]
    fn multiple_rules_applied_in_order() {
        let schema = Schema::new(vec![
            Field::new("untyped", DataType::Null, true),
            Field::new(
                "dict",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new("dur", DataType::Interval(IntervalUnit::DayTime), true),
        ]);
        let result = apply_rules(
            &schema,
            &[&DictionaryUnwrap, &NullToInt32, &IntervalToMonthDayNano],
        );
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
        assert_eq!(result.field(1).data_type(), &DataType::Utf8);
        assert_eq!(
            result.field(2).data_type(),
            &DataType::Interval(IntervalUnit::MonthDayNano)
        );
    }

    #[test]
    fn apply_rules_preserves_metadata() {
        use std::collections::HashMap;
        let mut meta = HashMap::new();
        meta.insert("key".to_string(), "val".to_string());
        let mut fmeta = HashMap::new();
        fmeta.insert("fkey".to_string(), "fval".to_string());
        let schema = Schema::new_with_metadata(
            vec![Field::new("x", DataType::Null, true).with_metadata(fmeta.clone())],
            meta.clone(),
        );
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result.metadata(), &meta);
        assert_eq!(result.field(0).metadata(), &fmeta);
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn noop_when_no_rules_match() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let result = apply_rules(&schema, &[&NullToInt32]);
        assert_eq!(result, schema);
    }

    #[test]
    fn dictionary_inside_dict_unwrap_then_null_rule() {
        // Dictionary(Int32, Null) — after DictionaryUnwrap → Null — after NullToInt32 → Int32
        let schema = Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Null)),
            true,
        )]);
        let result = apply_rules(&schema, &[&DictionaryUnwrap, &NullToInt32]);
        assert_eq!(result.field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn interval_nested_in_union() {
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("dur", DataType::Interval(IntervalUnit::YearMonth), true),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        let result = apply_rules(&schema, &[&IntervalToMonthDayNano]);
        let expected_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("i32", DataType::Int32, false),
                Field::new("dur", DataType::Interval(IntervalUnit::MonthDayNano), true),
            ],
        )
        .expect("expected union fields");
        assert_eq!(
            result.field(0).data_type(),
            &DataType::Union(expected_fields, UnionMode::Dense)
        );
    }

    #[test]
    fn test_normalize_dictionary_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "status",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "category",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                false,
            ),
        ]);
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(normalized.field(0).data_type(), &DataType::Int64);
        assert_eq!(normalized.field(1).data_type(), &DataType::Utf8);
        assert!(normalized.field(1).is_nullable());
        assert_eq!(normalized.field(2).data_type(), &DataType::Utf8);
        assert_eq!(normalized.field(3).data_type(), &DataType::LargeUtf8);
        assert!(!normalized.field(3).is_nullable());
    }

    #[test]
    fn test_normalize_schema_without_dictionary_is_noop() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(schema, normalized);
    }

    #[test]
    fn test_normalize_preserves_metadata() {
        use std::collections::HashMap;
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());
        let mut field_metadata = HashMap::new();
        field_metadata.insert("custom_key".to_string(), "custom_value".to_string());
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "col",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    false,
                )
                .with_metadata(field_metadata.clone()),
            ],
            metadata.clone(),
        );
        let normalized = normalize_dictionary_types(&schema);
        assert_eq!(normalized.metadata(), &metadata);
        assert_eq!(normalized.field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            normalized.field(0).metadata(),
            &field_metadata,
            "field-level metadata must be preserved after normalization"
        );
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_struct() {
        let schema = Schema::new(vec![Field::new(
            "record",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new(
                        "status",
                        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                        true,
                    ),
                ]
                .into(),
            ),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::Struct(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("status", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_union() {
        let union_fields = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new(
                    "dict_val",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                ),
            ],
        )
        .expect("union fields");
        let schema = Schema::new(vec![Field::new(
            "mixed",
            DataType::Union(union_fields, UnionMode::Dense),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected_union = UnionFields::try_new(
            vec![0, 1],
            vec![
                Field::new("int_val", DataType::Int32, false),
                Field::new("dict_val", DataType::Utf8, true),
            ],
        )
        .expect("expected union fields");
        assert_eq!(
            normalized.field(0).data_type(),
            &DataType::Union(expected_union, UnionMode::Dense)
        );
    }

    #[test]
    fn test_normalize_nested_dictionary_in_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::ListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::ListView(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_large_list_view() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::LargeListView(Arc::new(Field::new(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
                true,
            ))),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected =
            DataType::LargeListView(Arc::new(Field::new("item", DataType::LargeUtf8, true)));
        assert_eq!(normalized.field(0).data_type(), &expected);
    }

    #[test]
    fn test_normalize_nested_dictionary_in_run_end_encoded() {
        let schema = Schema::new(vec![Field::new(
            "encoded",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new(
                    "values",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                    true,
                )),
            ),
            false,
        )]);
        let normalized = normalize_dictionary_types(&schema);
        let expected = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert_eq!(normalized.field(0).data_type(), &expected);
    }
}
