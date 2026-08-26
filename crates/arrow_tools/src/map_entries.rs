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

//! Brings foreign Arrow data in line with the Arrow `Map` layout rules.
//!
//! The Arrow specification requires a map's `entries` field to be non-nullable, and
//! `MapArray::try_new` enforces both halves of that: it rejects an `entries` field
//! declared nullable *and* an entries array that carries nulls. Nothing enforces it on
//! the way in — the IPC reader builds a `MapArray` straight from `ArrayData` without
//! either check — so a producer that declares `entries` nullable hands us a column that
//! decodes cleanly and then fails in whichever kernel first rebuilds it. Every such
//! failure reports the same message, `MapArray entries cannot contain nulls`, whether or
//! not a null is involved.
//!
//! [`MapEntriesNormalizer`] relabels the declaration (metadata only — no buffer is touched)
//! and refuses the one shape that cannot be relabelled without inventing an answer: entries
//! that actually contain nulls.

use std::sync::Arc;

use arrow::array::{Array, ArrayData, ArrayRef, RecordBatch, make_array};
use arrow::datatypes::{DataType, SchemaRef};
use snafu::prelude::*;

use crate::type_rewrite::{MapEntriesNonNullable, apply_rules, relabel_array_data};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "column '{column}' holds a MAP whose entries contain nulls, which the Arrow map layout has no way to represent"
    ))]
    MapEntriesContainNulls { column: String },

    #[snafu(display("column '{column}' could not be rebuilt: {source}"))]
    UnableToNormalizeColumn {
        column: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("the normalized columns do not fit schema '{schema}': {source}"))]
    UnableToRebuildRecordBatch {
        schema: String,
        source: arrow::error::ArrowError,
    },
}

/// What the batches of one Arrow stream need, resolved once from the stream's schema.
///
/// An Arrow IPC stream carries a single schema, so whether a map declaration has to be
/// relabelled — and what it becomes — is fixed for every batch. Deciding it per batch would
/// rebuild the same schema over and over and hand each batch a distinct `SchemaRef`.
pub struct MapEntriesNormalizer {
    /// The stream's own schema, as its producer declared it.
    declared: SchemaRef,
    /// The schema every batch is relabelled to, shared by all of them. `None` when the
    /// stream's own declarations already conform.
    target: Option<SchemaRef>,
    /// Whether any field holds a `Map` at all. When none does, no batch can carry entry
    /// nulls, so no column is ever inspected.
    holds_map: bool,
}

impl MapEntriesNormalizer {
    #[must_use]
    pub fn for_schema(schema: &SchemaRef) -> Self {
        let holds_map = schema
            .fields()
            .iter()
            .any(|field| contains(field.data_type(), &is_map));

        let target = holds_map
            .then(|| conforming_schema(schema))
            .filter(|target| !Arc::ptr_eq(target, schema));

        Self {
            declared: Arc::clone(schema),
            target,
            holds_map,
        }
    }

    /// The schema every batch this normalizer returns carries: the relabelled one when the
    /// stream's declarations needed correcting, otherwise the stream's own.
    ///
    /// A decode point that hands its batches on under a separately declared schema — an
    /// `ExecutionPlan`'s output schema, a `RecordBatchStreamAdapter`'s — must declare this one,
    /// or it describes its batches with a type they no longer carry.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        self.target.as_ref().unwrap_or(&self.declared)
    }

    /// Returns `batch` with every `Map` column — nested ones included — declaring its
    /// `entries` field non-nullable, as the Arrow specification requires.
    ///
    /// Nullability lives in the type rather than in any buffer, so this only relabels: the
    /// offsets, validity and child arrays are carried over by reference, and a column whose
    /// type is already right is passed through untouched.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MapEntriesContainNulls`] when an entries array carries nulls. That is
    /// the one shape relabelling cannot fix, and it is not recoverable by guessing: Arrow
    /// gives a null entry no meaning, so treating it as a null map and treating it as a pair
    /// to drop are both inventions, and each yields different rows.
    pub fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if !self.holds_map {
            return Ok(batch);
        }

        let Some(target) = self.target.as_ref() else {
            // Nothing to relabel, but entry nulls fail downstream whatever the declaration
            // says, so they are still refused here where the column can be named.
            Self::refuse_entry_nulls(&batch)?;
            return Ok(batch);
        };

        let schema = batch.schema();
        let columns = schema
            .fields()
            .iter()
            .zip(batch.columns())
            .zip(target.fields())
            .map(|((field, column), target_field)| {
                if !contains(field.data_type(), &is_map) {
                    return Ok(Arc::clone(column));
                }
                let data = column.to_data();
                refuse_entry_nulls_in(&data, field.name())?;
                // `apply_rules` shares an unchanged field by refcount, so pointer equality is
                // an exact test for "this column needs nothing".
                if Arc::ptr_eq(field, target_field) {
                    return Ok(Arc::clone(column));
                }
                let relabelled = relabel_array_data(data, target_field.data_type()).context(
                    UnableToNormalizeColumnSnafu {
                        column: field.name(),
                    },
                )?;
                Ok(make_array(relabelled))
            })
            .collect::<Result<Vec<ArrayRef>>>()?;

        RecordBatch::try_new(Arc::clone(target), columns).context(UnableToRebuildRecordBatchSnafu {
            schema: target.to_string(),
        })
    }

    fn refuse_entry_nulls(batch: &RecordBatch) -> Result<()> {
        for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
            if contains(field.data_type(), &is_map) {
                refuse_entry_nulls_in(&column.to_data(), field.name())?;
            }
        }
        Ok(())
    }
}

/// The Arrow-conforming form of `schema`: every `Map` field, nested ones included, declares
/// its `entries` non-nullable.
///
/// A schema that already conforms is returned as the same `Arc`, so a caller can test whether
/// anything changed with [`Arc::ptr_eq`] and pay nothing when nothing did.
#[must_use]
pub fn conforming_schema(schema: &SchemaRef) -> SchemaRef {
    if schema
        .fields()
        .iter()
        .any(|field| contains(field.data_type(), &declares_nullable_entries))
    {
        Arc::new(apply_rules(schema, &[&MapEntriesNonNullable]))
    } else {
        Arc::clone(schema)
    }
}

/// Normalizes the batches of a decode point that learns its schema only from the batches
/// themselves.
///
/// An Arrow Flight `DoGet` stream hands out decoded `RecordBatch`es without surfacing the
/// schema message that preceded them, so the schema is whatever the first batch carries. It is
/// still one schema per stream, so the resolved [`MapEntriesNormalizer`] is kept and reused for
/// as long as the batches keep arriving under the same `SchemaRef`; a stream that does send a
/// second schema message resolves a second normalizer rather than relabelling to the first one's
/// target.
#[derive(Default)]
pub struct StreamNormalizer {
    resolved: Option<(SchemaRef, MapEntriesNormalizer)>,
}

impl StreamNormalizer {
    #[must_use]
    pub fn new() -> Self {
        Self { resolved: None }
    }

    /// Returns `batch` with every `Map` column declaring its `entries` field non-nullable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::MapEntriesContainNulls`] when an entries array carries nulls — see
    /// [`MapEntriesNormalizer::normalize`], which this delegates to.
    pub fn normalize(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let (schema, normalizer) = self
            .resolved
            .take()
            .filter(|(resolved_for, _)| Arc::ptr_eq(resolved_for, &schema))
            .unwrap_or_else(|| {
                (
                    Arc::clone(&schema),
                    MapEntriesNormalizer::for_schema(&schema),
                )
            });

        let normalized = normalizer.normalize(batch);
        self.resolved = Some((schema, normalizer));
        normalized
    }
}

fn is_map(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(_, _))
}

fn declares_nullable_entries(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Map(entries, _) if entries.is_nullable())
}

/// Returns `true` when `predicate` holds for `data_type` or for any type nested inside it.
///
/// Allocation-free and short-circuiting. Asking [`crate::type_rewrite::rewrite_data_type`] for
/// a rewritten copy and comparing it would answer the same question, but it rebuilds the whole
/// type tree — every nested `Field`, name included — to produce one bit.
fn contains(data_type: &DataType, predicate: &impl Fn(&DataType) -> bool) -> bool {
    if predicate(data_type) {
        return true;
    }
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _)
        | DataType::RunEndEncoded(_, field) => contains(field.data_type(), predicate),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| contains(field.data_type(), predicate)),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains(field.data_type(), predicate)),
        DataType::Dictionary(_, value_type) => contains(value_type, predicate),
        _ => false,
    }
}

/// Walks `data` and fails on the first `Map` whose entries array carries nulls.
fn refuse_entry_nulls_in(data: &ArrayData, column: &str) -> Result<()> {
    if let (DataType::Map(_, _), Some(entries)) = (data.data_type(), data.child_data().first()) {
        ensure!(
            entries.null_count() == 0,
            MapEntriesContainNullsSnafu { column }
        );
    }

    for child in data.child_data() {
        refuse_entry_nulls_in(child, column)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, MapArray, StringArray, StructArray};
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields, Schema};
    use arrow::error::ArrowError;

    /// Every batch of a stream is normalized against that stream's schema; a test holds one
    /// batch, so its own schema is the stream's.
    fn normalize(batch: RecordBatch) -> Result<RecordBatch> {
        MapEntriesNormalizer::for_schema(&batch.schema()).normalize(batch)
    }

    fn entry_fields() -> Fields {
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]
        .into()
    }

    fn map_type(entries_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(entry_fields()),
                entries_nullable,
            )),
            false,
        )
    }

    /// Builds a `MapArray` bypassing `MapArray::try_new`, the way the IPC reader does:
    /// `From<ArrayData>` performs neither of the two `entries` checks, which is why a
    /// non-conforming map reaches us at all.
    fn map_from_parts(
        entries_nullable: bool,
        entry_nulls: Option<NullBuffer>,
        offsets: &[i32],
        keys: Vec<&str>,
        values: Vec<Option<&str>>,
    ) -> MapArray {
        let entries = StructArray::try_new(
            entry_fields(),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
            entry_nulls,
        )
        .expect("entries struct");

        let data = ArrayData::builder(map_type(entries_nullable))
            .len(offsets.len() - 1)
            .add_buffer(Buffer::from_slice_ref(offsets))
            .add_child_data(entries.to_data())
            .build()
            .expect("map array data");

        MapArray::from(data)
    }

    fn batch_of(column: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col_map",
                column.data_type().clone(),
                true,
            )])),
            vec![column],
        )
        .expect("batch")
    }

    /// Rebuilds `map` through the public `MapArray` constructor, the one every kernel that
    /// touches a map column goes through, keeping the declared `entries` field.
    fn rebuild_through_public_constructor(map: &MapArray) -> Result<MapArray, ArrowError> {
        let (field, offsets, entries, nulls, ordered) = map.clone().into_parts();
        MapArray::try_new(field, offsets, entries, nulls, ordered)
    }

    /// Regression test for #7307: a `MAP` column whose `entries` field arrives declared
    /// nullable cannot be rebuilt by any kernel, and the refusal reports nulls the data does
    /// not contain — which is why the declaration was not the first suspect.
    #[test]
    fn a_nullable_entries_declaration_is_relabelled_so_the_column_can_be_rebuilt() {
        let map = map_from_parts(
            true,
            None,
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let batch = batch_of(Arc::new(map) as ArrayRef);

        let before_map = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map before");
        assert_eq!(
            before_map.entries().null_count(),
            0,
            "the data holds no nulls at all"
        );
        let err = rebuild_through_public_constructor(before_map)
            .expect_err("a nullable entries field is refused on its own");
        assert!(
            err.to_string()
                .contains("MapArray entries cannot contain nulls"),
            "unexpected error: {err}"
        );

        let normalized = normalize(batch.clone()).expect("normalization");

        match normalized.schema().field(0).data_type() {
            DataType::Map(entries, _) => assert!(
                !entries.is_nullable(),
                "entries must be relabelled non-nullable"
            ),
            other => panic!("expected a Map, got {other}"),
        }

        let after = normalized
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map after");
        rebuild_through_public_constructor(after)
            .expect("a spec-conforming map rebuilds through the public constructor");

        assert_eq!(normalized.num_rows(), batch.num_rows());
        assert_eq!(after.offsets(), before_map.offsets());
        assert_eq!(after.keys(), before_map.keys());
        assert_eq!(after.values(), before_map.values());
        assert_eq!(after.nulls(), before_map.nulls());
    }

    /// A null map row — validity 0 with an empty offset range — is preserved, since that is
    /// the layout the relabelling exists to keep reachable.
    #[test]
    fn a_null_map_row_survives_normalization() {
        let entries = StructArray::try_new(
            entry_fields(),
            vec![
                Arc::new(StringArray::from(vec!["k0"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("v0")])) as ArrayRef,
            ],
            None,
        )
        .expect("entries");
        let data = ArrayData::builder(map_type(true))
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0i32, 1, 1]))
            .nulls(Some(NullBuffer::from(vec![true, false])))
            .add_child_data(entries.to_data())
            .build()
            .expect("map data");
        let batch = batch_of(Arc::new(MapArray::from(data)) as ArrayRef);

        let normalized = normalize(batch).expect("normalization");
        let map = normalized
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map");
        assert_eq!(map.len(), 2);
        assert!(map.is_valid(0), "row 0 holds a map");
        assert!(map.is_null(1), "row 1 is a null map");
        assert_eq!(map.value(0).len(), 1);
    }

    /// Entries that genuinely carry nulls cannot be relabelled: Arrow gives a null entry no
    /// meaning, so the column is refused by name rather than guessed at.
    #[test]
    fn entry_level_nulls_are_refused_by_column_name() {
        let map = map_from_parts(
            true,
            Some(NullBuffer::from(vec![true, false])),
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), None],
        );
        let err = normalize(batch_of(Arc::new(map) as ArrayRef))
            .expect_err("entry nulls must be refused");
        assert!(
            matches!(&err, Error::MapEntriesContainNulls { column } if column == "col_map"),
            "unexpected error: {err}"
        );
    }

    /// The refusal holds for a map nested inside another container, where the offending
    /// entries array is not the column's own child.
    #[test]
    fn entry_level_nulls_nested_in_a_list_are_refused() {
        let map = map_from_parts(
            true,
            Some(NullBuffer::from(vec![false])),
            &[0, 1],
            vec!["k0"],
            vec![None],
        );
        let list = ListArray::try_new(
            Arc::new(Field::new("item", map.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(map) as ArrayRef,
            None,
        )
        .expect("list of maps");

        let err = normalize(batch_of(Arc::new(list) as ArrayRef))
            .expect_err("nested entry nulls must be refused");
        assert!(
            matches!(&err, Error::MapEntriesContainNulls { column } if column == "col_map"),
            "unexpected error: {err}"
        );
    }

    /// A map nested inside a struct inside a list is relabelled too — the rewrite has to
    /// reach every depth, not just a top-level map column.
    #[test]
    fn a_deeply_nested_map_declaration_is_relabelled() {
        let map = map_from_parts(true, None, &[0, 1], vec!["k0"], vec![Some("v0")]);
        let map_type_before = map.data_type().clone();
        let struct_fields: Fields = vec![
            Field::new("m", map_type_before, true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        let inner = StructArray::try_new(
            struct_fields.clone(),
            vec![
                Arc::new(map) as ArrayRef,
                Arc::new(Int32Array::from(vec![7])) as ArrayRef,
            ],
            None,
        )
        .expect("struct");
        let list = ListArray::try_new(
            Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(inner) as ArrayRef,
            None,
        )
        .expect("list");

        let batch = batch_of(Arc::new(list) as ArrayRef);

        let normalized = normalize(batch).expect("normalization");

        // Spelled out rather than asked of the rewrite rule: the map is relabelled, and the
        // field names and the untouched `n` beside it come through as they were.
        let expected_fields: Fields = vec![
            Field::new("m", map_type(false), true),
            Field::new("n", DataType::Int32, true),
        ]
        .into();
        assert_eq!(
            normalized.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(expected_fields),
                true
            )))
        );

        // The values are still reachable through the relabelled type.
        let list = normalized
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("list");
        let inner = list
            .value(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct")
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map")
            .value(0);
        assert_eq!(inner.len(), 1);
    }

    /// A map nested under each wrapper type the rewrite descends into is relabelled, children
    /// included.
    ///
    /// The rewrite rule reaches a map through `ListView`, `Union`, `Dictionary` and
    /// `RunEndEncoded` as readily as through `List` and `Struct`, so the relabel has to rebuild
    /// those children too. Where it does not, the wrapper is rebuilt carrying the new type over
    /// children that still hold the old one, and Arrow rejects the mismatch — the query returns
    /// an error rather than rows.
    #[test]
    fn a_map_under_every_traversed_wrapper_is_relabelled() {
        // Every wrapper is reported, not just the first to break: a loop that panics on wrapper
        // one leaves the rest unproven.
        let mut failures: Vec<String> = Vec::new();
        for (name, wrapped) in wrapped_maps() {
            let expected = crate::type_rewrite::rewrite_data_type(
                wrapped.data_type(),
                &[&MapEntriesNonNullable],
            );
            assert!(
                holds_nullable_entries(wrapped.data_type()) && !holds_nullable_entries(&expected),
                "{name}: the fixture must start non-conforming and have a conforming rewrite"
            );

            match normalize(batch_of(Arc::clone(&wrapped))) {
                Err(e) => failures.push(format!("{name}: {e}")),
                Ok(normalized) => {
                    let schema = normalized.schema();
                    let published = schema.field(0).data_type();
                    if published != &expected {
                        failures.push(format!("{name}: published {published}, want {expected}"));
                    }
                    // The child array carries the relabelled type too — a parent-only rewrite
                    // is what Arrow rejects.
                    if normalized.column(0).data_type() != &expected {
                        failures.push(format!(
                            "{name}: column type {}, want {expected}",
                            normalized.column(0).data_type()
                        ));
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "wrappers not normalized: {failures:#?}"
        );
    }

    /// Independent of the rewrite rule: walks the type for a nullable `entries` field.
    fn holds_nullable_entries(data_type: &DataType) -> bool {
        match data_type {
            DataType::Map(entries, _) => {
                entries.is_nullable() || holds_nullable_entries(entries.data_type())
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::FixedSizeList(f, _)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::RunEndEncoded(_, f) => holds_nullable_entries(f.data_type()),
            DataType::Struct(fields) => {
                fields.iter().any(|f| holds_nullable_entries(f.data_type()))
            }
            DataType::Union(fields, _) => fields
                .iter()
                .any(|(_, f)| holds_nullable_entries(f.data_type())),
            DataType::Dictionary(_, value) => holds_nullable_entries(value),
            _ => false,
        }
    }

    /// One single-element array per wrapper type, each holding a map whose `entries` field is
    /// declared nullable.
    fn wrapped_maps() -> Vec<(&'static str, ArrayRef)> {
        use arrow::array::{
            DictionaryArray, FixedSizeListArray, Int32Array, ListViewArray, RunArray, UnionArray,
        };
        use arrow::buffer::ScalarBuffer;
        use arrow::datatypes::{Int32Type, UnionFields};

        let map = || {
            Arc::new(map_from_parts(
                true,
                None,
                &[0, 1],
                vec!["k0"],
                vec![Some("v0")],
            ))
        };
        let item = |array: &ArrayRef| Arc::new(Field::new("item", array.data_type().clone(), true));

        let mut out: Vec<(&'static str, ArrayRef)> = Vec::new();

        let m = map() as ArrayRef;
        out.push((
            "FixedSizeList",
            Arc::new(FixedSizeListArray::new(item(&m), 1, Arc::clone(&m), None)),
        ));

        let m = map() as ArrayRef;
        out.push((
            "ListView",
            Arc::new(
                ListViewArray::try_new(
                    item(&m),
                    ScalarBuffer::from(vec![0]),
                    ScalarBuffer::from(vec![1]),
                    Arc::clone(&m),
                    None,
                )
                .expect("list view of a map"),
            ),
        ));

        let m = map() as ArrayRef;
        out.push((
            "Dictionary",
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), Arc::clone(&m))
                    .expect("dictionary of maps"),
            ),
        ));

        let m = map() as ArrayRef;
        out.push((
            "RunEndEncoded",
            Arc::new(
                RunArray::try_new(&Int32Array::from(vec![1]), m.as_ref())
                    .expect("run-end encoded maps"),
            ),
        ));

        let m = map() as ArrayRef;
        let union_fields: UnionFields =
            [(0_i8, Arc::new(Field::new("m", m.data_type().clone(), true)))]
                .into_iter()
                .collect();
        out.push((
            "Union",
            Arc::new(
                UnionArray::try_new(
                    union_fields,
                    ScalarBuffer::from(vec![0_i8]),
                    Some(ScalarBuffer::from(vec![0_i32])),
                    vec![Arc::clone(&m)],
                )
                .expect("union of maps"),
            ),
        ));

        out
    }

    /// A batch that already conforms is handed back as-is, so the common case pays nothing.
    #[test]
    fn a_conforming_batch_is_returned_unchanged() {
        let map = map_from_parts(false, None, &[0, 1], vec!["k0"], vec![Some("v0")]);
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let normalized = normalize(batch.clone()).expect("normalization");
        assert_eq!(normalized.schema(), batch.schema());
        assert_eq!(normalized.column(0).to_data(), batch.column(0).to_data());
    }

    /// A decode point that hands its batches on under a separately declared schema — an
    /// `ExecutionPlan`'s output schema, a `RecordBatchStreamAdapter`'s — has to declare the one
    /// the batches actually carry, or it describes them with a type they no longer have.
    #[test]
    fn the_reported_schema_is_the_one_the_normalized_batches_carry() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));
        let normalizer = MapEntriesNormalizer::for_schema(&declared);
        assert_eq!(
            normalizer.schema().field(0).data_type(),
            &map_type(false),
            "the reported schema still declares the entries the batches no longer do"
        );

        let batch = batch_of(Arc::new(map_from_parts(
            true,
            None,
            &[0, 1],
            vec!["k0"],
            vec![Some("v0")],
        )) as ArrayRef);
        let normalized = normalizer.normalize(batch).expect("normalization");
        assert_eq!(&normalized.schema(), normalizer.schema());
    }

    /// A stream whose declarations already conform is reported under its own schema, by
    /// reference — nothing is rebuilt to say the same thing.
    #[test]
    fn a_conforming_stream_is_reported_under_its_own_schema() {
        let declared = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(false),
            true,
        )]));
        let normalizer = MapEntriesNormalizer::for_schema(&declared);
        assert!(Arc::ptr_eq(normalizer.schema(), &declared));
    }

    /// `conforming_schema` hands back the same `Arc` when nothing needs correcting, so a caller
    /// can tell "already conformed" from "relabelled" and pay nothing in the common case.
    #[test]
    fn conforming_schema_shares_a_schema_that_already_conforms() {
        let conforming = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(false),
            true,
        )]));
        assert!(Arc::ptr_eq(&conforming_schema(&conforming), &conforming));

        let nullable_entries = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));
        let corrected = conforming_schema(&nullable_entries);
        assert!(!Arc::ptr_eq(&corrected, &nullable_entries));
        assert_eq!(corrected.field(0).data_type(), &map_type(false));
    }

    /// A schema holding no `Map` at all is shared rather than rebuilt, whatever else it holds.
    #[test]
    fn conforming_schema_shares_a_schema_holding_no_map() {
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]));
        assert!(Arc::ptr_eq(&conforming_schema(&schema), &schema));
    }

    /// A stream that only learns its schema from its batches resolves one normalizer and reuses
    /// it: every batch comes back under the *same* `SchemaRef`, which is what a downstream
    /// operator comparing schemas by pointer relies on.
    #[test]
    fn a_stream_normalizer_reuses_one_resolved_schema_across_batches() {
        let mut normalizer = StreamNormalizer::new();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col_map",
            map_type(true),
            true,
        )]));

        let of_schema = |keys: Vec<&str>, values: Vec<Option<&str>>, offsets: &[i32]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(map_from_parts(true, None, offsets, keys, values)) as ArrayRef],
            )
            .expect("batch")
        };

        let first = normalizer
            .normalize(of_schema(vec!["k0"], vec![Some("v0")], &[0, 1]))
            .expect("first batch");
        let second = normalizer
            .normalize(of_schema(vec!["k1"], vec![Some("v1")], &[0, 1]))
            .expect("second batch");

        assert_eq!(first.schema().field(0).data_type(), &map_type(false));
        assert!(
            Arc::ptr_eq(&first.schema(), &second.schema()),
            "every batch of one stream must come out under one schema"
        );
    }

    /// A second schema message mid-stream resolves a second normalizer: relabelling the new
    /// batches to the first schema's target would rename their columns and change their types.
    #[test]
    fn a_stream_normalizer_re_resolves_when_the_schema_changes() {
        let mut normalizer = StreamNormalizer::new();

        let with_map = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "col_map",
                map_type(true),
                true,
            )])),
            vec![Arc::new(map_from_parts(
                true,
                None,
                &[0, 1],
                vec!["k0"],
                vec![Some("v0")],
            )) as ArrayRef],
        )
        .expect("map batch");
        let without_map = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .expect("int batch");

        let first = normalizer.normalize(with_map).expect("map batch");
        assert_eq!(first.schema().field(0).data_type(), &map_type(false));

        let second = normalizer
            .normalize(without_map.clone())
            .expect("a batch under a new schema");
        assert_eq!(second.schema(), without_map.schema());
        assert_eq!(second.column(0).to_data(), without_map.column(0).to_data());
    }

    /// The one shape relabelling cannot fix is refused through the streaming form too, naming
    /// the column — a stream must not hand on a map whose entries carry nulls.
    #[test]
    fn a_stream_normalizer_refuses_entry_nulls() {
        let mut normalizer = StreamNormalizer::new();
        let entry_nulls = NullBuffer::from(vec![true, false]);
        let batch = batch_of(Arc::new(map_from_parts(
            true,
            Some(entry_nulls),
            &[0, 1, 2],
            vec!["k0", "k1"],
            vec![Some("v0"), Some("v1")],
        )) as ArrayRef);

        let err = normalizer
            .normalize(batch)
            .expect_err("entries carrying nulls have no representation to relabel to");
        assert!(
            err.to_string().contains("col_map"),
            "the refusal must name the column: {err}"
        );
    }

    /// An empty map column — zero rows — normalizes without touching offsets.
    #[test]
    fn an_empty_map_column_normalizes() {
        let map = map_from_parts(true, None, &[0], vec![], vec![]);
        let normalized = normalize(batch_of(Arc::new(map) as ArrayRef))
            .expect("normalization of an empty column");
        assert_eq!(normalized.num_rows(), 0);
        assert_eq!(normalized.schema().field(0).data_type(), &map_type(false));
    }
}
