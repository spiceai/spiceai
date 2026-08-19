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
//! [`normalize_map_entries`] relabels the declaration (metadata only — no buffer is
//! touched) and refuses the one shape that cannot be relabelled without inventing an
//! answer: entries that actually contain nulls.

use std::sync::Arc;

use arrow::array::{Array, ArrayData, RecordBatch, make_array};
use arrow::datatypes::{DataType, Schema};
use arrow_schema::SchemaRef;
use snafu::prelude::*;

use crate::type_rewrite::{MapEntriesNonNullable, apply_rules, rewrite_data_type};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "column '{column}' holds a MAP whose entries contain nulls, which the Arrow map layout has no way to represent"
    ))]
    MapEntriesContainNulls { column: String },

    #[snafu(display("column '{column}' could not be rebuilt: {source}"))]
    UnableToRelabelColumn {
        column: String,
        source: arrow::error::ArrowError,
    },

    #[snafu(display("the normalized columns could not be reassembled: {source}"))]
    UnableToRebuildRecordBatch { source: arrow::error::ArrowError },
}

/// Returns `true` when `data_type` declares a `Map` whose `entries` field is nullable,
/// at any depth.
#[must_use]
pub fn declares_nullable_map_entries(data_type: &DataType) -> bool {
    rewrite_data_type(data_type, &[&MapEntriesNonNullable]) != *data_type
}

/// Returns the schema with every `Map`'s `entries` field marked non-nullable.
#[must_use]
pub fn normalize_map_entries_schema(schema: &Schema) -> Schema {
    apply_rules(schema, &[&MapEntriesNonNullable])
}

/// Rewrites `batch` so every `Map` column — nested ones included — declares its `entries`
/// field non-nullable, as the Arrow specification requires.
///
/// Nullability lives in the type rather than in any buffer, so this only relabels: the
/// offsets, validity and child arrays are carried over by reference. A batch that already
/// conforms is returned untouched.
///
/// # Errors
///
/// Returns [`Error::MapEntriesContainNulls`] when an entries array carries nulls. That is
/// the one shape relabelling cannot fix, and it is not recoverable by guessing: Arrow
/// gives a null entry no meaning, so treating it as a null map and treating it as a pair
/// to drop are both inventions, and each yields different rows.
pub fn normalize_map_entries(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|field| declares_nullable_map_entries(field.data_type()))
    {
        // Still reject entry nulls: they fail downstream whatever the declaration says.
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            ensure_no_map_entry_nulls(&column.to_data(), field.name())?;
        }
        return Ok(batch);
    }

    let normalized: SchemaRef = Arc::new(normalize_map_entries_schema(schema.as_ref()));

    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .zip(normalized.fields())
        .map(|((field, column), target)| {
            let data = column.to_data();
            ensure_no_map_entry_nulls(&data, field.name())?;
            let relabelled = relabel_array_data(data, target.data_type()).context(
                UnableToRelabelColumnSnafu {
                    column: field.name(),
                },
            )?;
            Ok(make_array(relabelled))
        })
        .collect::<Result<Vec<_>>>()?;

    // `num_rows` is carried explicitly so a column-less batch keeps its row count.
    RecordBatch::try_new_with_options(
        normalized,
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .context(UnableToRebuildRecordBatchSnafu)
}

/// Walks `data` and fails on the first `Map` whose entries array carries nulls.
fn ensure_no_map_entry_nulls(data: &ArrayData, column: &str) -> Result<()> {
    if let (DataType::Map(_, _), Some(entries)) = (data.data_type(), data.child_data().first()) {
        ensure!(
            entries.null_count() == 0,
            MapEntriesContainNullsSnafu { column }
        );
    }

    for child in data.child_data() {
        ensure_no_map_entry_nulls(child, column)?;
    }

    Ok(())
}

/// Recursively rebuilds `data` so its (possibly nested) [`DataType`] becomes
/// `target_type`, without touching any values, buffers or null masks.
///
/// Only the parts of a type that carry no data may differ — field names and nested
/// nullability flags. Children are relabelled positionally, so `target_type` has to
/// describe the same physical layout.
///
/// # Errors
///
/// Returns an `ArrowError` when `target_type` does not describe the layout `data` holds:
/// the relabelled level goes back through [`ArrayData`] validation rather than
/// reinterpreting the buffers under a type that does not fit them.
pub fn relabel_array_data(
    data: ArrayData,
    target_type: &DataType,
) -> std::result::Result<ArrayData, arrow::error::ArrowError> {
    if data.data_type() == target_type {
        return Ok(data);
    }

    let target_child_types: Vec<DataType> = match target_type {
        DataType::Struct(fields) => fields.iter().map(|f| f.data_type().clone()).collect(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            vec![field.data_type().clone()]
        }
        DataType::Map(field, _) => vec![field.data_type().clone()],
        _ => Vec::new(),
    };

    let old_children = data.child_data().to_vec();
    let new_children = if target_child_types.len() == old_children.len() {
        old_children
            .into_iter()
            .zip(target_child_types.iter())
            .map(|(child, child_target)| relabel_array_data(child, child_target))
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?
    } else {
        old_children
    };

    data.into_builder()
        .data_type(target_type.clone())
        .child_data(new_children)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array, ListArray, MapArray, StringArray, StructArray};
    use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Fields};
    use arrow::error::ArrowError;

    /// Walks `data_type` for a `Map` with a nullable `entries` field, independently of the
    /// rewrite rule under test — asserting through [`declares_nullable_map_entries`] would
    /// be self-referential and would pass even if the rule stopped firing.
    fn any_nullable_map_entries(data_type: &DataType) -> bool {
        match data_type {
            DataType::Map(entries, _) => {
                entries.is_nullable() || any_nullable_map_entries(entries.data_type())
            }
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => any_nullable_map_entries(field.data_type()),
            DataType::Struct(fields) => fields
                .iter()
                .any(|f| any_nullable_map_entries(f.data_type())),
            _ => false,
        }
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

        let normalized = normalize_map_entries(batch.clone()).expect("normalization");

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

        let before = batch
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map before");
        assert_eq!(normalized.num_rows(), batch.num_rows());
        assert_eq!(after.offsets(), before.offsets());
        assert_eq!(after.keys(), before.keys());
        assert_eq!(after.values(), before.values());
        assert_eq!(after.nulls(), before.nulls());
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

        let normalized = normalize_map_entries(batch).expect("normalization");
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
        let err = normalize_map_entries(batch_of(Arc::new(map) as ArrayRef))
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

        let err = normalize_map_entries(batch_of(Arc::new(list) as ArrayRef))
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
        assert!(any_nullable_map_entries(
            batch.schema().field(0).data_type()
        ));

        let normalized = normalize_map_entries(batch).expect("normalization");
        assert!(
            !any_nullable_map_entries(normalized.schema().field(0).data_type()),
            "the nested map must be relabelled: {}",
            normalized.schema().field(0).data_type()
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

    /// A batch that already conforms is handed back as-is, so the common case pays nothing.
    #[test]
    fn a_conforming_batch_is_returned_unchanged() {
        let map = map_from_parts(false, None, &[0, 1], vec!["k0"], vec![Some("v0")]);
        let batch = batch_of(Arc::new(map) as ArrayRef);
        let normalized = normalize_map_entries(batch.clone()).expect("normalization");
        assert_eq!(normalized.schema(), batch.schema());
        assert_eq!(normalized.column(0).to_data(), batch.column(0).to_data());
    }

    /// A row-count-only batch (no columns, as `SELECT COUNT(1)` produces) keeps its rows.
    #[test]
    fn a_column_less_batch_keeps_its_row_count() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(5)),
        )
        .expect("batch");
        let normalized = normalize_map_entries(batch).expect("normalization");
        assert_eq!(normalized.num_rows(), 5);
    }

    /// An empty map column — zero rows — normalizes without touching offsets.
    #[test]
    fn an_empty_map_column_normalizes() {
        let map = map_from_parts(true, None, &[0], vec![], vec![]);
        let normalized = normalize_map_entries(batch_of(Arc::new(map) as ArrayRef))
            .expect("normalization of an empty column");
        assert_eq!(normalized.num_rows(), 0);
        assert!(!any_nullable_map_entries(
            normalized.schema().field(0).data_type()
        ));
    }
}
