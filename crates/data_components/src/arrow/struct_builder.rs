// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{any::Any, sync::Arc};

use arrow::{
    array::{ArrayBuilder, ArrayRef, StructArray, make_builder},
    datatypes::{DataType, Fields, SchemaBuilder},
};
use arrow_buffer::NullBufferBuilder;

/// This is modified from the original Arrow codebase to support getting the child field builders as dyn `ArrayBuilder`
///
/// Search for "MODIFIED:" below to see the modifications.
pub struct StructBuilder {
    fields: Fields,
    field_builders: Vec<Box<dyn ArrayBuilder>>,
    null_buffer_builder: NullBufferBuilder,
}

impl std::fmt::Debug for StructBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructBuilder")
            .field("fields", &self.fields)
            .field("bitmap_builder", &self.null_buffer_builder)
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl ArrayBuilder for StructBuilder {
    /// Returns the number of array slots in the builder.
    ///
    /// Note that this always return the first child field builder's length, and it is
    /// the caller's responsibility to maintain the consistency that all the child field
    /// builder should have the equal number of elements.
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Builds the array.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_ref` to get a reference on the specific builder.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific builder
    /// type. In this case, one can first cast this into a `Any`, and then use
    /// `downcast_mut` to get a reference on the specific builder.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl StructBuilder {
    /// Creates a new `StructBuilder`
    pub fn new(fields: impl Into<Fields>, field_builders: Vec<Box<dyn ArrayBuilder>>) -> Self {
        Self {
            field_builders,
            fields: fields.into(),
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }

    /// Creates a new `StructBuilder` from [`Fields`] and `capacity`
    pub fn from_fields(fields: impl Into<Fields>, capacity: usize) -> Self {
        let fields = fields.into();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::with_capacity(fields.len());
        for field in &fields {
            if let DataType::Struct(fields) = field.data_type() {
                builders.push(Box::new(StructBuilder::from_fields(
                    fields.clone(),
                    capacity,
                )));
                continue;
            }
            builders.push(make_builder(field.data_type(), capacity));
        }
        Self::new(fields, builders)
    }

    /// Returns a mutable reference to the child field builder at index `i`.
    /// Result will be `None` if the input type `T` provided doesn't match the actual
    /// field builder's type.
    pub fn field_builder<T: ArrayBuilder>(&mut self, i: usize) -> Option<&mut T> {
        self.field_builders[i].as_any_mut().downcast_mut::<T>()
    }

    /// MODIFIED: Return the raw field builder as a dyn `ArrayBuilder`
    pub fn field_builder_array(&mut self, i: usize) -> &mut dyn ArrayBuilder {
        &mut *self.field_builders[i]
    }

    /// MODIFIED: Add a way to get the schema of the struct being built
    #[must_use]
    pub fn fields(&self) -> Fields {
        self.fields.clone()
    }

    /// Returns the number of fields for the struct this builder is building.
    #[must_use]
    pub fn num_fields(&self) -> usize {
        self.field_builders.len()
    }

    /// Appends an element (either null or non-null) to the struct. The actual elements
    /// should be appended for each child sub-array in a consistent way.
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    /// Appends a null element to the struct.
    #[inline]
    pub fn append_null(&mut self) {
        self.append(false);
    }

    /// Builds the `StructArray` and reset this builder.
    pub fn finish(&mut self) -> StructArray {
        self.validate_content();
        if self.fields.is_empty() {
            return StructArray::new_empty_fields(self.len(), self.null_buffer_builder.finish());
        }

        let arrays = self
            .field_builders
            .iter_mut()
            .map(ArrayBuilder::finish)
            .collect();
        let nulls = self.null_buffer_builder.finish();
        StructArray::new(self.fields.clone(), arrays, nulls)
    }

    /// Builds the `StructArray` without resetting the builder.
    #[must_use]
    pub fn finish_cloned(&self) -> StructArray {
        self.validate_content();

        if self.fields.is_empty() {
            return StructArray::new_empty_fields(
                self.len(),
                self.null_buffer_builder.finish_cloned(),
            );
        }

        let arrays = self
            .field_builders
            .iter()
            .map(ArrayBuilder::finish_cloned)
            .collect();

        let nulls = self.null_buffer_builder.finish_cloned();

        StructArray::new(self.fields.clone(), arrays, nulls)
    }

    /// Constructs and validates contents in the builder to ensure that
    /// - fields and `field_builders` are of equal length
    /// - the number of items in individual `field_builders` are equal to `self.len()`
    #[expect(clippy::manual_assert)]
    fn validate_content(&self) {
        if self.fields.len() != self.field_builders.len() {
            panic!("Number of fields is not equal to the number of field_builders.");
        }
        self.field_builders.iter().enumerate().for_each(|(idx, x)| {
            if x.len() != self.len() {
                let builder = SchemaBuilder::from(&self.fields);
                let schema = builder.finish();

                panic!("{}", format!(
                    "StructBuilder ({:?}) and field_builder with index {} ({:?}) are of unequal lengths: ({} != {}).",
                    schema,
                    idx,
                    self.fields[idx].data_type(),
                    self.len(),
                    x.len()
                ));
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::StructBuilder;
    use arrow::array::{Array, ArrayBuilder, Int32Array, Int32Builder, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Fields};

    fn flat_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn nested_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new(
                "inner",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int32, true)])),
                true,
            ),
        ])
    }

    /// Append one `(id, name)` row, marking the struct slot valid.
    fn append_row(builder: &mut StructBuilder, id: Option<i32>, name: Option<&str>) {
        builder.append(true);
        builder
            .field_builder::<Int32Builder>(0)
            .expect("id builder")
            .append_option(id);
        builder
            .field_builder::<StringBuilder>(1)
            .expect("name builder")
            .append_option(name);
    }

    #[test]
    fn a_flat_struct_round_trips_its_values() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 2);
        append_row(&mut builder, Some(1), Some("a"));
        append_row(&mut builder, Some(2), Some("b"));

        let array = builder.finish();
        assert_eq!(array.len(), 2);
        assert_eq!(array.num_columns(), 2);

        let ids = array
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32");
        let names = array
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        assert_eq!(ids.values(), &[1, 2]);
        assert_eq!(names.value(0), "a");
        assert_eq!(names.value(1), "b");
    }

    /// A per-column null is distinct from a null struct slot: the row exists,
    /// one of its values does not. Collapsing the two would turn a partial row
    /// into a missing row.
    #[test]
    fn a_null_column_value_leaves_the_row_itself_valid() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 1);
        append_row(&mut builder, Some(1), None);

        let array = builder.finish();
        assert!(!array.is_null(0), "the struct row is present");
        assert!(
            array.column(1).is_null(0),
            "the name value within it is null"
        );
    }

    /// `append(false)` marks the whole struct slot null. The child builders
    /// still have to receive a slot each, or the arrays fall out of alignment.
    #[test]
    fn appending_an_invalid_slot_marks_the_whole_row_null() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 2);
        append_row(&mut builder, Some(1), Some("a"));
        builder.append_null();
        builder
            .field_builder::<Int32Builder>(0)
            .expect("id builder")
            .append_null();
        builder
            .field_builder::<StringBuilder>(1)
            .expect("name builder")
            .append_null();

        let array = builder.finish();
        assert_eq!(array.len(), 2);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
    }

    /// The Debezium row writer reaches nested structs through
    /// `field_builder_array` and downcasts them to *this* `StructBuilder`.
    /// Arrow's own `make_builder` would hand back its own type and the
    /// downcast — and every nested CDC row — would fail.
    #[test]
    fn a_nested_struct_field_gets_this_struct_builder() {
        let mut builder = StructBuilder::from_fields(nested_fields(), 1);

        let inner = builder
            .field_builder_array(1)
            .as_any_mut()
            .downcast_mut::<StructBuilder>();
        assert!(
            inner.is_some(),
            "nested struct fields must use the modified StructBuilder"
        );
    }

    #[test]
    fn a_nested_struct_round_trips_through_the_child_builder() {
        let mut builder = StructBuilder::from_fields(nested_fields(), 1);
        builder.append(true);
        builder
            .field_builder::<Int32Builder>(0)
            .expect("id builder")
            .append_value(7);
        {
            let inner = builder
                .field_builder_array(1)
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .expect("nested struct builder");
            inner.append(true);
            inner
                .field_builder::<Int32Builder>(0)
                .expect("x builder")
                .append_value(42);
        }

        let array = builder.finish();
        assert_eq!(array.len(), 1);
        let inner = array
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .expect("inner is a struct");
        let x = inner
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("x is Int32");
        assert_eq!(x.value(0), 42);
    }

    /// The guard that stops a misaligned struct from ever being produced: if a
    /// child builder falls behind, its values would silently shift onto the
    /// wrong rows. It must fail loudly instead.
    #[test]
    #[should_panic(expected = "unequal lengths")]
    fn a_child_builder_left_behind_is_rejected() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 1);
        builder.append(true);
        builder
            .field_builder::<Int32Builder>(0)
            .expect("id builder")
            .append_value(1);
        // `name` never appended — one column short.
        let _ = builder.finish();
    }

    #[test]
    fn finish_resets_the_builder_and_finish_cloned_does_not() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 1);
        append_row(&mut builder, Some(1), Some("a"));

        let cloned = builder.finish_cloned();
        assert_eq!(cloned.len(), 1);
        assert_eq!(
            ArrayBuilder::len(&builder),
            1,
            "finish_cloned leaves the builder intact"
        );

        let finished = builder.finish();
        assert_eq!(finished.len(), 1);
        assert_eq!(
            ArrayBuilder::len(&builder),
            0,
            "finish resets the builder for reuse"
        );
    }

    /// A struct with no fields still has to carry its row count — that is what
    /// makes `SELECT COUNT(*)`-shaped, column-less data survive the CDC path.
    #[test]
    fn a_field_less_struct_keeps_its_row_count() {
        let mut builder = StructBuilder::from_fields(Fields::empty(), 0);
        for _ in 0..3 {
            builder.append(true);
        }

        let array = builder.finish();
        assert_eq!(array.len(), 3);
        assert_eq!(array.num_columns(), 0);
    }

    #[test]
    fn the_builder_reports_the_declared_field_set() {
        let builder = StructBuilder::from_fields(flat_fields(), 0);
        assert_eq!(builder.num_fields(), 2);
        assert_eq!(builder.fields(), flat_fields());
        assert_eq!(ArrayBuilder::len(&builder), 0);
    }

    /// `new` accepts pre-built child builders; a field/builder count mismatch
    /// has to be caught at `finish` rather than producing a struct whose
    /// schema and columns disagree.
    #[test]
    #[should_panic(expected = "Number of fields is not equal")]
    fn a_field_and_builder_count_mismatch_is_rejected() {
        let builders: Vec<Box<dyn ArrayBuilder>> = vec![Box::new(Int32Builder::new())];
        let mut builder = StructBuilder::new(flat_fields(), builders);
        builder.append(true);
        builder
            .field_builder::<Int32Builder>(0)
            .expect("id builder")
            .append_value(1);
        let _ = builder.finish();
    }

    #[test]
    fn field_builder_returns_none_for_the_wrong_builder_type() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 0);
        assert!(
            builder.field_builder::<StringBuilder>(0).is_none(),
            "an Int32 field must not downcast to a StringBuilder"
        );
        assert!(builder.field_builder::<Int32Builder>(0).is_some());
    }

    #[test]
    fn finish_cloned_matches_finish_for_the_same_contents() {
        let mut builder = StructBuilder::from_fields(flat_fields(), 2);
        append_row(&mut builder, Some(1), Some("a"));
        append_row(&mut builder, None, None);

        let cloned = builder.finish_cloned();
        let finished = builder.finish();
        assert_eq!(cloned, finished);
    }
}
