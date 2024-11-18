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
    array::{make_builder, ArrayBuilder, ArrayRef, StructArray},
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
    #[allow(clippy::manual_assert)]
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
