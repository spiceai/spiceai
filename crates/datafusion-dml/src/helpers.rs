/*
Copyright 2026, Spice AI, Inc.

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

//! Generic helpers shared across DML integrations.

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DFSchemaRef;

/// Creates the shared output schema for DML result nodes — a single
/// `count: UInt64` column.
///
/// # Panics
///
/// Panics only if the fixed schema cannot be constructed, which is a
/// compile-time invariant.
#[must_use]
pub fn dml_count_output_schema() -> DFSchemaRef {
    DFSchemaRef::new(
        datafusion::common::DFSchema::try_from(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]))
        .unwrap_or_else(|e| unreachable!("fixed DML output schema must be valid: {e}")),
    )
}
