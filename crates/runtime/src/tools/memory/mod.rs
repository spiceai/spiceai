/*
Copyright 2024 The Spice.ai OSS Authors

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

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampSecondArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use spicepod::component::tool::Tool;

pub mod catalog;
pub mod load;
pub mod store;

pub static DEFAULT_MEMORY_TABLE: &str = "spice.public.store";

pub struct MemoryTableElement {
    pub value: String,
    pub created_by: Option<String>,
    pub created_at: i64, // Unix timestamp in Seconds
}

pub fn try_from(data: &[MemoryTableElement]) -> Result<RecordBatch, ArrowError> {
    let values = StringArray::from_iter_values(data.iter().map(|d| d.value.as_str()));
    let created_by = StringArray::from_iter(data.iter().map(|d| d.created_by.as_deref()));
    let created_at: TimestampSecondArray =
        TimestampSecondArray::from(data.iter().map(|e| e.created_at).collect::<Vec<_>>());

    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("created_by", DataType::Utf8, true),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(values) as ArrayRef,
            Arc::new(created_by) as ArrayRef,
            Arc::new(created_at) as ArrayRef,
        ],
    )
}

// Model tools must also be added to [`super::memory::factory::MemoryToolFactory`]
#[must_use]
pub fn get_memory_tool_spec() -> Vec<Tool> {
    vec![]
}
