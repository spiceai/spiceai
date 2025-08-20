/*
Copyright 2024-2025 The Spice.ai OSS Authors


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
use std::{collections::HashSet, sync::Arc};

use arrow::error::ArrowError;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::{catalog::TableProvider, common::Constraint};

/// Create a new [`SchemaRef`] with the additional fields specified.
///
/// If a new field is already in [`SchemaRef`], it will be ignored.
#[must_use]
pub fn append_fields(schema: &SchemaRef, new_fields: Vec<Arc<Field>>) -> SchemaRef {
    let existing_names: HashSet<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut all_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

    for field in new_fields {
        if !existing_names.contains(field.name().as_str()) {
            all_fields.push(field);
        }
    }

    Arc::new(Schema::new(all_fields))
}

pub async fn get_primary_keys(tbl: &Arc<dyn TableProvider>) -> Result<Vec<String>, ArrowError> {
    let constraints = tbl.constraints();

    tracing::trace!("Constraints table: {tbl:?}");

    tracing::trace!("Table constraints: {constraints:?}");

    let constraint_idx = constraints
        .map(|c| c.iter())
        .unwrap_or_default()
        .find_map(|c| match c {
            Constraint::PrimaryKey(columns) => Some(columns),
            Constraint::Unique(_) => None,
        })
        .cloned()
        .unwrap_or(Vec::new());

    tbl.schema()
        .project(&constraint_idx)
        .map(|schema_projection| {
            schema_projection
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        })
}
