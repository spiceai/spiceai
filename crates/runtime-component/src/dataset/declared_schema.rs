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

//! Build an Arrow `Schema` from a dataset's `columns[].type` declarations.
//!
//! Returns `None` if any column lacks an explicit `type`. The runtime
//! treats this as "the user did not declare a full schema" and falls
//! back to source-derived schema inference (today's path).
//!
//! Used by `HttpsFactory::static_schema` to decide
//! whether a dataset can be registered with a placeholder
//! for a given dataset.

use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use snafu::{ResultExt, Snafu};
use spicepod::semantic::Column;

use crate::dataset::DatasetSpec;
use crate::dataset::declared_type::{ParseTypeError, parse_declared_type};

#[derive(Debug, Snafu)]
pub enum DeclaredSchemaError {
    #[snafu(display(
        "Could not parse declared type for column `{column}` of dataset `{dataset}`: {source}"
    ))]
    InvalidColumnType {
        dataset: String,
        column: String,
        // Boxed to keep `Result<_, DeclaredSchemaError>` — and the
        // `dataset::Error` that wraps it — small enough for
        // `clippy::result_large_err`.
        #[snafu(source(from(ParseTypeError, Box::new)))]
        source: Box<ParseTypeError>,
    },
}

/// Build an Arrow schema from a dataset's `columns[]`, if every column
/// has an explicit `type`. Returns `None` otherwise.
///
/// `nullable` defaults to `true` when not specified — Arrow's standard
/// "unknown nullability is nullable" convention.
pub fn declared_schema_for(
    dataset: &DatasetSpec,
) -> Result<Option<SchemaRef>, DeclaredSchemaError> {
    if dataset.columns.is_empty() {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(dataset.columns.len());
    for column in &dataset.columns {
        let Some(type_str) = column.r#type.as_deref() else {
            return Ok(None);
        };
        let dt = parse_declared_type(type_str).context(InvalidColumnTypeSnafu {
            dataset: dataset.name.to_string(),
            column: column.name.clone(),
        })?;
        let nullable = column.nullable.unwrap_or(true);
        fields.push(Field::new(&column.name, dt, nullable));
    }

    Ok(Some(Arc::new(Schema::new(fields))))
}

/// Build an Arrow schema from a set of columns that have an explicit `type`.
///
/// Unlike `declared_schema_for`, columns without a `type` are silently skipped rather
/// than causing the whole result to be `None`. Returns `None` only when no column
/// carries a `type` declaration at all.
///
/// `dataset_name` is used only in error messages. Columns without a `type` are
/// skipped; an error is returned if a `type` string cannot be parsed.
pub fn schema_from_columns(
    dataset_name: &str,
    columns: &[Column],
) -> Result<Option<SchemaRef>, DeclaredSchemaError> {
    let mut fields = Vec::new();
    for column in columns {
        let Some(type_str) = column.r#type.as_deref() else {
            continue;
        };
        let dt = parse_declared_type(type_str).context(InvalidColumnTypeSnafu {
            dataset: dataset_name.to_string(),
            column: column.name.clone(),
        })?;
        let nullable = column.nullable.unwrap_or(true);
        fields.push(Field::new(&column.name, dt, nullable));
    }

    if fields.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Arc::new(Schema::new(fields))))
    }
}

// NOTE: `declared_schema_for` unit tests construct a full `Dataset` (via
// `DatasetBuilder` + `Runtime`) and therefore live with the wrapper in the
// `runtime` crate (`crates/runtime/src/component/dataset/mod.rs`), which can
// name `Runtime`. They call `declared_schema_for(&dataset)` — `&Dataset` derefs
// to `&DatasetSpec` at the call site.
