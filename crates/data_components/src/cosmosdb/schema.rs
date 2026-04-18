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

//! Schema inference for Cosmos DB documents.
//!
//! Cosmos DB `NoSQL` does not expose a static schema. The first `N` documents
//! returned from the configured query are sampled and handed to Arrow's
//! [`infer_json_schema_from_iterator`] to derive a best-effort Arrow schema.
//!
//! Cosmos always stamps every document with `_rid`, `_self`, `_etag`,
//! `_attachments`, and `_ts` system fields. These are stripped from the
//! sample set before inference to avoid polluting downstream tables with
//! metadata columns the user almost never wants to query.

use arrow::datatypes::{Schema, SchemaRef};
use arrow::json::reader::infer_json_schema_from_iterator;
use serde_json::Value;
use snafu::ResultExt;
use std::sync::Arc;

use super::{Error, SchemaInferenceSnafu};

/// System fields stamped on every Cosmos document. Stripped prior to schema
/// inference so they never become user-visible columns.
const COSMOS_SYSTEM_FIELDS: &[&str] = &["_rid", "_self", "_etag", "_attachments", "_ts"];

/// Strip Cosmos DB-internal system fields from a top-level JSON object. Any
/// non-object value is returned unchanged.
#[must_use]
pub fn strip_system_fields(value: Value) -> Value {
    match value {
        Value::Object(mut map) => {
            for field in COSMOS_SYSTEM_FIELDS {
                map.remove(*field);
            }
            Value::Object(map)
        }
        other => other,
    }
}

/// Infer an Arrow schema from a slice of sampled Cosmos documents. Callers
/// are expected to have already run the values through [`strip_system_fields`].
///
/// # Errors
/// Returns an error if the sample is empty or Arrow's JSON schema inference
/// fails.
pub fn infer_schema(samples: &[Value]) -> Result<SchemaRef, Error> {
    if samples.is_empty() {
        // Caller is expected to map this to a user-facing EmptyContainer
        // error. Returning an Arrow error here keeps this helper simple.
        return Ok(Arc::new(Schema::empty()));
    }

    let schema = infer_json_schema_from_iterator(
        samples
            .iter()
            .map(Result::<_, arrow::error::ArrowError>::Ok),
    )
    .context(SchemaInferenceSnafu)?;

    Ok(Arc::new(schema))
}
