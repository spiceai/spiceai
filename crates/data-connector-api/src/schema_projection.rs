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

//! Connector-agnostic parser that turns a dataset's `columns:` block into a
//! [`SchemaProjection`]. Replaces and de-duplicates the per-connector
//! `parse_json_nesting_static_fields` (`DynamoDB`) and `parse_http_json_nesting`
//! (HTTP) helpers.

use data_components::schema_projection::{
    ColumnSource, JSON_OBJECT_MARKER, JSON_OBJECT_WILDCARD, ProjectedColumn, SchemaProjection,
    SchemaProjectionError,
};
use serde_json::Value;

use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::declared_type::parse_declared_type;

use crate::{ConnectorComponent, DataConnectorError, DataConnectorResult};

/// Per-connector knobs for parsing a projection.
pub struct ProjectionPolicy<'a> {
    /// Connector name, used only for error messages (e.g. `"dynamodb"`).
    pub connector: &'a str,
    /// Columns that must be declared explicitly (e.g. primary-key / CDC-key
    /// columns) and may not be folded into the `json_object` catch-all.
    pub required_columns: Vec<String>,
}

impl<'a> ProjectionPolicy<'a> {
    #[must_use]
    pub fn new(connector: &'a str) -> Self {
        Self {
            connector,
            required_columns: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_required_columns(mut self, columns: Vec<String>) -> Self {
        self.required_columns = columns;
        self
    }
}

/// Parse a dataset's `columns:` into a [`SchemaProjection`].
///
/// Returns `Ok(None)` when the dataset declares no columns at all (the
/// projection would be a no-op and callers can keep their existing inference
/// path). Otherwise every declared column becomes a [`ProjectedColumn`]:
///
/// - a column carrying `metadata.json_object: "*"` becomes the catch-all
///   ([`ColumnSource::JsonObject`]); only `"*"` is accepted;
/// - any other column becomes a kept [`ColumnSource::Field`] read by its name.
///
/// # Errors
/// Returns an invalid-configuration error for a non-`"*"` catch-all marker, an
/// unparseable declared `type`, or any structural problem surfaced by
/// [`SchemaProjection::new`] (multiple catch-alls, duplicate names, a required
/// column folded into the catch-all).
pub fn parse_schema_projection(
    dataset: &DatasetSpec,
    policy: &ProjectionPolicy<'_>,
) -> DataConnectorResult<Option<SchemaProjection>> {
    if dataset.columns.is_empty() {
        return Ok(None);
    }

    let mut columns = Vec::with_capacity(dataset.columns.len());
    for column in &dataset.columns {
        let catch_all_marker = column.metadata.get(JSON_OBJECT_MARKER);

        let declared_type = match column.r#type.as_deref() {
            Some(type_str) => Some(parse_declared_type(type_str).map_err(|source| {
                invalid(
                    policy,
                    dataset,
                    format!("Column '{}' has an invalid 'type': {source}", column.name),
                )
            })?),
            None => None,
        };
        let nullable = column.nullable.unwrap_or(true);

        let source = if let Some(marker) = catch_all_marker {
            // Validate the marker value is exactly "*".
            let is_wildcard = matches!(marker, Value::String(s) if s == JSON_OBJECT_WILDCARD);
            if !is_wildcard {
                return Err(invalid(
                    policy,
                    dataset,
                    format!(
                        "Column '{}' has invalid '{JSON_OBJECT_MARKER}' value: {marker:?}. Only '{JSON_OBJECT_WILDCARD}' is supported.",
                        column.name
                    ),
                ));
            }
            ColumnSource::JsonObject
        } else {
            ColumnSource::Field
        };

        columns.push(ProjectedColumn {
            output_name: column.name.clone(),
            source,
            declared_type,
            nullable,
        });
    }

    let projection = SchemaProjection::new(columns, &policy.required_columns)
        .map_err(|e| projection_error(policy, dataset, &e))?;
    Ok(Some(projection))
}

fn invalid(
    policy: &ProjectionPolicy<'_>,
    dataset: &DatasetSpec,
    message: String,
) -> DataConnectorError {
    DataConnectorError::InvalidConfigurationNoSource {
        dataconnector: policy.connector.to_string(),
        connector_component: ConnectorComponent::from(dataset),
        message,
    }
}

fn projection_error(
    policy: &ProjectionPolicy<'_>,
    dataset: &DatasetSpec,
    err: &SchemaProjectionError,
) -> DataConnectorError {
    invalid(policy, dataset, err.to_string())
}
